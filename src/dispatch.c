/*
 * dispatch.c - Consumer dispatcher implementation (push model).
 *
 * Data model (all accessed only on the loop thread, hence lock-free):
 *   - consumer_t : one Basic.Consume registration. Lives in two structures at
 *                  once: a per-queue `group_t` (for round-robin delivery) and
 *                  the dispatcher's global linked list (for ack lookup and
 *                  connection cleanup).
 *   - group_t    : the set of consumers on a single queue, plus a round-robin
 *                  cursor. Stored in dispatcher->groups keyed by queue name.
 *   - pending    : queues that have been notified and need draining; serviced
 *                  by the uv_async callback.
 *
 * Threading: each worker thread owns one dispatcher, and all consumer state
 * (groups, the global list, unacked) is touched only on that worker's loop
 * thread - so it stays lock-free. The single exception is `pending`: a publish
 * on ANY thread wakes the relevant dispatchers via the queue waiter list, so
 * `pending` is guarded by `pending_lock` and the actual uv_async_send is
 * thread-safe. Servicing (which writes to sockets) only ever runs on the
 * owning loop thread.
 */
#include "dispatch.h"
#include "protocol.h"
#include "frame.h"
#include "queue.h"
#include "message.h"
#include "hashmap.h"
#include "net.h"
#include "cluster.h"
#include "logger.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint64_t          tag;
    beaver_message_t *msg; /* reference held until acked or requeued */
} unacked_t;

typedef struct group group_t;

typedef struct consumer {
    beaver_conn_t   *conn;
    uint16_t         channel;
    char            *tag;        /* owned */
    beaver_queue_t  *queue;      /* owned reference */
    char            *queue_name; /* owned */
    int              no_ack;

    unacked_t       *unacked;
    size_t           n_unacked, cap_unacked;

    group_t         *group;      /* owning per-queue group */
    struct consumer *prev, *next; /* dispatcher global list */
} consumer_t;

struct group {
    beaver_queue_t *queue;       /* owned reference */
    consumer_t    **arr;         /* consumers (borrowed; owned by global list) */
    size_t          count, cap;
    size_t          rr;          /* round-robin cursor */
};

struct beaver_dispatcher {
    uv_loop_t       *loop;
    beaver_broker_t *broker;

    uv_async_t       async;
    int              async_closing;

    hashmap_t       *groups;     /* queue_name -> group_t* */
    consumer_t      *consumers;  /* head of global list */

    pthread_mutex_t  pending_lock; /* guards pending/n_pending/cap_pending */
    beaver_queue_t **pending;    /* queues needing service (refs held) */
    size_t           n_pending, cap_pending;

    uint64_t         next_delivery_tag;

    struct cluster_node *cluster;       /* consume-watermark replication, or NULL */
    uv_timer_t           consume_timer; /* periodically replicates watermarks */
    int                  consume_timer_on;
};

/* ---- forward decls ------------------------------------------------------- */
static void on_async(uv_async_t *h);
static void dispatcher_notify(beaver_dispatcher_t *d, beaver_queue_t *q);
static void dispatcher_queue_waiter(void *ctx, beaver_queue_t *q);

/* ========================================================================= */
/* construction / teardown                                                    */
/* ========================================================================= */

beaver_dispatcher_t *dispatcher_new(uv_loop_t *loop, beaver_broker_t *broker)
{
    beaver_dispatcher_t *d = calloc(1, sizeof(*d));
    if (!d)
        return NULL;
    d->loop   = loop;
    d->broker = broker;
    d->groups = hashmap_new();
    if (!d->groups) {
        free(d);
        return NULL;
    }
    if (pthread_mutex_init(&d->pending_lock, NULL) != 0) {
        hashmap_free(d->groups, NULL);
        free(d);
        return NULL;
    }
    if (uv_async_init(loop, &d->async, on_async) != 0) {
        pthread_mutex_destroy(&d->pending_lock);
        hashmap_free(d->groups, NULL);
        free(d);
        return NULL;
    }
    d->async.data = d;
    d->next_delivery_tag = 0;
    uv_timer_init(loop, &d->consume_timer);
    d->consume_timer.data = d;
    return d;
}

/* Periodically replicate each consumed queue's watermark so the other nodes can
 * drain their replica copies. Runs on this worker's loop. */
/* Replicate a queue's current consume watermark (if it advanced). */
static void replicate_watermark(beaver_dispatcher_t *d, beaver_queue_t *q)
{
    if (!d->cluster)
        return;
    uint64_t wm = queue_consume_watermark(q);
    if (wm > 0 && queue_consume_mark_replicated(q, wm))
        cluster_replicate_consume(d->cluster, queue_vhost(q), queue_name(q), wm);
}

static void consume_timer_cb(uv_timer_t *t)
{
    beaver_dispatcher_t *d = t->data;
    if (!d->cluster)
        return;
    for (consumer_t *c = d->consumers; c; c = c->next)
        replicate_watermark(d, c->queue);
}

void dispatcher_note_pull(beaver_dispatcher_t *d, beaver_queue_t *q, uint64_t cid)
{
    if (!d || !d->cluster || !q || cid == 0)
        return;
    /* Auto-ack: the message is delivered AND consumed in one step (no_ack). That
     * advances the watermark; replicate it so the other nodes drop their copy. */
    queue_consume_on_deliver(q, cid, 1);
    replicate_watermark(d, q);
}

void dispatcher_set_cluster(beaver_dispatcher_t *d, struct cluster_node *cluster)
{
    if (!d)
        return;
    d->cluster = cluster;
    if (cluster && !d->consume_timer_on) {
        d->consume_timer_on = 1;
        uv_timer_start(&d->consume_timer, consume_timer_cb, 50, 50);
    }
}

void dispatcher_request_close(beaver_dispatcher_t *d)
{
    if (!d || d->async_closing)
        return;
    d->async_closing = 1;
    uv_close((uv_handle_t *)&d->async, NULL);
    if (!uv_is_closing((uv_handle_t *)&d->consume_timer))
        uv_close((uv_handle_t *)&d->consume_timer, NULL);
}

static void free_group_value(void *v)
{
    group_t *g = v;
    queue_unref(g->queue);
    free(g->arr);
    free(g);
}

/* Detach a consumer from its group + global list and free it. If `requeue`,
 * its unacked messages are put back on the queue (else just unref'd). */
static void consumer_destroy(beaver_dispatcher_t *d, consumer_t *c, int requeue)
{
    queue_consumers_dec(c->queue); /* management metric */

    for (size_t i = 0; i < c->n_unacked; i++) {
        if (requeue)
            queue_enqueue(c->queue, c->unacked[i].msg); /* stays in unacked set */
        else
            /* dropped, not requeued: let the watermark pass it so replicas
             * drop their copies too (consumed-and-gone everywhere). */
            queue_consume_on_ack(c->queue, c->unacked[i].msg->cluster_id);
        message_unref(c->unacked[i].msg);
    }
    free(c->unacked);

    /* Flush the final consume watermark before we forget this consumer, so a
     * short-lived consumer's last acks still drain the replicas. */
    replicate_watermark(d, c->queue);

    /* Remove from its group (swap-remove). */
    group_t *g = c->group;
    for (size_t i = 0; i < g->count; i++) {
        if (g->arr[i] == c) {
            g->arr[i] = g->arr[g->count - 1];
            g->count--;
            if (g->rr > g->count)
                g->rr = 0;
            break;
        }
    }
    if (g->count == 0) {
        /* Last consumer on this queue for this worker: stop being woken for it
         * and drop the now-empty group. */
        queue_remove_waiter(g->queue, dispatcher_queue_waiter, d);
        hashmap_remove(d->groups, c->queue_name);
        queue_unref(g->queue);
        free(g->arr);
        free(g);
    } else if (requeue) {
        dispatcher_notify(d, g->queue); /* let remaining consumers take them */
    }

    /* Remove from the global list. */
    if (c->prev)
        c->prev->next = c->next;
    else
        d->consumers = c->next;
    if (c->next)
        c->next->prev = c->prev;

    queue_unref(c->queue);
    free(c->tag);
    free(c->queue_name);
    free(c);
}

void dispatcher_free(beaver_dispatcher_t *d)
{
    if (!d)
        return;
    /* Drop every consumer without requeuing (we're tearing down). */
    while (d->consumers)
        consumer_destroy(d, d->consumers, 0);

    hashmap_free(d->groups, free_group_value);

    for (size_t i = 0; i < d->n_pending; i++)
        queue_unref(d->pending[i]);
    free(d->pending);
    pthread_mutex_destroy(&d->pending_lock);
    free(d);
}

/* ========================================================================= */
/* pending set + async notification                                           */
/* ========================================================================= */

/* Mark a queue as needing service and wake the loop. Thread-safe: may be
 * called from a publishing thread (via the queue waiter list) as well as from
 * this dispatcher's own loop thread. */
static void dispatcher_notify(beaver_dispatcher_t *d, beaver_queue_t *q)
{
    pthread_mutex_lock(&d->pending_lock);
    for (size_t i = 0; i < d->n_pending; i++) {
        if (d->pending[i] == q) {
            pthread_mutex_unlock(&d->pending_lock);
            return; /* already pending (holds a ref) */
        }
    }
    if (d->n_pending == d->cap_pending) {
        size_t nc = d->cap_pending ? d->cap_pending * 2 : 8;
        beaver_queue_t **np = realloc(d->pending, nc * sizeof(*np));
        if (!np) {
            pthread_mutex_unlock(&d->pending_lock);
            return; /* drop the notification; backlog drains on next event */
        }
        d->pending     = np;
        d->cap_pending = nc;
    }
    d->pending[d->n_pending++] = queue_ref(q);
    pthread_mutex_unlock(&d->pending_lock);
    uv_async_send(&d->async); /* uv_async_send is itself thread-safe */
}

/* queue_waiter_fn adapter: the broker calls this (on the publishing thread)
 * when a message lands in a queue this dispatcher has consumers on. */
static void dispatcher_queue_waiter(void *ctx, beaver_queue_t *q)
{
    dispatcher_notify((beaver_dispatcher_t *)ctx, q);
}

/* ========================================================================= */
/* delivery                                                                   */
/* ========================================================================= */

static int consumer_add_unacked(consumer_t *c, uint64_t tag,
                                beaver_message_t *msg)
{
    if (c->n_unacked == c->cap_unacked) {
        size_t nc = c->cap_unacked ? c->cap_unacked * 2 : 4;
        unacked_t *nu = realloc(c->unacked, nc * sizeof(*nu));
        if (!nu) {
            message_unref(msg);
            return -1;
        }
        c->unacked     = nu;
        c->cap_unacked = nc;
    }
    c->unacked[c->n_unacked].tag = tag;
    c->unacked[c->n_unacked].msg = msg;
    c->n_unacked++;
    return 0;
}

/*
 * Send one AMQP delivery. Hot path: the Basic.Deliver method frame, the content
 * header frame, and the content body frame(s) are built into a SINGLE buffer
 * and handed to the socket with one write (one syscall, no extra copy) instead
 * of three separate writes. Returns 0 on success, -1 on failure.
 */
static int deliver(beaver_dispatcher_t *d, consumer_t *c,
                   beaver_message_t *msg)
{
    uint64_t tag = ++d->next_delivery_tag;
    bmqp_buf_t out;
    bmqp_buf_init(&out);

    /* Method frame: Basic.Deliver(consumer-tag, delivery-tag, redelivered,
     * exchange, routing-key). Built directly into `out` (no temp buffer). */
    size_t mstart = bmqp_frame_begin(&out, BMQP_FRAME_METHOD, c->channel);
    bmqp_buf_put_u16(&out, BMQP_CLASS_BASIC);
    bmqp_buf_put_u16(&out, BMQP_BASIC_DELIVER);
    bmqp_buf_put_shortstr(&out, c->tag);
    bmqp_buf_put_u64(&out, tag);
    bmqp_buf_put_u8(&out, 0);                 /* redelivered */
    bmqp_buf_put_shortstr(&out, msg->exchange);
    bmqp_buf_put_shortstr(&out, msg->routing_key);
    bmqp_frame_finish(&out, mstart);

    /* Content header frame: class, weight, body-size, then the saved property
     * section (or empty property-flags). */
    size_t hstart = bmqp_frame_begin(&out, BMQP_FRAME_HEADER, c->channel);
    bmqp_buf_put_u16(&out, BMQP_CLASS_BASIC);
    bmqp_buf_put_u16(&out, 0);                /* weight */
    bmqp_buf_put_u64(&out, (uint64_t)msg->body_len);
    if (msg->props && msg->props_len)
        bmqp_buf_put_bytes(&out, msg->props, msg->props_len);
    else
        bmqp_buf_put_u16(&out, 0);            /* property-flags = none */
    bmqp_frame_finish(&out, hstart);

    uint32_t fmax = c->conn->frame_max ? c->conn->frame_max : 131072;
    size_t chunk = fmax > BMQP_FRAME_OVERHEAD ? fmax - BMQP_FRAME_OVERHEAD : 4096;

    int sent;
    if (msg->body_len > 0 && msg->body_len <= chunk) {
        /* Common case - the body fits in one frame. Append only the body frame's
         * 7-byte header and stream the body itself via scatter-gather, so the
         * (possibly large) payload is never copied into the send buffer. */
        bmqp_buf_put_u8(&out, BMQP_FRAME_BODY);
        bmqp_buf_put_u16(&out, c->channel);
        bmqp_buf_put_u32(&out, (uint32_t)msg->body_len);
        if (out.error) {
            bmqp_buf_free(&out);
            return -1;
        }
        char *payload = (char *)out.data;
        size_t plen = out.len;
        out.data = NULL; /* detach; ownership moves to the send call */
        bmqp_buf_free(&out);
        /* send_delivery appends the body bytes + frame-end sentinel itself. */
        sent = beaver_conn_send_delivery(c->conn, payload, plen, msg);
    } else {
        /* Empty body (no body frame) or a body large enough to be chunked across
         * multiple frames: assemble the whole thing into one buffer. */
        const uint8_t *bp = msg->body;
        size_t rem = msg->body_len;
        while (rem > 0) {
            size_t n = rem < chunk ? rem : chunk;
            bmqp_frame_write(&out, BMQP_FRAME_BODY, c->channel, bp, n);
            bp += n;
            rem -= n;
        }
        if (out.error) {
            bmqp_buf_free(&out);
            return -1;
        }
        char *payload = (char *)out.data;
        size_t plen = out.len;
        out.data = NULL; /* detach; ownership moves to beaver_conn_send_owned */
        bmqp_buf_free(&out);
        sent = beaver_conn_send_owned(c->conn, payload, plen);
    }
    if (sent != 0)
        return -1;

    if (!c->no_ack)
        consumer_add_unacked(c, tag, message_ref(msg));
    /* Cluster: record the consume so replica queues can drop their copies. */
    queue_consume_on_deliver(c->queue, msg->cluster_id, c->no_ack);
    return 0;
}

/* Drain a group's queue to its consumers, round-robin, until empty, all
 * consumers are backpressured/closing, or a delivery fails. */
static void service_group(beaver_dispatcher_t *d, group_t *g)
{
    while (g->count > 0) {
        /* Pick the next consumer that can actually accept a message: not
         * closing and whose write buffer is below the high-water mark. A full
         * consumer is marked paused and resumed from on_write once it drains
         * (flow control / backpressure), which bounds memory and keeps
         * latency steady instead of delivering one giant burst. */
        consumer_t *c = NULL;
        for (size_t tries = 0; tries < g->count; tries++) {
            consumer_t *cand = g->arr[g->rr % g->count];
            g->rr++;
            if (cand->conn->closing)
                continue;
            if (cand->conn->send_paused || beaver_conn_send_full(cand->conn)) {
                cand->conn->send_paused = 1;
                continue;
            }
            c = cand;
            break;
        }
        if (!c)
            break; /* every consumer is full or closing; resume later */

        beaver_message_t *msg = queue_dequeue(g->queue);
        if (!msg)
            break;

        if (deliver(d, c, msg) != 0) {
            queue_enqueue(g->queue, msg); /* put it back; peer went away */
            message_unref(msg);
            break;
        }
        message_unref(msg); /* drop the dequeue reference */
    }
}

static void on_async(uv_async_t *h)
{
    beaver_dispatcher_t *d = h->data;

    /* Atomically take ownership of the current pending list so re-notifications
     * (possibly from other threads) during servicing accumulate into a fresh
     * list and re-trigger the async. Servicing then runs lock-free on this
     * worker's own thread. */
    pthread_mutex_lock(&d->pending_lock);
    beaver_queue_t **list = d->pending;
    size_t n = d->n_pending;
    d->pending     = NULL;
    d->n_pending   = 0;
    d->cap_pending = 0;
    pthread_mutex_unlock(&d->pending_lock);

    for (size_t i = 0; i < n; i++) {
        beaver_queue_t *q = list[i];
        char vkey[512];   /* groups are keyed by (vhost, name), like the broker */
        broker_vkey(vkey, queue_vhost(q), queue_name(q));
        group_t *g = hashmap_get(d->groups, vkey);
        if (g && g->count > 0)
            service_group(d, g);
        queue_unref(q);
    }
    free(list);
}

/* ========================================================================= */
/* consumer registration / ack / cleanup                                      */
/* ========================================================================= */

static int group_add(group_t *g, consumer_t *c)
{
    if (g->count == g->cap) {
        size_t nc = g->cap ? g->cap * 2 : 4;
        consumer_t **na = realloc(g->arr, nc * sizeof(*na));
        if (!na)
            return -1;
        g->arr = na;
        g->cap = nc;
    }
    g->arr[g->count++] = c;
    return 0;
}

int dispatcher_add_consumer(beaver_dispatcher_t *d, beaver_conn_t *conn,
                            uint16_t channel, const char *tag,
                            const char *vhost, const char *queue_name,
                            int no_ack)
{
    beaver_queue_t *q = broker_get_queue(d->broker, vhost, queue_name);
    if (!q)
        return -1; /* cannot consume from a queue that does not exist */

    /* Groups are keyed by the same composite (vhost, name) key as the broker
     * registry, so "orders" in two vhosts never share a group. */
    char vkey[512];
    broker_vkey(vkey, vhost, queue_name);

    consumer_t *c = calloc(1, sizeof(*c));
    group_t    *g = hashmap_get(d->groups, vkey);
    int new_group = 0;
    if (!g) {
        g = calloc(1, sizeof(*g));
        new_group = 1;
    }
    if (!c || !g) {
        free(c);
        if (new_group)
            free(g);
        queue_unref(q);
        return -1;
    }

    c->conn       = conn;
    c->channel    = channel;
    c->tag        = strdup(tag);
    c->queue      = q; /* take the reference from broker_get_queue */
    c->queue_name = strdup(vkey);   /* composite: used only as the group key */
    c->no_ack     = no_ack;
    if (!c->tag || !c->queue_name) {
        free(c->tag);
        free(c->queue_name);
        free(c);
        if (new_group)
            free(g);
        queue_unref(q);
        return -1;
    }

    if (new_group) {
        g->queue = queue_ref(q);
        if (hashmap_put(d->groups, vkey, g, NULL) != 0) {
            queue_unref(g->queue);
            free(g);
            free(c->tag);
            free(c->queue_name);
            free(c);
            queue_unref(q);
            return -1;
        }
    }

    if (group_add(g, c) != 0) {
        free(c->tag);
        free(c->queue_name);
        queue_unref(q);
        free(c);
        return -1;
    }
    c->group = g;

    /* Link into the global list. */
    c->next = d->consumers;
    if (d->consumers)
        d->consumers->prev = c;
    d->consumers = c;

    queue_consumers_inc(q); /* management metric */

    /* First consumer on this queue for this worker: register interest so the
     * broker wakes us (cross-thread) when a message is routed here. */
    if (new_group)
        queue_add_waiter(q, dispatcher_queue_waiter, d);

    LOG_INFO("consumer '%s' registered on queue '%s' (%s)", tag, queue_name,
             no_ack ? "no-ack" : "manual-ack");

    /* Deliver any backlog. */
    dispatcher_notify(d, q);
    return 0;
}

void dispatcher_ack(beaver_dispatcher_t *d, beaver_conn_t *conn,
                    uint16_t channel, uint64_t delivery_tag)
{
    for (consumer_t *c = d->consumers; c; c = c->next) {
        if (c->conn != conn || c->channel != channel)
            continue;
        for (size_t i = 0; i < c->n_unacked; i++) {
            if (c->unacked[i].tag == delivery_tag) {
                queue_consume_on_ack(c->queue, c->unacked[i].msg->cluster_id);
                message_unref(c->unacked[i].msg);
                c->unacked[i] = c->unacked[c->n_unacked - 1];
                c->n_unacked--;
                LOG_DEBUG("ack delivery_tag=%" PRIu64 " on '%s'",
                          delivery_tag, c->tag);
                return;
            }
        }
    }
    LOG_WARN("ack for unknown delivery_tag=%" PRIu64, delivery_tag);
}

void dispatcher_resume_conn(beaver_dispatcher_t *d, beaver_conn_t *conn)
{
    if (!d)
        return;
    /* The connection's write buffer drained; re-attempt delivery on every
     * queue it consumes from. */
    for (consumer_t *c = d->consumers; c; c = c->next)
        if (c->conn == conn)
            dispatcher_notify(d, c->queue);
}

void dispatcher_cancel(beaver_dispatcher_t *d, beaver_conn_t *conn,
                       uint16_t channel, const char *tag)
{
    if (!d)
        return;
    for (consumer_t *c = d->consumers; c; c = c->next) {
        if (c->conn == conn && c->channel == channel &&
            strcmp(c->tag, tag) == 0) {
            LOG_INFO("cancelling consumer '%s'; requeuing %zu unacked",
                     c->tag, c->n_unacked);
            consumer_destroy(d, c, 1 /* requeue unacked */);
            return;
        }
    }
}

void dispatcher_remove_connection(beaver_dispatcher_t *d, beaver_conn_t *conn)
{
    if (!d)
        return;
    consumer_t *c = d->consumers;
    while (c) {
        consumer_t *next = c->next;
        if (c->conn == conn) {
            LOG_INFO("removing consumer '%s' (connection closed); requeuing "
                     "%zu unacked", c->tag, c->n_unacked);
            consumer_destroy(d, c, 1 /* requeue unacked */);
        }
        c = next;
    }
}
