/*
 * protocol.c - AMQP 0-9-1 protocol state machine.
 *
 * This implements the server side of AMQP 0-9-1 at the wire level, so standard
 * clients (pika, the Java/PHP clients, ...) interoperate as if talking to
 * RabbitMQ. It owns:
 *   - input accumulation (TCP is a byte stream; frames may span reads),
 *   - frame extraction and method dispatch,
 *   - the per-connection state machine + channel bookkeeping,
 *   - exact AMQP method argument encoding/decoding,
 *   - multi-frame content assembly: a Basic.Publish method frame is followed by
 *     a content-header frame (body size + properties) and one or more
 *     content-body frames, which are reassembled into a single message.
 *
 * The historical bmqp_* helper names denote BeaverMQ's serialization
 * utilities; the bytes on the wire are pure AMQP 0-9-1.
 */
#include "protocol.h"
#include "frame.h"
#include "net.h"
#include "broker.h"
#include "cluster.h"
#include "authstore.h"
#include "authlimit.h"
#include "dispatch.h"
#include "message.h"
#include "logger.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* protocol.c is otherwise libuv-agnostic; this one exception (a short poll
 * timer) is what lets a deferred cluster-commit response happen without
 * blocking the connection's event loop - see await_cluster_commit below. */
#include <uv.h>

/* Sanity cap for a single message body when max_message_size is unset
 * (prevents a hostile body-size field from triggering a huge allocation). */
#define AMQP_MAX_BODY_SIZE (128u * 1024u * 1024u)
#define AMQP_DEFAULT_FRAME_MAX 131072u
#define AMQP_CHANNEL_MAX 2047u        /* advertised in Connection.Tune */
#define AMQP_HEARTBEAT_SECONDS 60u    /* suggested in Connection.Tune */

/* A Basic.Get delivery with no_ack=false, awaiting Basic.Ack/Reject/Nack.
 * Basic.Get has no consumer_t (it's a direct dequeue, not push delivery), so
 * without this the broker had nothing to settle against: it always behaved
 * as no_ack even when the client asked for manual ack, and a client that
 * crashed before acking lost the message with no way to recover it. */
typedef struct {
    uint64_t          tag;
    beaver_message_t *msg;   /* ref held until acked/rejected/requeued */
    beaver_queue_t   *queue; /* ref held for the same reason */
} get_unacked_t;

/* One open channel: its id plus per-channel state (Basic.Qos prefetch). */
typedef struct {
    uint16_t id;
    uint16_t prefetch;   /* Basic.Qos prefetch-count; 0 = unlimited */
    get_unacked_t *get_unacked;
    size_t         n_get_unacked, cap_get_unacked;
} proto_chan_t;

struct beaver_proto {
    beaver_conn_t *conn;        /* owning connection (back-pointer) */
    bmqp_state_t   state;

    int            header_received;  /* 8-byte greeting consumed */
    int            connection_open;  /* Connection.Open completed */

    /* Input accumulation buffer for partial frames. Bytes in
     * [inbuf_pos, inbuf_len) are buffered-but-unprocessed; the consumed prefix
     * is reclaimed (a single memmove) on the next append, so processing a batch
     * of N frames is O(total bytes), not O(N * bytes). */
    uint8_t       *inbuf;
    size_t         inbuf_pos;
    size_t         inbuf_len;
    size_t         inbuf_cap;

    /* Negotiated tuning parameters (from Connection.TuneOk). */
    uint16_t       channel_max;
    uint32_t       frame_max;
    uint16_t       heartbeat;

    /* Set of currently-open channels (small; most clients use 1). */
    proto_chan_t  *channels;
    size_t         n_channels;
    size_t         cap_channels;

    uint64_t       consumer_seq;     /* auto-generated consumer tags / queue names */

    /* ---- in-progress content assembly (Basic.Publish) ---- */
    int            pub_active;        /* a publish awaits its content frames */
    int            pub_have_header;   /* content header received */
    uint16_t       pub_channel;
    char           pub_exchange[256];
    char           pub_routing_key[256];
    uint64_t       pub_body_size;     /* declared in the content header */
    uint8_t       *pub_body;          /* accumulation buffer */
    size_t         pub_body_received;
    uint8_t       *pub_props;         /* raw property-flags + property-list */
    size_t         pub_props_len;

    char           vhost[128];
    char           user[128];        /* authenticated username (perm checks) */
    uint32_t       user_tags;        /* authstore tag bits (admin/management) */

    struct pending_cluster_op *pending_ops; /* in-flight deferred cluster-commit
                                             * responses (see await_cluster_commit) */
};

/* ---- forward declarations ------------------------------------------------ */
static void proto_close(beaver_proto_t *p);
static void proto_fatal(beaver_proto_t *p, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void finalize_publish(beaver_proto_t *p, const uint8_t *body,
                             size_t body_len);
static void send_method(beaver_proto_t *p, uint16_t channel,
                        uint16_t class_id, uint16_t method_id,
                        const bmqp_buf_t *args);
static void send_channel_close(beaver_proto_t *p, uint16_t channel,
                               uint16_t code, const char *text,
                               uint16_t class_id, uint16_t method_id);

/* ========================================================================= */
/* small helpers                                                              */
/* ========================================================================= */

const char *bmqp_state_name(bmqp_state_t s)
{
    switch (s) {
    case BMQP_STATE_CONNECTED:    return "CONNECTED";
    case BMQP_STATE_HANDSHAKE:    return "HANDSHAKE";
    case BMQP_STATE_CHANNEL_OPEN: return "CHANNEL_OPEN";
    case BMQP_STATE_ACTIVE:       return "ACTIVE";
    case BMQP_STATE_CLOSING:      return "CLOSING";
    }
    return "?";
}

bmqp_state_t protocol_state(const beaver_proto_t *p) { return p->state; }

/* State only ever moves forward. CLOSING is terminal. The state is mirrored
 * into conn->amqp_state (atomic) so the management API can read it safely from
 * another thread. */
static void proto_advance(beaver_proto_t *p, bmqp_state_t s)
{
    if (p->state == BMQP_STATE_CLOSING || s <= p->state)
        return;
    LOG_DEBUG("conn #%" PRIu64 ": state %s -> %s",
              p->conn->id, bmqp_state_name(p->state), bmqp_state_name(s));
    p->state = s;
    atomic_store_explicit(&p->conn->amqp_state, (int)s, memory_order_relaxed);
}

/* Copy a non-terminated wire string into a fixed, NUL-terminated buffer. */
static void copy_str(char *dst, size_t cap, const char *src, size_t n)
{
    if (cap == 0)
        return;
    size_t c = (n < cap - 1) ? n : cap - 1;
    if (src && c)
        memcpy(dst, src, c);
    dst[c] = '\0';
}

/* Vhost / queue / exchange names travel into the broker's composite
 * "<vhost>\x01<name>" registry key, so control bytes (including \x01 itself)
 * must never appear in them - otherwise crafted names could alias objects
 * across vhosts. Rejects ASCII control characters and DEL. */
static int name_ok(const char *s)
{
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || (unsigned char)*s == 0x7f)
            return 0;
    return 1;
}

/* ---- input buffer management --------------------------------------------- */

static int inbuf_append(beaver_proto_t *p, const uint8_t *data, size_t len)
{
    /* Reclaim the already-consumed prefix with a single shift. */
    if (p->inbuf_pos > 0) {
        size_t rem = p->inbuf_len - p->inbuf_pos;
        if (rem)
            memmove(p->inbuf, p->inbuf + p->inbuf_pos, rem);
        p->inbuf_len = rem;
        p->inbuf_pos = 0;
    }
    if (p->inbuf_len + len > p->inbuf_cap) {
        size_t newcap = p->inbuf_cap ? p->inbuf_cap : 256;
        while (newcap < p->inbuf_len + len)
            newcap *= 2;
        uint8_t *nb = realloc(p->inbuf, newcap);
        if (!nb)
            return 0;
        p->inbuf     = nb;
        p->inbuf_cap = newcap;
    }
    memcpy(p->inbuf + p->inbuf_len, data, len);
    p->inbuf_len += len;
    return 1;
}

/* ---- channel bookkeeping ------------------------------------------------- */

static proto_chan_t *channel_find(beaver_proto_t *p, uint16_t ch)
{
    for (size_t i = 0; i < p->n_channels; i++)
        if (p->channels[i].id == ch)
            return &p->channels[i];
    return NULL;
}

static int channel_is_open(beaver_proto_t *p, uint16_t ch)
{
    return channel_find(p, ch) != NULL;
}

static int channel_add(beaver_proto_t *p, uint16_t ch)
{
    if (p->n_channels == p->cap_channels) {
        size_t nc = p->cap_channels ? p->cap_channels * 2 : 4;
        proto_chan_t *na = realloc(p->channels, nc * sizeof(proto_chan_t));
        if (!na)
            return 0;
        p->channels     = na;
        p->cap_channels = nc;
    }
    p->channels[p->n_channels].id             = ch;
    p->channels[p->n_channels].prefetch       = 0;
    p->channels[p->n_channels].get_unacked     = NULL;
    p->channels[p->n_channels].n_get_unacked   = 0;
    p->channels[p->n_channels].cap_get_unacked = 0;
    p->n_channels++;
    return 1;
}

/* Requeue every still-outstanding Basic.Get manual-ack delivery on `pc` (the
 * channel is going away - a client that crashed/closed before acking must
 * not lose the message). */
static void chan_release_get_unacked(proto_chan_t *pc)
{
    for (size_t i = 0; i < pc->n_get_unacked; i++) {
        get_unacked_t *g = &pc->get_unacked[i];
        /* Internal requeue: bypass publisher caps so a full queue cannot drop
         * an unacked Basic.Get delivery as the channel tears down. */
        if (queue_requeue_internal(g->queue, g->msg) != 0)
            LOG_ERROR("OOM requeuing unacked Basic.Get message on channel "
                      "teardown (queue=%s): message dropped",
                      queue_name(g->queue));
        message_unref(g->msg);
        queue_unref(g->queue);
    }
    free(pc->get_unacked);
    pc->get_unacked = NULL;
    pc->n_get_unacked = pc->cap_get_unacked = 0;
}

static void channel_remove(beaver_proto_t *p, uint16_t ch)
{
    for (size_t i = 0; i < p->n_channels; i++) {
        if (p->channels[i].id == ch) {
            chan_release_get_unacked(&p->channels[i]);
            p->channels[i] = p->channels[--p->n_channels];
            return;
        }
    }
}

/* Track a Basic.Get delivery (no_ack=false) awaiting Basic.Ack/Reject/Nack.
 * Takes ownership of both refs (released on settle or channel/conn teardown). */
static int chan_get_unacked_add(proto_chan_t *pc, uint64_t tag,
                                beaver_message_t *msg, beaver_queue_t *queue)
{
    if (pc->n_get_unacked == pc->cap_get_unacked) {
        size_t nc = pc->cap_get_unacked ? pc->cap_get_unacked * 2 : 8;
        get_unacked_t *na = realloc(pc->get_unacked, nc * sizeof(*na));
        if (!na)
            return 0;
        pc->get_unacked     = na;
        pc->cap_get_unacked = nc;
    }
    pc->get_unacked[pc->n_get_unacked].tag   = tag;
    pc->get_unacked[pc->n_get_unacked].msg   = msg;
    pc->get_unacked[pc->n_get_unacked].queue = queue;
    pc->n_get_unacked++;
    return 1;
}

/* Settle entries matching delivery_tag (multiple: everything <= tag, or ALL
 * if delivery_tag == 0, matching dispatch.c's settle_unacked semantics).
 * Returns the number settled. */
static size_t chan_get_unacked_settle(proto_chan_t *pc, uint64_t delivery_tag,
                                      int multiple, int requeue)
{
    size_t settled = 0;
    for (size_t i = 0; i < pc->n_get_unacked; ) {
        uint64_t t = pc->get_unacked[i].tag;
        int match = multiple ? (delivery_tag == 0 || t <= delivery_tag)
                             : (t == delivery_tag);
        if (!match) {
            i++;
            continue;
        }
        get_unacked_t g = pc->get_unacked[i];
        if (requeue) {
            /* Internal requeue: bypass publisher caps so a reject/nack onto a
             * full queue cannot silently drop the message. */
            if (queue_requeue_internal(g.queue, g.msg) != 0)
                LOG_ERROR("OOM requeuing rejected Basic.Get message "
                          "(queue=%s): message dropped", queue_name(g.queue));
        } else
            queue_consume_on_ack(g.queue, g.msg->cluster_id);
        message_unref(g.msg);
        queue_unref(g.queue);
        pc->get_unacked[i] = pc->get_unacked[--pc->n_get_unacked];
        settled++;
        if (!multiple)
            break;
    }
    return settled;
}

/* ---- deferred cluster-commit responses ------------------------------------
 * A durable Queue/Exchange.Declare or Queue.Bind must not tell the client it
 * succeeded until the op actually COMMITS on the cluster - sending the *-Ok
 * right after creating the local object (the old behavior) let a client see
 * success even with no leader/quorum, or when the propose call itself failed
 * (OOM). AMQP method handling runs synchronously within one event-loop tick,
 * so we cannot block here; instead a short interval timer polls
 * cluster_proposal_status() and only then sends the (deferred) response. */
#define CLUSTER_WAIT_POLL_MS    5
#define CLUSTER_WAIT_TIMEOUT_MS 5000

typedef enum {
    PENDING_EXCHANGE_DECLARE_OK,
    PENDING_QUEUE_DECLARE_OK,
    PENDING_QUEUE_BIND_OK,
} pending_kind_t;

typedef struct pending_cluster_op {
    struct pending_cluster_op *next;
    beaver_proto_t      *p;         /* NULL once cancelled (conn/channel gone) */
    struct cluster_node *cluster;
    uint64_t             seq;
    uint16_t             channel;
    uint16_t             class_id, method_id; /* for the failure channel-exception */
    pending_kind_t       kind;
    char                 qname[256];  /* PENDING_QUEUE_DECLARE_OK only */
    uint32_t             depth;
    uint64_t             deadline_ms;
    uv_timer_t           timer;
} pending_cluster_op_t;

static void pending_op_closed_cb(uv_handle_t *h)
{
    free(h->data);
}

/* Detach op from its connection's list without touching the timer handle. */
static void detach_pending_op(pending_cluster_op_t *op)
{
    if (!op->p)
        return;
    pending_cluster_op_t **link = &op->p->pending_ops;
    while (*link && *link != op)
        link = &(*link)->next;
    if (*link)
        *link = op->next;
    op->p = NULL;
}

/* Cancel every pending op on `p` (channel_filter < 0) or just on one channel
 * (Channel.Close). Cancelled ops never touch `p` again (see the timer cb). */
static void cancel_pending_ops(beaver_proto_t *p, int channel_filter)
{
    pending_cluster_op_t **link = &p->pending_ops;
    while (*link) {
        pending_cluster_op_t *op = *link;
        if (channel_filter < 0 || op->channel == (uint16_t)channel_filter) {
            *link = op->next;
            op->p = NULL;
            op->next = NULL;
            uv_close((uv_handle_t *)&op->timer, pending_op_closed_cb);
        } else {
            link = &op->next;
        }
    }
}

static void pending_op_timer_cb(uv_timer_t *t)
{
    pending_cluster_op_t *op = t->data;
    if (!op->p) {
        uv_close((uv_handle_t *)t, pending_op_closed_cb);
        return;
    }
    cluster_proposal_status_t st = cluster_proposal_status(op->cluster, op->seq);
    if (st == CL_PROPOSAL_PENDING) {
        if (uv_now(t->loop) < op->deadline_ms)
            return; /* keep polling */
        st = CL_PROPOSAL_REJECTED; /* timed out */
    }
    beaver_proto_t *p = op->p;
    uint16_t channel = op->channel;
    detach_pending_op(op);
    if (st == CL_PROPOSAL_COMMITTED) {
        switch (op->kind) {
        case PENDING_EXCHANGE_DECLARE_OK:
            send_method(p, channel, BMQP_CLASS_EXCHANGE,
                       BMQP_EXCHANGE_DECLARE_OK, NULL);
            break;
        case PENDING_QUEUE_DECLARE_OK: {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, op->qname, strlen(op->qname));
            bmqp_buf_put_u32(&a, op->depth); /* message-count */
            bmqp_buf_put_u32(&a, 0);         /* consumer-count */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, &a);
            bmqp_buf_free(&a);
            break;
        }
        case PENDING_QUEUE_BIND_OK:
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND_OK, NULL);
            break;
        }
    } else {
        send_channel_close(p, channel, 541,
                           "INTERNAL_ERROR - cluster commit failed or timed out",
                           op->class_id, op->method_id);
    }
    uv_close((uv_handle_t *)&op->timer, pending_op_closed_cb);
}

/* Register a poll for `seq`'s outcome; qname/depth are only used for
 * PENDING_QUEUE_DECLARE_OK (pass qname=NULL, depth=0 otherwise). */
static void await_cluster_commit(beaver_proto_t *p, uint16_t channel, uint64_t seq,
                                 uint16_t class_id, uint16_t method_id,
                                 pending_kind_t kind, const char *qname, uint32_t depth)
{
    pending_cluster_op_t *op = calloc(1, sizeof(*op));
    if (!op) {
        send_channel_close(p, channel, 541,
                           "INTERNAL_ERROR - out of memory awaiting cluster commit",
                           class_id, method_id);
        return;
    }
    op->p         = p;
    op->cluster   = p->conn->server->cluster;
    op->seq       = seq;
    op->channel   = channel;
    op->class_id  = class_id;
    op->method_id = method_id;
    op->kind      = kind;
    if (qname)
        snprintf(op->qname, sizeof(op->qname), "%s", qname);
    op->depth = depth;
    uv_loop_t *loop = p->conn->handle.loop;
    op->deadline_ms = uv_now(loop) + CLUSTER_WAIT_TIMEOUT_MS;
    uv_timer_init(loop, &op->timer);
    op->timer.data = op;
    op->next = p->pending_ops;
    p->pending_ops = op;
    uv_timer_start(&op->timer, pending_op_timer_cb,
                   CLUSTER_WAIT_POLL_MS, CLUSTER_WAIT_POLL_MS);
}

/* ---- teardown helpers ---------------------------------------------------- */

static void publish_reset(beaver_proto_t *p)
{
    free(p->pub_body);
    free(p->pub_props);
    p->pub_body          = NULL;
    p->pub_props         = NULL;
    p->pub_active        = 0;
    p->pub_have_header   = 0;
    p->pub_body_size     = 0;
    p->pub_body_received = 0;
    p->pub_props_len     = 0;
}

static void proto_close(beaver_proto_t *p)
{
    if (p->state == BMQP_STATE_CLOSING)
        return;
    p->state = BMQP_STATE_CLOSING;
    atomic_store_explicit(&p->conn->amqp_state, (int)BMQP_STATE_CLOSING,
                          memory_order_relaxed);
    beaver_conn_close(p->conn);
}

static void proto_fatal(beaver_proto_t *p, const char *fmt, ...)
{
    char msg[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    LOG_ERROR("conn #%" PRIu64 " (%s): protocol error: %s",
              p->conn->id, p->conn->peer, msg);
    proto_close(p);
}

/* ========================================================================= */
/* outbound framing                                                           */
/* ========================================================================= */

int protocol_send_method(beaver_conn_t *conn, uint16_t channel,
                         uint16_t class_id, uint16_t method_id,
                         const uint8_t *args, size_t args_len)
{
    bmqp_buf_t pl;
    bmqp_buf_init(&pl);
    bmqp_buf_put_u16(&pl, class_id);
    bmqp_buf_put_u16(&pl, method_id);
    if (args && args_len)
        bmqp_buf_put_bytes(&pl, args, args_len);

    bmqp_buf_t frame;
    bmqp_buf_init(&frame);
    bmqp_frame_write(&frame, BMQP_FRAME_METHOD, channel, pl.data, pl.len);

    int rc = -1;
    if (!pl.error && !frame.error)
        rc = beaver_conn_send(conn, frame.data, frame.len);

    bmqp_buf_free(&pl);
    bmqp_buf_free(&frame);
    return rc;
}

int protocol_send_content(beaver_conn_t *conn, uint16_t channel,
                          uint16_t class_id, const void *body, size_t body_len,
                          const void *props, size_t props_len,
                          uint32_t frame_max)
{
    /* Content header frame: class-id, weight(0), body-size, then the property
     * section (property-flags + property list). If we have no captured
     * properties, emit a single property-flags short of 0 (no properties). */
    bmqp_buf_t hdr;
    bmqp_buf_init(&hdr);
    bmqp_buf_put_u16(&hdr, class_id);
    bmqp_buf_put_u16(&hdr, 0);                 /* weight (unused, must be 0) */
    bmqp_buf_put_u64(&hdr, (uint64_t)body_len);
    if (props && props_len)
        bmqp_buf_put_bytes(&hdr, props, props_len);
    else
        bmqp_buf_put_u16(&hdr, 0);             /* property-flags = none */

    bmqp_buf_t frame;
    bmqp_buf_init(&frame);
    bmqp_frame_write(&frame, BMQP_FRAME_HEADER, channel, hdr.data, hdr.len);
    int rc = (hdr.error || frame.error) ? -1
                                        : beaver_conn_send(conn, frame.data,
                                                           frame.len);
    bmqp_buf_free(&hdr);
    bmqp_buf_free(&frame);
    if (rc != 0)
        return -1;

    /* Content body frame(s), chunked to the negotiated frame size. */
    if (frame_max < 1024)
        frame_max = AMQP_DEFAULT_FRAME_MAX;
    size_t chunk = (size_t)frame_max - BMQP_FRAME_OVERHEAD;

    const uint8_t *p = body;
    size_t remaining = body_len;
    while (remaining > 0) {
        size_t n = remaining < chunk ? remaining : chunk;
        bmqp_buf_t bf;
        bmqp_buf_init(&bf);
        bmqp_frame_write(&bf, BMQP_FRAME_BODY, channel, p, n);
        rc = bf.error ? -1 : beaver_conn_send(conn, bf.data, bf.len);
        bmqp_buf_free(&bf);
        if (rc != 0)
            return -1;
        p += n;
        remaining -= n;
    }
    return 0;
}

/* Build and send a method frame from a proto context (closes on failure). */
static void send_method(beaver_proto_t *p, uint16_t channel,
                        uint16_t class_id, uint16_t method_id,
                        const bmqp_buf_t *args)
{
    if (args && args->error) {
        proto_fatal(p, "out of memory building method args");
        return;
    }
    const uint8_t *a = args ? args->data : NULL;
    size_t alen      = args ? args->len : 0;
    if (protocol_send_method(p->conn, channel, class_id, method_id, a, alen) != 0)
        proto_fatal(p, "failed to write outbound method frame");
}

/* Connection exception: send Connection.Close(code, text) and close the conn.
 * Used for auth/vhost-access failures (AMQP reply-code 403 ACCESS_REFUSED, etc). */
static void send_connection_close(beaver_proto_t *p, uint16_t code, const char *text)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, code);
    bmqp_buf_put_shortstr(&a, text);
    bmqp_buf_put_u16(&a, 0);   /* class-id  (0 = not method-specific) */
    bmqp_buf_put_u16(&a, 0);   /* method-id */
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_CLOSE, &a);
    bmqp_buf_free(&a);
    proto_close(p);
}

/* Channel exception: send Channel.Close(code, text) on `channel`. Used to reject
 * a single operation (e.g. 403 ACCESS_REFUSED on a permission-denied declare/
 * publish/consume) without tearing down the whole connection. */
static void send_channel_close(beaver_proto_t *p, uint16_t channel,
                               uint16_t code, const char *text,
                               uint16_t class_id, uint16_t method_id)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, code);
    bmqp_buf_put_shortstr(&a, text);
    bmqp_buf_put_u16(&a, class_id);
    bmqp_buf_put_u16(&a, method_id);
    send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_CLOSE, &a);
    bmqp_buf_free(&a);
}

/* Permission gate: 1 if the connection's user may `kind` access to `object` in
 * its vhost. (A connection only exists after a successful login, so the store
 * is never empty here; no bypass.) */
static int perm_ok(beaver_proto_t *p, auth_perm_t kind, const char *object)
{
    struct authstore *as = p->conn->server->authstore;
    if (!as)
        return 1;
    return authstore_check(as, p->user, p->vhost, kind, object);
}

/* Check a permission and, if denied, raise a 403 channel exception. Returns 1 if
 * allowed (caller proceeds), 0 if denied (caller must stop). */
static int require_perm(beaver_proto_t *p, uint16_t channel, auth_perm_t kind,
                        const char *object, uint16_t class_id, uint16_t method_id)
{
    if (perm_ok(p, kind, object))
        return 1;
    char text[600];
    snprintf(text, sizeof text, "ACCESS_REFUSED - access to '%s' in vhost '%s' refused for user '%s'",
             object, p->vhost, p->user);
    LOG_WARN("conn #%" PRIu64 ": %s", p->conn->id, text);
    send_channel_close(p, channel, 403, text, class_id, method_id);
    return 0;
}

/* Append an AMQP field table holding a few server-property strings. */
static void put_table_str(bmqp_buf_t *t, const char *key, const char *val)
{
    bmqp_buf_put_shortstr(t, key);
    bmqp_buf_put_u8(t, 'S');                  /* field value type: long string */
    bmqp_buf_put_longstr(t, val, strlen(val));
}

static void put_server_properties(bmqp_buf_t *a)
{
    bmqp_buf_t t;
    bmqp_buf_init(&t);
    put_table_str(&t, "product", "BeaverMQ");
    put_table_str(&t, "version", "1.0.0");
    put_table_str(&t, "platform", "C");
    if (!t.error) {
        bmqp_buf_put_u32(a, (uint32_t)t.len);
        bmqp_buf_put_bytes(a, t.data, t.len);
    } else {
        bmqp_buf_put_u32(a, 0);
    }
    bmqp_buf_free(&t);
}

/* ========================================================================= */
/* state guards                                                               */
/* ========================================================================= */

static int require_open(beaver_proto_t *p)
{
    if (!p->connection_open) {
        proto_fatal(p, "method received before connection was opened");
        return 0;
    }
    return 1;
}

static int require_channel(beaver_proto_t *p, uint16_t channel)
{
    if (!require_open(p))
        return 0;
    if (channel == 0 || !channel_is_open(p, channel)) {
        proto_fatal(p, "operation on channel %u which is not open", channel);
        return 0;
    }
    return 1;
}

/* ========================================================================= */
/* connection class                                                           */
/* ========================================================================= */

static void send_connection_start(beaver_proto_t *p)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u8(&a, 0);             /* version-major (AMQP 0-9-1) */
    bmqp_buf_put_u8(&a, 9);             /* version-minor */
    put_server_properties(&a);          /* server-properties (field table) */
    bmqp_buf_put_longstr(&a, "PLAIN", 5);   /* mechanisms (long string) */
    bmqp_buf_put_longstr(&a, "en_US", 5);   /* locales    (long string) */
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_START, &a);
    bmqp_buf_free(&a);
}

static void handle_connection(beaver_proto_t *p, uint16_t channel,
                              uint16_t method, bmqp_reader_t *r)
{
    if (channel != 0) {
        proto_fatal(p, "connection class method on non-zero channel %u", channel);
        return;
    }

    switch (method) {
    case BMQP_CONNECTION_START_OK: {
        size_t n, rlen;
        bmqp_read_longstr(r, &n);   /* client-properties (field table; skip) */
        bmqp_read_shortstr(r, &n);  /* mechanism (we only offer PLAIN) */
        const char *resp = bmqp_read_longstr(r, &rlen);  /* SASL response */
        bmqp_read_shortstr(r, &n);  /* locale */
        if (r->error) {
            proto_fatal(p, "malformed Connection.StartOk");
            return;
        }
        /* SASL PLAIN response is "[authzid]\0authcid\0passwd". Split on NULs. */
        char user[128] = "", pass[256] = "";
        if (resp && rlen > 0) {
            size_t a0 = 0;
            while (a0 < rlen && resp[a0] != '\0') a0++;       /* skip authzid */
            size_t u = a0 + 1, ue = u;
            while (ue < rlen && resp[ue] != '\0') ue++;       /* authcid */
            size_t pw = ue + 1;
            if (u <= rlen) copy_str(user, sizeof user, resp + u,
                                    (ue > u ? ue - u : 0));
            if (pw <= rlen) copy_str(pass, sizeof pass, resp + pw,
                                     (rlen > pw ? rlen - pw : 0));
        }
        struct authstore *as = p->conn->server->authstore;
        if (as) {
            if (authstore_is_open(as)) {
                /* Fresh, unconfigured broker: refuse EVERY login (clients like
                 * pika silently try guest/guest - accepting them would look
                 * like "no auth"). The operator must create the first admin. */
                LOG_WARN("conn #%" PRIu64 ": login refused - no users configured "
                         "(create one: beavermq add-user <name> <password>)",
                         p->conn->id);
                send_connection_close(p, 403,
                    "ACCESS_REFUSED - no users configured; create the first "
                    "admin with 'beavermq add-user'");
                return;
            }
            /* Rate-limit password hashing (see authlimit): key on the client
             * IP only (strip the ":port" from conn->peer) so all attempts from
             * one address share a backoff. */
            char ip[64];
            copy_str(ip, sizeof ip, p->conn->peer, strlen(p->conn->peer));
            char *last_colon = strrchr(ip, ':');
            if (last_colon) *last_colon = '\0';
            uint64_t now_ms = uv_now(p->conn->handle.loop);

            if (!user[0]) {
                authlimit_record_failure(ip, now_ms);
                LOG_WARN("conn #%" PRIu64 ": auth FAILED (empty user)", p->conn->id);
                send_connection_close(p, 403, "ACCESS_REFUSED - login refused");
                return;
            }
            if (authlimit_retry_after_ms(ip, now_ms) > 0 ||
                authlimit_hash_begin() != 0) {
                LOG_WARN("conn #%" PRIu64 ": auth throttled for '%s' from %s",
                         p->conn->id, user, ip);
                send_connection_close(p, 403,
                    "ACCESS_REFUSED - too many attempts; retry later");
                return;
            }
            int verified = authstore_verify(as, user, pass);
            authlimit_hash_end();
            if (!verified) {
                authlimit_record_failure(ip, now_ms);
                LOG_WARN("conn #%" PRIu64 ": auth FAILED for user '%s'",
                         p->conn->id, user);
                send_connection_close(p, 403, "ACCESS_REFUSED - login refused");
                return;
            }
            authlimit_record_success(ip);
            p->user_tags = authstore_user_tags(as, user);
        }
        copy_str(p->user, sizeof p->user, user, strlen(user));
        LOG_INFO("conn #%" PRIu64 ": authenticated as '%s'", p->conn->id, p->user);

        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_u16(&a, AMQP_CHANNEL_MAX);        /* channel-max */
        bmqp_buf_put_u32(&a, AMQP_DEFAULT_FRAME_MAX);  /* frame-max   */
        bmqp_buf_put_u16(&a, AMQP_HEARTBEAT_SECONDS);  /* heartbeat suggestion */
        send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_TUNE, &a);
        bmqp_buf_free(&a);
        break;
    }
    case BMQP_CONNECTION_TUNE_OK: {
        p->channel_max = bmqp_read_u16(r);
        p->frame_max   = bmqp_read_u32(r);
        p->heartbeat   = bmqp_read_u16(r);
        if (r->error) {
            proto_fatal(p, "malformed Connection.TuneOk");
            return;
        }
        /* Spec: the client must pick a frame-max no larger than what we
         * offered (0 = "use yours"); clamp rather than trust the wire. */
        if (p->frame_max == 0 || p->frame_max > AMQP_DEFAULT_FRAME_MAX)
            p->frame_max = AMQP_DEFAULT_FRAME_MAX;
        p->conn->frame_max = p->frame_max;
        /* The TuneOk heartbeat is the negotiated value; 0 disables. Arm the
         * sender/dead-peer timer accordingly - either way, the handshake
         * completed, so the handshake-timeout deadline must not still fire. */
        if (p->heartbeat)
            beaver_conn_enable_heartbeat(p->conn, p->heartbeat);
        else
            beaver_conn_clear_handshake_timeout(p->conn);
        LOG_INFO("conn #%" PRIu64 ": Connection.TuneOk "
                 "(channel_max=%u frame_max=%u heartbeat=%u)",
                 p->conn->id, p->channel_max, p->frame_max, p->heartbeat);
        break;
    }
    case BMQP_CONNECTION_OPEN: {
        size_t vhlen, n;
        const char *vh = bmqp_read_shortstr(r, &vhlen); /* virtual-host */
        bmqp_read_shortstr(r, &n);                  /* reserved-1 (capabilities) */
        bmqp_read_u8(r);                            /* reserved-2 (insist bit) */
        if (r->error) {
            proto_fatal(p, "malformed Connection.Open");
            return;
        }
        copy_str(p->vhost, sizeof(p->vhost), vh, vhlen);
        if (!name_ok(p->vhost)) {
            send_connection_close(p, 530, "NOT_ALLOWED - illegal vhost name");
            return;
        }
        struct authstore *as = p->conn->server->authstore;
        if (as) {
            if (!authstore_vhost_exists(as, p->vhost)) {
                send_connection_close(p, 530,
                    "NOT_ALLOWED - vhost not found");
                return;
            }
            if (!authstore_can_access_vhost(as, p->user, p->vhost)) {
                LOG_WARN("conn #%" PRIu64 ": user '%s' denied vhost '%s'",
                         p->conn->id, p->user, p->vhost);
                send_connection_close(p, 530,
                    "NOT_ALLOWED - access to vhost refused");
                return;
            }
        }
        p->connection_open = 1;
        LOG_INFO("conn #%" PRIu64 ": Connection.Open vhost='%s' user='%s' -> ready",
                 p->conn->id, p->vhost, p->user);

        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_shortstr(&a, "");              /* reserved-1 (known-hosts) */
        send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_OPEN_OK, &a);
        bmqp_buf_free(&a);
        break;
    }
    case BMQP_CONNECTION_CLOSE: {
        uint16_t code = bmqp_read_u16(r);   /* reply-code */
        size_t n;
        bmqp_read_shortstr(r, &n);          /* reply-text */
        bmqp_read_u16(r);                   /* class-id */
        bmqp_read_u16(r);                   /* method-id */
        LOG_INFO("conn #%" PRIu64 ": Connection.Close requested by peer "
                 "(code=%u)", p->conn->id, code);
        send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_CLOSE_OK, NULL);
        proto_close(p);
        break;
    }
    case BMQP_CONNECTION_CLOSE_OK:
        proto_close(p);
        break;
    default:
        proto_fatal(p, "unexpected connection method %u", method);
    }
}

/* ========================================================================= */
/* channel class                                                              */
/* ========================================================================= */

static void handle_channel(beaver_proto_t *p, uint16_t channel,
                           uint16_t method, bmqp_reader_t *r)
{
    if (!require_open(p))
        return;

    switch (method) {
    case BMQP_CHANNEL_OPEN: {
        size_t n;
        bmqp_read_shortstr(r, &n);  /* reserved-1 */
        if (channel == 0) {
            proto_fatal(p, "Channel.Open on reserved channel 0");
            return;
        }
        if (channel > AMQP_CHANNEL_MAX) {
            proto_fatal(p, "Channel.Open: channel %u exceeds channel-max %u",
                        channel, AMQP_CHANNEL_MAX);
            return;
        }
        if (channel_is_open(p, channel)) {
            proto_fatal(p, "Channel.Open: channel %u already open", channel);
            return;
        }
        if (!channel_add(p, channel)) {
            proto_fatal(p, "out of memory tracking channel %u", channel);
            return;
        }
        proto_advance(p, BMQP_STATE_CHANNEL_OPEN);
        LOG_INFO("conn #%" PRIu64 ": Channel.Open ch=%u", p->conn->id, channel);

        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_longstr(&a, "", 0);   /* reserved-1 (long string) */
        send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_OPEN_OK, &a);
        bmqp_buf_free(&a);
        break;
    }
    case BMQP_CHANNEL_CLOSE: {
        uint16_t code = bmqp_read_u16(r);
        size_t n;
        bmqp_read_shortstr(r, &n);
        bmqp_read_u16(r);
        bmqp_read_u16(r);
        cancel_pending_ops(p, channel);
        dispatcher_remove_channel(p->conn->server->dispatcher, p->conn, channel);
        channel_remove(p, channel);
        LOG_INFO("conn #%" PRIu64 ": Channel.Close ch=%u (code=%u)",
                 p->conn->id, channel, code);
        send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_CLOSE_OK, NULL);
        break;
    }
    case BMQP_CHANNEL_CLOSE_OK:
        break;
    default:
        proto_fatal(p, "unexpected channel method %u", method);
    }
}

/* ========================================================================= */
/* exchange class                                                             */
/* ========================================================================= */

static void handle_exchange(beaver_proto_t *p, uint16_t channel,
                            uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_EXCHANGE_DECLARE: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t en, tn, tbl;
        const char *ex = bmqp_read_shortstr(r, &en);
        const char *ty = bmqp_read_shortstr(r, &tn);
        uint8_t bits   = bmqp_read_u8(r);       /* passive,durable,auto-del,internal,no-wait */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Exchange.Declare");
            return;
        }
        int no_wait = (bits & 0x10) != 0;
        char ename[256], etype[32];
        copy_str(ename, sizeof(ename), ex, en);
        copy_str(etype, sizeof(etype), ty, tn);
        if (!name_ok(ename)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal exchange name",
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_CONFIGURE, ename,
                          BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE))
            return;

        exchange_type_t xtype;
        if (exchange_type_from_name(etype, &xtype) != 0) {
            /* An unknown exchange type must be a channel exception, not a
             * silent direct-exchange substitution - the client believed it
             * declared (e.g.) a headers exchange and would route messages
             * accordingly, while the broker quietly created a direct one. */
            char text[300];
            snprintf(text, sizeof text,
                     "COMMAND_INVALID - unknown exchange type '%s'", etype);
            send_channel_close(p, channel, 503, text,
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }

        int created = 0;
        int drc = broker_declare_exchange(p->conn->server->broker, p->vhost,
                                          ename, xtype, bits & 0x0E, &created);
        if (drc == -2) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - exchange already "
                               "declared with different type/flags",
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }
        if (drc != 0) {
            proto_fatal(p, "failed to declare exchange '%s'", ename);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Exchange.Declare '%s' type=%s (%s)",
                 p->conn->id, channel, ename, exchange_type_name(xtype),
                 created ? "created" : "exists");
        /* Replicate only DURABLE topology: durable objects are clustered (HA +
         * survive restart); transient ones stay node-local and fast. Do NOT
         * send Declare-Ok until this actually COMMITS - the client must never
         * see success when there was no leader/quorum or the propose itself
         * failed (see await_cluster_commit). */
        if (p->conn->server->cluster && (bits & BMQP_FLAG_DURABLE)) {
            uint64_t seq = cluster_replicate_declare_exchange(
                p->conn->server->cluster, p->vhost, ename, (int)xtype, bits & 0x0E);
            if (no_wait)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Exchange.Declare", BMQP_CLASS_EXCHANGE,
                                   BMQP_EXCHANGE_DECLARE);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_EXCHANGE,
                                 BMQP_EXCHANGE_DECLARE,
                                 PENDING_EXCHANGE_DECLARE_OK, NULL, 0);
            break;
        }
        if (!no_wait)
            send_method(p, channel, BMQP_CLASS_EXCHANGE,
                        BMQP_EXCHANGE_DECLARE_OK, NULL);
        break;
    }
    default:
        proto_fatal(p, "unexpected exchange method %u", method);
    }
}

/* ========================================================================= */
/* queue class                                                                */
/* ========================================================================= */

static void handle_queue(beaver_proto_t *p, uint16_t channel,
                         uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_QUEUE_DECLARE: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        uint8_t bits  = bmqp_read_u8(r);        /* passive,durable,exclusive,auto-del,no-wait */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Queue.Declare");
            return;
        }
        int no_wait = (bits & 0x10) != 0;
        char qname[256];
        copy_str(qname, sizeof(qname), q, qn);
        if (qname[0] == '\0') /* server-generated name for anonymous queues */
            snprintf(qname, sizeof(qname), "amq.gen-%" PRIu64 "-%" PRIu64,
                     p->conn->id, ++p->consumer_seq);
        else if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_CONFIGURE, qname,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE))
            return;

        uint32_t depth = 0;
        int created = 0;
        /* passive/durable/exclusive/auto-delete bits map 1:1 to our flags. */
        int drc = broker_declare_queue(p->conn->server->broker, p->vhost, qname,
                                       bits & 0x0E, &depth, &created);
        if (drc == -2) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - queue already declared "
                               "with different flags",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
            return;
        }
        if (drc != 0) {
            proto_fatal(p, "failed to declare queue '%s'", qname);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Queue.Declare '%s' (%s, depth=%u)",
                 p->conn->id, channel, qname,
                 created ? "created" : "exists", depth);
        if (p->conn->server->cluster && (bits & BMQP_FLAG_DURABLE)) {
            uint64_t seq = cluster_replicate_declare_queue(
                p->conn->server->cluster, p->vhost, qname, bits & 0x0E);
            if (no_wait)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Queue.Declare", BMQP_CLASS_QUEUE,
                                   BMQP_QUEUE_DECLARE);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_QUEUE,
                                 BMQP_QUEUE_DECLARE, PENDING_QUEUE_DECLARE_OK,
                                 qname, depth);
            break;
        }

        if (!no_wait) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, qname, strlen(qname));
            bmqp_buf_put_u32(&a, depth);   /* message-count */
            bmqp_buf_put_u32(&a, 0);       /* consumer-count */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_QUEUE_BIND: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, en, kn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        const char *e = bmqp_read_shortstr(r, &en);
        const char *k = bmqp_read_shortstr(r, &kn);
        uint8_t no_wait = bmqp_read_u8(r);      /* no-wait bit */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Queue.Bind");
            return;
        }
        char qname[256], ename[256], key[256];
        copy_str(qname, sizeof(qname), q, qn);
        copy_str(ename, sizeof(ename), e, en);
        copy_str(key, sizeof(key), k, kn);
        if (!name_ok(qname) || !name_ok(ename)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue/exchange name",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND);
            return;
        }
        /* AMQP: binding needs WRITE on the exchange (source) + READ on the
         * queue (destination). */
        if (!require_perm(p, channel, AUTH_WRITE, ename,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND) ||
            !require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND))
            return;
        if (broker_bind(p->conn->server->broker, p->vhost, qname, ename, key) != 0) {
            proto_fatal(p, "Queue.Bind failed: queue '%s' or exchange '%s' "
                        "not found", qname, ename);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Queue.Bind q='%s' exchange='%s' "
                 "key='%s'", p->conn->id, channel, qname, ename, key);
        /* Replicate a bind only when its queue is durable (so the clustered
         * topology stays consistent with the durable queue/exchange set). */
        if (p->conn->server->cluster) {
            beaver_queue_t *bq = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
            int durable_q = bq && (queue_flags(bq) & BMQP_FLAG_DURABLE);
            uint64_t seq = 0;
            if (durable_q)
                seq = cluster_replicate_bind(p->conn->server->cluster, p->vhost,
                                             qname, ename, key);
            if (bq)
                queue_unref(bq);
            if (durable_q && !(no_wait & 0x01)) {
                if (seq == 0) {
                    send_channel_close(p, channel, 541,
                                       "INTERNAL_ERROR - failed to replicate "
                                       "Queue.Bind", BMQP_CLASS_QUEUE,
                                       BMQP_QUEUE_BIND);
                } else {
                    await_cluster_commit(p, channel, seq, BMQP_CLASS_QUEUE,
                                         BMQP_QUEUE_BIND, PENDING_QUEUE_BIND_OK,
                                         NULL, 0);
                }
                break;
            }
        }
        if (!(no_wait & 0x01))
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND_OK, NULL);
        break;
    }
    default:
        proto_fatal(p, "unexpected queue method %u", method);
    }
}

/* ========================================================================= */
/* basic class                                                                */
/* ========================================================================= */

static void handle_basic(beaver_proto_t *p, uint16_t channel,
                         uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_BASIC_QOS: {
        bmqp_read_u32(r);                        /* prefetch-size (unsupported) */
        uint16_t prefetch = bmqp_read_u16(r);    /* prefetch-count */
        bmqp_read_u8(r);                         /* global bit (per-channel here) */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Qos");
            return;
        }
        /* Remember the count on the channel (applies to consumers registered
         * later) and update every consumer already on this channel. */
        proto_chan_t *ch = channel_find(p, channel);
        if (ch)
            ch->prefetch = prefetch;
        dispatcher_set_prefetch(p->conn->server->dispatcher, p->conn, channel,
                                prefetch);
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Qos prefetch=%u",
                 p->conn->id, channel, prefetch);
        send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_QOS_OK, NULL);
        break;
    }
    case BMQP_BASIC_PUBLISH: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t en, kn;
        const char *e = bmqp_read_shortstr(r, &en);
        const char *k = bmqp_read_shortstr(r, &kn);
        bmqp_read_u8(r);                        /* mandatory, immediate bits */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Publish");
            return;
        }
        /* Begin content assembly; the content header + body frames follow. */
        publish_reset(p);
        p->pub_active  = 1;
        p->pub_channel = channel;
        copy_str(p->pub_exchange, sizeof(p->pub_exchange), e, en);
        copy_str(p->pub_routing_key, sizeof(p->pub_routing_key), k, kn);
        if (!name_ok(p->pub_exchange)) {
            proto_fatal(p, "illegal exchange name in Basic.Publish");
            return;
        }
        break;
    }
    case BMQP_BASIC_CONSUME: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, tn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        const char *t = bmqp_read_shortstr(r, &tn);
        uint8_t bits  = bmqp_read_u8(r);        /* no-local,no-ack,exclusive,no-wait */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Consume");
            return;
        }
        int no_ack  = (bits & 0x02) != 0;
        int no_wait = (bits & 0x08) != 0;
        char qname[256], ctag[128];
        copy_str(qname, sizeof(qname), q, qn);
        copy_str(ctag, sizeof(ctag), t, tn);
        if (ctag[0] == '\0')
            snprintf(ctag, sizeof(ctag), "ctag-%" PRIu64 "-%" PRIu64,
                     p->conn->id, ++p->consumer_seq);
        if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME))
            return;

        proto_chan_t *ch = channel_find(p, channel);
        if (dispatcher_add_consumer(p->conn->server->dispatcher, p->conn,
                                    channel, ctag, p->vhost, qname, no_ack,
                                    ch ? ch->prefetch : 0) != 0) {
            proto_fatal(p, "Basic.Consume: queue '%s' does not exist", qname);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Consume queue='%s' tag='%s' "
                 "(%s)", p->conn->id, channel, qname, ctag,
                 no_ack ? "no-ack" : "manual-ack");

        if (!no_wait) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, ctag, strlen(ctag));
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_BASIC_CANCEL: {
        size_t tn;
        const char *t = bmqp_read_shortstr(r, &tn);
        uint8_t no_wait = bmqp_read_u8(r);
        if (r->error) {
            proto_fatal(p, "malformed Basic.Cancel");
            return;
        }
        char ctag[128];
        copy_str(ctag, sizeof(ctag), t, tn);
        dispatcher_cancel(p->conn->server->dispatcher, p->conn, channel, ctag);
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Cancel tag='%s'",
                 p->conn->id, channel, ctag);
        if (!(no_wait & 0x01)) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, ctag, strlen(ctag));
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_CANCEL_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_BASIC_GET: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn;
        const char *q = bmqp_read_shortstr(r, &qn);
        uint8_t no_ack = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Get");
            return;
        }
        char qname[256];
        copy_str(qname, sizeof(qname), q, qn);
        if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_BASIC, BMQP_BASIC_GET);
            return;
        }
        if (!require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_BASIC, BMQP_BASIC_GET))
            return;

        beaver_queue_t *queue = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
        beaver_message_t *msg = queue ? queue_dequeue(queue) : NULL;
        if (!msg) {
            if (queue)
                queue_unref(queue);
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr(&a, "");      /* reserved-1 */
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_GET_EMPTY, &a);
            bmqp_buf_free(&a);
            break;
        }
        /* Basic.GetOk: delivery-tag, redelivered, exchange, routing-key,
         * message-count; followed by content header + body. */
        uint64_t tag = ++p->consumer_seq;
        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_u64(&a, tag);                 /* delivery-tag */
        bmqp_buf_put_u8(&a, 0);                    /* redelivered */
        bmqp_buf_put_shortstr(&a, msg->exchange);
        bmqp_buf_put_shortstr(&a, msg->routing_key);
        bmqp_buf_put_u32(&a, (uint32_t)queue_depth(queue)); /* message-count */
        send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_GET_OK, &a);
        bmqp_buf_free(&a);
        protocol_send_content(p->conn, channel, BMQP_CLASS_BASIC, msg->body,
                              msg->body_len, msg->props, msg->props_len,
                              p->conn->frame_max);
        if (no_ack) {
            /* Pull auto-acks: advance + replicate the consume watermark so
             * replica copies on the other nodes drain and the log can
             * compact past it. */
            dispatcher_note_pull(p->conn->server->dispatcher, queue, msg->cluster_id);
            message_unref(msg);
            queue_unref(queue);
        } else {
            /* Manual ack: keep both refs until Basic.Ack/Reject/Nack (or the
             * channel/connection closes, which requeues it) settles it -
             * msg/queue are NOT unref'd here; chan_get_unacked_settle or
             * chan_release_get_unacked does that later. */
            proto_chan_t *pc = channel_find(p, channel);
            if (!pc || !chan_get_unacked_add(pc, tag, msg, queue)) {
                /* OOM tracking the delivery: it's already on the wire, so we
                 * cannot un-send it - fail closed by tearing down the
                 * connection rather than silently losing manual-ack
                 * bookkeeping for a message the client thinks it must ack. */
                message_unref(msg);
                queue_unref(queue);
                proto_fatal(p, "out of memory tracking Basic.Get delivery");
                return;
            }
        }
        break;
    }
    case BMQP_BASIC_ACK: {
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t multiple = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Ack");
            return;
        }
        /* Basic.Get and Basic.Consume deliveries use separate delivery-tag
         * counters, so a "multiple" ack spanning both sources must settle
         * both; for a single tag, only try the dispatcher if it wasn't a
         * tracked Basic.Get delivery (avoids a spurious "unknown tag" log). */
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, multiple, 0) : 0;
        if (multiple || got == 0)
            dispatcher_ack(p->conn->server->dispatcher, p->conn, channel,
                          delivery_tag, multiple);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Ack delivery_tag=%" PRIu64
                  " multiple=%u", p->conn->id, channel, delivery_tag, multiple);
        break;
    }
    case BMQP_BASIC_REJECT: {
        /* delivery-tag (u64), requeue bit. Single-delivery negative ack. */
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t requeue = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Reject");
            return;
        }
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, 0, requeue) : 0;
        if (got == 0)
            dispatcher_nack(p->conn->server->dispatcher, p->conn, channel,
                           delivery_tag, 0 /* multiple */, requeue);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Reject delivery_tag=%" PRIu64
                  " requeue=%u", p->conn->id, channel, delivery_tag, requeue);
        break;
    }
    case BMQP_BASIC_NACK: {
        /* delivery-tag (u64), bits: 0x01 multiple, 0x02 requeue (RabbitMQ
         * extension; the batch-capable Basic.Reject). */
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t bits = bmqp_read_u8(r);
        if (r->error) {
            proto_fatal(p, "malformed Basic.Nack");
            return;
        }
        int multiple = (bits & 0x01) != 0;
        int requeue2 = (bits & 0x02) != 0;
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, multiple, requeue2) : 0;
        if (multiple || got == 0)
            dispatcher_nack(p->conn->server->dispatcher, p->conn, channel,
                           delivery_tag, multiple, requeue2);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Nack delivery_tag=%" PRIu64
                  " bits=0x%x", p->conn->id, channel, delivery_tag, bits);
        break;
    }
    default:
        proto_fatal(p, "unexpected basic method %u", method);
    }
}

/* ========================================================================= */
/* content frames (the body of a Basic.Publish)                               */
/* ========================================================================= */

static void handle_content_header(beaver_proto_t *p,
                                  const bmqp_frame_header_t *hdr,
                                  const uint8_t *payload)
{
    if (hdr->channel != p->pub_channel) {
        proto_fatal(p, "content header on channel %u, expected %u",
                    hdr->channel, p->pub_channel);
        return;
    }

    bmqp_reader_t r;
    bmqp_reader_init(&r, payload, hdr->payload_len);
    bmqp_read_u16(&r);                       /* class-id (60) */
    bmqp_read_u16(&r);                       /* weight */
    uint64_t body_size = bmqp_read_u64(&r);  /* total body size */
    if (r.error) {
        proto_fatal(p, "malformed content header");
        return;
    }
    uint64_t body_limit = p->conn->server->max_message_size
                          ? p->conn->server->max_message_size
                          : AMQP_MAX_BODY_SIZE;
    if (body_size > body_limit) {
        proto_fatal(p, "message body size %" PRIu64 " exceeds max_message_size "
                    "%" PRIu64, body_size, body_limit);
        return;
    }

    /* The remainder is the property section (flags + list); keep it verbatim. */
    size_t prop_len = hdr->payload_len - r.pos;
    if (prop_len > 0) {
        p->pub_props = malloc(prop_len);
        if (!p->pub_props) {
            proto_fatal(p, "out of memory storing content properties");
            return;
        }
        memcpy(p->pub_props, payload + r.pos, prop_len);
        p->pub_props_len = prop_len;
    }

    p->pub_body_size     = body_size;
    p->pub_body_received = 0;
    p->pub_have_header   = 1;
    /* pub_body is allocated lazily, and only when the body spans multiple
     * frames (see handle_content_body). The common single-frame case is built
     * straight from the read buffer with no intermediate copy. */
    if (body_size == 0)
        finalize_publish(p, NULL, 0); /* zero-length body: complete immediately */
}

static void handle_content_body(beaver_proto_t *p,
                                const bmqp_frame_header_t *hdr,
                                const uint8_t *payload)
{
    if (hdr->channel != p->pub_channel) {
        proto_fatal(p, "content body on channel %u, expected %u",
                    hdr->channel, p->pub_channel);
        return;
    }
    size_t n = hdr->payload_len;
    if (p->pub_body_received + n > p->pub_body_size) {
        proto_fatal(p, "content body exceeds declared size");
        return;
    }

    /* Fast path: the whole body arrived in this single frame. Build the message
     * directly from the read buffer - no intermediate accumulation copy. */
    if (!p->pub_body && p->pub_body_received == 0 && n == p->pub_body_size) {
        finalize_publish(p, payload, n);
        return;
    }

    /* Slow path: body spans multiple frames - accumulate into pub_body. */
    if (!p->pub_body) {
        p->pub_body = malloc(p->pub_body_size);
        if (!p->pub_body) {
            proto_fatal(p, "out of memory allocating %" PRIu64 "-byte body",
                        p->pub_body_size);
            return;
        }
    }
    if (n)
        memcpy(p->pub_body + p->pub_body_received, payload, n);
    p->pub_body_received += n;
    if (p->pub_body_received == p->pub_body_size)
        finalize_publish(p, p->pub_body, p->pub_body_size);
}

/* Is this AMQP basic-properties section marked persistent (delivery-mode = 2)?
 * Parses only as far as the delivery-mode property (the 4th basic property). */
static int props_is_persistent(const uint8_t *props, size_t len)
{
    if (!props || len < 2)
        return 0;
    bmqp_reader_t r;
    bmqp_reader_init(&r, props, len);
    uint16_t flags = bmqp_read_u16(&r);
    if (!(flags & 0x1000))                            /* no delivery-mode prop */
        return 0;
    size_t n;
    if (flags & 0x8000) bmqp_read_shortstr(&r, &n);   /* content-type     */
    if (flags & 0x4000) bmqp_read_shortstr(&r, &n);   /* content-encoding */
    if (flags & 0x2000) bmqp_read_longstr(&r, &n);    /* headers (table)  */
    uint8_t dm = bmqp_read_u8(&r);
    return !r.error && dm == 2;
}

static void finalize_publish(beaver_proto_t *p, const uint8_t *body,
                             size_t body_len)
{
    beaver_server_t *srv = p->conn->server;

    /* WRITE permission on the target exchange (default exchange "" included). */
    if (!perm_ok(p, AUTH_WRITE, p->pub_exchange)) {
        char text[600];
        snprintf(text, sizeof text,
                 "ACCESS_REFUSED - write access to exchange '%s' in vhost '%s' "
                 "refused for user '%s'", p->pub_exchange, p->vhost, p->user);
        LOG_WARN("conn #%" PRIu64 ": %s", p->conn->id, text);
        send_channel_close(p, p->pub_channel, 403, text,
                           BMQP_CLASS_BASIC, BMQP_BASIC_PUBLISH);
        publish_reset(p);
        return;
    }

    /* Persistent messages are replicated through the cluster (forwarded to the
     * leader, committed on a majority, then applied/enqueued on EVERY node).
     * We must NOT also route locally, or the origin node would enqueue twice. */
    int replicated = 0;
    if (srv->cluster && props_is_persistent(p->pub_props, p->pub_props_len)) {
        replicated = cluster_replicate_publish(srv->cluster, p->vhost,
                         p->pub_exchange, p->pub_routing_key, body, body_len,
                         p->pub_props, p->pub_props_len) == 0;
        /* Flow control: if the cluster is congested, pause this producer's reads
         * (TCP backpressure) so it can't outrun durable replication. */
        if (replicated && cluster_should_throttle(srv->cluster))
            beaver_conn_throttle_read(p->conn);
        if (!replicated) {
            /* Proposals are buffered even without a live leader, so this only
             * fails on OOM / oversized names. NEVER route a persistent message
             * locally instead - the replicas would silently diverge from the
             * origin node. Tell the client so it can retry. */
            send_channel_close(p, p->pub_channel, 506,
                               "RESOURCE_ERROR - cannot replicate publish; retry",
                               BMQP_CLASS_BASIC, BMQP_BASIC_PUBLISH);
            publish_reset(p);
            return;
        }
    }

    if (!replicated) {
        beaver_message_t *msg =
            message_new_full(p->pub_exchange, p->pub_routing_key, body,
                             body_len, p->pub_props, p->pub_props_len);
        if (!msg) {
            proto_fatal(p, "out of memory building published message");
            return;
        }
        broker_route(srv->broker, p->vhost, msg);
        message_unref(msg);
    }
    proto_advance(p, BMQP_STATE_ACTIVE);
    /* Hot path: keep at DEBUG so high-throughput publishing isn't throttled by
     * synchronous logging (the LOG_DEBUG macro is a no-op when filtered). */
    LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Publish exchange='%s' key='%s' "
              "body=%" PRIu64 " bytes -> %s",
              p->conn->id, p->pub_channel, p->pub_exchange, p->pub_routing_key,
              p->pub_body_size, replicated ? "replicated" : "routed");
    publish_reset(p);
}

/* ========================================================================= */
/* dispatch                                                                   */
/* ========================================================================= */

static void handle_method(beaver_proto_t *p, uint16_t channel,
                          const uint8_t *payload, size_t plen)
{
    bmqp_reader_t r;
    bmqp_reader_init(&r, payload, plen);
    uint16_t class_id  = bmqp_read_u16(&r);
    uint16_t method_id = bmqp_read_u16(&r);
    if (r.error) {
        proto_fatal(p, "truncated method header");
        return;
    }

    switch (class_id) {
    case BMQP_CLASS_CONNECTION: handle_connection(p, channel, method_id, &r); break;
    case BMQP_CLASS_CHANNEL:    handle_channel(p, channel, method_id, &r);    break;
    case BMQP_CLASS_EXCHANGE:   handle_exchange(p, channel, method_id, &r);   break;
    case BMQP_CLASS_QUEUE:      handle_queue(p, channel, method_id, &r);      break;
    case BMQP_CLASS_BASIC:      handle_basic(p, channel, method_id, &r);      break;
    default:
        proto_fatal(p, "unknown method class %u", class_id);
    }
}

static void dispatch_frame(beaver_proto_t *p, const bmqp_frame_header_t *hdr,
                           const uint8_t *payload)
{
    /* While assembling a publish, only its content frames are valid. */
    if (p->pub_active) {
        if (!p->pub_have_header) {
            if (hdr->type != BMQP_FRAME_HEADER) {
                proto_fatal(p, "expected content header, got frame type %u",
                            hdr->type);
                return;
            }
            handle_content_header(p, hdr, payload);
        } else {
            if (hdr->type != BMQP_FRAME_BODY) {
                proto_fatal(p, "expected content body, got frame type %u",
                            hdr->type);
                return;
            }
            handle_content_body(p, hdr, payload);
        }
        return;
    }

    switch (hdr->type) {
    case BMQP_FRAME_METHOD:
        handle_method(p, hdr->channel, payload, hdr->payload_len);
        break;
    case BMQP_FRAME_HEARTBEAT:
        LOG_DEBUG("conn #%" PRIu64 ": heartbeat frame", p->conn->id);
        break;
    case BMQP_FRAME_HEADER:
    case BMQP_FRAME_BODY:
        proto_fatal(p, "unexpected content frame (type %u) outside a publish",
                    hdr->type);
        break;
    default:
        proto_fatal(p, "unsupported frame type %u", hdr->type);
    }
}

/* ========================================================================= */
/* public API                                                                 */
/* ========================================================================= */

beaver_proto_t *protocol_conn_new(beaver_conn_t *conn)
{
    beaver_proto_t *p = calloc(1, sizeof(*p));
    if (!p)
        return NULL;
    p->conn  = conn;
    p->state = BMQP_STATE_CONNECTED;
    conn->frame_max = AMQP_DEFAULT_FRAME_MAX;
    return p;
}

void protocol_conn_free(beaver_proto_t *p)
{
    if (!p)
        return;
    cancel_pending_ops(p, -1);
    for (size_t i = 0; i < p->n_channels; i++)
        chan_release_get_unacked(&p->channels[i]);
    free(p->inbuf);
    free(p->channels);
    free(p->pub_body);
    free(p->pub_props);
    free(p);
}

void protocol_on_data(beaver_proto_t *p, const uint8_t *data, size_t len)
{
    if (p->state == BMQP_STATE_CLOSING)
        return;

    if (!inbuf_append(p, data, len)) {
        proto_fatal(p, "out of memory buffering %zu input bytes", len);
        return;
    }

    /* Step 1: consume the 8-byte AMQP protocol header exactly once. */
    if (!p->header_received) {
        if (p->inbuf_len - p->inbuf_pos < AMQP_PROTOCOL_HEADER_SIZE)
            return; /* wait for the full header */
        if (memcmp(p->inbuf + p->inbuf_pos, AMQP_PROTOCOL_HEADER,
                   AMQP_PROTOCOL_HEADER_SIZE) != 0) {
            /* Reply with the protocol version we speak, then close. */
            beaver_conn_send(p->conn, AMQP_PROTOCOL_HEADER,
                             AMQP_PROTOCOL_HEADER_SIZE);
            proto_fatal(p, "unsupported protocol header (not AMQP 0-9-1)");
            return;
        }
        p->header_received = 1;
        p->inbuf_pos += AMQP_PROTOCOL_HEADER_SIZE;
        proto_advance(p, BMQP_STATE_HANDSHAKE);
        LOG_INFO("conn #%" PRIu64 ": AMQP 0-9-1 handshake started", p->conn->id);
        send_connection_start(p);
    }

    /* Step 2: dispatch every complete frame buffered so far. The cursor
     * (inbuf_pos) advances without shifting memory; the consumed prefix is
     * reclaimed on the next inbuf_append. */
    for (;;) {
        if (p->state == BMQP_STATE_CLOSING)
            return;

        bmqp_frame_header_t hdr;
        const uint8_t *payload = NULL;
        ssize_t n = bmqp_frame_parse(p->inbuf + p->inbuf_pos,
                                     p->inbuf_len - p->inbuf_pos, &hdr, &payload);
        if (n == 0)
            break; /* incomplete; wait for more bytes */
        if (n < 0) {
            proto_fatal(p, "framing error (bad length or missing 0x%02X end)",
                        BMQP_FRAME_END);
            return;
        }
        /* bmqp_frame_parse() only enforces the global BMQP_MAX_FRAME_PAYLOAD
         * cap - not the per-connection frame_max WE negotiated with this
         * client in Connection.Tune/TuneOk (0 before that negotiation
         * completes, hence the guard). A client that agreed to a smaller
         * frame_max but then sends a larger frame anyway is violating the
         * negotiated contract, not just sending a large-but-legal frame. */
        if (p->frame_max > 0 && hdr.payload_len > p->frame_max) {
            proto_fatal(p, "frame of %u bytes exceeds negotiated frame_max=%u",
                       hdr.payload_len, p->frame_max);
            return;
        }
        dispatch_frame(p, &hdr, payload);
        p->inbuf_pos += (size_t)n;
    }
}
