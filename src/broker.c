/*
 * broker.c - Registry + routing core implementation.
 *
 * See broker.h for the locking hierarchy. All registry mutations and lookups
 * take b->lock; queue contents are guarded by each queue's own lock.
 */
#include "broker.h"
#include "hashmap.h"
#include "message.h"
#include "logger.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct beaver_broker {
    /* Read-write lock over both registries + bindings: publishing/routing takes
     * the READ lock (so all workers route concurrently), while declares and
     * binds take the WRITE lock (rare). */
    pthread_rwlock_t lock;
    hashmap_t      *queues;     /* name -> beaver_queue_t*  (owns a ref) */
    hashmap_t      *exchanges;  /* name -> beaver_exchange_t* (owned)    */

    _Atomic uint64_t next_msg_id;
    _Atomic uint64_t messages_published;
};

/* hashmap value-free adapters (hashmap_free wants void(*)(void*)). */
static void free_queue_value(void *v)    { queue_unref((beaver_queue_t *)v); }
static void free_exchange_value(void *v) { exchange_free((beaver_exchange_t *)v); }

beaver_broker_t *broker_new(void)
{
    beaver_broker_t *b = calloc(1, sizeof(*b));
    if (!b)
        return NULL;
    if (pthread_rwlock_init(&b->lock, NULL) != 0) {
        free(b);
        return NULL;
    }
    b->queues    = hashmap_new();
    b->exchanges = hashmap_new();
    if (!b->queues || !b->exchanges) {
        hashmap_free(b->queues, free_queue_value);
        hashmap_free(b->exchanges, free_exchange_value);
        pthread_rwlock_destroy(&b->lock);
        free(b);
        return NULL;
    }
    atomic_init(&b->next_msg_id, 1);
    return b;
}

void broker_free(beaver_broker_t *b)
{
    if (!b)
        return;
    /* Free exchanges first: each binding drops a queue reference, so queues
     * are released cleanly afterwards. */
    hashmap_free(b->exchanges, free_exchange_value);
    hashmap_free(b->queues, free_queue_value);
    pthread_rwlock_destroy(&b->lock);
    free(b);
}

int broker_declare_queue(beaver_broker_t *b, const char *vhost,
                         const char *name, uint8_t flags,
                         uint32_t *out_depth, int *out_created)
{
    char key[512];
    broker_vkey(key, vhost, name);
    int rc = 0;
    pthread_rwlock_wrlock(&b->lock);

    beaver_queue_t *q = hashmap_get(b->queues, key);
    int created = 0;
    if (!q) {
        q = queue_new(name, flags);
        if (!q) {
            rc = -1;
            goto done;
        }
        queue_set_vhost(q, vhost);
        if (hashmap_put(b->queues, key, q, NULL) != 0) {
            queue_unref(q);
            rc = -1;
            goto done;
        }
        created = 1;
    }

    if (out_created)
        *out_created = created;
    if (out_depth)
        *out_depth = (uint32_t)queue_depth(q); /* broker -> queue lock order */

done:
    pthread_rwlock_unlock(&b->lock);
    return rc;
}

int broker_declare_exchange(beaver_broker_t *b, const char *vhost,
                            const char *name, exchange_type_t type,
                            uint8_t flags, int *out_created)
{
    char key[512];
    broker_vkey(key, vhost, name);
    int rc = 0;
    pthread_rwlock_wrlock(&b->lock);

    beaver_exchange_t *ex = hashmap_get(b->exchanges, key);
    int created = 0;
    if (!ex) {
        ex = exchange_new(name, type, flags);
        if (!ex) {
            rc = -1;
            goto done;
        }
        exchange_set_vhost(ex, vhost);
        if (hashmap_put(b->exchanges, key, ex, NULL) != 0) {
            exchange_free(ex);
            rc = -1;
            goto done;
        }
        created = 1;
    }
    if (out_created)
        *out_created = created;

done:
    pthread_rwlock_unlock(&b->lock);
    return rc;
}

void broker_declare_default_exchanges(beaver_broker_t *b, const char *vhost)
{
    broker_declare_exchange(b, vhost, "amq.direct", EXCHANGE_DIRECT, 0, NULL);
    broker_declare_exchange(b, vhost, "amq.fanout", EXCHANGE_FANOUT, 0, NULL);
    broker_declare_exchange(b, vhost, "amq.topic",  EXCHANGE_TOPIC,  0, NULL);
}

int broker_bind(beaver_broker_t *b, const char *vhost, const char *queue,
                const char *exchange, const char *routing_key)
{
    char qkey[512], ekey[512];
    broker_vkey(qkey, vhost, queue);
    broker_vkey(ekey, vhost, exchange);
    int rc;
    pthread_rwlock_wrlock(&b->lock);

    beaver_exchange_t *ex = hashmap_get(b->exchanges, ekey);
    beaver_queue_t    *q  = hashmap_get(b->queues, qkey);
    if (!ex || !q) {
        LOG_WARN("bind failed (vhost '%s'): %s%s%s not found", vhost,
                 ex ? "" : "exchange ",
                 (!ex && !q) ? "and " : "",
                 q ? "" : "queue");
        rc = -1;
    } else {
        rc = exchange_bind(ex, routing_key, q);
    }

    pthread_rwlock_unlock(&b->lock);
    return rc;
}

int broker_route(beaver_broker_t *b, const char *vhost, beaver_message_t *msg)
{
    msg->id = atomic_fetch_add_explicit(&b->next_msg_id, 1, memory_order_relaxed);
    const char *exchange    = msg->exchange ? msg->exchange : "";
    const char *routing_key = msg->routing_key ? msg->routing_key : "";
    char key[512];

    /* Phase 1: collect target queue references under the registry lock. */
    beaver_queue_t **targets = NULL;
    size_t ntargets = 0;

    pthread_rwlock_rdlock(&b->lock);
    atomic_fetch_add_explicit(&b->messages_published, 1, memory_order_relaxed);
    if (exchange[0] == '\0') {
        /* Default exchange: route directly to the queue named routing_key. */
        broker_vkey(key, vhost, routing_key);
        beaver_queue_t *q = hashmap_get(b->queues, key);
        if (q) {
            targets = malloc(sizeof(*targets));
            if (targets) {
                targets[0] = queue_ref(q);
                ntargets   = 1;
            }
        }
    } else {
        broker_vkey(key, vhost, exchange);
        beaver_exchange_t *ex = hashmap_get(b->exchanges, key);
        if (ex)
            ntargets = exchange_route(ex, routing_key, &targets);
    }
    pthread_rwlock_unlock(&b->lock);

    /* Phase 2: enqueue WITHOUT the registry lock; queue refs keep them alive.
     * Notify the route callback (dispatcher) per queue so it can push to any
     * waiting consumers. */
    int routed = 0;
    for (size_t i = 0; i < ntargets; i++) {
        if (queue_enqueue(targets[i], msg) == 0) {
            routed++;
            /* Wake any worker dispatchers with consumers on this queue (this
             * runs on the publishing thread; the waiters fire thread-safe
             * uv_async_sends to their own loops). */
            queue_wake_waiters(targets[i]);
        }
        queue_unref(targets[i]);
    }
    free(targets);
    return routed;
}

int broker_publish(beaver_broker_t *b, const char *vhost, const char *exchange,
                   const char *routing_key, const void *body, size_t body_len)
{
    beaver_message_t *msg = message_new(exchange ? exchange : "",
                                        routing_key ? routing_key : "",
                                        body, body_len);
    if (!msg)
        return -1;
    int routed = broker_route(b, vhost, msg);
    message_unref(msg); /* drop our creating reference */
    return routed;
}

beaver_queue_t *broker_get_queue(beaver_broker_t *b, const char *vhost,
                                 const char *name)
{
    char key[512];
    broker_vkey(key, vhost, name);
    pthread_rwlock_rdlock(&b->lock);
    beaver_queue_t *q = hashmap_get(b->queues, key);
    if (q)
        queue_ref(q);
    pthread_rwlock_unlock(&b->lock);
    return q;
}

/* ---- stats / iteration --------------------------------------------------- */

typedef struct {
    uint64_t ready;
    uint64_t enqueued;
    uint64_t dequeued;
} queue_acc_t;

static int sum_queue_cb(const char *key, void *value, void *ctx)
{
    (void)key;
    beaver_queue_t *q = value;
    queue_acc_t *a = ctx;
    a->ready    += queue_depth(q);
    a->enqueued += queue_total_enqueued(q);
    a->dequeued += queue_total_dequeued(q);
    return 0;
}

void broker_stats(beaver_broker_t *b, broker_stats_t *out)
{
    queue_acc_t a = {0, 0, 0};
    pthread_rwlock_rdlock(&b->lock);
    out->queue_count        = hashmap_count(b->queues);
    out->exchange_count     = hashmap_count(b->exchanges);
    out->messages_published = atomic_load_explicit(&b->messages_published, memory_order_relaxed);
    hashmap_iter(b->queues, sum_queue_cb, &a);
    pthread_rwlock_unlock(&b->lock);
    out->messages_ready  = a.ready;
    out->total_enqueued  = a.enqueued;
    out->total_dequeued  = a.dequeued;
}

typedef struct {
    broker_queue_visit_fn fn;
    void                 *ctx;
} visit_ctx_t;

static int visit_queue_cb(const char *key, void *value, void *ctx)
{
    (void)key;
    visit_ctx_t *v = ctx;
    return v->fn((beaver_queue_t *)value, v->ctx);
}

void broker_foreach_queue(beaver_broker_t *b, broker_queue_visit_fn fn,
                          void *ctx)
{
    visit_ctx_t v = {fn, ctx};
    pthread_rwlock_rdlock(&b->lock);
    hashmap_iter(b->queues, visit_queue_cb, &v);
    pthread_rwlock_unlock(&b->lock);
}

static int min_live_cb(beaver_queue_t *q, void *ctx)
{
    uint64_t *m = ctx;
    uint64_t v = queue_oldest_live_cluster_id(q);  /* takes only the queue lock */
    if (v < *m) *m = v;
    return 0;
}

uint64_t broker_min_live_cluster_id(beaver_broker_t *b)
{
    uint64_t m = UINT64_MAX;
    broker_foreach_queue(b, min_live_cb, &m);
    return m;
}

typedef struct {
    broker_exchange_visit_fn fn;
    void                    *ctx;
} evisit_ctx_t;

static int visit_exchange_cb(const char *key, void *value, void *ctx)
{
    (void)key;
    evisit_ctx_t *v = ctx;
    return v->fn((beaver_exchange_t *)value, v->ctx);
}

void broker_foreach_exchange(beaver_broker_t *b, broker_exchange_visit_fn fn,
                             void *ctx)
{
    evisit_ctx_t v = {fn, ctx};
    pthread_rwlock_rdlock(&b->lock);
    hashmap_iter(b->exchanges, visit_exchange_cb, &v);
    pthread_rwlock_unlock(&b->lock);
}
