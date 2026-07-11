/*
 * queue.c - Growable ring-buffer FIFO, thread-safe via a per-queue mutex.
 *
 * The ring buffer stores message pointers in `slots[cap]`, with `head` the
 * next slot to dequeue and `tail` the next slot to fill. When it fills up the
 * buffer doubles and the live elements are re-linearized starting at index 0.
 *
 * The queue refcount is a C11 atomic so queue_ref/queue_unref need no lock;
 * the mutex only guards the buffer contents and counters.
 */
#include "queue.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define QUEUE_INITIAL_CAP 16

struct beaver_queue {
    char              *name;
    char              *vhost;   /* owning virtual host ("" until set) */
    uint8_t            flags;

    pthread_mutex_t    lock;
    beaver_message_t **slots;
    size_t             cap;
    size_t             head;
    size_t             tail;
    size_t             count;

    uint64_t           total_enqueued;
    uint64_t           total_dequeued;

    /* Cluster consume-tracking (guarded by `lock`). The replicated consume
     * watermark = the cluster_id below which everything is consumed: the oldest
     * unacked id minus one, or (nothing unacked) the highest delivered id. Held
     * here so it is GLOBAL across all worker dispatchers - the watermark must
     * never pass an unacked message, or a replica could drop undelivered data. */
    uint64_t           deliver_hi;        /* highest cluster_id handed to a consumer */
    uint64_t          *unacked;           /* sorted cluster_ids delivered, not acked */
    size_t             n_unacked, cap_unacked;
    uint64_t           consume_replicated; /* highest watermark already replicated */

    /* Waiters notified on enqueue (cross-thread delivery wakeup). Guarded by
     * `lock`. */
    struct queue_waiter {
        queue_waiter_fn fn;
        void           *ctx;
    }                 *waiters;
    size_t             n_waiters;
    size_t             cap_waiters;

    _Atomic int        consumers;  /* live consumer count (management metric) */
    _Atomic int        refcount;
};

beaver_queue_t *queue_new(const char *name, uint8_t flags)
{
    beaver_queue_t *q = calloc(1, sizeof(*q));
    if (!q)
        return NULL;
    q->name = strdup(name ? name : "");
    if (!q->name) {
        free(q);
        return NULL;
    }
    q->slots = calloc(QUEUE_INITIAL_CAP, sizeof(beaver_message_t *));
    if (!q->slots) {
        free(q->name);
        free(q);
        return NULL;
    }
    if (pthread_mutex_init(&q->lock, NULL) != 0) {
        free(q->slots);
        free(q->name);
        free(q);
        return NULL;
    }
    q->cap   = QUEUE_INITIAL_CAP;
    q->flags = flags;
    atomic_init(&q->consumers, 0);
    atomic_init(&q->refcount, 1);
    return q;
}

beaver_queue_t *queue_ref(beaver_queue_t *q)
{
    atomic_fetch_add_explicit(&q->refcount, 1, memory_order_relaxed);
    return q;
}

void queue_unref(beaver_queue_t *q)
{
    if (!q)
        return;
    if (atomic_fetch_sub_explicit(&q->refcount, 1, memory_order_acq_rel) == 1) {
        /* Last reference: drop any messages still buffered. */
        for (size_t i = 0; i < q->count; i++)
            message_unref(q->slots[(q->head + i) % q->cap]);
        pthread_mutex_destroy(&q->lock);
        free(q->waiters);
        free(q->unacked);
        free(q->slots);
        free(q->name);
        free(q->vhost);
        free(q);
    }
}

const char *queue_name(const beaver_queue_t *q)  { return q->name; }
const char *queue_vhost(const beaver_queue_t *q) { return q->vhost ? q->vhost : ""; }
void queue_set_vhost(beaver_queue_t *q, const char *vhost)
{
    free(q->vhost);
    q->vhost = strdup(vhost ? vhost : "");
}
uint8_t     queue_flags(const beaver_queue_t *q) { return q->flags; }

/* Double the ring buffer, re-linearizing live elements from index 0.
 * Caller must hold q->lock. Returns 0 or -1 on OOM. */
static int queue_grow(beaver_queue_t *q)
{
    size_t newcap = q->cap * 2;
    beaver_message_t **ns = malloc(newcap * sizeof(beaver_message_t *));
    if (!ns)
        return -1;
    for (size_t i = 0; i < q->count; i++)
        ns[i] = q->slots[(q->head + i) % q->cap];
    free(q->slots);
    q->slots = ns;
    q->cap   = newcap;
    q->head  = 0;
    q->tail  = q->count;
    return 0;
}

int queue_enqueue(beaver_queue_t *q, beaver_message_t *msg)
{
    pthread_mutex_lock(&q->lock);
    if (q->count == q->cap) {
        if (queue_grow(q) != 0) {
            pthread_mutex_unlock(&q->lock);
            return -1;
        }
    }
    q->slots[q->tail] = message_ref(msg);
    q->tail = (q->tail + 1) % q->cap;
    q->count++;
    q->total_enqueued++;
    pthread_mutex_unlock(&q->lock);
    return 0;
}

beaver_message_t *queue_dequeue(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    beaver_message_t *msg = NULL;
    if (q->count > 0) {
        msg = q->slots[q->head];
        q->slots[q->head] = NULL;
        q->head = (q->head + 1) % q->cap;
        q->count--;
        q->total_dequeued++;
    }
    pthread_mutex_unlock(&q->lock);
    return msg; /* reference transferred to caller */
}

/* ---- cluster consume tracking (drains replica queues) -------------------- */

void queue_consume_on_deliver(beaver_queue_t *q, uint64_t cid, int no_ack)
{
    if (cid == 0)
        return; /* non-replicated message: irrelevant to the watermark */
    pthread_mutex_lock(&q->lock);
    if (cid > q->deliver_hi)
        q->deliver_hi = cid;
    if (!no_ack) {
        /* Keep `unacked` sorted ascending so unacked[0] is the oldest. Insert at
         * the right spot (O(1) for in-order delivery, the common case) and skip
         * duplicates (re-delivery of a requeued message). */
        size_t i = q->n_unacked;
        while (i > 0 && q->unacked[i - 1] > cid)
            i--;
        if (!(i > 0 && q->unacked[i - 1] == cid)) { /* not already tracked */
            if (q->n_unacked == q->cap_unacked) {
                size_t nc = q->cap_unacked ? q->cap_unacked * 2 : 16;
                uint64_t *nu = realloc(q->unacked, nc * sizeof(*nu));
                if (nu) { q->unacked = nu; q->cap_unacked = nc; }
            }
            if (q->n_unacked < q->cap_unacked) {
                memmove(&q->unacked[i + 1], &q->unacked[i],
                        (q->n_unacked - i) * sizeof(*q->unacked));
                q->unacked[i] = cid;
                q->n_unacked++;
            }
        }
    }
    pthread_mutex_unlock(&q->lock);
}

void queue_consume_on_ack(beaver_queue_t *q, uint64_t cid)
{
    if (cid == 0)
        return;
    pthread_mutex_lock(&q->lock);
    for (size_t i = 0; i < q->n_unacked; i++) {
        if (q->unacked[i] == cid) {
            memmove(&q->unacked[i], &q->unacked[i + 1],
                    (q->n_unacked - i - 1) * sizeof(*q->unacked));
            q->n_unacked--;
            break;
        }
    }
    pthread_mutex_unlock(&q->lock);
}

uint64_t queue_consume_watermark(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    uint64_t w = q->n_unacked ? q->unacked[0] - 1 : q->deliver_hi;
    pthread_mutex_unlock(&q->lock);
    return w;
}

beaver_message_t **queue_snapshot_refs(beaver_queue_t *q, uint64_t max_cid,
                                       size_t *out_n)
{
    pthread_mutex_lock(&q->lock);
    beaver_message_t **arr = q->count ? malloc(q->count * sizeof(*arr)) : NULL;
    size_t n = 0;
    if (arr) {
        for (size_t i = 0; i < q->count; i++) {
            beaver_message_t *m = q->slots[(q->head + i) % q->cap];
            if (m->cluster_id > 0 && m->cluster_id <= max_cid)
                arr[n++] = message_ref(m);
        }
    }
    pthread_mutex_unlock(&q->lock);
    *out_n = n;
    if (arr && n == 0) { free(arr); arr = NULL; }
    return arr;
}

uint64_t queue_oldest_live_cluster_id(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    uint64_t r = UINT64_MAX;
    if (q->n_unacked) {
        /* Unacked (delivered-not-acked, incl. requeued) are tracked sorted, so
         * the smallest still-live replicated id is unacked[0]. */
        r = q->unacked[0];
    } else if (q->count) {
        /* Nothing outstanding: the still-ready messages are in enqueue order
         * (= ascending cluster_id), so the first with a real id is the oldest
         * live one. Transient (cid 0) messages aren't in the log -> skip. */
        for (size_t i = 0; i < q->count; i++) {
            uint64_t cid = q->slots[(q->head + i) % q->cap]->cluster_id;
            if (cid) { r = cid; break; }
        }
    }
    pthread_mutex_unlock(&q->lock);
    return r;
}

int queue_consume_mark_replicated(beaver_queue_t *q, uint64_t watermark)
{
    int advanced = 0;
    pthread_mutex_lock(&q->lock);
    if (watermark > q->consume_replicated) {
        q->consume_replicated = watermark;
        advanced = 1;
    }
    pthread_mutex_unlock(&q->lock);
    return advanced;
}

size_t queue_drain_consumed(beaver_queue_t *q, uint64_t watermark)
{
    size_t drained = 0;
    pthread_mutex_lock(&q->lock);
    while (q->count > 0) {
        beaver_message_t *m = q->slots[q->head];
        if (!m || m->cluster_id == 0 || m->cluster_id > watermark)
            break;
        q->slots[q->head] = NULL;
        q->head = (q->head + 1) % q->cap;
        q->count--;
        q->total_dequeued++;
        message_unref(m);
        drained++;
    }
    pthread_mutex_unlock(&q->lock);
    return drained;
}

size_t queue_depth(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    size_t n = q->count;
    pthread_mutex_unlock(&q->lock);
    return n;
}

uint64_t queue_total_enqueued(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    uint64_t n = q->total_enqueued;
    pthread_mutex_unlock(&q->lock);
    return n;
}

uint64_t queue_total_dequeued(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    uint64_t n = q->total_dequeued;
    pthread_mutex_unlock(&q->lock);
    return n;
}

size_t queue_purge(beaver_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    size_t purged = q->count;
    while (q->count > 0) {
        message_unref(q->slots[q->head]);
        q->slots[q->head] = NULL;
        q->head = (q->head + 1) % q->cap;
        q->count--;
    }
    pthread_mutex_unlock(&q->lock);
    return purged;
}

/* ---- waiters ------------------------------------------------------------- */

void queue_add_waiter(beaver_queue_t *q, queue_waiter_fn fn, void *ctx)
{
    pthread_mutex_lock(&q->lock);
    for (size_t i = 0; i < q->n_waiters; i++) {
        if (q->waiters[i].fn == fn && q->waiters[i].ctx == ctx) {
            pthread_mutex_unlock(&q->lock); /* already registered */
            return;
        }
    }
    if (q->n_waiters == q->cap_waiters) {
        size_t nc = q->cap_waiters ? q->cap_waiters * 2 : 4;
        struct queue_waiter *nw = realloc(q->waiters, nc * sizeof(*nw));
        if (!nw) {
            pthread_mutex_unlock(&q->lock);
            return; /* best-effort; a missed waiter just means no push wakeup */
        }
        q->waiters     = nw;
        q->cap_waiters = nc;
    }
    q->waiters[q->n_waiters].fn  = fn;
    q->waiters[q->n_waiters].ctx = ctx;
    q->n_waiters++;
    pthread_mutex_unlock(&q->lock);
}

void queue_remove_waiter(beaver_queue_t *q, queue_waiter_fn fn, void *ctx)
{
    pthread_mutex_lock(&q->lock);
    for (size_t i = 0; i < q->n_waiters; i++) {
        if (q->waiters[i].fn == fn && q->waiters[i].ctx == ctx) {
            q->waiters[i] = q->waiters[--q->n_waiters];
            break;
        }
    }
    pthread_mutex_unlock(&q->lock);
}

void queue_wake_waiters(beaver_queue_t *q)
{
    /* Held under the lock: each callback only takes its own (dispatcher)
     * pending lock - never a queue lock - so the queue->pending order is
     * consistent and deadlock-free. */
    pthread_mutex_lock(&q->lock);
    for (size_t i = 0; i < q->n_waiters; i++)
        q->waiters[i].fn(q->waiters[i].ctx, q);
    pthread_mutex_unlock(&q->lock);
}

void queue_consumers_inc(beaver_queue_t *q)
{
    atomic_fetch_add_explicit(&q->consumers, 1, memory_order_relaxed);
}

void queue_consumers_dec(beaver_queue_t *q)
{
    atomic_fetch_sub_explicit(&q->consumers, 1, memory_order_relaxed);
}

int queue_consumer_count(beaver_queue_t *q)
{
    return atomic_load_explicit(&q->consumers, memory_order_relaxed);
}
