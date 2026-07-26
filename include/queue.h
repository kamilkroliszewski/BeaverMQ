/*
 * queue.h - Thread-safe FIFO message queue for BeaverMQ.
 *
 * Implemented as a growable ring buffer of message references guarded by a
 * per-queue mutex. Enqueue and dequeue are O(1) amortized. The queue is itself
 * reference-counted so that a publisher or consumer can hold a queue pointer
 * safely even if the queue is removed from the broker registry concurrently.
 *
 * Reference conventions:
 *   - queue_enqueue() takes its OWN message reference (the caller keeps theirs).
 *   - queue_dequeue() TRANSFERS a message reference to the caller, who must
 *     message_unref() it when done.
 */
#ifndef BEAVERMQ_QUEUE_H
#define BEAVERMQ_QUEUE_H

#include "message.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct beaver_queue beaver_queue_t;

/* Create a queue with refcount 1. `flags` are the BMQP queue flag bits. */
beaver_queue_t *queue_new(const char *name, uint8_t flags);

/* Reference counting. queue_unref frees the queue (and drops refs on any
 * still-buffered messages) when the last reference is released. */
beaver_queue_t *queue_ref(beaver_queue_t *q);
void            queue_unref(beaver_queue_t *q);

const char *queue_name(const beaver_queue_t *q);
uint8_t     queue_flags(const beaver_queue_t *q);

/* Exclusive-queue ownership: the connection id that declared it exclusive, or 0
 * if the queue is not exclusive. Set by the broker/protocol at declare time and
 * read to reject access from other connections (AMQP RESOURCE_LOCKED). */
void     queue_set_exclusive_owner(beaver_queue_t *q, uint64_t conn_id);
uint64_t queue_exclusive_owner(beaver_queue_t *q);
/* Owning virtual host. Set once by the broker at declare time. */
const char *queue_vhost(const beaver_queue_t *q);
void        queue_set_vhost(beaver_queue_t *q, const char *vhost);

/* Global default per-queue limits (0 = unlimited), applied to EVERY queue by
 * queue_enqueue(). Without these an authenticated publisher can grow a single
 * queue's memory use without bound even though each individual message
 * respects max_message_size. Set once at startup. */
void queue_set_default_limits(uint64_t max_length, uint64_t max_bytes);

/* What a queue does when a publish would exceed its length/byte limit. */
typedef enum {
    QUEUE_OVERFLOW_REJECT_PUBLISH = 0, /* default: reject the new message (QUEUE_FULL) */
    QUEUE_OVERFLOW_DROP_HEAD      = 1, /* evict oldest message(s) to make room */
} queue_overflow_t;

/* Per-queue limit/overflow overrides (from the AMQP Queue.Declare arguments:
 * x-max-length, x-max-length-bytes, x-overflow). A 0 length/bytes value means
 * "fall back to the global default"; overflow selects the full-queue behavior.
 * Typically set once, right after the queue is created. */
void queue_set_limits(beaver_queue_t *q, uint64_t max_length, uint64_t max_bytes,
                      queue_overflow_t overflow);

/* Messages evicted by the drop-head overflow policy (management metric). */
uint64_t queue_total_dropped(beaver_queue_t *q);

/* ---- dead-lettering ------------------------------------------------------ *
 * A queue may have a dead-letter target: when a message leaves the queue as a
 * "dead letter" (rejected/nacked without requeue, or evicted by drop-head), it
 * is re-routed to another exchange instead of being discarded. Because the queue
 * layer does not know about the broker, the actual re-route is a callback the
 * broker installs; the queue just stores the target and invokes the callback. */
typedef void (*queue_dead_letter_fn)(void *ctx, beaver_queue_t *src,
                                     beaver_message_t *msg);

/* Configure (or clear, with exchange == NULL) the dead-letter target and the
 * broker callback that performs the re-route. routing_key may be NULL/"" to
 * reuse each message's original routing key. Set once at declare time. */
void queue_set_dead_letter(beaver_queue_t *q, const char *exchange,
                           const char *routing_key,
                           queue_dead_letter_fn fn, void *ctx);

/* 1 if the queue has a dead-letter target configured. */
int queue_has_dead_letter(beaver_queue_t *q);

/* Dead-letter target accessors (valid for the queue's lifetime; may be ""). */
const char *queue_dl_exchange(beaver_queue_t *q);
const char *queue_dl_routing_key(beaver_queue_t *q);

/* Re-route `msg` to this queue's dead-letter target via the installed callback
 * (no-op if none). Must be called WITHOUT holding the queue lock. Does not take
 * ownership of `msg` (the caller keeps its reference). */
void queue_dead_letter(beaver_queue_t *q, beaver_message_t *msg);

/* Returned by queue_enqueue() when a configured limit (see
 * queue_set_default_limits) would be exceeded - distinct from -1 (OOM) so
 * callers can tell "queue is full" apart from "allocation failed". */
#define QUEUE_FULL (-2)

/* Enqueue a message (adds an internal reference). Returns 0 on success, -1 on
 * OOM, or QUEUE_FULL if a configured length/byte limit would be exceeded. */
int queue_enqueue(beaver_queue_t *q, beaver_message_t *msg);

/* Return a previously-accepted message to the queue after a nack/reject/
 * disconnect. Adds an internal reference like queue_enqueue(), but DELIBERATELY
 * ignores the publisher-facing length/byte limits so a full queue can never
 * silently drop an in-flight message being requeued. Returns 0 on success or
 * -1 only on a genuine OOM growing the ring buffer; on -1 the caller must keep
 * its own reference (do NOT unref) since the message is not on the queue. */
int queue_requeue_internal(beaver_queue_t *q, beaver_message_t *msg);

/* Dequeue the oldest message, transferring its reference to the caller.
 * Returns NULL if the queue is empty. */
beaver_message_t *queue_dequeue(beaver_queue_t *q);

/* Current number of buffered messages. */
size_t queue_depth(beaver_queue_t *q);

/* Lifetime counters. */
uint64_t queue_total_enqueued(beaver_queue_t *q);
uint64_t queue_total_dequeued(beaver_queue_t *q);

/* Drop all buffered messages; returns how many were purged. */
size_t queue_purge(beaver_queue_t *q);

/* ---- cluster consume tracking -------------------------------------------- *
 * Track which replicated messages (by cluster_id) have been consumed so the
 * cluster can drain the same messages from replica queues on the other nodes.
 * State lives in the queue (shared across worker dispatchers) so the watermark
 * is GLOBAL and never passes a still-unacked message. */

/* A replicated message (cluster_id cid) was delivered to a consumer; no_ack
 * means it is already consumed, else it is outstanding until acked. */
void     queue_consume_on_deliver(beaver_queue_t *q, uint64_t cid, int no_ack);
/* A delivered message was acked (or requeued back to available). */
void     queue_consume_on_ack(beaver_queue_t *q, uint64_t cid);
/* The watermark to replicate: every cluster_id <= this is fully consumed. */
uint64_t queue_consume_watermark(beaver_queue_t *q);
/* Oldest still-live replicated cluster_id in this queue (ready or unacked), or
 * UINT64_MAX if none. The replicated log may be compacted below this safely:
 * everything older has been consumed (and drained) on every node. */
uint64_t queue_oldest_live_cluster_id(beaver_queue_t *q);
/* Record that `watermark` was replicated; returns 1 only if it advanced (so a
 * single worker replicates each advance). */
int      queue_consume_mark_replicated(beaver_queue_t *q, uint64_t watermark);
/* Apply on every node: unref front messages with cluster_id in (0, watermark].
 * No-op on the consumer node (already delivered); drains replicas. */
size_t   queue_drain_consumed(beaver_queue_t *q, uint64_t watermark);

/* Snapshot the queue's READY messages whose cluster_id is in (0, max_cid]:
 * returns a malloc'd array of message refs (caller message_unref's each and
 * frees the array) and the count. One lock hold; used by the cluster's state
 * transfer to re-seed a wiped node. NULL + *out_n == 0 means "nothing
 * matched" UNLESS out_oom is non-NULL and comes back 1, meaning the initial
 * allocation failed (OOM) - a real snapshot may be missing data, distinct
 * from a genuinely empty/non-matching queue. Pass out_oom = NULL to ignore
 * the distinction. */
beaver_message_t **queue_snapshot_refs(beaver_queue_t *q, uint64_t max_cid,
                                       size_t *out_n, int *out_oom);

/* ---- waiters (cross-thread delivery wakeup) ------------------------------ *
 * A waiter is a worker dispatcher interested in this queue. When a message is
 * enqueued, the broker calls queue_wake_waiters() to notify every registered
 * waiter (each fires a thread-safe uv_async_send to its own loop). The waiter
 * list is guarded by the queue's mutex, so registration and wakeups are safe
 * to call from any thread. */
typedef void (*queue_waiter_fn)(void *ctx, beaver_queue_t *q);

/* Register / unregister a waiter (idempotent by the (fn, ctx) pair). */
void queue_add_waiter(beaver_queue_t *q, queue_waiter_fn fn, void *ctx);
void queue_remove_waiter(beaver_queue_t *q, queue_waiter_fn fn, void *ctx);

/* Invoke every registered waiter's callback. */
void queue_wake_waiters(beaver_queue_t *q);

/* Consumer count (atomic): adjusted by the dispatcher as consumers come and go,
 * read by the management API. */
void queue_consumers_inc(beaver_queue_t *q);
void queue_consumers_dec(beaver_queue_t *q);
int  queue_consumer_count(beaver_queue_t *q);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_QUEUE_H */
