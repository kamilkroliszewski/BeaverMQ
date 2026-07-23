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
/* Owning virtual host. Set once by the broker at declare time. */
const char *queue_vhost(const beaver_queue_t *q);
void        queue_set_vhost(beaver_queue_t *q, const char *vhost);

/* Global default per-queue limits (0 = unlimited), applied to EVERY queue by
 * queue_enqueue(). Without these an authenticated publisher can grow a single
 * queue's memory use without bound even though each individual message
 * respects max_message_size. Set once at startup. */
void queue_set_default_limits(uint64_t max_length, uint64_t max_bytes);

/* Returned by queue_enqueue() when a configured limit (see
 * queue_set_default_limits) would be exceeded - distinct from -1 (OOM) so
 * callers can tell "queue is full" apart from "allocation failed". */
#define QUEUE_FULL (-2)

/* Enqueue a message (adds an internal reference). Returns 0 on success, -1 on
 * OOM, or QUEUE_FULL if a configured length/byte limit would be exceeded. */
int queue_enqueue(beaver_queue_t *q, beaver_message_t *msg);

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
