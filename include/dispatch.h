/*
 * dispatch.h - Consumer dispatcher (the push model) for BeaverMQ.
 *
 * The dispatcher tracks consumers (Basic.Consume registrations) and pushes
 * queued messages to them as Basic.Deliver frames. Delivery is driven by a
 * libuv async handle: when a message is routed into a queue (or a consumer
 * subscribes to a queue with a backlog), the queue is marked pending and the
 * async callback drains it to its consumers, round-robin across competing
 * consumers on the same queue.
 *
 * Acknowledgements:
 *   - no_ack consumers receive fire-and-forget deliveries.
 *   - manual-ack consumers have each delivery tracked as "unacked" until a
 *     Basic.Ack arrives; on disconnect, unacked messages are requeued so no
 *     message is lost.
 *
 * Threading: every entry point runs on the libuv loop thread (uv_write is not
 * thread-safe), so the dispatcher holds no locks. The broker's route callback
 * (dispatcher_on_message) is invoked from broker_publish, which in this broker
 * also runs on the loop thread.
 */
#ifndef BEAVERMQ_DISPATCH_H
#define BEAVERMQ_DISPATCH_H

#include "broker.h"

#include <uv.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct beaver_conn       beaver_conn_t;
typedef struct beaver_dispatcher beaver_dispatcher_t;
struct cluster_node;

beaver_dispatcher_t *dispatcher_new(uv_loop_t *loop, beaver_broker_t *broker);

/* Give the dispatcher the cluster node so it replicates per-queue consume
 * watermarks (draining replica queues on the other nodes). NULL = standalone.
 * Call before the loop runs. */
void dispatcher_set_cluster(beaver_dispatcher_t *d, struct cluster_node *cluster);

/* A Basic.Get auto-ack pull consumed message `cid` from `q`. This path bypasses
 * the push dispatcher, so it advances and replicates the queue's consume
 * watermark here too - otherwise the replica copies on the other nodes would
 * never drain and the log would never compact past a pull-consumed message.
 * No-op when standalone or for a non-replicated message (cid == 0). Must run on
 * the dispatcher's loop thread (it is - same worker as the connection). */
void dispatcher_note_pull(beaver_dispatcher_t *d, beaver_queue_t *q, uint64_t cid);

/* Free all consumer state. The async handle must already have been closed via
 * dispatcher_request_close() and the loop drained. */
void dispatcher_free(beaver_dispatcher_t *d);

/* Close the internal async handle so the event loop can exit. Idempotent;
 * call during graceful shutdown before the final loop drain. */
void dispatcher_request_close(beaver_dispatcher_t *d);

/*
 * Register a consumer on a queue (within `vhost`). Returns 0 on success, -1 if
 * the queue does not exist or on allocation failure. Schedules any backlog.
 */
int dispatcher_add_consumer(beaver_dispatcher_t *d, beaver_conn_t *conn,
                            uint16_t channel, const char *tag,
                            const char *vhost, const char *queue_name,
                            int no_ack);

/* Acknowledge a delivery on (conn, channel). Drops the tracked message. */
void dispatcher_ack(beaver_dispatcher_t *d, beaver_conn_t *conn,
                    uint16_t channel, uint64_t delivery_tag);

/* Cancel a single consumer (Basic.Cancel) by tag, requeuing its unacked. */
void dispatcher_cancel(beaver_dispatcher_t *d, beaver_conn_t *conn,
                       uint16_t channel, const char *tag);

/* Resume delivery to a connection after its write buffer drained (called from
 * the network layer's write-completion callback). */
void dispatcher_resume_conn(beaver_dispatcher_t *d, beaver_conn_t *conn);

/* Remove all consumers belonging to a connection, requeuing their unacked
 * messages. Called when a connection closes. */
void dispatcher_remove_connection(beaver_dispatcher_t *d, beaver_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_DISPATCH_H */
