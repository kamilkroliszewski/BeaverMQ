/*
 * broker.h - Central in-memory registry and routing core for BeaverMQ.
 *
 * The broker owns the queue registry, the exchange registry, and all bindings.
 * It is the single thread-safe entry point the protocol layer calls to declare
 * queues/exchanges, bind them, and publish messages.
 *
 * Locking hierarchy (to prevent deadlock):
 *     broker registry lock  >  individual queue lock
 * The broker only ever takes a queue lock while holding (or after releasing)
 * its registry lock, never the reverse. Publish deliberately collects target
 * queue references under the registry lock, RELEASES it, and only then locks
 * each queue to enqueue - so no queue lock is held nested inside another.
 */
#ifndef BEAVERMQ_BROKER_H
#define BEAVERMQ_BROKER_H

#include "exchange.h"
#include "queue.h"

#include <stddef.h>
#include <stdio.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct beaver_broker beaver_broker_t;

typedef struct {
    size_t   queue_count;
    size_t   exchange_count;
    uint64_t messages_ready;      /* sum of current queue depths */
    uint64_t total_enqueued;      /* lifetime, across all queues */
    uint64_t total_dequeued;      /* lifetime, across all queues */
    uint64_t messages_published;  /* publish calls received */
} broker_stats_t;

beaver_broker_t *broker_new(void);
void broker_free(beaver_broker_t *b);

/*
 * Object names are namespaced PER VIRTUAL HOST: "orders" in vhost "/" and in
 * vhost "prod" are different queues. Every lookup/declare therefore takes the
 * vhost. Internally the registries key on "vhost \x01 name" (0x01 cannot appear
 * in an AMQP short string, so the key is unambiguous).
 */

/*
 * Declare a queue (idempotent). Returns 0 on success; -1 on allocation
 * failure. If non-NULL, *out_depth receives the queue's current depth and
 * *out_created is set to 1 when the queue was newly created (0 if it existed).
 */
int broker_declare_queue(beaver_broker_t *b, const char *vhost,
                         const char *name, uint8_t flags,
                         uint32_t *out_depth, int *out_created);

/* Declare an exchange (idempotent). Returns 0 / -1; *out_created as above. */
int broker_declare_exchange(beaver_broker_t *b, const char *vhost,
                            const char *name, exchange_type_t type,
                            uint8_t flags, int *out_created);

/* Declare the standard amq.direct / amq.fanout / amq.topic set for a vhost
 * (called when a vhost is created; idempotent). */
void broker_declare_default_exchanges(beaver_broker_t *b, const char *vhost);

/*
 * Bind a queue to an exchange under a routing key (both within `vhost`).
 * Returns 0 on success, or -1 if either does not exist (or on OOM).
 */
int broker_bind(beaver_broker_t *b, const char *vhost, const char *queue,
                const char *exchange, const char *routing_key);

/*
 * Route and store a message within `vhost`. Exchange "" (empty) is the default
 * exchange and delivers straight to the queue named by routing_key. Returns the
 * number of queues the message was enqueued into (0 if unroutable), -1 on OOM.
 */
int broker_publish(beaver_broker_t *b, const char *vhost, const char *exchange,
                   const char *routing_key, const void *body, size_t body_len);

/*
 * Route an already-constructed message (using its own exchange/routing_key)
 * within `vhost`. Assigns the message id, enqueues it into every matching
 * queue, and fires the route callback per queue. Does NOT take ownership of the
 * caller's reference. Returns the number of queues it was enqueued into, or -1.
 */
int broker_route(beaver_broker_t *b, const char *vhost, beaver_message_t *msg);

/* Look up a queue by (vhost, name), returning a NEW reference (caller
 * queue_unref's) or NULL. */
beaver_queue_t *broker_get_queue(beaver_broker_t *b, const char *vhost,
                                 const char *name);

/* Build the composite registry key "vhost \x01 name" (shared with the consumer
 * dispatcher, which keys its per-queue groups the same way). */
static inline void broker_vkey(char out[512], const char *vhost, const char *name)
{
    snprintf(out, 512, "%s\x01%s", vhost ? vhost : "", name ? name : "");
}

/* Snapshot aggregate statistics. */
void broker_stats(beaver_broker_t *b, broker_stats_t *out);

/* Oldest still-live replicated cluster_id across ALL queues (UINT64_MAX if none
 * anywhere). The cluster uses (this - 1) as the floor for replicated-log
 * compaction: every log entry at or below it has been consumed on every node. */
uint64_t broker_min_live_cluster_id(beaver_broker_t *b);

/* Visit every queue under the registry lock. The callback MUST NOT call back
 * into the broker (it would deadlock). Used by the management API (Phase 6). */
typedef int (*broker_queue_visit_fn)(beaver_queue_t *q, void *ctx);
void broker_foreach_queue(beaver_broker_t *b, broker_queue_visit_fn fn,
                          void *ctx);

/* Visit every exchange under the registry lock. Same contract as above. */
typedef int (*broker_exchange_visit_fn)(beaver_exchange_t *ex, void *ctx);
void broker_foreach_exchange(beaver_broker_t *b, broker_exchange_visit_fn fn,
                             void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_BROKER_H */
