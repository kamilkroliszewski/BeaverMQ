/*
 * exchange.h - Exchange + binding registry for BeaverMQ.
 *
 * An exchange owns a list of bindings (routing_key -> queue) and knows how to
 * route a message to the set of matching queues according to its type:
 *   - DIRECT : binding key must equal the message routing key exactly.
 *   - FANOUT : routing key ignored; every bound queue matches.
 *   - TOPIC  : binding key is a pattern with '*' (one word) and '#' (zero or
 *              more words) over a dot-separated routing key.
 *
 * Exchanges are not internally locked: the broker serializes all binding and
 * routing operations under its registry lock (see the lock hierarchy in
 * broker.c). Each binding holds a reference to its target queue.
 */
#ifndef BEAVERMQ_EXCHANGE_H
#define BEAVERMQ_EXCHANGE_H

#include "queue.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    EXCHANGE_DIRECT = 0,
    EXCHANGE_FANOUT,
    EXCHANGE_TOPIC
} exchange_type_t;

typedef struct beaver_exchange beaver_exchange_t;

/* Map a type name ("direct"/"fanout"/"topic") to the enum. Returns 0 on
 * success; on an unknown name returns -1 and sets *out to EXCHANGE_DIRECT. */
int exchange_type_from_name(const char *name, exchange_type_t *out);
const char *exchange_type_name(exchange_type_t type);

beaver_exchange_t *exchange_new(const char *name, exchange_type_t type,
                                uint8_t flags);
void exchange_free(beaver_exchange_t *ex);

const char     *exchange_name(const beaver_exchange_t *ex);
exchange_type_t exchange_type(const beaver_exchange_t *ex);
uint8_t         exchange_flags(const beaver_exchange_t *ex);
size_t          exchange_binding_count(const beaver_exchange_t *ex);
/* Owning virtual host. Set once by the broker at declare time. */
const char     *exchange_vhost(const beaver_exchange_t *ex);
void            exchange_set_vhost(beaver_exchange_t *ex, const char *vhost);

/*
 * Bind `q` to the exchange under `routing_key`. Takes its own reference to the
 * queue. Re-binding the same (queue, key) pair is a no-op. Returns 0 or -1.
 */
int exchange_bind(beaver_exchange_t *ex, const char *routing_key,
                  beaver_queue_t *q);

/*
 * Compute the set of queues a message with `routing_key` routes to. On success
 * returns the count and stores a malloc'd array of queue pointers in *out
 * (each queue_ref'd; the caller must queue_unref each and free the array).
 * Returns 0 with *out = NULL when nothing matches. Duplicates are removed.
 * If out_oom is non-NULL, it is set to 1 when the 0 return was actually an
 * allocation failure rather than a genuine no-match - distinguishing the two
 * matters because they call for different responses (retry/backpressure vs.
 * "this routing key has no destination"). Pass NULL to ignore the distinction.
 */
size_t exchange_route(const beaver_exchange_t *ex, const char *routing_key,
                      beaver_queue_t ***out, int *out_oom);

/* True if `pattern` (with '*'/'#') matches the dot-separated `key`. Exposed
 * for unit testing. */
int exchange_topic_match(const char *pattern, const char *key);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_EXCHANGE_H */
