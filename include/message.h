/*
 * message.h - Reference-counted message for BeaverMQ.
 *
 * A message owns its exchange name, routing key, and body. It is shared across
 * queues (e.g. a fanout exchange enqueues the *same* message into every bound
 * queue) without copying the body: each holder keeps a reference, and the
 * message frees itself when the last reference is dropped. The reference count
 * is a C11 atomic, so ref/unref are safe to call from any thread.
 */
#ifndef BEAVERMQ_MESSAGE_H
#define BEAVERMQ_MESSAGE_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct beaver_message {
    uint64_t     id;           /* monotonic id assigned by the broker */
    uint64_t     cluster_id;   /* Raft log index of the replicated publish (0 =
                                * not replicated); cluster-wide message identity
                                * used to drain replica queues on consume */
    char        *exchange;     /* owned copy */
    char        *routing_key;  /* owned copy */
    void        *body;         /* owned copy */
    size_t       body_len;
    /* Raw AMQP content-header property section (property-flags + property
     * list), captured verbatim from Basic.Publish so it can be replayed
     * unchanged in Basic.Deliver. NULL means "no properties". */
    uint8_t     *props;
    size_t       props_len;
    _Atomic int  refcount;     /* use message_ref/message_unref only */
} beaver_message_t;

/*
 * Create a message with refcount 1. The exchange, routing_key, and body are
 * copied. Returns NULL on allocation failure.
 */
beaver_message_t *message_new(const char *exchange, const char *routing_key,
                              const void *body, size_t body_len);

/*
 * Like message_new, but also captures the AMQP content-property bytes (the
 * property-flags + property-list section of a content header). `props` may be
 * NULL/0. Returns NULL on allocation failure.
 */
beaver_message_t *message_new_full(const char *exchange, const char *routing_key,
                                   const void *body, size_t body_len,
                                   const void *props, size_t props_len);

/* Increment the refcount and return the same pointer. */
beaver_message_t *message_ref(beaver_message_t *m);

/* Decrement the refcount; frees the message when it reaches zero. */
void message_unref(beaver_message_t *m);

/* Current reference count (for tests/diagnostics). */
int message_refcount(const beaver_message_t *m);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_MESSAGE_H */
