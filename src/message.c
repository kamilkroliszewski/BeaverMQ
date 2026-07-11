/*
 * message.c - Reference-counted message implementation.
 */
#include "message.h"

#include <stdlib.h>
#include <string.h>

beaver_message_t *message_new_full(const char *exchange, const char *routing_key,
                                   const void *body, size_t body_len,
                                   const void *props, size_t props_len)
{
    size_t elen  = exchange ? strlen(exchange) : 0;
    size_t rklen = routing_key ? strlen(routing_key) : 0;
    if (!props)
        props_len = 0;

    /* Single allocation for the whole message: the struct header followed by
     * the exchange, routing key and body (each NUL-terminated so logging never
     * reads past the end) and the raw property bytes. This collapses what used
     * to be up to five malloc()s per message into one - a big cut in allocator
     * traffic (and tail-latency jitter) on the publish hot path. */
    size_t need = sizeof(beaver_message_t)
                + elen + 1 + rklen + 1 + body_len + 1 + props_len;
    beaver_message_t *m = malloc(need);
    if (!m)
        return NULL;
    memset(m, 0, sizeof(*m));

    char *p = (char *)m + sizeof(*m);

    m->exchange = p;
    if (elen) memcpy(p, exchange, elen);
    p[elen] = '\0';
    p += elen + 1;

    m->routing_key = p;
    if (rklen) memcpy(p, routing_key, rklen);
    p[rklen] = '\0';
    p += rklen + 1;

    m->body = p;
    if (body_len && body) memcpy(p, body, body_len);
    p[body_len] = '\0';
    p += body_len + 1;

    if (props_len) {
        m->props     = (uint8_t *)p;
        memcpy(p, props, props_len);
        m->props_len = props_len;
    }

    m->body_len = body_len;
    atomic_init(&m->refcount, 1);
    return m;
}

beaver_message_t *message_new(const char *exchange, const char *routing_key,
                              const void *body, size_t body_len)
{
    return message_new_full(exchange, routing_key, body, body_len, NULL, 0);
}

beaver_message_t *message_ref(beaver_message_t *m)
{
    atomic_fetch_add_explicit(&m->refcount, 1, memory_order_relaxed);
    return m;
}

void message_unref(beaver_message_t *m)
{
    if (!m)
        return;
    /* acq_rel so the thread that drops the count to zero sees all prior writes
     * from other threads before it frees the object. The struct and all its
     * fields live in one block, so a single free() releases everything. */
    if (atomic_fetch_sub_explicit(&m->refcount, 1, memory_order_acq_rel) == 1)
        free(m);
}

int message_refcount(const beaver_message_t *m)
{
    return atomic_load_explicit(&m->refcount, memory_order_relaxed);
}
