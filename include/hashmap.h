/*
 * hashmap.h - Minimal string-keyed hash map for BeaverMQ.
 *
 * Separate-chaining hash map with automatic resize. Keys are copied
 * internally; values are opaque (void *) and owned by the caller. The map is
 * NOT internally synchronized - callers that share a map across threads must
 * provide their own locking (the broker does this with its registry lock).
 */
#ifndef BEAVERMQ_HASHMAP_H
#define BEAVERMQ_HASHMAP_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct hashmap hashmap_t;

/* Called for each value when freeing the map / clearing entries (may be NULL). */
typedef void (*hashmap_free_fn)(void *value);

/* Visitor for hashmap_iter; returning nonzero stops iteration early. */
typedef int (*hashmap_iter_fn)(const char *key, void *value, void *ctx);

hashmap_t *hashmap_new(void);

/* Free the map. If free_value is non-NULL it is invoked on every value. */
void hashmap_free(hashmap_t *m, hashmap_free_fn free_value);

/* Look up a key; returns the value or NULL if absent. */
void *hashmap_get(const hashmap_t *m, const char *key);

/*
 * Insert or replace. On replace, the previous value is returned via *old (if
 * non-NULL) so the caller can free it; otherwise *old is set to NULL.
 * Returns 0 on success, -1 on allocation failure.
 */
int hashmap_put(hashmap_t *m, const char *key, void *value, void **old);

/* Remove a key, returning its value (not freed) or NULL if absent. */
void *hashmap_remove(hashmap_t *m, const char *key);

size_t hashmap_count(const hashmap_t *m);

/* Iterate all entries (order unspecified). */
void hashmap_iter(const hashmap_t *m, hashmap_iter_fn fn, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_HASHMAP_H */
