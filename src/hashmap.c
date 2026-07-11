/*
 * hashmap.c - Separate-chaining string hash map with automatic growth.
 */
#include "hashmap.h"

#include <stdlib.h>
#include <string.h>

typedef struct entry {
    char         *key;
    void         *value;
    struct entry *next;
    size_t        hash;
} entry_t;

struct hashmap {
    entry_t **buckets;
    size_t    nbuckets;
    size_t    count;
};

#define HASHMAP_INITIAL_BUCKETS 16
#define HASHMAP_MAX_LOAD        0.75

/* djb2 string hash. */
static size_t hash_str(const char *s)
{
    size_t h = 5381;
    int c;
    while ((c = (unsigned char)*s++))
        h = ((h << 5) + h) + (size_t)c;
    return h;
}

hashmap_t *hashmap_new(void)
{
    hashmap_t *m = calloc(1, sizeof(*m));
    if (!m)
        return NULL;
    m->buckets = calloc(HASHMAP_INITIAL_BUCKETS, sizeof(entry_t *));
    if (!m->buckets) {
        free(m);
        return NULL;
    }
    m->nbuckets = HASHMAP_INITIAL_BUCKETS;
    m->count    = 0;
    return m;
}

void hashmap_free(hashmap_t *m, hashmap_free_fn free_value)
{
    if (!m)
        return;
    for (size_t i = 0; i < m->nbuckets; i++) {
        entry_t *e = m->buckets[i];
        while (e) {
            entry_t *next = e->next;
            if (free_value)
                free_value(e->value);
            free(e->key);
            free(e);
            e = next;
        }
    }
    free(m->buckets);
    free(m);
}

static entry_t *find_entry(const hashmap_t *m, const char *key, size_t h)
{
    for (entry_t *e = m->buckets[h % m->nbuckets]; e; e = e->next)
        if (e->hash == h && strcmp(e->key, key) == 0)
            return e;
    return NULL;
}

void *hashmap_get(const hashmap_t *m, const char *key)
{
    entry_t *e = find_entry(m, key, hash_str(key));
    return e ? e->value : NULL;
}

/* Grow the bucket array and rehash existing entries. */
static int hashmap_resize(hashmap_t *m, size_t new_nbuckets)
{
    entry_t **nb = calloc(new_nbuckets, sizeof(entry_t *));
    if (!nb)
        return -1;
    for (size_t i = 0; i < m->nbuckets; i++) {
        entry_t *e = m->buckets[i];
        while (e) {
            entry_t *next = e->next;
            size_t idx = e->hash % new_nbuckets;
            e->next = nb[idx];
            nb[idx] = e;
            e = next;
        }
    }
    free(m->buckets);
    m->buckets  = nb;
    m->nbuckets = new_nbuckets;
    return 0;
}

int hashmap_put(hashmap_t *m, const char *key, void *value, void **old)
{
    if (old)
        *old = NULL;

    size_t h = hash_str(key);
    entry_t *e = find_entry(m, key, h);
    if (e) {
        if (old)
            *old = e->value;
        e->value = value;
        return 0;
    }

    if ((double)(m->count + 1) > (double)m->nbuckets * HASHMAP_MAX_LOAD) {
        if (hashmap_resize(m, m->nbuckets * 2) != 0)
            return -1; /* keep operating at the old size on OOM */
    }

    e = malloc(sizeof(*e));
    if (!e)
        return -1;
    e->key = strdup(key);
    if (!e->key) {
        free(e);
        return -1;
    }
    e->value = value;
    e->hash  = h;
    size_t idx = h % m->nbuckets;
    e->next = m->buckets[idx];
    m->buckets[idx] = e;
    m->count++;
    return 0;
}

void *hashmap_remove(hashmap_t *m, const char *key)
{
    size_t h = hash_str(key);
    size_t idx = h % m->nbuckets;
    entry_t *prev = NULL;
    for (entry_t *e = m->buckets[idx]; e; prev = e, e = e->next) {
        if (e->hash == h && strcmp(e->key, key) == 0) {
            if (prev)
                prev->next = e->next;
            else
                m->buckets[idx] = e->next;
            void *v = e->value;
            free(e->key);
            free(e);
            m->count--;
            return v;
        }
    }
    return NULL;
}

size_t hashmap_count(const hashmap_t *m)
{
    return m->count;
}

void hashmap_iter(const hashmap_t *m, hashmap_iter_fn fn, void *ctx)
{
    for (size_t i = 0; i < m->nbuckets; i++) {
        for (entry_t *e = m->buckets[i]; e; e = e->next) {
            if (fn(e->key, e->value, ctx))
                return;
        }
    }
}
