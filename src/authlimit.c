/*
 * authlimit.c - see authlimit.h.
 *
 * A single fixed-size table of per-key backoff entries guarded by one mutex,
 * plus an atomic in-flight counter for the concurrency cap. Logins are rare
 * and themselves rate-limited, so a plain linear scan of the table per call is
 * more than fast enough and keeps the code obviously correct.
 */
#include "authlimit.h"

#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <stdio.h>

/* ---- policy tunables ------------------------------------------------------ */

#define AUTHLIMIT_TABLE          1024   /* distinct keys tracked at once      */
#define AUTHLIMIT_KEYLEN         64     /* max key length (an IP fits easily) */
#define AUTHLIMIT_FREE_ATTEMPTS  5u     /* failures allowed before any delay  */
#define AUTHLIMIT_BASE_MS        500u   /* first backoff delay                */
#define AUTHLIMIT_MAX_MS         30000u /* backoff ceiling                    */
#define AUTHLIMIT_IDLE_RESET_MS  900000ull /* forget a key idle this long     */
#define AUTHLIMIT_HASH_CONCURRENCY 8    /* max hash operations in flight      */

typedef struct {
    char     key[AUTHLIMIT_KEYLEN];
    uint32_t failures;
    uint64_t next_ms;   /* earliest time the next attempt may be hashed */
    uint64_t last_ms;   /* last time this key was touched (for eviction/decay) */
    int      used;
} entry_t;

static entry_t         g_tab[AUTHLIMIT_TABLE];
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_int      g_inflight;

void authlimit_init(void)
{
    /* State is statically zero-initialized; nothing else is required. Kept so
     * startup can express intent and so a future seed has a home. */
}

/* Copy `key` into a bounded local buffer (keys longer than KEYLEN-1 simply
 * share a bucket - harmless for a rate limiter). */
static void norm_key(char *dst, const char *key)
{
    snprintf(dst, AUTHLIMIT_KEYLEN, "%s", key ? key : "");
}

/* Caller holds g_lock. Return the entry for `key`, or NULL if absent. */
static entry_t *find_locked(const char *key)
{
    for (size_t i = 0; i < AUTHLIMIT_TABLE; i++)
        if (g_tab[i].used && strcmp(g_tab[i].key, key) == 0)
            return &g_tab[i];
    return NULL;
}

/* Caller holds g_lock. Return the entry for `key`, creating it (reusing a free
 * slot, else evicting the least-recently-touched one) if absent. */
static entry_t *find_or_create_locked(const char *key, uint64_t now_ms)
{
    entry_t *e = find_locked(key);
    if (e)
        return e;
    entry_t *slot = NULL, *oldest = &g_tab[0];
    for (size_t i = 0; i < AUTHLIMIT_TABLE; i++) {
        if (!g_tab[i].used) { slot = &g_tab[i]; break; }
        if (g_tab[i].last_ms < oldest->last_ms)
            oldest = &g_tab[i];
    }
    if (!slot)
        slot = oldest; /* table full: evict the stalest key */
    memset(slot, 0, sizeof(*slot));
    snprintf(slot->key, sizeof(slot->key), "%s", key);
    slot->used    = 1;
    slot->last_ms = now_ms;
    return slot;
}

uint64_t authlimit_retry_after_ms(const char *key, uint64_t now_ms)
{
    if (!key || !key[0])
        return 0;
    char k[AUTHLIMIT_KEYLEN];
    norm_key(k, key);

    uint64_t wait = 0;
    pthread_mutex_lock(&g_lock);
    entry_t *e = find_locked(k);
    if (e) {
        if (now_ms - e->last_ms > AUTHLIMIT_IDLE_RESET_MS)
            e->used = 0; /* stale: forget it, start fresh next failure */
        else if (e->next_ms > now_ms)
            wait = e->next_ms - now_ms;
    }
    pthread_mutex_unlock(&g_lock);
    return wait;
}

void authlimit_record_failure(const char *key, uint64_t now_ms)
{
    if (!key || !key[0])
        return;
    char k[AUTHLIMIT_KEYLEN];
    norm_key(k, key);

    pthread_mutex_lock(&g_lock);
    entry_t *e = find_or_create_locked(k, now_ms);
    if (now_ms - e->last_ms > AUTHLIMIT_IDLE_RESET_MS)
        e->failures = 0; /* decayed since last touch */
    e->failures++;
    e->last_ms = now_ms;

    uint64_t delay = 0;
    if (e->failures > AUTHLIMIT_FREE_ATTEMPTS) {
        uint32_t over = e->failures - AUTHLIMIT_FREE_ATTEMPTS; /* 1, 2, 3, ... */
        uint64_t d = AUTHLIMIT_BASE_MS;
        for (uint32_t i = 1; i < over && d < AUTHLIMIT_MAX_MS; i++)
            d <<= 1;
        delay = d > AUTHLIMIT_MAX_MS ? AUTHLIMIT_MAX_MS : d;
    }
    e->next_ms = now_ms + delay;
    pthread_mutex_unlock(&g_lock);
}

void authlimit_record_success(const char *key)
{
    if (!key || !key[0])
        return;
    char k[AUTHLIMIT_KEYLEN];
    norm_key(k, key);

    pthread_mutex_lock(&g_lock);
    entry_t *e = find_locked(k);
    if (e)
        e->used = 0; /* clean slate: a real login clears accumulated backoff */
    pthread_mutex_unlock(&g_lock);
}

int authlimit_hash_begin(void)
{
    int cur = atomic_load_explicit(&g_inflight, memory_order_relaxed);
    while (cur < AUTHLIMIT_HASH_CONCURRENCY) {
        if (atomic_compare_exchange_weak_explicit(&g_inflight, &cur, cur + 1,
                                                  memory_order_acq_rel,
                                                  memory_order_relaxed))
            return 0;
    }
    return -1; /* at capacity */
}

void authlimit_hash_end(void)
{
    atomic_fetch_sub_explicit(&g_inflight, 1, memory_order_acq_rel);
}

void authlimit_reset_for_test(void)
{
    pthread_mutex_lock(&g_lock);
    memset(g_tab, 0, sizeof(g_tab));
    pthread_mutex_unlock(&g_lock);
    atomic_store_explicit(&g_inflight, 0, memory_order_release);
}
