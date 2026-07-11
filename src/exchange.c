/*
 * exchange.c - Exchange types, bindings, and routing.
 */
#include "exchange.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    char           *routing_key; /* owned */
    beaver_queue_t *queue;       /* owned reference */
} binding_t;

struct beaver_exchange {
    char           *name;
    char           *vhost;      /* owning virtual host ("" until set) */
    exchange_type_t type;
    uint8_t         flags;

    binding_t      *bindings;
    size_t          n_bindings;
    size_t          cap_bindings;
};

/* ---- type helpers -------------------------------------------------------- */

int exchange_type_from_name(const char *name, exchange_type_t *out)
{
    if (name && strcmp(name, "fanout") == 0) { *out = EXCHANGE_FANOUT; return 0; }
    if (name && strcmp(name, "topic") == 0)  { *out = EXCHANGE_TOPIC;  return 0; }
    if (name && strcmp(name, "direct") == 0) { *out = EXCHANGE_DIRECT; return 0; }
    *out = EXCHANGE_DIRECT; /* sensible default */
    return -1;
}

const char *exchange_type_name(exchange_type_t type)
{
    switch (type) {
    case EXCHANGE_DIRECT: return "direct";
    case EXCHANGE_FANOUT: return "fanout";
    case EXCHANGE_TOPIC:  return "topic";
    }
    return "unknown";
}

/* ---- lifecycle ----------------------------------------------------------- */

beaver_exchange_t *exchange_new(const char *name, exchange_type_t type,
                                uint8_t flags)
{
    beaver_exchange_t *ex = calloc(1, sizeof(*ex));
    if (!ex)
        return NULL;
    ex->name = strdup(name ? name : "");
    if (!ex->name) {
        free(ex);
        return NULL;
    }
    ex->type  = type;
    ex->flags = flags;
    return ex;
}

void exchange_free(beaver_exchange_t *ex)
{
    if (!ex)
        return;
    for (size_t i = 0; i < ex->n_bindings; i++) {
        free(ex->bindings[i].routing_key);
        queue_unref(ex->bindings[i].queue);
    }
    free(ex->bindings);
    free(ex->name);
    free(ex->vhost);
    free(ex);
}

const char     *exchange_name(const beaver_exchange_t *ex) { return ex->name; }
exchange_type_t exchange_type(const beaver_exchange_t *ex) { return ex->type; }
size_t exchange_binding_count(const beaver_exchange_t *ex) { return ex->n_bindings; }
const char *exchange_vhost(const beaver_exchange_t *ex)
{
    return ex->vhost ? ex->vhost : "";
}
void exchange_set_vhost(beaver_exchange_t *ex, const char *vhost)
{
    free(ex->vhost);
    ex->vhost = strdup(vhost ? vhost : "");
}

/* ---- bindings ------------------------------------------------------------ */

int exchange_bind(beaver_exchange_t *ex, const char *routing_key,
                  beaver_queue_t *q)
{
    if (!routing_key)
        routing_key = "";

    /* Idempotent: ignore an identical (queue, key) binding. */
    for (size_t i = 0; i < ex->n_bindings; i++) {
        if (ex->bindings[i].queue == q &&
            strcmp(ex->bindings[i].routing_key, routing_key) == 0)
            return 0;
    }

    if (ex->n_bindings == ex->cap_bindings) {
        size_t nc = ex->cap_bindings ? ex->cap_bindings * 2 : 4;
        binding_t *nb = realloc(ex->bindings, nc * sizeof(binding_t));
        if (!nb)
            return -1;
        ex->bindings     = nb;
        ex->cap_bindings = nc;
    }

    char *key = strdup(routing_key);
    if (!key)
        return -1;
    ex->bindings[ex->n_bindings].routing_key = key;
    ex->bindings[ex->n_bindings].queue       = queue_ref(q);
    ex->n_bindings++;
    return 0;
}

/* ---- topic matching ------------------------------------------------------ */

/* Split `s` on '.' into a buffer + pointer array (single allocation each). */
typedef struct {
    char  *buf;
    char **words;
    int    n;
} words_t;

static int split_words(const char *s, words_t *w)
{
    w->buf = strdup(s);
    if (!w->buf)
        return -1;

    int n = 1;
    for (const char *p = s; *p; p++)
        if (*p == '.')
            n++;

    w->words = malloc((size_t)n * sizeof(char *));
    if (!w->words) {
        free(w->buf);
        return -1;
    }

    int idx = 0;
    char *start = w->buf;
    for (char *p = w->buf;; p++) {
        if (*p == '.' || *p == '\0') {
            char saved = *p;
            *p = '\0';
            w->words[idx++] = start;
            start = p + 1;
            if (saved == '\0')
                break;
        }
    }
    w->n = idx;
    return 0;
}

static void free_words(words_t *w)
{
    free(w->words);
    free(w->buf);
}

/* Recursive token matcher: '*' matches one word, '#' matches zero or more. */
static int match_tokens(char **pat, int pi, int np, char **key, int ki, int nk)
{
    if (pi == np)
        return ki == nk;

    if (strcmp(pat[pi], "#") == 0) {
        for (int k = ki; k <= nk; k++)
            if (match_tokens(pat, pi + 1, np, key, k, nk))
                return 1;
        return 0;
    }
    if (ki == nk)
        return 0;
    if (strcmp(pat[pi], "*") == 0 || strcmp(pat[pi], key[ki]) == 0)
        return match_tokens(pat, pi + 1, np, key, ki + 1, nk);
    return 0;
}

int exchange_topic_match(const char *pattern, const char *key)
{
    words_t pw, kw;
    if (split_words(pattern, &pw) != 0)
        return 0;
    if (split_words(key, &kw) != 0) {
        free_words(&pw);
        return 0;
    }
    int r = match_tokens(pw.words, 0, pw.n, kw.words, 0, kw.n);
    free_words(&pw);
    free_words(&kw);
    return r;
}

/* ---- routing ------------------------------------------------------------- */

static int binding_matches(const beaver_exchange_t *ex, const binding_t *b,
                           const char *routing_key)
{
    switch (ex->type) {
    case EXCHANGE_FANOUT: return 1;
    case EXCHANGE_DIRECT: return strcmp(b->routing_key, routing_key) == 0;
    case EXCHANGE_TOPIC:  return exchange_topic_match(b->routing_key, routing_key);
    }
    return 0;
}

size_t exchange_route(const beaver_exchange_t *ex, const char *routing_key,
                      beaver_queue_t ***out)
{
    *out = NULL;
    if (ex->n_bindings == 0)
        return 0;
    if (!routing_key)
        routing_key = "";

    beaver_queue_t **targets = malloc(ex->n_bindings * sizeof(beaver_queue_t *));
    if (!targets)
        return 0;

    size_t n = 0;
    for (size_t i = 0; i < ex->n_bindings; i++) {
        if (!binding_matches(ex, &ex->bindings[i], routing_key))
            continue;
        /* De-duplicate: a queue bound under several matching keys gets one copy. */
        beaver_queue_t *q = ex->bindings[i].queue;
        int dup = 0;
        for (size_t j = 0; j < n; j++) {
            if (targets[j] == q) { dup = 1; break; }
        }
        if (!dup)
            targets[n++] = queue_ref(q);
    }

    if (n == 0) {
        free(targets);
        return 0;
    }
    *out = targets;
    return n;
}
