/*
 * authstore.c - replicated vhost / user / permission store (see authstore.h).
 *
 * Small tables (a handful of vhosts/users/perms), so plain dynamic arrays under
 * one rwlock are simpler and quite fast enough. Passwords are stored as salted
 * PBKDF2-HMAC-SHA256 (legacy single-round salted SHA-256 hashes still verify);
 * permissions are POSIX extended regexes matched against the object name
 * (RabbitMQ-compatible semantics: an empty pattern denies everything).
 */
#include "authstore.h"
#include "crypto.h"

#include <pthread.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static void to_hex(const uint8_t *b, size_t n, char *out)
{
    static const char *h = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) { out[i*2]=h[b[i]>>4]; out[i*2+1]=h[b[i]&0xf]; }
    out[n*2] = '\0';
}
static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    c |= 32;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}
static int from_hex(const char *s, uint8_t *out, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        int a = s[i*2]   ? hexval(s[i*2])   : -1;
        int b = (a >= 0 && s[i*2+1]) ? hexval(s[i*2+1]) : -1;
        if (a < 0 || b < 0) return -1;
        out[i] = (uint8_t)((a<<4)|b);
    }
    return 0;
}

/* ---- PBKDF2 (RFC 8018), built on crypto_hmac_sha256 ----------------------- */

/* PBKDF2-HMAC-SHA256 producing one 32-byte block (dkLen == hash length). */
static void pbkdf2_sha256(const char *pass, const uint8_t *salt, size_t saltlen,
                          uint32_t iters, uint8_t out[32])
{
    const uint8_t *pw = (const uint8_t *)pass;
    size_t pwlen = strlen(pass);
    uint8_t msg[64], u[32];
    memcpy(msg, salt, saltlen);
    msg[saltlen]     = 0; msg[saltlen + 1] = 0;
    msg[saltlen + 2] = 0; msg[saltlen + 3] = 1;    /* INT(1), big-endian */
    crypto_hmac_sha256(pw, pwlen, msg, saltlen + 4, u);   /* U1 */
    memcpy(out, u, 32);
    for (uint32_t i = 1; i < iters; i++) {
        crypto_hmac_sha256(pw, pwlen, u, 32, u);          /* Ui = PRF(P, Ui-1) */
        for (int j = 0; j < 32; j++) out[j] ^= u[j];
    }
}

/*
 * Iteration count for NEWLY created hashes. Verification now runs OFF the event
 * loop (a libuv worker thread - see the async auth paths in protocol.c and
 * http.c), so a high work factor no longer stalls connection handling, and this
 * is raised well above the original 10000 to make offline brute-force of a
 * leaked store much more expensive. It is bounded below the very top of the
 * OWASP range only because HTTP Basic re-hashes on every request and this
 * build's portable SHA-256 is unaccelerated (~114ms per hash at 100000 as
 * measured); the per-IP backoff + concurrent-hash cap (authlimit) bound abuse.
 * Existing "$p2$<iters>$..." records keep verifying against THEIR stored
 * iteration count, so raising this never invalidates already-stored passwords.
 */
#define AUTH_PBKDF2_ITERS 210000u

/* Returns 0 on success, -1 if no CSPRNG source was available. There is
 * deliberately NO rand() fallback: a predictable salt from a non-CSPRNG PRNG
 * defeats the whole point of salting (precomputation/rainbow-table reuse
 * across accounts becomes feasible again) - refusing to create the password
 * is safer than silently creating a weakly-salted one. */
static int read_salt(uint8_t *salt, size_t n)
{
    return crypto_random_bytes(salt, n);
}

/* Stored form (v2): "$p2$<iters>$<hex salt[16]>$<hex pbkdf2-digest[32]>".
 * Legacy (v1) form hex(salt[4] || sha256(salt||password)) is still VERIFIED
 * (existing stores keep working) but never produced anymore. */
int authstore_hash_password(const char *password, char *out, size_t out_cap)
{
    if (out_cap < AUTHSTORE_HASH_MAX) { if (out_cap) out[0] = '\0'; return -1; }
    uint8_t salt[16], dig[32];
    if (read_salt(salt, sizeof salt) != 0) {
        out[0] = '\0';
        return -1;
    }
    pbkdf2_sha256(password, salt, sizeof salt, AUTH_PBKDF2_ITERS, dig);
    char sh[33], dh[65];
    to_hex(salt, sizeof salt, sh);
    to_hex(dig, sizeof dig, dh);
    snprintf(out, out_cap, "$p2$%u$%s$%s", AUTH_PBKDF2_ITERS, sh, dh);
    return 0;
}

static int hash_matches(const char *stored, const char *password)
{
    if (strncmp(stored, "$p2$", 4) == 0) {
        char *end = NULL;
        unsigned long iters = strtoul(stored + 4, &end, 10);
        /* Cap at 10x our own default: this server never produces a hash above
         * AUTH_PBKDF2_ITERS, so a stored record demanding far more work can
         * only be corrupted or maliciously crafted, and running that inflated
         * PBKDF2 cost on the event loop per login attempt is itself a CPU-
         * exhaustion DoS. Legacy 10000-iter records stay well under the cap. */
        if (!end || *end != '$' || iters == 0 || iters > 10u * AUTH_PBKDF2_ITERS)
            return 0;
        const char *sh = end + 1;                 /* salt hex, then '$', digest */
        if (strlen(sh) != 32 + 1 + 64 || sh[32] != '$')
            return 0;
        uint8_t salt[16], want[32], dig[32];
        if (from_hex(sh, salt, sizeof salt) != 0 ||
            from_hex(sh + 33, want, sizeof want) != 0)
            return 0;
        pbkdf2_sha256(password, salt, sizeof salt, (uint32_t)iters, dig);
        return crypto_ct_memcmp(dig, want, sizeof dig) == 0;
    }

    /* Legacy v1: hex( salt[4] || sha256(salt || password) ) = 72 hex chars. */
    uint8_t blob[36];
    if (strlen(stored) < 72 || from_hex(stored, blob, 36) != 0) return 0;
    size_t pl = strlen(password);
    uint8_t *buf = malloc(4 + pl);
    if (!buf) return 0;
    memcpy(buf, blob, 4); memcpy(buf + 4, password, pl);
    uint8_t dig[32]; crypto_sha256(buf, 4 + pl, dig); free(buf);
    return crypto_ct_memcmp(dig, blob + 4, 32) == 0;
}

/* ---- store --------------------------------------------------------------- */

typedef struct { char name[AUTHSTORE_NAME_MAX]; } vhost_t;
typedef struct { char name[AUTHSTORE_NAME_MAX]; char hash[AUTHSTORE_HASH_MAX]; uint32_t tags; } user_t;
typedef struct {
    char user[AUTHSTORE_NAME_MAX], vhost[AUTHSTORE_NAME_MAX];
    char conf[AUTHSTORE_REGEX_MAX], wr[AUTHSTORE_REGEX_MAX], rd[AUTHSTORE_REGEX_MAX];
    regex_t rc, rw, rr;
    int compiled;          /* regexes built (must regfree before overwrite/del) */
} perm_t;

/* Reject over-long inputs instead of letting snprintf() truncate them (see
 * AUTHSTORE_NAME_MAX). `n` includes the NUL terminator. */
static int too_long(const char *s, size_t n)
{
    return s && strlen(s) >= n;
}

struct authstore {
    pthread_rwlock_t lock;
    vhost_t *vhosts; size_t nv, cv;
    user_t  *users;  size_t nu, cu;
    perm_t  *perms;  size_t np, cp;
};

authstore_t *authstore_new(void)
{
    authstore_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    pthread_rwlock_init(&s->lock, NULL);
    return s;
}

static void perm_free_regex(perm_t *p)
{
    if (p->compiled) { regfree(&p->rc); regfree(&p->rw); regfree(&p->rr); p->compiled = 0; }
}

void authstore_free(authstore_t *s)
{
    if (!s) return;
    for (size_t i = 0; i < s->np; i++) perm_free_regex(&s->perms[i]);
    free(s->vhosts); free(s->users); free(s->perms);
    pthread_rwlock_destroy(&s->lock);
    free(s);
}

#define GROW(arr, n, cap) do { \
    if ((n) == (cap)) { size_t _nc = (cap) ? (cap)*2 : 8; \
        void *_p = realloc((arr), _nc * sizeof(*(arr))); \
        if (!_p) return -1; \
        (arr) = _p; (cap) = _nc; } } while (0)

static size_t find_vhost(authstore_t *s, const char *v)
{
    for (size_t i = 0; i < s->nv; i++) if (!strcmp(s->vhosts[i].name, v)) return i;
    return (size_t)-1;
}
static size_t find_user(authstore_t *s, const char *u)
{
    for (size_t i = 0; i < s->nu; i++) if (!strcmp(s->users[i].name, u)) return i;
    return (size_t)-1;
}
static size_t find_perm(authstore_t *s, const char *u, const char *v)
{
    for (size_t i = 0; i < s->np; i++)
        if (!strcmp(s->perms[i].user, u) && !strcmp(s->perms[i].vhost, v)) return i;
    return (size_t)-1;
}

/* Remove every perm row for (user==u or vhost==v); pass NULL to ignore a field. */
static void drop_perms_matching(authstore_t *s, const char *u, const char *v)
{
    for (size_t i = 0; i < s->np; ) {
        if ((u && !strcmp(s->perms[i].user, u)) ||
            (v && !strcmp(s->perms[i].vhost, v))) {
            perm_free_regex(&s->perms[i]);
            s->perms[i] = s->perms[--s->np];
        } else i++;
    }
}

int authstore_add_vhost(authstore_t *s, const char *vhost)
{
    if (too_long(vhost, AUTHSTORE_NAME_MAX)) return -1;
    pthread_rwlock_wrlock(&s->lock);
    int rc = 0;
    if (find_vhost(s, vhost) == (size_t)-1) {
        GROW(s->vhosts, s->nv, s->cv);
        snprintf(s->vhosts[s->nv].name, sizeof(s->vhosts[s->nv].name), "%s", vhost);
        s->nv++;
    }
    pthread_rwlock_unlock(&s->lock);
    return rc;
}

int authstore_del_vhost(authstore_t *s, const char *vhost)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_vhost(s, vhost);
    if (i != (size_t)-1) { s->vhosts[i] = s->vhosts[--s->nv]; drop_perms_matching(s, NULL, vhost); }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int authstore_add_user(authstore_t *s, const char *user,
                       const char *pass_hash, uint32_t tags)
{
    if (too_long(user, AUTHSTORE_NAME_MAX) ||
        too_long(pass_hash, AUTHSTORE_HASH_MAX)) return -1;
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_user(s, user);
    if (i == (size_t)-1) { GROW(s->users, s->nu, s->cu); i = s->nu++; }
    snprintf(s->users[i].name, sizeof(s->users[i].name), "%s", user);
    snprintf(s->users[i].hash, sizeof(s->users[i].hash), "%s", pass_hash);
    s->users[i].tags = tags;
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int authstore_del_user(authstore_t *s, const char *user)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_user(s, user);
    if (i != (size_t)-1) { s->users[i] = s->users[--s->nu]; drop_perms_matching(s, user, NULL); }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int authstore_set_perm(authstore_t *s, const char *user, const char *vhost,
                       const char *configure, const char *write, const char *read)
{
    /* Reject over-long inputs up front so no row is created from truncated
     * names/patterns (which would diverge from the caller's intent). */
    if (too_long(user, AUTHSTORE_NAME_MAX) || too_long(vhost, AUTHSTORE_NAME_MAX) ||
        too_long(configure, AUTHSTORE_REGEX_MAX) || too_long(write, AUTHSTORE_REGEX_MAX) ||
        too_long(read, AUTHSTORE_REGEX_MAX))
        return -1;
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_perm(s, user, vhost);
    if (i == (size_t)-1) { GROW(s->perms, s->np, s->cp); i = s->np++;
                           memset(&s->perms[i], 0, sizeof(s->perms[i])); }
    perm_t *p = &s->perms[i];
    perm_free_regex(p);
    snprintf(p->user, sizeof p->user, "%s", user);
    snprintf(p->vhost, sizeof p->vhost, "%s", vhost);
    snprintf(p->conf, sizeof p->conf, "%s", configure);
    snprintf(p->wr,   sizeof p->wr,   "%s", write);
    snprintf(p->rd,   sizeof p->rd,   "%s", read);
    /* Compile non-empty patterns; an empty pattern stays "deny" (no regex).
     * Track which regexes actually compiled so that, if a later one fails, the
     * earlier successful compiles are freed instead of leaking (they would
     * otherwise never be regfree'd, since p->compiled stays 0). */
    int ok = 1, rc_ok = 0, rw_ok = 0, rr_ok = 0;
    if (p->conf[0]) { if (regcomp(&p->rc, p->conf, REG_EXTENDED | REG_NOSUB)) ok = 0; else rc_ok = 1; }
    if (ok && p->wr[0]) { if (regcomp(&p->rw, p->wr, REG_EXTENDED | REG_NOSUB)) ok = 0; else rw_ok = 1; }
    if (ok && p->rd[0]) { if (regcomp(&p->rr, p->rd, REG_EXTENDED | REG_NOSUB)) ok = 0; else rr_ok = 1; }
    if (!ok) {
        if (rc_ok) regfree(&p->rc);
        if (rw_ok) regfree(&p->rw);
        if (rr_ok) regfree(&p->rr);
    }
    p->compiled = ok;  /* if a pattern was malformed, treat the row as deny-all */
    pthread_rwlock_unlock(&s->lock);
    return ok ? 0 : -1;
}

int authstore_clear_perm(authstore_t *s, const char *user, const char *vhost)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_perm(s, user, vhost);
    if (i != (size_t)-1) { perm_free_regex(&s->perms[i]); s->perms[i] = s->perms[--s->np]; }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

/* ---- queries ------------------------------------------------------------- */

int authstore_is_open(authstore_t *s)
{
    pthread_rwlock_rdlock(&s->lock);
    int open = (s->nu == 0);
    pthread_rwlock_unlock(&s->lock);
    return open;
}

int authstore_vhost_exists(authstore_t *s, const char *vhost)
{
    pthread_rwlock_rdlock(&s->lock);
    int e = find_vhost(s, vhost) != (size_t)-1;
    pthread_rwlock_unlock(&s->lock);
    return e;
}

int authstore_can_access_vhost(authstore_t *s, const char *user, const char *vhost)
{
    pthread_rwlock_rdlock(&s->lock);
    int ok = find_perm(s, user, vhost) != (size_t)-1;
    pthread_rwlock_unlock(&s->lock);
    return ok;
}

int authstore_verify(authstore_t *s, const char *user, const char *password)
{
    pthread_rwlock_rdlock(&s->lock);
    size_t i = find_user(s, user);
    int ok = (i != (size_t)-1) && hash_matches(s->users[i].hash, password);
    pthread_rwlock_unlock(&s->lock);
    return ok;
}

int authstore_lookup_hash(authstore_t *s, const char *user, char *out, size_t cap)
{
    if (!out || cap == 0)
        return 0;
    out[0] = '\0';
    pthread_rwlock_rdlock(&s->lock);
    size_t i = find_user(s, user);
    int found = 0;
    if (i != (size_t)-1 && strlen(s->users[i].hash) < cap) {
        memcpy(out, s->users[i].hash, strlen(s->users[i].hash) + 1);
        found = 1;
    }
    pthread_rwlock_unlock(&s->lock);
    return found;
}

int authstore_password_matches(const char *stored_hash, const char *password)
{
    /* Pure/reentrant: no store, no lock - safe to run on a worker thread. */
    if (!stored_hash || !stored_hash[0] || !password)
        return 0;
    return hash_matches(stored_hash, password);
}

uint32_t authstore_user_tags(authstore_t *s, const char *user)
{
    pthread_rwlock_rdlock(&s->lock);
    size_t i = find_user(s, user);
    uint32_t t = (i != (size_t)-1) ? s->users[i].tags : 0;
    pthread_rwlock_unlock(&s->lock);
    return t;
}

int authstore_check(authstore_t *s, const char *user, const char *vhost,
                    auth_perm_t kind, const char *object)
{
    pthread_rwlock_rdlock(&s->lock);
    int ok = 0;
    size_t i = find_perm(s, user, vhost);
    if (i != (size_t)-1 && s->perms[i].compiled) {
        perm_t *p = &s->perms[i];
        const char *pat = kind == AUTH_CONFIGURE ? p->conf
                        : kind == AUTH_WRITE     ? p->wr : p->rd;
        regex_t *re = kind == AUTH_CONFIGURE ? &p->rc
                    : kind == AUTH_WRITE     ? &p->rw : &p->rr;
        if (pat[0])                       /* empty pattern = deny */
            ok = regexec(re, object, 0, NULL, 0) == 0;
    }
    pthread_rwlock_unlock(&s->lock);
    return ok;
}

/* ---- enumeration --------------------------------------------------------- */

void authstore_foreach_vhost(authstore_t *s, authstore_vhost_fn fn, void *ctx)
{
    pthread_rwlock_rdlock(&s->lock);
    for (size_t i = 0; i < s->nv; i++) fn(s->vhosts[i].name, ctx);
    pthread_rwlock_unlock(&s->lock);
}
void authstore_foreach_user(authstore_t *s, authstore_user_fn fn, void *ctx)
{
    pthread_rwlock_rdlock(&s->lock);
    for (size_t i = 0; i < s->nu; i++) fn(s->users[i].name, s->users[i].tags, ctx);
    pthread_rwlock_unlock(&s->lock);
}
void authstore_foreach_perm(authstore_t *s, authstore_perm_fn fn, void *ctx)
{
    pthread_rwlock_rdlock(&s->lock);
    for (size_t i = 0; i < s->np; i++)
        fn(s->perms[i].user, s->perms[i].vhost, s->perms[i].conf,
           s->perms[i].wr, s->perms[i].rd, ctx);
    pthread_rwlock_unlock(&s->lock);
}
