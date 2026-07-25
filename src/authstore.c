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
#include "logger.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

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

    /* Local persistence (STANDALONE only; a cluster persists via Raft). When
     * persist_path is set, every mutation atomically rewrites the file. */
    char persist_path[512];
    /* Sticky once the first user is ever created: the first-boot bootstrap
     * window never re-opens just because users were later deleted. */
    int  bootstrap_completed;
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

/* ---- local persistence (standalone) -------------------------------------- *
 * A compact binary file: magic, version, bootstrap flag, then the vhosts, users
 * (name, hash, tags) and permissions (patterns). Written atomically (temp +
 * fsync + rename, 0600) on every mutation; loaded once at startup. A cluster
 * does NOT use this - its authstore is replicated and snapshotted via Raft. */
#define AUTHSTORE_DB_MAGIC   0x41535442u /* 'ASTB' */
#define AUTHSTORE_DB_VERSION 1u

static int put_u32(FILE *f, uint32_t v)
{
    uint8_t b[4] = { (uint8_t)(v>>24), (uint8_t)(v>>16), (uint8_t)(v>>8), (uint8_t)v };
    return fwrite(b, 1, 4, f) == 4 ? 0 : -1;
}
static int put_str(FILE *f, const char *s)
{
    size_t n = strlen(s);
    if (n > 0xffff) return -1;
    uint8_t b[2] = { (uint8_t)(n>>8), (uint8_t)n };
    if (fwrite(b, 1, 2, f) != 2) return -1;
    return (n == 0 || fwrite(s, 1, n, f) == n) ? 0 : -1;
}
static int get_u32(FILE *f, uint32_t *out)
{
    uint8_t b[4];
    if (fread(b, 1, 4, f) != 4) return -1;
    *out = (uint32_t)b[0]<<24 | (uint32_t)b[1]<<16 | (uint32_t)b[2]<<8 | b[3];
    return 0;
}
/* Read a length-prefixed string into `out` (cap includes the NUL). Rejects a
 * field that would not fit (corrupt/hostile file). */
static int get_str(FILE *f, char *out, size_t cap)
{
    uint8_t b[2];
    if (fread(b, 1, 2, f) != 2) return -1;
    size_t n = (size_t)b[0]<<8 | b[1];
    if (n >= cap) return -1;
    if (n && fread(out, 1, n, f) != n) return -1;
    out[n] = '\0';
    return 0;
}

/* Serialize the whole store to persist_path atomically. Caller holds the lock.
 * Best-effort: a write failure is logged, never fatal (the in-memory store
 * stays authoritative). */
static void authstore_save_locked(authstore_t *s)
{
    if (!s->persist_path[0])
        return;
    char tmp[600];
    int n = snprintf(tmp, sizeof tmp, "%s.tmp", s->persist_path);
    if (n < 0 || (size_t)n >= sizeof tmp)
        return;
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600); /* hashes: 0600 */
    if (fd < 0) { LOG_WARN("authstore: cannot write '%s': %s", tmp, strerror(errno)); return; }
    FILE *f = fdopen(fd, "wb");
    if (!f) { close(fd); return; }

    int ok = put_u32(f, AUTHSTORE_DB_MAGIC) == 0 &&
             put_u32(f, AUTHSTORE_DB_VERSION) == 0 &&
             put_u32(f, (uint32_t)s->bootstrap_completed) == 0 &&
             put_u32(f, (uint32_t)s->nv) == 0;
    for (size_t i = 0; ok && i < s->nv; i++) ok = put_str(f, s->vhosts[i].name) == 0;
    ok = ok && put_u32(f, (uint32_t)s->nu) == 0;
    for (size_t i = 0; ok && i < s->nu; i++)
        ok = put_str(f, s->users[i].name) == 0 &&
             put_str(f, s->users[i].hash) == 0 &&
             put_u32(f, s->users[i].tags) == 0;
    ok = ok && put_u32(f, (uint32_t)s->np) == 0;
    for (size_t i = 0; ok && i < s->np; i++)
        ok = put_str(f, s->perms[i].user) == 0 &&
             put_str(f, s->perms[i].vhost) == 0 &&
             put_str(f, s->perms[i].conf) == 0 &&
             put_str(f, s->perms[i].wr) == 0 &&
             put_str(f, s->perms[i].rd) == 0;

    if (fflush(f) != 0 || fsync(fileno(f)) != 0)
        ok = 0;
    fclose(f); /* closes fd */
    if (!ok || rename(tmp, s->persist_path) != 0) {
        LOG_WARN("authstore: failed to persist to '%s'", s->persist_path);
        unlink(tmp);
        return;
    }
    /* Make the rename itself durable. */
    char dir[512];
    snprintf(dir, sizeof dir, "%s", s->persist_path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        int dfd = open(dir[0] ? dir : "/", O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
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
        authstore_save_locked(s);
    }
    pthread_rwlock_unlock(&s->lock);
    return rc;
}

int authstore_del_vhost(authstore_t *s, const char *vhost)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_vhost(s, vhost);
    if (i != (size_t)-1) {
        s->vhosts[i] = s->vhosts[--s->nv];
        drop_perms_matching(s, NULL, vhost);
        authstore_save_locked(s);
    }
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
    s->bootstrap_completed = 1; /* a user now exists: bootstrap never re-opens */
    authstore_save_locked(s);
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

int authstore_del_user(authstore_t *s, const char *user)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_user(s, user);
    if (i != (size_t)-1) {
        s->users[i] = s->users[--s->nu];
        drop_perms_matching(s, user, NULL);
        authstore_save_locked(s);
    }
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
    authstore_save_locked(s);
    pthread_rwlock_unlock(&s->lock);
    return ok ? 0 : -1;
}

int authstore_clear_perm(authstore_t *s, const char *user, const char *vhost)
{
    pthread_rwlock_wrlock(&s->lock);
    size_t i = find_perm(s, user, vhost);
    if (i != (size_t)-1) {
        perm_free_regex(&s->perms[i]);
        s->perms[i] = s->perms[--s->np];
        authstore_save_locked(s);
    }
    pthread_rwlock_unlock(&s->lock);
    return 0;
}

/* ---- queries ------------------------------------------------------------- */

int authstore_is_open(authstore_t *s)
{
    pthread_rwlock_rdlock(&s->lock);
    /* The bootstrap window is open ONLY on a truly fresh store: no users AND no
     * user was ever created (bootstrap_completed is sticky). Deleting the last
     * user therefore does NOT re-open bootstrap - recovery is an explicit,
     * out-of-band action, not something a delete silently re-enables. */
    int open = (s->nu == 0 && !s->bootstrap_completed);
    pthread_rwlock_unlock(&s->lock);
    return open;
}

/* Load a persisted store from `path` and enable auto-save to it (standalone
 * only; a cluster must not call this). If the file does not exist yet, the path
 * is still remembered so future mutations create it. Returns 0 on success (incl.
 * "no file yet"), -1 on a corrupt file (the store is left as-is). */
int authstore_load(authstore_t *s, const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f) {
        /* No store yet: remember the path so the first mutation writes it. */
        pthread_rwlock_wrlock(&s->lock);
        snprintf(s->persist_path, sizeof s->persist_path, "%s", path);
        pthread_rwlock_unlock(&s->lock);
        return 0;
    }

    uint32_t magic = 0, version = 0, boot = 0, n = 0;
    int bad = get_u32(f, &magic) != 0 || magic != AUTHSTORE_DB_MAGIC ||
              get_u32(f, &version) != 0 || version != AUTHSTORE_DB_VERSION ||
              get_u32(f, &boot) != 0;
    char name[AUTHSTORE_NAME_MAX], hash[AUTHSTORE_HASH_MAX];
    char vh[AUTHSTORE_NAME_MAX];
    char cf[AUTHSTORE_REGEX_MAX], wr[AUTHSTORE_REGEX_MAX], rd[AUTHSTORE_REGEX_MAX];

    if (!bad && get_u32(f, &n) == 0) {
        for (uint32_t i = 0; i < n && !bad; i++) {
            if (get_str(f, name, sizeof name) != 0) { bad = 1; break; }
            authstore_add_vhost(s, name); /* persist_path not set yet -> no save */
        }
    } else bad = 1;
    if (!bad && get_u32(f, &n) == 0) {
        for (uint32_t i = 0; i < n && !bad; i++) {
            uint32_t tags = 0;
            if (get_str(f, name, sizeof name) != 0 ||
                get_str(f, hash, sizeof hash) != 0 ||
                get_u32(f, &tags) != 0) { bad = 1; break; }
            authstore_add_user(s, name, hash, tags);
        }
    } else bad = 1;
    if (!bad && get_u32(f, &n) == 0) {
        for (uint32_t i = 0; i < n && !bad; i++) {
            if (get_str(f, name, sizeof name) != 0 ||
                get_str(f, vh, sizeof vh) != 0 ||
                get_str(f, cf, sizeof cf) != 0 ||
                get_str(f, wr, sizeof wr) != 0 ||
                get_str(f, rd, sizeof rd) != 0) { bad = 1; break; }
            authstore_set_perm(s, name, vh, cf, wr, rd);
        }
    } else bad = 1;
    fclose(f);

    if (bad) {
        LOG_ERROR("authstore: '%s' is corrupt or truncated; ignoring it", path);
        return -1;
    }
    /* Restore the sticky bootstrap flag and enable future auto-saves. */
    pthread_rwlock_wrlock(&s->lock);
    if (boot)
        s->bootstrap_completed = 1;
    snprintf(s->persist_path, sizeof s->persist_path, "%s", path);
    pthread_rwlock_unlock(&s->lock);
    LOG_INFO("authstore: loaded %zu user(s), %zu vhost(s), %zu permission(s) from '%s'",
             s->nu, s->nv, s->np, path);
    return 0;
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
