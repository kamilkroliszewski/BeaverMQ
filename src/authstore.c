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

#include <pthread.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

/* ---- SHA-256 (FIPS 180-4) ------------------------------------------------ */

typedef struct { uint32_t s[8]; uint64_t len; uint8_t buf[64]; size_t n; } sha256_t;

static uint32_t ror(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

static void sha256_block(sha256_t *c, const uint8_t *p)
{
    static const uint32_t K[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2 };
    uint32_t w[64], a,b,cc,d,e,f,g,h;
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t)p[i*4]<<24 | (uint32_t)p[i*4+1]<<16 | (uint32_t)p[i*4+2]<<8 | p[i*4+3];
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = ror(w[i-15],7)^ror(w[i-15],18)^(w[i-15]>>3);
        uint32_t s1 = ror(w[i-2],17)^ror(w[i-2],19)^(w[i-2]>>10);
        w[i] = w[i-16]+s0+w[i-7]+s1;
    }
    a=c->s[0];b=c->s[1];cc=c->s[2];d=c->s[3];e=c->s[4];f=c->s[5];g=c->s[6];h=c->s[7];
    for (int i = 0; i < 64; i++) {
        uint32_t S1=ror(e,6)^ror(e,11)^ror(e,25), ch=(e&f)^(~e&g);
        uint32_t t1=h+S1+ch+K[i]+w[i];
        uint32_t S0=ror(a,2)^ror(a,13)^ror(a,22), maj=(a&b)^(a&cc)^(b&cc);
        uint32_t t2=S0+maj;
        h=g;g=f;f=e;e=d+t1;d=cc;cc=b;b=a;a=t1+t2;
    }
    c->s[0]+=a;c->s[1]+=b;c->s[2]+=cc;c->s[3]+=d;c->s[4]+=e;c->s[5]+=f;c->s[6]+=g;c->s[7]+=h;
}

static void sha256(const uint8_t *data, size_t len, uint8_t out[32])
{
    sha256_t c = { {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
                    0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19}, 0, {0}, 0 };
    c.len = len;
    while (len >= 64) { sha256_block(&c, data); data += 64; len -= 64; }
    uint8_t tail[128]; size_t t = 0;
    memcpy(tail, data, len); t = len;
    tail[t++] = 0x80;
    size_t pad = (t <= 56) ? (56 - t) : (120 - t);
    memset(tail + t, 0, pad); t += pad;
    uint64_t bits = c.len * 8;
    for (int i = 0; i < 8; i++) tail[t++] = (uint8_t)(bits >> (56 - i*8));
    for (size_t o = 0; o < t; o += 64) sha256_block(&c, tail + o);
    for (int i = 0; i < 8; i++) {
        out[i*4]   = (uint8_t)(c.s[i] >> 24); out[i*4+1] = (uint8_t)(c.s[i] >> 16);
        out[i*4+2] = (uint8_t)(c.s[i] >> 8);  out[i*4+3] = (uint8_t)c.s[i];
    }
}

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

/* Constant-time comparison: no early exit, so a byte-by-byte timing probe
 * learns nothing about how much of the digest matched. Returns 0 if equal. */
static int ct_memcmp(const void *a, const void *b, size_t n)
{
    const uint8_t *x = a, *y = b;
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++)
        d |= (uint8_t)(x[i] ^ y[i]);
    return d;
}

/* ---- HMAC-SHA256 / PBKDF2 (RFC 2104 / RFC 8018) --------------------------- */

/* msglen must be <= 64 (all our uses: salt+counter = 20, or a 32-byte U). */
static void hmac_sha256(const uint8_t *key, size_t keylen,
                        const uint8_t *msg, size_t msglen, uint8_t out[32])
{
    uint8_t k[64] = {0}, buf[64 + 64], dig[32];
    if (keylen > 64) { sha256(key, keylen, dig); memcpy(k, dig, 32); }
    else memcpy(k, key, keylen);
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(k[i] ^ 0x36); /* ipad */
    memcpy(buf + 64, msg, msglen);
    sha256(buf, 64 + msglen, dig);
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(k[i] ^ 0x5c); /* opad */
    memcpy(buf + 64, dig, 32);
    sha256(buf, 64 + 32, out);
}

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
    hmac_sha256(pw, pwlen, msg, saltlen + 4, u);   /* U1 */
    memcpy(out, u, 32);
    for (uint32_t i = 1; i < iters; i++) {
        hmac_sha256(pw, pwlen, u, 32, u);          /* Ui = PRF(P, Ui-1) */
        for (int j = 0; j < 32; j++) out[j] ^= u[j];
    }
}

/*
 * Iteration count: enough to make offline brute-force of a leaked store
 * expensive (10000x the legacy single round), low enough that a login costs
 * only a few ms - auth runs on the worker event loop, so a much higher count
 * would stall other connections on that worker during a login burst.
 */
#define AUTH_PBKDF2_ITERS 10000u

static void read_salt(uint8_t *salt, size_t n)
{
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        ssize_t got = read(fd, salt, n);
        close(fd);
        if (got == (ssize_t)n)
            return;
    }
    for (size_t i = 0; i < n; i++)   /* last-resort fallback */
        salt[i] = (uint8_t)(rand() >> (i & 7));
}

/* Stored form (v2): "$p2$<iters>$<hex salt[16]>$<hex pbkdf2-digest[32]>".
 * Legacy (v1) form hex(salt[4] || sha256(salt||password)) is still VERIFIED
 * (existing stores keep working) but never produced anymore. */
void authstore_hash_password(const char *password, char *out, size_t out_cap)
{
    if (out_cap < AUTHSTORE_HASH_MAX) { if (out_cap) out[0] = '\0'; return; }
    uint8_t salt[16], dig[32];
    read_salt(salt, sizeof salt);
    pbkdf2_sha256(password, salt, sizeof salt, AUTH_PBKDF2_ITERS, dig);
    char sh[33], dh[65];
    to_hex(salt, sizeof salt, sh);
    to_hex(dig, sizeof dig, dh);
    snprintf(out, out_cap, "$p2$%u$%s$%s", AUTH_PBKDF2_ITERS, sh, dh);
}

static int hash_matches(const char *stored, const char *password)
{
    if (strncmp(stored, "$p2$", 4) == 0) {
        char *end = NULL;
        unsigned long iters = strtoul(stored + 4, &end, 10);
        if (!end || *end != '$' || iters == 0 || iters > 10000000ul)
            return 0;
        const char *sh = end + 1;                 /* salt hex, then '$', digest */
        if (strlen(sh) != 32 + 1 + 64 || sh[32] != '$')
            return 0;
        uint8_t salt[16], want[32], dig[32];
        if (from_hex(sh, salt, sizeof salt) != 0 ||
            from_hex(sh + 33, want, sizeof want) != 0)
            return 0;
        pbkdf2_sha256(password, salt, sizeof salt, (uint32_t)iters, dig);
        return ct_memcmp(dig, want, sizeof dig) == 0;
    }

    /* Legacy v1: hex( salt[4] || sha256(salt || password) ) = 72 hex chars. */
    uint8_t blob[36];
    if (strlen(stored) < 72 || from_hex(stored, blob, 36) != 0) return 0;
    size_t pl = strlen(password);
    uint8_t *buf = malloc(4 + pl);
    if (!buf) return 0;
    memcpy(buf, blob, 4); memcpy(buf + 4, password, pl);
    uint8_t dig[32]; sha256(buf, 4 + pl, dig); free(buf);
    return ct_memcmp(dig, blob + 4, 32) == 0;
}

/* ---- store --------------------------------------------------------------- */

typedef struct { char name[128]; } vhost_t;
typedef struct { char name[128]; char hash[AUTHSTORE_HASH_MAX]; uint32_t tags; } user_t;
typedef struct {
    char user[128], vhost[128];
    char conf[256], wr[256], rd[256];
    regex_t rc, rw, rr;
    int compiled;          /* regexes built (must regfree before overwrite/del) */
} perm_t;

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
    /* Compile non-empty patterns; an empty pattern stays "deny" (no regex). */
    int ok = 1;
    if (p->conf[0] && regcomp(&p->rc, p->conf, REG_EXTENDED | REG_NOSUB)) ok = 0;
    if (ok && p->wr[0] && regcomp(&p->rw, p->wr, REG_EXTENDED | REG_NOSUB)) ok = 0;
    if (ok && p->rd[0] && regcomp(&p->rr, p->rd, REG_EXTENDED | REG_NOSUB)) ok = 0;
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
