/*
 * crypto.c - SHA-256 / HMAC-SHA256 (see crypto.h). Extracted from authstore.c
 * so cluster.c can reuse the same primitives for mesh peer authentication
 * instead of duplicating (or worse, skipping) them.
 */
#include "crypto.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

int crypto_random_bytes(uint8_t *out, size_t n)
{
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0)
        return -1;
    ssize_t got = read(fd, out, n);
    close(fd);
    return got == (ssize_t)n ? 0 : -1;
}

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

void crypto_sha256(const uint8_t *data, size_t len, uint8_t out[32])
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

/* ---- HMAC-SHA256 (RFC 2104) ------------------------------------------------ */

void crypto_hmac_sha256(const uint8_t *key, size_t keylen,
                        const uint8_t *msg, size_t msglen, uint8_t out[32])
{
    uint8_t k[64] = {0}, buf[64 + 64], dig[32];
    if (keylen > 64) { crypto_sha256(key, keylen, dig); memcpy(k, dig, 32); }
    else memcpy(k, key, keylen);
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(k[i] ^ 0x36); /* ipad */
    memcpy(buf + 64, msg, msglen);
    crypto_sha256(buf, 64 + msglen, dig);
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(k[i] ^ 0x5c); /* opad */
    memcpy(buf + 64, dig, 32);
    crypto_sha256(buf, 64 + 32, out);
}

int crypto_ct_memcmp(const void *a, const void *b, size_t n)
{
    const uint8_t *x = a, *y = b;
    uint8_t d = 0;
    for (size_t i = 0; i < n; i++)
        d |= (uint8_t)(x[i] ^ y[i]);
    return d;
}
