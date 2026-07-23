/*
 * crypto.h - small, dependency-free primitives shared by authstore.c
 * (password hashing) and cluster.c (mesh peer authentication).
 */
#ifndef BEAVER_CRYPTO_H
#define BEAVER_CRYPTO_H

#include <stddef.h>
#include <stdint.h>

/* SHA-256 (FIPS 180-4) over a single buffer. */
void crypto_sha256(const uint8_t *data, size_t len, uint8_t out[32]);

/* HMAC-SHA256 (RFC 2104). msglen must be <= 64 (all current callers pass a
 * short fixed-format message: salt+counter, a 32-byte U, or a small
 * authentication tag input). */
void crypto_hmac_sha256(const uint8_t *key, size_t keylen,
                        const uint8_t *msg, size_t msglen, uint8_t out[32]);

/* Constant-time comparison: no early exit, so a byte-by-byte timing probe
 * learns nothing about how much of the buffer matched. Returns 0 if equal. */
int crypto_ct_memcmp(const void *a, const void *b, size_t n);

/* Fill `n` bytes from a CSPRNG (/dev/urandom). Returns 0 on success, -1 if no
 * such source was available - callers must treat that as a hard failure
 * (salts, tokens, keys), never fall back to a non-cryptographic PRNG. */
int crypto_random_bytes(uint8_t *out, size_t n);

#endif
