/*
 * authlimit.h - process-wide throttle for password verification.
 *
 * Password checks run PBKDF2 (deliberately expensive), so an unauthenticated
 * client that spams logins can burn CPU and brute-force a leaked store. This
 * module bounds both, independently of the AMQP/HTTP auth paths that call it:
 *
 *   - per-source-key backoff: after a burst of failed attempts a key (the
 *     client IP) must wait an exponentially growing delay before its next
 *     attempt is even hashed. Successful auth clears the key.
 *   - a global cap on concurrent hash operations, so a flood from many
 *     distinct IPs still cannot saturate every CPU at once.
 *
 * Keying on the client IP (not the username) is deliberate: a username-keyed
 * lockout would let an attacker lock a victim out by failing that victim's
 * name. All functions are thread-safe (called from several worker loops).
 */
#ifndef BEAVERMQ_AUTHLIMIT_H
#define BEAVERMQ_AUTHLIMIT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Idempotent; safe to call more than once. Not strictly required (state is
 * statically zero-initialized) but seeds the internal hash and marks intent. */
void authlimit_init(void);

/* 0 if an attempt from `key` may proceed now; otherwise the number of
 * milliseconds remaining before it may (the caller denies WITHOUT hashing).
 * `key` is typically the client IP string; NULL/empty is always allowed. */
uint64_t authlimit_retry_after_ms(const char *key, uint64_t now_ms);

/* Record the outcome of a completed attempt, updating the key's backoff. */
void authlimit_record_failure(const char *key, uint64_t now_ms);
void authlimit_record_success(const char *key);

/* Acquire/release one of the limited concurrent-hash slots. begin() returns 0
 * on success (the caller MUST pair it with end()), or -1 if the cap is already
 * reached (the caller denies the login WITHOUT hashing). */
int  authlimit_hash_begin(void);
void authlimit_hash_end(void);

/* Test hook: wipe all backoff state and reset the in-flight counter. */
void authlimit_reset_for_test(void);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_AUTHLIMIT_H */
