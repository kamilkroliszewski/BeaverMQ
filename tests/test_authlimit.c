/*
 * test_authlimit.c - unit tests for the login rate limiter / concurrent-hash
 * cap (authlimit.c). Time is passed in explicitly, so backoff behaviour is
 * tested deterministically without sleeping.
 */
#include "authlimit.h"
#include "test_util.h"

/* Must match authlimit.c's policy tunables (kept in sync deliberately - the
 * test documents the contract these constants encode). */
#define FREE_ATTEMPTS 5u
#define BASE_MS       500u
#define MAX_MS        30000u
#define CONCURRENCY   8

static void test_free_attempts_then_backoff(void)
{
    TEST_SECTION("first N failures are free, then exponential backoff kicks in");
    authlimit_reset_for_test();
    const char *ip = "203.0.113.7";
    uint64_t t = 1000;

    /* The first FREE_ATTEMPTS failures impose no delay. */
    for (unsigned i = 0; i < FREE_ATTEMPTS; i++) {
        authlimit_record_failure(ip, t);
        CHECK_EQ(authlimit_retry_after_ms(ip, t), 0);
    }
    /* The next failure (FREE_ATTEMPTS + 1) is the first to impose a delay. */
    authlimit_record_failure(ip, t);
    uint64_t w1 = authlimit_retry_after_ms(ip, t);
    CHECK(w1 > 0);
    CHECK_EQ(w1, BASE_MS);

    authlimit_record_failure(ip, t);           /* one more failure */
    uint64_t w2 = authlimit_retry_after_ms(ip, t);
    CHECK(w2 >= w1);                            /* backoff does not shrink */

    /* Backoff is capped. */
    for (int i = 0; i < 40; i++)
        authlimit_record_failure(ip, t);
    CHECK(authlimit_retry_after_ms(ip, t) <= MAX_MS);
}

static void test_delay_expires_with_time(void)
{
    TEST_SECTION("a throttled key is allowed again once its delay elapses");
    authlimit_reset_for_test();
    const char *ip = "198.51.100.4";
    uint64_t t = 5000;
    for (unsigned i = 0; i <= FREE_ATTEMPTS; i++)
        authlimit_record_failure(ip, t);       /* pushes next_allowed into the future */
    uint64_t wait = authlimit_retry_after_ms(ip, t);
    CHECK(wait > 0);
    /* Before the delay elapses: still blocked. */
    CHECK(authlimit_retry_after_ms(ip, t + wait - 1) > 0);
    /* After it elapses: allowed. */
    CHECK_EQ(authlimit_retry_after_ms(ip, t + wait), 0);
}

static void test_success_clears_backoff(void)
{
    TEST_SECTION("a successful login clears accumulated backoff for that key");
    authlimit_reset_for_test();
    const char *ip = "192.0.2.9";
    uint64_t t = 2000;
    for (unsigned i = 0; i <= FREE_ATTEMPTS + 2; i++)
        authlimit_record_failure(ip, t);
    CHECK(authlimit_retry_after_ms(ip, t) > 0);
    authlimit_record_success(ip);
    CHECK_EQ(authlimit_retry_after_ms(ip, t), 0);
}

static void test_keys_are_independent(void)
{
    TEST_SECTION("throttling one IP does not affect another");
    authlimit_reset_for_test();
    const char *bad = "10.0.0.1", *good = "10.0.0.2";
    uint64_t t = 3000;
    for (unsigned i = 0; i <= FREE_ATTEMPTS + 3; i++)
        authlimit_record_failure(bad, t);
    CHECK(authlimit_retry_after_ms(bad, t) > 0);
    CHECK_EQ(authlimit_retry_after_ms(good, t), 0);
}

static void test_empty_key_never_throttled(void)
{
    TEST_SECTION("an empty/NULL key is always allowed (never throttled)");
    authlimit_reset_for_test();
    for (int i = 0; i < 50; i++)
        authlimit_record_failure("", 1000);
    CHECK_EQ(authlimit_retry_after_ms("", 1000), 0);
    CHECK_EQ(authlimit_retry_after_ms(NULL, 1000), 0);
}

static void test_hash_concurrency_cap(void)
{
    TEST_SECTION("the concurrent-hash cap admits exactly CONCURRENCY slots");
    authlimit_reset_for_test();
    for (int i = 0; i < CONCURRENCY; i++)
        CHECK_EQ(authlimit_hash_begin(), 0);   /* all slots acquired */
    CHECK_EQ(authlimit_hash_begin(), -1);      /* over capacity: denied */
    authlimit_hash_end();                      /* free one */
    CHECK_EQ(authlimit_hash_begin(), 0);       /* now one fits again */
    CHECK_EQ(authlimit_hash_begin(), -1);      /* full once more */
    for (int i = 0; i < CONCURRENCY; i++)
        authlimit_hash_end();                  /* release everything */
    CHECK_EQ(authlimit_hash_begin(), 0);
    authlimit_hash_end();
}

int main(void)
{
    authlimit_init();
    test_free_attempts_then_backoff();
    test_delay_expires_with_time();
    test_success_clears_backoff();
    test_keys_are_independent();
    test_empty_key_never_throttled();
    test_hash_concurrency_cap();
    return test_summary("test_authlimit");
}
