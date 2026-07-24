/*
 * test_authstore.c - unit tests for the access-control store, focused on the
 * split verification the off-event-loop auth paths rely on
 * (authstore_lookup_hash on the loop + authstore_password_matches off it), plus
 * the input-length rejection and PBKDF2 round-trip.
 */
#include "authstore.h"
#include "test_util.h"

#include <string.h>

static void test_hash_roundtrip_and_split(void)
{
    TEST_SECTION("PBKDF2 hash round-trips via the on-loop/off-loop split");
    authstore_t *s = authstore_new();
    CHECK(s != NULL);

    char hash[AUTHSTORE_HASH_MAX];
    CHECK_EQ(authstore_hash_password("s3cret-pw", hash, sizeof hash), 0);
    CHECK_EQ(authstore_add_user(s, "alice", hash, AUTH_TAG_ADMINISTRATOR), 0);

    /* Step 1 (on the loop): look up the stored hash. */
    char stored[AUTHSTORE_HASH_MAX];
    CHECK_EQ(authstore_lookup_hash(s, "alice", stored, sizeof stored), 1);
    CHECK_STR_EQ(stored, hash);
    /* A missing user reports "not found" and clears the buffer. */
    CHECK_EQ(authstore_lookup_hash(s, "nobody", stored, sizeof stored), 0);
    CHECK_STR_EQ(stored, "");

    /* Step 2 (off the loop): pure verification against the looked-up hash. */
    CHECK_EQ(authstore_password_matches(hash, "s3cret-pw"), 1);
    CHECK_EQ(authstore_password_matches(hash, "wrong-pw"), 0);
    CHECK_EQ(authstore_password_matches("", "s3cret-pw"), 0);
    CHECK_EQ(authstore_password_matches(hash, ""), 0);

    /* The split must agree with the all-in-one authstore_verify(). */
    CHECK_EQ(authstore_verify(s, "alice", "s3cret-pw"), 1);
    CHECK_EQ(authstore_verify(s, "alice", "wrong-pw"), 0);
    CHECK_EQ(authstore_verify(s, "nobody", "s3cret-pw"), 0);

    authstore_free(s);
}

static void test_length_rejection(void)
{
    TEST_SECTION("over-long names/patterns are rejected, not truncated");
    authstore_t *s = authstore_new();
    CHECK(s != NULL);

    char longname[AUTHSTORE_NAME_MAX + 16];
    memset(longname, 'a', sizeof longname - 1);
    longname[sizeof longname - 1] = '\0';

    char hash[AUTHSTORE_HASH_MAX];
    CHECK_EQ(authstore_hash_password("pw", hash, sizeof hash), 0);
    CHECK_EQ(authstore_add_user(s, longname, hash, 0), -1);   /* rejected */
    CHECK_EQ(authstore_add_vhost(s, longname), -1);           /* rejected */

    /* A short name still works, proving it is the length that was rejected. */
    CHECK_EQ(authstore_add_user(s, "bob", hash, 0), 0);
    CHECK_EQ(authstore_lookup_hash(s, "bob", hash, sizeof hash), 1);

    char longpat[AUTHSTORE_REGEX_MAX + 16];
    memset(longpat, '.', sizeof longpat - 1);
    longpat[sizeof longpat - 1] = '\0';
    CHECK_EQ(authstore_add_vhost(s, "/"), 0);
    CHECK_EQ(authstore_set_perm(s, "bob", "/", longpat, ".*", ".*"), -1);
    CHECK_EQ(authstore_set_perm(s, "bob", "/", ".*", ".*", ".*"), 0);

    authstore_free(s);
}

int main(void)
{
    test_hash_roundtrip_and_split();
    test_length_rejection();
    return test_summary("test_authstore");
}
