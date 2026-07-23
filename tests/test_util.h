/*
 * test_util.h - minimal, dependency-free assertion macros shared by the C
 * unit tests in tests/ (one .c file per suite). Each test file compiles to
 * its own standalone binary (see the Makefile's per-file pattern rule), so
 * the static counters below are safe: only one translation unit per binary
 * ever includes this header.
 *
 * A test binary's main() calls CHECK/CHECK_EQ/CHECK_STR_EQ as needed and
 * finishes with `return test_summary("suite name");`, which prints a summary
 * and returns 0 (all checks passed) or 1 (at least one failed) - the exit
 * code `make test`'s runner loop relies on to detect a real failure.
 */
#ifndef BEAVERMQ_TEST_UTIL_H
#define BEAVERMQ_TEST_UTIL_H

#include <stdio.h>
#include <string.h>

static int g_test_checks   = 0;
static int g_test_failures = 0;

#define CHECK(cond) do { \
    g_test_checks++; \
    if (!(cond)) { \
        g_test_failures++; \
        fprintf(stderr, "  FAIL %s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); \
    } \
} while (0)

#define CHECK_EQ(a, b) do { \
    g_test_checks++; \
    long long _ca = (long long)(a); \
    long long _cb = (long long)(b); \
    if (_ca != _cb) { \
        g_test_failures++; \
        fprintf(stderr, "  FAIL %s:%d: %s == %s (%lld != %lld)\n", \
                __FILE__, __LINE__, #a, #b, _ca, _cb); \
    } \
} while (0)

#define CHECK_STR_EQ(a, b) do { \
    g_test_checks++; \
    const char *_sa = (a); \
    const char *_sb = (b); \
    if (!_sa || !_sb || strcmp(_sa, _sb) != 0) { \
        g_test_failures++; \
        fprintf(stderr, "  FAIL %s:%d: %s == %s (\"%s\" != \"%s\")\n", \
                __FILE__, __LINE__, #a, #b, _sa ? _sa : "(null)", _sb ? _sb : "(null)"); \
    } \
} while (0)

#define TEST_SECTION(name) fprintf(stderr, "-- %s --\n", (name))

static int test_summary(const char *suite_name)
{
    if (g_test_failures == 0) {
        fprintf(stderr, "%s: %d/%d checks passed\n",
                suite_name, g_test_checks, g_test_checks);
        return 0;
    }
    fprintf(stderr, "%s: %d/%d checks FAILED\n",
            suite_name, g_test_failures, g_test_checks);
    return 1;
}

#endif /* BEAVERMQ_TEST_UTIL_H */
