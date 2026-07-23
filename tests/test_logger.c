/*
 * test_logger.c - unit tests for src/logger.c: leveled filtering, and a
 * concurrency stress test verifying that log lines from many threads never
 * interleave/tear each other (the whole point of serializing every write
 * under one lock - see the design note at the top of logger.c).
 */
#include "logger.h"
#include "test_util.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

static void test_level_name(void)
{
    TEST_SECTION("log_level_name()");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_DEBUG), "DEBUG");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_INFO),  "INFO");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_WARN),  "WARN");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_ERROR), "ERROR");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_FATAL), "FATAL");
    CHECK_STR_EQ(log_level_name(LOG_LEVEL_NONE),  "NONE");
    CHECK_STR_EQ(log_level_name((log_level_t)999), "?????");
}

/* Count lines in `f` (from the current position to EOF) whose message part
 * (everything after the last "] ") starts with `needle`. Leaves the file
 * position at EOF. */
static int count_lines_containing(FILE *f, const char *needle)
{
    char line[4096];
    int count = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, needle))
            count++;
    }
    return count;
}

static void test_level_filtering(void)
{
    TEST_SECTION("log_set_level() filters below the threshold, both API and env-set state");
    FILE *f = tmpfile();
    CHECK(f != NULL);
    if (!f)
        return;

    log_init(LOG_LEVEL_WARN, f);
    CHECK_EQ(log_get_level(), LOG_LEVEL_WARN);
    log_set_color_mode(0); /* force plain output regardless of isatty() */

    LOG_DEBUG("should be filtered out (debug)");
    LOG_INFO("should be filtered out (info)");
    LOG_WARN("__MARK_WARN__ should appear");
    LOG_ERROR("__MARK_ERROR__ should appear");
    fflush(f);

    rewind(f);
    CHECK_EQ(count_lines_containing(f, "should be filtered out"), 0);
    rewind(f);
    CHECK_EQ(count_lines_containing(f, "__MARK_WARN__"), 1);
    rewind(f);
    CHECK_EQ(count_lines_containing(f, "__MARK_ERROR__"), 1);

    log_set_level(LOG_LEVEL_DEBUG);
    CHECK_EQ(log_get_level(), LOG_LEVEL_DEBUG);
    LOG_DEBUG("__MARK_DEBUG2__ now visible");
    fflush(f);
    rewind(f);
    CHECK_EQ(count_lines_containing(f, "__MARK_DEBUG2__"), 1);

    fclose(f);
}

/* ---- concurrency: no torn/interleaved lines ------------------------------ */

#define NUM_THREADS       8
#define LINES_PER_THREAD  300
#define MSG_LEN           40

typedef struct {
    int   thread_index; /* 0..NUM_THREADS-1 */
} thread_arg_t;

static void *logger_thread_main(void *arg_)
{
    thread_arg_t *arg = arg_;
    /* Each thread only ever logs a message made of ITS OWN repeated digit -
     * if two threads' writes ever interleaved at the byte level (the bug
     * that per-line locking prevents), a captured line would contain a MIX
     * of digits instead of one uniform run. */
    char digit = (char)('0' + (arg->thread_index % 10));
    char msg[MSG_LEN + 1];
    memset(msg, digit, MSG_LEN);
    msg[MSG_LEN] = '\0';

    for (int i = 0; i < LINES_PER_THREAD; i++)
        LOG_INFO("%s", msg);
    return NULL;
}

/* A line's message part is everything after the last "] " marker (skipping
 * the space), up to the trailing newline. Returns 1 and fills *out_digit if
 * the message is a clean, single-character run of length MSG_LEN; 0 if the
 * line is malformed/torn in any way. */
static int parse_uniform_message(const char *line, char *out_digit)
{
    const char *marker = strrchr(line, ']');
    if (!marker || marker[1] != ' ')
        return 0;
    const char *msg = marker + 2;
    size_t len = strlen(msg);
    if (len > 0 && msg[len - 1] == '\n')
        len--;
    if (len != MSG_LEN)
        return 0;
    char first = msg[0];
    for (size_t i = 1; i < len; i++)
        if (msg[i] != first)
            return 0;
    *out_digit = first;
    return 1;
}

static void test_concurrent_logging_no_torn_lines(void)
{
    TEST_SECTION("many threads logging concurrently: every line is intact, none lost/merged");
    FILE *f = tmpfile();
    CHECK(f != NULL);
    if (!f)
        return;

    log_init(LOG_LEVEL_INFO, f);
    log_set_color_mode(0);

    pthread_t threads[NUM_THREADS];
    thread_arg_t args[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].thread_index = i;
        CHECK(pthread_create(&threads[i], NULL, logger_thread_main, &args[i]) == 0);
    }
    for (int i = 0; i < NUM_THREADS; i++)
        pthread_join(threads[i], NULL);
    log_shutdown(); /* just flushes */

    rewind(f);
    int total_lines = 0, malformed = 0;
    int per_digit_count[10] = {0};
    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        total_lines++;
        char digit;
        if (parse_uniform_message(line, &digit))
            per_digit_count[digit - '0']++;
        else
            malformed++;
    }

    CHECK_EQ(total_lines, NUM_THREADS * LINES_PER_THREAD);
    CHECK_EQ(malformed, 0);
    for (int i = 0; i < NUM_THREADS; i++)
        CHECK_EQ(per_digit_count[i], LINES_PER_THREAD);

    fclose(f);
}

int main(void)
{
    test_level_name();
    test_level_filtering();
    test_concurrent_logging_no_torn_lines();
    return test_summary("test_logger");
}
