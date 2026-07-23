/*
 * test_logger.c - Unit and concurrency test for src/logger.c.
 *
 * The concurrency test spins up several threads hammering LOG_*() while
 * another thread flips the minimum level via log_set_level()/log_get_level().
 * This is the scenario that exercises g_min_level's fast-path read in
 * log_log(); run under `make tsan` it must complete without any
 * ThreadSanitizer data-race report.
 */
#include "logger.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>

#define NUM_LOGGER_THREADS 8
#define NUM_ITERATIONS     5000

static void *logger_thread(void *arg)
{
    (void)arg;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        LOG_DEBUG("worker message %d", i);
        LOG_INFO("worker message %d", i);
        LOG_WARN("worker message %d", i);
    }
    return NULL;
}

static void *level_flipper_thread(void *arg)
{
    (void)arg;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        log_set_level(i % 2 == 0 ? LOG_LEVEL_DEBUG : LOG_LEVEL_ERROR);
        (void)log_get_level();
    }
    return NULL;
}

static void test_level_filtering(FILE *devnull)
{
    log_init(LOG_LEVEL_WARN, devnull);
    assert(log_get_level() == LOG_LEVEL_WARN);

    log_set_level(LOG_LEVEL_ERROR);
    assert(log_get_level() == LOG_LEVEL_ERROR);

    log_set_level(LOG_LEVEL_DEBUG);
    assert(log_get_level() == LOG_LEVEL_DEBUG);
}

static void test_concurrent_logging(FILE *devnull)
{
    log_init(LOG_LEVEL_INFO, devnull);

    pthread_t loggers[NUM_LOGGER_THREADS];
    pthread_t flipper;

    for (int i = 0; i < NUM_LOGGER_THREADS; i++)
        assert(pthread_create(&loggers[i], NULL, logger_thread, NULL) == 0);
    assert(pthread_create(&flipper, NULL, level_flipper_thread, NULL) == 0);

    for (int i = 0; i < NUM_LOGGER_THREADS; i++)
        pthread_join(loggers[i], NULL);
    pthread_join(flipper, NULL);

    log_shutdown();
}

int main(void)
{
    FILE *devnull = fopen("/dev/null", "w");
    assert(devnull != NULL);

    test_level_filtering(devnull);
    test_concurrent_logging(devnull);

    fclose(devnull);

    printf("test_logger: all tests passed\n");
    return 0;
}
