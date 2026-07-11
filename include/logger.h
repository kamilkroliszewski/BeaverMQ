/*
 * logger.h - Thread-safe leveled logging utility for BeaverMQ.
 *
 * Provides timestamped, leveled log output (DEBUG/INFO/WARN/ERROR/FATAL).
 * All output is serialized through an internal mutex so it is safe to call
 * from any thread or libuv callback. ANSI colors are emitted automatically
 * when the destination stream is a TTY.
 *
 * Usage:
 *     log_init(LOG_LEVEL_INFO, stderr);   // optional; sane defaults apply
 *     LOG_INFO("listening on port %d", 5672);
 *     log_shutdown();                      // optional; flushes the stream
 */
#ifndef BEAVERMQ_LOGGER_H
#define BEAVERMQ_LOGGER_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Ordered by increasing severity. LOG_LEVEL_NONE disables all output. */
typedef enum {
    LOG_LEVEL_DEBUG = 0,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_NONE
} log_level_t;

/*
 * Initialize the logger. Both arguments are optional in spirit:
 *   - level: minimum severity that will be emitted.
 *   - out:   destination stream; if NULL, stderr is used.
 * Calling this more than once simply re-applies the configuration.
 * The logger is also usable without calling this (defaults: INFO -> stderr).
 */
void log_init(log_level_t level, FILE *out);

/* Change the minimum severity at runtime. Thread-safe. */
void log_set_level(log_level_t level);

/* Return the current minimum severity. Thread-safe. */
log_level_t log_get_level(void);

/*
 * Force-enable (1) or force-disable (0) ANSI color output, or pass -1 to
 * restore automatic detection (color only when the stream is a TTY).
 */
void log_set_color_mode(int mode);

/* Flush the output stream. Safe to call at shutdown. */
void log_shutdown(void);

/* Convert a level to its short uppercase name ("INFO", "WARN", ...). */
const char *log_level_name(log_level_t level);

/*
 * Core logging entry point. Prefer the LOG_* macros below, which capture the
 * source location automatically. The format string is checked by the compiler.
 */
void log_log(log_level_t level, const char *file, int line,
             const char *fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((format(printf, 4, 5)))
#endif
    ;

#define LOG_DEBUG(...) log_log(LOG_LEVEL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  log_log(LOG_LEVEL_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  log_log(LOG_LEVEL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) log_log(LOG_LEVEL_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_FATAL(...) log_log(LOG_LEVEL_FATAL, __FILE__, __LINE__, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_LOGGER_H */
