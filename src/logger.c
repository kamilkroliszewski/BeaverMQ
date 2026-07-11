/*
 * logger.c - Implementation of the thread-safe leveled logger.
 *
 * Design notes:
 *   - A single static mutex serializes both the configuration and the actual
 *     write, so log lines from concurrent threads never interleave.
 *   - The mutex uses PTHREAD_MUTEX_INITIALIZER, which means logging works
 *     correctly even before log_init() is ever called.
 *   - Each line is composed into a stack buffer and written with a single
 *     fwrite() under the lock to minimize the critical section and avoid
 *     partial-line interleaving.
 */
#include "logger.h"

#include <pthread.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

/* ANSI color escapes, indexed by log_level_t. */
static const char *const LEVEL_COLORS[] = {
    "\033[36m", /* DEBUG - cyan   */
    "\033[32m", /* INFO  - green  */
    "\033[33m", /* WARN  - yellow */
    "\033[31m", /* ERROR - red    */
    "\033[35m", /* FATAL - magenta*/
};
static const char *const COLOR_RESET = "\033[0m";
static const char *const COLOR_DIM   = "\033[90m"; /* bright black / gray */

static const char *const LEVEL_NAMES[] = {
    "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "NONE"
};

/* Global logger state, guarded by g_lock. */
static pthread_mutex_t g_lock      = PTHREAD_MUTEX_INITIALIZER;
static log_level_t     g_min_level = LOG_LEVEL_INFO;
static FILE           *g_stream    = NULL; /* lazily resolves to stderr */
static int             g_color_mode = -1;  /* -1 auto, 0 off, 1 on */

/* Resolve the active stream; must be called with g_lock held. */
static FILE *active_stream(void)
{
    return g_stream ? g_stream : stderr;
}

/* Decide whether to colorize; must be called with g_lock held. */
static int use_color(FILE *stream)
{
    if (g_color_mode == 0)
        return 0;
    if (g_color_mode == 1)
        return 1;
    return isatty(fileno(stream)); /* auto */
}

/* Strip directory components so logs show "net.c" not "src/net.c". */
static const char *short_file(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

void log_init(log_level_t level, FILE *out)
{
    pthread_mutex_lock(&g_lock);
    g_min_level = level;
    g_stream    = out;
    pthread_mutex_unlock(&g_lock);
}

void log_set_level(log_level_t level)
{
    pthread_mutex_lock(&g_lock);
    g_min_level = level;
    pthread_mutex_unlock(&g_lock);
}

log_level_t log_get_level(void)
{
    pthread_mutex_lock(&g_lock);
    log_level_t lvl = g_min_level;
    pthread_mutex_unlock(&g_lock);
    return lvl;
}

void log_set_color_mode(int mode)
{
    pthread_mutex_lock(&g_lock);
    g_color_mode = mode;
    pthread_mutex_unlock(&g_lock);
}

const char *log_level_name(log_level_t level)
{
    if (level < LOG_LEVEL_DEBUG || level > LOG_LEVEL_NONE)
        return "?????";
    return LEVEL_NAMES[level];
}

void log_shutdown(void)
{
    pthread_mutex_lock(&g_lock);
    fflush(active_stream());
    pthread_mutex_unlock(&g_lock);
}

void log_log(log_level_t level, const char *file, int line,
             const char *fmt, ...)
{
    /* Fast path: bail out before doing any work if the level is filtered.
     * Reading g_min_level without the lock is a benign race: the worst case
     * is a single message emitted or dropped right as the level changes. */
    if (level < g_min_level || level >= LOG_LEVEL_NONE)
        return;

    /* Build the timestamp: "YYYY-MM-DD HH:MM:SS.mmm". */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_buf;
    localtime_r(&tv.tv_sec, &tm_buf);

    char ts[32];
    size_t n = strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);
    snprintf(ts + n, sizeof(ts) - n, ".%03ld", (long)(tv.tv_usec / 1000));

    /* Format the user message into a bounded buffer. */
    char msg[2048];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&g_lock);

    /* Re-check under the lock to honor a concurrent log_set_level(). */
    if (level >= g_min_level && level < LOG_LEVEL_NONE) {
        FILE *stream = active_stream();
        const char *fname = short_file(file);

        if (use_color(stream)) {
            fprintf(stream,
                    "%s%s%s %s%-5s%s %s[%s:%d]%s %s\n",
                    COLOR_DIM, ts, COLOR_RESET,
                    LEVEL_COLORS[level], LEVEL_NAMES[level], COLOR_RESET,
                    COLOR_DIM, fname, line, COLOR_RESET,
                    msg);
        } else {
            fprintf(stream,
                    "%s %-5s [%s:%d] %s\n",
                    ts, LEVEL_NAMES[level], fname, line, msg);
        }
        fflush(stream);
    }

    pthread_mutex_unlock(&g_lock);
}
