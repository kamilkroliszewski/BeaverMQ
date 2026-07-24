/*
 * faultlib.c - an LD_PRELOAD fault-injection shim for the storage layer, used
 * by tests/test_fault_storage.sh to prove the WAL/snapshot fail-stop paths
 * (audit P0: persist_append / persist_write_flush / persist_sync / snapshot
 * fsync + rename) actually mark the node storage_failed on an I/O error.
 *
 * It is DELIBERATELY SCOPED to the cluster persistence files: it only fails
 * calls on file descriptors that were open()ed on a path containing the
 * marker substring (default "cluster-"), and rename() only when a path
 * contains it. Socket, log and every other I/O is passed straight through, so
 * injecting a fault can't take down the whole process by accident.
 *
 * Controlled by environment variables (unset = never fail that call):
 *   FAULT_MARKER          path substring that marks a target file (def "cluster-")
 *   FAULT_FSYNC_AFTER=N   fail fsync/fdatasync on a target after N successes
 *   FAULT_WRITE_AFTER=N   fail write/pwrite on a target after N successes
 *   FAULT_TRUNCATE_AFTER=N fail ftruncate on a target after N successes
 *   FAULT_RENAME_AFTER=N  fail rename touching a target after N successes
 *
 * Build: cc -shared -fPIC -D_GNU_SOURCE tests/faultlib.c -ldl -o build/faultlib.so
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#define MAX_FDS 4096

static char           g_marker[64] = "cluster-";
static int            g_fsync_after = -1, g_write_after = -1;
static int            g_trunc_after = -1, g_rename_after = -1;
static atomic_int     g_fsync_n, g_write_n, g_trunc_n, g_rename_n;
static atomic_char    g_tracked[MAX_FDS]; /* 1 == fd is a target file */

static int   (*real_open)(const char *, int, ...);
static int   (*real_open64)(const char *, int, ...);
static int   (*real_openat)(int, const char *, int, ...);
static int   (*real_close)(int);
static int   (*real_fsync)(int);
static int   (*real_fdatasync)(int);
static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static int   (*real_ftruncate)(int, off_t);
static int   (*real_rename)(const char *, const char *);

static int env_after(const char *name)
{
    const char *v = getenv(name);
    return v ? atoi(v) : -1;
}

__attribute__((constructor))
static void faultlib_init(void)
{
    real_open      = dlsym(RTLD_NEXT, "open");
    real_open64    = dlsym(RTLD_NEXT, "open64");
    real_openat    = dlsym(RTLD_NEXT, "openat");
    real_close     = dlsym(RTLD_NEXT, "close");
    real_fsync     = dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_write     = dlsym(RTLD_NEXT, "write");
    real_pwrite    = dlsym(RTLD_NEXT, "pwrite");
    real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");
    real_rename    = dlsym(RTLD_NEXT, "rename");

    const char *m = getenv("FAULT_MARKER");
    if (m && *m) { strncpy(g_marker, m, sizeof g_marker - 1); g_marker[sizeof g_marker - 1] = '\0'; }
    g_fsync_after  = env_after("FAULT_FSYNC_AFTER");
    g_write_after  = env_after("FAULT_WRITE_AFTER");
    g_trunc_after  = env_after("FAULT_TRUNCATE_AFTER");
    g_rename_after = env_after("FAULT_RENAME_AFTER");
}

static int is_target_path(const char *path)
{
    return path && strstr(path, g_marker) != NULL;
}
static void track(int fd, const char *path)
{
    if (fd >= 0 && fd < MAX_FDS)
        atomic_store(&g_tracked[fd], is_target_path(path) ? 1 : 0);
}
static int is_tracked(int fd)
{
    return fd >= 0 && fd < MAX_FDS && atomic_load(&g_tracked[fd]);
}

/* Should this call fail? `after` is the configured threshold (-1 = disabled),
 * `counter` the per-call success count. Fails once counter >= after. */
static int should_fail(int after, atomic_int *counter)
{
    if (after < 0)
        return 0;
    return atomic_fetch_add(counter, 1) >= after;
}

int open(const char *path, int flags, ...)
{
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    int fd = real_open(path, flags, mode);
    track(fd, path);
    return fd;
}
int open64(const char *path, int flags, ...)
{
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    int fd = real_open64 ? real_open64(path, flags, mode) : real_open(path, flags, mode);
    track(fd, path);
    return fd;
}
int openat(int dirfd, const char *path, int flags, ...)
{
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    int fd = real_openat(dirfd, path, flags, mode);
    track(fd, path);
    return fd;
}
int close(int fd)
{
    if (fd >= 0 && fd < MAX_FDS)
        atomic_store(&g_tracked[fd], 0);
    return real_close(fd);
}

int fsync(int fd)
{
    if (is_tracked(fd) && should_fail(g_fsync_after, &g_fsync_n)) { errno = EIO; return -1; }
    return real_fsync(fd);
}
int fdatasync(int fd)
{
    if (is_tracked(fd) && should_fail(g_fsync_after, &g_fsync_n)) { errno = EIO; return -1; }
    return real_fdatasync(fd);
}
ssize_t write(int fd, const void *buf, size_t n)
{
    if (is_tracked(fd) && should_fail(g_write_after, &g_write_n)) { errno = EIO; return -1; }
    return real_write(fd, buf, n);
}
ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
    if (is_tracked(fd) && should_fail(g_write_after, &g_write_n)) { errno = EIO; return -1; }
    return real_pwrite(fd, buf, n, off);
}
int ftruncate(int fd, off_t len)
{
    if (is_tracked(fd) && should_fail(g_trunc_after, &g_trunc_n)) { errno = EIO; return -1; }
    return real_ftruncate(fd, len);
}
int rename(const char *oldp, const char *newp)
{
    if ((is_target_path(oldp) || is_target_path(newp)) &&
        should_fail(g_rename_after, &g_rename_n)) { errno = EIO; return -1; }
    return real_rename(oldp, newp);
}
