/*
 * supervisor.h - process-level Supervisor ("let it crash") for BeaverMQ.
 *
 * The supervisor is a separate, deliberately dumb outer process: it forks and
 * execve()s a fresh copy of the SAME beavermq binary as one whole "worker"
 * process (today's entire multi-threaded broker, unmodified), watches it for
 * crashes (SIGCHLD) and frozen event loops (a heartbeat pipe), and respawns
 * it - with a restart-rate limit so a deterministic startup bug doesn't spin
 * forever - instead of requiring an operator to notice and restart manually.
 *
 * Hard architectural rule, not just a style preference: this file must NEVER
 * #include "broker.h", "dispatch.h", or "cluster.h", and supervisor_main()
 * must never construct any of those types or start a uv_thread_t. fork() in
 * a process with multiple active threads/libuv loops is undefined in the
 * child (only the calling thread is duplicated; other threads' mutex/epoll
 * state is not). The supervisor is therefore single-threaded for its entire
 * lifetime, and the only thing it does between fork() and execve() in the
 * child is close()/fcntl()/build an envp/execve() - nothing that could touch
 * a partially-copied multi-threaded broker.
 *
 * Also do not add uv_spawn()/uv_process_t usage here without first removing
 * the manual SIGCHLD + waitpid(-1, ...) reaping loop: libuv's own SIGCHLD
 * handling does per-child waitpid(pid, ..., WNOHANG) for its own uv_process_t
 * handles, and a competing waitpid(-1, ...) can "steal" the reap before libuv
 * sees it, silently breaking its exit_cb.
 */
#ifndef BEAVERMQ_SUPERVISOR_H
#define BEAVERMQ_SUPERVISOR_H

#include <stdint.h>
#include <sys/types.h> /* pid_t */
#include <uv.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Ring buffer size for restart timestamps; must be >= max_restarts with some
 * margin (we prune old entries lazily, not eagerly). */
#define SUPERVISOR_MAX_RESTART_SAMPLES 16

typedef struct {
    int      max_restarts;            /* BEAVERMQ_SUPERVISOR_MAX_RESTARTS, default 5 */
    uint64_t restart_window_ms;       /* BEAVERMQ_SUPERVISOR_RESTART_WINDOW_MS, 10000 */
    uint64_t shutdown_timeout_ms;     /* BEAVERMQ_SUPERVISOR_SHUTDOWN_TIMEOUT_MS, 5000 */
    uint64_t heartbeat_interval_ms;   /* BEAVERMQ_SUPERVISOR_HEARTBEAT_MS, 2000 */
    uint64_t heartbeat_timeout_ms;    /* BEAVERMQ_SUPERVISOR_HEARTBEAT_TIMEOUT_MS, 6000 */
    int      nworkers;                /* BEAVERMQ_SUPERVISOR_WORKERS, default 1 */
    char     data_dir[256];           /* for supervisor.pid/worker-<i>.pid; "" = no pid files */
} supervisor_config_t;

typedef enum {
    WORKER_STARTING,  /* forked, exec'd, no heartbeat received yet */
    WORKER_RUNNING,   /* at least one heartbeat received */
    WORKER_STOPPING,  /* SIGTERM sent, waiting for SIGCHLD (graceful shutdown) */
    WORKER_DEAD,      /* reaped; either about to be respawned or given up on */
} worker_state_t;

typedef struct supervisor supervisor_t;

typedef struct supervised_worker {
    int             index;               /* 0..nworkers-1; used for worker-<i>.pid naming */
    pid_t           pid;                 /* -1 when no live process */
    worker_state_t  state;

    int             heartbeat_read_fd;   /* -1 once closed */
    uv_poll_t       heartbeat_poll;      /* polls heartbeat_read_fd for readability */
    int             heartbeat_timer_armed; /* timer starts on the FIRST byte received,
                                            * not at fork time - see supervisor.c for why */
    uv_timer_t      heartbeat_timeout_timer;
    uint64_t        last_heartbeat_ms;

    uint64_t        restart_times_ms[SUPERVISOR_MAX_RESTART_SAMPLES]; /* ring buffer */
    int             restart_count;       /* entries currently valid (<= MAX_RESTART_SAMPLES) */
    int             restart_head;        /* next slot to write */
    int             gave_up;             /* 1 once the restart-rate limit was exceeded, or a
                                          * respawn attempt itself failed (fork/pipe/exec) */

    /* Teardown bookkeeping: uv_close() is asynchronous, so a respawn (or the
     * final "is everything torn down" check during shutdown/give-up) must
     * wait for BOTH handles' close callbacks before reusing this slot. */
    int             pending_respawn;     /* decided in on_sigchld, acted on once closed */
    int             closing_handles;     /* counts down 2 -> 0 as poll/timer close */

    supervisor_t   *sup;                 /* back-pointer for libuv callbacks */
} supervised_worker_t;

struct supervisor {
    supervisor_config_t   cfg;
    uv_loop_t              loop;
    uv_signal_t            sig_term, sig_int, sig_chld;
    supervised_worker_t   *workers;      /* [cfg.nworkers] */
    int                    shutting_down; /* blocks respawn once set */
    uv_timer_t             shutdown_deadline_timer;
    int                    exit_code;

    char                   self_exe[4096]; /* /proc/self/exe, fallback argv[0] */
    int                    child_argc;
    char                 **child_argv;     /* argv to execve() into each worker (NULL-terminated,
                                            * "--supervisor" already stripped by main.c) */
};

/*
 * Fill `cfg` from BEAVERMQ_SUPERVISOR_* environment variables, with the
 * defaults documented above for anything unset or invalid (invalid values are
 * logged as a warning and replaced with the default - this never fails).
 * `data_dir_hint`/`cluster_enabled_hint` come from resolving the broker's own
 * config (see config_find_file/config_load_file/config_apply_env in
 * config.h) so pid files land next to the WAL and so nworkers > 1 without
 * clustering can be rejected (see supervisor_main).
 */
void supervisor_config_from_env(supervisor_config_t *cfg, const char *data_dir_hint,
                                int cluster_enabled_hint);

/*
 * Entry point for `beavermq --supervisor ...` / BEAVERMQ_SUPERVISOR=on. Never
 * runs the broker itself - only forks/execve()s copies of the current binary
 * (resolved via /proc/self/exe) with `argv` (already stripped of
 * "--supervisor") as each worker's arguments, and supervises them for the
 * life of the process. Returns the process exit code.
 */
int supervisor_main(int argc, char **argv);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_SUPERVISOR_H */
