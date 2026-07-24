/*
 * supervisor.c - process-level Supervisor ("let it crash") for BeaverMQ.
 *
 * See supervisor.h for the full design rationale. Summary of the invariant
 * that must never be violated by future changes to this file: supervisor_main()
 * runs single-threaded for its entire life and never starts a uv_thread_t or
 * constructs a broker/dispatcher/cluster object in ITS OWN process - it only
 * fork()s and execve()s a fresh copy of the current binary as a whole worker.
 * Do not #include "broker.h"/"dispatch.h"/"cluster.h" here.
 *
 * Do not add uv_spawn()/uv_process_t usage without first removing the manual
 * SIGCHLD + waitpid(-1, ...) reaping loop below (see supervisor.h for why the
 * two are mutually exclusive).
 */
#include "supervisor.h"
#include "config.h"
#include "logger.h"
#include "version.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

/* ---- env var parsing (never fails; invalid/missing -> logged + default) -- */

static uint64_t env_u64(const char *name, uint64_t def, uint64_t min, uint64_t max)
{
    const char *v = getenv(name);
    if (!v || !v[0])
        return def;
    errno = 0;
    char *end = NULL;
    unsigned long long n = strtoull(v, &end, 10);
    if (end == v || *end != '\0' || errno != 0 || n < min || n > max) {
        LOG_WARN("supervisor: invalid %s='%s'; using default %llu",
                name, v, (unsigned long long)def);
        return def;
    }
    return (uint64_t)n;
}

static int env_int(const char *name, int def, int min, int max)
{
    return (int)env_u64(name, (uint64_t)def, (uint64_t)min, (uint64_t)max);
}

void supervisor_config_from_env(supervisor_config_t *cfg, const char *data_dir_hint,
                                int cluster_enabled_hint)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->max_restarts = env_int("BEAVERMQ_SUPERVISOR_MAX_RESTARTS", 5, 1,
                                SUPERVISOR_MAX_RESTART_SAMPLES);
    cfg->restart_window_ms = env_u64("BEAVERMQ_SUPERVISOR_RESTART_WINDOW_MS",
                                     10000, 100, 3600000);
    cfg->shutdown_timeout_ms = env_u64("BEAVERMQ_SUPERVISOR_SHUTDOWN_TIMEOUT_MS",
                                       5000, 100, 300000);
    cfg->heartbeat_interval_ms = env_u64("BEAVERMQ_SUPERVISOR_HEARTBEAT_MS",
                                         2000, 100, 300000);
    cfg->heartbeat_timeout_ms = env_u64("BEAVERMQ_SUPERVISOR_HEARTBEAT_TIMEOUT_MS",
                                        6000, 200, 600000);
    cfg->nworkers = env_int("BEAVERMQ_SUPERVISOR_WORKERS", 1, 1, 64);
    (void)cluster_enabled_hint;
    if (cfg->nworkers != 1)
        LOG_WARN("supervisor: BEAVERMQ_SUPERVISOR_WORKERS=%d requested - multiple "
                "worker processes are unsupported (they share one node_id, "
                "data_dir, WAL and port set) and will be rejected at startup",
                cfg->nworkers);
    if (data_dir_hint)
        snprintf(cfg->data_dir, sizeof(cfg->data_dir), "%s", data_dir_hint);
}

/* ---- pid files (best-effort observability; never fatal) ------------------ */

static void write_pidfile(const char *data_dir, const char *name, pid_t pid)
{
    if (!data_dir || !data_dir[0])
        return;
    char final_path[320], tmp_path[336];
    int n = snprintf(final_path, sizeof(final_path), "%s/%s", data_dir, name);
    if (n < 0 || (size_t)n >= sizeof(final_path))
        return;
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_WARN("supervisor: cannot write '%s': %s", tmp_path, strerror(errno));
        return;
    }
    char buf[32];
    int blen = snprintf(buf, sizeof(buf), "%d\n", (int)pid);
    ssize_t wn = write(fd, buf, (size_t)blen);
    close(fd);
    if (wn != blen) {
        LOG_WARN("supervisor: short write to '%s'", tmp_path);
        return;
    }
    /* Atomic replace, same pattern as cluster.c's snapshot .tmp -> final
     * rename: a concurrent reader (e.g. a test script) never observes a
     * partially-written pid file. */
    if (rename(tmp_path, final_path) != 0)
        LOG_WARN("supervisor: cannot rename '%s' -> '%s': %s",
                tmp_path, final_path, strerror(errno));
}

static void worker_pidfile_name(char *out, size_t outsz, int index, int nworkers)
{
    if (nworkers <= 1)
        snprintf(out, outsz, "worker.pid");
    else
        snprintf(out, outsz, "worker-%d.pid", index);
}

/* ---- restart-rate limiting (ring buffer, no allocation in the hot path) -- */

static void record_restart(supervised_worker_t *w, uint64_t now_ms)
{
    w->restart_times_ms[w->restart_head] = now_ms;
    w->restart_head = (w->restart_head + 1) % SUPERVISOR_MAX_RESTART_SAMPLES;
    if (w->restart_count < SUPERVISOR_MAX_RESTART_SAMPLES)
        w->restart_count++;
}

/* Entries are inserted in monotonic time order, so walking backward from the
 * most recently written one, the first entry older than the window means
 * every entry before it is even older - safe to stop early. */
static int restart_rate_exceeded(supervised_worker_t *w, uint64_t window_ms,
                                 int max_restarts, uint64_t now_ms)
{
    int count_in_window = 0;
    for (int i = 0; i < w->restart_count; i++) {
        int idx = (w->restart_head - 1 - i + SUPERVISOR_MAX_RESTART_SAMPLES) %
                  SUPERVISOR_MAX_RESTART_SAMPLES;
        uint64_t t = w->restart_times_ms[idx];
        if (now_ms - t > window_ms)
            break;
        count_in_window++;
    }
    return count_in_window >= max_restarts;
}

/* ---- fork + execve -------------------------------------------------------- */

/* Build an envp for the child: a copy of the pointers in the parent's
 * (child's-copy-of, post-fork) `environ`, with the bare BEAVERMQ_SUPERVISOR
 * on/off flag filtered out (so a worker never recurses into supervisor mode -
 * see supervisor.h) and BEAVERMQ_HEARTBEAT_FD added. Deliberately an EXACT
 * key match ("BEAVERMQ_SUPERVISOR=", not a prefix match): the tuning vars
 * like BEAVERMQ_SUPERVISOR_HEARTBEAT_MS must reach the worker unchanged, since
 * main.c's heartbeat-writer reads that exact same variable (one definition of
 * the default interval, shared between supervisor and worker). Called only
 * in the freshly forked child, single-threaded, immediately before execve() -
 * the small leak on a successful exec (which replaces the whole image) or on
 * the _exit(127) fallback (which tears down the whole process) is irrelevant. */
static char **build_worker_envp(int heartbeat_fd)
{
    static const char RECURSION_KEY[] = "BEAVERMQ_SUPERVISOR=";
    int n = 0;
    for (char **e = environ; *e; e++)
        if (strncmp(*e, RECURSION_KEY, sizeof(RECURSION_KEY) - 1) != 0)
            n++;
    char **out = malloc((size_t)(n + 2) * sizeof(*out));
    if (!out)
        return NULL;
    int i = 0;
    for (char **e = environ; *e; e++)
        if (strncmp(*e, RECURSION_KEY, sizeof(RECURSION_KEY) - 1) != 0)
            out[i++] = *e;
    char buf[40];
    snprintf(buf, sizeof(buf), "BEAVERMQ_HEARTBEAT_FD=%d", heartbeat_fd);
    out[i] = strdup(buf);
    out[i + 1] = NULL;
    return out;
}

static void on_heartbeat_readable(uv_poll_t *handle, int status, int events);
static void on_heartbeat_timeout(uv_timer_t *timer);

/* Fork + execve a fresh copy of the current binary as `w`. Assumes `w`'s
 * handles are NOT currently active (fresh, or fully closed by a prior
 * teardown - see on_worker_handle_closed). Returns 0 on success, -1 on
 * failure (fork()/pipe2() error; the caller treats this the same as
 * exceeding the restart-rate limit - see supervisor_main/on_worker_handle_closed). */
static int spawn_worker(supervisor_t *sup, supervised_worker_t *w)
{
    int fds[2];
    if (pipe2(fds, O_CLOEXEC) != 0) {
        LOG_ERROR("supervisor: pipe2 failed for worker %d: %s", w->index, strerror(errno));
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("supervisor: fork failed for worker %d: %s", w->index, strerror(errno));
        close(fds[0]);
        close(fds[1]);
        return -1;
    }

    if (pid == 0) {
        /* Child: everything from here to execve() must be async-signal-safe-
         * adjacent (no libuv, no touching the parent's loop state - fork()
         * only duplicated this single thread, which is exactly what we rely
         * on). LOG_* is safe here specifically because the supervisor is
         * single-threaded, so the logger mutex was never held by a thread
         * that didn't get copied. */
        close(fds[0]); /* child only writes */
        fcntl(fds[1], F_SETFD, 0); /* clear O_CLOEXEC: THIS end must survive our own exec */
        char **envp = build_worker_envp(fds[1]);
        if (envp) {
            execve(sup->self_exe, sup->child_argv, envp);
            LOG_ERROR("supervisor: execve('%s') failed: %s", sup->self_exe, strerror(errno));
        } else {
            LOG_ERROR("supervisor: OOM building worker environment");
        }
        _exit(127); /* NEVER exit()/return here: would flush stdio buffers
                     * duplicated from the parent at fork() time, potentially
                     * duplicating the parent's own buffered output. */
    }

    /* Parent. */
    close(fds[1]); /* parent only reads */
    if (fcntl(fds[0], F_SETFL, O_NONBLOCK) != 0)
        LOG_WARN("supervisor: could not set heartbeat fd non-blocking for worker %d",
                w->index);

    w->pid                  = pid;
    w->state                = WORKER_STARTING;
    w->heartbeat_read_fd    = fds[0];
    w->heartbeat_timer_armed = 0;
    w->last_heartbeat_ms    = uv_now(&sup->loop);
    w->pending_respawn      = 0;
    w->closing_handles      = 0;

    uv_poll_init(&sup->loop, &w->heartbeat_poll, fds[0]);
    w->heartbeat_poll.data = w;
    uv_poll_start(&w->heartbeat_poll, UV_READABLE, on_heartbeat_readable);

    uv_timer_init(&sup->loop, &w->heartbeat_timeout_timer);
    w->heartbeat_timeout_timer.data = w;
    /* Not started yet: armed on the worker's FIRST heartbeat byte, not at
     * fork time - see supervisor.h / the plan for why (avoids a false-
     * positive timeout while the worker is still starting up: binding
     * listeners, replaying the Raft WAL, etc.). */

    char pidname[32];
    worker_pidfile_name(pidname, sizeof(pidname), w->index, sup->cfg.nworkers);
    write_pidfile(sup->cfg.data_dir, pidname, pid);

    LOG_INFO("supervisor: spawned worker %d (pid %d)", w->index, (int)pid);
    return 0;
}

/* ---- heartbeat watchdog ---------------------------------------------------- */

static void on_heartbeat_readable(uv_poll_t *handle, int status, int events)
{
    supervised_worker_t *w = handle->data;
    if (status < 0 || !(events & UV_READABLE))
        return;
    uint8_t buf[64];
    ssize_t n;
    int got_any = 0;
    while ((n = read(w->heartbeat_read_fd, buf, sizeof(buf))) > 0)
        got_any = 1;
    if (!got_any)
        return; /* spurious wakeup / EAGAIN / EOF-ish: nothing to act on here,
                 * a real worker death is handled uniformly via SIGCHLD */

    w->last_heartbeat_ms = uv_now(&w->sup->loop);
    if (w->state == WORKER_STARTING)
        w->state = WORKER_RUNNING;
    if (!w->heartbeat_timer_armed) {
        w->heartbeat_timer_armed = 1;
        uv_timer_start(&w->heartbeat_timeout_timer, on_heartbeat_timeout,
                      w->sup->cfg.heartbeat_timeout_ms, w->sup->cfg.heartbeat_timeout_ms);
    }
}

static void on_heartbeat_timeout(uv_timer_t *timer)
{
    supervised_worker_t *w = timer->data;
    uint64_t now = uv_now(&w->sup->loop);
    if (now - w->last_heartbeat_ms <= w->sup->cfg.heartbeat_timeout_ms)
        return; /* still within budget */
    LOG_ERROR("supervisor: worker %d (pid %d) missed its heartbeat for %llums "
             "(frozen event loop?); sending SIGKILL",
             w->index, (int)w->pid, (unsigned long long)(now - w->last_heartbeat_ms));
    if (w->pid > 0)
        kill(w->pid, SIGKILL);
    uv_timer_stop(timer); /* don't SIGKILL repeatedly while waiting for the reap */
}

/* ---- worker teardown + respawn -------------------------------------------- */

static void check_all_workers_settled(supervisor_t *sup);

static void on_worker_handle_closed(uv_handle_t *handle)
{
    supervised_worker_t *w = handle->data;
    if (--w->closing_handles > 0)
        return; /* wait for the other handle (poll + timer) to finish closing too */

    if (w->heartbeat_read_fd >= 0) {
        close(w->heartbeat_read_fd);
        w->heartbeat_read_fd = -1;
    }

    if (w->pending_respawn) {
        w->pending_respawn = 0;
        if (spawn_worker(w->sup, w) != 0) {
            LOG_FATAL("supervisor: failed to respawn worker %d; giving up on it",
                     w->index);
            w->gave_up = 1;
        }
    }
    check_all_workers_settled(w->sup);
}

/* Begin tearing down a dead worker's handles. `pending_respawn` decides what
 * happens once both handles report closed (see on_worker_handle_closed). */
static void begin_worker_teardown(supervised_worker_t *w, int pending_respawn)
{
    w->state = WORKER_DEAD;
    w->pending_respawn = pending_respawn;
    w->closing_handles = 2;
    uv_poll_stop(&w->heartbeat_poll);
    uv_close((uv_handle_t *)&w->heartbeat_poll, on_worker_handle_closed);
    uv_timer_stop(&w->heartbeat_timeout_timer);
    uv_close((uv_handle_t *)&w->heartbeat_timeout_timer, on_worker_handle_closed);
}

static void log_exit_reason(const supervised_worker_t *w, int status)
{
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code == 0)
            LOG_INFO("supervisor: worker %d (pid %d) exited cleanly",
                     w->index, (int)w->pid);
        else
            LOG_ERROR("supervisor: worker %d (pid %d) exited with code %d",
                     w->index, (int)w->pid, code);
    } else if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        LOG_ERROR("supervisor: worker %d (pid %d) killed by signal %d (%s)%s",
                 w->index, (int)w->pid, sig, strsignal(sig),
                 WCOREDUMP(status) ? " [core dumped]" : "");
    } else {
        LOG_WARN("supervisor: worker %d (pid %d) exited with unrecognized status 0x%x",
                w->index, (int)w->pid, (unsigned)status);
    }
}

static supervised_worker_t *find_worker_by_pid(supervisor_t *sup, pid_t pid)
{
    for (int i = 0; i < sup->cfg.nworkers; i++)
        if (sup->workers[i].pid == pid)
            return &sup->workers[i];
    return NULL;
}

/* A worker is "settled" once it has no live process AND its handles have
 * fully closed - i.e. nothing left in flight for it. Once every worker is
 * settled AND (we are deliberately shutting down OR every one of them has
 * permanently given up), there is nothing left to supervise. */
static void check_all_workers_settled(supervisor_t *sup)
{
    int all_gave_up = 1;
    for (int i = 0; i < sup->cfg.nworkers; i++) {
        supervised_worker_t *w = &sup->workers[i];
        if (w->pid > 0 || w->closing_handles > 0)
            return; /* still alive or mid-teardown/mid-respawn */
        if (!w->gave_up)
            all_gave_up = 0;
    }
    if (!sup->shutting_down && !all_gave_up)
        return; /* a normal respawn just completed; nothing to finish */

    if (!sup->shutting_down && all_gave_up)
        sup->exit_code = 1; /* crash-loop exhausted on every worker: propagate failure up */

    if (uv_is_active((uv_handle_t *)&sup->shutdown_deadline_timer))
        uv_timer_stop(&sup->shutdown_deadline_timer);
    uv_close((uv_handle_t *)&sup->shutdown_deadline_timer, NULL);
    uv_signal_stop(&sup->sig_term);
    uv_close((uv_handle_t *)&sup->sig_term, NULL);
    uv_signal_stop(&sup->sig_int);
    uv_close((uv_handle_t *)&sup->sig_int, NULL);
    uv_signal_stop(&sup->sig_chld);
    uv_close((uv_handle_t *)&sup->sig_chld, NULL);
    uv_stop(&sup->loop);
}

/* ---- signal handling -------------------------------------------------------
 * uv_signal_t callbacks run as ordinary libuv-loop callbacks (via libuv's own
 * internal self-pipe), not inside real POSIX signal-handler context - so it
 * is safe to log, close handles, fork, etc. from here, exactly like net.c's
 * existing SIGINT/SIGTERM handling for the broker itself. */

static void on_sigchld(uv_signal_t *handle, int signum)
{
    (void)signum;
    supervisor_t *sup = handle->data;
    int status;
    pid_t pid;
    /* SIGCHLD delivery can coalesce multiple deaths into one notification -
     * reap everything currently reapable, not just one. */
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        supervised_worker_t *w = find_worker_by_pid(sup, pid);
        if (!w) {
            LOG_WARN("supervisor: reaped unknown pid %d", (int)pid);
            continue;
        }
        log_exit_reason(w, status);
        w->pid = -1;

        int respawn = !sup->shutting_down;
        if (respawn) {
            uint64_t now = uv_now(&sup->loop);
            record_restart(w, now);
            if (restart_rate_exceeded(w, sup->cfg.restart_window_ms,
                                      sup->cfg.max_restarts, now)) {
                LOG_FATAL("supervisor: worker %d crashed %d+ times within %llums; "
                         "giving up (not respawning)",
                         w->index, sup->cfg.max_restarts,
                         (unsigned long long)sup->cfg.restart_window_ms);
                w->gave_up = 1;
                respawn = 0;
            }
        }
        begin_worker_teardown(w, respawn);
    }
}

static void on_shutdown_deadline(uv_timer_t *timer)
{
    supervisor_t *sup = timer->data;
    int killed_any = 0;
    for (int i = 0; i < sup->cfg.nworkers; i++) {
        supervised_worker_t *w = &sup->workers[i];
        if (w->pid > 0) {
            LOG_WARN("supervisor: worker %d did not exit within %llums; sending SIGKILL",
                    w->index, (unsigned long long)sup->cfg.shutdown_timeout_ms);
            kill(w->pid, SIGKILL);
            killed_any = 1;
        }
    }
    if (killed_any)
        sup->exit_code = 1; /* signal "not everyone shut down cleanly" */
    /* Do not finish here: the SIGKILL'd processes still need to be reaped via
     * SIGCHLD (on_sigchld -> check_all_workers_settled), which will actually
     * stop the loop once done - finishing here would race the reap. */
}

static void on_shutdown_signal(uv_signal_t *handle, int signum)
{
    supervisor_t *sup = handle->data;
    if (sup->shutting_down)
        return; /* a second Ctrl-C etc. while already shutting down: ignore */
    LOG_WARN("supervisor: received signal %d; forwarding SIGTERM to %d worker(s)",
            signum, sup->cfg.nworkers);
    sup->shutting_down = 1;

    int any_alive = 0;
    for (int i = 0; i < sup->cfg.nworkers; i++) {
        supervised_worker_t *w = &sup->workers[i];
        if (w->pid > 0) {
            kill(w->pid, SIGTERM);
            w->state = WORKER_STOPPING;
            any_alive = 1;
        }
    }
    if (!any_alive) {
        check_all_workers_settled(sup);
        return;
    }
    uv_timer_start(&sup->shutdown_deadline_timer, on_shutdown_deadline,
                  sup->cfg.shutdown_timeout_ms, 0);
}

/* ---- entry point ------------------------------------------------------------ */

static void resolve_self_exe(supervisor_t *sup, const char *argv0)
{
    ssize_t n = readlink("/proc/self/exe", sup->self_exe, sizeof(sup->self_exe) - 1);
    if (n > 0) {
        sup->self_exe[n] = '\0';
        return;
    }
    snprintf(sup->self_exe, sizeof(sup->self_exe), "%s", argv0 ? argv0 : "beavermq");
}

int supervisor_main(int argc, char **argv)
{
    log_init(LOG_LEVEL_INFO, stderr);
    signal(SIGPIPE, SIG_IGN); /* defensive; the supervisor itself does no AMQP I/O */

    /* Resolve just enough of the BROKER's own config to know data_dir (for
     * pid files) and cluster_enabled (for the nworkers>1 guard) - config.c is
     * a pure parser with no broker/dispatch/cluster dependency, so this does
     * NOT violate the "never initialize a broker here" rule. argv[1], if
     * present, is either a port number or a config path exactly like the
     * normal (non-supervisor) startup path in main.c; config_find_file()
     * already ignores it if it isn't a readable file. */
    beaver_config_t bcfg;
    config_defaults(&bcfg);
    const char *cli_hint = argc > 1 ? argv[1] : NULL;
    char conf_path[600];
    if (config_find_file(conf_path, sizeof(conf_path), cli_hint))
        config_load_file(&bcfg, conf_path);
    config_apply_env(&bcfg);

    supervisor_t sup;
    memset(&sup, 0, sizeof(sup));
    supervisor_config_from_env(&sup.cfg, bcfg.data_dir, bcfg.cluster_enabled);

    if (sup.cfg.nworkers != 1) {
        /* Multiple worker PROCESSES are unsupported, even with cluster=on. The
         * supervisor gives every child an identical command line and
         * environment: same node_id, same data_dir, same cluster-N.log/.meta/
         * .snap WAL files, same AMQP/HTTP/cluster ports, same peer set. They do
         * not become distinct Raft nodes - they collide on the WAL and ports
         * and can corrupt persisted data. Until per-process orchestration
         * (unique node_id/data_dir/ports) exists, run multiple worker THREADS
         * inside a single process instead (BEAVERMQ workers), not multiple
         * supervisor processes. */
        LOG_FATAL("supervisor: BEAVERMQ_SUPERVISOR_WORKERS=%d is unsupported - "
                 "multiple worker processes would share one node_id, data_dir, "
                 "WAL and port set and can corrupt persisted data; refusing to "
                 "start. Use a single supervised process.", sup.cfg.nworkers);
        return 1;
    }

    resolve_self_exe(&sup, argv[0]);
    sup.child_argc = argc;
    sup.child_argv = argv; /* already NULL-terminated per C, already stripped
                            * of "--supervisor" by main.c */

    if (uv_loop_init(&sup.loop) != 0) {
        LOG_FATAL("supervisor: uv_loop_init failed");
        return 1;
    }

    uv_signal_init(&sup.loop, &sup.sig_term);
    sup.sig_term.data = &sup;
    uv_signal_start(&sup.sig_term, on_shutdown_signal, SIGTERM);
    uv_signal_init(&sup.loop, &sup.sig_int);
    sup.sig_int.data = &sup;
    uv_signal_start(&sup.sig_int, on_shutdown_signal, SIGINT);
    uv_signal_init(&sup.loop, &sup.sig_chld);
    sup.sig_chld.data = &sup;
    uv_signal_start(&sup.sig_chld, on_sigchld, SIGCHLD);
    uv_timer_init(&sup.loop, &sup.shutdown_deadline_timer);
    sup.shutdown_deadline_timer.data = &sup;

    sup.workers = calloc((size_t)sup.cfg.nworkers, sizeof(*sup.workers));
    if (!sup.workers) {
        LOG_FATAL("supervisor: OOM allocating worker table");
        return 1;
    }
    for (int i = 0; i < sup.cfg.nworkers; i++) {
        sup.workers[i].index = i;
        sup.workers[i].pid = -1;
        sup.workers[i].heartbeat_read_fd = -1;
        sup.workers[i].sup = &sup;
    }

    write_pidfile(sup.cfg.data_dir, "supervisor.pid", getpid());

    LOG_INFO("BeaverMQ %s (build %s) supervisor starting: %d worker process(es), "
            "restart limit %d/%llums, heartbeat %llums/timeout %llums",
            BEAVER_VERSION, beaver_build_stamp, sup.cfg.nworkers, sup.cfg.max_restarts,
            (unsigned long long)sup.cfg.restart_window_ms,
            (unsigned long long)sup.cfg.heartbeat_interval_ms,
            (unsigned long long)sup.cfg.heartbeat_timeout_ms);

    for (int i = 0; i < sup.cfg.nworkers; i++) {
        if (spawn_worker(&sup, &sup.workers[i]) != 0) {
            LOG_FATAL("supervisor: failed to start worker %d; aborting startup", i);
            free(sup.workers);
            uv_loop_close(&sup.loop);
            return 1;
        }
    }

    uv_run(&sup.loop, UV_RUN_DEFAULT);

    LOG_INFO("supervisor: stopped (exit code %d)", sup.exit_code);
    free(sup.workers);
    uv_loop_close(&sup.loop);
    return sup.exit_code;
}
