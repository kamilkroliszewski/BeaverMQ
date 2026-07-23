/*
 * main.c - BeaverMQ entry point and worker-thread orchestration.
 *
 * Starts N worker threads (N from config / auto-detected cores). Each worker
 * runs its own libuv loop with a SO_REUSEPORT listener, so the kernel
 * load-balances connections across cores. Worker 0 runs on the main thread and
 * additionally hosts the HTTP management server, the signal handlers, and the
 * stats timer. The broker (queues/exchanges) is shared and thread-safe.
 *
 * Shutdown: a signal on worker 0 fans out a thread-safe "stop" async to every
 * worker; each closes its handles and its loop returns; the main thread joins
 * the others and frees everything.
 */
#include "logger.h"
#include "net.h"
#include "broker.h"
#include "dispatch.h"
#include "http.h"
#include "cluster.h"
#include "config.h"
#include "authstore.h"
#include "crypto.h"
#include "version.h"

#include <uv.h>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef __GLIBC__
#include <malloc.h>
#endif

typedef struct {
    int                  id;
    uv_loop_t            loop;
    beaver_server_t      server;
    beaver_dispatcher_t *dispatcher;
    uv_thread_t          thread;     /* unused for worker 0 (the main thread) */
} worker_t;

typedef struct {
    beaver_config_t       config;
    beaver_broker_t      *broker;
    struct authstore     *authstore;
    beaver_stats_t        stats;
    worker_t             *workers;
    int                   nworkers;
    /* The HTTP management server runs on its OWN loop/thread so it is never
     * starved by AMQP load on the worker loops. */
    beaver_http_server_t *http;
    uv_loop_t             mgmt_loop;
    uv_thread_t           mgmt_thread;
    int                   mgmt_started;
    /* Cluster control plane (optional): its OWN loop/thread, like the mgmt
     * server, so Raft is never starved by AMQP load and needs no locks. */
    cluster_node_t       *cluster;
    uv_loop_t             cluster_loop;
    uv_thread_t           cluster_thread;
    int                   cluster_started;
} app_t;

/* ---- small helpers ------------------------------------------------------- */

static void raise_fd_limit(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0)
        return;
    if (rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

/* Directory of the running executable (via /proc/self/exe). */
static int executable_dir(char *out, size_t outsz)
{
    char path[4096];
    ssize_t n = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (n <= 0)
        return 0;
    path[n] = '\0';
    char *slash = strrchr(path, '/');
    if (!slash)
        return 0;
    *slash = '\0';
    size_t len = strlen(path);
    if (len >= outsz)
        return 0;
    memcpy(out, path, len + 1);
    return 1;
}

static int file_readable(const char *p) { return access(p, R_OK) == 0; }

/* Locate a beavermq.conf: explicit CLI arg, env, cwd, then next to the binary. */
static int find_config(char *out, size_t outsz, const char *cli)
{
    if (cli && file_readable(cli)) { snprintf(out, outsz, "%s", cli); return 1; }
    const char *env = getenv("BEAVERMQ_CONF");
    if (env && file_readable(env)) { snprintf(out, outsz, "%s", env); return 1; }
    if (file_readable("beavermq.conf")) { snprintf(out, outsz, "beavermq.conf"); return 1; }

    char exedir[256];
    if (executable_dir(exedir, sizeof(exedir))) {
        char p[512];
        snprintf(p, sizeof(p), "%s/beavermq.conf", exedir);
        if (file_readable(p)) { snprintf(out, outsz, "%s", p); return 1; }
        snprintf(p, sizeof(p), "%s/../beavermq.conf", exedir);
        if (file_readable(p)) { snprintf(out, outsz, "%s", p); return 1; }
    }
    return 0;
}

static int has_dashboard(const char *dir)
{
    char probe[1024];
    int n = snprintf(probe, sizeof(probe), "%s/index.html", dir);
    if (n < 0 || (size_t)n >= sizeof(probe))
        return 0;
    return file_readable(probe);
}

static int resolve_web_root(const beaver_config_t *cfg, char *out, size_t outsz)
{
    if (cfg->web_root[0]) { snprintf(out, outsz, "%s", cfg->web_root); return 1; }
    snprintf(out, outsz, "web");
    if (has_dashboard(out)) return 1;
    char exedir[256];
    if (executable_dir(exedir, sizeof(exedir))) {
        snprintf(out, outsz, "%s/web", exedir);
        if (has_dashboard(out)) return 1;
        snprintf(out, outsz, "%s/../web", exedir);
        if (has_dashboard(out)) return 1;
    }
    return 0;
}

/* ---- worker lifecycle ---------------------------------------------------- */

static void worker_thread_main(void *arg)
{
    worker_t *w = arg;
    uv_run(&w->loop, UV_RUN_DEFAULT);
}

/* Initialize one worker (loop + dispatcher + reuseport listener). Runs on the
 * main thread before any worker loop starts, so there is no concurrency yet. */
static int worker_init(app_t *app, worker_t *w, int id)
{
    w->id = id;
    if (uv_loop_init(&w->loop) != 0) {
        LOG_FATAL("worker %d: uv_loop_init failed", id);
        return -1;
    }
    w->dispatcher = dispatcher_new(&w->loop, app->broker);
    if (!w->dispatcher) {
        LOG_FATAL("worker %d: dispatcher_new failed", id);
        return -1;
    }
    int rc = beaver_server_init(&w->server, &w->loop, app->config.bind_addr,
                                app->config.amqp_port, BEAVER_DEFAULT_BACKLOG,
                                1 /* SO_REUSEPORT */);
    if (rc != 0) {
        LOG_FATAL("worker %d: failed to bind %s:%d (%s)", id,
                  app->config.bind_addr, app->config.amqp_port, uv_strerror(rc));
        return -1;
    }
    w->server.broker     = app->broker;
    w->server.authstore  = app->authstore;
    w->server.dispatcher = w->dispatcher;
    w->server.stats      = &app->stats;
    w->server.max_connections  = app->config.max_connections;
    w->server.max_message_size = app->config.max_message_size;
    beaver_server_install_shutdown_async(&w->server);
    return 0;
}

static void mgmt_thread_main(void *arg)
{
    app_t *app = arg;
    uv_run(&app->mgmt_loop, UV_RUN_DEFAULT);
}

static void cluster_thread_main(void *arg)
{
    app_t *app = arg;
    uv_run(&app->cluster_loop, UV_RUN_DEFAULT);
}

/* Derive this node's mesh listen port from its own cluster_nodes[] entry. */
static int cluster_listen_port(const beaver_config_t *cfg)
{
    const char *s = cfg->cluster_nodes[cfg->node_id];
    const char *colon = strrchr(s, ':');
    return colon ? atoi(colon + 1) : 0;
}

/* Bring up the cluster control plane on its own loop/thread. Returns 0 on
 * success, -1 on failure. When cfg->cluster_enabled is set the caller MUST
 * treat -1 as fatal (fail closed): silently falling back to an unreplicated
 * standalone broker when the operator asked for clustering is worse than
 * refusing to start. */
static int start_cluster(app_t *app)
{
    beaver_config_t *cfg = &app->config;
    if (cfg->node_id < 0 || cfg->node_id >= cfg->cluster_nnodes) {
        LOG_ERROR("cluster: node_id %d out of range [0,%d); clustering disabled",
                  cfg->node_id, cfg->cluster_nnodes);
        return -1;
    }
    int port = cluster_listen_port(cfg);
    if (port <= 0) {
        LOG_ERROR("cluster: cannot derive listen port from '%s'; disabled",
                  cfg->cluster_nodes[cfg->node_id]);
        return -1;
    }

    cluster_config_t cc;
    cluster_config_defaults(&cc);
    cc.self_id      = cfg->node_id;
    cc.nnodes       = cfg->cluster_nnodes;
    cc.cluster_port = port;
    snprintf(cc.bind_addr, sizeof(cc.bind_addr), "%s", cfg->bind_addr);
    snprintf(cc.data_dir, sizeof(cc.data_dir), "%s", cfg->data_dir);
    snprintf(cc.secret, sizeof(cc.secret), "%s", cfg->cluster_secret);
    cc.durable_commit = cfg->cluster_durable_commit;
    /* Optional Raft timing overrides (0 = keep the built-in defaults). The
     * election window is [floor, 2*floor] so a single knob controls both ends. */
    if (cfg->election_timeout_ms > 0) {
        cc.election_min_ms = (uint32_t)cfg->election_timeout_ms;
        cc.election_max_ms = (uint32_t)cfg->election_timeout_ms * 2;
    }
    if (cfg->heartbeat_ms > 0)
        cc.heartbeat_ms = (uint32_t)cfg->heartbeat_ms;
    for (int i = 0; i < cfg->cluster_nnodes && i < CLUSTER_MAX_NODES; i++)
        snprintf(cc.node_addr[i], sizeof(cc.node_addr[i]), "%s",
                 cfg->cluster_nodes[i]);

    if (uv_loop_init(&app->cluster_loop) != 0) {
        LOG_ERROR("cluster: uv_loop_init failed; clustering disabled");
        return -1;
    }
    app->cluster = cluster_node_new(&app->cluster_loop, &cc, app->broker,
                                    app->authstore);
    if (!app->cluster) {
        LOG_ERROR("cluster: cluster_node_new failed; clustering disabled");
        uv_loop_close(&app->cluster_loop);
        return -1;
    }
    int rc = cluster_node_listen(app->cluster, cfg->bind_addr, port, 64);
    if (rc != 0) {
        LOG_ERROR("cluster: failed to listen on %s:%d (%s); clustering disabled",
                  cfg->bind_addr, port, uv_strerror(rc));
        cluster_node_free(app->cluster);
        app->cluster = NULL;
        uv_loop_close(&app->cluster_loop);
        return -1;
    }
    cluster_node_install_shutdown_async(app->cluster);
    cluster_node_start(app->cluster);
    if (uv_thread_create(&app->cluster_thread, cluster_thread_main, app) == 0) {
        app->cluster_started = 1;
        /* Let each worker's protocol layer replicate topology declarations, and
         * its dispatcher replicate consume watermarks. */
        for (int i = 0; i < app->nworkers; i++) {
            app->workers[i].server.cluster = app->cluster;
            dispatcher_set_cluster(app->workers[i].dispatcher, app->cluster);
        }
        LOG_INFO("cluster: node %d up on %s:%d (%d-node cluster)",
                 cfg->node_id, cfg->bind_addr, port, cfg->cluster_nnodes);
        return 0;
    }
    LOG_ERROR("cluster: failed to spawn cluster thread");
    cluster_node_free(app->cluster);
    app->cluster = NULL;
    uv_loop_close(&app->cluster_loop);
    return -1;
}

/* Signal handler hook (runs on worker 0): ask every worker + the management
 * thread + the cluster node to stop. */
static void app_shutdown_all(void *ctx)
{
    app_t *app = ctx;
    for (int i = 0; i < app->nworkers; i++)
        beaver_server_request_shutdown(&app->workers[i].server);
    if (app->http)
        http_server_request_shutdown(app->http);
    if (app->cluster)
        cluster_node_request_shutdown(app->cluster);
}

/* Path used for the token that gates the first-boot management-API bootstrap
 * window (see http_server_set_bootstrap_token / generate_bootstrap_token). */
static void bootstrap_token_path(const beaver_config_t *cfg, char *out, size_t cap)
{
    if (cfg->data_dir[0])
        snprintf(out, cap, "%s/bootstrap_token", cfg->data_dir);
    else
        out[0] = '\0';
}

/* Generate a fresh random bootstrap token, write it to data_dir (if
 * configured, mode 0600 - `beavermq add-user` reads it from there
 * automatically) and log it, since that log line is the ONLY way to learn it
 * when data_dir is unset. A TCP peer address proves nothing about the real
 * client behind a reverse proxy, so unlike the old behavior this is NOT
 * gated on "looks like localhost" - only on holding this token. Leaves
 * out[0] = '\0' (bootstrap window stays closed - fail safe, not fail open)
 * if no CSPRNG source was available. */
static void generate_bootstrap_token(const beaver_config_t *cfg, char *out, size_t out_cap)
{
    out[0] = '\0';
    uint8_t raw[24];
    if (crypto_random_bytes(raw, sizeof raw) != 0) {
        LOG_ERROR("bootstrap: no CSPRNG available; the management API "
                  "bootstrap window is disabled (fix /dev/urandom, or create "
                  "the first admin via an already-configured cluster node)");
        return;
    }
    static const char *hex = "0123456789abcdef";
    size_t n = sizeof raw;
    if (out_cap < n * 2 + 1)
        n = (out_cap - 1) / 2;
    for (size_t i = 0; i < n; i++) {
        out[i * 2]     = hex[raw[i] >> 4];
        out[i * 2 + 1] = hex[raw[i] & 0xf];
    }
    out[n * 2] = '\0';

    char path[300];
    bootstrap_token_path(cfg, path, sizeof path);
    if (path[0]) {
        int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd >= 0) {
            ssize_t wn = write(fd, out, strlen(out));
            close(fd);
            if (wn == (ssize_t)strlen(out)) {
                LOG_WARN("bootstrap: no users configured - create the first "
                        "admin with: beavermq add-user <name> <password> "
                        "(reads the token from '%s' automatically)", path);
                return;
            }
        }
        LOG_WARN("bootstrap: could not write token to '%s'; pass it "
                 "explicitly: beavermq add-user <name> <password> -t %s",
                 path, out);
        return;
    }
    LOG_WARN("bootstrap: no users configured and no data_dir set - create "
             "the first admin with: beavermq add-user <name> <password> -t %s",
             out);
}

/* ---- `beavermq add-user` CLI subcommand ----------------------------------- *
 * Creates (or updates) a user by POSTing to the local management API over a
 * plain socket, so operators can bootstrap the FIRST administrator on a fresh
 * broker without curl. While the broker has no users, the request must carry
 * the bootstrap token (auto-read from data_dir/bootstrap_token, or passed
 * explicitly with -t); afterwards pass -u admin:password. In a cluster, run
 * it against any live node - the change replicates through Raft. */

static int b64_encode(const unsigned char *in, size_t n, char *out, size_t cap)
{
    static const char *T =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        if (o + 5 > cap) return -1;
        unsigned v = (unsigned)in[i] << 16;
        if (i + 1 < n) v |= (unsigned)in[i + 1] << 8;
        if (i + 2 < n) v |= in[i + 2];
        out[o++] = T[(v >> 18) & 63];
        out[o++] = T[(v >> 12) & 63];
        out[o++] = (i + 1 < n) ? T[(v >> 6) & 63] : '=';
        out[o++] = (i + 2 < n) ? T[v & 63] : '=';
    }
    out[o] = '\0';
    return 0;
}

/* Escape `"` and `\` for a JSON string literal. */
static void json_escape(const char *in, char *out, size_t cap)
{
    size_t o = 0;
    for (const char *p = in; *p && o + 2 < cap; p++) {
        if (*p == '"' || *p == '\\')
            out[o++] = '\\';
        out[o++] = *p;
    }
    out[o] = '\0';
}

/* One blocking HTTP POST to host:port; returns the response status (0 = I/O
 * error). `authhdr` is "" or a full "Authorization: ...\r\n" line. */
static int cli_http_post(const char *host, int port, const char *path,
                         const char *body, const char *authhdr)
{
    char req[2048];
    int rlen = snprintf(req, sizeof req,
                        "POST %s HTTP/1.1\r\n"
                        "Host: %s\r\n%s"
                        "Content-Type: application/json\r\n"
                        "Content-Length: %zu\r\n"
                        "Connection: close\r\n\r\n%s",
                        path, host, authhdr, strlen(body), body);
    if (rlen <= 0 || (size_t)rlen >= sizeof req)
        return 0;

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "error: -H expects an IPv4 address (got '%s')\n", host);
        return 0;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0 || connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) {
        fprintf(stderr, "error: cannot connect to %s:%d (%s) - is the broker "
                "running?\n", host, port, strerror(errno));
        if (fd >= 0) close(fd);
        return 0;
    }
    ssize_t off = 0;
    while (off < rlen) {
        ssize_t w = write(fd, req + off, (size_t)(rlen - off));
        if (w <= 0) { close(fd); return 0; }
        off += w;
    }
    char resp[512];
    ssize_t got = read(fd, resp, sizeof resp - 1);
    close(fd);
    if (got <= 0)
        return 0;
    resp[got] = '\0';
    int status = 0;
    sscanf(resp, "HTTP/1.1 %d", &status);
    return status;
}

static int cli_add_user(int argc, char **argv)
{
    log_init(LOG_LEVEL_ERROR, stderr);   /* quiet: config loading logs at INFO */
    const char *user = NULL, *pass = NULL, *tags = "administrator";
    const char *admin = NULL, *token = NULL, *host = "127.0.0.1";
    int port = 0;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) admin = argv[++i];
        else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) token = argv[++i];
        else if (strcmp(argv[i], "-H") == 0 && i + 1 < argc) host = argv[++i];
        else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (!user) user = argv[i];
        else if (!pass) pass = argv[i];
        else tags = argv[i];
    }
    if (!user || !pass) {
        fprintf(stderr,
            "usage: beavermq add-user <name> <password> [tags] [-u admin:pass] "
            "[-t bootstrap-token] [-H host] [-p http_port]\n"
            "  tags: administrator (default) | management | \"\" (none)\n"
            "  On a fresh broker (no users yet) pass -t with the token logged\n"
            "  at startup, or omit it: it is read automatically from\n"
            "  data_dir/bootstrap_token if data_dir is configured.\n");
        return 2;
    }

    beaver_config_t cfg;
    config_defaults(&cfg);
    char conf_path[600];
    if (find_config(conf_path, sizeof(conf_path), NULL))
        config_load_file(&cfg, conf_path);
    config_apply_env(&cfg);
    if (!port)
        port = cfg.http_port;

    char eu[256], ep[512], et[128];
    json_escape(user, eu, sizeof eu);
    json_escape(pass, ep, sizeof ep);
    json_escape(tags, et, sizeof et);
    char body[1024];
    snprintf(body, sizeof body,
                        "{\"name\":\"%s\",\"password\":\"%s\",\"tags\":\"%s\"}",
                        eu, ep, et);

    char authhdr[600] = "";
    if (admin) {
        char b64[512];
        if (b64_encode((const unsigned char *)admin, strlen(admin),
                       b64, sizeof b64) != 0) {
            fprintf(stderr, "error: -u credentials too long\n");
            return 2;
        }
        snprintf(authhdr, sizeof authhdr, "Authorization: Basic %s\r\n", b64);
    } else {
        char tokbuf[80];
        if (!token && cfg.data_dir[0]) {
            char path[300];
            bootstrap_token_path(&cfg, path, sizeof path);
            FILE *f = fopen(path, "r");
            if (f) {
                if (fgets(tokbuf, sizeof tokbuf, f)) {
                    char *nl = strpbrk(tokbuf, "\r\n");
                    if (nl) *nl = '\0';
                    token = tokbuf;
                }
                fclose(f);
            }
        }
        if (token)
            snprintf(authhdr, sizeof authhdr, "X-Bootstrap-Token: %s\r\n", token);
    }

    int status = cli_http_post(host, port, "/api/users", body, authhdr);
    if (status != 200) {
        if (status == 401)
            fprintf(stderr, "error: authentication required - pass -u admin:password "
                    "or -t <bootstrap-token>\n");
        else if (status == 403)
            fprintf(stderr, "error: the -u user is not an administrator\n");
        else if (status == 503)
            fprintf(stderr, "error: the cluster has no leader/quorum - start the "
                    "other nodes, or run standalone (cluster = off)\n");
        else
            fprintf(stderr, "error: HTTP %d creating user\n", status);
        return 1;
    }
    printf("ok: user '%s' created/updated (tags: %s)\n", user, tags);

    /* Grant full permissions on the default vhost so the account is usable
     * immediately (like rabbitmqctl's usual add_user + set_permissions pair).
     * Authenticate as -u if given, else as the just-created user. */
    snprintf(body, sizeof body,
             "{\"user\":\"%s\",\"vhost\":\"/\",\"configure\":\".*\","
             "\"write\":\".*\",\"read\":\".*\"}", eu);
    if (!admin) {
        char up[600], b64[512];
        snprintf(up, sizeof up, "%s:%s", user, pass);
        if (b64_encode((const unsigned char *)up, strlen(up), b64, sizeof b64) == 0)
            snprintf(authhdr, sizeof authhdr, "Authorization: Basic %s\r\n", b64);
    }
    status = cli_http_post(host, port, "/api/permissions", body, authhdr);
    if (status == 200)
        printf("ok: full permissions on vhost '/' granted to '%s'\n", user);
    else
        fprintf(stderr, "warning: could not set permissions (HTTP %d) - grant "
                "them via POST /api/permissions\n", status);
    return 0;
}

/* ---- main ---------------------------------------------------------------- */

int main(int argc, char **argv)
{
    /* Subcommands (no broker start). */
    if (argc > 1 && strcmp(argv[1], "add-user") == 0)
        return cli_add_user(argc - 2, argv + 2);
    if (argc > 1 && (strcmp(argv[1], "--version") == 0 ||
                     strcmp(argv[1], "-v") == 0 ||
                     strcmp(argv[1], "version") == 0)) {
        printf("BeaverMQ %s (build %s)\n", BEAVER_VERSION, beaver_build_stamp);
        return 0;
    }

    log_init(LOG_LEVEL_INFO, stderr);
    /* Ignore SIGPIPE so a write to a vanished peer returns EPIPE (handled by
     * libuv) instead of killing the broker. */
    signal(SIGPIPE, SIG_IGN);

#ifdef __GLIBC__
    /* Keep freed memory inside the process for reuse instead of handing it back
     * to the kernel on every dip in load. Under bursty message rates the
     * default heap-trim + mmap/munmap behaviour causes page-fault and syscall
     * storms that surface as p99/max latency spikes; pinning these flattens the
     * tail at the cost of a slightly higher steady-state RSS. */
    mallopt(M_TRIM_THRESHOLD, -1);
    mallopt(M_MMAP_MAX, 0);
#endif

    app_t app;
    memset(&app, 0, sizeof(app));
    config_defaults(&app.config);

    /* CLI arg: a config file path, or a bare number as an AMQP port override.
     * Anything else (a typo, an unknown subcommand, a missing file) is a hard
     * error - silently starting a full broker with default config instead of
     * e.g. running `add-user` is far worse than refusing. */
    const char *cli_conf = NULL;
    int cli_port = 0;
    if (argc > 1) {
        char *end = NULL;
        long p = strtol(argv[1], &end, 10);
        if (*end == '\0' && p > 0 && p < 65536) {
            cli_port = (int)p;
        } else if (file_readable(argv[1])) {
            cli_conf = argv[1];
        } else {
            fprintf(stderr,
                "error: '%s' is neither a readable config file, a port number, "
                "nor a known subcommand\n"
                "usage: beavermq [config.conf | amqp_port]\n"
                "       beavermq add-user <name> <password> [tags] "
                "[-u admin:pw] [-H host] [-p http_port]\n"
                "       beavermq --version\n", argv[1]);
            return 2;
        }
    }

    char conf_path[600];
    if (find_config(conf_path, sizeof(conf_path), cli_conf))
        config_load_file(&app.config, conf_path);
    config_apply_env(&app.config);
    if (cli_port)
        app.config.amqp_port = cli_port;
    config_resolve_threads(&app.config);
    log_set_level(app.config.log_level);
    queue_set_default_limits(app.config.queue_max_length, app.config.queue_max_bytes);

    LOG_INFO("BeaverMQ %s (build %s) starting up "
             "(%d worker thread%s, AMQP :%d, HTTP :%d)",
             BEAVER_VERSION, beaver_build_stamp,
             app.config.threads, app.config.threads == 1 ? "" : "s",
             app.config.amqp_port, app.config.http_port);
    raise_fd_limit();

    /* Shared broker + standard exchanges. */
    app.broker = broker_new();
    if (!app.broker) {
        LOG_FATAL("failed to allocate broker core; exiting");
        return EXIT_FAILURE;
    }
    /* Standard exchanges for the default vhost. (Other vhosts get their own set
     * when they are created - see the CL_OP_ADD_VHOST apply path.) */
    broker_declare_default_exchanges(app.broker, "/");

    /* Access-control table. In a cluster it is seeded + kept consistent via Raft;
     * standalone we seed only the default vhost here. There is deliberately NO
     * default user: AMQP logins are refused until the operator creates the first
     * administrator (`beavermq add-user <name> <password>`). */
    app.authstore = authstore_new();
    if (!app.authstore) {
        LOG_FATAL("failed to allocate authstore; exiting");
        broker_free(app.broker);
        return EXIT_FAILURE;
    }
    if (!app.config.cluster_enabled) {
        authstore_add_vhost(app.authstore, "/");
        LOG_WARN("no users configured - AMQP logins are refused");
    }
    char bootstrap_token[64];
    generate_bootstrap_token(&app.config, bootstrap_token, sizeof bootstrap_token);

    atomic_init(&app.stats.next_conn_id, 0);
    atomic_init(&app.stats.total_conns, 0);
    atomic_init(&app.stats.active_conns, 0);
    atomic_init(&app.stats.total_bytes, 0);
    atomic_init(&app.stats.total_bytes_sent, 0);

    app.nworkers = app.config.threads;
    app.workers  = calloc((size_t)app.nworkers, sizeof(worker_t));
    if (!app.workers) {
        LOG_FATAL("out of memory allocating workers");
        broker_free(app.broker);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < app.nworkers; i++) {
        if (worker_init(&app, &app.workers[i], i) != 0)
            return EXIT_FAILURE; /* fatal already logged */
    }

    /* Worker 0 (the main thread) hosts the signal handlers + stats timer. */
    worker_t *w0 = &app.workers[0];
    w0->server.on_shutdown     = app_shutdown_all;
    w0->server.on_shutdown_ctx = &app;
    beaver_server_install_signals(&w0->server);
    beaver_server_install_stats(&w0->server, 2000 /* ms */);

    /* HTTP management server on a DEDICATED loop/thread (never starved by AMQP
     * load). It reads the shared broker + each worker's connection list. */
    beaver_server_t **servers = calloc((size_t)app.nworkers, sizeof(*servers));
    if (servers)
        for (int i = 0; i < app.nworkers; i++)
            servers[i] = &app.workers[i].server;

    uv_loop_init(&app.mgmt_loop);
    app.http = http_server_new(&app.mgmt_loop, app.broker, servers,
                               app.nworkers, &app.stats);
    if (app.http) {
        http_server_set_limits(app.http, app.config.http_max_body_bytes,
                               app.config.http_max_connections,
                               app.config.http_request_timeout_ms);
        http_server_set_bootstrap_token(app.http, bootstrap_token);
        char web_root[1024];
        if (resolve_web_root(&app.config, web_root, sizeof(web_root))) {
            http_server_set_web_root(app.http, web_root);
            LOG_INFO("serving web dashboard from '%s'", web_root);
        } else {
            LOG_WARN("web dashboard files not found; UI disabled (JSON API "
                     "still works; set web_root / BEAVERMQ_WEB_ROOT)");
        }
        http_server_install_shutdown_async(app.http);
        if (http_server_listen(app.http, app.config.bind_addr,
                               app.config.http_port, 128) != 0) {
            LOG_WARN("management API failed to start; continuing without it");
            http_server_free(app.http);
            app.http = NULL;
        } else if (uv_thread_create(&app.mgmt_thread, mgmt_thread_main,
                                    &app) == 0) {
            app.mgmt_started = 1;
        }
    }

    /* Cluster control plane on its own loop/thread (if configured). Fail
     * closed: an operator who asked for clustering must get a hard failure,
     * not a silently unreplicated standalone broker. */
    if (app.config.cluster_enabled && start_cluster(&app) != 0) {
        LOG_FATAL("cluster: failed to start with cluster=on; refusing to run "
                  "as an unreplicated standalone broker (see errors above)");
        return EXIT_FAILURE;
    }

    /* Spawn workers 1..N-1; worker 0 runs on this (main) thread. */
    for (int i = 1; i < app.nworkers; i++) {
        if (uv_thread_create(&app.workers[i].thread, worker_thread_main,
                             &app.workers[i]) != 0) {
            LOG_FATAL("failed to spawn worker thread %d", i);
            return EXIT_FAILURE;
        }
    }

    LOG_INFO("BeaverMQ ready on %s:%d across %d core%s; Ctrl-C to stop",
             app.config.bind_addr, app.config.amqp_port, app.nworkers,
             app.nworkers == 1 ? "" : "s");

    uv_run(&w0->loop, UV_RUN_DEFAULT);

    /* Worker 0's loop exited (shutdown). Join the other workers + mgmt thread. */
    for (int i = 1; i < app.nworkers; i++)
        uv_thread_join(&app.workers[i].thread);
    if (app.mgmt_started)
        uv_thread_join(&app.mgmt_thread);
    if (app.cluster_started)
        uv_thread_join(&app.cluster_thread);

    LOG_INFO("all workers stopped; cleaning up");

    for (int i = 0; i < app.nworkers; i++) {
        dispatcher_free(app.workers[i].dispatcher);
        beaver_server_dispose(&app.workers[i].server);
        uv_loop_close(&app.workers[i].loop);
    }
    if (app.http)
        http_server_free(app.http);
    uv_loop_close(&app.mgmt_loop);
    if (app.cluster) {
        cluster_node_free(app.cluster);
        uv_loop_close(&app.cluster_loop);
    }
    broker_free(app.broker);
    authstore_free(app.authstore);
    free(servers);
    free(app.workers);

    LOG_INFO("BeaverMQ stopped cleanly");
    log_shutdown();
    return EXIT_SUCCESS;
}
