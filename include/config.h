/*
 * config.h - Runtime configuration for BeaverMQ.
 *
 * Settings come from (lowest to highest precedence): built-in defaults, a
 * config file (key=value), then environment variables. The worker-thread count
 * may be a fixed number or "auto" (detected from the available CPU cores).
 */
#ifndef BEAVERMQ_CONFIG_H
#define BEAVERMQ_CONFIG_H

#include "logger.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BEAVER_MAX_THREADS 64
/* Must match CLUSTER_MAX_NODES in cluster.h. */
#define BEAVER_MAX_CLUSTER_NODES 5

typedef struct {
    int         threads;        /* worker threads; 0 = auto-detect */
    int         amqp_port;
    int         http_port;
    char        bind_addr[64];
    log_level_t log_level;
    char        web_root[512];  /* "" = auto-detect */

    /* Resource limits (DoS protection). */
    int      max_connections;   /* max concurrent AMQP connections; 0 = unlimited */
    uint32_t max_message_size;  /* max message body bytes; 0 = built-in cap */
    uint32_t http_max_body_bytes;   /* max HTTP request body bytes */
    int      http_max_connections;  /* max concurrent HTTP connections; 0 = unlimited */
    uint32_t http_request_timeout_ms; /* max time to receive one full request */
    uint64_t queue_max_length;      /* max messages per queue; 0 = unlimited */
    uint64_t queue_max_bytes;       /* max body bytes per queue; 0 = unlimited */

    /* Clustering (optional; disabled unless cluster_nodes is configured). */
    int  cluster_enabled;
    int  cluster_explicit;      /* `cluster` key was set: it wins over the
                                 * implicit enable from cluster_nodes */
    int  node_id;               /* this node's id; index into cluster_nodes */
    int  cluster_nnodes;        /* number of entries parsed from cluster_nodes */
    char cluster_nodes[BEAVER_MAX_CLUSTER_NODES][72]; /* "ip:port" per node id */
    char data_dir[256];         /* persist the replicated log here ("" = off) */
    int  election_timeout_ms;   /* Raft election timeout floor; 0 = built-in default */
    int  heartbeat_ms;          /* leader heartbeat interval; 0 = built-in default */
    char cluster_secret[128];   /* shared PSK, identical on every node: without it
                                 * any host that can reach the mesh port can
                                 * impersonate a peer (forge HELLO/VOTE/APPEND
                                 * frames). Required whenever clustering is on. */
    int  cluster_durable_commit; /* 1 (default) = a leader only commits an entry
                                 * once a MAJORITY has fsync'd it (survives a
                                 * simultaneous power loss on a majority); 0 =
                                 * commit on page-cache ack alone (faster, but a
                                 * "committed" entry can be lost on power loss
                                 * before the next fsync). cluster_sync_policy
                                 * key: "durable" (default) or "fast". */
} beaver_config_t;

/* Populate with built-in defaults. */
void config_defaults(beaver_config_t *c);

/*
 * Load settings from a key=value file. Returns 0 if the file was read, or -1
 * if it could not be opened (defaults are left intact). Unknown keys are
 * warned about and skipped.
 */
int config_load_file(beaver_config_t *c, const char *path);

/* Apply BEAVERMQ_* environment-variable overrides. */
void config_apply_env(beaver_config_t *c);

/*
 * Resolve threads==0 ("auto") to the number of available CPU cores, and clamp
 * the result to [1, BEAVER_MAX_THREADS].
 */
void config_resolve_threads(beaver_config_t *c);

/* ---- filesystem helpers shared by main.c (broker startup) and supervisor.c
 * (config resolution without ever touching broker/dispatch/cluster) --------- */

/* True if `p` is readable (0400+ and exists). */
int config_file_readable(const char *p);

/* Directory containing the running executable, via /proc/self/exe. Returns 0
 * if unavailable (e.g. not on Linux, or /proc unmounted). */
int config_executable_dir(char *out, size_t outsz);

/*
 * Locate a beavermq.conf: explicit `cli` arg, then BEAVERMQ_CONF env, then
 * ./beavermq.conf, then next to the executable (and its parent dir). Returns
 * 1 and fills `out` on success, 0 if nothing was found.
 */
int config_find_file(char *out, size_t outsz, const char *cli);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_CONFIG_H */
