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

    /* Clustering (optional; disabled unless cluster_nodes is configured). */
    int  cluster_enabled;
    int  node_id;               /* this node's id; index into cluster_nodes */
    int  cluster_nnodes;        /* number of entries parsed from cluster_nodes */
    char cluster_nodes[BEAVER_MAX_CLUSTER_NODES][72]; /* "ip:port" per node id */
    char data_dir[256];         /* persist the replicated log here ("" = off) */
    int  election_timeout_ms;   /* Raft election timeout floor; 0 = built-in default */
    int  heartbeat_ms;          /* leader heartbeat interval; 0 = built-in default */
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

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_CONFIG_H */
