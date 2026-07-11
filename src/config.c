/*
 * config.c - Configuration loading (defaults -> file -> environment).
 */
#include "config.h"
#include "logger.h"

#include <uv.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h> /* strcasecmp */

void config_defaults(beaver_config_t *c)
{
    c->threads   = 0; /* auto */
    c->amqp_port = 5672;
    c->http_port = 15672;
    snprintf(c->bind_addr, sizeof(c->bind_addr), "%s", "0.0.0.0");
    c->log_level = LOG_LEVEL_INFO;
    c->web_root[0] = '\0';

    c->cluster_enabled = 0;
    c->node_id         = 0;
    c->cluster_nnodes  = 0;
    for (int i = 0; i < BEAVER_MAX_CLUSTER_NODES; i++)
        c->cluster_nodes[i][0] = '\0';
    c->data_dir[0] = '\0';
}

/* Trim leading/trailing ASCII whitespace in place; returns the start. */
static char *trim(char *s)
{
    while (*s && isspace((unsigned char)*s))
        s++;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1]))
        *--end = '\0';
    return s;
}

static int parse_log_level(const char *v, log_level_t *out)
{
    if (strcasecmp(v, "debug") == 0) { *out = LOG_LEVEL_DEBUG; return 0; }
    if (strcasecmp(v, "info") == 0)  { *out = LOG_LEVEL_INFO;  return 0; }
    if (strcasecmp(v, "warn") == 0 || strcasecmp(v, "warning") == 0)
        { *out = LOG_LEVEL_WARN; return 0; }
    if (strcasecmp(v, "error") == 0) { *out = LOG_LEVEL_ERROR; return 0; }
    return -1;
}

/* threads value may be "auto" (0) or a positive integer. */
static int parse_threads(const char *v, int *out)
{
    if (strcasecmp(v, "auto") == 0) { *out = 0; return 0; }
    char *end = NULL;
    long n = strtol(v, &end, 10);
    if (*end != '\0' || n < 0 || n > BEAVER_MAX_THREADS)
        return -1;
    *out = (int)n;
    return 0;
}

/* Parse on/off/true/false/yes/no/1/0. Returns 0 on success. */
static int parse_bool(const char *v, int *out)
{
    if (strcasecmp(v, "on") == 0 || strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "yes") == 0 || strcmp(v, "1") == 0) { *out = 1; return 0; }
    if (strcasecmp(v, "off") == 0 || strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "no") == 0 || strcmp(v, "0") == 0) { *out = 0; return 0; }
    return -1;
}

/* Parse a comma-separated "ip:port,ip:port,..." list into the node table.
 * The index of each entry is its node id. Enables clustering on success. */
static void parse_cluster_nodes(beaver_config_t *c, const char *val,
                                const char *src)
{
    char buf[BEAVER_MAX_CLUSTER_NODES * 72];
    snprintf(buf, sizeof(buf), "%s", val);

    int n = 0;
    char *save = NULL;
    for (char *tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        while (*tok == ' ' || *tok == '\t') tok++;
        if (*tok == '\0')
            continue;
        if (n >= BEAVER_MAX_CLUSTER_NODES) {
            LOG_WARN("config(%s): too many cluster_nodes (max %d)", src,
                     BEAVER_MAX_CLUSTER_NODES);
            break;
        }
        if (!strrchr(tok, ':')) {
            LOG_WARN("config(%s): cluster node '%s' missing ':port'", src, tok);
            continue;
        }
        snprintf(c->cluster_nodes[n], sizeof(c->cluster_nodes[n]), "%s", tok);
        n++;
    }
    c->cluster_nnodes  = n;
    c->cluster_enabled = (n > 0);
}

static void apply_kv(beaver_config_t *c, const char *key, const char *val,
                     const char *src)
{
    if (strcmp(key, "threads") == 0) {
        if (parse_threads(val, &c->threads) != 0)
            LOG_WARN("config(%s): invalid threads '%s' (use auto or 1-%d)",
                     src, val, BEAVER_MAX_THREADS);
    } else if (strcmp(key, "amqp_port") == 0) {
        c->amqp_port = atoi(val);
    } else if (strcmp(key, "http_port") == 0) {
        c->http_port = atoi(val);
    } else if (strcmp(key, "bind") == 0 || strcmp(key, "bind_addr") == 0) {
        snprintf(c->bind_addr, sizeof(c->bind_addr), "%s", val);
    } else if (strcmp(key, "log_level") == 0) {
        if (parse_log_level(val, &c->log_level) != 0)
            LOG_WARN("config(%s): invalid log_level '%s'", src, val);
    } else if (strcmp(key, "web_root") == 0) {
        snprintf(c->web_root, sizeof(c->web_root), "%s", val);
    } else if (strcmp(key, "cluster") == 0) {
        if (parse_bool(val, &c->cluster_enabled) != 0)
            LOG_WARN("config(%s): invalid cluster '%s' (use on/off)", src, val);
    } else if (strcmp(key, "node_id") == 0) {
        c->node_id = atoi(val);
    } else if (strcmp(key, "cluster_nodes") == 0) {
        parse_cluster_nodes(c, val, src);
    } else if (strcmp(key, "data_dir") == 0) {
        snprintf(c->data_dir, sizeof(c->data_dir), "%s", val);
    } else if (strcmp(key, "election_timeout_ms") == 0) {
        c->election_timeout_ms = atoi(val);
    } else if (strcmp(key, "heartbeat_ms") == 0) {
        c->heartbeat_ms = atoi(val);
    } else {
        LOG_WARN("config(%s): unknown key '%s'", src, key);
    }
}

int config_load_file(beaver_config_t *c, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return -1;

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        /* Strip comments starting with '#'. */
        char *hash = strchr(line, '#');
        if (hash)
            *hash = '\0';

        char *s = trim(line);
        if (*s == '\0')
            continue;

        char *eq = strchr(s, '=');
        if (!eq) {
            LOG_WARN("config(%s): ignoring malformed line '%s'", path, s);
            continue;
        }
        *eq = '\0';
        char *key = trim(s);
        char *val = trim(eq + 1);
        apply_kv(c, key, val, path);
    }
    fclose(f);
    LOG_INFO("loaded configuration from '%s'", path);
    return 0;
}

void config_apply_env(beaver_config_t *c)
{
    const char *v;
    if ((v = getenv("BEAVERMQ_THREADS")))    apply_kv(c, "threads", v, "env");
    if ((v = getenv("BEAVERMQ_AMQP_PORT")))  apply_kv(c, "amqp_port", v, "env");
    if ((v = getenv("BEAVERMQ_HTTP_PORT")))  apply_kv(c, "http_port", v, "env");
    if ((v = getenv("BEAVERMQ_BIND")))       apply_kv(c, "bind", v, "env");
    if ((v = getenv("BEAVERMQ_LOG_LEVEL")))  apply_kv(c, "log_level", v, "env");
    if ((v = getenv("BEAVERMQ_WEB_ROOT")))   apply_kv(c, "web_root", v, "env");
    if ((v = getenv("BEAVERMQ_CLUSTER")))       apply_kv(c, "cluster", v, "env");
    if ((v = getenv("BEAVERMQ_NODE_ID")))       apply_kv(c, "node_id", v, "env");
    if ((v = getenv("BEAVERMQ_CLUSTER_NODES"))) apply_kv(c, "cluster_nodes", v, "env");
    if ((v = getenv("BEAVERMQ_DATA_DIR")))      apply_kv(c, "data_dir", v, "env");
}

void config_resolve_threads(beaver_config_t *c)
{
    if (c->threads <= 0) {
        unsigned int cores = uv_available_parallelism();
        c->threads = (int)cores;
    }
    if (c->threads < 1)
        c->threads = 1;
    if (c->threads > BEAVER_MAX_THREADS)
        c->threads = BEAVER_MAX_THREADS;
}
