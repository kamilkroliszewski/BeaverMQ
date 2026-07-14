/*
 * http.c - Embedded HTTP management server + JSON API.
 *
 * Minimal HTTP/1.1: each connection accumulates bytes until the end of the
 * request headers ("\r\n\r\n"), the request line is parsed, a handler builds a
 * JSON body with jansson, and a single response is written with
 * "Connection: close" - the connection is closed once the write completes.
 *
 * Lifecycle mirrors net.c: an intrusive list of live connections allows the
 * server to close everything on shutdown; per-connection memory is freed in
 * the libuv close callback.
 */
#include "http.h"
#include "net.h"
#include "broker.h"
#include "queue.h"
#include "protocol.h"
#include "cluster.h"
#include "authstore.h"
#include "logger.h"
#include "version.h"

#include <jansson.h>

#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdio.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define HTTP_MAX_HEADER (64u * 1024u)        /* reject huge request heads     */
#define HTTP_MAX_STATIC (8u * 1024u * 1024u) /* max static file size to serve */

typedef struct http_conn http_conn_t;

struct beaver_http_server {
    uv_loop_t        *loop;
    uv_tcp_t          tcp;
    beaver_broker_t  *broker;     /* not owned */
    beaver_server_t **servers;    /* per-worker servers (not owned) */
    int               nservers;
    beaver_stats_t   *stats;      /* shared aggregate counters (not owned) */
    time_t            start_time;
    char              web_root[512];
    http_conn_t      *conns;      /* intrusive list of live HTTP connections */
    uv_async_t        shutdown_async;
    int               shutdown_installed;
    int               listening;
    int               shutting_down;
};

struct http_conn {
    uv_tcp_t              handle;  /* MUST be first */
    beaver_http_server_t *server;
    char                 *inbuf;
    size_t                inbuf_len;
    size_t                inbuf_cap;
    int                   closing;
    int                   responded;
    http_conn_t          *prev, *next;
};

typedef struct {
    uv_write_t   req;   /* MUST be first */
    char        *data;
    http_conn_t *conn;
} http_write_t;

/* ---- forward decls ------------------------------------------------------- */
static void on_http_connection(uv_stream_t *server_stream, int status);
static void http_alloc(uv_handle_t *h, size_t suggested, uv_buf_t *buf);
static void on_http_read(uv_stream_t *s, ssize_t nread, const uv_buf_t *buf);
static void on_http_close(uv_handle_t *handle);
static void http_conn_close(http_conn_t *c);
static void handle_request(http_conn_t *c);

/* ---- connection list ----------------------------------------------------- */

static void conn_list_push(beaver_http_server_t *s, http_conn_t *c)
{
    c->prev = NULL;
    c->next = s->conns;
    if (s->conns)
        s->conns->prev = c;
    s->conns = c;
}

static void conn_list_remove(beaver_http_server_t *s, http_conn_t *c)
{
    if (c->prev)
        c->prev->next = c->next;
    else
        s->conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
    c->prev = c->next = NULL;
}

/* ========================================================================= */
/* JSON builders (each returns a malloc'd string the caller frees, or NULL)   */
/* ========================================================================= */

static char *build_overview(beaver_http_server_t *h)
{
    broker_stats_t s;
    broker_stats(h->broker, &s);

    json_t *root = json_object();

    json_object_set_new(root, "broker", json_string("BeaverMQ"));
    json_object_set_new(root, "version", json_string(BEAVER_VERSION));
    json_object_set_new(root, "build", json_string(beaver_build_stamp));
    json_object_set_new(root, "workers", json_integer((json_int_t)h->nservers));
    json_object_set_new(root, "uptime_seconds",
                        json_integer((json_int_t)(time(NULL) - h->start_time)));

    json_t *totals = json_object();
    json_object_set_new(totals, "queues", json_integer((json_int_t)s.queue_count));
    json_object_set_new(totals, "exchanges",
                        json_integer((json_int_t)s.exchange_count));
    json_object_set_new(totals, "connections",
                        json_integer((json_int_t)atomic_load_explicit(
                            &h->stats->active_conns, memory_order_relaxed)));
    json_object_set_new(root, "object_totals", totals);

    json_t *qt = json_object();
    json_object_set_new(qt, "messages_ready",
                        json_integer((json_int_t)s.messages_ready));
    json_object_set_new(qt, "total_enqueued",
                        json_integer((json_int_t)s.total_enqueued));
    json_object_set_new(qt, "total_dequeued",
                        json_integer((json_int_t)s.total_dequeued));
    json_object_set_new(root, "queue_totals", qt);

    json_t *ms = json_object();
    json_object_set_new(ms, "publish",
                        json_integer((json_int_t)s.messages_published));
    json_object_set_new(root, "message_stats", ms);

    json_t *net = json_object();
    json_object_set_new(net, "connections_total",
                        json_integer((json_int_t)atomic_load_explicit(
                            &h->stats->total_conns, memory_order_relaxed)));
    json_object_set_new(net, "bytes_received",
                        json_integer((json_int_t)atomic_load_explicit(
                            &h->stats->total_bytes, memory_order_relaxed)));
    json_object_set_new(net, "bytes_sent",
                        json_integer((json_int_t)atomic_load_explicit(
                            &h->stats->total_bytes_sent, memory_order_relaxed)));
    json_object_set_new(root, "network", net);

    char *out = json_dumps(root, JSON_INDENT(2));
    json_decref(root);
    return out;
}

/* broker_foreach_queue visitor: append one queue object to the array. */
static int queue_to_json(beaver_queue_t *q, void *ctx)
{
    json_t *arr = ctx;
    json_t *o = json_object();
    json_object_set_new(o, "name", json_string(queue_name(q)));
    json_object_set_new(o, "vhost", json_string(queue_vhost(q)));
    json_object_set_new(o, "messages", json_integer((json_int_t)queue_depth(q)));
    json_object_set_new(o, "consumers",
                        json_integer((json_int_t)queue_consumer_count(q)));
    json_object_set_new(o, "enqueued",
                        json_integer((json_int_t)queue_total_enqueued(q)));
    json_object_set_new(o, "dequeued",
                        json_integer((json_int_t)queue_total_dequeued(q)));
    json_object_set_new(o, "durable",
                        json_boolean(queue_flags(q) & BMQP_FLAG_DURABLE));
    json_array_append_new(arr, o);
    return 0;
}

static char *build_queues(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    broker_foreach_queue(h->broker, queue_to_json, arr);
    char *out = json_dumps(arr, JSON_INDENT(2));
    json_decref(arr);
    return out;
}

/* beaver_conn_visit_fn: append one connection object. Runs under the owning
 * worker's connection lock; reads atomic fields so there is no data race. */
static int conn_to_json(const beaver_conn_t *c, void *ctx)
{
    json_t *arr = ctx;
    json_t *o = json_object();
    json_object_set_new(o, "id", json_integer((json_int_t)c->id));
    json_object_set_new(o, "peer", json_string(c->peer));
    int st = atomic_load_explicit(&c->amqp_state, memory_order_relaxed);
    json_object_set_new(o, "state",
                        json_string(bmqp_state_name((bmqp_state_t)st)));
    json_object_set_new(o, "recv_bytes",
                        json_integer((json_int_t)atomic_load_explicit(
                            &c->bytes_received, memory_order_relaxed)));
    json_object_set_new(o, "sent_bytes",
                        json_integer((json_int_t)atomic_load_explicit(
                            &c->bytes_sent, memory_order_relaxed)));
    json_array_append_new(arr, o);
    return 0;
}

static int exchange_to_json(beaver_exchange_t *ex, void *ctx)
{
    json_t *arr = ctx;
    json_t *o = json_object();
    json_object_set_new(o, "name", json_string(exchange_name(ex)));
    json_object_set_new(o, "vhost", json_string(exchange_vhost(ex)));
    json_object_set_new(o, "type",
                        json_string(exchange_type_name(exchange_type(ex))));
    json_object_set_new(o, "bindings",
                        json_integer((json_int_t)exchange_binding_count(ex)));
    json_array_append_new(arr, o);
    return 0;
}

static char *build_exchanges(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    broker_foreach_exchange(h->broker, exchange_to_json, arr);
    char *out = json_dumps(arr, JSON_INDENT(2));
    json_decref(arr);
    return out;
}

static char *build_connections(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    for (int i = 0; i < h->nservers; i++)
        beaver_server_foreach_conn(h->servers[i], conn_to_json, arr);
    char *out = json_dumps(arr, JSON_INDENT(2));
    json_decref(arr);
    return out;
}

static const char *cl_role_name(cluster_role_t r)
{
    switch (r) {
    case CL_ROLE_LEADER:    return "leader";
    case CL_ROLE_CANDIDATE: return "candidate";
    default:                return "follower";
    }
}

static const char *cl_state_name(cluster_state_t s)
{
    switch (s) {
    case CL_STATE_HEALTHY:   return "healthy";
    case CL_STATE_SYNCING:   return "syncing";
    case CL_STATE_DEGRADED:  return "degraded";
    default:                 return "no_quorum";
    }
}

/* Cluster status. The cluster node hangs off any worker server (set once the
 * cluster loop starts); NULL means clustering is disabled on this node. */
static char *build_cluster(beaver_http_server_t *h)
{
    struct cluster_node *cl = (h->nservers > 0) ? h->servers[0]->cluster : NULL;
    json_t *root = json_object();
    if (!cl) {
        json_object_set_new(root, "enabled", json_false());
        char *out = json_dumps(root, JSON_INDENT(2));
        json_decref(root);
        return out;
    }

    cluster_status_t st;
    cluster_get_status(cl, &st);
    json_object_set_new(root, "enabled", json_true());
    json_object_set_new(root, "self_id", json_integer(st.self_id));
    json_object_set_new(root, "leader_id", json_integer(st.leader_id));
    json_object_set_new(root, "term", json_integer((json_int_t)st.term));
    json_object_set_new(root, "role", json_string(cl_role_name(st.role)));
    json_object_set_new(root, "health",
                        json_string(st.health == CL_HEALTH_OK ? "ok" : "isolated"));
    json_object_set_new(root, "state", json_string(cl_state_name(st.state)));
    json_object_set_new(root, "quorum", json_integer(st.quorum));
    json_object_set_new(root, "nodes", json_integer(st.nnodes));
    json_object_set_new(root, "commit_index", json_integer((json_int_t)st.commit_index));
    json_object_set_new(root, "last_index", json_integer((json_int_t)st.last_index));
    json_object_set_new(root, "target_index", json_integer((json_int_t)st.target_index));

    json_t *members = json_array();
    for (int i = 0; i < st.nmembers; i++) {
        cluster_member_t *m = &st.members[i];
        json_t *o = json_object();
        json_object_set_new(o, "node_id", json_integer(m->node_id));
        json_object_set_new(o, "address", json_string(m->addr));
        json_object_set_new(o, "self", json_boolean(m->is_self));
        json_object_set_new(o, "leader", json_boolean(m->is_leader));
        json_object_set_new(o, "reachable", json_boolean(m->reachable));
        json_object_set_new(o, "replicated_index", json_integer((json_int_t)m->match_index));
        /* how far this node is behind the replication target (0 = in sync) */
        json_int_t behind = (json_int_t)st.target_index - (json_int_t)m->match_index;
        json_object_set_new(o, "behind", json_integer(behind > 0 ? behind : 0));
        json_array_append_new(members, o);
    }
    json_object_set_new(root, "members", members);

    char *out = json_dumps(root, JSON_INDENT(2));
    json_decref(root);
    return out;
}

static char *build_root_index(void)
{
    json_t *root = json_object();
    json_object_set_new(root, "service", json_string("BeaverMQ Management API"));
    json_t *eps = json_array();
    json_array_append_new(eps, json_string("/api/overview"));
    json_array_append_new(eps, json_string("/api/queues"));
    json_array_append_new(eps, json_string("/api/exchanges"));
    json_array_append_new(eps, json_string("/api/connections"));
    json_array_append_new(eps, json_string("/api/cluster"));
    json_object_set_new(root, "endpoints", eps);
    char *out = json_dumps(root, JSON_INDENT(2));
    json_decref(root);
    return out;
}

/* ========================================================================= */
/* response writing                                                           */
/* ========================================================================= */

static void on_http_write(uv_write_t *req, int status)
{
    http_write_t *wr = (http_write_t *)req;
    if (status < 0)
        LOG_DEBUG("http write failed: %s", uv_strerror(status));
    http_conn_t *c = wr->conn;
    free(wr->data);
    free(wr);
    http_conn_close(c); /* one request per connection: close after responding */
}

static void http_respond(http_conn_t *c, int status, const char *status_text,
                         const char *ctype, const char *body, size_t body_len)
{
    if (c->responded || c->closing)
        return;
    c->responded = 1;

    char header[256];
    int hn = snprintf(header, sizeof(header),
                      "HTTP/1.1 %d %s\r\n"
                      "Content-Type: %s\r\n"
                      "Content-Length: %zu\r\n"
                      "Connection: close\r\n"
                      "\r\n",
                      status, status_text, ctype, body_len);
    if (hn < 0) {
        http_conn_close(c);
        return;
    }

    size_t total = (size_t)hn + body_len;
    char *resp = malloc(total);
    http_write_t *wr = malloc(sizeof(*wr));
    if (!resp || !wr) {
        free(resp);
        free(wr);
        http_conn_close(c);
        return;
    }
    memcpy(resp, header, (size_t)hn);
    if (body_len)
        memcpy(resp + hn, body, body_len);

    wr->data = resp;
    wr->conn = c;
    uv_buf_t buf = uv_buf_init(resp, (unsigned int)total);
    int rc = uv_write(&wr->req, (uv_stream_t *)&c->handle, &buf, 1,
                      on_http_write);
    if (rc != 0) {
        LOG_DEBUG("uv_write (http) failed: %s", uv_strerror(rc));
        free(resp);
        free(wr);
        http_conn_close(c);
    }
}

/* Send a JSON body (200), or 500 if serialization failed. Frees `json`. */
static void http_respond_json(http_conn_t *c, char *json)
{
    if (!json) {
        const char *err = "{\"error\":\"internal serialization error\"}";
        http_respond(c, 500, "Internal Server Error", "application/json",
                     err, strlen(err));
        return;
    }
    http_respond(c, 200, "OK", "application/json", json, strlen(json));
    free(json);
}

/* 401 with WWW-Authenticate so browsers show a login prompt (that is the GUI's
 * login screen: authenticate once, the browser then attaches the credentials to
 * every same-origin request including the dashboard's fetch() calls). */
static void http_respond_401(http_conn_t *c)
{
    const char *body = "{\"error\":\"authentication required\"}";
    char resp[512];
    int n = snprintf(resp, sizeof resp,
                     "HTTP/1.1 401 Unauthorized\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %zu\r\n"
                     "WWW-Authenticate: Basic realm=\"BeaverMQ management\"\r\n"
                     "Connection: close\r\n\r\n%s",
                     strlen(body), body);
    http_write_t *wr = malloc(sizeof(*wr));
    char *data = n > 0 ? strndup(resp, (size_t)n) : NULL;
    if (!wr || !data) { free(wr); free(data); http_conn_close(c); return; }
    wr->data = data; wr->conn = c;
    uv_buf_t buf = uv_buf_init(data, (unsigned)n);
    if (uv_write(&wr->req, (uv_stream_t *)&c->handle, &buf, 1, on_http_write)) {
        free(data); free(wr); http_conn_close(c);
    }
}

static int b64_val(int ch)
{
    if (ch >= 'A' && ch <= 'Z') return ch - 'A';
    if (ch >= 'a' && ch <= 'z') return ch - 'a' + 26;
    if (ch >= '0' && ch <= '9') return ch - '0' + 52;
    if (ch == '+') return 62;
    if (ch == '/') return 63;
    return -1;
}
static size_t b64_decode(const char *in, size_t inlen, char *out, size_t outcap)
{
    size_t o = 0; int acc = 0, bits = 0;
    for (size_t i = 0; i < inlen && in[i] != '='; i++) {
        int v = b64_val((unsigned char)in[i]);
        if (v < 0) return 0;
        acc = (acc << 6) | v; bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (o + 1 >= outcap) return 0;
            out[o++] = (char)((acc >> bits) & 0xFF);
        }
    }
    out[o] = '\0';
    return o;
}

/* Is this HTTP connection's peer the loopback interface? Used to confine the
 * first-boot bootstrap window (no users -> open access) to localhost, so a
 * remote attacker can never race the operator to create the first admin. */
static int http_peer_is_loopback(http_conn_t *c)
{
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getpeername(&c->handle, (struct sockaddr *)&ss, &len) != 0)
        return 0;
    if (ss.ss_family == AF_INET) {
        uint32_t a = ntohl(((struct sockaddr_in *)&ss)->sin_addr.s_addr);
        return (a >> 24) == 127;             /* 127.0.0.0/8 */
    }
    if (ss.ss_family == AF_INET6) {
        const struct in6_addr *a6 = &((struct sockaddr_in6 *)&ss)->sin6_addr;
        if (IN6_IS_ADDR_LOOPBACK(a6))
            return 1;
        if (IN6_IS_ADDR_V4MAPPED(a6))        /* ::ffff:127.x.x.x */
            return a6->s6_addr[12] == 127;
    }
    return 0;
}

/* Authenticate the request against the authstore via HTTP Basic auth.
 * Returns: 3 = bootstrap window (loopback, no users yet), 2 = administrator,
 * 1 = valid non-admin user, 0 = no/bad credentials. The authenticated
 * username is copied into user_out ("" in the bootstrap window).
 * While the store has NO users (first boot), LOOPBACK requests are allowed
 * so the local operator can create the initial admin; remote requests are
 * refused even then. The moment a user exists, all management access requires
 * credentials. */
static int mgmt_authenticate(http_conn_t *c, const char *head, size_t head_len,
                             char *user_out, size_t user_cap)
{
    if (user_cap)
        user_out[0] = '\0';
    authstore_t *as = c->server->nservers > 0
                      ? (authstore_t *)c->server->servers[0]->authstore : NULL;
    if (!as) return http_peer_is_loopback(c) ? 3 : 0;
    if (authstore_is_open(as))              /* bootstrap window */
        return http_peer_is_loopback(c) ? 3 : 0;

    /* Find "Authorization: Basic <b64>" among the request headers. */
    for (const char *p = head; p + 21 < head + head_len; p++) {
        if ((p == head || p[-1] == '\n') &&
            strncasecmp(p, "Authorization:", 14) == 0) {
            p += 14;
            while (*p == ' ') p++;
            if (strncasecmp(p, "Basic", 5) != 0) return 0;
            p += 5;
            while (*p == ' ') p++;
            const char *e = p;
            while (e < head + head_len && *e != '\r' && *e != '\n') e++;
            char plain[300];
            if (!b64_decode(p, (size_t)(e - p), plain, sizeof plain)) return 0;
            char *colon = strchr(plain, ':');
            if (!colon) return 0;
            *colon = '\0';
            if (!authstore_verify(as, plain, colon + 1)) return 0;
            uint32_t tags = authstore_user_tags(as, plain);
            size_t ul = strlen(plain);
            if (user_cap) {
                if (ul >= user_cap) ul = user_cap - 1;
                memcpy(user_out, plain, ul);
                user_out[ul] = '\0';
            }
            return (tags & AUTH_TAG_ADMINISTRATOR) ? 2 : 1;
        }
    }
    return 0;
}

/* Serialize a json_t (transferring ownership), then respond + free. */
static void respond_obj(http_conn_t *c, json_t *o)
{
    char *out = json_dumps(o, JSON_INDENT(2));
    json_decref(o);
    http_respond_json(c, out);
}

static void http_respond_error(http_conn_t *c, int status, const char *text,
                               const char *message)
{
    char body[256];
    int n = snprintf(body, sizeof(body), "{\"error\":\"%s\"}", message);
    http_respond(c, status, text, "application/json", body, (size_t)n);
}

/* ========================================================================= */
/* static file serving (the web dashboard)                                    */
/* ========================================================================= */

static const char *content_type_for(const char *path)
{
    const char *dot = strrchr(path, '.');
    if (!dot)
        return "application/octet-stream";
    if (strcmp(dot, ".html") == 0) return "text/html; charset=utf-8";
    if (strcmp(dot, ".js") == 0)   return "application/javascript; charset=utf-8";
    if (strcmp(dot, ".css") == 0)  return "text/css; charset=utf-8";
    if (strcmp(dot, ".json") == 0) return "application/json";
    if (strcmp(dot, ".svg") == 0)  return "image/svg+xml";
    if (strcmp(dot, ".png") == 0)  return "image/png";
    if (strcmp(dot, ".ico") == 0)  return "image/x-icon";
    if (strcmp(dot, ".map") == 0)  return "application/json";
    return "application/octet-stream";
}

/* Serve a file from the configured web root. "/" maps to /index.html. Rejects
 * any path containing ".." to prevent directory traversal. */
static void serve_static(http_conn_t *c, const char *path)
{
    beaver_http_server_t *h = c->server;

    const char *rel = (strcmp(path, "/") == 0) ? "/index.html" : path;
    if (rel[0] != '/' || strstr(rel, "..") != NULL) {
        http_respond_error(c, 403, "Forbidden", "invalid path");
        return;
    }

    char full[1024];
    int n = snprintf(full, sizeof(full), "%s%s", h->web_root, rel);
    if (n < 0 || (size_t)n >= sizeof(full)) {
        http_respond_error(c, 414, "URI Too Long", "path too long");
        return;
    }

    FILE *f = fopen(full, "rb");
    if (!f) {
        http_respond_error(c, 404, "Not Found", "file not found");
        return;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        http_respond_error(c, 500, "Internal Server Error", "seek failed");
        return;
    }
    long sz = ftell(f);
    rewind(f);
    if (sz < 0 || (unsigned long)sz > HTTP_MAX_STATIC) {
        fclose(f);
        http_respond_error(c, 500, "Internal Server Error", "file unreadable");
        return;
    }

    char *body = malloc((size_t)sz ? (size_t)sz : 1);
    if (!body) {
        fclose(f);
        http_respond_error(c, 500, "Internal Server Error", "out of memory");
        return;
    }
    size_t rd = fread(body, 1, (size_t)sz, f);
    fclose(f);
    if (rd != (size_t)sz) {
        free(body);
        http_respond_error(c, 500, "Internal Server Error", "read failed");
        return;
    }

    http_respond(c, 200, "OK", content_type_for(rel), body, (size_t)sz);
    free(body);
}

/* ========================================================================= */
/* access-control management API (vhosts / users / permissions)               */
/* ========================================================================= */

static authstore_t *http_auth(beaver_http_server_t *h)
{
    return h->nservers > 0 ? (authstore_t *)h->servers[0]->authstore : NULL;
}
static struct cluster_node *http_cluster(beaver_http_server_t *h)
{
    return h->nservers > 0 ? h->servers[0]->cluster : NULL;
}

static void vh_cb(const char *v, void *ctx)
{
    json_array_append_new((json_t *)ctx, json_string(v));
}
static json_t *build_vhosts(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    if (http_auth(h)) authstore_foreach_vhost(http_auth(h), vh_cb, arr);
    return arr;
}
static void usr_cb(const char *u, uint32_t tags, void *ctx)
{
    json_t *o = json_object();
    json_object_set_new(o, "name", json_string(u));
    json_t *t = json_array();
    if (tags & AUTH_TAG_ADMINISTRATOR) json_array_append_new(t, json_string("administrator"));
    if (tags & AUTH_TAG_MANAGEMENT)    json_array_append_new(t, json_string("management"));
    json_object_set_new(o, "tags", t);
    json_array_append_new((json_t *)ctx, o);
}
static json_t *build_users(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    if (http_auth(h)) authstore_foreach_user(http_auth(h), usr_cb, arr);
    return arr;
}
static void perm_cb(const char *user, const char *vhost, const char *conf,
                    const char *write, const char *read, void *ctx)
{
    json_t *o = json_object();
    json_object_set_new(o, "user", json_string(user));
    json_object_set_new(o, "vhost", json_string(vhost));
    json_object_set_new(o, "configure", json_string(conf));
    json_object_set_new(o, "write", json_string(write));
    json_object_set_new(o, "read", json_string(read));
    json_array_append_new((json_t *)ctx, o);
}
static json_t *build_permissions(beaver_http_server_t *h)
{
    json_t *arr = json_array();
    if (http_auth(h)) authstore_foreach_perm(http_auth(h), perm_cb, arr);
    return arr;
}

/* Apply a config change: through Raft when clustered (consistent on every node),
 * else directly on the local authstore. `op` selects the mutation; unused string
 * args are passed as "". Returns 0 on success, -1 on bad input/OOM, or -2 when
 * the cluster cannot currently commit (no leader / no quorum). */
enum cfg_op { CFG_ADD_VHOST, CFG_DEL_VHOST, CFG_ADD_USER, CFG_DEL_USER,
              CFG_SET_PERM, CFG_CLEAR_PERM };
static int cfg_apply(beaver_http_server_t *h, enum cfg_op op,
                     const char *a, const char *b, const char *c2,
                     const char *d, const char *e, uint32_t tags)
{
    struct cluster_node *cl = http_cluster(h);
    authstore_t *as = http_auth(h);
    if (!as) return -1;
    if (cl) {
        /* Without a reachable leader a proposal would only sit in the inbox
         * while the client saw a bogus "ok" - refuse honestly instead, so the
         * operator learns the cluster is down rather than "losing" the change. */
        if (cluster_health(cl) != CL_HEALTH_OK)
            return -2;
        switch (op) {
        case CFG_ADD_VHOST:  return cluster_replicate_add_vhost(cl, a);
        case CFG_DEL_VHOST:  return cluster_replicate_del_vhost(cl, a);
        case CFG_ADD_USER:   return cluster_replicate_add_user(cl, a, b, tags);
        case CFG_DEL_USER:   return cluster_replicate_del_user(cl, a);
        case CFG_SET_PERM:   return cluster_replicate_set_perm(cl, a, b, c2, d, e);
        case CFG_CLEAR_PERM: return cluster_replicate_clear_perm(cl, a, b);
        }
    } else {
        switch (op) {
        case CFG_ADD_VHOST: {
            int rc = authstore_add_vhost(as, a);
            if (rc == 0)   /* a vhost carries its own standard exchange set */
                broker_declare_default_exchanges(h->broker, a);
            return rc;
        }
        case CFG_DEL_VHOST:  return authstore_del_vhost(as, a);
        case CFG_ADD_USER:   return authstore_add_user(as, a, b, tags);
        case CFG_DEL_USER:   return authstore_del_user(as, a);
        case CFG_SET_PERM:   return authstore_set_perm(as, a, b, c2, d, e);
        case CFG_CLEAR_PERM: return authstore_clear_perm(as, a, b);
        }
    }
    return -1;
}

static const char *jstr(json_t *o, const char *k, const char *dflt)
{
    json_t *v = json_object_get(o, k);
    return (v && json_is_string(v)) ? json_string_value(v) : dflt;
}

/* Vhost/user names end up in the broker's composite "<vhost>\x01<name>"
 * registry keys and in log lines, so control bytes are never allowed (same
 * rule the AMQP layer enforces). */
static int mgmt_name_ok(const char *s)
{
    if (!s || !s[0])
        return 0;
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || (unsigned char)*s == 0x7f)
            return 0;
    return 1;
}

/* URL-decode in place (handles %XX and '+'); returns the new length. */
static size_t url_decode(char *s)
{
    char *o = s;
    for (char *p = s; *p; p++) {
        if (*p == '%' && p[1] && p[2]) {
            int hi = p[1], lo = p[2];
            hi = (hi>='0'&&hi<='9')?hi-'0':(hi|32)-'a'+10;
            lo = (lo>='0'&&lo<='9')?lo-'0':(lo|32)-'a'+10;
            *o++ = (char)((hi<<4)|lo); p += 2;
        } else if (*p == '+') *o++ = ' ';
        else *o++ = *p;
    }
    *o = '\0';
    return (size_t)(o - s);
}

static uint32_t parse_tags(const char *s)
{
    uint32_t t = 0;
    if (s && strstr(s, "administrator")) t |= AUTH_TAG_ADMINISTRATOR;
    if (s && strstr(s, "management"))    t |= AUTH_TAG_MANAGEMENT;
    return t;
}

/* Answer a cfg_apply() outcome: ok, 400 (bad input), or 503 (cluster down). */
static void respond_cfg_result(http_conn_t *c, int rc, const char *badreq_msg)
{
    if (rc == -2) {
        http_respond_error(c, 503, "Service Unavailable",
                           "cluster has no leader/quorum; check /api/cluster");
    } else if (rc) {
        http_respond_error(c, 400, "Bad Request", badreq_msg);
    } else {
        json_t *o = json_object();
        json_object_set_new(o, "status", json_string("ok"));
        respond_obj(c, o);
    }
}

/* Handle POST/DELETE on the management endpoints. Returns 1 if it owned the
 * route (responded), 0 if the path is not a management mutation. */
static int handle_mgmt(http_conn_t *c, const char *method, const char *path,
                       const char *body, size_t body_len)
{
    beaver_http_server_t *h = c->server;
    int is_post = strcmp(method, "POST") == 0;
    int is_del  = strcmp(method, "DELETE") == 0;
    if (!is_post && !is_del) return 0;

    if (is_post && strcmp(path, "/api/vhosts") == 0) {
        json_error_t err; json_t *j = json_loadb(body, body_len, 0, &err);
        const char *name = j ? jstr(j, "name", "") : "";
        int rc = mgmt_name_ok(name) ? cfg_apply(h, CFG_ADD_VHOST, name, "", "", "", "", 0) : -1;
        if (j) json_decref(j);
        respond_cfg_result(c, rc, "missing/invalid 'name'");
        return 1;
    }
    if (is_post && strcmp(path, "/api/users") == 0) {
        json_error_t err; json_t *j = json_loadb(body, body_len, 0, &err);
        const char *name = j ? jstr(j,"name","") : "";
        const char *pass = j ? jstr(j,"password","") : "";
        uint32_t tags = parse_tags(j ? jstr(j,"tags","") : "");
        int rc = -1;
        if (mgmt_name_ok(name) && pass[0]) {
            char hash[AUTHSTORE_HASH_MAX]; authstore_hash_password(pass, hash, sizeof hash);
            rc = cfg_apply(h, CFG_ADD_USER, name, hash, "", "", "", tags);
        }
        if (j) json_decref(j);
        respond_cfg_result(c, rc, "missing 'name'/'password'");
        return 1;
    }
    if (is_post && strcmp(path, "/api/permissions") == 0) {
        json_error_t err; json_t *j = json_loadb(body, body_len, 0, &err);
        const char *user = j ? jstr(j,"user","") : "";
        const char *vh   = j ? jstr(j,"vhost","") : "";
        const char *cf   = j ? jstr(j,"configure",".*") : ".*";
        const char *wr   = j ? jstr(j,"write",".*") : ".*";
        const char *rd   = j ? jstr(j,"read",".*") : ".*";
        int rc = (user[0] && vh[0]) ? cfg_apply(h, CFG_SET_PERM, user, vh, cf, wr, rd, 0) : -1;
        if (j) json_decref(j);
        respond_cfg_result(c, rc, "missing 'user'/'vhost'");
        return 1;
    }
    if (is_del && strncmp(path, "/api/vhosts/", 12) == 0) {
        char name[256]; snprintf(name, sizeof name, "%s", path + 12); url_decode(name);
        respond_cfg_result(c, cfg_apply(h, CFG_DEL_VHOST, name, "", "", "", "", 0),
                           "delete failed");
        return 1;
    }
    if (is_del && strncmp(path, "/api/users/", 11) == 0) {
        char name[256]; snprintf(name, sizeof name, "%s", path + 11); url_decode(name);
        respond_cfg_result(c, cfg_apply(h, CFG_DEL_USER, name, "", "", "", "", 0),
                           "delete failed");
        return 1;
    }
    if (is_del && strncmp(path, "/api/permissions/", 17) == 0) {
        /* /api/permissions/<user>/<vhost> */
        char rest[256]; snprintf(rest, sizeof rest, "%s", path + 17);
        char *slash = strchr(rest, '/');
        if (!slash) { http_respond_error(c, 400, "Bad Request", "need user/vhost"); return 1; }
        *slash = '\0';
        char user[256], vh[256];
        snprintf(user, sizeof user, "%s", rest); url_decode(user);
        snprintf(vh, sizeof vh, "%s", slash + 1); url_decode(vh);
        respond_cfg_result(c, cfg_apply(h, CFG_CLEAR_PERM, user, vh, "", "", "", 0),
                           "delete failed");
        return 1;
    }
    return 0;
}

/* ========================================================================= */
/* request parsing + routing                                                  */
/* ========================================================================= */

static void handle_request(http_conn_t *c)
{
    const char *buf = c->inbuf;
    size_t len = c->inbuf_len;

    /* Parse "METHOD SP PATH SP VERSION". */
    const char *sp1 = memchr(buf, ' ', len);
    if (!sp1) {
        http_respond_error(c, 400, "Bad Request", "malformed request line");
        return;
    }
    size_t method_len = (size_t)(sp1 - buf);
    const char *path_start = sp1 + 1;
    const char *sp2 = memchr(path_start, ' ', len - (size_t)(path_start - buf));
    if (!sp2) {
        http_respond_error(c, 400, "Bad Request", "malformed request line");
        return;
    }

    char method[8] = {0};
    if (method_len >= sizeof(method))
        method_len = sizeof(method) - 1;
    memcpy(method, buf, method_len);

    char path[256] = {0};
    size_t path_len = (size_t)(sp2 - path_start);
    /* Strip any query string. */
    const char *q = memchr(path_start, '?', path_len);
    if (q)
        path_len = (size_t)(q - path_start);
    if (path_len >= sizeof(path))
        path_len = sizeof(path) - 1;
    memcpy(path, path_start, path_len);

    LOG_INFO("HTTP %s %s", method, path);

    beaver_http_server_t *h = c->server;

    /* Liveness/build probe - deliberately UNAUTHENTICATED (no broker state
     * beyond the version): lets a deploy script confirm which build the
     * running process is, so a stale not-restarted broker is caught at once. */
    if (strcmp(method, "GET") == 0 && strcmp(path, "/api/healthz") == 0) {
        json_t *o = json_object();
        json_object_set_new(o, "status", json_string("ok"));
        json_object_set_new(o, "version", json_string(BEAVER_VERSION));
        json_object_set_new(o, "build", json_string(beaver_build_stamp));
        respond_obj(c, o);
        return;
    }

    /* Access control: everything (API + dashboard) requires HTTP Basic auth once
     * any user exists; mutations additionally require the administrator tag. On
     * a fresh broker (no users) access is open so the first admin can be made. */
    char *head_end = memmem(c->inbuf, c->inbuf_len, "\r\n\r\n", 4);
    size_t head_len = head_end ? (size_t)(head_end - c->inbuf) + 4 : c->inbuf_len;
    char auth_user[128];
    int auth_level = mgmt_authenticate(c, c->inbuf, head_len,
                                       auth_user, sizeof auth_user);
    if (auth_level == 0) {
        http_respond_401(c);
        return;
    }

    /* Management mutations (POST/DELETE on /api/vhosts|users|permissions). */
    if (strcmp(method, "POST") == 0 || strcmp(method, "DELETE") == 0) {
        if (auth_level == 1) {   /* valid user but not an administrator */
            http_respond_error(c, 403, "Forbidden",
                               "administrator tag required");
            return;
        }
        char *body = memmem(c->inbuf, c->inbuf_len, "\r\n\r\n", 4);
        size_t body_len = 0;
        if (body) { body += 4; body_len = c->inbuf_len - (size_t)(body - c->inbuf); }
        if (handle_mgmt(c, method, path, body ? body : "", body_len))
            return;
        /* A known read-only endpoint hit with POST/DELETE is 405, not 404. */
        if (strcmp(path, "/api/overview") == 0 || strcmp(path, "/api/queues") == 0 ||
            strcmp(path, "/api/exchanges") == 0 ||
            strcmp(path, "/api/connections") == 0 ||
            strcmp(path, "/api/cluster") == 0 || strcmp(path, "/api") == 0)
            http_respond_error(c, 405, "Method Not Allowed", "read-only endpoint");
        else
            http_respond_error(c, 404, "Not Found", "no such API resource");
        return;
    }

    if (strcmp(method, "GET") != 0) {
        http_respond_error(c, 405, "Method Not Allowed", "GET/POST/DELETE only");
        return;
    }

    if (strcmp(path, "/api/whoami") == 0) {
        /* Who is making this request: drives the GUI's login indicator. */
        json_t *o = json_object();
        json_object_set_new(o, "user", json_string(auth_user));
        json_object_set_new(o, "level",
            json_string(auth_level == 3 ? "open"
                        : auth_level == 2 ? "administrator" : "user"));
        respond_obj(c, o);
    }
    else if (strcmp(path, "/api/overview") == 0)
        http_respond_json(c, build_overview(h));
    else if (strcmp(path, "/api/queues") == 0)
        http_respond_json(c, build_queues(h));
    else if (strcmp(path, "/api/exchanges") == 0)
        http_respond_json(c, build_exchanges(h));
    else if (strcmp(path, "/api/connections") == 0)
        http_respond_json(c, build_connections(h));
    else if (strcmp(path, "/api/cluster") == 0)
        http_respond_json(c, build_cluster(h));
    else if (strcmp(path, "/api/vhosts") == 0)
        respond_obj(c, build_vhosts(h));
    else if (strcmp(path, "/api/users") == 0)
        respond_obj(c, build_users(h));
    else if (strcmp(path, "/api/permissions") == 0)
        respond_obj(c, build_permissions(h));
    else if (strcmp(path, "/api") == 0)
        http_respond_json(c, build_root_index());
    else if (strncmp(path, "/api", 4) == 0)
        http_respond_error(c, 404, "Not Found", "no such API resource");
    else
        serve_static(c, path); /* "/" -> index.html, plus app.js, css, ... */
}

/* ========================================================================= */
/* connection io                                                              */
/* ========================================================================= */

static int inbuf_append(http_conn_t *c, const char *data, size_t len)
{
    if (c->inbuf_len + len > c->inbuf_cap) {
        size_t nc = c->inbuf_cap ? c->inbuf_cap : 1024;
        while (nc < c->inbuf_len + len)
            nc *= 2;
        char *nb = realloc(c->inbuf, nc);
        if (!nb)
            return 0;
        c->inbuf     = nb;
        c->inbuf_cap = nc;
    }
    memcpy(c->inbuf + c->inbuf_len, data, len);
    c->inbuf_len += len;
    return 1;
}

static void http_alloc(uv_handle_t *handle, size_t suggested, uv_buf_t *buf)
{
    (void)handle;
    buf->base = malloc(suggested);
    buf->len  = buf->base ? suggested : 0;
}

/* Once the header block is in, a request is ready only when its body (if any,
 * per Content-Length) is also fully buffered - so POST/PUT handlers see it all. */
static int http_request_ready(http_conn_t *c)
{
    char *he = memmem(c->inbuf, c->inbuf_len, "\r\n\r\n", 4);
    if (!he) return 0;
    size_t head_len = (size_t)(he - c->inbuf) + 4;
    size_t clen = 0;
    for (char *p = c->inbuf; p + 15 < he; p++) {
        if ((p == c->inbuf || p[-1] == '\n') &&
            strncasecmp(p, "Content-Length:", 15) == 0) {
            clen = (size_t)strtoul(p + 15, NULL, 10);
            break;
        }
    }
    return c->inbuf_len >= head_len + clen;
}

static void on_http_read(uv_stream_t *stream, ssize_t nread,
                         const uv_buf_t *buf)
{
    http_conn_t *c = stream->data;

    if (nread > 0) {
        if (!inbuf_append(c, buf->base, (size_t)nread)) {
            http_respond_error(c, 500, "Internal Server Error", "out of memory");
        } else if (http_request_ready(c)) {
            handle_request(c); /* full request (head + body) received */
        } else if (c->inbuf_len > HTTP_MAX_HEADER) {
            http_respond_error(c, 431, "Request Header Fields Too Large",
                               "header too large");
        }
    } else if (nread < 0) {
        if (nread != UV_EOF)
            LOG_DEBUG("http read error: %s", uv_strerror((int)nread));
        http_conn_close(c);
    }

    free(buf->base);
}

static void on_http_close(uv_handle_t *handle)
{
    http_conn_t *c = handle->data;
    free(c->inbuf);
    free(c);
}

static void http_conn_close(http_conn_t *c)
{
    if (c->closing)
        return;
    c->closing = 1;
    conn_list_remove(c->server, c);
    uv_close((uv_handle_t *)&c->handle, on_http_close);
}

static void on_http_connection(uv_stream_t *server_stream, int status)
{
    beaver_http_server_t *s = server_stream->data;
    if (status < 0) {
        LOG_ERROR("http incoming connection error: %s", uv_strerror(status));
        return;
    }

    http_conn_t *c = calloc(1, sizeof(*c));
    if (!c) {
        LOG_ERROR("http: OOM accepting connection");
        return;
    }
    if (uv_tcp_init(s->loop, &c->handle) != 0) {
        free(c);
        return;
    }
    c->handle.data = c;
    c->server      = s;

    if (uv_accept(server_stream, (uv_stream_t *)&c->handle) != 0) {
        uv_close((uv_handle_t *)&c->handle, on_http_close);
        return;
    }
    conn_list_push(s, c);
    uv_read_start((uv_stream_t *)&c->handle, http_alloc, on_http_read);
}

/* ========================================================================= */
/* public API                                                                 */
/* ========================================================================= */

beaver_http_server_t *http_server_new(uv_loop_t *loop, beaver_broker_t *broker,
                                      beaver_server_t **servers, int nservers,
                                      beaver_stats_t *stats)
{
    beaver_http_server_t *h = calloc(1, sizeof(*h));
    if (!h)
        return NULL;
    h->loop       = loop;
    h->broker     = broker;
    h->servers    = servers;
    h->nservers   = nservers;
    h->stats      = stats;
    h->start_time = time(NULL);
    snprintf(h->web_root, sizeof(h->web_root), "%s", "web");
    return h;
}

void http_server_set_web_root(beaver_http_server_t *h, const char *path)
{
    if (h && path && path[0])
        snprintf(h->web_root, sizeof(h->web_root), "%s", path);
}

int http_server_listen(beaver_http_server_t *h, const char *ip, int port,
                       int backlog)
{
    int rc = uv_tcp_init(h->loop, &h->tcp);
    if (rc != 0) {
        LOG_ERROR("http uv_tcp_init failed: %s", uv_strerror(rc));
        return rc;
    }
    h->tcp.data = h;

    struct sockaddr_in addr;
    rc = uv_ip4_addr(ip, port, &addr);
    if (rc != 0) {
        LOG_ERROR("http invalid address %s:%d: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&h->tcp, NULL);
        return rc;
    }
    rc = uv_tcp_bind(&h->tcp, (const struct sockaddr *)&addr, 0);
    if (rc != 0) {
        LOG_ERROR("http bind %s:%d failed: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&h->tcp, NULL);
        return rc;
    }
    rc = uv_listen((uv_stream_t *)&h->tcp, backlog, on_http_connection);
    if (rc != 0) {
        LOG_ERROR("http listen on %s:%d failed: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&h->tcp, NULL);
        return rc;
    }
    h->listening = 1;
    LOG_INFO("BeaverMQ management API listening on http://%s:%d", ip, port);
    return 0;
}

static void on_http_shutdown_async(uv_async_t *handle)
{
    http_server_shutdown((beaver_http_server_t *)handle->data);
}

int http_server_install_shutdown_async(beaver_http_server_t *h)
{
    int rc = uv_async_init(h->loop, &h->shutdown_async, on_http_shutdown_async);
    if (rc != 0)
        return rc;
    h->shutdown_async.data = h;
    h->shutdown_installed  = 1;
    return 0;
}

void http_server_request_shutdown(beaver_http_server_t *h)
{
    if (h && h->shutdown_installed)
        uv_async_send(&h->shutdown_async); /* thread-safe */
}

void http_server_shutdown(beaver_http_server_t *h)
{
    if (!h || h->shutting_down)
        return;
    h->shutting_down = 1;

    http_conn_t *c = h->conns;
    while (c) {
        http_conn_t *next = c->next;
        http_conn_close(c);
        c = next;
    }
    if (h->listening && !uv_is_closing((uv_handle_t *)&h->tcp))
        uv_close((uv_handle_t *)&h->tcp, NULL);
    if (h->shutdown_installed &&
        !uv_is_closing((uv_handle_t *)&h->shutdown_async))
        uv_close((uv_handle_t *)&h->shutdown_async, NULL);
}

void http_server_free(beaver_http_server_t *h)
{
    free(h);
}
