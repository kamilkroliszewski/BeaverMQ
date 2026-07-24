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
#include "authlimit.h"
#include "crypto.h"
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
#define HTTP_DEFAULT_MAX_BODY_BYTES     (16u * 1024u * 1024u)
#define HTTP_DEFAULT_MAX_CONNECTIONS    512
#define HTTP_DEFAULT_REQUEST_TIMEOUT_MS 30000u

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
    int               n_conns;    /* length of `conns` (single-threaded loop,
                                   * so a plain counter needs no atomics) */
    uv_async_t        shutdown_async;
    int               shutdown_installed;
    int               listening;
    int               shutting_down;

    /* DoS limits (see http_server_set_limits); permissive defaults so a
     * caller that forgets to configure them still gets SOME protection. */
    uint32_t          max_body_bytes;
    int               max_connections;
    uint32_t          request_timeout_ms;

    /* Bootstrap window token (see http_server_set_bootstrap_token): while the
     * authstore has no users, a request must present this exact token (via
     * the X-Bootstrap-Token header) instead of being trusted purely for
     * appearing to originate from loopback - a loopback PEER ADDRESS proves
     * nothing about the real client behind a local reverse proxy. "" (unset)
     * means the bootstrap window is simply closed (no way in at all). */
    char              bootstrap_token[80];
};

struct http_conn {
    uv_tcp_t              handle;  /* MUST be first */
    beaver_http_server_t *server;
    char                 *inbuf;
    size_t                inbuf_len;
    size_t                inbuf_cap;
    int                   closing;
    int                   responded;
    uv_timer_t            timeout_timer; /* slowloris guard: one absolute
                                         * deadline for the whole request,
                                         * not reset per-read - a client
                                         * trickling one byte at a time to
                                         * keep resetting a rolling timer
                                         * would otherwise still starve us */
    int                   n_handles;        /* live handles (tcp [+ timer]) */
    int                   n_handles_closed;
    struct http_auth_work *pending_auth;    /* in-flight off-loop Basic-auth hash */
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
static void on_http_handle_closed(uv_handle_t *handle);
static void http_conn_close(http_conn_t *c);
static void handle_request(http_conn_t *c);
static void on_http_conn_timeout(uv_timer_t *t);

/* ---- connection list ----------------------------------------------------- */

static void conn_list_push(beaver_http_server_t *s, http_conn_t *c)
{
    c->prev = NULL;
    c->next = s->conns;
    if (s->conns)
        s->conns->prev = c;
    s->conns = c;
    s->n_conns++;
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
    s->n_conns--;
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
    /* acc is unsigned and masked to its meaningful low bits: the old signed
     * `int acc` was left-shifted every iteration without ever clearing the high
     * bits, so on a long (attacker-controlled) Authorization header the shift
     * eventually overflowed a signed int - undefined behaviour. At most bits+6
     * (<= 13) bits are ever live, so a 24-bit mask is always safe. */
    size_t o = 0; uint32_t acc = 0; int bits = 0;
    for (size_t i = 0; i < inlen && in[i] != '='; i++) {
        int v = b64_val((unsigned char)in[i]);
        if (v < 0) return 0;
        acc = ((acc << 6) | (uint32_t)v) & 0xFFFFFFu; bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (o + 1 >= outcap) return 0;
            out[o++] = (char)((acc >> bits) & 0xFF);
        }
    }
    out[o] = '\0';
    return o;
}

/* Does this request carry a matching "X-Bootstrap-Token: <token>" header?
 * Used to confine the first-boot bootstrap window (no users -> open access)
 * to whoever holds the token (see http_server_set_bootstrap_token) - NOT to
 * whoever appears to connect from loopback: a TCP peer address only tells you
 * about the socket THIS process accepted, which is always the reverse
 * proxy's address if one sits in front of the management API, regardless of
 * where the real client is. The token is not vulnerable to that confusion. */
static int http_has_bootstrap_token(http_conn_t *c, const char *head, size_t head_len)
{
    if (c->server->bootstrap_token[0] == '\0')
        return 0; /* no token configured: bootstrap window is simply closed */
    for (const char *p = head; p + 18 < head + head_len; p++) {
        if ((p == head || p[-1] == '\n') &&
            strncasecmp(p, "X-Bootstrap-Token:", 18) == 0) {
            p += 18;
            while (*p == ' ') p++;
            const char *e = p;
            while (e < head + head_len && *e != '\r' && *e != '\n') e++;
            size_t vlen = (size_t)(e - p);
            size_t tlen = strlen(c->server->bootstrap_token);
            if (vlen != tlen)
                return 0;
            return crypto_ct_memcmp(p, c->server->bootstrap_token, tlen) == 0;
        }
    }
    return 0;
}

/* Resolve the client's IP (no port) into `out`, or "" if unavailable. Used as
 * the authlimit key so all connections from one address share a backoff. */
static void http_peer_ip(http_conn_t *c, char *out, size_t cap)
{
    if (cap) out[0] = '\0';
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getpeername(&c->handle, (struct sockaddr *)&ss, &len) != 0)
        return;
    if (ss.ss_family == AF_INET)
        uv_ip4_name((struct sockaddr_in *)&ss, out, cap);
    else if (ss.ss_family == AF_INET6)
        uv_ip6_name((struct sockaddr_in6 *)&ss, out, cap);
}

/* Map a user's tags to a management auth level: 2 = administrator, 1 = read-only
 * management, -1 = authenticated but no management tag (caller answers 403). */
static int mgmt_level_from_tags(uint32_t tags)
{
    if (tags & AUTH_TAG_ADMINISTRATOR) return 2;
    if (tags & AUTH_TAG_MANAGEMENT)    return 1;
    return -1;
}

/* Forward decls: the routing continuation (run once the auth level is known),
 * and the error responder (defined further down). */
static void http_dispatch_level(http_conn_t *c, const char *method,
                                const char *path, int auth_level,
                                const char *auth_user);
static void http_respond_error(http_conn_t *c, int status, const char *text,
                               const char *message);

/* Off-loop HTTP Basic password verification (mirrors protocol.c's SASL path):
 * the expensive PBKDF2 runs on a libuv worker thread so it never blocks the
 * HTTP event loop. The work object outlives a connection teardown that happens
 * mid-hash - on_http_handle_closed nulls ->c so the completion callback (which
 * always fires) just cleans up. Auth levels: 3 = bootstrap window, 2 = admin,
 * 1 = read-only management, 0 = no/bad credentials (401), -1 = authenticated
 * but no management tag (403), -2 = rate limited (429, no hash computed). */
typedef struct http_auth_work {
    uv_work_t    req;      /* MUST be first */
    http_conn_t *c;        /* NULL once the connection is gone */
    char         user[128];
    char         ip[64];
    char         pass[256];
    char         stored[AUTHSTORE_HASH_MAX];
    uint64_t     now_ms;
    int          user_exists;
    int          result;   /* set off-loop by http_auth_work_cb */
    char         method[8];
    char         path[256];
} http_auth_work_t;

static void http_auth_work_cb(uv_work_t *req)
{
    http_auth_work_t *w = (http_auth_work_t *)req;
    w->result = w->user_exists && authstore_password_matches(w->stored, w->pass);
    memset(w->pass, 0, sizeof w->pass); /* don't linger the plaintext in the heap */
}

static void http_auth_after_cb(uv_work_t *req, int status)
{
    http_auth_work_t *w = (http_auth_work_t *)req;
    authlimit_hash_end(); /* release the slot held across the hash */

    http_conn_t *c = w->c;
    if (!c || status == UV_ECANCELED) { free(w); return; } /* connection gone */
    c->pending_auth = NULL;

    int level;
    char user[128] = "";
    if (!w->result) {
        authlimit_record_failure(w->ip, w->now_ms);
        level = 0; /* -> 401 */
    } else {
        authlimit_record_success(w->ip);
        authstore_t *as = c->server->nservers > 0
                          ? (authstore_t *)c->server->servers[0]->authstore : NULL;
        level = mgmt_level_from_tags(as ? authstore_user_tags(as, w->user) : 0);
        snprintf(user, sizeof user, "%s", w->user);
    }
    http_dispatch_level(c, w->method, w->path, level, user);
    free(w);
}

/* Begin authenticating a fully-received request. Everything that does NOT need
 * the expensive hash (bootstrap window, missing/garbled credentials, the
 * rate-limit) is resolved on the loop and dispatched immediately; a real Basic
 * credential has its stored hash looked up here, then PBKDF2 runs OFF the loop
 * and http_auth_after_cb dispatches the result. */
static void mgmt_auth_start(http_conn_t *c, const char *method, const char *path,
                            const char *head, size_t head_len)
{
    authstore_t *as = c->server->nservers > 0
                      ? (authstore_t *)c->server->servers[0]->authstore : NULL;
    if (!as || authstore_is_open(as)) {   /* bootstrap window (or no store) */
        int lvl = http_has_bootstrap_token(c, head, head_len) ? 3 : 0;
        http_dispatch_level(c, method, path, lvl, "");
        return;
    }

    /* Find "Authorization: Basic <b64>" among the request headers. */
    const char *auth = NULL;
    for (const char *p = head; p + 21 < head + head_len; p++) {
        if ((p == head || p[-1] == '\n') &&
            strncasecmp(p, "Authorization:", 14) == 0) { auth = p; break; }
    }
    if (!auth) { http_dispatch_level(c, method, path, 0, ""); return; }
    auth += 14;
    while (*auth == ' ') auth++;
    if (strncasecmp(auth, "Basic", 5) != 0) { http_dispatch_level(c, method, path, 0, ""); return; }
    auth += 5;
    while (*auth == ' ') auth++;
    const char *e = auth;
    while (e < head + head_len && *e != '\r' && *e != '\n') e++;
    char plain[300];
    if (!b64_decode(auth, (size_t)(e - auth), plain, sizeof plain)) {
        http_dispatch_level(c, method, path, 0, ""); return;
    }
    char *colon = strchr(plain, ':');
    if (!colon) { http_dispatch_level(c, method, path, 0, ""); return; }
    *colon = '\0';
    /* A username at/over the store's max length can never match a stored user
     * (names that long are rejected at creation), and would only truncate into
     * the work buffer - treat it as a bad credential without hashing. */
    if (strlen(plain) >= AUTHSTORE_NAME_MAX) {
        http_dispatch_level(c, method, path, 0, ""); return;
    }

    /* Throttle expensive hashing: a client spamming Basic auth from one IP is
     * backed off, and a flood from many IPs is bounded by the global
     * concurrent-hash cap - both deny WITHOUT hashing (429). */
    char ip[64];
    http_peer_ip(c, ip, sizeof ip);
    uint64_t now_ms = uv_now(c->server->loop);
    if (authlimit_retry_after_ms(ip, now_ms) > 0 || authlimit_hash_begin() != 0) {
        http_dispatch_level(c, method, path, -2, "");
        return;
    }

    http_auth_work_t *w = calloc(1, sizeof *w);
    if (!w) {
        authlimit_hash_end();
        http_respond_error(c, 500, "Internal Server Error", "out of memory");
        return;
    }
    w->c      = c;
    w->now_ms = now_ms;
    snprintf(w->user,   sizeof w->user,   "%s", plain);
    snprintf(w->ip,     sizeof w->ip,     "%s", ip);
    snprintf(w->pass,   sizeof w->pass,   "%s", colon + 1);
    snprintf(w->method, sizeof w->method, "%s", method);
    snprintf(w->path,   sizeof w->path,   "%s", path);
    w->user_exists = authstore_lookup_hash(as, plain, w->stored, sizeof w->stored);
    c->pending_auth = w;
    /* Stop reading until we answer: one request per connection (the response
     * closes it), so a second request must never start a second auth. */
    uv_read_stop((uv_stream_t *)&c->handle);
    if (uv_queue_work(c->server->loop, &w->req,
                      http_auth_work_cb, http_auth_after_cb) != 0) {
        c->pending_auth = NULL;
        authlimit_hash_end();
        free(w);
        http_respond_error(c, 500, "Internal Server Error", "failed to start auth");
    }
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
    /* Reject at the door what the authstore would reject anyway, so the client
     * gets a clean 400 instead of the name silently failing to apply later. */
    if (strlen(s) >= AUTHSTORE_NAME_MAX)
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

/* Parse a comma/space/semicolon separated tag list, matching each token
 * EXACTLY. The old code used strstr(), so "not-administrator" or "xmanagement"
 * granted the corresponding privilege - a token-boundary bug that could elevate
 * a user beyond what was intended. */
static uint32_t parse_tags(const char *s)
{
    uint32_t t = 0;
    if (!s) return 0;
    while (*s) {
        while (*s == ',' || *s == ' ' || *s == '\t' || *s == ';') s++;
        const char *start = s;
        while (*s && *s != ',' && *s != ' ' && *s != '\t' && *s != ';') s++;
        size_t len = (size_t)(s - start);
        if (len == 13 && strncmp(start, "administrator", 13) == 0)
            t |= AUTH_TAG_ADMINISTRATOR;
        else if (len == 10 && strncmp(start, "management", 10) == 0)
            t |= AUTH_TAG_MANAGEMENT;
    }
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
            char hash[AUTHSTORE_HASH_MAX];
            if (authstore_hash_password(pass, hash, sizeof hash) == 0)
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
        /* Reject over-long names/patterns at the door so they are never
         * truncated into a different policy than the caller sent. */
        int rc = -1;
        if (mgmt_name_ok(user) && mgmt_name_ok(vh) &&
            strlen(cf) < AUTHSTORE_REGEX_MAX && strlen(wr) < AUTHSTORE_REGEX_MAX &&
            strlen(rd) < AUTHSTORE_REGEX_MAX)
            rc = cfg_apply(h, CFG_SET_PERM, user, vh, cf, wr, rd, 0);
        if (j) json_decref(j);
        respond_cfg_result(c, rc, "missing/invalid 'user'/'vhost'/pattern");
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

/* Gate on the resolved auth level, then route the request. Called either
 * synchronously (bootstrap / bad-credential / rate-limited cases) or from the
 * off-loop hash completion (http_auth_after_cb). `method`/`path` are copies;
 * the request body is still in c->inbuf. */
static void http_dispatch_level(http_conn_t *c, const char *method,
                                const char *path, int auth_level,
                                const char *auth_user)
{
    if (c->closing || c->responded)
        return;
    beaver_http_server_t *h = c->server;

    if (auth_level == 0) {
        http_respond_401(c);
        return;
    }
    if (auth_level == -2) {
        /* Too many recent failed logins from this IP, or too many hashes in
         * flight: shed load without spending a PBKDF2 hash on this request. */
        http_respond_error(c, 429, "Too Many Requests",
                           "rate limited; slow down and retry");
        return;
    }
    if (auth_level < 0) {
        /* Valid credentials, but the user has neither the administrator nor the
         * management tag: deny outright (403) rather than expose the API. */
        http_respond_error(c, 403, "Forbidden",
                           "administrator or management tag required");
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
     * a fresh broker (no users) access is open so the first admin can be made.
     * The password hash runs OFF the event loop; routing continues in
     * http_dispatch_level once the auth level is known. */
    char *head_end = memmem(c->inbuf, c->inbuf_len, "\r\n\r\n", 4);
    size_t head_len = head_end ? (size_t)(head_end - c->inbuf) + 4 : c->inbuf_len;
    mgmt_auth_start(c, method, path, c->inbuf, head_len);
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

/* Find the end of the header block, or NULL if not seen yet. */
static char *http_header_end(http_conn_t *c)
{
    return memmem(c->inbuf, c->inbuf_len, "\r\n\r\n", 4);
}

static size_t http_content_length(http_conn_t *c, const char *he)
{
    size_t clen = 0;
    for (char *p = c->inbuf; p + 15 < he; p++) {
        if ((p == c->inbuf || p[-1] == '\n') &&
            strncasecmp(p, "Content-Length:", 15) == 0) {
            clen = (size_t)strtoul(p + 15, NULL, 10);
            break;
        }
    }
    return clen;
}

static void on_http_conn_timeout(uv_timer_t *t)
{
    http_conn_t *c = t->data;
    LOG_DEBUG("http: request timed out, closing connection");
    http_conn_close(c);
}

static void on_http_read(uv_stream_t *stream, ssize_t nread,
                         const uv_buf_t *buf)
{
    http_conn_t *c = stream->data;

    if (nread > 0) {
        if (!inbuf_append(c, buf->base, (size_t)nread)) {
            http_respond_error(c, 500, "Internal Server Error", "out of memory");
            free(buf->base);
            return;
        }
        char *he = http_header_end(c);
        /* Check the HEADER portion specifically (not the whole buffer, which
         * may already include a large-but-legitimate body) on EVERY read,
         * not only once the request looks otherwise incomplete - a complete
         * request whose headers exceed the limit but arrive in a single read
         * used to skip this check entirely (it only ran in the "not ready
         * yet" branch). */
        size_t header_len = he ? (size_t)(he - c->inbuf) + 4 : c->inbuf_len;
        if (header_len > HTTP_MAX_HEADER) {
            http_respond_error(c, 431, "Request Header Fields Too Large",
                               "header too large");
            free(buf->base);
            return;
        }
        if (he) {
            size_t clen = http_content_length(c, he);
            if (clen > c->server->max_body_bytes) {
                http_respond_error(c, 413, "Payload Too Large",
                                   "request body too large");
                free(buf->base);
                return;
            }
            if (c->inbuf_len >= header_len + clen)
                handle_request(c); /* full request (head + body) received */
        }
    } else if (nread < 0) {
        if (nread != UV_EOF)
            LOG_DEBUG("http read error: %s", uv_strerror((int)nread));
        http_conn_close(c);
    }

    free(buf->base);
}

static void on_http_handle_closed(uv_handle_t *handle)
{
    http_conn_t *c = handle->data;
    if (++c->n_handles_closed < c->n_handles)
        return;
    /* If a password verification is still running on a worker thread, detach it
     * so its completion callback (which always fires) cleans itself up without
     * touching this freed connection. The work object is freed by that callback. */
    if (c->pending_auth)
        ((http_auth_work_t *)c->pending_auth)->c = NULL;
    free(c->inbuf);
    free(c);
}

static void http_conn_close(http_conn_t *c)
{
    if (c->closing)
        return;
    c->closing = 1;
    conn_list_remove(c->server, c);
    if (c->n_handles > 1 && !uv_is_closing((uv_handle_t *)&c->timeout_timer))
        uv_close((uv_handle_t *)&c->timeout_timer, on_http_handle_closed);
    uv_close((uv_handle_t *)&c->handle, on_http_handle_closed);
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
    c->n_handles   = 1;

    if (uv_accept(server_stream, (uv_stream_t *)&c->handle) != 0) {
        uv_close((uv_handle_t *)&c->handle, on_http_handle_closed);
        return;
    }

    /* Connection cap (DoS guard): accept (to drain the backlog), then refuse -
     * `c` is already heap-allocated at this point, so closing it is just the
     * normal error-path close, no separate stack-local handle needed. */
    if (s->max_connections > 0 && s->n_conns >= s->max_connections) {
        LOG_WARN("http: refusing connection, at max_connections (%d)",
                 s->max_connections);
        uv_close((uv_handle_t *)&c->handle, on_http_handle_closed);
        return;
    }

    if (uv_timer_init(s->loop, &c->timeout_timer) == 0) {
        c->timeout_timer.data = c;
        c->n_handles = 2;
        uv_timer_start(&c->timeout_timer, on_http_conn_timeout,
                       s->request_timeout_ms, 0);
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
    h->max_body_bytes    = HTTP_DEFAULT_MAX_BODY_BYTES;
    h->max_connections   = HTTP_DEFAULT_MAX_CONNECTIONS;
    h->request_timeout_ms = HTTP_DEFAULT_REQUEST_TIMEOUT_MS;
    return h;
}

void http_server_set_web_root(beaver_http_server_t *h, const char *path)
{
    if (h && path && path[0])
        snprintf(h->web_root, sizeof(h->web_root), "%s", path);
}

void http_server_set_limits(beaver_http_server_t *h, uint32_t max_body_bytes,
                            int max_connections, uint32_t request_timeout_ms)
{
    if (!h)
        return;
    if (max_body_bytes)
        h->max_body_bytes = max_body_bytes;
    h->max_connections = max_connections;
    if (request_timeout_ms)
        h->request_timeout_ms = request_timeout_ms;
}

void http_server_set_bootstrap_token(beaver_http_server_t *h, const char *token)
{
    if (!h)
        return;
    snprintf(h->bootstrap_token, sizeof(h->bootstrap_token), "%s", token ? token : "");
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
