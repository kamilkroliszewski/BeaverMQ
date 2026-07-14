/*
 * net.c - Multi-threaded libuv TCP server for BeaverMQ.
 *
 * One beaver_server runs per worker thread, each with its own loop and a
 * SO_REUSEPORT listener (the kernel load-balances connections). A connection
 * is handled entirely on its worker's loop, so per-connection I/O and protocol
 * parsing stay single-threaded. The only shared state touched here is the
 * aggregate stats (atomics) and each server's connection list (guarded by
 * conns_lock so the management thread can enumerate it).
 *
 * Buffer strategy: read buffers are malloc'd per-read in alloc_buffer and
 * freed in on_read; idle connections hold no read buffer.
 */
#include "net.h"
#include "logger.h"
#include "protocol.h"
#include "dispatch.h"
#include "message.h"
#include "http.h"
#include "cluster.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

/* ---- forward declarations (file-local callbacks) ------------------------- */
static void on_new_connection(uv_stream_t *server_stream, int status);
static void alloc_buffer(uv_handle_t *handle, size_t suggested, uv_buf_t *buf);
static void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_conn_handle_closed(uv_handle_t *handle);
static void on_write(uv_write_t *req, int status);

typedef struct {
    uv_write_t        req;     /* MUST be first: we cast req <-> write_req_t */
    char             *data;    /* owned heap buffer, freed in on_write */
    beaver_conn_t    *conn;    /* owning connection (for drain/resume) */
    beaver_message_t *msg_ref; /* held during a scatter-gather body write; NULL otherwise */
} write_req_t;

/* A single AMQP frame-end byte (0xCE), used read-only as the trailing iovec of
 * every scatter-gather delivery. Never modified after this initializer, so it is
 * safe to share across all connections/threads. */
static uint8_t g_frame_end = 0xCE;

/* Delivery flow control: pause feeding a consumer when libuv has buffered more
 * than HIGH bytes for it; resume once it drains below LOW. */
#define CONN_SEND_HIGH_WATER (256u * 1024u)
#define CONN_SEND_LOW_WATER   (64u * 1024u)

/* ---- connection list (intrusive, guarded by server->conns_lock) ---------- */

static void conn_list_push(beaver_server_t *server, beaver_conn_t *conn)
{
    pthread_mutex_lock(&server->conns_lock);
    conn->prev = NULL;
    conn->next = server->conns_head;
    if (server->conns_head)
        server->conns_head->prev = conn;
    server->conns_head = conn;
    pthread_mutex_unlock(&server->conns_lock);
}

static void conn_list_remove(beaver_server_t *server, beaver_conn_t *conn)
{
    pthread_mutex_lock(&server->conns_lock);
    if (conn->prev)
        conn->prev->next = conn->next;
    else
        server->conns_head = conn->next; /* conn was the head */
    if (conn->next)
        conn->next->prev = conn->prev;
    conn->prev = conn->next = NULL;
    pthread_mutex_unlock(&server->conns_lock);
}

void beaver_server_foreach_conn(beaver_server_t *server,
                                beaver_conn_visit_fn fn, void *ctx)
{
    pthread_mutex_lock(&server->conns_lock);
    for (beaver_conn_t *c = server->conns_head; c; c = c->next)
        if (fn(c, ctx))
            break;
    pthread_mutex_unlock(&server->conns_lock);
}

/* Resolve and cache the peer's "ip:port" into conn->peer. */
static void fill_peername(beaver_conn_t *conn)
{
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);

    if (uv_tcp_getpeername(&conn->handle, (struct sockaddr *)&ss, &len) != 0) {
        snprintf(conn->peer, sizeof(conn->peer), "unknown");
        return;
    }

    char ip[INET6_ADDRSTRLEN] = {0};
    int port = 0;
    if (ss.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&ss;
        uv_ip4_name(s, ip, sizeof(ip));
        port = ntohs(s->sin_port);
    } else if (ss.ss_family == AF_INET6) {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&ss;
        uv_ip6_name(s, ip, sizeof(ip));
        port = ntohs(s->sin6_port);
    }
    snprintf(conn->peer, sizeof(conn->peer), "%s:%d", ip, port);
}

/* ---- read path ----------------------------------------------------------- */

/* Cap the per-connection read buffer so many idle connections don't each pin a
 * large buffer; libuv just performs more (cheap) reads for bigger bursts. */
#define CONN_READ_BUF_MAX (64u * 1024u)

static void alloc_buffer(uv_handle_t *handle, size_t suggested, uv_buf_t *buf)
{
    beaver_conn_t *conn = handle->data;
    size_t want = suggested > CONN_READ_BUF_MAX ? CONN_READ_BUF_MAX : suggested;
    if (conn->rbuf_cap < want) {
        char *nb = realloc(conn->rbuf, want);
        if (!nb) { buf->base = NULL; buf->len = 0; return; }
        conn->rbuf     = nb;
        conn->rbuf_cap = want;
    }
    buf->base = conn->rbuf;       /* reused across reads; not freed in on_read */
    buf->len  = conn->rbuf_cap;
}

static void on_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    beaver_conn_t *conn = stream->data;

    if (nread > 0) {
        conn->last_recv_ms = uv_now(conn->server->loop); /* heartbeat liveness */
        atomic_fetch_add_explicit(&conn->bytes_received, (uint64_t)nread,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&conn->server->stats->total_bytes,
                                  (uint64_t)nread, memory_order_relaxed);
        /* Drive the protocol state machine with the freshly-read bytes. */
        protocol_on_data(conn->proto, (const uint8_t *)buf->base, (size_t)nread);
    } else if (nread < 0) {
        if (nread == UV_EOF)
            LOG_INFO("conn #%" PRIu64 " (%s): closed by peer (EOF)",
                     conn->id, conn->peer);
        else
            LOG_DEBUG("conn #%" PRIu64 " (%s): read error: %s",
                      conn->id, conn->peer, uv_strerror((int)nread));
        beaver_conn_close(conn);
    }
    /* nread == 0 is the libuv equivalent of EAGAIN: nothing to do. */

    (void)buf; /* buf->base is conn->rbuf, reused across reads; freed on close */
}

/* ---- connection teardown ------------------------------------------------- */

/* Shared close callback for every uv handle embedded in the connection (the
 * TCP handle and, when initialized, the heartbeat timer). The connection is
 * freed only after the LAST handle has closed - freeing earlier would leave
 * libuv holding a pointer into freed memory. */
static void on_conn_handle_closed(uv_handle_t *handle)
{
    beaver_conn_t *conn = handle->data;
    if (++conn->n_handles_closed < conn->n_handles)
        return;
    protocol_conn_free(conn->proto); /* safe even if NULL */
    free(conn->rbuf);
    free(conn);
}

/* Close every uv handle the connection owns (exactly once each). */
static void conn_close_handles(beaver_conn_t *conn)
{
    if (conn->n_handles > 1) {
        uv_timer_stop(&conn->hb_timer);
        if (!uv_is_closing((uv_handle_t *)&conn->hb_timer))
            uv_close((uv_handle_t *)&conn->hb_timer, on_conn_handle_closed);
    }
    if (!uv_is_closing((uv_handle_t *)&conn->handle))
        uv_close((uv_handle_t *)&conn->handle, on_conn_handle_closed);
}

void beaver_conn_close(beaver_conn_t *conn)
{
    if (conn->closing)
        return; /* uv_close must be called exactly once per handle */
    conn->closing = 1;

    beaver_server_t *server = conn->server;

    /* Drop this connection's consumers (requeues their unacked messages)
     * before the handle is torn down. */
    if (server->dispatcher)
        dispatcher_remove_connection(server->dispatcher, conn);

    conn_list_remove(server, conn);
    int64_t active = atomic_fetch_sub_explicit(&server->stats->active_conns, 1,
                                               memory_order_relaxed) - 1;

    LOG_INFO("conn #%" PRIu64 " (%s): disconnected; %" PRId64 " active",
             conn->id, conn->peer, active);

    conn_close_handles(conn);
}

/* ---- heartbeats ----------------------------------------------------------- */

/* A complete AMQP heartbeat frame: type 8, channel 0, empty payload, 0xCE. */
static const uint8_t HEARTBEAT_FRAME[8] = { 8, 0, 0, 0, 0, 0, 0, 0xCE };

static void on_heartbeat_timer(uv_timer_t *timer)
{
    beaver_conn_t *conn = timer->data;
    if (conn->closing)
        return;
    uint64_t now = uv_now(conn->server->loop);
    if (now - conn->last_recv_ms > 2ull * conn->hb_interval_ms) {
        LOG_WARN("conn #%" PRIu64 " (%s): no traffic for 2 heartbeat "
                 "intervals; closing dead connection", conn->id, conn->peer);
        beaver_conn_close(conn);
        return;
    }
    beaver_conn_send(conn, HEARTBEAT_FRAME, sizeof(HEARTBEAT_FRAME));
}

void beaver_conn_enable_heartbeat(beaver_conn_t *conn, uint16_t seconds)
{
    if (seconds == 0 || conn->n_handles < 2 || conn->closing)
        return; /* heartbeats disabled, or the timer never initialized */
    conn->hb_interval_ms = (uint32_t)seconds * 1000u;
    conn->last_recv_ms   = uv_now(conn->server->loop);
    uv_timer_start(&conn->hb_timer, on_heartbeat_timer,
                   conn->hb_interval_ms / 2, conn->hb_interval_ms / 2);
}

/* ---- write path ---------------------------------------------------------- */

static void on_write(uv_write_t *req, int status)
{
    write_req_t *wr = (write_req_t *)req; /* req is the first member */
    beaver_conn_t *conn = wr->conn;
    if (status < 0)
        LOG_DEBUG("write failed: %s", uv_strerror(status));
    free(wr->data);
    if (wr->msg_ref)
        message_unref(wr->msg_ref); /* body stayed alive for the writev */
    free(wr);

    /* If deliveries were paused for backpressure and the write buffer has now
     * drained, resume feeding this connection. Pending write callbacks fire
     * before the close callback, so conn is still valid even mid-close. */
    if (conn->send_paused && !conn->closing &&
        beaver_conn_pending_bytes(conn) <= CONN_SEND_LOW_WATER) {
        conn->send_paused = 0;
        if (conn->server->dispatcher)
            dispatcher_resume_conn(conn->server->dispatcher, conn);
    }
}

size_t beaver_conn_pending_bytes(const beaver_conn_t *conn)
{
    return uv_stream_get_write_queue_size((const uv_stream_t *)&conn->handle);
}

int beaver_conn_send_full(const beaver_conn_t *conn)
{
    return beaver_conn_pending_bytes(conn) > CONN_SEND_HIGH_WATER;
}

int beaver_conn_send_owned(beaver_conn_t *conn, char *data, size_t len)
{
    if (conn->closing || len == 0) {
        free(data);
        return -1;
    }
    write_req_t *wr = malloc(sizeof(*wr));
    if (!wr) {
        LOG_ERROR("conn #%" PRIu64 ": OOM allocating write request", conn->id);
        free(data);
        return -1;
    }
    wr->data    = data; /* take ownership; no copy */
    wr->conn    = conn;
    wr->msg_ref = NULL;

    uv_buf_t buf = uv_buf_init(data, (unsigned int)len);
    int rc = uv_write(&wr->req, (uv_stream_t *)&conn->handle, &buf, 1, on_write);
    if (rc != 0) {
        LOG_ERROR("conn #%" PRIu64 ": uv_write failed: %s",
                  conn->id, uv_strerror(rc));
        free(data);
        free(wr);
        return rc;
    }
    atomic_fetch_add_explicit(&conn->bytes_sent, (uint64_t)len,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&conn->server->stats->total_bytes_sent,
                              (uint64_t)len, memory_order_relaxed);
    return 0;
}

int beaver_conn_send_delivery(beaver_conn_t *conn, char *header, size_t header_len,
                              beaver_message_t *body_msg)
{
    if (conn->closing || header_len == 0) {
        free(header);
        return -1;
    }
    write_req_t *wr = malloc(sizeof(*wr));
    if (!wr) {
        LOG_ERROR("conn #%" PRIu64 ": OOM allocating write request", conn->id);
        free(header);
        return -1;
    }
    wr->data    = header;
    wr->conn    = conn;
    wr->msg_ref = message_ref(body_msg); /* keep the body alive during the write */

    /* writev: [method+header+body-frame-header] [body bytes] [frame-end]. The
     * body is referenced in place (no copy); libuv copies the small iovec array
     * itself, but the buffers must outlive the write - hence the held ref. */
    uv_buf_t bufs[3];
    unsigned nb = 0;
    bufs[nb++] = uv_buf_init(header, (unsigned int)header_len);
    if (body_msg->body_len)
        bufs[nb++] = uv_buf_init((char *)body_msg->body,
                                 (unsigned int)body_msg->body_len);
    bufs[nb++] = uv_buf_init((char *)&g_frame_end, 1);

    int rc = uv_write(&wr->req, (uv_stream_t *)&conn->handle, bufs, nb, on_write);
    if (rc != 0) {
        LOG_ERROR("conn #%" PRIu64 ": uv_write (delivery) failed: %s",
                  conn->id, uv_strerror(rc));
        message_unref(wr->msg_ref);
        free(header);
        free(wr);
        return rc;
    }
    size_t total = header_len + body_msg->body_len + 1;
    atomic_fetch_add_explicit(&conn->bytes_sent, (uint64_t)total,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&conn->server->stats->total_bytes_sent,
                              (uint64_t)total, memory_order_relaxed);
    return 0;
}

int beaver_conn_send(beaver_conn_t *conn, const void *data, size_t len)
{
    if (conn->closing || len == 0)
        return -1;
    char *copy = malloc(len);
    if (!copy) {
        LOG_ERROR("conn #%" PRIu64 ": OOM allocating %zu-byte write buffer",
                  conn->id, len);
        return -1;
    }
    memcpy(copy, data, len);
    return beaver_conn_send_owned(conn, copy, len); /* frees copy on failure */
}

/* ---- accept path --------------------------------------------------------- */

static void on_new_connection(uv_stream_t *server_stream, int status)
{
    beaver_server_t *server = server_stream->data;

    if (status < 0) {
        LOG_ERROR("incoming connection error: %s", uv_strerror(status));
        return;
    }

    beaver_conn_t *conn = calloc(1, sizeof(*conn));
    if (!conn) {
        LOG_ERROR("out of memory accepting connection; dropping it");
        return;
    }

    int rc = uv_tcp_init(server->loop, &conn->handle);
    if (rc != 0) {
        LOG_ERROR("uv_tcp_init failed: %s", uv_strerror(rc));
        free(conn);
        return;
    }
    conn->handle.data = conn;
    conn->server      = server;
    conn->n_handles   = 1;
    conn->id = atomic_fetch_add_explicit(&server->stats->next_conn_id, 1,
                                         memory_order_relaxed) + 1;
    /* Heartbeat timer (armed later, once TuneOk negotiates an interval). */
    if (uv_timer_init(server->loop, &conn->hb_timer) == 0) {
        conn->hb_timer.data = conn;
        conn->n_handles = 2;
    }
    conn->last_recv_ms = uv_now(server->loop);

    rc = uv_accept(server_stream, (uv_stream_t *)&conn->handle);
    if (rc != 0 || server->shutting_down) {
        conn_close_handles(conn);
        return;
    }

    fill_peername(conn);

    /* Connection cap (DoS guard): accept (to drain the backlog), then refuse. */
    if (server->max_connections > 0 &&
        atomic_load_explicit(&server->stats->active_conns,
                             memory_order_relaxed) >= server->max_connections) {
        LOG_WARN("conn #%" PRIu64 " (%s): refused - max_connections (%d) reached",
                 conn->id, conn->peer, server->max_connections);
        conn_close_handles(conn);
        return;
    }

    uv_tcp_nodelay(&conn->handle, 1); /* low latency: disable Nagle */

    conn->proto = protocol_conn_new(conn);
    if (!conn->proto) {
        LOG_ERROR("conn #%" PRIu64 ": OOM allocating protocol state", conn->id);
        conn_close_handles(conn);
        return;
    }

    conn_list_push(server, conn);
    atomic_fetch_add_explicit(&server->stats->active_conns, 1,
                              memory_order_relaxed);
    uint64_t total = atomic_fetch_add_explicit(&server->stats->total_conns, 1,
                                               memory_order_relaxed) + 1;

    rc = uv_read_start((uv_stream_t *)&conn->handle, alloc_buffer, on_read);
    if (rc != 0) {
        LOG_ERROR("conn #%" PRIu64 ": uv_read_start failed: %s",
                  conn->id, uv_strerror(rc));
        beaver_conn_close(conn);
        return;
    }

    LOG_INFO("conn #%" PRIu64 " (%s): accepted (%" PRIu64 " total)",
             conn->id, conn->peer, total);
}

/* ---- periodic stats ------------------------------------------------------ */

static void on_stats_timer(uv_timer_t *timer)
{
    beaver_server_t *server = timer->data;
    int64_t active = atomic_load_explicit(&server->stats->active_conns,
                                          memory_order_relaxed);
    if (active == server->last_reported)
        return; /* only log on change to avoid spamming */
    server->last_reported = active;
    LOG_INFO("stats: %" PRId64 " active connections, %" PRIu64
             " total accepted, %" PRIu64 " bytes read",
             active,
             atomic_load_explicit(&server->stats->total_conns, memory_order_relaxed),
             atomic_load_explicit(&server->stats->total_bytes, memory_order_relaxed));
}

/* ---- shutdown coordination ----------------------------------------------- */

static void on_signal(uv_signal_t *handle, int signum)
{
    beaver_server_t *server = handle->data;
    LOG_WARN("received signal %d; initiating graceful shutdown", signum);
    if (server->on_shutdown)
        server->on_shutdown(server->on_shutdown_ctx); /* stop all workers */
    else
        beaver_server_shutdown(server);
}

static void on_shutdown_async(uv_async_t *handle)
{
    beaver_server_shutdown((beaver_server_t *)handle->data);
}

/* ---- public API ---------------------------------------------------------- */

/* ---- cluster flow control (producer read pause / resume) ----------------- */

/* While any producer is paused, this per-server timer polls the cluster's
 * congestion flag and resumes reads once it clears. */
static void on_throttle_timer(uv_timer_t *timer)
{
    beaver_server_t *server = timer->data;
    if (!server->shutting_down && server->cluster &&
        cluster_should_throttle(server->cluster))
        return; /* still congested: keep producers paused */

    pthread_mutex_lock(&server->conns_lock);
    for (beaver_conn_t *c = server->conns_head; c; c = c->next) {
        if (c->read_paused && !c->closing) {
            c->read_paused = 0;
            uv_read_start((uv_stream_t *)&c->handle, alloc_buffer, on_read);
        }
    }
    pthread_mutex_unlock(&server->conns_lock);
    uv_timer_stop(timer);
    server->throttle_started = 0;
}

void beaver_conn_throttle_read(beaver_conn_t *conn)
{
    beaver_server_t *server = conn->server;
    if (conn->read_paused || conn->closing)
        return;
    uv_read_stop((uv_stream_t *)&conn->handle);
    conn->read_paused = 1;
    if (!server->throttle_started) {
        server->throttle_started = 1;
        uv_timer_start(&server->throttle_timer, on_throttle_timer, 4, 4);
    }
}

int beaver_server_init(beaver_server_t *server, uv_loop_t *loop,
                       const char *ip, int port, int backlog, int reuseport)
{
    memset(server, 0, sizeof(*server));
    server->loop = loop;
    if (pthread_mutex_init(&server->conns_lock, NULL) != 0) {
        LOG_ERROR("pthread_mutex_init (conns) failed");
        return -1;
    }

    int rc = uv_tcp_init(loop, &server->tcp);
    if (rc != 0) {
        LOG_ERROR("uv_tcp_init (server) failed: %s", uv_strerror(rc));
        pthread_mutex_destroy(&server->conns_lock);
        return rc;
    }
    server->tcp.data = server;

    struct sockaddr_in addr;
    rc = uv_ip4_addr(ip, port, &addr);
    if (rc != 0) {
        LOG_ERROR("invalid bind address %s:%d: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&server->tcp, NULL);
        return rc;
    }

    unsigned int flags = reuseport ? UV_TCP_REUSEPORT : 0;
    rc = uv_tcp_bind(&server->tcp, (const struct sockaddr *)&addr, flags);
    if (rc != 0) {
        LOG_ERROR("bind %s:%d failed: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&server->tcp, NULL);
        return rc;
    }

    rc = uv_listen((uv_stream_t *)&server->tcp, backlog, on_new_connection);
    if (rc != 0) {
        LOG_ERROR("listen on %s:%d failed: %s", ip, port, uv_strerror(rc));
        uv_close((uv_handle_t *)&server->tcp, NULL);
        return rc;
    }

    uv_timer_init(loop, &server->throttle_timer);
    server->throttle_timer.data = server;
    return 0;
}

int beaver_server_install_signals(beaver_server_t *server)
{
    int rc = uv_signal_init(server->loop, &server->sig_int);
    if (rc != 0)
        return rc;
    server->sig_int.data = server;

    rc = uv_signal_init(server->loop, &server->sig_term);
    if (rc != 0) {
        uv_close((uv_handle_t *)&server->sig_int, NULL);
        return rc;
    }
    server->sig_term.data = server;

    uv_signal_start(&server->sig_int,  on_signal, SIGINT);
    uv_signal_start(&server->sig_term, on_signal, SIGTERM);
    server->signals_installed = 1;
    return 0;
}

int beaver_server_install_stats(beaver_server_t *server, uint64_t interval_ms)
{
    int rc = uv_timer_init(server->loop, &server->stats_timer);
    if (rc != 0)
        return rc;
    server->stats_timer.data = server;
    server->stats_installed  = 1;
    uv_timer_start(&server->stats_timer, on_stats_timer, interval_ms, interval_ms);
    return 0;
}

int beaver_server_install_shutdown_async(beaver_server_t *server)
{
    int rc = uv_async_init(server->loop, &server->shutdown_async,
                           on_shutdown_async);
    if (rc != 0)
        return rc;
    server->shutdown_async.data = server;
    server->shutdown_installed  = 1;
    return 0;
}

void beaver_server_request_shutdown(beaver_server_t *server)
{
    if (server->shutdown_installed)
        uv_async_send(&server->shutdown_async); /* thread-safe */
}

void beaver_server_shutdown(beaver_server_t *server)
{
    if (server->shutting_down)
        return;
    server->shutting_down = 1;

    /* Snapshot the connections under the lock, then close them without it
     * (beaver_conn_close re-takes the lock to unlink). */
    pthread_mutex_lock(&server->conns_lock);
    size_t n = 0;
    for (beaver_conn_t *c = server->conns_head; c; c = c->next)
        n++;
    beaver_conn_t **arr = n ? malloc(n * sizeof(*arr)) : NULL;
    size_t i = 0;
    if (arr)
        for (beaver_conn_t *c = server->conns_head; c; c = c->next)
            arr[i++] = c;
    pthread_mutex_unlock(&server->conns_lock);

    LOG_INFO("worker shutting down: closing %zu active connection(s)", n);
    if (arr) {
        for (size_t k = 0; k < n; k++)
            beaver_conn_close(arr[k]);
        free(arr);
    } else {
        /* OOM fallback: same thread, so the list is stable between removals. */
        beaver_conn_t *c = server->conns_head;
        while (c) {
            beaver_conn_t *next = c->next;
            beaver_conn_close(c);
            c = next;
        }
    }

    if (server->dispatcher)
        dispatcher_request_close(server->dispatcher);
    if (server->http)
        http_server_shutdown(server->http);

    if (!uv_is_closing((uv_handle_t *)&server->tcp))
        uv_close((uv_handle_t *)&server->tcp, NULL);

    if (server->shutdown_installed &&
        !uv_is_closing((uv_handle_t *)&server->shutdown_async))
        uv_close((uv_handle_t *)&server->shutdown_async, NULL);

    if (server->signals_installed) {
        uv_signal_stop(&server->sig_int);
        uv_signal_stop(&server->sig_term);
        if (!uv_is_closing((uv_handle_t *)&server->sig_int))
            uv_close((uv_handle_t *)&server->sig_int, NULL);
        if (!uv_is_closing((uv_handle_t *)&server->sig_term))
            uv_close((uv_handle_t *)&server->sig_term, NULL);
    }

    if (server->stats_installed) {
        uv_timer_stop(&server->stats_timer);
        if (!uv_is_closing((uv_handle_t *)&server->stats_timer))
            uv_close((uv_handle_t *)&server->stats_timer, NULL);
    }

    uv_timer_stop(&server->throttle_timer);
    if (!uv_is_closing((uv_handle_t *)&server->throttle_timer))
        uv_close((uv_handle_t *)&server->throttle_timer, NULL);
}

void beaver_server_dispose(beaver_server_t *server)
{
    pthread_mutex_destroy(&server->conns_lock);
}
