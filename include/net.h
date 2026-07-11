/*
 * net.h - Asynchronous, multi-threaded TCP network core for BeaverMQ.
 *
 * BeaverMQ runs one event loop per worker thread. Each worker has its own
 * beaver_server (its own SO_REUSEPORT listener, connection list, and consumer
 * dispatcher); the kernel load-balances incoming connections across the
 * workers. A connection lives entirely on one worker's loop, so per-connection
 * I/O and protocol parsing stay single-threaded.
 *
 * Shared, cross-thread state:
 *   - the broker (queues/exchanges/bindings) - guarded by its own locks,
 *   - aggregate stats (beaver_stats_t) - C11 atomics,
 *   - each server's connection list - guarded by conns_lock so the management
 *     API thread can enumerate it safely.
 * Per-connection hot counters and the protocol state are atomics so the
 * management API can read them from another thread without data races.
 */
#ifndef BEAVERMQ_NET_H
#define BEAVERMQ_NET_H

#include <uv.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BEAVER_DEFAULT_BIND    "0.0.0.0"
#define BEAVER_DEFAULT_PORT    5672    /* default AMQP port */
#define BEAVER_DEFAULT_BACKLOG 511     /* listen() backlog */

typedef struct beaver_server beaver_server_t;
typedef struct beaver_conn   beaver_conn_t;
/* Per-connection protocol state, owned by protocol.c (opaque here). */
typedef struct beaver_proto  beaver_proto_t;
/* Central broker registry, owned by broker.c (opaque here). */
typedef struct beaver_broker beaver_broker_t;
/* Consumer dispatcher, owned by dispatch.c (opaque here). */
typedef struct beaver_dispatcher beaver_dispatcher_t;
/* Reference-counted message, owned by message.c (full type in message.h). */
struct beaver_message;
/* HTTP management server, owned by http.c (opaque here). */
typedef struct beaver_http_server beaver_http_server_t;

/* Aggregate counters shared by all workers (atomic, lock-free). */
typedef struct beaver_stats {
    _Atomic uint64_t next_conn_id;  /* allocator: ids unique across workers */
    _Atomic uint64_t total_conns;   /* accepted since start */
    _Atomic int64_t  active_conns;  /* currently connected */
    _Atomic uint64_t total_bytes;   /* bytes read across all connections */
    _Atomic uint64_t total_bytes_sent; /* bytes written across all connections */
} beaver_stats_t;

/*
 * Per-connection state. The uv_tcp_t handle is embedded first so a (uv_tcp_t *)
 * and (beaver_conn_t *) are interchangeable. Hot counters and the protocol
 * state are atomic so the management API can sample them cross-thread.
 */
struct beaver_conn {
    uv_tcp_t         handle;          /* MUST be first member */
    beaver_server_t *server;          /* owning worker server */
    uint64_t         id;              /* set before listing; then read-only */
    char             peer[64];        /* "ip:port"; set before listing */

    _Atomic uint64_t bytes_received;
    _Atomic uint64_t bytes_sent;
    _Atomic int      amqp_state;      /* bmqp_state_t, published by protocol.c */

    uint32_t         frame_max;       /* negotiated AMQP max frame size */

    int              closing;         /* guard so we uv_close exactly once */
    int              send_paused;     /* delivery paused: write buffer is full */
    int              read_paused;     /* reads paused: cluster backpressure */

    beaver_proto_t  *proto;           /* AMQP protocol state (protocol.c) */

    /* Reusable read buffer: allocated once and handed to libuv on every read
     * instead of malloc/free-ing per read (the protocol layer copies the bytes
     * it needs out of it during on_read, so reuse is safe). */
    char            *rbuf;
    size_t           rbuf_cap;

    beaver_conn_t   *prev;            /* intrusive list links (server->conns) */
    beaver_conn_t   *next;
};

/* Per-worker server. One per thread. */
struct beaver_server {
    uv_loop_t           *loop;        /* this worker's loop (not owned) */
    uv_tcp_t             tcp;         /* this worker's reuseport listener */

    beaver_broker_t     *broker;      /* shared routing core (not owned) */
    struct authstore    *authstore;   /* shared access-control table (not owned) */
    beaver_dispatcher_t *dispatcher;  /* this worker's push dispatcher */
    beaver_http_server_t *http;       /* management server (worker 0 only) */
    beaver_stats_t      *stats;       /* shared aggregate counters (not owned) */
    struct cluster_node *cluster;     /* cluster control plane, or NULL (not owned) */

    uv_signal_t          sig_int;     /* SIGINT/SIGTERM (worker 0 only) */
    uv_signal_t          sig_term;
    int                  signals_installed;
    void               (*on_shutdown)(void *ctx); /* coordinate all workers */
    void                *on_shutdown_ctx;

    uv_timer_t           stats_timer; /* periodic stats log (worker 0 only) */
    int                  stats_installed;
    int64_t              last_reported;

    uv_timer_t           throttle_timer; /* polls cluster congestion to resume reads */
    int                  throttle_started;

    uv_async_t           shutdown_async; /* request shutdown from any thread */
    int                  shutdown_installed;

    pthread_mutex_t      conns_lock;  /* guards conns_head for cross-thread reads */
    beaver_conn_t       *conns_head;

    int                  shutting_down;
};

/*
 * Bind to ip:port and start listening. `reuseport` enables SO_REUSEPORT so
 * multiple workers can share the port (the kernel load-balances). Returns 0,
 * or a negative libuv error code (handle cleaned up on failure).
 */
int beaver_server_init(beaver_server_t *server, uv_loop_t *loop,
                       const char *ip, int port, int backlog, int reuseport);

/* Install SIGINT/SIGTERM handlers (call on one worker only). On signal, the
 * server invokes on_shutdown(on_shutdown_ctx) if set, else shuts itself down. */
int beaver_server_install_signals(beaver_server_t *server);

/* Periodic stats log every interval_ms (call on one worker only). */
int beaver_server_install_stats(beaver_server_t *server, uint64_t interval_ms);

/* Install an async handle so another thread can request this worker's shutdown
 * via beaver_server_request_shutdown(). */
int beaver_server_install_shutdown_async(beaver_server_t *server);

/* Ask this worker to shut down. Thread-safe (wakes the worker's loop). */
void beaver_server_request_shutdown(beaver_server_t *server);

/*
 * Begin graceful shutdown ON THIS WORKER'S LOOP THREAD: stop accepting, close
 * every live connection and the server's handles. Idempotent. After all
 * handles close, the worker's uv_run() returns.
 */
void beaver_server_shutdown(beaver_server_t *server);

/* Destroy per-server resources (the conns mutex). Call after the loop ended. */
void beaver_server_dispose(beaver_server_t *server);

/* Visit each live connection under the connection lock (for the management
 * API). The callback runs while the lock is held; it must not block. */
typedef int (*beaver_conn_visit_fn)(const beaver_conn_t *conn, void *ctx);
void beaver_server_foreach_conn(beaver_server_t *server,
                                beaver_conn_visit_fn fn, void *ctx);

/*
 * Queue `len` bytes to be written to the peer (data is copied). Returns 0, or
 * negative if closing / the write could not start. Must be called on the
 * connection's own loop thread.
 */
int beaver_conn_send(beaver_conn_t *conn, const void *data, size_t len);

/*
 * Like beaver_conn_send but TAKES OWNERSHIP of `data` (a malloc'd buffer): no
 * copy is made, and the buffer is freed when the write completes (or on
 * failure). Lets the delivery hot path build one combined frame buffer and
 * hand it off with zero extra copies.
 */
int beaver_conn_send_owned(beaver_conn_t *conn, char *data, size_t len);

/*
 * Send one delivery via scatter-gather (a single writev), avoiding a copy of the
 * body: `header` is an owned malloc'd buffer holding the Basic.Deliver method
 * frame, the content-header frame, and the body frame's 7-byte header; the
 * body bytes are taken straight from `body_msg` and the AMQP frame-end sentinel
 * is appended from a shared constant. A reference to `body_msg` is held until the
 * write completes, so the body stays alive in flight regardless of ack mode.
 * Takes ownership of `header` (frees it when done / on failure). Returns 0 on
 * success. Must be called on the connection's own loop thread.
 */
int beaver_conn_send_delivery(beaver_conn_t *conn, char *header, size_t header_len,
                              struct beaver_message *body_msg);

/* Bytes queued inside libuv waiting to be written (delivery flow control). */
size_t beaver_conn_pending_bytes(const beaver_conn_t *conn);

/* True when the outbound buffer is over the high-water mark. */
int beaver_conn_send_full(const beaver_conn_t *conn);

/* Begin closing a single connection (idempotent), on its own loop thread. */
void beaver_conn_close(beaver_conn_t *conn);

/* Pause reads on a producer connection for cluster flow control (TCP
 * backpressure). A per-server timer resumes it once the cluster reports it is no
 * longer congested. Call on the connection's own loop thread. */
void beaver_conn_throttle_read(beaver_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_NET_H */
