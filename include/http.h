/*
 * http.h - Embedded HTTP management server for BeaverMQ.
 *
 * A second libuv TCP listener (default port 15672) that serves a small JSON
 * REST API for monitoring: GET /api/overview, /api/queues, /api/connections.
 * It shares the broker's event loop, so its handlers read broker state (which
 * is internally locked) and the AMQP server's connection list (owned by the
 * same loop thread) without additional synchronization.
 *
 * Phase 7 extends this server to also serve the static web dashboard.
 */
#ifndef BEAVERMQ_HTTP_H
#define BEAVERMQ_HTTP_H

#include <uv.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque dependencies (full definitions live in their own headers). */
typedef struct beaver_broker beaver_broker_t;
typedef struct beaver_server beaver_server_t;
typedef struct beaver_stats  beaver_stats_t;
typedef struct beaver_http_server beaver_http_server_t;

#define BEAVER_HTTP_DEFAULT_PORT 15672

/*
 * Create a management server bound to the given loop. `broker` supplies queue
 * statistics; `servers` is the array of per-worker servers (for enumerating
 * connections across all worker threads); `stats` supplies the shared
 * aggregate counters. Nothing is owned. Returns NULL on allocation failure.
 */
beaver_http_server_t *http_server_new(uv_loop_t *loop, beaver_broker_t *broker,
                                      beaver_server_t **servers, int nservers,
                                      beaver_stats_t *stats);

/* Set the directory served for static assets (default "web"). */
void http_server_set_web_root(beaver_http_server_t *h, const char *path);

/* Bind and start listening. Returns 0 or a negative libuv error code. */
int http_server_listen(beaver_http_server_t *h, const char *ip, int port,
                       int backlog);

/* Install an async handle so another thread can request shutdown of the
 * management loop via http_server_request_shutdown(). Call before running the
 * loop. */
int http_server_install_shutdown_async(beaver_http_server_t *h);

/* Ask the management server to shut down. Thread-safe (wakes its loop). */
void http_server_request_shutdown(beaver_http_server_t *h);

/* Close the listener and all in-flight HTTP connections (for graceful
 * shutdown). Runs on the management loop's thread. Idempotent. */
void http_server_shutdown(beaver_http_server_t *h);

/* Free the server. Call after the loop has drained (handles closed). */
void http_server_free(beaver_http_server_t *h);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_HTTP_H */
