/*
 * test_protocol.c - End-to-end AMQP protocol tests, no external client.
 *
 * The whole broker stack is real (protocol + broker + queue + net), so these
 * tests exercise the actual wire path rather than the parser in isolation. The
 * seam is a socketpair: the connection's uv_tcp_t is uv_tcp_open()'d on one end,
 * we feed request bytes straight into protocol_on_data() and read the broker's
 * response frames off the other end after pumping the loop. No worker thread and
 * no dispatcher are needed for the paths under test (dispatcher_* tolerate a
 * NULL dispatcher; authstore/cluster are NULL, i.e. the no-auth, single-node
 * configuration), which keeps the harness synchronous and deterministic.
 *
 * Coverage focuses on the response *bytes* for features that previously had only
 * unit-level (non-e2e) coverage: the handshake, Queue.Declare x-overflow /
 * x-max-length arguments, Basic.Return for unroutable mandatory publishes, and
 * publisher-confirm Ack vs Nack.
 */
#include "protocol.h"
#include "frame.h"
#include "net.h"
#include "broker.h"
#include "queue.h"
#include "message.h"
#include "dispatch.h"
#include "test_util.h"

#include <uv.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ------------------------------------------------------------------------- */
/* harness                                                                    */
/* ------------------------------------------------------------------------- */

typedef struct {
    uv_loop_t        loop;
    beaver_stats_t   stats;
    beaver_broker_t *broker;
    beaver_server_t  server;
    beaver_conn_t    conn;
    beaver_proto_t  *proto;
    int              cli_fd; /* our (client) end of the socketpair */
} harness_t;

static void h_setup(harness_t *h)
{
    memset(h, 0, sizeof(*h));
    uv_loop_init(&h->loop);
    h->broker = broker_new();

    h->server.loop       = &h->loop;
    h->server.broker     = h->broker;
    h->server.stats      = &h->stats;
    h->server.authstore  = NULL;   /* no-auth: StartOk is accepted immediately */
    h->server.cluster    = NULL;
    /* A real push dispatcher on this loop, exactly as a worker has one, so the
     * Basic.Consume -> Basic.Deliver -> Basic.Ack path is exercised for real
     * (delivery is driven by the dispatcher's uv_async, serviced when we pump
     * the loop). */
    h->server.dispatcher = dispatcher_new(&h->loop, h->broker);
    CHECK(h->server.dispatcher != NULL);

    int fds[2];
    /* AF_UNIX stream pair; libuv drives it as a stream via uv_tcp_open (we never
     * call the TCP-only ops like getpeername/nodelay on it). */
    int rc = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    CHECK_EQ(rc, 0);
    fcntl(fds[1], F_SETFL, O_NONBLOCK);
    h->cli_fd = fds[1];

    uv_tcp_init(&h->loop, &h->conn.handle);
    uv_tcp_open(&h->conn.handle, fds[0]);
    h->conn.handle.data = &h->conn;
    h->conn.server      = &h->server;
    h->conn.id          = 1;
    h->conn.n_handles   = 1;  /* no hb timer: enable_heartbeat/clear are no-ops */
    snprintf(h->conn.peer, sizeof h->conn.peer, "test:0");

    h->proto = protocol_conn_new(&h->conn);
    CHECK(h->proto != NULL);
}

static void h_teardown(harness_t *h)
{
    protocol_conn_free(h->proto);
    /* Mirror the real shutdown order: request_close (closes the dispatcher's uv
     * handles), drain the loop so those closes complete, then free. */
    dispatcher_request_close(h->server.dispatcher);
    if (!uv_is_closing((uv_handle_t *)&h->conn.handle))
        uv_close((uv_handle_t *)&h->conn.handle, NULL);
    for (int i = 0; i < 32; i++)
        uv_run(&h->loop, UV_RUN_NOWAIT);
    dispatcher_free(h->server.dispatcher);
    uv_loop_close(&h->loop);
    close(h->cli_fd);
    broker_free(h->broker);
}

/* Feed request bytes into the state machine (writes are queued on the loop). */
static void h_send(harness_t *h, const uint8_t *data, size_t len)
{
    protocol_on_data(h->proto, data, len);
}

/* Pump the loop (flushing queued writes to the socket) and read everything the
 * broker sent back. Returns the number of bytes collected. */
static size_t h_collect(harness_t *h, uint8_t *out, size_t cap)
{
    size_t total = 0;
    for (int iter = 0; iter < 64 && total < cap; iter++) {
        uv_run(&h->loop, UV_RUN_NOWAIT);
        ssize_t n;
        while (total < cap && (n = read(h->cli_fd, out + total, cap - total)) > 0)
            total += (size_t)n;
    }
    return total;
}

/* ------------------------------------------------------------------------- */
/* response frame reader                                                      */
/* ------------------------------------------------------------------------- */

typedef struct { const uint8_t *buf; size_t len, pos; } rdr_t;

static void rdr_init(rdr_t *r, const uint8_t *buf, size_t len)
{
    r->buf = buf; r->len = len; r->pos = 0;
}

/* Pull the next complete frame; returns 0 if none remains. */
static int rdr_next(rdr_t *r, bmqp_frame_header_t *hdr, const uint8_t **payload)
{
    ssize_t n = bmqp_frame_parse(r->buf + r->pos, r->len - r->pos, hdr, payload);
    if (n <= 0)
        return 0;
    r->pos += (size_t)n;
    return 1;
}

/* Assert the next frame is a method frame with the given class/method. Returns a
 * pointer to the method arguments (past the 4-byte class/method header) in
 * *args and their length in *args_len when non-NULL. */
static int expect_method(rdr_t *r, uint16_t cls, uint16_t method,
                         const uint8_t **args, size_t *args_len)
{
    bmqp_frame_header_t h;
    const uint8_t *pl;
    if (!rdr_next(r, &h, &pl)) {
        CHECK(0 && "expected a method frame, got none");
        return 0;
    }
    CHECK_EQ(h.type, BMQP_FRAME_METHOD);
    uint16_t c = (uint16_t)((pl[0] << 8) | pl[1]);
    uint16_t m = (uint16_t)((pl[2] << 8) | pl[3]);
    CHECK_EQ(c, cls);
    CHECK_EQ(m, method);
    if (args)     *args     = pl + 4;
    if (args_len) *args_len = h.payload_len - 4;
    return c == cls && m == method;
}

/* ------------------------------------------------------------------------- */
/* request builders                                                           */
/* ------------------------------------------------------------------------- */

/* Append a method frame (class + method + args) on `channel` to `b`. */
static void put_method(bmqp_buf_t *b, uint16_t channel, uint16_t cls,
                       uint16_t method, const uint8_t *args, size_t alen)
{
    bmqp_buf_t pl;
    bmqp_buf_init(&pl);
    bmqp_buf_put_u16(&pl, cls);
    bmqp_buf_put_u16(&pl, method);
    if (args && alen)
        bmqp_buf_put_bytes(&pl, args, alen);
    bmqp_frame_write(b, BMQP_FRAME_METHOD, channel, pl.data, pl.len);
    bmqp_buf_free(&pl);
}

/* Send one method frame and discard whatever comes back. */
static void send_method_frame(harness_t *h, uint16_t channel, uint16_t cls,
                              uint16_t method, const uint8_t *args, size_t alen)
{
    bmqp_buf_t b;
    bmqp_buf_init(&b);
    put_method(&b, channel, cls, method, args, alen);
    h_send(h, b.data, b.len);
    bmqp_buf_free(&b);
}

/* Drive protocol header + StartOk + TuneOk + Connection.Open + Channel.Open(1),
 * draining (and discarding) all handshake responses. Leaves connection open with
 * channel 1 ready. */
static void h_handshake(harness_t *h)
{
    uint8_t scratch[8192];

    /* Protocol header -> Connection.Start. */
    h_send(h, AMQP_PROTOCOL_HEADER, AMQP_PROTOCOL_HEADER_SIZE);
    h_collect(h, scratch, sizeof scratch);

    /* Connection.StartOk: client-props(empty table) + mechanism + response + locale. */
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u32(&a, 0);                          /* client-properties: empty table */
    bmqp_buf_put_shortstr(&a, "PLAIN");               /* mechanism */
    bmqp_buf_put_longstr(&a, "\0guest\0guest", 12);   /* SASL response */
    bmqp_buf_put_shortstr(&a, "en_US");               /* locale */
    send_method_frame(h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_START_OK,
                      a.data, a.len);
    bmqp_buf_free(&a);
    h_collect(h, scratch, sizeof scratch);            /* -> Connection.Tune */

    /* Connection.TuneOk: channel-max, frame-max, heartbeat=0. */
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 2047);
    bmqp_buf_put_u32(&a, 131072);
    bmqp_buf_put_u16(&a, 0);
    send_method_frame(h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_TUNE_OK,
                      a.data, a.len);
    bmqp_buf_free(&a);

    /* Connection.Open: vhost + reserved + reserved. */
    bmqp_buf_init(&a);
    bmqp_buf_put_shortstr(&a, "/");
    bmqp_buf_put_shortstr(&a, "");
    bmqp_buf_put_u8(&a, 0);
    send_method_frame(h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_OPEN,
                      a.data, a.len);
    bmqp_buf_free(&a);
    h_collect(h, scratch, sizeof scratch);            /* -> Connection.Open-Ok */

    /* Channel.Open(1): reserved shortstr. */
    bmqp_buf_init(&a);
    bmqp_buf_put_shortstr(&a, "");
    send_method_frame(h, 1, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_OPEN, a.data, a.len);
    bmqp_buf_free(&a);
    h_collect(h, scratch, sizeof scratch);            /* -> Channel.Open-Ok */
}

/* Build a Queue.Declare method: reserved + name + bits + arguments(field table). */
static void put_queue_declare(bmqp_buf_t *out, uint16_t channel, const char *name,
                              uint8_t bits, const uint8_t *tbl, size_t tbl_len)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 0);                 /* reserved-1 */
    bmqp_buf_put_shortstr(&a, name);
    bmqp_buf_put_u8(&a, bits);
    bmqp_buf_put_longstr(&a, tbl, tbl_len);  /* arguments */
    put_method(out, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE, a.data, a.len);
    bmqp_buf_free(&a);
}

/* Publish a small message: Basic.Publish method + content header + body frame. */
static void put_publish(bmqp_buf_t *out, uint16_t channel, const char *exchange,
                        const char *rkey, int mandatory, const char *body)
{
    size_t blen = body ? strlen(body) : 0;

    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 0);                 /* reserved-1 */
    bmqp_buf_put_shortstr(&a, exchange);
    bmqp_buf_put_shortstr(&a, rkey);
    bmqp_buf_put_u8(&a, mandatory ? 0x01 : 0x00); /* mandatory bit */
    put_method(out, channel, BMQP_CLASS_BASIC, BMQP_BASIC_PUBLISH, a.data, a.len);
    bmqp_buf_free(&a);

    /* Content header: class(60) + weight(0) + body-size + property-flags(0). */
    bmqp_buf_t hdr;
    bmqp_buf_init(&hdr);
    bmqp_buf_put_u16(&hdr, BMQP_CLASS_BASIC);
    bmqp_buf_put_u16(&hdr, 0);
    bmqp_buf_put_u64(&hdr, (uint64_t)blen);
    bmqp_buf_put_u16(&hdr, 0);
    bmqp_frame_write(out, BMQP_FRAME_HEADER, channel, hdr.data, hdr.len);
    bmqp_buf_free(&hdr);

    if (blen)
        bmqp_frame_write(out, BMQP_FRAME_BODY, channel, (const uint8_t *)body, blen);
}

/* Basic.Consume: reserved + queue + consumer-tag + bits + arguments(empty). */
static void put_consume(bmqp_buf_t *out, uint16_t channel, const char *queue,
                        const char *ctag, uint8_t bits)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 0);           /* reserved-1 */
    bmqp_buf_put_shortstr(&a, queue);
    bmqp_buf_put_shortstr(&a, ctag);   /* "" => server-generated */
    bmqp_buf_put_u8(&a, bits);         /* no-local,no-ack,exclusive,no-wait */
    bmqp_buf_put_longstr(&a, "", 0);   /* arguments (empty field table) */
    put_method(out, channel, BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME, a.data, a.len);
    bmqp_buf_free(&a);
}

/* Exchange.Declare: reserved + exchange + type + bits + arguments(empty). */
static void put_exchange_declare(bmqp_buf_t *out, uint16_t channel,
                                 const char *name, const char *type)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 0);
    bmqp_buf_put_shortstr(&a, name);
    bmqp_buf_put_shortstr(&a, type);
    bmqp_buf_put_u8(&a, 0);            /* passive,durable,auto-del,internal,no-wait */
    bmqp_buf_put_longstr(&a, "", 0);  /* arguments */
    put_method(out, channel, BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE, a.data, a.len);
    bmqp_buf_free(&a);
}

/* Queue.Bind: reserved + queue + exchange + routing-key + no-wait + args. */
static void put_queue_bind(bmqp_buf_t *out, uint16_t channel, const char *queue,
                           const char *exchange, const char *rkey)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 0);
    bmqp_buf_put_shortstr(&a, queue);
    bmqp_buf_put_shortstr(&a, exchange);
    bmqp_buf_put_shortstr(&a, rkey);
    bmqp_buf_put_u8(&a, 0);            /* no-wait */
    bmqp_buf_put_longstr(&a, "", 0);  /* arguments */
    put_method(out, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND, a.data, a.len);
    bmqp_buf_free(&a);
}

/* Parse a Basic.Deliver method's arguments and return its delivery-tag. */
static uint64_t deliver_tag(const uint8_t *args, size_t alen)
{
    bmqp_reader_t r;
    bmqp_reader_init(&r, args, alen);
    size_t n;
    bmqp_read_shortstr(&r, &n); /* consumer-tag */
    return bmqp_read_u64(&r);   /* delivery-tag */
}

/* ------------------------------------------------------------------------- */
/* tests                                                                      */
/* ------------------------------------------------------------------------- */

/* The handshake produces exactly Start, Tune, Open-Ok, Channel.Open-Ok in order. */
static void test_handshake_sequence(void)
{
    TEST_SECTION("handshake: header->Start, StartOk->Tune, Open->Open-Ok, Channel.Open->Open-Ok");
    harness_t h;
    h_setup(&h);
    uint8_t buf[8192];
    rdr_t r;

    /* header -> Connection.Start */
    h_send(&h, AMQP_PROTOCOL_HEADER, AMQP_PROTOCOL_HEADER_SIZE);
    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_START, NULL, NULL);

    /* StartOk -> Connection.Tune */
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u32(&a, 0);
    bmqp_buf_put_shortstr(&a, "PLAIN");
    bmqp_buf_put_longstr(&a, "\0guest\0guest", 12);
    bmqp_buf_put_shortstr(&a, "en_US");
    send_method_frame(&h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_START_OK, a.data, a.len);
    bmqp_buf_free(&a);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_TUNE, NULL, NULL);

    /* TuneOk (no reply) + Open -> Open-Ok */
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, 2047); bmqp_buf_put_u32(&a, 131072); bmqp_buf_put_u16(&a, 0);
    send_method_frame(&h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_TUNE_OK, a.data, a.len);
    bmqp_buf_free(&a);

    bmqp_buf_init(&a);
    bmqp_buf_put_shortstr(&a, "/"); bmqp_buf_put_shortstr(&a, ""); bmqp_buf_put_u8(&a, 0);
    send_method_frame(&h, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_OPEN, a.data, a.len);
    bmqp_buf_free(&a);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_OPEN_OK, NULL, NULL);

    /* Channel.Open -> Channel.Open-Ok */
    bmqp_buf_init(&a);
    bmqp_buf_put_shortstr(&a, "");
    send_method_frame(&h, 1, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_OPEN, a.data, a.len);
    bmqp_buf_free(&a);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_OPEN_OK, NULL, NULL);

    h_teardown(&h);
}

/* Queue.Declare with x-max-length=2 + x-overflow=drop-head is acknowledged and
 * the parsed policy actually takes effect: a 3rd publish evicts the oldest, so
 * the queue's depth stays capped at 2 (drop-head, driven entirely over the wire). */
static void test_queue_declare_overflow_args(void)
{
    TEST_SECTION("Queue.Declare x-overflow=drop-head/x-max-length applied end-to-end");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;

    /* Field table: x-max-length ('l' long-long = 2) + x-overflow ('S' = drop-head). */
    bmqp_buf_t tbl;
    bmqp_buf_init(&tbl);
    bmqp_buf_put_shortstr(&tbl, "x-max-length");
    bmqp_buf_put_u8(&tbl, 'l');
    bmqp_buf_put_u64(&tbl, 2);
    bmqp_buf_put_shortstr(&tbl, "x-overflow");
    bmqp_buf_put_u8(&tbl, 'S');
    bmqp_buf_put_longstr(&tbl, "drop-head", 9);

    bmqp_buf_t req;
    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "qov", 0, tbl.data, tbl.len);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    bmqp_buf_free(&tbl);

    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, NULL, NULL);

    /* Publish 3 messages via the default exchange straight to "qov". */
    for (int i = 0; i < 3; i++) {
        bmqp_buf_init(&req);
        char body[2] = { (char)('1' + i), 0 };
        put_publish(&req, 1, "", "qov", 0, body);
        h_send(&h, req.data, req.len);
        bmqp_buf_free(&req);
        h_collect(&h, buf, sizeof buf); /* no confirms in this mode */
    }

    /* drop-head cap of 2: the queue holds 2, and one message was dropped. */
    beaver_queue_t *q = broker_get_queue(h.broker, "/", "qov");
    CHECK(q != NULL);
    if (q) {
        CHECK_EQ(queue_depth(q), 2);
        CHECK_EQ(queue_total_dropped(q), 1);
        queue_unref(q);
    }

    h_teardown(&h);
}

/* A mandatory publish to an exchange that routes nowhere comes back as
 * Basic.Return (312 NO_ROUTE), followed by the returned content. */
static void test_mandatory_return(void)
{
    TEST_SECTION("mandatory publish to an unroutable exchange -> Basic.Return");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;

    bmqp_buf_t req;
    bmqp_buf_init(&req);
    /* Named exchange "nx" that was never declared: routes to nothing. */
    put_publish(&req, 1, "nx", "anykey", 1 /* mandatory */, "hello");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);

    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    /* First frame back must be the Basic.Return method. */
    const uint8_t *args; size_t alen;
    if (expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_RETURN, &args, &alen)) {
        /* reply-code (u16) should be 312. */
        uint16_t code = (uint16_t)((args[0] << 8) | args[1]);
        CHECK_EQ(code, 312);
    }

    h_teardown(&h);
}

/* Publisher confirms: Confirm.Select is acknowledged; an unroutable publish is
 * still Ack'd (RabbitMQ semantics), while a publish rejected by a full
 * reject-publish queue is Nack'd. */
static void test_publisher_confirms(void)
{
    TEST_SECTION("Confirm.Select-Ok; unroutable->Basic.Ack; full reject-publish->Basic.Nack");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;

    /* Confirm.Select -> Confirm.Select-Ok. */
    uint8_t nowait = 0;
    send_method_frame(&h, 1, BMQP_CLASS_CONFIRM, BMQP_CONFIRM_SELECT, &nowait, 1);
    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_CONFIRM, BMQP_CONFIRM_SELECT_OK, NULL, NULL);

    /* Unroutable publish (no matching queue) is accepted -> Basic.Ack. */
    bmqp_buf_t req;
    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "no-such-queue", 0, "x");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_ACK, NULL, NULL);

    /* Declare a reject-publish queue capped at 1 message, fill it, then a 2nd
     * publish must be rejected end-to-end -> Basic.Nack. */
    bmqp_buf_t tbl;
    bmqp_buf_init(&tbl);
    bmqp_buf_put_shortstr(&tbl, "x-max-length");
    bmqp_buf_put_u8(&tbl, 'l');
    bmqp_buf_put_u64(&tbl, 1);
    /* default overflow = reject-publish (no x-overflow key) */
    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "cap1", 0, tbl.data, tbl.len);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    bmqp_buf_free(&tbl);
    h_collect(&h, buf, sizeof buf); /* Queue.Declare-Ok */

    /* First publish fills the queue -> Ack. */
    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "cap1", 0, "a");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_ACK, NULL, NULL);

    /* Second publish is rejected (queue full, reject-publish) -> Nack. */
    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "cap1", 0, "b");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_NACK, NULL, NULL);

    h_teardown(&h);
}

/* Full push path: Basic.Consume (manual ack) -> a publish is delivered as
 * Basic.Deliver + content -> Basic.Ack settles it, leaving the connection usable
 * (a follow-up Queue.Declare still returns Declare-Ok, proving the ack did not
 * fault the channel). Exercises the dispatcher, real scatter-gather delivery,
 * and the ack path end-to-end. */
static void test_consume_deliver_ack(void)
{
    TEST_SECTION("Basic.Consume -> Basic.Deliver + content -> Basic.Ack (manual ack)");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;
    bmqp_buf_t req;

    /* Declare the queue, then consume it with manual ack. */
    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "cq", 0, NULL, 0);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf); /* Queue.Declare-Ok */

    bmqp_buf_init(&req);
    put_consume(&req, 1, "cq", "ctag-test", 0 /* manual ack */);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME_OK, NULL, NULL);

    /* Publish a message; it must be pushed to the consumer. */
    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "cq", 0, "hi");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);

    const uint8_t *args; size_t alen;
    uint64_t tag = 0;
    if (expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_DELIVER, &args, &alen))
        tag = deliver_tag(args, alen);

    /* Delivery is followed by a content header frame and the body frame "hi". */
    bmqp_frame_header_t hh;
    const uint8_t *pl;
    CHECK(rdr_next(&r, &hh, &pl));            /* content header */
    CHECK_EQ(hh.type, BMQP_FRAME_HEADER);
    CHECK(rdr_next(&r, &hh, &pl));            /* content body */
    CHECK_EQ(hh.type, BMQP_FRAME_BODY);
    CHECK_EQ(hh.payload_len, 2);
    CHECK(hh.payload_len == 2 && memcmp(pl, "hi", 2) == 0);

    /* Ack the delivery (single). */
    bmqp_buf_t ack;
    bmqp_buf_init(&ack);
    bmqp_buf_put_u64(&ack, tag);
    bmqp_buf_put_u8(&ack, 0);                 /* multiple = 0 */
    send_method_frame(&h, 1, BMQP_CLASS_BASIC, BMQP_BASIC_ACK, ack.data, ack.len);
    bmqp_buf_free(&ack);
    h_collect(&h, buf, sizeof buf);           /* ack yields no response */

    /* The channel is still healthy after the ack: a fresh declare is answered
     * (a spurious "unknown tag" fatal would have closed the connection). */
    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "after-ack", 0, NULL, 0);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, NULL, NULL);

    h_teardown(&h);
}

/* A no-ack consumer receives the delivery without the broker awaiting an ack
 * (the auto-ack push path). */
static void test_consume_no_ack(void)
{
    TEST_SECTION("Basic.Consume no-ack -> Basic.Deliver (auto-ack push)");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;
    bmqp_buf_t req;

    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "nq", 0, NULL, 0);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf);

    bmqp_buf_init(&req);
    put_consume(&req, 1, "nq", "", 0x02 /* no-ack bit */);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf); /* Basic.Consume-Ok */

    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "nq", 0, "yo");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_DELIVER, NULL, NULL);

    /* No-ack means the queue is drained (no unacked retained). */
    beaver_queue_t *q = broker_get_queue(h.broker, "/", "nq");
    CHECK(q != NULL);
    if (q) { CHECK_EQ(queue_depth(q), 0); queue_unref(q); }

    h_teardown(&h);
}

/* Dead-lettering end-to-end: a queue declared with x-dead-letter-exchange, whose
 * delivery is nack'd without requeue, re-routes the message to the DLX (and thus
 * to a queue bound to it). Exercises the arg parsing, the broker's dead-letter
 * re-router, and the dispatcher nack path together. */
static void test_dead_letter_on_nack(void)
{
    TEST_SECTION("nack(requeue=0) on a queue with x-dead-letter-exchange re-routes to the DLX");
    harness_t h;
    h_setup(&h);
    h_handshake(&h);
    uint8_t buf[4096];
    rdr_t r;
    bmqp_buf_t req;

    /* DLX topology: fanout exchange "dlx" with a queue "dlq" bound to it. */
    bmqp_buf_init(&req);
    put_exchange_declare(&req, 1, "dlx", "fanout");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf); /* Exchange.Declare-Ok */

    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "dlq", 0, NULL, 0);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf);

    bmqp_buf_init(&req);
    put_queue_bind(&req, 1, "dlq", "dlx", "");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf); /* Queue.Bind-Ok */

    /* Source queue with a dead-letter target of exchange "dlx". */
    bmqp_buf_t tbl;
    bmqp_buf_init(&tbl);
    bmqp_buf_put_shortstr(&tbl, "x-dead-letter-exchange");
    bmqp_buf_put_u8(&tbl, 'S');
    bmqp_buf_put_longstr(&tbl, "dlx", 3);
    bmqp_buf_init(&req);
    put_queue_declare(&req, 1, "srcq", 0, tbl.data, tbl.len);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    bmqp_buf_free(&tbl);
    h_collect(&h, buf, sizeof buf); /* Queue.Declare-Ok */

    /* Consume srcq (manual ack), publish, receive, then nack without requeue. */
    bmqp_buf_init(&req);
    put_consume(&req, 1, "srcq", "cs", 0);
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    h_collect(&h, buf, sizeof buf); /* Consume-Ok */

    bmqp_buf_init(&req);
    put_publish(&req, 1, "", "srcq", 0, "dead");
    h_send(&h, req.data, req.len);
    bmqp_buf_free(&req);
    size_t n = h_collect(&h, buf, sizeof buf);
    rdr_init(&r, buf, n);
    const uint8_t *args; size_t alen;
    uint64_t tag = 0;
    if (expect_method(&r, BMQP_CLASS_BASIC, BMQP_BASIC_DELIVER, &args, &alen))
        tag = deliver_tag(args, alen);

    /* Basic.Nack(delivery-tag, bits=0): multiple=0, requeue=0 -> dead-letter. */
    bmqp_buf_t nack;
    bmqp_buf_init(&nack);
    bmqp_buf_put_u64(&nack, tag);
    bmqp_buf_put_u8(&nack, 0x00);
    send_method_frame(&h, 1, BMQP_CLASS_BASIC, BMQP_BASIC_NACK, nack.data, nack.len);
    bmqp_buf_free(&nack);
    h_collect(&h, buf, sizeof buf); /* drive the re-route */

    /* The message must have landed in the dead-letter queue. */
    beaver_queue_t *dlq = broker_get_queue(h.broker, "/", "dlq");
    CHECK(dlq != NULL);
    if (dlq) {
        CHECK_EQ(queue_depth(dlq), 1);
        beaver_message_t *m = queue_dequeue(dlq);
        CHECK(m && m->body_len == 4 && memcmp(m->body, "dead", 4) == 0);
        if (m) message_unref(m);
        queue_unref(dlq);
    }
    /* srcq is empty (the message left it). */
    beaver_queue_t *src = broker_get_queue(h.broker, "/", "srcq");
    CHECK(src != NULL);
    if (src) { CHECK_EQ(queue_depth(src), 0); queue_unref(src); }

    h_teardown(&h);
}

int main(void)
{
    test_handshake_sequence();
    test_queue_declare_overflow_args();
    test_mandatory_return();
    test_publisher_confirms();
    test_consume_deliver_ack();
    test_consume_no_ack();
    test_dead_letter_on_nack();
    return test_summary("test_protocol");
}
