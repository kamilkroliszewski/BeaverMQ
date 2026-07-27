/*
 * protocol.c - AMQP 0-9-1 protocol state machine.
 *
 * This implements the server side of AMQP 0-9-1 at the wire level, so standard
 * clients (pika, the Java/PHP clients, ...) interoperate as if talking to
 * RabbitMQ. It owns:
 *   - input accumulation (TCP is a byte stream; frames may span reads),
 *   - frame extraction and method dispatch,
 *   - the per-connection state machine + channel bookkeeping,
 *   - exact AMQP method argument encoding/decoding,
 *   - multi-frame content assembly: a Basic.Publish method frame is followed by
 *     a content-header frame (body size + properties) and one or more
 *     content-body frames, which are reassembled into a single message.
 *
 * The historical bmqp_* helper names denote BeaverMQ's serialization
 * utilities; the bytes on the wire are pure AMQP 0-9-1.
 */
#include "protocol.h"
#include "frame.h"
#include "net.h"
#include "broker.h"
#include "cluster.h"
#include "authstore.h"
#include "authlimit.h"
#include "dispatch.h"
#include "message.h"
#include "logger.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* protocol.c is otherwise libuv-agnostic; this one exception (a short poll
 * timer) is what lets a deferred cluster-commit response happen without
 * blocking the connection's event loop - see await_cluster_commit below. */
#include <uv.h>

/* Sanity cap for a single message body when max_message_size is unset
 * (prevents a hostile body-size field from triggering a huge allocation). */
#define AMQP_MAX_BODY_SIZE (128u * 1024u * 1024u)
#define AMQP_DEFAULT_FRAME_MAX 131072u
#define AMQP_CHANNEL_MAX 2047u        /* advertised in Connection.Tune */
#define AMQP_HEARTBEAT_SECONDS 60u    /* suggested in Connection.Tune */

/* A Basic.Get delivery with no_ack=false, awaiting Basic.Ack/Reject/Nack.
 * Basic.Get has no consumer_t (it's a direct dequeue, not push delivery), so
 * without this the broker had nothing to settle against: it always behaved
 * as no_ack even when the client asked for manual ack, and a client that
 * crashed before acking lost the message with no way to recover it. */
typedef struct {
    uint64_t          tag;
    beaver_message_t *msg;   /* ref held until acked/rejected/requeued */
    beaver_queue_t   *queue; /* ref held for the same reason */
} get_unacked_t;

/* One open channel: its id plus per-channel state (Basic.Qos prefetch). */
typedef struct {
    uint16_t id;
    uint16_t prefetch;   /* Basic.Qos prefetch-count; 0 = unlimited */
    get_unacked_t *get_unacked;
    size_t         n_get_unacked, cap_get_unacked;
    int            confirm_mode; /* Confirm.Select seen: publishes are (n)acked */
    uint64_t       confirm_seq;  /* last publisher delivery-tag assigned on this
                                  * channel (the next is confirm_seq + 1) */
} proto_chan_t;

struct beaver_proto {
    beaver_conn_t *conn;        /* owning connection (back-pointer) */
    bmqp_state_t   state;

    int            header_received;  /* 8-byte greeting consumed */
    int            connection_open;  /* Connection.Open completed */

    /* Input accumulation buffer for partial frames. Bytes in
     * [inbuf_pos, inbuf_len) are buffered-but-unprocessed; the consumed prefix
     * is reclaimed (a single memmove) on the next append, so processing a batch
     * of N frames is O(total bytes), not O(N * bytes). */
    uint8_t       *inbuf;
    size_t         inbuf_pos;
    size_t         inbuf_len;
    size_t         inbuf_cap;

    /* Negotiated tuning parameters (from Connection.TuneOk). */
    uint16_t       channel_max;
    uint32_t       frame_max;
    uint16_t       heartbeat;

    /* Set of currently-open channels (small; most clients use 1). */
    proto_chan_t  *channels;
    size_t         n_channels;
    size_t         cap_channels;

    uint64_t       consumer_seq;     /* auto-generated consumer tags / queue names */

    /* ---- in-progress content assembly (Basic.Publish) ---- */
    int            pub_active;        /* a publish awaits its content frames */
    int            pub_have_header;   /* content header received */
    int            pub_mandatory;     /* mandatory bit: Basic.Return if unroutable */
    uint16_t       pub_channel;
    char           pub_exchange[256];
    char           pub_routing_key[256];
    uint64_t       pub_body_size;     /* declared in the content header */
    uint8_t       *pub_body;          /* accumulation buffer */
    size_t         pub_body_received;
    uint8_t       *pub_props;         /* raw property-flags + property-list */
    size_t         pub_props_len;

    char           vhost[128];
    char           user[128];        /* authenticated username (perm checks) */
    uint32_t       user_tags;        /* authstore tag bits (admin/management) */

    struct pending_cluster_op *pending_ops; /* in-flight deferred cluster-commit
                                             * responses (see await_cluster_commit) */
    int            conn_blocked;    /* Connection.Blocked sent, awaiting unblock */
    struct amqp_auth_work *pending_auth;    /* in-flight off-loop SASL password
                                             * verification (see auth_work_*) */

    /* Exclusive queues declared by THIS connection - deleted when it closes
     * (AMQP: an exclusive queue lives only as long as its declaring
     * connection). Stored as "<vhost>\x01<name>" registry keys. */
    char         **excl_queues;
    size_t         n_excl, cap_excl;
};

/* ---- forward declarations ------------------------------------------------ */
static void proto_close(beaver_proto_t *p);
static void proto_fatal(beaver_proto_t *p, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void finalize_publish(beaver_proto_t *p, const uint8_t *body,
                             size_t body_len);
static void send_method(beaver_proto_t *p, uint16_t channel,
                        uint16_t class_id, uint16_t method_id,
                        const bmqp_buf_t *args);
static void send_channel_close(beaver_proto_t *p, uint16_t channel,
                               uint16_t code, const char *text,
                               uint16_t class_id, uint16_t method_id);
static void send_connection_blocked(beaver_proto_t *p, const char *reason);

/* ========================================================================= */
/* small helpers                                                              */
/* ========================================================================= */

const char *bmqp_state_name(bmqp_state_t s)
{
    switch (s) {
    case BMQP_STATE_CONNECTED:    return "CONNECTED";
    case BMQP_STATE_HANDSHAKE:    return "HANDSHAKE";
    case BMQP_STATE_CHANNEL_OPEN: return "CHANNEL_OPEN";
    case BMQP_STATE_ACTIVE:       return "ACTIVE";
    case BMQP_STATE_CLOSING:      return "CLOSING";
    }
    return "?";
}

bmqp_state_t protocol_state(const beaver_proto_t *p) { return p->state; }

/* State only ever moves forward. CLOSING is terminal. The state is mirrored
 * into conn->amqp_state (atomic) so the management API can read it safely from
 * another thread. */
static void proto_advance(beaver_proto_t *p, bmqp_state_t s)
{
    if (p->state == BMQP_STATE_CLOSING || s <= p->state)
        return;
    LOG_DEBUG("conn #%" PRIu64 ": state %s -> %s",
              p->conn->id, bmqp_state_name(p->state), bmqp_state_name(s));
    p->state = s;
    atomic_store_explicit(&p->conn->amqp_state, (int)s, memory_order_relaxed);
}

/* Copy a non-terminated wire string into a fixed, NUL-terminated buffer. */
static void copy_str(char *dst, size_t cap, const char *src, size_t n)
{
    if (cap == 0)
        return;
    size_t c = (n < cap - 1) ? n : cap - 1;
    if (src && c)
        memcpy(dst, src, c);
    dst[c] = '\0';
}

/* Vhost / queue / exchange names travel into the broker's composite
 * "<vhost>\x01<name>" registry key, so control bytes (including \x01 itself)
 * must never appear in them - otherwise crafted names could alias objects
 * across vhosts. Rejects ASCII control characters and DEL. */
static int name_ok(const char *s)
{
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || (unsigned char)*s == 0x7f)
            return 0;
    return 1;
}

/* ---- Queue.Declare argument parsing (x-overflow / x-max-length[-bytes]) --- */

typedef struct {
    int      has_overflow;
    char     overflow[32];
    int      has_max_length;
    uint64_t max_length;
    int      has_max_bytes;
    uint64_t max_bytes;
    int      has_dlx;
    char     dlx[256];
    char     dlx_rkey[256];
} queue_args_t;

/* Consume one AMQP field-table value of the given type, yielding its integer
 * value in *ival for numeric types (0 otherwise). Returns 1 on success, 0 if
 * the type is unknown - after which the reader can no longer be trusted (its
 * width is unknown), so the caller must stop walking the table. */
static int read_field_value(bmqp_reader_t *r, uint8_t type, uint64_t *ival)
{
    size_t n;
    *ival = 0;
    switch (type) {
    case 't': case 'b': case 'B': *ival = bmqp_read_u8(r);  return !r->error;
    case 's': case 'u': case 'U': *ival = bmqp_read_u16(r); return !r->error;
    case 'I': case 'i': case 'f': *ival = bmqp_read_u32(r); return !r->error;
    case 'l': case 'L': case 'T':
    case 'd':                     *ival = bmqp_read_u64(r); return !r->error;
    case 'D': bmqp_read_u8(r); bmqp_read_u32(r); return !r->error; /* decimal */
    case 'V': return !r->error;                                    /* void */
    case 'S': case 'x': case 'A': case 'F':                        /* len-prefixed */
        bmqp_read_longstr(r, &n); return !r->error;
    default:
        r->error = 1; return 0;   /* unknown type: cannot know its width */
    }
}

/* Walk the Queue.Declare arguments field table, pulling out the queue-policy
 * keys we honor. Unknown fields are skipped by type; a value type we don't
 * recognize stops the walk (rather than risk desyncing). Best-effort: anything
 * malformed just leaves the corresponding has_* flag unset. */
static void parse_queue_args(const uint8_t *tbl, size_t len, queue_args_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!tbl || len == 0)
        return;
    bmqp_reader_t r;
    bmqp_reader_init(&r, tbl, len);
    while (!r.error && bmqp_reader_remaining(&r) > 0) {
        size_t nlen;
        const char *name = bmqp_read_shortstr(&r, &nlen);
        uint8_t type = bmqp_read_u8(&r);
        if (r.error || !name)
            break;
        /* String-valued keys we care about: capture in place before consuming. */
        if (type == 'S') {
            if (nlen == 10 && memcmp(name, "x-overflow", 10) == 0) {
                size_t vlen;
                const char *v = bmqp_read_longstr(&r, &vlen);
                if (r.error) break;
                out->has_overflow = 1;
                copy_str(out->overflow, sizeof out->overflow, v, vlen);
                continue;
            }
            if (nlen == 22 && memcmp(name, "x-dead-letter-exchange", 22) == 0) {
                size_t vlen;
                const char *v = bmqp_read_longstr(&r, &vlen);
                if (r.error) break;
                out->has_dlx = 1;
                copy_str(out->dlx, sizeof out->dlx, v, vlen);
                continue;
            }
            if (nlen == 25 && memcmp(name, "x-dead-letter-routing-key", 25) == 0) {
                size_t vlen;
                const char *v = bmqp_read_longstr(&r, &vlen);
                if (r.error) break;
                copy_str(out->dlx_rkey, sizeof out->dlx_rkey, v, vlen);
                continue;
            }
        }
        uint64_t ival;
        if (!read_field_value(&r, type, &ival))
            break;
        if (nlen == 12 && memcmp(name, "x-max-length", 12) == 0) {
            out->has_max_length = 1;
            out->max_length = ival;
        } else if (nlen == 18 && memcmp(name, "x-max-length-bytes", 18) == 0) {
            out->has_max_bytes = 1;
            out->max_bytes = ival;
        }
    }
}

/* ---- input buffer management --------------------------------------------- */

static int inbuf_append(beaver_proto_t *p, const uint8_t *data, size_t len)
{
    /* Reclaim the already-consumed prefix with a single shift. */
    if (p->inbuf_pos > 0) {
        size_t rem = p->inbuf_len - p->inbuf_pos;
        if (rem)
            memmove(p->inbuf, p->inbuf + p->inbuf_pos, rem);
        p->inbuf_len = rem;
        p->inbuf_pos = 0;
    }
    size_t need = p->inbuf_len + len;
    if (need < p->inbuf_len)
        return 0; /* size_t overflow: reject rather than under-allocate */
    if (need > p->inbuf_cap) {
        size_t newcap = p->inbuf_cap ? p->inbuf_cap : 256;
        while (newcap < need) {
            if (newcap > SIZE_MAX / 2) { newcap = need; break; }
            newcap *= 2;
        }
        uint8_t *nb = realloc(p->inbuf, newcap);
        if (!nb)
            return 0;
        p->inbuf     = nb;
        p->inbuf_cap = newcap;
    }
    memcpy(p->inbuf + p->inbuf_len, data, len);
    p->inbuf_len += len;
    return 1;
}

/* ---- channel bookkeeping ------------------------------------------------- */

static proto_chan_t *channel_find(beaver_proto_t *p, uint16_t ch)
{
    for (size_t i = 0; i < p->n_channels; i++)
        if (p->channels[i].id == ch)
            return &p->channels[i];
    return NULL;
}

static int channel_is_open(beaver_proto_t *p, uint16_t ch)
{
    return channel_find(p, ch) != NULL;
}

static int channel_add(beaver_proto_t *p, uint16_t ch)
{
    if (p->n_channels == p->cap_channels) {
        size_t nc = p->cap_channels ? p->cap_channels * 2 : 4;
        proto_chan_t *na = realloc(p->channels, nc * sizeof(proto_chan_t));
        if (!na)
            return 0;
        p->channels     = na;
        p->cap_channels = nc;
    }
    p->channels[p->n_channels].id             = ch;
    p->channels[p->n_channels].prefetch       = 0;
    p->channels[p->n_channels].get_unacked     = NULL;
    p->channels[p->n_channels].n_get_unacked   = 0;
    p->channels[p->n_channels].cap_get_unacked = 0;
    p->channels[p->n_channels].confirm_mode    = 0;
    p->channels[p->n_channels].confirm_seq     = 0;
    p->n_channels++;
    return 1;
}

/* Requeue every still-outstanding Basic.Get manual-ack delivery on `pc` (the
 * channel is going away - a client that crashed/closed before acking must
 * not lose the message). */
static void chan_release_get_unacked(proto_chan_t *pc)
{
    for (size_t i = 0; i < pc->n_get_unacked; i++) {
        get_unacked_t *g = &pc->get_unacked[i];
        /* Internal requeue: bypass publisher caps so a full queue cannot drop
         * an unacked Basic.Get delivery as the channel tears down. */
        if (queue_requeue_internal(g->queue, g->msg) != 0)
            LOG_ERROR("OOM requeuing unacked Basic.Get message on channel "
                      "teardown (queue=%s): message dropped",
                      queue_name(g->queue));
        message_unref(g->msg);
        queue_unref(g->queue);
    }
    free(pc->get_unacked);
    pc->get_unacked = NULL;
    pc->n_get_unacked = pc->cap_get_unacked = 0;
}

static void channel_remove(beaver_proto_t *p, uint16_t ch)
{
    for (size_t i = 0; i < p->n_channels; i++) {
        if (p->channels[i].id == ch) {
            chan_release_get_unacked(&p->channels[i]);
            p->channels[i] = p->channels[--p->n_channels];
            return;
        }
    }
}

/* Track a Basic.Get delivery (no_ack=false) awaiting Basic.Ack/Reject/Nack.
 * Takes ownership of both refs (released on settle or channel/conn teardown). */
static int chan_get_unacked_add(proto_chan_t *pc, uint64_t tag,
                                beaver_message_t *msg, beaver_queue_t *queue)
{
    if (pc->n_get_unacked == pc->cap_get_unacked) {
        size_t nc = pc->cap_get_unacked ? pc->cap_get_unacked * 2 : 8;
        get_unacked_t *na = realloc(pc->get_unacked, nc * sizeof(*na));
        if (!na)
            return 0;
        pc->get_unacked     = na;
        pc->cap_get_unacked = nc;
    }
    pc->get_unacked[pc->n_get_unacked].tag   = tag;
    pc->get_unacked[pc->n_get_unacked].msg   = msg;
    pc->get_unacked[pc->n_get_unacked].queue = queue;
    pc->n_get_unacked++;
    return 1;
}

/* Settle entries matching delivery_tag (multiple: everything <= tag, or ALL
 * if delivery_tag == 0, matching dispatch.c's settle_unacked semantics).
 * Returns the number settled. */
static size_t chan_get_unacked_settle(proto_chan_t *pc, uint64_t delivery_tag,
                                      int multiple, int requeue, int rejected)
{
    size_t settled = 0;
    for (size_t i = 0; i < pc->n_get_unacked; ) {
        uint64_t t = pc->get_unacked[i].tag;
        int match = multiple ? (delivery_tag == 0 || t <= delivery_tag)
                             : (t == delivery_tag);
        if (!match) {
            i++;
            continue;
        }
        get_unacked_t g = pc->get_unacked[i];
        if (requeue) {
            /* Internal requeue: bypass publisher caps so a reject/nack onto a
             * full queue cannot silently drop the message. */
            if (queue_requeue_internal(g.queue, g.msg) != 0)
                LOG_ERROR("OOM requeuing rejected Basic.Get message "
                          "(queue=%s): message dropped", queue_name(g.queue));
        } else {
            /* Reject/nack without requeue dead-letters (if a DLX is set); a
             * positive ack (rejected == 0) is just consumed-and-gone. */
            if (rejected)
                queue_dead_letter(g.queue, g.msg);
            queue_consume_on_ack(g.queue, g.msg->cluster_id);
        }
        message_unref(g.msg);
        queue_unref(g.queue);
        pc->get_unacked[i] = pc->get_unacked[--pc->n_get_unacked];
        settled++;
        if (!multiple)
            break;
    }
    return settled;
}

/* ---- exclusive queue tracking -------------------------------------------- *
 * An exclusive queue exists only for the lifetime of the connection that
 * declared it; when that connection closes we delete every one it owns. We key
 * on the queue name (a connection has a single vhost, p->vhost). */
static void track_exclusive_queue(beaver_proto_t *p, const char *name)
{
    for (size_t i = 0; i < p->n_excl; i++)
        if (strcmp(p->excl_queues[i], name) == 0)
            return; /* already tracked */
    if (p->n_excl == p->cap_excl) {
        size_t nc = p->cap_excl ? p->cap_excl * 2 : 4;
        char **na = realloc(p->excl_queues, nc * sizeof(*na));
        if (!na)
            return; /* best effort: worst case the queue lingers until restart */
        p->excl_queues = na;
        p->cap_excl    = nc;
    }
    char *dup = strdup(name);
    if (dup)
        p->excl_queues[p->n_excl++] = dup;
}

static void delete_exclusive_queues(beaver_proto_t *p)
{
    if (!p->conn || !p->conn->server || !p->conn->server->broker)
        return;
    for (size_t i = 0; i < p->n_excl; i++) {
        if (broker_delete_queue(p->conn->server->broker, p->vhost,
                                p->excl_queues[i], 0, 0, NULL) == 0)
            LOG_INFO("conn #%" PRIu64 ": deleted exclusive queue '%s' on close",
                     p->conn->id, p->excl_queues[i]);
        free(p->excl_queues[i]);
    }
    free(p->excl_queues);
    p->excl_queues = NULL;
    p->n_excl = p->cap_excl = 0;
}

/* ---- deferred cluster-commit responses ------------------------------------
 * A durable Queue/Exchange.Declare or Queue.Bind must not tell the client it
 * succeeded until the op actually COMMITS on the cluster - sending the *-Ok
 * right after creating the local object (the old behavior) let a client see
 * success even with no leader/quorum, or when the propose call itself failed
 * (OOM). AMQP method handling runs synchronously within one event-loop tick,
 * so we cannot block here; instead a short interval timer polls
 * cluster_proposal_status() and only then sends the (deferred) response. */
#define CLUSTER_WAIT_POLL_MS    5
#define CLUSTER_WAIT_TIMEOUT_MS 5000

typedef enum {
    PENDING_EXCHANGE_DECLARE_OK,
    PENDING_QUEUE_DECLARE_OK,
    PENDING_QUEUE_BIND_OK,
    PENDING_QUEUE_DELETE_OK,  /* Queue.Delete-Ok (message-count in op->depth) */
    PENDING_PUBLISH_CONFIRM,  /* publisher confirm: Basic.Ack on commit, Nack on fail */
} pending_kind_t;

typedef struct pending_cluster_op {
    struct pending_cluster_op *next;
    beaver_proto_t      *p;         /* NULL once cancelled (conn/channel gone) */
    struct cluster_node *cluster;
    uint64_t             seq;
    uint16_t             channel;
    uint16_t             class_id, method_id; /* for the failure channel-exception */
    pending_kind_t       kind;
    char                 qname[256];  /* PENDING_QUEUE_DECLARE_OK only */
    uint32_t             depth;
    uint64_t             confirm_tag; /* PENDING_PUBLISH_CONFIRM only */
    uint64_t             deadline_ms;
    uv_timer_t           timer;
} pending_cluster_op_t;

static void pending_op_closed_cb(uv_handle_t *h)
{
    free(h->data);
}

/* Detach op from its connection's list without touching the timer handle. */
static void detach_pending_op(pending_cluster_op_t *op)
{
    if (!op->p)
        return;
    pending_cluster_op_t **link = &op->p->pending_ops;
    while (*link && *link != op)
        link = &(*link)->next;
    if (*link)
        *link = op->next;
    op->p = NULL;
}

/* Cancel every pending op on `p` (channel_filter < 0) or just on one channel
 * (Channel.Close). Cancelled ops never touch `p` again (see the timer cb). */
static void cancel_pending_ops(beaver_proto_t *p, int channel_filter)
{
    pending_cluster_op_t **link = &p->pending_ops;
    while (*link) {
        pending_cluster_op_t *op = *link;
        if (channel_filter < 0 || op->channel == (uint16_t)channel_filter) {
            *link = op->next;
            op->p = NULL;
            op->next = NULL;
            uv_close((uv_handle_t *)&op->timer, pending_op_closed_cb);
        } else {
            link = &op->next;
        }
    }
}

/* Send a publisher confirm (Basic.Ack, or Basic.Nack on failure) for a single
 * delivery-tag on `channel`. Body: delivery-tag (u64) + a bits octet (bit 0 =
 * multiple; for Nack bit 1 = requeue - always 0 here, one tag at a time). */
static void send_publish_confirm(beaver_proto_t *p, uint16_t channel,
                                 uint64_t tag, int nack)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u64(&a, tag);
    bmqp_buf_put_u8(&a, 0); /* multiple = 0 */
    send_method(p, channel, BMQP_CLASS_BASIC,
                nack ? BMQP_BASIC_NACK : BMQP_BASIC_ACK, &a);
    bmqp_buf_free(&a);
}

/* Connection.Blocked / Unblocked (the RabbitMQ extension clients advertise as
 * the "connection.blocked" capability). Tells the publisher WHY it stopped
 * making progress instead of leaving it staring at a silent socket. */
static void send_connection_blocked(beaver_proto_t *p, const char *reason)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_shortstr(&a, reason);
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_BLOCKED, &a);
    bmqp_buf_free(&a);
    LOG_WARN("conn #%" PRIu64 ": blocked (%s)", p->conn->id, reason);
}

static void send_connection_unblocked(beaver_proto_t *p)
{
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_UNBLOCKED, NULL);
    LOG_INFO("conn #%" PRIu64 ": unblocked", p->conn->id);
}

/* Called from the network layer when the memory alarm clears, for every
 * connection that was told it was blocked. */
void protocol_conn_unblock(beaver_proto_t *p)
{
    if (!p || !p->conn_blocked)
        return;
    p->conn_blocked = 0;
    send_connection_unblocked(p);
}

static void pending_op_timer_cb(uv_timer_t *t)
{
    pending_cluster_op_t *op = t->data;
    if (!op->p) {
        uv_close((uv_handle_t *)t, pending_op_closed_cb);
        return;
    }
    cluster_proposal_status_t st = cluster_proposal_status(op->cluster, op->seq);
    if (st == CL_PROPOSAL_PENDING) {
        if (uv_now(t->loop) < op->deadline_ms)
            return; /* keep polling */
        st = CL_PROPOSAL_REJECTED; /* timed out */
    }
    beaver_proto_t *p = op->p;
    uint16_t channel = op->channel;
    detach_pending_op(op);
    /* A publisher confirm reports the outcome IN BAND (Basic.Ack/Nack) rather
     * than as a channel exception, so the channel stays usable either way. */
    if (op->kind == PENDING_PUBLISH_CONFIRM) {
        send_publish_confirm(p, channel, op->confirm_tag,
                             st != CL_PROPOSAL_COMMITTED /* nack on fail/timeout */);
        uv_close((uv_handle_t *)&op->timer, pending_op_closed_cb);
        return;
    }
    if (st == CL_PROPOSAL_COMMITTED) {
        switch (op->kind) {
        case PENDING_EXCHANGE_DECLARE_OK:
            send_method(p, channel, BMQP_CLASS_EXCHANGE,
                       BMQP_EXCHANGE_DECLARE_OK, NULL);
            break;
        case PENDING_QUEUE_DECLARE_OK: {
            /* Report the queue's real depth: for a replicated declare the queue
             * was created by apply_op at commit (not locally beforehand), so
             * read it from the broker now. Falls back to op->depth if the local
             * apply hasn't landed yet (a fresh queue is 0 anyway). */
            uint32_t depth = op->depth;
            beaver_queue_t *dq = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, op->qname);
            if (dq) { depth = (uint32_t)queue_depth(dq); queue_unref(dq); }
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, op->qname, strlen(op->qname));
            bmqp_buf_put_u32(&a, depth);     /* message-count */
            bmqp_buf_put_u32(&a, 0);         /* consumer-count */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, &a);
            bmqp_buf_free(&a);
            break;
        }
        case PENDING_QUEUE_BIND_OK:
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND_OK, NULL);
            break;
        case PENDING_QUEUE_DELETE_OK: {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_u32(&a, op->depth); /* message-count purged (pre-delete) */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE_OK, &a);
            bmqp_buf_free(&a);
            break;
        }
        case PENDING_PUBLISH_CONFIRM:
            break; /* handled above */
        }
    } else {
        send_channel_close(p, channel, 541,
                           "INTERNAL_ERROR - cluster commit failed or timed out",
                           op->class_id, op->method_id);
    }
    uv_close((uv_handle_t *)&op->timer, pending_op_closed_cb);
}

/* Register a poll for `seq`'s outcome; qname/depth are only used for
 * PENDING_QUEUE_DECLARE_OK (pass qname=NULL, depth=0 otherwise). */
static void await_cluster_commit(beaver_proto_t *p, uint16_t channel, uint64_t seq,
                                 uint16_t class_id, uint16_t method_id,
                                 pending_kind_t kind, const char *qname, uint32_t depth)
{
    pending_cluster_op_t *op = calloc(1, sizeof(*op));
    if (!op) {
        send_channel_close(p, channel, 541,
                           "INTERNAL_ERROR - out of memory awaiting cluster commit",
                           class_id, method_id);
        return;
    }
    op->p         = p;
    op->cluster   = p->conn->server->cluster;
    op->seq       = seq;
    op->channel   = channel;
    op->class_id  = class_id;
    op->method_id = method_id;
    op->kind      = kind;
    if (qname)
        snprintf(op->qname, sizeof(op->qname), "%s", qname);
    op->depth = depth;
    uv_loop_t *loop = p->conn->handle.loop;
    op->deadline_ms = uv_now(loop) + CLUSTER_WAIT_TIMEOUT_MS;
    uv_timer_init(loop, &op->timer);
    op->timer.data = op;
    op->next = p->pending_ops;
    p->pending_ops = op;
    uv_timer_start(&op->timer, pending_op_timer_cb,
                   CLUSTER_WAIT_POLL_MS, CLUSTER_WAIT_POLL_MS);
}

/* Register a poll that sends a publisher confirm (Basic.Ack) for `tag` once the
 * cluster proposal `seq` commits, or a Basic.Nack if it is rejected / times
 * out. On allocation failure we Nack immediately: the publish may still commit,
 * but we can no longer track it, and a spurious Nack (client retries) is safer
 * than silently never confirming. */
static void await_publish_confirm(beaver_proto_t *p, uint16_t channel,
                                  uint64_t seq, uint64_t tag)
{
    pending_cluster_op_t *op = calloc(1, sizeof(*op));
    if (!op) {
        send_publish_confirm(p, channel, tag, 1 /* nack */);
        return;
    }
    op->p           = p;
    op->cluster     = p->conn->server->cluster;
    op->seq         = seq;
    op->channel     = channel;
    op->kind        = PENDING_PUBLISH_CONFIRM;
    op->confirm_tag = tag;
    uv_loop_t *loop = p->conn->handle.loop;
    op->deadline_ms = uv_now(loop) + CLUSTER_WAIT_TIMEOUT_MS;
    uv_timer_init(loop, &op->timer);
    op->timer.data = op;
    op->next = p->pending_ops;
    p->pending_ops = op;
    uv_timer_start(&op->timer, pending_op_timer_cb,
                   CLUSTER_WAIT_POLL_MS, CLUSTER_WAIT_POLL_MS);
}

/* ---- teardown helpers ---------------------------------------------------- */

static void publish_reset(beaver_proto_t *p)
{
    free(p->pub_body);
    free(p->pub_props);
    p->pub_body          = NULL;
    p->pub_props         = NULL;
    p->pub_active        = 0;
    p->pub_have_header   = 0;
    p->pub_mandatory     = 0;
    p->pub_body_size     = 0;
    p->pub_body_received = 0;
    p->pub_props_len     = 0;
}

static void proto_close(beaver_proto_t *p)
{
    if (p->state == BMQP_STATE_CLOSING)
        return;
    p->state = BMQP_STATE_CLOSING;
    atomic_store_explicit(&p->conn->amqp_state, (int)BMQP_STATE_CLOSING,
                          memory_order_relaxed);
    beaver_conn_close(p->conn);
}

static void proto_fatal(beaver_proto_t *p, const char *fmt, ...)
{
    char msg[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    LOG_ERROR("conn #%" PRIu64 " (%s): protocol error: %s",
              p->conn->id, p->conn->peer, msg);
    proto_close(p);
}

/* ========================================================================= */
/* outbound framing                                                           */
/* ========================================================================= */

int protocol_send_method(beaver_conn_t *conn, uint16_t channel,
                         uint16_t class_id, uint16_t method_id,
                         const uint8_t *args, size_t args_len)
{
    bmqp_buf_t pl;
    bmqp_buf_init(&pl);
    bmqp_buf_put_u16(&pl, class_id);
    bmqp_buf_put_u16(&pl, method_id);
    if (args && args_len)
        bmqp_buf_put_bytes(&pl, args, args_len);

    bmqp_buf_t frame;
    bmqp_buf_init(&frame);
    bmqp_frame_write(&frame, BMQP_FRAME_METHOD, channel, pl.data, pl.len);

    int rc = -1;
    if (!pl.error && !frame.error)
        rc = beaver_conn_send(conn, frame.data, frame.len);

    bmqp_buf_free(&pl);
    bmqp_buf_free(&frame);
    return rc;
}

int protocol_send_content(beaver_conn_t *conn, uint16_t channel,
                          uint16_t class_id, const void *body, size_t body_len,
                          const void *props, size_t props_len,
                          uint32_t frame_max)
{
    /* Content header frame: class-id, weight(0), body-size, then the property
     * section (property-flags + property list). If we have no captured
     * properties, emit a single property-flags short of 0 (no properties). */
    bmqp_buf_t hdr;
    bmqp_buf_init(&hdr);
    bmqp_buf_put_u16(&hdr, class_id);
    bmqp_buf_put_u16(&hdr, 0);                 /* weight (unused, must be 0) */
    bmqp_buf_put_u64(&hdr, (uint64_t)body_len);
    if (props && props_len)
        bmqp_buf_put_bytes(&hdr, props, props_len);
    else
        bmqp_buf_put_u16(&hdr, 0);             /* property-flags = none */

    bmqp_buf_t frame;
    bmqp_buf_init(&frame);
    bmqp_frame_write(&frame, BMQP_FRAME_HEADER, channel, hdr.data, hdr.len);
    int rc = (hdr.error || frame.error) ? -1
                                        : beaver_conn_send(conn, frame.data,
                                                           frame.len);
    bmqp_buf_free(&hdr);
    bmqp_buf_free(&frame);
    if (rc != 0)
        return -1;

    /* Content body frame(s), chunked to the negotiated frame size. */
    if (frame_max < 1024)
        frame_max = AMQP_DEFAULT_FRAME_MAX;
    size_t chunk = (size_t)frame_max - BMQP_FRAME_OVERHEAD;

    const uint8_t *p = body;
    size_t remaining = body_len;
    while (remaining > 0) {
        size_t n = remaining < chunk ? remaining : chunk;
        bmqp_buf_t bf;
        bmqp_buf_init(&bf);
        bmqp_frame_write(&bf, BMQP_FRAME_BODY, channel, p, n);
        rc = bf.error ? -1 : beaver_conn_send(conn, bf.data, bf.len);
        bmqp_buf_free(&bf);
        if (rc != 0)
            return -1;
        p += n;
        remaining -= n;
    }
    return 0;
}

/* Build and send a method frame from a proto context (closes on failure). */
static void send_method(beaver_proto_t *p, uint16_t channel,
                        uint16_t class_id, uint16_t method_id,
                        const bmqp_buf_t *args)
{
    if (args && args->error) {
        proto_fatal(p, "out of memory building method args");
        return;
    }
    const uint8_t *a = args ? args->data : NULL;
    size_t alen      = args ? args->len : 0;
    if (protocol_send_method(p->conn, channel, class_id, method_id, a, alen) != 0)
        proto_fatal(p, "failed to write outbound method frame");
}

/* Connection exception: send Connection.Close(code, text) and close the conn.
 * Used for auth/vhost-access failures (AMQP reply-code 403 ACCESS_REFUSED, etc). */
static void send_connection_close(beaver_proto_t *p, uint16_t code, const char *text)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, code);
    bmqp_buf_put_shortstr(&a, text);
    bmqp_buf_put_u16(&a, 0);   /* class-id  (0 = not method-specific) */
    bmqp_buf_put_u16(&a, 0);   /* method-id */
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_CLOSE, &a);
    bmqp_buf_free(&a);
    proto_close(p);
}

/* Channel exception: send Channel.Close(code, text) on `channel`. Used to reject
 * a single operation (e.g. 403 ACCESS_REFUSED on a permission-denied declare/
 * publish/consume) without tearing down the whole connection. */
static void send_channel_close(beaver_proto_t *p, uint16_t channel,
                               uint16_t code, const char *text,
                               uint16_t class_id, uint16_t method_id)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, code);
    bmqp_buf_put_shortstr(&a, text);
    bmqp_buf_put_u16(&a, class_id);
    bmqp_buf_put_u16(&a, method_id);
    send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_CLOSE, &a);
    bmqp_buf_free(&a);

    /* A channel exception CLOSES the channel: release its state here, exactly
     * like a client-initiated Channel.Close does. Without this the channel
     * stayed registered, so the standard client recovery - reopen a channel
     * with the same number - was answered with "channel already open" and we
     * killed the whole connection (this is what made RabbitMQ's perf-test die
     * after its first passive-declare probe). The client still owes us a
     * Channel.Close-Ok, which is accepted and ignored. */
    cancel_pending_ops(p, (int)channel);
    if (p->conn->server->dispatcher)
        dispatcher_remove_channel(p->conn->server->dispatcher, p->conn, channel);
    channel_remove(p, channel);
}

/* Permission gate: 1 if the connection's user may `kind` access to `object` in
 * its vhost. (A connection only exists after a successful login, so the store
 * is never empty here; no bypass.) */
static int perm_ok(beaver_proto_t *p, auth_perm_t kind, const char *object)
{
    struct authstore *as = p->conn->server->authstore;
    if (!as)
        return 1;
    return authstore_check(as, p->user, p->vhost, kind, object);
}

/* Check a permission and, if denied, raise a 403 channel exception. Returns 1 if
 * allowed (caller proceeds), 0 if denied (caller must stop). */
static int require_perm(beaver_proto_t *p, uint16_t channel, auth_perm_t kind,
                        const char *object, uint16_t class_id, uint16_t method_id)
{
    if (perm_ok(p, kind, object))
        return 1;
    char text[600];
    snprintf(text, sizeof text, "ACCESS_REFUSED - access to '%s' in vhost '%s' refused for user '%s'",
             object, p->vhost, p->user);
    LOG_WARN("conn #%" PRIu64 ": %s", p->conn->id, text);
    send_channel_close(p, channel, 403, text, class_id, method_id);
    return 0;
}

/* Append an AMQP field table holding a few server-property strings. */
static void put_table_str(bmqp_buf_t *t, const char *key, const char *val)
{
    bmqp_buf_put_shortstr(t, key);
    bmqp_buf_put_u8(t, 'S');                  /* field value type: long string */
    bmqp_buf_put_longstr(t, val, strlen(val));
}

/* Append a boolean-valued field ('t') to a field table. */
static void put_table_bool(bmqp_buf_t *t, const char *key, int val)
{
    bmqp_buf_put_shortstr(t, key);
    bmqp_buf_put_u8(t, 't');                  /* field value type: boolean */
    bmqp_buf_put_u8(t, val ? 1 : 0);
}

/* Append the "capabilities" field: a nested field table ('F') of booleans that
 * clients (e.g. pika) inspect before using an extension. We advertise exactly
 * what we implement: publisher confirms (Confirm.Select) and Basic.Nack. */
static void put_capabilities(bmqp_buf_t *t)
{
    bmqp_buf_t caps;
    bmqp_buf_init(&caps);
    put_table_bool(&caps, "publisher_confirms", 1);
    put_table_bool(&caps, "basic.nack", 1);
    put_table_bool(&caps, "connection.blocked", 1);
    bmqp_buf_put_shortstr(t, "capabilities");
    bmqp_buf_put_u8(t, 'F');                  /* field value type: field table */
    if (!caps.error) {
        bmqp_buf_put_u32(t, (uint32_t)caps.len);
        bmqp_buf_put_bytes(t, caps.data, caps.len);
    } else {
        bmqp_buf_put_u32(t, 0);
    }
    bmqp_buf_free(&caps);
}

static void put_server_properties(bmqp_buf_t *a)
{
    bmqp_buf_t t;
    bmqp_buf_init(&t);
    put_table_str(&t, "product", "BeaverMQ");
    put_table_str(&t, "version", "1.0.0");
    put_table_str(&t, "platform", "C");
    put_capabilities(&t);
    if (!t.error) {
        bmqp_buf_put_u32(a, (uint32_t)t.len);
        bmqp_buf_put_bytes(a, t.data, t.len);
    } else {
        bmqp_buf_put_u32(a, 0);
    }
    bmqp_buf_free(&t);
}

/* ========================================================================= */
/* state guards                                                               */
/* ========================================================================= */

static int require_open(beaver_proto_t *p)
{
    if (!p->connection_open) {
        proto_fatal(p, "method received before connection was opened");
        return 0;
    }
    return 1;
}

static int require_channel(beaver_proto_t *p, uint16_t channel)
{
    if (!require_open(p))
        return 0;
    if (channel == 0 || !channel_is_open(p, channel)) {
        proto_fatal(p, "operation on channel %u which is not open", channel);
        return 0;
    }
    return 1;
}

/* ========================================================================= */
/* connection class                                                           */
/* ========================================================================= */

static void send_connection_start(beaver_proto_t *p)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u8(&a, 0);             /* version-major (AMQP 0-9-1) */
    bmqp_buf_put_u8(&a, 9);             /* version-minor */
    put_server_properties(&a);          /* server-properties (field table) */
    bmqp_buf_put_longstr(&a, "PLAIN", 5);   /* mechanisms (long string) */
    bmqp_buf_put_longstr(&a, "en_US", 5);   /* locales    (long string) */
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_START, &a);
    bmqp_buf_free(&a);
}

/* Finish a successful Connection.StartOk: record the identity and answer with
 * Connection.Tune. Shared by the no-authstore path and the async auth result. */
static void send_connection_tune(beaver_proto_t *p, const char *user, uint32_t tags)
{
    p->user_tags = tags;
    copy_str(p->user, sizeof p->user, user, strlen(user));
    LOG_INFO("conn #%" PRIu64 ": authenticated as '%s'", p->conn->id, p->user);

    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, AMQP_CHANNEL_MAX);        /* channel-max */
    bmqp_buf_put_u32(&a, AMQP_DEFAULT_FRAME_MAX);  /* frame-max   */
    bmqp_buf_put_u16(&a, AMQP_HEARTBEAT_SECONDS);  /* heartbeat suggestion */
    send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_TUNE, &a);
    bmqp_buf_free(&a);
}

/* Off-loop SASL password verification. The expensive PBKDF2 runs on a libuv
 * worker thread (work_cb) so it never blocks the connection's event loop; the
 * result is applied back on the loop (after_cb). The work object outlives a
 * connection teardown that happens mid-hash: protocol_conn_free() nulls ->p so
 * after_cb knows to just clean up (the cancellation discipline the deferred
 * cluster ops already use). */
typedef struct amqp_auth_work {
    uv_work_t       req;      /* MUST be first */
    beaver_proto_t *p;        /* NULL once the connection is gone */
    char            user[128];
    char            ip[64];
    char            pass[256];
    char            stored[AUTHSTORE_HASH_MAX];
    uint64_t        now_ms;
    int             user_exists;
    int             result;   /* set off-loop by work_cb */
} amqp_auth_work_t;

static void auth_work_cb(uv_work_t *req)
{
    amqp_auth_work_t *w = (amqp_auth_work_t *)req;
    /* Pure/reentrant: touches only the copied strings, no store, no loop. */
    w->result = w->user_exists &&
                authstore_password_matches(w->stored, w->pass);
    /* Do not leave the plaintext password lying in the heap longer than needed. */
    memset(w->pass, 0, sizeof w->pass);
}

static void auth_work_after_cb(uv_work_t *req, int status)
{
    amqp_auth_work_t *w = (amqp_auth_work_t *)req;
    authlimit_hash_end(); /* release the concurrency slot held across the hash */

    beaver_proto_t *p = w->p;
    if (!p || status == UV_ECANCELED) { /* connection gone (or loop torn down) */
        free(w);
        return;
    }
    p->pending_auth = NULL;

    if (!w->result) {
        authlimit_record_failure(w->ip, w->now_ms);
        LOG_WARN("conn #%" PRIu64 ": auth FAILED for user '%s'", p->conn->id, w->user);
        send_connection_close(p, 403, "ACCESS_REFUSED - login refused");
        free(w);
        return;
    }
    authlimit_record_success(w->ip);
    struct authstore *as = p->conn->server->authstore;
    send_connection_tune(p, w->user, as ? authstore_user_tags(as, w->user) : 0);
    free(w);
}

static void handle_connection(beaver_proto_t *p, uint16_t channel,
                              uint16_t method, bmqp_reader_t *r)
{
    if (channel != 0) {
        proto_fatal(p, "connection class method on non-zero channel %u", channel);
        return;
    }
    /* A password verification is in flight off-loop; the client must wait for
     * our Connection.Tune before sending anything else. Any frame arriving in
     * that window is a protocol violation (or a hostile flood). */
    if (p->pending_auth) {
        proto_fatal(p, "connection frame received while authentication in progress");
        return;
    }

    switch (method) {
    case BMQP_CONNECTION_START_OK: {
        size_t n, rlen;
        bmqp_read_longstr(r, &n);   /* client-properties (field table; skip) */
        bmqp_read_shortstr(r, &n);  /* mechanism (we only offer PLAIN) */
        const char *resp = bmqp_read_longstr(r, &rlen);  /* SASL response */
        bmqp_read_shortstr(r, &n);  /* locale */
        if (r->error) {
            proto_fatal(p, "malformed Connection.StartOk");
            return;
        }
        /* SASL PLAIN response is "[authzid]\0authcid\0passwd". Split on NULs. */
        char user[128] = "", pass[256] = "";
        if (resp && rlen > 0) {
            size_t a0 = 0;
            while (a0 < rlen && resp[a0] != '\0') a0++;       /* skip authzid */
            size_t u = a0 + 1, ue = u;
            while (ue < rlen && resp[ue] != '\0') ue++;       /* authcid */
            size_t pw = ue + 1;
            if (u <= rlen) copy_str(user, sizeof user, resp + u,
                                    (ue > u ? ue - u : 0));
            if (pw <= rlen) copy_str(pass, sizeof pass, resp + pw,
                                     (rlen > pw ? rlen - pw : 0));
        }
        struct authstore *as = p->conn->server->authstore;
        if (!as) {
            /* No auth configured: accept and Tune immediately. */
            send_connection_tune(p, user, 0);
            break;
        }
        if (authstore_is_open(as)) {
            /* Fresh, unconfigured broker: refuse EVERY login (clients like
             * pika silently try guest/guest - accepting them would look
             * like "no auth"). The operator must create the first admin. */
            LOG_WARN("conn #%" PRIu64 ": login refused - no users configured "
                     "(create one: beavermq add-user <name> <password>)",
                     p->conn->id);
            send_connection_close(p, 403,
                "ACCESS_REFUSED - no users configured; create the first "
                "admin with 'beavermq add-user'");
            return;
        }
        /* Rate-limit password hashing (see authlimit): key on the client IP
         * only (strip the ":port" from conn->peer) so all attempts from one
         * address share a backoff. */
        char ip[64];
        copy_str(ip, sizeof ip, p->conn->peer, strlen(p->conn->peer));
        char *last_colon = strrchr(ip, ':');
        if (last_colon) *last_colon = '\0';
        uint64_t now_ms = uv_now(p->conn->handle.loop);

        if (!user[0]) {
            authlimit_record_failure(ip, now_ms);
            LOG_WARN("conn #%" PRIu64 ": auth FAILED (empty user)", p->conn->id);
            send_connection_close(p, 403, "ACCESS_REFUSED - login refused");
            return;
        }
        if (authlimit_retry_after_ms(ip, now_ms) > 0 ||
            authlimit_hash_begin() != 0) {
            LOG_WARN("conn #%" PRIu64 ": auth throttled for '%s' from %s",
                     p->conn->id, user, ip);
            send_connection_close(p, 403,
                "ACCESS_REFUSED - too many attempts; retry later");
            return;
        }
        /* Verify the password OFF the event loop: look the stored hash up here
         * (cheap, under the store's read lock), then run PBKDF2 on a worker
         * thread so a slow hash cannot stall this worker's other connections.
         * The result is applied in auth_work_after_cb. */
        amqp_auth_work_t *w = calloc(1, sizeof *w);
        if (!w) {
            authlimit_hash_end();
            proto_fatal(p, "out of memory starting authentication");
            return;
        }
        w->p      = p;
        w->now_ms = now_ms;
        copy_str(w->user, sizeof w->user, user, strlen(user));
        copy_str(w->ip,   sizeof w->ip,   ip,   strlen(ip));
        copy_str(w->pass, sizeof w->pass, pass, strlen(pass));
        w->user_exists = authstore_lookup_hash(as, user, w->stored, sizeof w->stored);
        p->pending_auth = w;
        if (uv_queue_work(p->conn->handle.loop, &w->req,
                          auth_work_cb, auth_work_after_cb) != 0) {
            p->pending_auth = NULL;
            authlimit_hash_end();
            free(w);
            proto_fatal(p, "failed to queue authentication work");
            return;
        }
        break; /* async: auth_work_after_cb sends Tune or Close */
    }
    case BMQP_CONNECTION_TUNE_OK: {
        p->channel_max = bmqp_read_u16(r);
        p->frame_max   = bmqp_read_u32(r);
        p->heartbeat   = bmqp_read_u16(r);
        if (r->error) {
            proto_fatal(p, "malformed Connection.TuneOk");
            return;
        }
        /* Spec: the client must pick a frame-max no larger than what we
         * offered (0 = "use yours"); clamp rather than trust the wire. */
        if (p->frame_max == 0 || p->frame_max > AMQP_DEFAULT_FRAME_MAX)
            p->frame_max = AMQP_DEFAULT_FRAME_MAX;
        p->conn->frame_max = p->frame_max;
        /* The TuneOk heartbeat is the negotiated value; 0 disables. Arm the
         * sender/dead-peer timer accordingly - either way, the handshake
         * completed, so the handshake-timeout deadline must not still fire. */
        if (p->heartbeat)
            beaver_conn_enable_heartbeat(p->conn, p->heartbeat);
        else
            beaver_conn_clear_handshake_timeout(p->conn);
        LOG_INFO("conn #%" PRIu64 ": Connection.TuneOk "
                 "(channel_max=%u frame_max=%u heartbeat=%u)",
                 p->conn->id, p->channel_max, p->frame_max, p->heartbeat);
        break;
    }
    case BMQP_CONNECTION_OPEN: {
        size_t vhlen, n;
        const char *vh = bmqp_read_shortstr(r, &vhlen); /* virtual-host */
        bmqp_read_shortstr(r, &n);                  /* reserved-1 (capabilities) */
        bmqp_read_u8(r);                            /* reserved-2 (insist bit) */
        if (r->error) {
            proto_fatal(p, "malformed Connection.Open");
            return;
        }
        copy_str(p->vhost, sizeof(p->vhost), vh, vhlen);
        if (!name_ok(p->vhost)) {
            send_connection_close(p, 530, "NOT_ALLOWED - illegal vhost name");
            return;
        }
        struct authstore *as = p->conn->server->authstore;
        if (as) {
            if (!authstore_vhost_exists(as, p->vhost)) {
                send_connection_close(p, 530,
                    "NOT_ALLOWED - vhost not found");
                return;
            }
            if (!authstore_can_access_vhost(as, p->user, p->vhost)) {
                LOG_WARN("conn #%" PRIu64 ": user '%s' denied vhost '%s'",
                         p->conn->id, p->user, p->vhost);
                send_connection_close(p, 530,
                    "NOT_ALLOWED - access to vhost refused");
                return;
            }
        }
        p->connection_open = 1;
        LOG_INFO("conn #%" PRIu64 ": Connection.Open vhost='%s' user='%s' -> ready",
                 p->conn->id, p->vhost, p->user);

        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_shortstr(&a, "");              /* reserved-1 (known-hosts) */
        send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_OPEN_OK, &a);
        bmqp_buf_free(&a);
        break;
    }
    case BMQP_CONNECTION_CLOSE: {
        uint16_t code = bmqp_read_u16(r);   /* reply-code */
        size_t n;
        bmqp_read_shortstr(r, &n);          /* reply-text */
        bmqp_read_u16(r);                   /* class-id */
        bmqp_read_u16(r);                   /* method-id */
        LOG_INFO("conn #%" PRIu64 ": Connection.Close requested by peer "
                 "(code=%u)", p->conn->id, code);
        send_method(p, 0, BMQP_CLASS_CONNECTION, BMQP_CONNECTION_CLOSE_OK, NULL);
        proto_close(p);
        break;
    }
    case BMQP_CONNECTION_CLOSE_OK:
        proto_close(p);
        break;
    default:
        proto_fatal(p, "unexpected connection method %u", method);
    }
}

/* ========================================================================= */
/* channel class                                                              */
/* ========================================================================= */

static void handle_channel(beaver_proto_t *p, uint16_t channel,
                           uint16_t method, bmqp_reader_t *r)
{
    if (!require_open(p))
        return;

    switch (method) {
    case BMQP_CHANNEL_OPEN: {
        size_t n;
        bmqp_read_shortstr(r, &n);  /* reserved-1 */
        if (channel == 0) {
            proto_fatal(p, "Channel.Open on reserved channel 0");
            return;
        }
        if (channel > AMQP_CHANNEL_MAX) {
            proto_fatal(p, "Channel.Open: channel %u exceeds channel-max %u",
                        channel, AMQP_CHANNEL_MAX);
            return;
        }
        if (channel_is_open(p, channel)) {
            proto_fatal(p, "Channel.Open: channel %u already open", channel);
            return;
        }
        if (!channel_add(p, channel)) {
            proto_fatal(p, "out of memory tracking channel %u", channel);
            return;
        }
        proto_advance(p, BMQP_STATE_CHANNEL_OPEN);
        LOG_INFO("conn #%" PRIu64 ": Channel.Open ch=%u", p->conn->id, channel);

        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_longstr(&a, "", 0);   /* reserved-1 (long string) */
        send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_OPEN_OK, &a);
        bmqp_buf_free(&a);
        break;
    }
    case BMQP_CHANNEL_CLOSE: {
        uint16_t code = bmqp_read_u16(r);
        size_t n;
        bmqp_read_shortstr(r, &n);
        bmqp_read_u16(r);
        bmqp_read_u16(r);
        cancel_pending_ops(p, channel);
        dispatcher_remove_channel(p->conn->server->dispatcher, p->conn, channel);
        channel_remove(p, channel);
        LOG_INFO("conn #%" PRIu64 ": Channel.Close ch=%u (code=%u)",
                 p->conn->id, channel, code);
        send_method(p, channel, BMQP_CLASS_CHANNEL, BMQP_CHANNEL_CLOSE_OK, NULL);
        break;
    }
    case BMQP_CHANNEL_CLOSE_OK:
        break;
    default:
        proto_fatal(p, "unexpected channel method %u", method);
    }
}

/* ========================================================================= */
/* exchange class                                                             */
/* ========================================================================= */

static void handle_exchange(beaver_proto_t *p, uint16_t channel,
                            uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_EXCHANGE_DECLARE: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t en, tn, tbl;
        const char *ex = bmqp_read_shortstr(r, &en);
        const char *ty = bmqp_read_shortstr(r, &tn);
        uint8_t bits   = bmqp_read_u8(r);       /* passive,durable,auto-del,internal,no-wait */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Exchange.Declare");
            return;
        }
        int no_wait = (bits & 0x10) != 0;
        char ename[256], etype[32];
        copy_str(ename, sizeof(ename), ex, en);
        copy_str(etype, sizeof(etype), ty, tn);
        if (!name_ok(ename)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal exchange name",
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_CONFIGURE, ename,
                          BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE))
            return;

        /* passive = "does this exchange exist?" - never create, and IGNORE the
         * type/durable/auto-delete/arguments fields entirely (clients send an
         * EMPTY type here: RabbitMQ's exchangeDeclarePassive does exactly that,
         * and validating it as a real type used to fail the call with
         * COMMAND_INVALID). Answer Declare-Ok if it exists, 404 if it does not.
         * The default exchange ("") always exists. */
        if (bits & BMQP_FLAG_PASSIVE) {
            int exists = (ename[0] == '\0') ||
                         broker_exchange_exists(p->conn->server->broker,
                                                p->vhost, ename);
            if (!exists) {
                char text[600];
                snprintf(text, sizeof text,
                         "NOT_FOUND - no exchange '%s' in vhost '%s'",
                         ename, p->vhost);
                send_channel_close(p, channel, 404, text,
                                   BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
                return;
            }
            if (!no_wait)
                send_method(p, channel, BMQP_CLASS_EXCHANGE,
                            BMQP_EXCHANGE_DECLARE_OK, NULL);
            break;
        }

        exchange_type_t xtype;
        if (exchange_type_from_name(etype, &xtype) != 0) {
            /* An unknown exchange type must be a channel exception, not a
             * silent direct-exchange substitution - the client believed it
             * declared (e.g.) a headers exchange and would route messages
             * accordingly, while the broker quietly created a direct one. */
            char text[300];
            snprintf(text, sizeof text,
                     "COMMAND_INVALID - unknown exchange type '%s'", etype);
            send_channel_close(p, channel, 503, text,
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }

        /* Cluster + durable == replicated topology: propose, wait for the Raft
         * commit, and let apply_op create it on EVERY node. No local mutation
         * before commit - otherwise a failed op (election / lost quorum) would
         * leave an orphan exchange on this node while the client saw an error.
         * (A redeclare with a different type in a cluster is not rejected
         * pre-commit here - apply_op is idempotent; the local path below keeps
         * the -2 precondition for standalone/transient exchanges.) */
        if (p->conn->server->cluster && (bits & BMQP_FLAG_DURABLE)) {
            uint64_t seq = cluster_replicate_declare_exchange(
                p->conn->server->cluster, p->vhost, ename, (int)xtype, bits & 0x0E);
            if (no_wait)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Exchange.Declare", BMQP_CLASS_EXCHANGE,
                                   BMQP_EXCHANGE_DECLARE);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_EXCHANGE,
                                 BMQP_EXCHANGE_DECLARE,
                                 PENDING_EXCHANGE_DECLARE_OK, NULL, 0);
            break;
        }

        int created = 0;
        int drc = broker_declare_exchange(p->conn->server->broker, p->vhost,
                                          ename, xtype, bits & 0x0E, &created);
        if (drc == -2) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - exchange already "
                               "declared with different type/flags",
                               BMQP_CLASS_EXCHANGE, BMQP_EXCHANGE_DECLARE);
            return;
        }
        if (drc != 0) {
            proto_fatal(p, "failed to declare exchange '%s'", ename);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Exchange.Declare '%s' type=%s (%s)",
                 p->conn->id, channel, ename, exchange_type_name(xtype),
                 created ? "created" : "exists");
        if (!no_wait)
            send_method(p, channel, BMQP_CLASS_EXCHANGE,
                        BMQP_EXCHANGE_DECLARE_OK, NULL);
        break;
    }
    default:
        proto_fatal(p, "unexpected exchange method %u", method);
    }
}

/* ========================================================================= */
/* queue class                                                                */
/* ========================================================================= */

static void handle_queue(beaver_proto_t *p, uint16_t channel,
                         uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_QUEUE_DECLARE: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        uint8_t bits  = bmqp_read_u8(r);        /* passive,durable,exclusive,auto-del,no-wait */
        const char *args = bmqp_read_longstr(r, &tbl); /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Queue.Declare");
            return;
        }
        int no_wait = (bits & 0x10) != 0;
        char qname[256];
        copy_str(qname, sizeof(qname), q, qn);
        if (qname[0] == '\0') /* server-generated name for anonymous queues */
            snprintf(qname, sizeof(qname), "amq.gen-%" PRIu64 "-%" PRIu64,
                     p->conn->id, ++p->consumer_seq);
        else if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_CONFIGURE, qname,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE))
            return;

        /* passive = "does this queue exist?" - never create, and ignore the
         * flags/arguments (AMQP: a passive declare only probes). Declare-Ok with
         * the live counts if it exists, 404 if it does not. */
        if (bits & BMQP_FLAG_PASSIVE) {
            beaver_queue_t *pq = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
            if (!pq) {
                char text[600];
                snprintf(text, sizeof text,
                         "NOT_FOUND - no queue '%s' in vhost '%s'", qname, p->vhost);
                send_channel_close(p, channel, 404, text,
                                   BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
                return;
            }
            uint32_t pdepth = (uint32_t)queue_depth(pq);
            uint32_t pcons  = (uint32_t)queue_consumer_count(pq);
            queue_unref(pq);
            if (!no_wait) {
                bmqp_buf_t a;
                bmqp_buf_init(&a);
                bmqp_buf_put_shortstr_n(&a, qname, strlen(qname));
                bmqp_buf_put_u32(&a, pdepth);
                bmqp_buf_put_u32(&a, pcons);
                send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, &a);
                bmqp_buf_free(&a);
            }
            break;
        }

        /* Parse the per-queue policy args once, up front: needed both to apply
         * locally (on create) and to carry in the replicated op so every node's
         * copy gets the same limits/overflow/DLX. */
        queue_args_t qa;
        parse_queue_args((const uint8_t *)args, tbl, &qa);
        uint8_t qa_overflow = (qa.has_overflow &&
                               strcmp(qa.overflow, "drop-head") == 0) ? 1 : 0;
        uint64_t qa_maxlen = qa.has_max_length ? qa.max_length : 0;
        uint64_t qa_maxbytes = qa.has_max_bytes ? qa.max_bytes : 0;

        /* A declared dead-letter exchange with an illegal name would make
         * dead-lettering silently unroutable; reject it before doing anything
         * (checked here so it applies to both the local and cluster paths). */
        if (qa.has_dlx && !name_ok(qa.dlx)) {
            send_channel_close(p, channel, 406,
                "PRECONDITION_FAILED - illegal x-dead-letter-exchange",
                BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
            return;
        }

        /* Cluster + durable + non-exclusive == replicated topology: do NOT touch
         * the local broker here. Propose the declare, wait for the Raft commit,
         * and let apply_op create it on EVERY node (including this one). Mutating
         * locally first would leave an orphan queue on this node if the op never
         * commits (election / lost quorum). Exclusive queues are connection-
         * scoped and never replicated, so they fall through to the local path. */
        if (p->conn->server->cluster && (bits & BMQP_FLAG_DURABLE) &&
            !(bits & BMQP_FLAG_EXCLUSIVE)) {
            /* Read-only precondition: reject a redeclare with different flags
             * (the local path's broker_declare_queue -2 rule) without mutating. */
            beaver_queue_t *ex = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
            if (ex) {
                int mismatch = queue_flags(ex) != (bits & 0x0E);
                queue_unref(ex);
                if (mismatch) {
                    send_channel_close(p, channel, 406,
                        "PRECONDITION_FAILED - queue already declared with "
                        "different flags", BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
                    return;
                }
            }
            uint64_t seq = cluster_replicate_declare_queue(
                p->conn->server->cluster, p->vhost, qname, bits & 0x0E,
                qa_overflow, qa_maxlen, qa_maxbytes,
                qa.has_dlx ? qa.dlx : "", qa.has_dlx ? qa.dlx_rkey : "");
            if (no_wait)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Queue.Declare", BMQP_CLASS_QUEUE,
                                   BMQP_QUEUE_DECLARE);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_QUEUE,
                                 BMQP_QUEUE_DECLARE, PENDING_QUEUE_DECLARE_OK,
                                 qname, 0 /* real depth read from broker at reply */);
            break;
        }

        uint32_t depth = 0;
        int created = 0;
        /* passive/durable/exclusive/auto-delete bits map 1:1 to our flags. */
        int drc = broker_declare_queue(p->conn->server->broker, p->vhost, qname,
                                       bits & 0x0E, &depth, &created);
        if (drc == -2) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - queue already declared "
                               "with different flags",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
            return;
        }
        if (drc != 0) {
            proto_fatal(p, "failed to declare queue '%s'", qname);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Queue.Declare '%s' (%s, depth=%u)",
                 p->conn->id, channel, qname,
                 created ? "created" : "exists", depth);

        /* Apply per-queue overflow/limit policy from the declare arguments
         * (x-overflow / x-max-length / x-max-length-bytes). Only on create: an
         * existing queue keeps the policy it was declared with, matching the
         * flag-mismatch rule above. */
        if (created) {
            if (qa.has_overflow || qa.has_max_length || qa.has_max_bytes ||
                qa.has_dlx) {
                beaver_queue_t *lq = broker_get_queue(p->conn->server->broker,
                                                      p->vhost, qname);
                if (lq) {
                    queue_set_limits(lq,
                                     qa.has_max_length ? qa.max_length : 0,
                                     qa.has_max_bytes  ? qa.max_bytes  : 0,
                                     (qa.has_overflow &&
                                      strcmp(qa.overflow, "drop-head") == 0)
                                         ? QUEUE_OVERFLOW_DROP_HEAD
                                         : QUEUE_OVERFLOW_REJECT_PUBLISH);
                    if (qa.has_overflow &&
                        strcmp(qa.overflow, "drop-head") != 0 &&
                        strcmp(qa.overflow, "reject-publish") != 0)
                        LOG_WARN("conn #%" PRIu64 ": unsupported x-overflow '%s' "
                                 "on queue '%s'; using reject-publish",
                                 p->conn->id, qa.overflow, qname);
                    if (qa.has_dlx) /* name already validated above */
                        broker_set_queue_dead_letter(p->conn->server->broker, lq,
                                                     qa.dlx, qa.dlx_rkey);
                    LOG_INFO("conn #%" PRIu64 ": queue '%s' limits "
                             "max_length=%" PRIu64 " max_bytes=%" PRIu64
                             " overflow=%s dlx='%s'", p->conn->id, qname,
                             qa.has_max_length ? qa.max_length : 0,
                             qa.has_max_bytes ? qa.max_bytes : 0,
                             (qa.has_overflow &&
                              strcmp(qa.overflow, "drop-head") == 0)
                                 ? "drop-head" : "reject-publish",
                             qa.has_dlx ? qa.dlx : "");
                    queue_unref(lq);
                }
            }
        }

        /* Exclusive queues: the first declarer owns the queue; a declare from
         * any OTHER connection is refused (AMQP RESOURCE_LOCKED). The owner is
         * remembered so the queue is deleted when this connection closes. */
        if (bits & BMQP_FLAG_EXCLUSIVE) {
            beaver_queue_t *eq = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
            if (eq) {
                uint64_t owner = queue_exclusive_owner(eq);
                if (owner != 0 && owner != p->conn->id) {
                    queue_unref(eq);
                    send_channel_close(p, channel, 405,
                        "RESOURCE_LOCKED - queue is exclusive to another connection",
                        BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE);
                    return;
                }
                if (owner == 0)
                    queue_set_exclusive_owner(eq, p->conn->id);
                queue_unref(eq);
                track_exclusive_queue(p, qname);
            }
        }

        if (!no_wait) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, qname, strlen(qname));
            bmqp_buf_put_u32(&a, depth);   /* message-count */
            bmqp_buf_put_u32(&a, 0);       /* consumer-count */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DECLARE_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_QUEUE_BIND: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, en, kn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        const char *e = bmqp_read_shortstr(r, &en);
        const char *k = bmqp_read_shortstr(r, &kn);
        uint8_t no_wait = bmqp_read_u8(r);      /* no-wait bit */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Queue.Bind");
            return;
        }
        char qname[256], ename[256], key[256];
        copy_str(qname, sizeof(qname), q, qn);
        copy_str(ename, sizeof(ename), e, en);
        copy_str(key, sizeof(key), k, kn);
        if (!name_ok(qname) || !name_ok(ename)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue/exchange name",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND);
            return;
        }
        /* AMQP: binding needs WRITE on the exchange (source) + READ on the
         * queue (destination). */
        if (!require_perm(p, channel, AUTH_WRITE, ename,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND) ||
            !require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND))
            return;
        /* A bind on a DURABLE queue is replicated topology: do NOT bind locally
         * first (an orphan binding would survive a failed op). Decide durability
         * read-only, verify both endpoints exist, propose, and let apply_op bind
         * on every node at commit. A non-durable queue binds locally (node-local
         * and fast), matching the transient-topology policy. */
        beaver_queue_t *bq = broker_get_queue(p->conn->server->broker,
                                              p->vhost, qname);
        int durable_q = bq && (queue_flags(bq) & BMQP_FLAG_DURABLE);
        if (bq)
            queue_unref(bq);

        if (p->conn->server->cluster && durable_q) {
            if (!broker_exchange_exists(p->conn->server->broker, p->vhost, ename)) {
                proto_fatal(p, "Queue.Bind failed: exchange '%s' not found", ename);
                return;
            }
            uint64_t seq = cluster_replicate_bind(p->conn->server->cluster,
                                                  p->vhost, qname, ename, key);
            if (no_wait & 0x01)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Queue.Bind", BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_QUEUE,
                                 BMQP_QUEUE_BIND, PENDING_QUEUE_BIND_OK, NULL, 0);
            break;
        }

        if (broker_bind(p->conn->server->broker, p->vhost, qname, ename, key) != 0) {
            proto_fatal(p, "Queue.Bind failed: queue '%s' or exchange '%s' "
                        "not found", qname, ename);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Queue.Bind q='%s' exchange='%s' "
                 "key='%s'", p->conn->id, channel, qname, ename, key);
        if (!(no_wait & 0x01))
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_BIND_OK, NULL);
        break;
    }
    case BMQP_QUEUE_DELETE: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn;
        const char *q = bmqp_read_shortstr(r, &qn);
        uint8_t bits  = bmqp_read_u8(r);        /* if-unused, if-empty, no-wait */
        if (r->error) {
            proto_fatal(p, "malformed Queue.Delete");
            return;
        }
        int if_unused = (bits & 0x01) != 0;
        int if_empty  = (bits & 0x02) != 0;
        int no_wait   = (bits & 0x04) != 0;
        char qname[256];
        copy_str(qname, sizeof(qname), q, qn);
        if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        /* Deleting a queue is a CONFIGURE operation. */
        if (!require_perm(p, channel, AUTH_CONFIGURE, qname,
                          BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE))
            return;

        /* Inspect the queue read-only: existence (404), exclusive owner (405),
         * durability, message count, and the if-unused / if-empty preconditions
         * (406). Doing this WITHOUT mutating lets the cluster path propose the
         * delete and have apply_op remove it on every node - deleting locally
         * first would drop this node's copy (and its messages) even if the op
         * never commits, leaving the queue "resurrected" on the other replicas. */
        beaver_queue_t *dq = broker_get_queue(p->conn->server->broker,
                                              p->vhost, qname);
        if (!dq) {
            send_channel_close(p, channel, 404, "NOT_FOUND - no such queue",
                               BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
            return;
        }
        uint64_t owner = queue_exclusive_owner(dq);
        int was_durable = (queue_flags(dq) & BMQP_FLAG_DURABLE) != 0;
        uint32_t msgcount = (uint32_t)queue_depth(dq);
        int has_consumers = queue_consumer_count(dq) > 0;
        queue_unref(dq);
        if (owner != 0 && owner != p->conn->id) {
            send_channel_close(p, channel, 405,
                "RESOURCE_LOCKED - queue is exclusive to another connection",
                BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
            return;
        }
        if (if_unused && has_consumers) {
            send_channel_close(p, channel, 406,
                "PRECONDITION_FAILED - queue in use (has consumers)",
                BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
            return;
        }
        if (if_empty && msgcount > 0) {
            send_channel_close(p, channel, 406,
                "PRECONDITION_FAILED - queue not empty",
                BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
            return;
        }

        if (p->conn->server->cluster && was_durable) {
            /* Replicated delete: tracked proposal, apply-only, confirmed after
             * commit (Delete-Ok carries the pre-delete message count). */
            uint64_t seq = cluster_replicate_delete_queue(p->conn->server->cluster,
                                                          p->vhost, qname);
            if (no_wait)
                break;
            if (seq == 0) {
                send_channel_close(p, channel, 541,
                                   "INTERNAL_ERROR - failed to replicate "
                                   "Queue.Delete", BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE);
                break;
            }
            await_cluster_commit(p, channel, seq, BMQP_CLASS_QUEUE,
                                 BMQP_QUEUE_DELETE, PENDING_QUEUE_DELETE_OK,
                                 NULL, msgcount);
            break;
        }

        /* Local path (standalone, or a non-durable queue). */
        broker_delete_queue(p->conn->server->broker, p->vhost, qname,
                            if_unused, if_empty, &msgcount);
        /* Forget it from our exclusive-cleanup list (it's already gone). */
        for (size_t i = 0; i < p->n_excl; i++) {
            if (strcmp(p->excl_queues[i], qname) == 0) {
                free(p->excl_queues[i]);
                p->excl_queues[i] = p->excl_queues[--p->n_excl];
                break;
            }
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Queue.Delete '%s' (%u message(s))",
                 p->conn->id, channel, qname, msgcount);
        if (!no_wait) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_u32(&a, msgcount); /* message-count */
            send_method(p, channel, BMQP_CLASS_QUEUE, BMQP_QUEUE_DELETE_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    default:
        proto_fatal(p, "unexpected queue method %u", method);
    }
}

/* ========================================================================= */
/* confirm class (publisher confirms)                                         */
/* ========================================================================= */

static void handle_confirm(beaver_proto_t *p, uint16_t channel,
                           uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;
    if (method != BMQP_CONFIRM_SELECT) {
        proto_fatal(p, "unsupported confirm method %u", method);
        return;
    }
    uint8_t nowait = bmqp_read_u8(r) & 0x01;
    if (r->error) {
        proto_fatal(p, "malformed Confirm.Select");
        return;
    }
    proto_chan_t *ch = channel_find(p, channel);
    if (ch)
        ch->confirm_mode = 1; /* subsequent publishes are (n)acked */
    LOG_INFO("conn #%" PRIu64 " ch=%u: Confirm.Select (publisher confirms on)",
             p->conn->id, channel);
    if (!nowait)
        send_method(p, channel, BMQP_CLASS_CONFIRM, BMQP_CONFIRM_SELECT_OK, NULL);
}

/* ========================================================================= */
/* basic class                                                                */
/* ========================================================================= */

static void handle_basic(beaver_proto_t *p, uint16_t channel,
                         uint16_t method, bmqp_reader_t *r)
{
    if (!require_channel(p, channel))
        return;

    switch (method) {
    case BMQP_BASIC_QOS: {
        bmqp_read_u32(r);                        /* prefetch-size (unsupported) */
        uint16_t prefetch = bmqp_read_u16(r);    /* prefetch-count */
        bmqp_read_u8(r);                         /* global bit (per-channel here) */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Qos");
            return;
        }
        /* Remember the count on the channel (applies to consumers registered
         * later) and update every consumer already on this channel. */
        proto_chan_t *ch = channel_find(p, channel);
        if (ch)
            ch->prefetch = prefetch;
        dispatcher_set_prefetch(p->conn->server->dispatcher, p->conn, channel,
                                prefetch);
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Qos prefetch=%u",
                 p->conn->id, channel, prefetch);
        send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_QOS_OK, NULL);
        break;
    }
    case BMQP_BASIC_PUBLISH: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t en, kn;
        const char *e = bmqp_read_shortstr(r, &en);
        const char *k = bmqp_read_shortstr(r, &kn);
        uint8_t pubbits = bmqp_read_u8(r);      /* bit0 mandatory, bit1 immediate */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Publish");
            return;
        }
        /* Begin content assembly; the content header + body frames follow. */
        publish_reset(p);
        p->pub_active    = 1;
        p->pub_mandatory = (pubbits & 0x01) != 0;
        p->pub_channel   = channel;
        copy_str(p->pub_exchange, sizeof(p->pub_exchange), e, en);
        copy_str(p->pub_routing_key, sizeof(p->pub_routing_key), k, kn);
        if (!name_ok(p->pub_exchange)) {
            proto_fatal(p, "illegal exchange name in Basic.Publish");
            return;
        }
        break;
    }
    case BMQP_BASIC_CONSUME: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn, tn, tbl;
        const char *q = bmqp_read_shortstr(r, &qn);
        const char *t = bmqp_read_shortstr(r, &tn);
        uint8_t bits  = bmqp_read_u8(r);        /* no-local,no-ack,exclusive,no-wait */
        bmqp_read_longstr(r, &tbl);             /* arguments (field table) */
        if (r->error) {
            proto_fatal(p, "malformed Basic.Consume");
            return;
        }
        int no_ack  = (bits & 0x02) != 0;
        int no_wait = (bits & 0x08) != 0;
        char qname[256], ctag[128];
        copy_str(qname, sizeof(qname), q, qn);
        copy_str(ctag, sizeof(ctag), t, tn);
        if (ctag[0] == '\0')
            snprintf(ctag, sizeof(ctag), "ctag-%" PRIu64 "-%" PRIu64,
                     p->conn->id, ++p->consumer_seq);
        if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME);
            return;
        }
        proto_advance(p, BMQP_STATE_ACTIVE);
        if (!require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME))
            return;

        proto_chan_t *ch = channel_find(p, channel);
        if (dispatcher_add_consumer(p->conn->server->dispatcher, p->conn,
                                    channel, ctag, p->vhost, qname, no_ack,
                                    ch ? ch->prefetch : 0) != 0) {
            proto_fatal(p, "Basic.Consume: queue '%s' does not exist", qname);
            return;
        }
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Consume queue='%s' tag='%s' "
                 "(%s)", p->conn->id, channel, qname, ctag,
                 no_ack ? "no-ack" : "manual-ack");

        if (!no_wait) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, ctag, strlen(ctag));
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_CONSUME_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_BASIC_CANCEL: {
        size_t tn;
        const char *t = bmqp_read_shortstr(r, &tn);
        uint8_t no_wait = bmqp_read_u8(r);
        if (r->error) {
            proto_fatal(p, "malformed Basic.Cancel");
            return;
        }
        char ctag[128];
        copy_str(ctag, sizeof(ctag), t, tn);
        dispatcher_cancel(p->conn->server->dispatcher, p->conn, channel, ctag);
        LOG_INFO("conn #%" PRIu64 " ch=%u: Basic.Cancel tag='%s'",
                 p->conn->id, channel, ctag);
        if (!(no_wait & 0x01)) {
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr_n(&a, ctag, strlen(ctag));
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_CANCEL_OK, &a);
            bmqp_buf_free(&a);
        }
        break;
    }
    case BMQP_BASIC_GET: {
        bmqp_read_u16(r);                       /* reserved-1 */
        size_t qn;
        const char *q = bmqp_read_shortstr(r, &qn);
        uint8_t no_ack = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Get");
            return;
        }
        char qname[256];
        copy_str(qname, sizeof(qname), q, qn);
        if (!name_ok(qname)) {
            send_channel_close(p, channel, 406,
                               "PRECONDITION_FAILED - illegal queue name",
                               BMQP_CLASS_BASIC, BMQP_BASIC_GET);
            return;
        }
        if (!require_perm(p, channel, AUTH_READ, qname,
                          BMQP_CLASS_BASIC, BMQP_BASIC_GET))
            return;

        beaver_queue_t *queue = broker_get_queue(p->conn->server->broker,
                                                  p->vhost, qname);
        beaver_message_t *msg = queue ? queue_dequeue(queue) : NULL;
        if (!msg) {
            if (queue)
                queue_unref(queue);
            bmqp_buf_t a;
            bmqp_buf_init(&a);
            bmqp_buf_put_shortstr(&a, "");      /* reserved-1 */
            send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_GET_EMPTY, &a);
            bmqp_buf_free(&a);
            break;
        }
        /* Basic.GetOk: delivery-tag, redelivered, exchange, routing-key,
         * message-count; followed by content header + body. */
        uint64_t tag = ++p->consumer_seq;
        bmqp_buf_t a;
        bmqp_buf_init(&a);
        bmqp_buf_put_u64(&a, tag);                 /* delivery-tag */
        bmqp_buf_put_u8(&a, 0);                    /* redelivered */
        bmqp_buf_put_shortstr(&a, msg->exchange);
        bmqp_buf_put_shortstr(&a, msg->routing_key);
        bmqp_buf_put_u32(&a, (uint32_t)queue_depth(queue)); /* message-count */
        send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_GET_OK, &a);
        bmqp_buf_free(&a);
        protocol_send_content(p->conn, channel, BMQP_CLASS_BASIC, msg->body,
                              msg->body_len, msg->props, msg->props_len,
                              p->conn->frame_max);
        if (no_ack) {
            /* Pull auto-acks: advance + replicate the consume watermark so
             * replica copies on the other nodes drain and the log can
             * compact past it. */
            dispatcher_note_pull(p->conn->server->dispatcher, queue, msg->cluster_id);
            message_unref(msg);
            queue_unref(queue);
        } else {
            /* Manual ack: keep both refs until Basic.Ack/Reject/Nack (or the
             * channel/connection closes, which requeues it) settles it -
             * msg/queue are NOT unref'd here; chan_get_unacked_settle or
             * chan_release_get_unacked does that later. */
            proto_chan_t *pc = channel_find(p, channel);
            if (!pc || !chan_get_unacked_add(pc, tag, msg, queue)) {
                /* OOM tracking the delivery: it's already on the wire, so we
                 * cannot un-send it - fail closed by tearing down the
                 * connection rather than silently losing manual-ack
                 * bookkeeping for a message the client thinks it must ack. */
                message_unref(msg);
                queue_unref(queue);
                proto_fatal(p, "out of memory tracking Basic.Get delivery");
                return;
            }
        }
        break;
    }
    case BMQP_BASIC_ACK: {
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t multiple = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Ack");
            return;
        }
        /* Basic.Get and Basic.Consume deliveries use separate delivery-tag
         * counters, so a "multiple" ack spanning both sources must settle
         * both; for a single tag, only try the dispatcher if it wasn't a
         * tracked Basic.Get delivery (avoids a spurious "unknown tag" log). */
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, multiple, 0, 0) : 0;
        if (multiple || got == 0)
            dispatcher_ack(p->conn->server->dispatcher, p->conn, channel,
                          delivery_tag, multiple);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Ack delivery_tag=%" PRIu64
                  " multiple=%u", p->conn->id, channel, delivery_tag, multiple);
        break;
    }
    case BMQP_BASIC_REJECT: {
        /* delivery-tag (u64), requeue bit. Single-delivery negative ack. */
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t requeue = bmqp_read_u8(r) & 0x01;
        if (r->error) {
            proto_fatal(p, "malformed Basic.Reject");
            return;
        }
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, 0, requeue,
                                                  !requeue) : 0;
        if (got == 0)
            dispatcher_nack(p->conn->server->dispatcher, p->conn, channel,
                           delivery_tag, 0 /* multiple */, requeue);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Reject delivery_tag=%" PRIu64
                  " requeue=%u", p->conn->id, channel, delivery_tag, requeue);
        break;
    }
    case BMQP_BASIC_NACK: {
        /* delivery-tag (u64), bits: 0x01 multiple, 0x02 requeue (RabbitMQ
         * extension; the batch-capable Basic.Reject). */
        uint64_t delivery_tag = bmqp_read_u64(r);
        uint8_t bits = bmqp_read_u8(r);
        if (r->error) {
            proto_fatal(p, "malformed Basic.Nack");
            return;
        }
        int multiple = (bits & 0x01) != 0;
        int requeue2 = (bits & 0x02) != 0;
        proto_chan_t *pc = channel_find(p, channel);
        size_t got = pc ? chan_get_unacked_settle(pc, delivery_tag, multiple,
                                                  requeue2, !requeue2) : 0;
        if (multiple || got == 0)
            dispatcher_nack(p->conn->server->dispatcher, p->conn, channel,
                           delivery_tag, multiple, requeue2);
        LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Nack delivery_tag=%" PRIu64
                  " bits=0x%x", p->conn->id, channel, delivery_tag, bits);
        break;
    }
    default:
        proto_fatal(p, "unexpected basic method %u", method);
    }
}

/* ========================================================================= */
/* content frames (the body of a Basic.Publish)                               */
/* ========================================================================= */

static void handle_content_header(beaver_proto_t *p,
                                  const bmqp_frame_header_t *hdr,
                                  const uint8_t *payload)
{
    if (hdr->channel != p->pub_channel) {
        proto_fatal(p, "content header on channel %u, expected %u",
                    hdr->channel, p->pub_channel);
        return;
    }

    bmqp_reader_t r;
    bmqp_reader_init(&r, payload, hdr->payload_len);
    bmqp_read_u16(&r);                       /* class-id (60) */
    bmqp_read_u16(&r);                       /* weight */
    uint64_t body_size = bmqp_read_u64(&r);  /* total body size */
    if (r.error) {
        proto_fatal(p, "malformed content header");
        return;
    }
    uint64_t body_limit = p->conn->server->max_message_size
                          ? p->conn->server->max_message_size
                          : AMQP_MAX_BODY_SIZE;
    if (body_size > body_limit) {
        proto_fatal(p, "message body size %" PRIu64 " exceeds max_message_size "
                    "%" PRIu64, body_size, body_limit);
        return;
    }

    /* The remainder is the property section (flags + list); keep it verbatim. */
    size_t prop_len = hdr->payload_len - r.pos;
    if (prop_len > 0) {
        p->pub_props = malloc(prop_len);
        if (!p->pub_props) {
            proto_fatal(p, "out of memory storing content properties");
            return;
        }
        memcpy(p->pub_props, payload + r.pos, prop_len);
        p->pub_props_len = prop_len;
    }

    p->pub_body_size     = body_size;
    p->pub_body_received = 0;
    p->pub_have_header   = 1;
    /* pub_body is allocated lazily, and only when the body spans multiple
     * frames (see handle_content_body). The common single-frame case is built
     * straight from the read buffer with no intermediate copy. */
    if (body_size == 0)
        finalize_publish(p, NULL, 0); /* zero-length body: complete immediately */
}

static void handle_content_body(beaver_proto_t *p,
                                const bmqp_frame_header_t *hdr,
                                const uint8_t *payload)
{
    if (hdr->channel != p->pub_channel) {
        proto_fatal(p, "content body on channel %u, expected %u",
                    hdr->channel, p->pub_channel);
        return;
    }
    size_t n = hdr->payload_len;
    if (p->pub_body_received + n > p->pub_body_size) {
        proto_fatal(p, "content body exceeds declared size");
        return;
    }

    /* Fast path: the whole body arrived in this single frame. Build the message
     * directly from the read buffer - no intermediate accumulation copy. */
    if (!p->pub_body && p->pub_body_received == 0 && n == p->pub_body_size) {
        finalize_publish(p, payload, n);
        return;
    }

    /* Slow path: body spans multiple frames - accumulate into pub_body. */
    if (!p->pub_body) {
        p->pub_body = malloc(p->pub_body_size);
        if (!p->pub_body) {
            proto_fatal(p, "out of memory allocating %" PRIu64 "-byte body",
                        p->pub_body_size);
            return;
        }
    }
    if (n)
        memcpy(p->pub_body + p->pub_body_received, payload, n);
    p->pub_body_received += n;
    if (p->pub_body_received == p->pub_body_size)
        finalize_publish(p, p->pub_body, p->pub_body_size);
}

/* Is this AMQP basic-properties section marked persistent (delivery-mode = 2)?
 * Parses only as far as the delivery-mode property (the 4th basic property). */
static int props_is_persistent(const uint8_t *props, size_t len)
{
    if (!props || len < 2)
        return 0;
    bmqp_reader_t r;
    bmqp_reader_init(&r, props, len);
    uint16_t flags = bmqp_read_u16(&r);
    if (!(flags & 0x1000))                            /* no delivery-mode prop */
        return 0;
    size_t n;
    if (flags & 0x8000) bmqp_read_shortstr(&r, &n);   /* content-type     */
    if (flags & 0x4000) bmqp_read_shortstr(&r, &n);   /* content-encoding */
    if (flags & 0x2000) bmqp_read_longstr(&r, &n);    /* headers (table)  */
    uint8_t dm = bmqp_read_u8(&r);
    return !r.error && dm == 2;
}

/* Return an unroutable `mandatory` publish to its sender: a Basic.Return method
 * frame (reply-code + text + the original exchange/routing-key) followed by the
 * message's content header and body, exactly as a delivery carries them. Without
 * this a mandatory publish that matched no queue was dropped silently, which is
 * precisely the case the mandatory flag exists to surface. */
static void send_basic_return(beaver_proto_t *p, uint16_t channel,
                              uint16_t code, const char *text,
                              const char *exchange, const char *routing_key,
                              const uint8_t *body, size_t body_len,
                              const uint8_t *props, size_t props_len)
{
    bmqp_buf_t a;
    bmqp_buf_init(&a);
    bmqp_buf_put_u16(&a, code);
    bmqp_buf_put_shortstr(&a, text);
    bmqp_buf_put_shortstr(&a, exchange);
    bmqp_buf_put_shortstr(&a, routing_key);
    send_method(p, channel, BMQP_CLASS_BASIC, BMQP_BASIC_RETURN, &a);
    bmqp_buf_free(&a);
    /* send_method closes the connection on failure; protocol_send_content is a
     * no-op on a closing connection, so this stays safe either way. */
    protocol_send_content(p->conn, channel, BMQP_CLASS_BASIC, body, body_len,
                          props, props_len, p->conn->frame_max);
}

static void finalize_publish(beaver_proto_t *p, const uint8_t *body,
                             size_t body_len)
{
    beaver_server_t *srv = p->conn->server;

    /* WRITE permission on the target exchange (default exchange "" included). */
    if (!perm_ok(p, AUTH_WRITE, p->pub_exchange)) {
        char text[600];
        snprintf(text, sizeof text,
                 "ACCESS_REFUSED - write access to exchange '%s' in vhost '%s' "
                 "refused for user '%s'", p->pub_exchange, p->vhost, p->user);
        LOG_WARN("conn #%" PRIu64 ": %s", p->conn->id, text);
        send_channel_close(p, p->pub_channel, 403, text,
                           BMQP_CLASS_BASIC, BMQP_BASIC_PUBLISH);
        publish_reset(p);
        return;
    }

    /* Publisher confirms (RabbitMQ's Confirm.Select): once a channel is in
     * confirm mode, every successful publish is answered with a Basic.Ack -
     * IMMEDIATELY for a transient message that is routed locally, or only after
     * the cluster COMMITS it on a quorum for a persistent (replicated) one, so
     * the publisher can tell durably-committed from merely-accepted. */
    proto_chan_t *pubch = channel_find(p, p->pub_channel);
    int confirm = pubch && pubch->confirm_mode;

    /* Persistent messages are replicated through the cluster (forwarded to the
     * leader, committed on a majority, then applied/enqueued on EVERY node).
     * We must NOT also route locally, or the origin node would enqueue twice. */
    int replicated = 0;
    if (srv->cluster && props_is_persistent(p->pub_props, p->pub_props_len)) {
        if (confirm) {
            /* Track the proposal so the confirm follows the actual commit. */
            uint64_t seq = cluster_replicate_publish_tracked(srv->cluster,
                              p->vhost, p->pub_exchange, p->pub_routing_key,
                              body, body_len, p->pub_props, p->pub_props_len);
            replicated = seq != 0;
            if (replicated)
                await_publish_confirm(p, p->pub_channel, seq,
                                      ++pubch->confirm_seq);
        } else {
            replicated = cluster_replicate_publish(srv->cluster, p->vhost,
                             p->pub_exchange, p->pub_routing_key, body, body_len,
                             p->pub_props, p->pub_props_len) == 0;
        }
        /* Flow control: if the cluster is congested, pause this producer's reads
         * (TCP backpressure) so it can't outrun durable replication. */
        if (replicated && cluster_should_throttle(srv->cluster))
            beaver_conn_throttle_read(p->conn);
        if (!replicated) {
            /* Proposals are buffered even without a live leader, so this only
             * fails on OOM / oversized names. NEVER route a persistent message
             * locally instead - the replicas would silently diverge from the
             * origin node. Tell the client so it can retry (a channel exception
             * also implicitly nacks any outstanding confirms). */
            send_channel_close(p, p->pub_channel, 506,
                               "RESOURCE_ERROR - cannot replicate publish; retry",
                               BMQP_CLASS_BASIC, BMQP_BASIC_PUBLISH);
            publish_reset(p);
            return;
        }
    }

    if (!replicated) {
        beaver_message_t *msg =
            message_new_full(p->pub_exchange, p->pub_routing_key, body,
                             body_len, p->pub_props, p->pub_props_len);
        if (!msg) {
            proto_fatal(p, "out of memory building published message");
            return;
        }
        int routed = broker_route(srv->broker, p->vhost, msg);
        message_unref(msg);
        /* Transient publish: nothing to wait for, confirm right away. A
         * resource error (every target queue full or OOM) means the message was
         * NOT accepted anywhere, so it must be Nack'd - the old code sent a
         * positive Basic.Ack unconditionally, telling the publisher a dropped
         * message was safely handled. Unroutable (routed == 0, no matching
         * queue) is still an Ack, matching RabbitMQ: the broker accepted it,
         * there was simply nowhere to route it (mandatory handling is separate).
         */
        if (confirm)
            send_publish_confirm(p, p->pub_channel, ++pubch->confirm_seq,
                                 routed == ROUTE_RESOURCE_ERROR /* nack */);
        /* mandatory: a message that matched no queue is handed back to the
         * publisher (Basic.Return + content) instead of vanishing silently. A
         * resource error is a different failure (already Nack'd above under
         * confirms) and is not a NO_ROUTE. */
        if (p->pub_mandatory && routed == ROUTE_UNROUTABLE)
            send_basic_return(p, p->pub_channel, 312, "NO_ROUTE",
                              p->pub_exchange, p->pub_routing_key,
                              body, body_len, p->pub_props, p->pub_props_len);
    }
    proto_advance(p, BMQP_STATE_ACTIVE);
    /* Local producer flow control: if any queue is over its high-water mark,
     * pause this producer's reads (TCP backpressure) so it cannot outrun the
     * consumers. The per-server throttle timer resumes reads once the broker-wide
     * flow alarm clears (see on_throttle_timer / beaver_conn_throttle_read). This
     * complements the cluster-congestion throttle on the replicated path above. */
    /* Broker-wide memory watermark (RabbitMQ's memory alarm): queues are
     * unlimited by default, so TOTAL memory is what bounds a runaway producer.
     * While the alarm is on, block this publisher - but never a connection that
     * also CONSUMES: pausing that one would stop its ACKs, and the memory we are
     * waiting to be freed can only be freed by consumers acking. Blocking pure
     * publishers is safe because the alarm clears without anything from them. */
    if (queue_memory_alarm_active() &&
        !dispatcher_conn_has_consumers(srv->dispatcher, p->conn)) {
        if (!p->conn_blocked) {
            p->conn_blocked = 1;
            send_connection_blocked(p, "low on memory");
        }
        beaver_conn_throttle_read(p->conn);
    }
    /* NOTE: a queue at its length/byte limit does NOT pause the connection. The
     * limit is itself the memory bound - an over-limit publish is rejected
     * (QUEUE_FULL -> nack under confirms) or drop-head'd, so reading the client
     * costs nothing extra. Pausing the socket here was actively harmful: it
     * stops reading the WHOLE connection, so consumer ACKs queued behind the
     * publishes could not be read either. The prefetch window then never freed,
     * deliveries stopped, the queue never drained and the alarm never cleared -
     * a permanently wedged connection (and, with consumers on separate
     * connections, multi-second stop/go throughput swings). Backpressure that
     * genuinely needs to slow a producer down - cluster replication backlog -
     * still throttles above, and is bounded in time by the throttle timer. */
    /* Hot path: keep at DEBUG so high-throughput publishing isn't throttled by
     * synchronous logging (the LOG_DEBUG macro is a no-op when filtered). */
    LOG_DEBUG("conn #%" PRIu64 " ch=%u: Basic.Publish exchange='%s' key='%s' "
              "body=%" PRIu64 " bytes -> %s",
              p->conn->id, p->pub_channel, p->pub_exchange, p->pub_routing_key,
              p->pub_body_size, replicated ? "replicated" : "routed");
    publish_reset(p);
}

/* ========================================================================= */
/* dispatch                                                                   */
/* ========================================================================= */

static void handle_method(beaver_proto_t *p, uint16_t channel,
                          const uint8_t *payload, size_t plen)
{
    bmqp_reader_t r;
    bmqp_reader_init(&r, payload, plen);
    uint16_t class_id  = bmqp_read_u16(&r);
    uint16_t method_id = bmqp_read_u16(&r);
    if (r.error) {
        proto_fatal(p, "truncated method header");
        return;
    }

    switch (class_id) {
    case BMQP_CLASS_CONNECTION: handle_connection(p, channel, method_id, &r); break;
    case BMQP_CLASS_CHANNEL:    handle_channel(p, channel, method_id, &r);    break;
    case BMQP_CLASS_EXCHANGE:   handle_exchange(p, channel, method_id, &r);   break;
    case BMQP_CLASS_QUEUE:      handle_queue(p, channel, method_id, &r);      break;
    case BMQP_CLASS_BASIC:      handle_basic(p, channel, method_id, &r);      break;
    case BMQP_CLASS_CONFIRM:    handle_confirm(p, channel, method_id, &r);    break;
    default:
        proto_fatal(p, "unknown method class %u", class_id);
    }
}

static void dispatch_frame(beaver_proto_t *p, const bmqp_frame_header_t *hdr,
                           const uint8_t *payload)
{
    /* While assembling a publish, only its content frames are valid. */
    if (p->pub_active) {
        if (!p->pub_have_header) {
            if (hdr->type != BMQP_FRAME_HEADER) {
                proto_fatal(p, "expected content header, got frame type %u",
                            hdr->type);
                return;
            }
            handle_content_header(p, hdr, payload);
        } else {
            if (hdr->type != BMQP_FRAME_BODY) {
                proto_fatal(p, "expected content body, got frame type %u",
                            hdr->type);
                return;
            }
            handle_content_body(p, hdr, payload);
        }
        return;
    }

    switch (hdr->type) {
    case BMQP_FRAME_METHOD:
        handle_method(p, hdr->channel, payload, hdr->payload_len);
        break;
    case BMQP_FRAME_HEARTBEAT:
        LOG_DEBUG("conn #%" PRIu64 ": heartbeat frame", p->conn->id);
        break;
    case BMQP_FRAME_HEADER:
    case BMQP_FRAME_BODY:
        proto_fatal(p, "unexpected content frame (type %u) outside a publish",
                    hdr->type);
        break;
    default:
        proto_fatal(p, "unsupported frame type %u", hdr->type);
    }
}

/* ========================================================================= */
/* public API                                                                 */
/* ========================================================================= */

beaver_proto_t *protocol_conn_new(beaver_conn_t *conn)
{
    beaver_proto_t *p = calloc(1, sizeof(*p));
    if (!p)
        return NULL;
    p->conn  = conn;
    p->state = BMQP_STATE_CONNECTED;
    conn->frame_max = AMQP_DEFAULT_FRAME_MAX;
    return p;
}

void protocol_conn_free(beaver_proto_t *p)
{
    if (!p)
        return;
    /* If a password verification is still running on a worker thread, detach it
     * so its completion callback (which always fires) cleans itself up without
     * touching this now-freed proto. The work object is freed by that callback,
     * not here. */
    if (p->pending_auth)
        p->pending_auth->p = NULL;
    /* Delete any exclusive queues this connection owned (AMQP: they live only
     * as long as the declaring connection). */
    delete_exclusive_queues(p);
    cancel_pending_ops(p, -1);
    for (size_t i = 0; i < p->n_channels; i++)
        chan_release_get_unacked(&p->channels[i]);
    free(p->inbuf);
    free(p->channels);
    free(p->pub_body);
    free(p->pub_props);
    free(p);
}

void protocol_on_data(beaver_proto_t *p, const uint8_t *data, size_t len)
{
    if (p->state == BMQP_STATE_CLOSING)
        return;

    if (!inbuf_append(p, data, len)) {
        proto_fatal(p, "out of memory buffering %zu input bytes", len);
        return;
    }

    /* Step 1: consume the 8-byte AMQP protocol header exactly once. */
    if (!p->header_received) {
        if (p->inbuf_len - p->inbuf_pos < AMQP_PROTOCOL_HEADER_SIZE)
            return; /* wait for the full header */
        if (memcmp(p->inbuf + p->inbuf_pos, AMQP_PROTOCOL_HEADER,
                   AMQP_PROTOCOL_HEADER_SIZE) != 0) {
            /* Reply with the protocol version we speak, then close. */
            beaver_conn_send(p->conn, AMQP_PROTOCOL_HEADER,
                             AMQP_PROTOCOL_HEADER_SIZE);
            proto_fatal(p, "unsupported protocol header (not AMQP 0-9-1)");
            return;
        }
        p->header_received = 1;
        p->inbuf_pos += AMQP_PROTOCOL_HEADER_SIZE;
        proto_advance(p, BMQP_STATE_HANDSHAKE);
        LOG_INFO("conn #%" PRIu64 ": AMQP 0-9-1 handshake started", p->conn->id);
        send_connection_start(p);
    }

    /* Step 2: dispatch every complete frame buffered so far. The cursor
     * (inbuf_pos) advances without shifting memory; the consumed prefix is
     * reclaimed on the next inbuf_append. */
    for (;;) {
        if (p->state == BMQP_STATE_CLOSING)
            return;

        bmqp_frame_header_t hdr;
        const uint8_t *payload = NULL;
        ssize_t n = bmqp_frame_parse(p->inbuf + p->inbuf_pos,
                                     p->inbuf_len - p->inbuf_pos, &hdr, &payload);
        if (n == 0)
            break; /* incomplete; wait for more bytes */
        if (n < 0) {
            proto_fatal(p, "framing error (bad length or missing 0x%02X end)",
                        BMQP_FRAME_END);
            return;
        }
        /* bmqp_frame_parse() only enforces the global BMQP_MAX_FRAME_PAYLOAD
         * cap - not the per-connection frame_max WE negotiated with this
         * client in Connection.Tune/TuneOk (0 before that negotiation
         * completes, hence the guard). A client that agreed to a smaller
         * frame_max but then sends a larger frame anyway is violating the
         * negotiated contract, not just sending a large-but-legal frame. */
        if (p->frame_max > 0 && hdr.payload_len > p->frame_max) {
            proto_fatal(p, "frame of %u bytes exceeds negotiated frame_max=%u",
                       hdr.payload_len, p->frame_max);
            return;
        }
        dispatch_frame(p, &hdr, payload);
        p->inbuf_pos += (size_t)n;
    }
}
