/*
 * protocol.h - BMQP protocol state machine for BeaverMQ.
 *
 * One beaver_proto_t is attached to each TCP connection. It accumulates
 * incoming bytes, extracts complete frames (via frame.c), drives the
 * per-connection state machine, parses method arguments, and writes response
 * frames back through the network layer.
 *
 * Connection lifecycle / state progression (matches the project blueprint):
 *
 *     CONNECTED ──header──> HANDSHAKE ──Conn.Open──> (connection open)
 *         │                     │
 *         │                     └── Channel.Open ──> CHANNEL_OPEN
 *         │                                              │
 *         │                          Queue/Basic op ──> ACTIVE
 *
 * Phase 3 scope: the handshake, channel open/close, and parsing of
 * Queue.Declare / Exchange.Declare / Queue.Bind / Basic.Publish /
 * Basic.Consume are implemented and answered with the appropriate "Ok"
 * methods. Actual queue storage and message routing are stubbed here and land
 * in Phases 4-5.
 */
#ifndef BEAVERMQ_PROTOCOL_H
#define BEAVERMQ_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Avoid pulling net.h into every includer; only the .c needs the full type. */
typedef struct beaver_conn  beaver_conn_t;
typedef struct beaver_proto beaver_proto_t;

/* ---- method classes ------------------------------------------------------ */
#define BMQP_CLASS_CONNECTION 10
#define BMQP_CLASS_CHANNEL    20
#define BMQP_CLASS_EXCHANGE   40
#define BMQP_CLASS_QUEUE      50
#define BMQP_CLASS_BASIC      60

/* ---- connection methods (class 10) --------------------------------------- */
#define BMQP_CONNECTION_START     10  /* S->C */
#define BMQP_CONNECTION_START_OK  11  /* C->S */
#define BMQP_CONNECTION_TUNE      30  /* S->C */
#define BMQP_CONNECTION_TUNE_OK   31  /* C->S */
#define BMQP_CONNECTION_OPEN      40  /* C->S */
#define BMQP_CONNECTION_OPEN_OK   41  /* S->C */
#define BMQP_CONNECTION_CLOSE     50  /* either */
#define BMQP_CONNECTION_CLOSE_OK  51  /* either */

/* ---- channel methods (class 20) ------------------------------------------ */
#define BMQP_CHANNEL_OPEN      10  /* C->S */
#define BMQP_CHANNEL_OPEN_OK   11  /* S->C */
#define BMQP_CHANNEL_CLOSE     40  /* either */
#define BMQP_CHANNEL_CLOSE_OK  41  /* either */

/* ---- exchange methods (class 40) ----------------------------------------- */
#define BMQP_EXCHANGE_DECLARE     10  /* C->S */
#define BMQP_EXCHANGE_DECLARE_OK  11  /* S->C */

/* ---- queue methods (class 50) -------------------------------------------- */
#define BMQP_QUEUE_DECLARE     10  /* C->S */
#define BMQP_QUEUE_DECLARE_OK  11  /* S->C */
#define BMQP_QUEUE_BIND        20  /* C->S */
#define BMQP_QUEUE_BIND_OK     21  /* S->C */

/* ---- basic methods (class 60) -------------------------------------------- */
#define BMQP_BASIC_CONSUME     20  /* C->S */
#define BMQP_BASIC_CONSUME_OK  21  /* S->C */
#define BMQP_BASIC_QOS         10  /* C->S */
#define BMQP_BASIC_QOS_OK      11  /* S->C */
#define BMQP_BASIC_CANCEL      30  /* C->S */
#define BMQP_BASIC_CANCEL_OK   31  /* S->C */
#define BMQP_BASIC_PUBLISH     40  /* C->S */
#define BMQP_BASIC_DELIVER     60  /* S->C */
#define BMQP_BASIC_GET         70  /* C->S */
#define BMQP_BASIC_GET_OK      71  /* S->C */
#define BMQP_BASIC_GET_EMPTY   72  /* S->C */
#define BMQP_BASIC_ACK         80  /* C->S */

/* ---- queue/exchange flag bits (in the u8 flags argument) ----------------- */
#define BMQP_FLAG_PASSIVE      0x01
#define BMQP_FLAG_DURABLE      0x02
#define BMQP_FLAG_EXCLUSIVE    0x04
#define BMQP_FLAG_AUTO_DELETE  0x08
#define BMQP_FLAG_NO_ACK       0x10  /* basic.consume */

/* ---- per-connection protocol state --------------------------------------- */
typedef enum {
    BMQP_STATE_CONNECTED = 0, /* TCP up, awaiting 8-byte protocol header */
    BMQP_STATE_HANDSHAKE,     /* header seen; negotiating the connection */
    BMQP_STATE_CHANNEL_OPEN,  /* a channel has been opened */
    BMQP_STATE_ACTIVE,        /* queue/exchange/basic operations in progress */
    BMQP_STATE_CLOSING        /* terminal: shutting the connection down */
} bmqp_state_t;

const char *bmqp_state_name(bmqp_state_t s);

/* ---- integration API (called from the network layer) --------------------- */

/* Create protocol state bound to `conn`. Returns NULL on OOM. */
beaver_proto_t *protocol_conn_new(beaver_conn_t *conn);

/* Release all protocol state. Safe with NULL. */
void protocol_conn_free(beaver_proto_t *p);

/*
 * Feed `len` freshly-read bytes into the state machine. Buffers partial
 * frames internally, dispatches every complete frame, and may send response
 * frames or close the connection on protocol violations.
 */
void protocol_on_data(beaver_proto_t *p, const uint8_t *data, size_t len);

/* Current state (for diagnostics / the management API later). */
bmqp_state_t protocol_state(const beaver_proto_t *p);

/*
 * Build a method frame (class_id + method_id + args) and send it on `conn`.
 * `args` are the already-encoded bytes that follow the method header (may be
 * NULL/0). Returns 0 on success or -1 on failure. Exposed so the consumer
 * dispatcher can emit Basic.Deliver frames without duplicating framing logic.
 */
int protocol_send_method(beaver_conn_t *conn, uint16_t channel,
                         uint16_t class_id, uint16_t method_id,
                         const uint8_t *args, size_t args_len);

/*
 * Send an AMQP content header frame followed by content body frame(s) for a
 * message. `props` is the raw property section (property-flags + property
 * list) to replay, or NULL for none. The body is split into frames no larger
 * than `frame_max`. Used after a Basic.Deliver / Basic.GetOk method frame.
 * Returns 0 on success, -1 on failure.
 */
int protocol_send_content(beaver_conn_t *conn, uint16_t channel,
                          uint16_t class_id, const void *body, size_t body_len,
                          const void *props, size_t props_len,
                          uint32_t frame_max);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_PROTOCOL_H */
