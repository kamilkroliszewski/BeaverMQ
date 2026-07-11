/*
 * frame.h - AMQP 0-9-1 binary framing and field codecs for BeaverMQ.
 *
 * This module implements the AMQP 0-9-1 wire format: the generic frame
 * envelope and the primitive field types (octet, short, long, longlong,
 * short string, long string) that method arguments and content headers are
 * built from. It knows nothing about protocol semantics (that lives in
 * protocol.c). The encoding is byte-for-byte compatible with RabbitMQ, so
 * standard clients (pika, the Java/PHP clients, etc.) interoperate directly.
 *
 * The buffer/reader helpers keep the historical bmqp_* prefix; the bytes they
 * produce are pure AMQP 0-9-1.
 *
 * Frame envelope (all integers big-endian / network order):
 *
 *     +--------+-----------+------------------+-----------------+--------+
 *     | type   | channel   | payload_len      | payload ...     | 0xCE   |
 *     | u8     | u16       | u32              | payload_len B   | u8     |
 *     +--------+-----------+------------------+-----------------+--------+
 *      <--------- 7-byte header --------------> <-- payload --->  <-end->
 *
 * A method-frame payload is: class_id(u16) | method_id(u16) | arguments.
 *
 * Two helper types do all the heavy lifting safely:
 *   - bmqp_reader_t : bounds-checked cursor over an input buffer. Any
 *                     out-of-bounds read sets ->error and returns zero/NULL;
 *                     callers check ->error once after parsing.
 *   - bmqp_buf_t    : growable output buffer. Allocation failure sets ->error
 *                     and turns subsequent appends into no-ops; callers check
 *                     ->error once before sending.
 */
#ifndef BEAVERMQ_FRAME_H
#define BEAVERMQ_FRAME_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h> /* ssize_t */

#ifdef __cplusplus
extern "C" {
#endif

/* ---- protocol constants -------------------------------------------------- */

#define BMQP_FRAME_HEADER_SIZE 7      /* type(1) + channel(2) + len(4) */
#define BMQP_FRAME_END         0xCE   /* trailing sentinel byte */
#define BMQP_FRAME_OVERHEAD    (BMQP_FRAME_HEADER_SIZE + 1)

/* Frame types (AMQP 0-9-1). */
#define BMQP_FRAME_METHOD    1
#define BMQP_FRAME_HEADER    2   /* content header (properties + body size) */
#define BMQP_FRAME_BODY      3   /* content body bytes */
#define BMQP_FRAME_HEARTBEAT 8

/* 8-byte greeting an AMQP 0-9-1 client sends on connect: "AMQP" 0 0 9 1. */
#define AMQP_PROTOCOL_HEADER_SIZE 8
extern const uint8_t AMQP_PROTOCOL_HEADER[AMQP_PROTOCOL_HEADER_SIZE];

/* Guard against absurd allocations from a hostile/corrupt length field. */
#define BMQP_MAX_FRAME_PAYLOAD (16u * 1024u * 1024u) /* 16 MiB */

/* ---- parsed frame header ------------------------------------------------- */

typedef struct {
    uint8_t  type;
    uint16_t channel;
    uint32_t payload_len;
} bmqp_frame_header_t;

/* ---- input reader (bounds-checked) --------------------------------------- */

typedef struct {
    const uint8_t *data;
    size_t         len;
    size_t         pos;
    int            error; /* nonzero once any read ran past the end */
} bmqp_reader_t;

void     bmqp_reader_init(bmqp_reader_t *r, const uint8_t *data, size_t len);
uint8_t  bmqp_read_u8(bmqp_reader_t *r);
uint16_t bmqp_read_u16(bmqp_reader_t *r);
uint32_t bmqp_read_u32(bmqp_reader_t *r);
uint64_t bmqp_read_u64(bmqp_reader_t *r);

/*
 * Read a short string (u8 length prefix) or long string (u32 length prefix).
 * Returns a pointer INTO the reader's buffer (no allocation, not NUL
 * terminated) and writes the length to *out_len. On error returns NULL and
 * sets ->error. The caller must copy the bytes if they must outlive the input.
 */
const char *bmqp_read_shortstr(bmqp_reader_t *r, size_t *out_len);
const char *bmqp_read_longstr(bmqp_reader_t *r, size_t *out_len);

/* Bytes not yet consumed. */
size_t bmqp_reader_remaining(const bmqp_reader_t *r);

/* ---- output buffer (growable) -------------------------------------------- */

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
    int      error; /* nonzero if an allocation failed */
} bmqp_buf_t;

void bmqp_buf_init(bmqp_buf_t *b);
void bmqp_buf_free(bmqp_buf_t *b);
void bmqp_buf_reset(bmqp_buf_t *b);

void bmqp_buf_put_u8(bmqp_buf_t *b, uint8_t v);
void bmqp_buf_put_u16(bmqp_buf_t *b, uint16_t v);
void bmqp_buf_put_u32(bmqp_buf_t *b, uint32_t v);
void bmqp_buf_put_u64(bmqp_buf_t *b, uint64_t v);
void bmqp_buf_put_bytes(bmqp_buf_t *b, const void *data, size_t len);

/* Short string: u8 length + bytes (length must be <= 255). */
void bmqp_buf_put_shortstr(bmqp_buf_t *b, const char *s);
void bmqp_buf_put_shortstr_n(bmqp_buf_t *b, const char *s, size_t n);
/* Long string: u32 length + bytes. */
void bmqp_buf_put_longstr(bmqp_buf_t *b, const void *data, size_t len);

/* ---- frame envelope ------------------------------------------------------ */

/*
 * Attempt to parse one full frame from the front of buf[0..len).
 * Returns:
 *    >0  total bytes consumed; *out_hdr filled, *out_payload points into buf.
 *     0  incomplete - need more bytes.
 *    -1  protocol error (oversized payload or bad frame-end sentinel).
 */
ssize_t bmqp_frame_parse(const uint8_t *buf, size_t len,
                         bmqp_frame_header_t *out_hdr,
                         const uint8_t **out_payload);

/*
 * Append a complete frame (header + payload + end sentinel) to `out`,
 * wrapping the already-encoded payload bytes.
 */
void bmqp_frame_write(bmqp_buf_t *out, uint8_t type, uint16_t channel,
                      const uint8_t *payload, size_t payload_len);

/*
 * In-place frame construction: write a frame header with a placeholder length,
 * then append the payload fields directly into `out` (no temporary buffer), then
 * call bmqp_frame_finish() to back-patch the length and append the end sentinel.
 *
 *     size_t at = bmqp_frame_begin(out, BMQP_FRAME_METHOD, channel);
 *     bmqp_buf_put_u16(out, class_id); ... more fields ...
 *     bmqp_frame_finish(out, at);
 *
 * bmqp_frame_begin returns the payload start offset to pass to bmqp_frame_finish.
 */
size_t bmqp_frame_begin(bmqp_buf_t *out, uint8_t type, uint16_t channel);
void   bmqp_frame_finish(bmqp_buf_t *out, size_t payload_start);

#ifdef __cplusplus
}
#endif

#endif /* BEAVERMQ_FRAME_H */
