/*
 * frame.c - BMQP framing and field codec implementation.
 *
 * All multi-byte integers are encoded big-endian (network order) using
 * explicit shifts, so the code is portable regardless of host endianness.
 */
#include "frame.h"

#include <stdlib.h>
#include <string.h>

/* The AMQP 0-9-1 protocol header: "AMQP" followed by 0, 0, 9, 1. */
const uint8_t AMQP_PROTOCOL_HEADER[AMQP_PROTOCOL_HEADER_SIZE] = {
    'A', 'M', 'Q', 'P', 0, 0, 9, 1
};

/* ========================================================================= */
/* Reader                                                                     */
/* ========================================================================= */

void bmqp_reader_init(bmqp_reader_t *r, const uint8_t *data, size_t len)
{
    r->data  = data;
    r->len   = len;
    r->pos   = 0;
    r->error = 0;
}

/* Returns 1 and advances if `need` bytes are available, else flags error. */
static int reader_take(bmqp_reader_t *r, size_t need, size_t *at)
{
    if (r->error || r->pos + need > r->len) {
        r->error = 1;
        return 0;
    }
    *at = r->pos;
    r->pos += need;
    return 1;
}

uint8_t bmqp_read_u8(bmqp_reader_t *r)
{
    size_t at;
    if (!reader_take(r, 1, &at))
        return 0;
    return r->data[at];
}

uint16_t bmqp_read_u16(bmqp_reader_t *r)
{
    size_t at;
    if (!reader_take(r, 2, &at))
        return 0;
    return (uint16_t)((uint16_t)r->data[at] << 8 | r->data[at + 1]);
}

uint32_t bmqp_read_u32(bmqp_reader_t *r)
{
    size_t at;
    if (!reader_take(r, 4, &at))
        return 0;
    return (uint32_t)r->data[at] << 24 | (uint32_t)r->data[at + 1] << 16 |
           (uint32_t)r->data[at + 2] << 8 | (uint32_t)r->data[at + 3];
}

uint64_t bmqp_read_u64(bmqp_reader_t *r)
{
    size_t at;
    if (!reader_take(r, 8, &at))
        return 0;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = (v << 8) | r->data[at + i];
    return v;
}

const char *bmqp_read_shortstr(bmqp_reader_t *r, size_t *out_len)
{
    uint8_t n = bmqp_read_u8(r);
    size_t at;
    if (!reader_take(r, n, &at)) {
        *out_len = 0;
        return NULL;
    }
    *out_len = n;
    return (const char *)(r->data + at);
}

const char *bmqp_read_longstr(bmqp_reader_t *r, size_t *out_len)
{
    uint32_t n = bmqp_read_u32(r);
    size_t at;
    if (!reader_take(r, n, &at)) {
        *out_len = 0;
        return NULL;
    }
    *out_len = n;
    return (const char *)(r->data + at);
}

size_t bmqp_reader_remaining(const bmqp_reader_t *r)
{
    return r->pos <= r->len ? r->len - r->pos : 0;
}

/* ========================================================================= */
/* Output buffer                                                              */
/* ========================================================================= */

void bmqp_buf_init(bmqp_buf_t *b)
{
    b->data  = NULL;
    b->len   = 0;
    b->cap   = 0;
    b->error = 0;
}

void bmqp_buf_free(bmqp_buf_t *b)
{
    free(b->data);
    b->data  = NULL;
    b->len   = 0;
    b->cap   = 0;
    b->error = 0;
}

void bmqp_buf_reset(bmqp_buf_t *b)
{
    b->len = 0;
    /* keep error sticky until explicitly re-init'd */
}

/* Ensure room for `extra` more bytes; sets ->error on OOM. */
static int buf_reserve(bmqp_buf_t *b, size_t extra)
{
    if (b->error)
        return 0;
    if (b->len + extra <= b->cap)
        return 1;

    size_t newcap = b->cap ? b->cap : 64;
    while (newcap < b->len + extra)
        newcap *= 2;

    uint8_t *p = realloc(b->data, newcap);
    if (!p) {
        b->error = 1;
        return 0;
    }
    b->data = p;
    b->cap  = newcap;
    return 1;
}

void bmqp_buf_put_u8(bmqp_buf_t *b, uint8_t v)
{
    if (!buf_reserve(b, 1))
        return;
    b->data[b->len++] = v;
}

void bmqp_buf_put_u16(bmqp_buf_t *b, uint16_t v)
{
    if (!buf_reserve(b, 2))
        return;
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)(v & 0xFF);
}

void bmqp_buf_put_u32(bmqp_buf_t *b, uint32_t v)
{
    if (!buf_reserve(b, 4))
        return;
    b->data[b->len++] = (uint8_t)(v >> 24);
    b->data[b->len++] = (uint8_t)(v >> 16);
    b->data[b->len++] = (uint8_t)(v >> 8);
    b->data[b->len++] = (uint8_t)(v & 0xFF);
}

void bmqp_buf_put_u64(bmqp_buf_t *b, uint64_t v)
{
    if (!buf_reserve(b, 8))
        return;
    for (int i = 7; i >= 0; i--)
        b->data[b->len++] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

void bmqp_buf_put_bytes(bmqp_buf_t *b, const void *data, size_t len)
{
    if (len == 0)
        return;
    if (!buf_reserve(b, len))
        return;
    memcpy(b->data + b->len, data, len);
    b->len += len;
}

void bmqp_buf_put_shortstr_n(bmqp_buf_t *b, const char *s, size_t n)
{
    if (n > 255)
        n = 255; /* shortstr length field is a single byte */
    bmqp_buf_put_u8(b, (uint8_t)n);
    bmqp_buf_put_bytes(b, s, n);
}

void bmqp_buf_put_shortstr(bmqp_buf_t *b, const char *s)
{
    bmqp_buf_put_shortstr_n(b, s, s ? strlen(s) : 0);
}

void bmqp_buf_put_longstr(bmqp_buf_t *b, const void *data, size_t len)
{
    bmqp_buf_put_u32(b, (uint32_t)len);
    bmqp_buf_put_bytes(b, data, len);
}

/* ========================================================================= */
/* Frame envelope                                                             */
/* ========================================================================= */

ssize_t bmqp_frame_parse(const uint8_t *buf, size_t len,
                         bmqp_frame_header_t *out_hdr,
                         const uint8_t **out_payload)
{
    if (len < BMQP_FRAME_HEADER_SIZE)
        return 0; /* not even a full header yet */

    bmqp_frame_header_t h;
    h.type        = buf[0];
    h.channel     = (uint16_t)((uint16_t)buf[1] << 8 | buf[2]);
    h.payload_len = (uint32_t)buf[3] << 24 | (uint32_t)buf[4] << 16 |
                    (uint32_t)buf[5] << 8 | (uint32_t)buf[6];

    if (h.payload_len > BMQP_MAX_FRAME_PAYLOAD)
        return -1; /* corrupt or hostile length */

    size_t total = BMQP_FRAME_HEADER_SIZE + h.payload_len + 1; /* +end byte */
    if (len < total)
        return 0; /* full frame not buffered yet */

    if (buf[total - 1] != BMQP_FRAME_END)
        return -1; /* framing lost: missing sentinel */

    *out_hdr     = h;
    *out_payload = buf + BMQP_FRAME_HEADER_SIZE;
    return (ssize_t)total;
}

void bmqp_frame_write(bmqp_buf_t *out, uint8_t type, uint16_t channel,
                      const uint8_t *payload, size_t payload_len)
{
    bmqp_buf_put_u8(out, type);
    bmqp_buf_put_u16(out, channel);
    bmqp_buf_put_u32(out, (uint32_t)payload_len);
    bmqp_buf_put_bytes(out, payload, payload_len);
    bmqp_buf_put_u8(out, BMQP_FRAME_END);
}

size_t bmqp_frame_begin(bmqp_buf_t *out, uint8_t type, uint16_t channel)
{
    bmqp_buf_put_u8(out, type);
    bmqp_buf_put_u16(out, channel);
    bmqp_buf_put_u32(out, 0);   /* payload length placeholder (back-patched) */
    return out->len;            /* payload starts here */
}

void bmqp_frame_finish(bmqp_buf_t *out, size_t payload_start)
{
    if (out->error)
        return; /* a reserve failed somewhere; caller checks ->error */
    size_t payload_len = out->len - payload_start;
    size_t lp = payload_start - 4; /* the u32 length field precedes the payload */
    out->data[lp]     = (uint8_t)(payload_len >> 24);
    out->data[lp + 1] = (uint8_t)(payload_len >> 16);
    out->data[lp + 2] = (uint8_t)(payload_len >> 8);
    out->data[lp + 3] = (uint8_t)(payload_len & 0xFF);
    bmqp_buf_put_u8(out, BMQP_FRAME_END);
}
