/*
 * test_frame.c - unit tests for src/frame.c (AMQP 0-9-1 framing + codecs).
 * Round-trips the output buffer through the bounds-checked reader, exercises
 * partial-frame buffering, and checks the two documented failure modes of
 * bmqp_frame_parse() (oversized length, missing end sentinel).
 */
#include "frame.h"
#include "test_util.h"

#include <string.h>

static void test_buf_put_primitives(void)
{
    TEST_SECTION("bmqp_buf_t primitive encoding (big-endian)");
    bmqp_buf_t b;
    bmqp_buf_init(&b);

    bmqp_buf_put_u8(&b, 0xAB);
    bmqp_buf_put_u16(&b, 0x1234);
    bmqp_buf_put_u32(&b, 0xDEADBEEFu);
    bmqp_buf_put_u64(&b, 0x0102030405060708ull);

    CHECK(!b.error);
    CHECK_EQ(b.len, 1 + 2 + 4 + 8);
    static const uint8_t expect[] = {
        0xAB,
        0x12, 0x34,
        0xDE, 0xAD, 0xBE, 0xEF,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    };
    CHECK(memcmp(b.data, expect, sizeof(expect)) == 0);

    bmqp_buf_free(&b);
}

static void test_buf_strings(void)
{
    TEST_SECTION("shortstr / longstr encoding + shortstr truncation at 255");
    bmqp_buf_t b;
    bmqp_buf_init(&b);

    bmqp_buf_put_shortstr(&b, "hello");
    CHECK_EQ(b.len, 1 + 5);
    CHECK_EQ(b.data[0], 5);
    CHECK(memcmp(b.data + 1, "hello", 5) == 0);

    bmqp_buf_reset(&b);
    char long_name[300];
    memset(long_name, 'x', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    bmqp_buf_put_shortstr(&b, long_name); /* longer than 255: must clamp */
    CHECK_EQ(b.data[0], 255);
    CHECK_EQ(b.len, 1 + 255);

    bmqp_buf_reset(&b);
    const char body[] = "payload bytes";
    bmqp_buf_put_longstr(&b, body, sizeof(body) - 1);
    CHECK_EQ(b.len, 4 + sizeof(body) - 1);
    uint32_t declared_len = (uint32_t)b.data[0] << 24 | (uint32_t)b.data[1] << 16 |
                            (uint32_t)b.data[2] << 8 | b.data[3];
    CHECK_EQ(declared_len, sizeof(body) - 1);
    CHECK(memcmp(b.data + 4, body, sizeof(body) - 1) == 0);

    bmqp_buf_free(&b);
}

static void test_buf_growth(void)
{
    TEST_SECTION("bmqp_buf_t growth across many reallocations");
    bmqp_buf_t b;
    bmqp_buf_init(&b);
    /* Initial capacity is 64 bytes and doubles; 10000 single-byte puts forces
     * several reallocations. Every byte must survive each realloc intact. */
    for (int i = 0; i < 10000; i++)
        bmqp_buf_put_u8(&b, (uint8_t)(i & 0xFF));
    CHECK(!b.error);
    CHECK_EQ(b.len, 10000);
    int ok = 1;
    for (int i = 0; i < 10000; i++)
        if (b.data[i] != (uint8_t)(i & 0xFF)) { ok = 0; break; }
    CHECK(ok);
    bmqp_buf_free(&b);
}

static void test_reader_round_trip(void)
{
    TEST_SECTION("bmqp_reader_t round-trips everything bmqp_buf_t wrote");
    bmqp_buf_t b;
    bmqp_buf_init(&b);
    bmqp_buf_put_u8(&b, 7);
    bmqp_buf_put_u16(&b, 4242);
    bmqp_buf_put_u32(&b, 1234567890u);
    bmqp_buf_put_u64(&b, 0x1122334455667788ull);
    bmqp_buf_put_shortstr(&b, "queue-name");
    bmqp_buf_put_longstr(&b, "a longer body", 13);

    bmqp_reader_t r;
    bmqp_reader_init(&r, b.data, b.len);
    CHECK_EQ(bmqp_read_u8(&r), 7);
    CHECK_EQ(bmqp_read_u16(&r), 4242);
    CHECK_EQ(bmqp_read_u32(&r), 1234567890u);
    CHECK_EQ(bmqp_read_u64(&r), (int64_t)0x1122334455667788ull);
    size_t slen = 0;
    const char *s = bmqp_read_shortstr(&r, &slen);
    CHECK_EQ(slen, 10);
    CHECK(s && memcmp(s, "queue-name", 10) == 0);
    size_t llen = 0;
    const char *l = bmqp_read_longstr(&r, &llen);
    CHECK_EQ(llen, 13);
    CHECK(l && memcmp(l, "a longer body", 13) == 0);
    CHECK(!r.error);
    CHECK_EQ(bmqp_reader_remaining(&r), 0);

    bmqp_buf_free(&b);
}

static void test_reader_bounds_checking(void)
{
    TEST_SECTION("bmqp_reader_t flags ->error on out-of-bounds reads (never reads OOB)");
    uint8_t tiny[2] = { 0xAA, 0xBB };
    bmqp_reader_t r;

    /* u32 needs 4 bytes; only 2 are available. */
    bmqp_reader_init(&r, tiny, sizeof(tiny));
    uint32_t v = bmqp_read_u32(&r);
    CHECK_EQ(v, 0); /* short read returns 0, not garbage */
    CHECK(r.error);

    /* Once flagged, the reader stays sticky-broken: a subsequent read that
     * WOULD be in-bounds on its own must still fail. */
    bmqp_reader_init(&r, tiny, sizeof(tiny));
    bmqp_read_u8(&r);          /* consumes byte 0, fine */
    bmqp_read_u32(&r);         /* fails: only 1 byte left */
    CHECK(r.error);
    uint8_t after = bmqp_read_u8(&r); /* would have byte 1 available, but reader is broken */
    CHECK_EQ(after, 0);
    CHECK(r.error);

    /* A shortstr claiming more bytes than remain must fail, not overrun. */
    uint8_t claim_big[1] = { 200 }; /* says "200 bytes follow"; none do */
    bmqp_reader_init(&r, claim_big, sizeof(claim_big));
    size_t out_len = 999;
    const char *s = bmqp_read_shortstr(&r, &out_len);
    CHECK(s == NULL);
    CHECK_EQ(out_len, 0);
    CHECK(r.error);
}

static void test_frame_round_trip(void)
{
    TEST_SECTION("bmqp_frame_parse round-trips a frame built with bmqp_frame_begin/finish");
    bmqp_buf_t out;
    bmqp_buf_init(&out);
    size_t at = bmqp_frame_begin(&out, BMQP_FRAME_METHOD, 3 /* channel */);
    bmqp_buf_put_u16(&out, 10);  /* pretend class_id */
    bmqp_buf_put_u16(&out, 40);  /* pretend method_id */
    bmqp_buf_put_shortstr(&out, "hi");
    bmqp_frame_finish(&out, at);
    CHECK(!out.error);

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;
    ssize_t n = bmqp_frame_parse(out.data, out.len, &hdr, &payload);
    CHECK_EQ(n, (ssize_t)out.len);
    CHECK_EQ(hdr.type, BMQP_FRAME_METHOD);
    CHECK_EQ(hdr.channel, 3);
    CHECK_EQ(hdr.payload_len, 2 + 2 + 1 + 2); /* class+method+shortstr(len+"hi") */

    bmqp_reader_t r;
    bmqp_reader_init(&r, payload, hdr.payload_len);
    CHECK_EQ(bmqp_read_u16(&r), 10);
    CHECK_EQ(bmqp_read_u16(&r), 40);
    size_t slen = 0;
    const char *s = bmqp_read_shortstr(&r, &slen);
    CHECK_EQ(slen, 2);
    CHECK(s && memcmp(s, "hi", 2) == 0);

    bmqp_buf_free(&out);
}

static void test_frame_write_helper(void)
{
    TEST_SECTION("bmqp_frame_write produces a frame bmqp_frame_parse accepts");
    const uint8_t body[] = { 1, 2, 3, 4, 5 };
    bmqp_buf_t out;
    bmqp_buf_init(&out);
    bmqp_frame_write(&out, BMQP_FRAME_BODY, 1, body, sizeof(body));

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;
    ssize_t n = bmqp_frame_parse(out.data, out.len, &hdr, &payload);
    CHECK_EQ(n, (ssize_t)(BMQP_FRAME_HEADER_SIZE + sizeof(body) + 1));
    CHECK_EQ(hdr.type, BMQP_FRAME_BODY);
    CHECK_EQ(hdr.payload_len, sizeof(body));
    CHECK(memcmp(payload, body, sizeof(body)) == 0);

    bmqp_buf_free(&out);
}

static void test_frame_partial_buffering(void)
{
    TEST_SECTION("bmqp_frame_parse returns 0 (need more bytes), never reads past what's given");
    const uint8_t body[] = { 9, 9, 9 };
    bmqp_buf_t out;
    bmqp_buf_init(&out);
    bmqp_frame_write(&out, BMQP_FRAME_BODY, 0, body, sizeof(body));

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;

    /* Fewer than 7 bytes: not even a full header. */
    CHECK_EQ(bmqp_frame_parse(out.data, 3, &hdr, &payload), 0);
    /* Exactly the header, no payload/end byte yet. */
    CHECK_EQ(bmqp_frame_parse(out.data, BMQP_FRAME_HEADER_SIZE, &hdr, &payload), 0);
    /* Header + partial payload, still missing the end sentinel. */
    CHECK_EQ(bmqp_frame_parse(out.data, out.len - 1, &hdr, &payload), 0);
    /* The full frame parses once everything is present. */
    CHECK_EQ(bmqp_frame_parse(out.data, out.len, &hdr, &payload), (ssize_t)out.len);

    bmqp_buf_free(&out);
}

static void test_frame_rejects_bad_end_sentinel(void)
{
    TEST_SECTION("bmqp_frame_parse rejects a corrupted/wrong end-of-frame byte");
    const uint8_t body[] = { 1 };
    bmqp_buf_t out;
    bmqp_buf_init(&out);
    bmqp_frame_write(&out, BMQP_FRAME_BODY, 0, body, sizeof(body));
    out.data[out.len - 1] = 0x00; /* corrupt the trailing 0xCE sentinel */

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;
    CHECK_EQ(bmqp_frame_parse(out.data, out.len, &hdr, &payload), -1);

    bmqp_buf_free(&out);
}

static void test_frame_rejects_oversized_payload_len(void)
{
    TEST_SECTION("bmqp_frame_parse rejects a payload_len beyond BMQP_MAX_FRAME_PAYLOAD");
    /* A hostile/corrupt header can claim an enormous length; the parser must
     * reject it from the header alone, without waiting for (or allocating)
     * that many bytes. */
    uint8_t hdr_bytes[BMQP_FRAME_HEADER_SIZE] = {0};
    hdr_bytes[0] = BMQP_FRAME_METHOD;
    uint32_t hostile_len = BMQP_MAX_FRAME_PAYLOAD + 1;
    hdr_bytes[3] = (uint8_t)(hostile_len >> 24);
    hdr_bytes[4] = (uint8_t)(hostile_len >> 16);
    hdr_bytes[5] = (uint8_t)(hostile_len >> 8);
    hdr_bytes[6] = (uint8_t)(hostile_len & 0xFF);

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;
    CHECK_EQ(bmqp_frame_parse(hdr_bytes, sizeof(hdr_bytes), &hdr, &payload), -1);
}

static void test_frame_consecutive_frames(void)
{
    TEST_SECTION("two frames back-to-back in one buffer parse independently in order");
    bmqp_buf_t out;
    bmqp_buf_init(&out);
    const uint8_t body1[] = { 'a', 'b' };
    const uint8_t body2[] = { 'c', 'd', 'e' };
    bmqp_frame_write(&out, BMQP_FRAME_BODY, 1, body1, sizeof(body1));
    bmqp_frame_write(&out, BMQP_FRAME_BODY, 2, body2, sizeof(body2));

    bmqp_frame_header_t hdr;
    const uint8_t *payload = NULL;
    ssize_t n1 = bmqp_frame_parse(out.data, out.len, &hdr, &payload);
    CHECK(n1 > 0);
    CHECK_EQ(hdr.channel, 1);
    CHECK_EQ(hdr.payload_len, sizeof(body1));
    CHECK(memcmp(payload, body1, sizeof(body1)) == 0);

    ssize_t n2 = bmqp_frame_parse(out.data + n1, out.len - (size_t)n1, &hdr, &payload);
    CHECK(n2 > 0);
    CHECK_EQ(hdr.channel, 2);
    CHECK_EQ(hdr.payload_len, sizeof(body2));
    CHECK(memcmp(payload, body2, sizeof(body2)) == 0);
    CHECK_EQ((size_t)(n1 + n2), out.len);

    bmqp_buf_free(&out);
}

int main(void)
{
    test_buf_put_primitives();
    test_buf_strings();
    test_buf_growth();
    test_reader_round_trip();
    test_reader_bounds_checking();
    test_frame_round_trip();
    test_frame_write_helper();
    test_frame_partial_buffering();
    test_frame_rejects_bad_end_sentinel();
    test_frame_rejects_oversized_payload_len();
    test_frame_consecutive_frames();
    return test_summary("test_frame");
}
