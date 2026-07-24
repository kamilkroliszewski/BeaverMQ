/*
 * test_fuzz_frame.c - a self-contained fuzzer for the AMQP frame envelope
 * parser and the bounds-checked field readers (frame.c). The parser and
 * readers are the code most exposed to hostile input, so this feeds them a
 * large number of random and semi-structured byte streams and asserts the
 * safety invariants hold on every one - most valuably when built under ASan
 * (see the Makefile's `test-asan` target), which turns any out-of-bounds read
 * into a hard failure.
 *
 * Two entry points from one body:
 *   - default: a deterministic PRNG drives many iterations, wired into the
 *     normal `make test` suite (reproducible via a fixed seed).
 *   - FUZZ_LIBFUZZER: exposes LLVMFuzzerTestOneInput for coverage-guided
 *     fuzzing (`make fuzz`), which explores far deeper than the fixed corpus.
 */
#include "frame.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef FUZZ_LIBFUZZER
#include "test_util.h"
#endif

/* Exercise the parser + readers on one input. Returns 1 if every invariant
 * held, 0 if one was violated (checked by the deterministic driver; libFuzzer
 * relies on ASan aborting instead). MUST NOT read out of bounds on any input. */
static int fuzz_one(const uint8_t *data, size_t len)
{
    int ok = 1;
    size_t off = 0;

    /* Walk frames exactly like protocol.c's read loop: parse, consume, repeat. */
    for (int guard = 0; guard < 4096 && off <= len; guard++) {
        bmqp_frame_header_t hdr;
        const uint8_t *payload = NULL;
        ssize_t n = bmqp_frame_parse(data + off, len - off, &hdr, &payload);

        if (n == 0)
            break;               /* need more bytes: stop (real loop waits) */
        if (n < 0)
            break;               /* protocol error: real loop closes the conn */

        /* A successful parse must consume at least the envelope overhead and
         * never more than the bytes we actually gave it. */
        if ((size_t)n > len - off) { ok = 0; break; }
        if ((size_t)n < BMQP_FRAME_OVERHEAD) { ok = 0; break; }
        /* The payload must lie within the consumed region. */
        if (hdr.payload_len > BMQP_MAX_FRAME_PAYLOAD) { ok = 0; break; }
        if (payload && (size_t)(payload - (data + off)) + hdr.payload_len > (size_t)n) {
            ok = 0; break;
        }

        /* Drive the bounds-checked readers over the payload as the protocol
         * layer would for a method frame: two u16s then a mix of fields. The
         * reader must flag ->error rather than read past the end - never crash. */
        bmqp_reader_t r;
        bmqp_reader_init(&r, payload, hdr.payload_len);
        (void)bmqp_read_u16(&r);
        (void)bmqp_read_u16(&r);
        for (int f = 0; f < 8 && !r.error; f++) {
            size_t sl = 0;
            switch (f & 3) {
            case 0: (void)bmqp_read_u32(&r); break;
            case 1: (void)bmqp_read_u64(&r); break;
            case 2: (void)bmqp_read_shortstr(&r, &sl); break;
            case 3: (void)bmqp_read_longstr(&r, &sl); break;
            }
        }
        /* Remaining must never exceed the buffer we handed the reader. */
        if (bmqp_reader_remaining(&r) > hdr.payload_len) { ok = 0; break; }

        off += (size_t)n;
    }
    return ok;
}

#ifdef FUZZ_LIBFUZZER
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_one(data, size);
    return 0;
}
#else

/* Small, fast, deterministic PRNG (xorshift64) so the run is reproducible. */
static uint64_t g_rng = 0x9e3779b97f4a7c15ull;
static uint32_t rnd(void)
{
    g_rng ^= g_rng << 13; g_rng ^= g_rng >> 7; g_rng ^= g_rng << 17;
    return (uint32_t)(g_rng >> 32);
}

/* Build an input biased toward *nearly*-valid frames: real headers with random
 * type/channel/len and a payload, sometimes a correct end sentinel, sometimes
 * truncated - the cases most likely to trip an off-by-one. */
static size_t make_structured(uint8_t *buf, size_t cap)
{
    size_t o = 0;
    int frames = 1 + (int)(rnd() % 4);
    for (int i = 0; i < frames && o + 8 < cap; i++) {
        uint32_t plen = rnd() % 40;
        if (rnd() % 16 == 0) plen = rnd();          /* occasional absurd length */
        buf[o++] = (uint8_t)(rnd() % 12);           /* type (some invalid)      */
        buf[o++] = (uint8_t)rnd(); buf[o++] = (uint8_t)rnd();      /* channel   */
        buf[o++] = (uint8_t)(plen >> 24); buf[o++] = (uint8_t)(plen >> 16);
        buf[o++] = (uint8_t)(plen >> 8);  buf[o++] = (uint8_t)plen;            /* len */
        uint32_t body = plen > 40 ? (rnd() % 40) : plen;          /* actual body */
        for (uint32_t b = 0; b < body && o < cap; b++) buf[o++] = (uint8_t)rnd();
        if (o < cap) buf[o++] = (rnd() % 4 == 0) ? (uint8_t)rnd() /* bad end */
                                                 : BMQP_FRAME_END;
    }
    return o;
}

int main(void)
{
    TEST_SECTION("frame parser / readers survive random + structured input (ASan-checked)");

    uint8_t buf[512];
    int violations = 0;

    /* 1) Pure random noise of every length up to the buffer. */
    for (int iter = 0; iter < 200000; iter++) {
        size_t len = rnd() % sizeof(buf);
        for (size_t i = 0; i < len; i++) buf[i] = (uint8_t)rnd();
        if (!fuzz_one(buf, len)) violations++;
    }
    /* 2) Semi-structured near-valid frames (deeper into the happy path). */
    for (int iter = 0; iter < 200000; iter++) {
        size_t len = make_structured(buf, sizeof(buf));
        if (!fuzz_one(buf, len)) violations++;
    }
    /* 3) Degenerate edge cases. */
    CHECK(fuzz_one(NULL, 0) == 1);
    CHECK(fuzz_one((const uint8_t *)"", 0) == 1);
    uint8_t ce[64]; memset(ce, BMQP_FRAME_END, sizeof ce);
    CHECK(fuzz_one(ce, sizeof ce) == 1);

    /* No input may violate the parser/reader safety invariants. */
    CHECK_EQ(violations, 0);

    return test_summary("test_fuzz_frame");
}
#endif
