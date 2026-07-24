/*
 * test_routing.c - unit tests for message.c (refcounting), queue.c (FIFO
 * ring buffer + limits), and exchange.c (direct/fanout/topic routing), plus
 * a multi-threaded producer/consumer stress test exercising the queue's
 * actual thread-safety guarantee under concurrent load.
 */
#include "message.h"
#include "queue.h"
#include "exchange.h"
#include "test_util.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* ---- message refcounting -------------------------------------------------- */

static void test_message_refcounting(void)
{
    TEST_SECTION("message_new/ref/unref refcounting and deep-copy semantics");
    char body[] = "original body";
    beaver_message_t *m = message_new("ex", "rk", body, sizeof(body));
    CHECK(m != NULL);
    CHECK_EQ(message_refcount(m), 1);

    /* Mutating the caller's buffer after creation must not affect the
     * message: message_new() must COPY, never alias. */
    memcpy(body, "MUTATED BODY!!", sizeof(body) - 1);
    CHECK(memcmp(m->body, "original body", sizeof(body)) == 0);
    CHECK_STR_EQ(m->exchange, "ex");
    CHECK_STR_EQ(m->routing_key, "rk");
    CHECK_EQ(m->body_len, sizeof(body));

    beaver_message_t *m2 = message_ref(m);
    CHECK(m2 == m); /* same object, not a copy */
    CHECK_EQ(message_refcount(m), 2);

    message_unref(m2);
    CHECK_EQ(message_refcount(m), 1);

    message_unref(m); /* drops to 0: frees; do not touch m again */
}

static void test_message_new_full_props(void)
{
    TEST_SECTION("message_new_full captures properties verbatim");
    const uint8_t props[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    beaver_message_t *m = message_new_full("e", "k", "b", 1, props, sizeof(props));
    CHECK(m != NULL);
    CHECK_EQ(m->props_len, sizeof(props));
    CHECK(m->props && memcmp(m->props, props, sizeof(props)) == 0);
    message_unref(m);

    beaver_message_t *m_noprops = message_new_full("e", "k", "b", 1, NULL, 0);
    CHECK(m_noprops != NULL);
    CHECK_EQ(m_noprops->props_len, 0);
    message_unref(m_noprops);
}

/* ---- queue: FIFO order, counters, purge, limits --------------------------- */

static void test_queue_fifo_order(void)
{
    TEST_SECTION("queue_enqueue/dequeue preserves FIFO order, including across a regrow");
    beaver_queue_t *q = queue_new("q1", 0);
    CHECK(q != NULL);

    /* Enqueue more than the ring buffer's initial capacity so at least one
     * internal regrow happens; order must survive it. */
    enum { N = 200 };
    for (int i = 0; i < N; i++) {
        char rk[16];
        snprintf(rk, sizeof(rk), "%d", i);
        beaver_message_t *m = message_new("", rk, &i, sizeof(i));
        CHECK_EQ(queue_enqueue(q, m), 0);
        message_unref(m); /* queue holds its own ref */
    }
    CHECK_EQ(queue_depth(q), N);
    CHECK_EQ(queue_total_enqueued(q), N);

    int ok = 1;
    for (int i = 0; i < N; i++) {
        beaver_message_t *m = queue_dequeue(q);
        if (!m) { ok = 0; break; }
        int expect;
        memcpy(&expect, m->body, sizeof(expect));
        if (expect != i) ok = 0;
        message_unref(m);
    }
    CHECK(ok);
    CHECK_EQ(queue_depth(q), 0);
    CHECK_EQ(queue_total_dequeued(q), N);
    CHECK(queue_dequeue(q) == NULL); /* empty: must not fabricate a message */

    queue_unref(q);
}

static void test_queue_purge(void)
{
    TEST_SECTION("queue_purge drops everything and reports how many");
    beaver_queue_t *q = queue_new("q2", 0);
    for (int i = 0; i < 10; i++) {
        beaver_message_t *m = message_new("", "", "x", 1);
        queue_enqueue(q, m);
        message_unref(m);
    }
    CHECK_EQ(queue_depth(q), 10);
    CHECK_EQ(queue_purge(q), 10);
    CHECK_EQ(queue_depth(q), 0);
    CHECK_EQ(queue_purge(q), 0); /* purging an already-empty queue: 0, not an error */
    queue_unref(q);
}

static void test_queue_default_limits(void)
{
    TEST_SECTION("queue_set_default_limits enforces max_length / max_bytes as QUEUE_FULL");
    queue_set_default_limits(2, 0); /* at most 2 messages, any size */
    beaver_queue_t *q = queue_new("q3", 0);
    beaver_message_t *m1 = message_new("", "", "a", 1);
    beaver_message_t *m2 = message_new("", "", "b", 1);
    beaver_message_t *m3 = message_new("", "", "c", 1);
    CHECK_EQ(queue_enqueue(q, m1), 0);
    CHECK_EQ(queue_enqueue(q, m2), 0);
    CHECK_EQ(queue_enqueue(q, m3), QUEUE_FULL); /* 3rd exceeds max_length=2 */
    CHECK_EQ(queue_depth(q), 2);
    message_unref(m1); message_unref(m2); message_unref(m3);
    queue_unref(q);

    /* Reset to unlimited: every other test in this binary (esp. the 100k
     * stress test below) relies on the default (no cap). */
    queue_set_default_limits(0, 0);
}

/* Regression: queue_drain_consumed() must decrement total_bytes so a replica
 * that drains consumed messages does not keep counting their bytes against
 * queue_max_bytes and spuriously reject later enqueues (audit 3.1). */
static void test_queue_drain_frees_bytes(void)
{
    TEST_SECTION("queue_drain_consumed frees total_bytes so max_bytes recovers");
    queue_set_default_limits(0, 4); /* at most 4 bytes queued at once */
    beaver_queue_t *q = queue_new("qdrain", 0);

    beaver_message_t *m1 = message_new("", "", "abcd", 4); /* 4 bytes: exactly full */
    m1->cluster_id = 1;
    CHECK_EQ(queue_enqueue(q, m1), 0);
    CHECK_EQ(queue_depth(q), 1);

    /* A second message must be rejected while the first still occupies the cap. */
    beaver_message_t *reject = message_new("", "", "x", 1);
    reject->cluster_id = 2;
    CHECK_EQ(queue_enqueue(q, reject), QUEUE_FULL);
    message_unref(reject);

    /* Drain everything consumed up to cluster_id 1: depth AND bytes must clear. */
    CHECK_EQ(queue_drain_consumed(q, 1), 1);
    CHECK_EQ(queue_depth(q), 0);

    /* If total_bytes was correctly decremented, a fresh 4-byte enqueue fits;
     * the old bug left 4 "phantom" bytes charged and this returned QUEUE_FULL. */
    beaver_message_t *m2 = message_new("", "", "wxyz", 4);
    m2->cluster_id = 3;
    CHECK_EQ(queue_enqueue(q, m2), 0);
    CHECK_EQ(queue_depth(q), 1);

    message_unref(m1); message_unref(m2);
    queue_unref(q);
    queue_set_default_limits(0, 0);
}

/* Regression: an internal requeue (nack/reject/disconnect) must never be
 * dropped just because the queue hit its publisher-facing limit - otherwise a
 * full queue silently loses in-flight messages on requeue (audit 2.2). */
static void test_queue_requeue_bypasses_limits(void)
{
    TEST_SECTION("queue_requeue_internal ignores length/byte caps (lossless)");
    queue_set_default_limits(1, 0); /* at most 1 message via the publisher path */
    beaver_queue_t *q = queue_new("qrequeue", 0);

    beaver_message_t *m1 = message_new("", "", "a", 1);
    beaver_message_t *m2 = message_new("", "", "b", 1);
    CHECK_EQ(queue_enqueue(q, m1), 0);
    CHECK_EQ(queue_enqueue(q, m2), QUEUE_FULL); /* publisher path is capped at 1 */

    /* The internal requeue path must still accept m2, growing past the cap. */
    CHECK_EQ(queue_requeue_internal(q, m2), 0);
    CHECK_EQ(queue_depth(q), 2);

    message_unref(m1); message_unref(m2);
    queue_unref(q);
    queue_set_default_limits(0, 0);
}

/* ---- exchange: direct / fanout / topic ------------------------------------ */

static void free_route_result(beaver_queue_t **arr, size_t n)
{
    for (size_t i = 0; i < n; i++)
        queue_unref(arr[i]);
    free(arr);
}

static int result_contains(beaver_queue_t **arr, size_t n, beaver_queue_t *q)
{
    for (size_t i = 0; i < n; i++)
        if (arr[i] == q)
            return 1;
    return 0;
}

static void test_exchange_type_name_roundtrip(void)
{
    TEST_SECTION("exchange_type_from_name / exchange_type_name");
    exchange_type_t t;
    CHECK_EQ(exchange_type_from_name("direct", &t), 0);
    CHECK_EQ(t, EXCHANGE_DIRECT);
    CHECK_EQ(exchange_type_from_name("fanout", &t), 0);
    CHECK_EQ(t, EXCHANGE_FANOUT);
    CHECK_EQ(exchange_type_from_name("topic", &t), 0);
    CHECK_EQ(t, EXCHANGE_TOPIC);

    t = EXCHANGE_TOPIC; /* pre-set to something else to prove it gets reset */
    CHECK_EQ(exchange_type_from_name("headers-does-not-exist", &t), -1);
    CHECK_EQ(t, EXCHANGE_DIRECT); /* documented fallback on unknown name */

    CHECK_STR_EQ(exchange_type_name(EXCHANGE_DIRECT), "direct");
    CHECK_STR_EQ(exchange_type_name(EXCHANGE_FANOUT), "fanout");
    CHECK_STR_EQ(exchange_type_name(EXCHANGE_TOPIC), "topic");
}

static void test_exchange_direct_routing(void)
{
    TEST_SECTION("direct exchange: only the exact routing-key match is returned");
    beaver_exchange_t *ex = exchange_new("d", EXCHANGE_DIRECT, 0);
    beaver_queue_t *qa = queue_new("qa", 0);
    beaver_queue_t *qb = queue_new("qb", 0);
    CHECK_EQ(exchange_bind(ex, "orders", qa), 0);
    CHECK_EQ(exchange_bind(ex, "invoices", qb), 0);

    beaver_queue_t **out = NULL;
    size_t n = exchange_route(ex, "orders", &out, NULL);
    CHECK_EQ(n, 1);
    CHECK(out && out[0] == qa);
    free_route_result(out, n);

    out = NULL;
    n = exchange_route(ex, "nonexistent-key", &out, NULL);
    CHECK_EQ(n, 0);
    CHECK(out == NULL);

    queue_unref(qa);
    queue_unref(qb);
    exchange_free(ex);
}

static void test_exchange_fanout_routing_and_dedup(void)
{
    TEST_SECTION("fanout exchange: every bound queue matches regardless of key, deduped");
    beaver_exchange_t *ex = exchange_new("f", EXCHANGE_FANOUT, 0);
    beaver_queue_t *qa = queue_new("qa", 0);
    beaver_queue_t *qb = queue_new("qb", 0);
    beaver_queue_t *qc = queue_new("qc", 0);
    exchange_bind(ex, "any-key-1", qa);
    exchange_bind(ex, "any-key-2", qb);
    /* Bind qc under TWO different keys: fanout ignores the key entirely, so
     * a route() must still report qc only ONCE (documented dedup), not
     * twice just because it has two bindings. */
    exchange_bind(ex, "k1", qc);
    exchange_bind(ex, "k2", qc);

    beaver_queue_t **out = NULL;
    size_t n = exchange_route(ex, "totally-irrelevant", &out, NULL);
    CHECK_EQ(n, 3);
    CHECK(result_contains(out, n, qa));
    CHECK(result_contains(out, n, qb));
    CHECK(result_contains(out, n, qc));
    free_route_result(out, n);

    queue_unref(qa);
    queue_unref(qb);
    queue_unref(qc);
    exchange_free(ex);
}

static void test_exchange_topic_match_patterns(void)
{
    TEST_SECTION("exchange_topic_match: '*' = exactly one word, '#' = zero or more words");
    CHECK(exchange_topic_match("orders.*.created", "orders.eu.created"));
    CHECK(!exchange_topic_match("orders.*.created", "orders.eu.west.created")); /* * is ONE word */
    CHECK(exchange_topic_match("orders.#", "orders.eu.created"));
    CHECK(exchange_topic_match("orders.#", "orders")); /* # can match zero words */
    CHECK(exchange_topic_match("#", "anything.at.all"));
    CHECK(!exchange_topic_match("orders.eu", "orders.us"));
    CHECK(exchange_topic_match("orders.eu", "orders.eu"));
    CHECK(!exchange_topic_match("orders.*", "orders")); /* * requires a word to be present */
}

static void test_exchange_topic_routing(void)
{
    TEST_SECTION("topic exchange routes through exchange_route() using the same patterns");
    beaver_exchange_t *ex = exchange_new("t", EXCHANGE_TOPIC, 0);
    beaver_queue_t *q_eu = queue_new("q_eu", 0);
    beaver_queue_t *q_all = queue_new("q_all", 0);
    exchange_bind(ex, "orders.eu.*", q_eu);
    exchange_bind(ex, "orders.#", q_all);

    beaver_queue_t **out = NULL;
    size_t n = exchange_route(ex, "orders.eu.created", &out, NULL);
    CHECK_EQ(n, 2); /* both patterns match */
    free_route_result(out, n);

    out = NULL;
    n = exchange_route(ex, "orders.us.created", &out, NULL);
    CHECK_EQ(n, 1); /* only the catch-all matches */
    CHECK(out && out[0] == q_all);
    free_route_result(out, n);

    queue_unref(q_eu);
    queue_unref(q_all);
    exchange_free(ex);
}

/* ---- 100k-message, 4 producer x 4 consumer thread stress test ------------ */

#define STRESS_PRODUCERS   4
#define STRESS_CONSUMERS   4
#define STRESS_PER_PRODUCER 25000
#define STRESS_TOTAL       (STRESS_PRODUCERS * STRESS_PER_PRODUCER) /* 100000 */

static beaver_queue_t   *g_stress_q;
static _Atomic uint64_t  g_next_id;
static _Atomic uint64_t  g_consumed_count;
static _Atomic uint64_t  g_checksum;

static void *stress_producer_main(void *unused)
{
    (void)unused;
    for (int i = 0; i < STRESS_PER_PRODUCER; i++) {
        uint64_t id = atomic_fetch_add(&g_next_id, 1);
        beaver_message_t *m = message_new("", "", &id, sizeof(id));
        if (!m)
            continue; /* OOM in a stress test: skip, don't crash the test */
        /* Retry on QUEUE_FULL/-1: with default (unlimited) settings this
         * should not happen, but never busy-loop forever on a genuine error. */
        while (queue_enqueue(g_stress_q, m) != 0)
            ; /* no-op retry */
        message_unref(m);
    }
    return NULL;
}

static void *stress_consumer_main(void *unused)
{
    (void)unused;
    while (atomic_load(&g_consumed_count) < STRESS_TOTAL) {
        beaver_message_t *m = queue_dequeue(g_stress_q);
        if (!m)
            continue; /* momentarily empty: producers are still catching up */
        uint64_t id;
        memcpy(&id, m->body, sizeof(id));
        message_unref(m);
        atomic_fetch_add(&g_checksum, id);
        atomic_fetch_add(&g_consumed_count, 1);
    }
    return NULL;
}

static void test_producer_consumer_stress(void)
{
    TEST_SECTION("100k messages, 4 producer + 4 consumer threads, one shared queue");
    g_stress_q = queue_new("stress", 0);
    atomic_init(&g_next_id, 0);
    atomic_init(&g_consumed_count, 0);
    atomic_init(&g_checksum, 0);

    pthread_t producers[STRESS_PRODUCERS], consumers[STRESS_CONSUMERS];
    for (int i = 0; i < STRESS_CONSUMERS; i++)
        CHECK(pthread_create(&consumers[i], NULL, stress_consumer_main, NULL) == 0);
    for (int i = 0; i < STRESS_PRODUCERS; i++)
        CHECK(pthread_create(&producers[i], NULL, stress_producer_main, NULL) == 0);

    for (int i = 0; i < STRESS_PRODUCERS; i++)
        pthread_join(producers[i], NULL);
    for (int i = 0; i < STRESS_CONSUMERS; i++)
        pthread_join(consumers[i], NULL);

    CHECK_EQ(atomic_load(&g_consumed_count), STRESS_TOTAL);
    CHECK_EQ(queue_total_enqueued(g_stress_q), STRESS_TOTAL);
    CHECK_EQ(queue_total_dequeued(g_stress_q), STRESS_TOTAL);
    CHECK_EQ(queue_depth(g_stress_q), 0);
    /* Every id in [0, STRESS_TOTAL) delivered exactly once (not merely
     * "the right COUNT of messages" - a dropped id masked by a duplicate
     * elsewhere would still pass a pure count check but fails this sum). */
    uint64_t expected_sum = (uint64_t)STRESS_TOTAL * (STRESS_TOTAL - 1) / 2;
    CHECK_EQ(atomic_load(&g_checksum), expected_sum);

    queue_unref(g_stress_q);
}

int main(void)
{
    test_message_refcounting();
    test_message_new_full_props();
    test_queue_fifo_order();
    test_queue_purge();
    test_queue_default_limits();
    test_queue_drain_frees_bytes();
    test_queue_requeue_bypasses_limits();
    test_exchange_type_name_roundtrip();
    test_exchange_direct_routing();
    test_exchange_fanout_routing_and_dedup();
    test_exchange_topic_match_patterns();
    test_exchange_topic_routing();
    test_producer_consumer_stress();
    return test_summary("test_routing");
}
