#!/usr/bin/env python3
"""
test_flow_control.py - regressions for two bugs that made a real workload
(rabbitmq-perf-test) fail or stall:

1. passive declare. A passive Exchange/Queue.Declare is an existence probe: the
   type/flags/arguments are ignored (clients send an EMPTY exchange type), it
   must never create anything, and it answers 404 when the object is missing.
   Rejecting the empty type used to fail the call with COMMAND_INVALID, which
   killed perf-test on startup.

2. a full queue must not wedge its connection. A queue at its limit used to trip
   a broker-wide flow alarm that paused the whole TCP connection - including the
   consumer ACKs travelling on it. The prefetch window then never freed, so
   deliveries stopped, the queue never drained, and the alarm never cleared:
   the connection was stuck forever. The queue limit alone bounds memory, so
   reads must keep flowing.

Usage: test_flow_control.py <host> <amqp_port> <user> <password>
"""
import sys
import time

import pika


def conn(host, port, user, pw):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=host, port=port, credentials=pika.PlainCredentials(user, pw),
        heartbeat=0, blocked_connection_timeout=5))


def test_passive(host, port, user, pw):
    c = conn(host, port, user, pw)
    ch = c.channel()
    # Missing exchange -> 404 (NOT 503 "unknown exchange type").
    try:
        ch.exchange_declare(exchange="no.such.exchange", passive=True)
        raise AssertionError("passive declare of a missing exchange should fail")
    except pika.exceptions.ChannelClosedByBroker as e:
        assert e.reply_code == 404, f"expected 404, got {e.reply_code} {e.reply_text}"
    # Existing exchange, and the default exchange, are accepted.
    ch = c.channel()
    ch.exchange_declare(exchange="amq.direct", passive=True)
    ch.exchange_declare(exchange="", passive=True)
    print("OK: passive exchange declare (empty type) probes instead of failing")

    # Missing queue -> 404; existing queue -> Ok with counts; never creates.
    try:
        ch.queue_declare(queue="no.such.queue", passive=True)
        raise AssertionError("passive declare of a missing queue should fail")
    except pika.exceptions.ChannelClosedByBroker as e:
        assert e.reply_code == 404, f"expected 404, got {e.reply_code}"
    ch = c.channel()
    ch.queue_declare(queue="fc_probe", durable=False)
    ch.basic_publish(exchange="", routing_key="fc_probe", body=b"x")
    r = ch.queue_declare(queue="fc_probe", passive=True)
    assert r.method.message_count >= 1, "passive declare should report the depth"
    ch.queue_delete(queue="fc_probe")
    print("OK: passive queue declare probes (and never creates)")
    c.close()


def test_full_queue_does_not_wedge(host, port, user, pw):
    """Publish far past a queue's limit while consuming on the SAME connection."""
    c = conn(host, port, user, pw)
    pub, con = c.channel(), c.channel()
    q = "fc_wedge"
    pub.queue_declare(queue=q, durable=False, arguments={"x-max-length": 2000})
    con.basic_qos(prefetch_count=10)     # small window: needs ACKs to keep flowing

    got = [0]
    def cb(ch, method, props, body):
        got[0] += 1
        ch.basic_ack(method.delivery_tag)   # ack travels on the SAME connection
    con.basic_consume(queue=q, on_message_callback=cb)

    body = b"x" * 850
    sent = 0
    while sent < 20000:                  # well past the 2000-message cap
        pub.basic_publish(exchange="", routing_key=q, body=body)
        sent += 1
    # Drain: with the connection readable, ACKs flow and deliveries continue.
    deadline = time.time() + 15
    while got[0] < 2000 and time.time() < deadline:
        c.process_data_events(time_limit=0.5)
    assert got[0] >= 2000, (
        f"connection wedged: only {got[0]} messages delivered after publishing "
        f"{sent} past the limit (a full queue must not pause the socket)")
    print(f"OK: full queue kept delivering (sent={sent}, received={got[0]})")
    pub.queue_delete(queue=q)
    c.close()


def main() -> int:
    host, port, user, pw = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    test_passive(host, port, user, pw)
    test_full_queue_does_not_wedge(host, port, user, pw)
    print("PASS: flow control + passive declare")
    return 0


if __name__ == "__main__":
    sys.exit(main())
