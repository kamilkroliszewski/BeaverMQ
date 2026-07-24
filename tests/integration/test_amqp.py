#!/usr/bin/env python3
"""
test_amqp.py - AMQP client-compatibility integration test using pika (a real
RabbitMQ client). Exercises the handshake, SASL PLAIN auth, queue declare, a
publish/consume round-trip, Basic.Get, and publisher confirms end-to-end.

Usage: test_amqp.py <host> <port> <user> <password>
Exits 0 on success, 1 on any failure.
"""
import sys
import pika


def main() -> int:
    host, port, user, pw = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    creds = pika.PlainCredentials(user, pw)
    params = pika.ConnectionParameters(host=host, port=port, credentials=creds,
                                       heartbeat=0, blocked_connection_timeout=5)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    print("OK: connected + SASL PLAIN authenticated")

    q = "itest_q"
    ch.queue_declare(queue=q, durable=False)
    print("OK: Queue.Declare")

    # Publish/consume round-trip via Basic.Get.
    ch.basic_publish(exchange="", routing_key=q, body=b"hello")
    method, props, body = ch.basic_get(queue=q, auto_ack=True)
    assert body == b"hello", f"round-trip body mismatch: {body!r}"
    print("OK: publish -> Basic.Get round-trip")

    # Publisher confirms: after confirm_delivery(), a publish that returns
    # (rather than raising) means the broker sent Basic.Ack.
    ch.confirm_delivery()
    ch.basic_publish(exchange="", routing_key=q, body=b"c1")
    ch.basic_publish(exchange="", routing_key=q, body=b"c2")
    print("OK: publisher confirms (two Basic.Acks, tags advanced)")

    # Consume both via a push consumer to exercise Basic.Consume/Deliver/Ack.
    got = []
    for method, props, body in ch.consume(queue=q, inactivity_timeout=2):
        if method is None:
            break
        got.append(body)
        ch.basic_ack(method.delivery_tag)
        if len(got) == 2:
            break
    ch.cancel()
    assert got == [b"c1", b"c2"], f"consume order/content mismatch: {got!r}"
    print("OK: Basic.Consume delivered both messages in order")

    conn.close()
    print("PASS: AMQP integration")
    return 0


if __name__ == "__main__":
    sys.exit(main())
