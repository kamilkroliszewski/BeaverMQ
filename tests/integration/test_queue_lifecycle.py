#!/usr/bin/env python3
"""
test_queue_lifecycle.py - AMQP queue lifecycle integration test (pika). Verifies
that queues are removed like a real broker, which BeaverMQ previously never did:
  - Queue.Delete removes a queue;
  - an exclusive queue vanishes when its declaring connection closes;
  - an auto-delete queue vanishes when its last consumer goes away;
  - an exclusive queue is locked (405) against other connections.

Uses the management API (/api/queues) to observe the registry.

Usage: test_queue_lifecycle.py <host> <amqp_port> <http_port> <user> <password>
"""
import base64
import json
import sys
import time
import urllib.request

import pika


def main() -> int:
    host, amqp, http = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    user, pw = sys.argv[4], sys.argv[5]

    def queues():
        r = urllib.request.Request(f"http://{host}:{http}/api/queues")
        tok = base64.b64encode(f"{user}:{pw}".encode()).decode()
        r.add_header("Authorization", "Basic " + tok)
        with urllib.request.urlopen(r, timeout=5) as resp:
            return {q["name"] for q in json.loads(resp.read().decode())}

    def conn():
        return pika.BlockingConnection(pika.ConnectionParameters(
            host=host, port=amqp, credentials=pika.PlainCredentials(user, pw),
            heartbeat=0, blocked_connection_timeout=5))

    # 1) Explicit Queue.Delete.
    c = conn(); ch = c.channel()
    ch.queue_declare(queue="qd_explicit", durable=False)
    assert "qd_explicit" in queues()
    ch.queue_delete(queue="qd_explicit")
    assert "qd_explicit" not in queues(), "Queue.Delete did not remove the queue"
    print("OK: Queue.Delete removes the queue")
    c.close()

    # 2) Exclusive queue removed on connection close.
    c = conn(); ch = c.channel()
    ch.queue_declare(queue="qd_excl", exclusive=True)
    assert "qd_excl" in queues()
    c.close()
    time.sleep(0.3)
    assert "qd_excl" not in queues(), "exclusive queue survived connection close"
    print("OK: exclusive queue auto-deleted on connection close")

    # 3) Auto-delete queue removed after last consumer cancels.
    c = conn(); ch = c.channel()
    ch.queue_declare(queue="qd_auto", auto_delete=True)
    tag = ch.basic_consume(queue="qd_auto",
                           on_message_callback=lambda *a: None, auto_ack=True)
    time.sleep(0.2)
    assert "qd_auto" in queues()
    ch.basic_cancel(tag)
    time.sleep(0.3)
    assert "qd_auto" not in queues(), "auto-delete queue survived last consumer"
    print("OK: auto-delete queue removed after last consumer cancels")
    c.close()

    # 4) Exclusive access is refused from another connection (405).
    c1 = conn(); ch1 = c1.channel(); ch1.queue_declare(queue="qd_lock", exclusive=True)
    c2 = conn(); ch2 = c2.channel()
    try:
        ch2.queue_declare(queue="qd_lock", exclusive=True)
        print("FAIL: cross-connection exclusive access was allowed")
        return 1
    except pika.exceptions.ChannelClosedByBroker as e:
        assert e.reply_code == 405, f"expected 405, got {e.reply_code}"
        print("OK: exclusive queue locked against other connections (405)")
    c1.close()
    try:
        c2.close()
    except Exception:
        pass

    print("PASS: queue lifecycle")
    return 0


if __name__ == "__main__":
    sys.exit(main())
