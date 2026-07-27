#!/usr/bin/env python3
"""
cluster_config_check.py - declare a durable queue WITH per-queue policy
(x-max-length, x-overflow=drop-head, x-dead-letter-exchange) on one cluster
node, then assert every node's /api/queues reports the SAME policy - i.e. the
full queue configuration replicated, not just the name/flags (audit P2).

Usage: cluster_config_check.py <user> <pass> <amqp_port> <http_port_0> [<http_port_1> ...]
The AMQP declare is done against <amqp_port> (any node forwards to the leader);
the HTTP checks run against every listed http port.
"""
import base64
import json
import sys
import time
import urllib.request

import pika

QNAME = "cfg_repl_q"


def get_queue(http_port, user, pw):
    r = urllib.request.Request(f"http://127.0.0.1:{http_port}/api/queues")
    tok = base64.b64encode(f"{user}:{pw}".encode()).decode()
    r.add_header("Authorization", "Basic " + tok)
    with urllib.request.urlopen(r, timeout=5) as resp:
        for q in json.loads(resp.read().decode()):
            if q["name"] == QNAME:
                return q
    return None


def main() -> int:
    user, pw = sys.argv[1], sys.argv[2]
    amqp = int(sys.argv[3])
    http_ports = [int(x) for x in sys.argv[4:]]

    # Retry the connect: user/permission commit is eventually consistent, so a
    # freshly-bootstrapped account may briefly 530/403 before its Raft entry
    # applies on the node we hit.
    conn = None
    params = pika.ConnectionParameters(
        host="127.0.0.1", port=amqp, credentials=pika.PlainCredentials(user, pw),
        heartbeat=0, blocked_connection_timeout=5)
    for _ in range(30):
        try:
            conn = pika.BlockingConnection(params)
            break
        except (pika.exceptions.ProbableAccessDeniedError,
                pika.exceptions.ProbableAuthenticationError,
                pika.exceptions.AMQPConnectionError):
            time.sleep(0.3)
    if conn is None:
        print("FAIL: could not connect (user/permission never became usable)")
        return 1
    ch = conn.channel()
    ch.queue_declare(queue=QNAME, durable=True, arguments={
        "x-max-length": 5,
        "x-overflow": "drop-head",
        "x-dead-letter-exchange": "dlx.target",
        "x-dead-letter-routing-key": "dead",
    })
    conn.close()
    print(f"OK: declared durable '{QNAME}' with policy on node :{amqp}")

    # Let the DECLARE commit + apply on every node.
    expected = {"max_length": 5, "overflow": "drop-head",
                "dead_letter_exchange": "dlx.target",
                "dead_letter_routing_key": "dead"}
    for port in http_ports:
        q = None
        for _ in range(50):
            q = get_queue(port, user, pw)
            if q and q.get("max_length") == 5:
                break
            time.sleep(0.1)
        if not q:
            print(f"FAIL: queue not present on node :{port}")
            return 1
        for k, v in expected.items():
            if q.get(k) != v:
                print(f"FAIL: node :{port} {k}={q.get(k)!r}, expected {v!r} "
                      f"(policy did not replicate)")
                return 1
        print(f"OK: node :{port} has matching policy "
              f"(max_length={q['max_length']}, overflow={q['overflow']}, "
              f"dlx={q['dead_letter_exchange']})")

    # P5: a replicated Queue.Delete must remove the queue from EVERY node (not
    # just the one it was issued on - otherwise it "resurrects" after failover).
    conn = pika.BlockingConnection(params)
    conn.channel().queue_delete(queue=QNAME)
    conn.close()
    print(f"OK: Queue.Delete '{QNAME}' issued on node :{amqp}")
    for port in http_ports:
        gone = False
        for _ in range(50):
            if get_queue(port, user, pw) is None:
                gone = True
                break
            time.sleep(0.1)
        if not gone:
            print(f"FAIL: queue still present on node :{port} after delete")
            return 1
        print(f"OK: node :{port} no longer has the queue")

    print("PASS: queue configuration + deletion replicated to every node")
    return 0


if __name__ == "__main__":
    sys.exit(main())
