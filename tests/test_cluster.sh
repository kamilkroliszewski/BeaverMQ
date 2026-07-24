#!/usr/bin/env bash
#
# test_cluster.sh - 3-node Raft smoke test: bring up a local cluster, verify a
# leader is elected, kill the leader, and verify the surviving majority elects
# a NEW leader at a higher term (failover). Uses the broker's own leadership
# log line ("became LEADER (term N)") as the signal, so it needs no auth.
#
# Not part of `make test` (starts 3 processes); run it directly or in CI:
#   bash tests/test_cluster.sh
#
# Requires: bash, a built ./build/beavermq (run `make` first).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"

SECRET="test-cluster-secret"
MESH="127.0.0.1:16001,127.0.0.1:16002,127.0.0.1:16003"
BASE="$(mktemp -d)"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    sleep 0.3
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    done
    rm -rf "$BASE"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1"
    for i in 0 1 2; do
        echo "--- node $i log (tail) ---"; tail -15 "$BASE/n$i.log" 2>/dev/null
    done
    exit 1
}

[ -x "$BIN" ] || fail "binary not found at '$BIN' - run 'make' first"

start_node() {
    local i="$1"
    BEAVERMQ_CLUSTER=on BEAVERMQ_NODE_ID="$i" BEAVERMQ_CLUSTER_NODES="$MESH" \
        BEAVERMQ_CLUSTER_SECRET="$SECRET" \
        BEAVERMQ_AMQP_PORT="$((25990 + 2*i))" BEAVERMQ_HTTP_PORT="$((25991 + 2*i))" \
        BEAVERMQ_DATA_DIR="$BASE/n$i" BEAVERMQ_BIND=127.0.0.1 BEAVERMQ_LOG_LEVEL=info \
        "$BIN" >"$BASE/n$i.log" 2>&1 &
    PIDS[$i]=$!
    disown
}

# Return the term a node last became leader at, or "" if it never did.
leader_term() {
    grep -oE "became LEADER \(term [0-9]+\)" "$BASE/n$1.log" 2>/dev/null \
        | tail -1 | grep -oE "[0-9]+"
}

echo "== starting 3-node cluster =="
for i in 0 1 2; do mkdir -p "$BASE/n$i"; start_node "$i"; done

# Wait for a first leader to emerge.
LEADER=""; TERM1=""
for _ in $(seq 1 100); do
    for i in 0 1 2; do
        t="$(leader_term "$i")"
        if [ -n "$t" ]; then LEADER="$i"; TERM1="$t"; break; fi
    done
    [ -n "$LEADER" ] && break
    # A node dying during bring-up is a real failure, not something to wait out.
    for i in 0 1 2; do kill -0 "${PIDS[$i]}" 2>/dev/null || fail "node $i exited during startup"; done
    sleep 0.2
done
[ -n "$LEADER" ] || fail "no leader was elected within timeout"
echo "OK: node $LEADER became leader (term $TERM1)"

echo "== killing leader (node $LEADER) to force failover =="
kill -9 "${PIDS[$LEADER]}" 2>/dev/null
PIDS[$LEADER]=""

# A surviving node must become leader at a strictly higher term.
NEW_LEADER=""; TERM2=""
for _ in $(seq 1 150); do
    for i in 0 1 2; do
        [ "$i" = "$LEADER" ] && continue
        t="$(leader_term "$i")"
        if [ -n "$t" ] && [ "$t" -gt "$TERM1" ]; then NEW_LEADER="$i"; TERM2="$t"; break; fi
    done
    [ -n "$NEW_LEADER" ] && break
    sleep 0.2
done
[ -n "$NEW_LEADER" ] || fail "surviving majority did not elect a new leader after failover"
echo "OK: node $NEW_LEADER became new leader (term $TERM2 > $TERM1)"

echo
echo "PASS: cluster leader election + failover verified"
exit 0
