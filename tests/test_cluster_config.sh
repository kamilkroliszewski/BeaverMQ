#!/usr/bin/env bash
#
# test_cluster_config.sh - verify the FULL queue configuration replicates across
# a cluster (audit P2). Brings up 3 nodes, declares a durable queue with
# x-max-length / x-overflow=drop-head / x-dead-letter-exchange on one node, and
# checks every node's /api/queues reports the same policy - not just the name.
#
# Not part of `make test`; run via `make cluster-config-test`. Needs the `pika`
# client (installed into a venv, or the test SKIPs) + a built ./build/beavermq.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"
[ -x "$BIN" ] || { echo "FAIL: binary not found at '$BIN' - run 'make' first"; exit 1; }

SECRET="cfg-repl-secret"
MESH="127.0.0.1:16010,127.0.0.1:16011,127.0.0.1:16012"
USER=cfguser
PASS=cfgpass
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
fail() { echo "FAIL: $1"; for i in 0 1 2; do echo "--- node $i ---"; tail -8 "$BASE/n$i.log" 2>/dev/null; done; exit 1; }

find_pika_python() {
    if python3 -c "import pika" >/dev/null 2>&1; then echo "python3"; return; fi
    local venv="$REPO_ROOT/build/itest-venv"
    if [ ! -x "$venv/bin/python" ]; then
        python3 -m venv "$venv" >/dev/null 2>&1 || return 0
        "$venv/bin/pip" install --quiet pika >/dev/null 2>&1 || return 0
    fi
    "$venv/bin/python" -c "import pika" >/dev/null 2>&1 && echo "$venv/bin/python"
}

amqp_port() { echo $((26000 + 2*$1)); }
http_port() { echo $((26001 + 2*$1)); }

start_node() {
    local i="$1"; mkdir -p "$BASE/n$i"
    BEAVERMQ_CLUSTER=on BEAVERMQ_NODE_ID="$i" BEAVERMQ_CLUSTER_NODES="$MESH" \
        BEAVERMQ_CLUSTER_SECRET="$SECRET" \
        BEAVERMQ_AMQP_PORT="$(amqp_port "$i")" BEAVERMQ_HTTP_PORT="$(http_port "$i")" \
        BEAVERMQ_DATA_DIR="$BASE/n$i" BEAVERMQ_BIND=127.0.0.1 BEAVERMQ_LOG_LEVEL=info \
        "$BIN" >"$BASE/n$i.log" 2>&1 &
    PIDS[$i]=$!
    disown
}

PYBIN="$(find_pika_python)"
if [ -z "$PYBIN" ]; then
    echo "SKIP: 'pika' unavailable (offline?); cluster config-replication test not run."
    exit 0
fi

echo "== starting 3-node cluster =="
for i in 0 1 2; do start_node "$i"; done

# Wait for a leader (any node logs it).
for _ in $(seq 1 100); do
    grep -q "became LEADER" "$BASE"/n*.log 2>/dev/null && break
    for i in 0 1 2; do kill -0 "${PIDS[$i]}" 2>/dev/null || fail "node $i exited early"; done
    sleep 0.2
done
grep -q "became LEADER" "$BASE"/n*.log 2>/dev/null || fail "no leader elected"

# Bootstrap a user on node 0 (replicates through Raft); retry until it lands.
ok=0
for _ in $(seq 1 50); do
    if BEAVERMQ_DATA_DIR="$BASE/n0" "$BIN" add-user "$USER" "$PASS" \
        -H 127.0.0.1 -p "$(http_port 0)" >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.2
done
[ "$ok" = 1 ] || fail "could not bootstrap a user"

# add-user's permission grant races the (not-yet-committed) user in cluster mode
# (audit P12), so grant explicitly, retrying until it lands (200 = the user is
# now committed AND the permission is accepted).
gok=0
for _ in $(seq 1 60); do
    hc=$(curl -s -o /dev/null -w '%{http_code}' -u "$USER:$PASS" \
        -X POST -H 'Content-Type: application/json' \
        -d "{\"user\":\"$USER\",\"vhost\":\"/\",\"configure\":\".*\",\"write\":\".*\",\"read\":\".*\"}" \
        "http://127.0.0.1:$(http_port 0)/api/permissions" 2>/dev/null)
    [ "$hc" = "200" ] && { gok=1; break; }
    sleep 0.3
done
[ "$gok" = 1 ] || fail "could not grant permissions to '$USER'"
sleep 1  # let the SET_PERM entry commit + apply on every node

echo "== declaring durable queue with policy + checking every node =="
"$PYBIN" "$SCRIPT_DIR/integration/cluster_config_check.py" "$USER" "$PASS" \
    "$(amqp_port 0)" "$(http_port 0)" "$(http_port 1)" "$(http_port 2)" || fail "config did not replicate"

echo
echo "PASS: cluster queue-configuration replication"
exit 0
