#!/usr/bin/env bash
#
# test_fault_storage.sh - storage-layer fault injection for the WAL/snapshot
# fail-stop (audit P0). Using the LD_PRELOAD shim tests/faultlib.c, it fails
# fsync / write / rename ONLY on the cluster persistence files and asserts the
# node reacts by marking itself storage_failed ("storage is no longer durable")
# instead of silently continuing on storage it can no longer trust. A control
# run with no faults proves the signal only appears when a fault is injected.
#
# Not part of `make test` (starts a broker + preloads a shim); run directly or
# via `make fault-test`:
#   bash tests/test_fault_storage.sh
#
# Requires: bash, cc, a built ./build/beavermq (run `make` first).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"
SHIM="$REPO_ROOT/build/faultlib.so"

[ -x "$BIN" ] || { echo "FAIL: binary not found at '$BIN' - run 'make' first"; exit 1; }

echo "== building fault-injection shim =="
cc -shared -fPIC -D_GNU_SOURCE "$SCRIPT_DIR/faultlib.c" -ldl -o "$SHIM" \
    || { echo "FAIL: could not build faultlib.so"; exit 1; }

PORT_BASE=25960
RUN_IDX=0
FAILED=0

# run_case <name> <expect: healthy|failstop> [ENV=VAL ...]
run_case() {
    local name="$1" expect="$2"; shift 2
    local dd; dd="$(mktemp -d)"
    local amqp=$((PORT_BASE + RUN_IDX*3)) http=$((PORT_BASE + RUN_IDX*3 + 1))
    local mesh="127.0.0.1:$((PORT_BASE + RUN_IDX*3 + 2))"
    RUN_IDX=$((RUN_IDX + 1))

    # `env` so all VAR=VAL words (including the per-case fault vars in "$@")
    # are treated as environment assignments, not as a command to run.
    env LD_PRELOAD="$SHIM" "$@" \
        BEAVERMQ_CLUSTER=on BEAVERMQ_NODE_ID=0 BEAVERMQ_CLUSTER_NODES="$mesh" \
        BEAVERMQ_CLUSTER_SECRET=faulttest \
        BEAVERMQ_AMQP_PORT="$amqp" BEAVERMQ_HTTP_PORT="$http" \
        BEAVERMQ_DATA_DIR="$dd" BEAVERMQ_BIND=127.0.0.1 BEAVERMQ_LOG_LEVEL=info \
        "$BIN" >"$dd/log" 2>&1 &
    local pid=$!
    sleep 3
    kill -TERM "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

    local durable_hits
    durable_hits=$(grep -c "storage is no longer durable" "$dd/log" 2>/dev/null)
    durable_hits=${durable_hits:-0}

    if [ "$expect" = "failstop" ]; then
        if [ "$durable_hits" -ge 1 ]; then
            echo "OK: [$name] fail-stop fired ($durable_hits x 'no longer durable')"
        else
            echo "FAIL: [$name] expected a storage fail-stop, but none was logged"
            tail -8 "$dd/log"; FAILED=1
        fi
    else # healthy
        if [ "$durable_hits" -eq 0 ] && grep -q "became LEADER" "$dd/log"; then
            echo "OK: [$name] node came up healthy, no false storage failure"
        else
            echo "FAIL: [$name] expected a clean run, got $durable_hits durability errors"
            tail -8 "$dd/log"; FAILED=1
        fi
    fi
    rm -rf "$dd"
}

echo "== control: no faults -> healthy leader, no storage error =="
run_case "control" healthy

echo "== inject fsync failure on WAL/meta/snap -> fail-stop =="
run_case "fsync" failstop FAULT_FSYNC_AFTER=0

echo "== inject write failure on WAL/meta/snap -> fail-stop =="
run_case "write" failstop FAULT_WRITE_AFTER=0

echo
if [ "$FAILED" -eq 0 ]; then
    echo "PASS: storage fault injection - fail-stop verified, control clean"
    exit 0
else
    echo "FAIL: storage fault injection"
    exit 1
fi
