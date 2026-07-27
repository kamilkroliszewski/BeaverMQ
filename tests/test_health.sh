#!/usr/bin/env bash
#
# test_health.sh - per-loop health aggregation. The supervisor heartbeat is
# gated on EVERY critical loop staying fresh, so a frozen non-main loop (here
# the management loop, frozen via the BEAVERMQ_TEST_FREEZE_MGMT_LOOP hook) must
# make the worker miss its heartbeat and get respawned - even though worker 0's
# loop is still ticking. A control run (no freeze) proves the worker is NOT
# respawned spuriously.
#
# Not part of `make test`; run directly or via `make health-test`.
# Requires: bash, curl, a built ./build/beavermq.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"
[ -x "$BIN" ] || { echo "FAIL: binary not found at '$BIN' - run 'make' first"; exit 1; }

FAILED=0

# run_case <name> <freeze:0|1> <expect_respawn:0|1>
run_case() {
    local name="$1" freeze="$2" expect="$3"
    local dd; dd="$(mktemp -d)"
    local sup_pid=""
    # Short heartbeat so a stall is caught quickly; timeout > interval.
    local env_freeze=""
    [ "$freeze" = "1" ] && env_freeze="BEAVERMQ_TEST_FREEZE_MGMT_LOOP=1"

    env BEAVERMQ_DATA_DIR="$dd" BEAVERMQ_AMQP_PORT=25730 BEAVERMQ_HTTP_PORT=25731 \
        BEAVERMQ_BIND=127.0.0.1 \
        BEAVERMQ_SUPERVISOR_HEARTBEAT_MS=400 BEAVERMQ_SUPERVISOR_HEARTBEAT_TIMEOUT_MS=1200 \
        $env_freeze "$BIN" --supervisor >"$dd/log" 2>&1 &
    sup_pid=$!
    disown

    # Wait for the first worker.pid.
    local first=""
    for _ in $(seq 1 60); do
        first="$(cat "$dd/worker.pid" 2>/dev/null || echo "")"
        [ -n "$first" ] && kill -0 "$first" 2>/dev/null && break
        sleep 0.1
    done
    if [ -z "$first" ]; then
        echo "FAIL: [$name] worker never started"; FAILED=1
        kill -TERM "$sup_pid" 2>/dev/null; rm -rf "$dd"; return
    fi

    # Watch for a respawn (worker.pid changes to a different live pid).
    local respawned=0
    for _ in $(seq 1 60); do   # up to ~6s
        cur="$(cat "$dd/worker.pid" 2>/dev/null || echo "")"
        if [ -n "$cur" ] && [ "$cur" != "$first" ] && kill -0 "$cur" 2>/dev/null; then
            respawned=1; break
        fi
        sleep 0.1
    done

    kill -TERM "$sup_pid" 2>/dev/null
    for _ in $(seq 1 30); do kill -0 "$sup_pid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$sup_pid" 2>/dev/null

    if [ "$respawned" = "$expect" ]; then
        echo "OK: [$name] respawned=$respawned (expected $expect)"
    else
        echo "FAIL: [$name] respawned=$respawned (expected $expect)"
        tail -12 "$dd/log"; FAILED=1
    fi
    rm -rf "$dd"
}

echo "== control: no frozen loop -> worker stays up (no respawn) =="
run_case "control" 0 0
echo "== frozen management loop -> heartbeat withheld -> worker respawned =="
run_case "frozen-mgmt" 1 1

echo
if [ "$FAILED" -eq 0 ]; then
    echo "PASS: per-loop health aggregation (frozen loop -> respawn; healthy -> stable)"
    exit 0
else
    echo "FAIL: per-loop health aggregation"
    exit 1
fi
