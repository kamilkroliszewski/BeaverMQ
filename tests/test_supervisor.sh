#!/usr/bin/env bash
#
# test_supervisor.sh - end-to-end smoke test for the supervisor/worker
# ("let it crash") architecture (src/supervisor.c). Starts a supervised
# broker, SIGSEGVs the worker, and verifies it is respawned with zero
# operator intervention while the supervisor itself survives; then verifies
# graceful shutdown on SIGTERM.
#
# NOT wired into `make test`: that target builds/runs compiled tests/*.c
# binaries only (see the Makefile's wildcard-based discovery). Run this
# manually or as a separate CI step:
#
#   bash tests/test_supervisor.sh
#
# Requires: bash, curl, a built ./build/beavermq (run `make` first).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"

AMQP_PORT=25972
HTTP_PORT=25973
DATA_DIR="$(mktemp -d)"
LOG_FILE="$DATA_DIR/out.log"

SUP_PID=""
FAILED=0

cleanup() {
    if [ -n "$SUP_PID" ] && kill -0 "$SUP_PID" 2>/dev/null; then
        kill -TERM "$SUP_PID" 2>/dev/null
        for _ in $(seq 1 30); do
            kill -0 "$SUP_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$SUP_PID" 2>/dev/null
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1"
    echo "--- log ---"
    cat "$LOG_FILE" 2>/dev/null
    FAILED=1
    exit 1
}

wait_for() {
    # wait_for <description> <max_tries> <check-command...>
    local desc="$1" tries="$2"
    shift 2
    for _ in $(seq 1 "$tries"); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    fail "timed out waiting for: $desc"
}

healthz_ok() {
    curl -sf "http://127.0.0.1:$HTTP_PORT/api/healthz" >/dev/null 2>&1
}

[ -x "$BIN" ] || fail "binary not found at '$BIN' - run 'make' first"

echo "== starting supervisor (data_dir=$DATA_DIR, amqp=$AMQP_PORT, http=$HTTP_PORT) =="
BEAVERMQ_DATA_DIR="$DATA_DIR" BEAVERMQ_AMQP_PORT="$AMQP_PORT" BEAVERMQ_HTTP_PORT="$HTTP_PORT" \
    BEAVERMQ_BIND=127.0.0.1 \
    "$BIN" --supervisor >"$LOG_FILE" 2>&1 &
SUP_PID=$!
disown

wait_for "supervisor.pid to appear" 50 test -f "$DATA_DIR/supervisor.pid"
wait_for "worker.pid to appear"     50 test -f "$DATA_DIR/worker.pid"

SUP_PID_FILE=$(cat "$DATA_DIR/supervisor.pid")
[ "$SUP_PID_FILE" = "$SUP_PID" ] || fail "supervisor.pid ($SUP_PID_FILE) != launched pid ($SUP_PID)"

wait_for "broker to answer /api/healthz" 50 healthz_ok
echo "OK: supervisor up (pid $SUP_PID), worker up, /api/healthz responding"

WORKER_PID_1=$(cat "$DATA_DIR/worker.pid")
kill -0 "$WORKER_PID_1" 2>/dev/null || fail "worker.pid ($WORKER_PID_1) is not a live process"
echo "== crashing worker (pid $WORKER_PID_1) with SIGSEGV =="
kill -SEGV "$WORKER_PID_1"

# Wait for worker.pid to change to a DIFFERENT, live pid - not just for the
# file to exist (it already does, from the first spawn), and not just for it
# to be re-created with the SAME content mid-write (see the write-tmp+rename
# pattern in supervisor.c - a reader never sees a partial file, but it could
# still read stale content if it checks too early).
NEW_PID=""
for _ in $(seq 1 50); do
    CUR=$(cat "$DATA_DIR/worker.pid" 2>/dev/null || echo "")
    if [ -n "$CUR" ] && [ "$CUR" != "$WORKER_PID_1" ] && kill -0 "$CUR" 2>/dev/null; then
        NEW_PID="$CUR"
        break
    fi
    sleep 0.1
done
[ -n "$NEW_PID" ] || fail "worker was never respawned (worker.pid still $WORKER_PID_1 or dead)"
echo "OK: respawned as pid $NEW_PID"

wait_for "respawned broker to answer /api/healthz" 50 healthz_ok
echo "OK: /api/healthz responding again after respawn"

CUR_SUP=$(cat "$DATA_DIR/supervisor.pid")
[ "$CUR_SUP" = "$SUP_PID" ] || fail "supervisor.pid changed ($CUR_SUP) - the supervisor itself crashed!"
kill -0 "$SUP_PID" 2>/dev/null || fail "supervisor process is no longer alive"
echo "OK: supervisor (pid $SUP_PID) survived the worker crash"

echo "== sending SIGTERM to supervisor (graceful shutdown) =="
kill -TERM "$SUP_PID"
for _ in $(seq 1 80); do
    kill -0 "$SUP_PID" 2>/dev/null || { echo "OK: supervisor exited after SIGTERM"; SUP_PID=""; break; }
    sleep 0.1
done
if [ -n "$SUP_PID" ] && kill -0 "$SUP_PID" 2>/dev/null; then
    fail "supervisor did not exit within timeout after SIGTERM"
fi

# Give the (already SIGTERM'd) worker a moment to have been reaped too, then
# confirm there is no leftover process tree at all.
sleep 0.2
if pgrep -f "$BIN" >/dev/null 2>&1; then
    fail "a beavermq process is still running after supervisor shutdown"
fi
echo "OK: no leftover processes after shutdown"

echo
echo "PASS: worker crash -> respawn -> supervisor survives -> graceful shutdown, all verified"
exit 0
