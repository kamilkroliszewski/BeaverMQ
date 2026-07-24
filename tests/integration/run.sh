#!/usr/bin/env bash
#
# run.sh - AMQP + management API integration tests against a live broker.
#
# Starts a real ./build/beavermq on ephemeral ports with a temp data_dir,
# bootstraps a user, then runs:
#   - test_management.py  (Python stdlib only; always runs)
#   - test_amqp.py        (needs the `pika` AMQP client; auto-installed into a
#                          venv, or SKIPPED with a clear message if offline)
#
# Not part of `make test` (that runs the dependency-free C unit tests). Run:
#   bash tests/integration/run.sh        # or: make integration
#
# Requires: bash, python3, a built ./build/beavermq (run `make` first).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"

AMQP_PORT=25980
HTTP_PORT=25981
USER=itest
PASS=itestpw
DATA_DIR="$(mktemp -d)"
LOG_FILE="$DATA_DIR/broker.log"
BROKER_PID=""
FAILED=0

cleanup() {
    if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill -TERM "$BROKER_PID" 2>/dev/null
        for _ in $(seq 1 30); do kill -0 "$BROKER_PID" 2>/dev/null || break; sleep 0.1; done
        kill -9 "$BROKER_PID" 2>/dev/null
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $1"; echo "--- broker log ---"; cat "$LOG_FILE" 2>/dev/null; exit 1; }

[ -x "$BIN" ] || fail "binary not found at '$BIN' - run 'make' first"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"

# Resolve a python interpreter that can 'import pika' (system, or a venv we
# create). Echoes the interpreter path, or nothing if pika can't be obtained.
find_pika_python() {
    if python3 -c "import pika" >/dev/null 2>&1; then echo "python3"; return; fi
    local venv="$REPO_ROOT/build/itest-venv"
    if [ ! -x "$venv/bin/python" ]; then
        python3 -m venv "$venv" >/dev/null 2>&1 || return 0
        "$venv/bin/pip" install --quiet pika >/dev/null 2>&1 || return 0
    fi
    "$venv/bin/python" -c "import pika" >/dev/null 2>&1 && echo "$venv/bin/python"
}

echo "== starting broker (amqp=$AMQP_PORT http=$HTTP_PORT data_dir=$DATA_DIR) =="
BEAVERMQ_AMQP_PORT="$AMQP_PORT" BEAVERMQ_HTTP_PORT="$HTTP_PORT" \
    BEAVERMQ_DATA_DIR="$DATA_DIR" BEAVERMQ_BIND=127.0.0.1 BEAVERMQ_LOG_LEVEL=warn \
    "$BIN" >"$LOG_FILE" 2>&1 &
BROKER_PID=$!
disown

for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$HTTP_PORT/api/healthz" >/dev/null 2>&1 && break
    kill -0 "$BROKER_PID" 2>/dev/null || fail "broker exited during startup"
    sleep 0.1
done
curl -sf "http://127.0.0.1:$HTTP_PORT/api/healthz" >/dev/null 2>&1 || fail "broker never answered /api/healthz"
echo "OK: broker up"

BEAVERMQ_DATA_DIR="$DATA_DIR" "$BIN" add-user "$USER" "$PASS" \
    -H 127.0.0.1 -p "$HTTP_PORT" >/dev/null 2>&1 || fail "add-user failed"
echo "OK: bootstrapped user '$USER'"

# AMQP runs BEFORE the management test: the latter ends with a wrong-login
# burst that deliberately rate-limits 127.0.0.1 (authlimit is process-global
# and keyed by client IP, shared by the AMQP and HTTP auth paths), which would
# otherwise block the AMQP login that also comes from 127.0.0.1.
echo "== AMQP client-compat test (pika) =="
PYBIN="$(find_pika_python)"
if [ -z "$PYBIN" ]; then
    echo "SKIP: 'pika' is unavailable and could not be installed (offline?);"
    echo "      AMQP client-compat test not run. Install pika to enable it."
else
    "$PYBIN" "$SCRIPT_DIR/test_amqp.py" 127.0.0.1 "$AMQP_PORT" "$USER" "$PASS" || FAILED=1
fi

echo "== management API test (stdlib) =="
python3 "$SCRIPT_DIR/test_management.py" 127.0.0.1 "$HTTP_PORT" "$USER" "$PASS" || FAILED=1

echo
if [ "$FAILED" -eq 0 ]; then echo "PASS: integration suite"; else fail "one or more integration tests failed"; fi
