#!/usr/bin/env bash
#
# test_persistence.sh - standalone authstore persistence + durable bootstrap.
# Verifies that a standalone broker:
#   1. persists users across a restart (they are NOT lost, unlike before);
#   2. writes the store 0600 (it holds password hashes);
#   3. keeps the first-boot bootstrap window CLOSED once a user has ever
#      existed - even after the last user is deleted, bootstrap does not reopen.
#
# Not part of `make test` (starts a broker across restarts); run directly or via
# `make persistence-test`. Requires: bash, curl, a built ./build/beavermq.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/build/beavermq"
[ -x "$BIN" ] || { echo "FAIL: binary not found at '$BIN' - run 'make' first"; exit 1; }

AMQP=25710
HTTP=25711
DD="$(mktemp -d)"
PID=""

cleanup() {
    [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null && { kill -TERM "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; }
    rm -rf "$DD"
}
trap cleanup EXIT

fail() { echo "FAIL: $1"; echo "--- log ---"; cat "$DD/log" 2>/dev/null; exit 1; }

start() {
    BEAVERMQ_AMQP_PORT="$AMQP" BEAVERMQ_HTTP_PORT="$HTTP" BEAVERMQ_DATA_DIR="$DD" \
        BEAVERMQ_BIND=127.0.0.1 BEAVERMQ_LOG_LEVEL=info "$BIN" >>"$DD/log" 2>&1 &
    PID=$!
    for _ in $(seq 1 50); do
        curl -sf "http://127.0.0.1:$HTTP/api/healthz" >/dev/null 2>&1 && return 0
        kill -0 "$PID" 2>/dev/null || fail "broker exited during startup"
        sleep 0.1
    done
    fail "broker did not answer healthz"
}
stop() { kill -TERM "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
code() { curl -s -o /dev/null -w "%{http_code}" "$@"; }

echo "== boot 1: fresh broker, bootstrap the first admin =="
start
BEAVERMQ_DATA_DIR="$DD" "$BIN" add-user admin secret -H 127.0.0.1 -p "$HTTP" >/dev/null 2>&1 \
    || fail "bootstrap add-user should succeed on a fresh broker"
[ "$(code -u admin:secret http://127.0.0.1:$HTTP/api/overview)" = "200" ] \
    || fail "admin login should work right after bootstrap"
[ -f "$DD/authstore.db" ] || fail "authstore.db was not written"
[ "$(stat -c '%a' "$DD/authstore.db")" = "600" ] \
    || fail "authstore.db must be 0600 (it holds password hashes)"
echo "OK: bootstrapped admin, login works, authstore.db is 0600"
stop

echo "== boot 2: restart - the user must persist (no re-bootstrap needed) =="
start
[ "$(code -u admin:secret http://127.0.0.1:$HTTP/api/overview)" = "200" ] \
    || fail "admin login should still work after restart (persistence)"
if BEAVERMQ_DATA_DIR="$DD" "$BIN" add-user intruder pw -H 127.0.0.1 -p "$HTTP" >/dev/null 2>&1; then
    fail "bootstrap window must be CLOSED once a user exists"
fi
echo "OK: user persisted across restart; bootstrap window stays closed"

echo "== delete the last user, then restart =="
[ "$(code -u admin:secret -X DELETE http://127.0.0.1:$HTTP/api/users/admin)" = "200" ] \
    || fail "deleting the admin user should succeed"
stop

echo "== boot 3: after deleting the last user, bootstrap must NOT reopen =="
start
if BEAVERMQ_DATA_DIR="$DD" "$BIN" add-user intruder pw -H 127.0.0.1 -p "$HTTP" >/dev/null 2>&1; then
    fail "deleting the last user must NOT reopen the bootstrap window"
fi
grep -q "bootstrap already completed" "$DD/log" \
    || fail "expected the durable 'bootstrap already completed' marker to hold"
echo "OK: durable bootstrap - window stays closed after last user deleted"
stop

echo
echo "PASS: standalone persistence + durable bootstrap"
exit 0
