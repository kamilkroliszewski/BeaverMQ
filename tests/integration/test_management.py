#!/usr/bin/env python3
"""
test_management.py - management HTTP API integration test using only the
standard library (no third-party deps). Verifies the auth gate: healthz is
open, the API requires credentials (401), correct credentials work (200), and
a burst of wrong credentials from one IP is rate-limited (429).

Usage: test_management.py <host> <http_port> <user> <password>
Exits 0 on success, 1 on any failure.
"""
import base64
import sys
import urllib.error
import urllib.request


def req(url, user=None, pw=None):
    r = urllib.request.Request(url)
    if user is not None:
        tok = base64.b64encode(f"{user}:{pw}".encode()).decode()
        r.add_header("Authorization", "Basic " + tok)
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def main() -> int:
    host, port, user, pw = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    base = f"http://{host}:{port}"

    assert req(base + "/api/healthz") == 200, "healthz should be open"
    print("OK: /api/healthz is unauthenticated (200)")

    assert req(base + "/api/overview") == 401, "API without creds should be 401"
    print("OK: management API requires credentials (401)")

    assert req(base + "/api/overview", user, pw) == 200, "correct creds should be 200"
    print("OK: correct credentials accepted (200)")

    # A burst of wrong-password requests from this IP must eventually be
    # rate-limited (429) rather than hashing every one (authlimit).
    codes = [req(base + "/api/overview", user, "wrongpw") for _ in range(12)]
    assert 429 in codes, f"expected a 429 among wrong-login burst, got {codes}"
    assert codes[0] == 401, f"first wrong login should be 401, got {codes[0]}"
    print(f"OK: wrong-login burst rate-limited (codes: {codes})")

    print("PASS: management API integration")
    return 0


if __name__ == "__main__":
    sys.exit(main())
