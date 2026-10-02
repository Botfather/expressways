#!/usr/bin/env python3
"""Bounded loopback-only destination used by adapter conformance tests."""

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

MAX_BODY_BYTES = 1024 * 1024


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bearer", required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--fail-first", type=int, default=0)
    args = parser.parse_args()

    if args.listen not in {"127.0.0.1", "::1"}:
        raise SystemExit("fixture server only permits loopback listeners")
    if not 0 <= args.fail_first <= 100:
        raise SystemExit("--fail-first must be between 0 and 100")
    args.log.parent.mkdir(parents=True, exist_ok=True)
    state = {"attempts": 0}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        server_version = "ExpresswaysAdapterFixture/1"

        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/health":
                self.send_error(404)
                return
            with lock:
                body = json.dumps({"ok": True, "attempts": state["attempts"]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/replies":
                self.send_error(404)
                return
            if self.headers.get("Authorization") != f"Bearer {args.bearer}":
                self.send_error(401)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.send_error(400)
                return
            if not 1 <= length <= MAX_BODY_BYTES:
                self.send_error(413)
                return
            raw = self.rfile.read(length)
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError:
                self.send_error(400)
                return

            with lock:
                state["attempts"] += 1
                attempt = state["attempts"]
                status = 503 if attempt <= args.fail_first else 204
                record = {
                    "attempt": attempt,
                    "status": status,
                    "idempotency_key": self.headers.get("Idempotency-Key"),
                    "correlation_id": self.headers.get("X-Expressways-Correlation-Id"),
                    "authorization": self.headers.get("Authorization"),
                    "envelope": envelope,
                }
                with args.log.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    os.fsync(output.fileno())

            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer((args.listen, args.port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
