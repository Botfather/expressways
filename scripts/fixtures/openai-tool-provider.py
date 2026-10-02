#!/usr/bin/env python3
"""Loopback-only OpenAI-compatible tool-call fixture for backbone tests."""

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
    parser.add_argument("--read-path", required=True)
    parser.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()

    if args.listen not in {"127.0.0.1", "::1"}:
        raise SystemExit("fixture server only permits loopback listeners")
    args.log.parent.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    state = {"requests": 0, "tool_requests": 0, "final_requests": 0}

    class Handler(BaseHTTPRequestHandler):
        server_version = "ExpresswaysOpenAIToolFixture/1"

        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/health":
                self.send_error(404)
                return
            with lock:
                body = json.dumps({"ok": True, **state}).encode()
            self._send_json(200, body)

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/chat/completions":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self.send_error(400)
                return
            if not 1 <= length <= MAX_BODY_BYTES:
                self.send_error(413)
                return
            try:
                request = json.loads(self.rfile.read(length))
            except json.JSONDecodeError:
                self.send_error(400)
                return
            messages = request.get("messages")
            tools = request.get("tools")
            if not isinstance(messages, list) or not isinstance(tools, list):
                self.send_error(400)
                return

            tool_result = next(
                (
                    message.get("content")
                    for message in reversed(messages)
                    if message.get("role") == "tool"
                    and message.get("name") == "read_file"
                ),
                None,
            )
            with lock:
                state["requests"] += 1
                if tool_result is None:
                    state["tool_requests"] += 1
                    message = {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_backbone_read",
                                "type": "function",
                                "function": {
                                    "name": "read_file",
                                    "arguments": json.dumps(
                                        {"path": args.read_path, "max_bytes": 4096}
                                    ),
                                },
                            }
                        ],
                    }
                else:
                    state["final_requests"] += 1
                    message = {
                        "role": "assistant",
                        "content": f"Tool-backed reply: {tool_result}",
                    }
                with args.log.open("a", encoding="utf-8") as output:
                    output.write(
                        json.dumps(
                            {
                                "sequence": state["requests"],
                                "saw_tool_result": tool_result is not None,
                                "request": request,
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
                    output.flush()
                    os.fsync(output.fileno())

            body = json.dumps(
                {
                    "id": f"chatcmpl-backbone-{state['requests']}",
                    "object": "chat.completion",
                    "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
                }
            ).encode()
            self._send_json(200, body)

        def _send_json(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    ThreadingHTTPServer((args.listen, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
