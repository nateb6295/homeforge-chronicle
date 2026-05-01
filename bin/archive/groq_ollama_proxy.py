#!/usr/bin/env python3
"""Groq→Ollama format proxy.

Accepts Ollama-format /api/chat requests, forwards to Groq's OpenAI-compatible
API, translates response back to Ollama format. Lets the MCP binary use Groq
for cognitive state compression without modifying the Rust binary.

Usage:
  python3 groq_ollama_proxy.py          # runs on port 11436
  python3 groq_ollama_proxy.py --port 8080
"""

import http.server
import json
import os
import sys
import urllib.request

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "llama-3.3-70b-versatile"
PORT = 11436


class OllamaProxy(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet

    def do_GET(self):
        if self.path == "/api/tags":
            # Return fake model list so MCP thinks this is Ollama
            resp = {"models": [{"name": DEFAULT_MODEL, "model": DEFAULT_MODEL}]}
            self._respond(200, resp)
        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/api/chat":
            self._respond(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        # Translate Ollama request → OpenAI request
        model = body.get("model", DEFAULT_MODEL)
        # Map known Ollama model names to Groq models
        groq_model = DEFAULT_MODEL
        messages = body.get("messages", [])

        groq_body = {
            "model": groq_model,
            "messages": messages,
            "temperature": body.get("options", {}).get("temperature", 0.7),
            "max_tokens": body.get("options", {}).get("num_predict", 4096),
            "stream": False,
        }

        try:
            req = urllib.request.Request(
                GROQ_URL,
                data=json.dumps(groq_body).encode(),
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                groq_resp = json.loads(resp.read())

            # Translate OpenAI response → Ollama response
            choice = groq_resp["choices"][0]
            ollama_resp = {
                "model": model,
                "created_at": groq_resp.get("created", ""),
                "message": {
                    "role": "assistant",
                    "content": choice["message"]["content"],
                },
                "done": True,
                "total_duration": 0,
                "eval_count": groq_resp.get("usage", {}).get("completion_tokens", 0),
                "prompt_eval_count": groq_resp.get("usage", {}).get("prompt_tokens", 0),
            }
            self._respond(200, ollama_resp)

        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()

    if not GROQ_API_KEY:
        print("ERROR: GROQ_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    server = http.server.HTTPServer(("127.0.0.1", args.port), OllamaProxy)
    print(f"Groq→Ollama proxy on port {args.port} (model: {DEFAULT_MODEL})")
    server.serve_forever()


if __name__ == "__main__":
    main()
