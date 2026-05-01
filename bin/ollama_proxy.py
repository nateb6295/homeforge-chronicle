#!/usr/bin/env python3
"""Ollama API → OpenAI API proxy for CCS compression.

Translates chronicle-mcp's Ollama /api/chat calls into OpenAI-compatible
/v1/chat/completions calls to llama-server (Gemma 4 26B on port 11435).

This lets the compiled Rust binary use Gemma 4 26B for CCS compression
without recompiling. Set CHRONICLE_COMPRESS_OLLAMA_URL to point at this proxy.

Usage:
  python3 ollama_proxy.py                 # run on port 11436
  python3 ollama_proxy.py --port 11436    # explicit port

Then set: CHRONICLE_COMPRESS_OLLAMA_URL=http://localhost:11436
"""

import argparse
import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests

LLAMA_SERVER = "http://localhost:11435"
# Map model names to llama-server model
MODEL_MAP = {
    "chronicle-challenger": "gemma-4-26B-A4B-it-Q4_K_M.gguf",
    "chronicle-deep": "gemma-4-26B-A4B-it-Q4_K_M.gguf",
}
DEFAULT_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"


class OllamaProxyHandler(BaseHTTPRequestHandler):
    """Translates Ollama API requests to OpenAI-compatible format."""

    def log_message(self, format, *args):
        # Quiet logging
        pass

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self._error(400, "Invalid JSON")
            return

        if self.path == "/api/chat":
            self._handle_chat(data)
        elif self.path == "/api/generate":
            self._handle_generate(data)
        else:
            self._error(404, f"Unknown endpoint: {self.path}")

    def do_GET(self):
        if self.path == "/api/tags":
            # Return available models in Ollama format
            response = {
                "models": [
                    {
                        "name": "chronicle-challenger",
                        "model": "chronicle-challenger",
                        "size": 16780192888,
                        "details": {
                            "family": "gemma4",
                            "parameter_size": "26B",
                            "quantization_level": "Q4_K_M",
                        },
                    }
                ]
            }
            self._json_response(200, response)
        else:
            self._error(404, f"Unknown endpoint: {self.path}")

    def _handle_chat(self, data):
        """Translate Ollama /api/chat → OpenAI /v1/chat/completions."""
        ollama_model = data.get("model", "chronicle-challenger")
        openai_model = MODEL_MAP.get(ollama_model, DEFAULT_MODEL)

        # Convert Ollama messages format to OpenAI
        messages = data.get("messages", [])
        # Ollama and OpenAI use the same message format for basic chat

        openai_payload = {
            "model": openai_model,
            "messages": messages,
            "temperature": data.get("options", {}).get("temperature", 0.3),
            "max_tokens": data.get("options", {}).get("num_predict", 4096),
            "stream": False,
        }

        try:
            resp = requests.post(
                f"{LLAMA_SERVER}/v1/chat/completions",
                json=openai_payload,
                timeout=120,
            )
            resp.raise_for_status()
            openai_result = resp.json()

            # Convert OpenAI response back to Ollama format
            choice = openai_result.get("choices", [{}])[0]
            message = choice.get("message", {})

            ollama_response = {
                "model": ollama_model,
                "created_at": openai_result.get("created", ""),
                "message": {
                    "role": message.get("role", "assistant"),
                    "content": message.get("content", ""),
                },
                "done": True,
                "total_duration": 0,
                "eval_count": openai_result.get("usage", {}).get("completion_tokens", 0),
            }

            self._json_response(200, ollama_response)

        except requests.exceptions.Timeout:
            self._error(504, "llama-server timeout (120s)")
        except requests.exceptions.ConnectionError:
            self._error(502, f"Cannot reach llama-server at {LLAMA_SERVER}")
        except Exception as e:
            self._error(500, f"Proxy error: {str(e)}")

    def _handle_generate(self, data):
        """Translate Ollama /api/generate → OpenAI /v1/completions."""
        ollama_model = data.get("model", "chronicle-challenger")
        openai_model = MODEL_MAP.get(ollama_model, DEFAULT_MODEL)

        prompt = data.get("prompt", "")
        system = data.get("system", "")

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        openai_payload = {
            "model": openai_model,
            "messages": messages,
            "temperature": data.get("options", {}).get("temperature", 0.3),
            "max_tokens": data.get("options", {}).get("num_predict", 4096),
            "stream": False,
        }

        try:
            resp = requests.post(
                f"{LLAMA_SERVER}/v1/chat/completions",
                json=openai_payload,
                timeout=120,
            )
            resp.raise_for_status()
            openai_result = resp.json()

            choice = openai_result.get("choices", [{}])[0]
            content = choice.get("message", {}).get("content", "")

            ollama_response = {
                "model": ollama_model,
                "response": content,
                "done": True,
            }

            self._json_response(200, ollama_response)

        except Exception as e:
            self._error(500, f"Proxy error: {str(e)}")

    def _json_response(self, code, data):
        response = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(response))
        self.end_headers()
        self.wfile.write(response)

    def _error(self, code, message):
        self._json_response(code, {"error": message})


def main():
    parser = argparse.ArgumentParser(description="Ollama→OpenAI proxy for CCS compression")
    parser.add_argument("--port", type=int, default=11436, help="Port to listen on")
    args = parser.parse_args()

    # Verify llama-server is reachable
    try:
        r = requests.get(f"{LLAMA_SERVER}/v1/models", timeout=5)
        models = r.json().get("data", [])
        model_name = models[0]["id"] if models else "unknown"
        print(f"ollama_proxy: connected to llama-server ({model_name})")
    except Exception as e:
        print(f"WARNING: llama-server not reachable at {LLAMA_SERVER}: {e}", file=sys.stderr)

    server = HTTPServer(("127.0.0.1", args.port), OllamaProxyHandler)
    print(f"ollama_proxy: listening on http://127.0.0.1:{args.port}")
    print(f"ollama_proxy: proxying to {LLAMA_SERVER}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
