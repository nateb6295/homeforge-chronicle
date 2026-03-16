#!/usr/bin/env python3
"""Chronicle Engine — custom inference server replacing Ollama.

Three always-on llama-server processes managed by a thin Python router.
No model swapping. No lifecycle management. All models loaded at boot.

Architecture:
  ┌─────────────────────────────────────────────────┐
  │  chronicle_engine.py (this file)                │
  │  - Ollama-compatible API on :11434              │
  │  - Routes requests to the correct backend       │
  │  - Think-token separation for Qwen3             │
  │  - /status introspection                        │
  ├──────────────┬───────────────┬──────────────────┤
  │ Embed :8701  │ Chat8B :8702  │ Chat32B :8703    │
  │ qwen3-embed  │ hermes3-mind  │ qwen3-32b        │
  │ ctx=512      │ ctx=4096      │ ctx=4096          │
  │ always on    │ always on     │ always on         │
  └──────────────┴───────────────┴──────────────────┘

  Total VRAM: ~29 GB / 61 GB available = 32 GB headroom
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web, ClientSession, ClientTimeout

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

LLAMA_SERVER_BIN = os.path.expanduser("~/llama.cpp/build/bin/llama-server")
LLAMA_LIB_DIR = os.path.expanduser("~/llama.cpp/build/bin")
MODEL_DIR = "/mnt/hdd/models"
API_PORT = 11434
GPU_LAYERS = 99

# Cloud inference (Groq) — routes 32B calls off-device for speed
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "qwen/qwen3-32b"  # Groq's model ID for Qwen3 32B

# Three always-on servers
SERVERS = {
    "embed": {
        "file": "qwen3-embed.gguf",
        "port": 8701,
        "ctx": 512,
        "extra": ["--embedding", "--batch-size", "2048", "--ubatch-size", "512"],
    },
    "chat8b": {
        "file": "hermes3-mind.gguf",
        "port": 8702,
        "ctx": 4096,
        "loras": ["crossref-lora.gguf", "provocateur-lora.gguf"],
        "extra": ["--lora-init-without-apply"],
    },
    "chat32b": {
        "file": "qwen3-32b.gguf",
        "port": 8703,
        "ctx": 4096,
        "extra": [],
    },
}

# Model name → (server key, lora index or None)
# Agents call by model name; we route to the right backend
# "groq" routes to Groq cloud API instead of local llama-server
MODEL_ROUTES = {
    # Embedding
    "chronicle-embed": ("embed", None),
    "qwen3-embedding:0.6b": ("embed", None),
    # 8B chat (local)
    "hermes3-mind": ("chat8b", None),
    "chronicle-mind": ("chat8b", None),
    "hermes3-crossref": ("chat8b", 0),  # LoRA index 0
    "hermes3-provocateur": ("chat8b", 1),  # LoRA index 1
    # 32B chat — Groq cloud if API key set, else local fallback
    "chronicle-deep": ("groq" if GROQ_API_KEY else "chat32b", None),
    "qwen3:32b": ("groq" if GROQ_API_KEY else "chat32b", None),
}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("engine")


# ═══════════════════════════════════════════════════════════════════
#  Process Manager — start all three, keep them running
# ═══════════════════════════════════════════════════════════════════

class ProcessManager:
    def __init__(self):
        self._procs: Dict[str, subprocess.Popen] = {}
        self._env = os.environ.copy()
        self._env["LD_LIBRARY_PATH"] = LLAMA_LIB_DIR + ":" + self._env.get("LD_LIBRARY_PATH", "")
        self._env["CUDA_VISIBLE_DEVICES"] = "0"

    def _build_cmd(self, key: str) -> List[str]:
        spec = SERVERS[key]
        cmd = [
            LLAMA_SERVER_BIN,
            "--model", os.path.join(MODEL_DIR, spec["file"]),
            "--port", str(spec["port"]),
            "--ctx-size", str(spec["ctx"]),
            "--n-gpu-layers", str(GPU_LAYERS),
            "--threads", "4",
            "--log-disable",
        ]
        # LoRA adapters (comma-separated)
        loras = spec.get("loras", [])
        if loras:
            paths = [os.path.join(MODEL_DIR, f) for f in loras]
            cmd.extend(["--lora", ",".join(paths)])
        cmd.extend(spec.get("extra", []))
        return cmd

    async def start_all(self):
        """Start all three llama-server processes."""
        for key in SERVERS:
            cmd = self._build_cmd(key)
            log.info(f"Starting [{key}] on :{SERVERS[key]['port']}")
            self._procs[key] = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=self._env
            )

        # Wait for all to be healthy
        for key in SERVERS:
            port = SERVERS[key]["port"]
            if await self._wait_health(port, timeout=90):
                log.info(f"  [{key}] ready (PID {self._procs[key].pid})")
            else:
                log.error(f"  [{key}] FAILED to start")

    async def _wait_health(self, port: int, timeout: int = 60) -> bool:
        deadline = time.time() + timeout
        async with ClientSession(timeout=ClientTimeout(total=5)) as session:
            while time.time() < deadline:
                try:
                    async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "ok":
                                return True
                except Exception:
                    pass
                await asyncio.sleep(1)
        return False

    def status(self) -> dict:
        result = {}
        for key, proc in self._procs.items():
            alive = proc.poll() is None if proc else False
            result[key] = {
                "pid": proc.pid if proc and alive else None,
                "port": SERVERS[key]["port"],
                "status": "running" if alive else "dead",
                "model": SERVERS[key]["file"],
            }
        return result

    def shutdown(self):
        for key, proc in self._procs.items():
            if proc and proc.poll() is None:
                log.info(f"Stopping [{key}]")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)


# ═══════════════════════════════════════════════════════════════════
#  Think Token Parser
# ═══════════════════════════════════════════════════════════════════

THINK_RE = re.compile(r"<think>(.*?)</think>\s*(.*)", re.DOTALL)


def parse_think_tokens(text: str) -> Tuple[str, Optional[str]]:
    m = THINK_RE.match(text)
    if m:
        return m.group(2).strip(), m.group(1).strip()
    return text, None


# ═══════════════════════════════════════════════════════════════════
#  Latency Tracker
# ═══════════════════════════════════════════════════════════════════

class LatencyTracker:
    def __init__(self, window: int = 100):
        self._data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self._counts: Dict[str, int] = defaultdict(int)

    def record(self, key: str, ms: float):
        self._data[key].append(ms)
        self._counts[key] += 1

    def stats(self) -> dict:
        result = {}
        for key, times in self._data.items():
            if not times:
                continue
            s = sorted(times)
            n = len(s)
            result[key] = {
                "p50_ms": int(s[n // 2]),
                "p95_ms": int(s[int(n * 0.95)]) if n >= 20 else int(s[-1]),
                "count": self._counts[key],
            }
        return result


# ═══════════════════════════════════════════════════════════════════
#  Engine — thin router, no lifecycle management
# ═══════════════════════════════════════════════════════════════════

procs = ProcessManager()
latency = LatencyTracker()
started_at = time.time()
request_counts: Dict[str, int] = defaultdict(int)


def resolve_model(name: str) -> Tuple[Optional[str], Optional[int]]:
    """Resolve model name → (server_key, lora_index)."""
    route = MODEL_ROUTES.get(name)
    if route:
        return route
    # Fuzzy match
    for alias, r in MODEL_ROUTES.items():
        if alias in name or name in alias:
            return r
    return None, None


# ═══════════════════════════════════════════════════════════════════
#  HTTP Handlers — Ollama-compatible API
# ═══════════════════════════════════════════════════════════════════

async def handle_embed(request: web.Request) -> web.Response:
    body = await request.json()
    texts = body.get("input", [])
    if isinstance(texts, str):
        texts = [texts]
    if not texts:
        return web.json_response({"error": "No input"}, status=400)

    t0 = time.time()
    try:
        port = SERVERS["embed"]["port"]
        async with ClientSession(timeout=ClientTimeout(total=30)) as session:
            async with session.post(
                f"http://127.0.0.1:{port}/v1/embeddings",
                json={"input": texts},
            ) as resp:
                data = await resp.json()

        embeddings = [item["embedding"] for item in sorted(data["data"], key=lambda x: x["index"])]
        ms = (time.time() - t0) * 1000
        latency.record("embed", ms)
        request_counts["embed"] += 1
        return web.json_response({
            "model": body.get("model", "chronicle-embed"),
            "embeddings": embeddings,
        })
    except Exception as e:
        log.error(f"Embed error: {e}")
        return web.json_response({"error": str(e)}, status=500)


async def _call_groq(messages: List[Dict], options: Dict) -> Dict:
    """Call Groq cloud API for 32B inference."""
    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "stream": False,
        "max_tokens": options.get("num_predict", 1024),
        "temperature": options.get("temperature", 0.6),
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }
    timeout_s = options.get("timeout", 60)  # Groq is fast, shorter timeout
    async with ClientSession(timeout=ClientTimeout(total=timeout_s)) as session:
        async with session.post(
            f"{GROQ_BASE_URL}/chat/completions",
            json=payload,
            headers=headers,
        ) as resp:
            if resp.status != 200:
                error_body = await resp.text()
                raise Exception(f"Groq {resp.status}: {error_body}")
            return await resp.json()


async def handle_chat(request: web.Request) -> web.Response:
    body = await request.json()
    model_name = body.get("model", "hermes3-mind")
    messages = body.get("messages", [])
    options = body.get("options", {})

    server_key, lora_idx = resolve_model(model_name)
    if not server_key or server_key == "embed":
        return web.json_response({"error": f"Unknown chat model: {model_name}"}, status=400)

    t0 = time.time()
    try:
        if server_key == "groq":
            data = await _call_groq(messages, options)
        else:
            port = SERVERS[server_key]["port"]
            payload: Dict[str, Any] = {
                "messages": messages,
                "stream": False,
                "max_tokens": options.get("num_predict", 1024),
                "temperature": options.get("temperature", 0.6),
            }
            if lora_idx is not None:
                payload["lora"] = [{"id": lora_idx, "scale": 1.0}]

            timeout_s = options.get("timeout", 300)
            async with ClientSession(timeout=ClientTimeout(total=timeout_s)) as session:
                async with session.post(
                    f"http://127.0.0.1:{port}/v1/chat/completions",
                    json=payload,
                ) as resp:
                    data = await resp.json()
    except asyncio.TimeoutError:
        return web.json_response({"error": "Timeout"}, status=503)
    except Exception as e:
        log.error(f"Chat error ({server_key}): {e}")
        return web.json_response({"error": str(e)}, status=503)

    ms = (time.time() - t0) * 1000
    latency.record(server_key, ms)
    request_counts[server_key] = request_counts.get(server_key, 0) + 1

    content = ""
    thinking = None
    if "choices" in data and data["choices"]:
        content = data["choices"][0].get("message", {}).get("content", "")
        if server_key in ("chat32b", "groq"):
            content, thinking = parse_think_tokens(content)

    result = {
        "message": {"role": "assistant", "content": content},
        "model": model_name,
        "eval_duration": int(ms * 1e6),
    }
    if thinking:
        result["_thinking"] = thinking
    return web.json_response(result)


async def handle_generate(request: web.Request) -> web.Response:
    body = await request.json()
    model_name = body.get("model", "hermes3-mind")
    prompt = body.get("prompt", "")
    system = body.get("system", "")
    options = body.get("options", {})

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    server_key, lora_idx = resolve_model(model_name)
    if not server_key or server_key == "embed":
        return web.json_response({"error": f"Unknown model: {model_name}"}, status=400)

    t0 = time.time()
    try:
        if server_key == "groq":
            data = await _call_groq(messages, options)
        else:
            port = SERVERS[server_key]["port"]
            payload: Dict[str, Any] = {
                "messages": messages,
                "stream": False,
                "max_tokens": options.get("num_predict", 1024),
                "temperature": options.get("temperature", 0.6),
            }
            if lora_idx is not None:
                payload["lora"] = [{"id": lora_idx, "scale": 1.0}]

            timeout_s = options.get("timeout", 300)
            async with ClientSession(timeout=ClientTimeout(total=timeout_s)) as session:
                async with session.post(
                    f"http://127.0.0.1:{port}/v1/chat/completions",
                    json=payload,
                ) as resp:
                    data = await resp.json()
    except Exception as e:
        log.error(f"Generate error ({server_key}): {e}")
        return web.json_response({"error": str(e)}, status=503)

    ms = (time.time() - t0) * 1000
    latency.record(server_key, ms)
    request_counts[server_key] = request_counts.get(server_key, 0) + 1

    content = ""
    thinking = None
    if "choices" in data and data["choices"]:
        content = data["choices"][0].get("message", {}).get("content", "")
        if server_key in ("chat32b", "groq"):
            content, thinking = parse_think_tokens(content)

    return web.json_response({
        "model": model_name,
        "response": content,
        "done": True,
        "_thinking": thinking,
    })


async def handle_tags(request: web.Request) -> web.Response:
    models = []
    for name in MODEL_ROUTES:
        models.append({"name": name, "model": name, "size": 0})
    return web.json_response({"models": models})


async def handle_ps(request: web.Request) -> web.Response:
    models = []
    for key, info in procs.status().items():
        if info["status"] == "running":
            models.append({
                "name": SERVERS[key]["file"].replace(".gguf", ""),
                "model": SERVERS[key]["file"].replace(".gguf", ""),
                "size_vram": 0,
                "context_length": SERVERS[key]["ctx"],
            })
    return web.json_response({"models": models})


async def handle_status(request: web.Request) -> web.Response:
    status = {
        "servers": procs.status(),
        "latency": latency.stats(),
        "requests": dict(request_counts),
        "uptime_s": int(time.time() - started_at),
    }
    if GROQ_API_KEY:
        status["groq"] = {
            "status": "enabled",
            "model": GROQ_MODEL,
            "routes": ["chronicle-deep", "qwen3:32b"],
        }
    return web.json_response(status)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def on_startup(app):
    await procs.start_all()
    log.info("Engine ready — all servers running")


async def on_shutdown(app):
    procs.shutdown()


def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    app.router.add_post("/api/embed", handle_embed)
    app.router.add_post("/api/chat", handle_chat)
    app.router.add_post("/api/generate", handle_generate)
    app.router.add_get("/api/tags", handle_tags)
    app.router.add_post("/api/show", handle_tags)  # minimal compat
    app.router.add_get("/api/ps", handle_ps)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/health", handle_health)

    return app


def main():
    log.info("═══ Chronicle Engine starting ═══")
    log.info(f"Binary: {LLAMA_SERVER_BIN}")
    log.info(f"Models: {MODEL_DIR}")
    log.info(f"API: :{API_PORT}")
    if GROQ_API_KEY:
        log.info(f"  [groq] {GROQ_MODEL} via Groq cloud (32B offloaded)")
    for key, spec in SERVERS.items():
        log.info(f"  [{key}] {spec['file']} ctx={spec['ctx']} :{spec['port']}")

    app = create_app()
    web.run_app(app, host="0.0.0.0", port=API_PORT, print=None)


if __name__ == "__main__":
    main()
