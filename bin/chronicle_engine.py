#!/usr/bin/env python3
"""Chronicle Engine — custom inference server replacing Ollama.

Three always-on llama-server processes managed by a thin Python router.
No model swapping. All models loaded at boot. Health monitor auto-restarts crashed servers.

Architecture:
  ┌─────────────────────────────────────────────────┐
  │  chronicle_engine.py (this file)                │
  │  - Ollama-compatible API on :11434              │
  │  - Routes requests to the correct backend       │
  │  - Think-token separation for Qwen3             │
  │  - /status introspection                        │
  ├──────────────┬───────────────┬──────────────────┤
  │ Embed :8701  │ Chat32B :8703 (local fallback)  │
  │ qwen3-embed  │ gemma4-26b (local) │ qwen3-32b (Darby/Ada via Groq) │
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
API_PORT = 11436  # Ollama systemd owns 11434; engine proxies on 11436
GPU_LAYERS = 99

# Cloud inference (Groq) — routes 32B calls off-device for speed
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "qwen/qwen3-32b"  # Groq's model ID for Qwen3 32B
GROQ_MODEL_ALT = "openai/gpt-oss-120b"  # OpenAI OSS 120B — different family from Qwen and Llama

# Cloud inference (Cerebras) — Darby upgrade to Qwen3-235B
CEREBRAS_API_KEY = os.environ.get("CEREBRAS_API_KEY", "")
CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"
CEREBRAS_MODEL = "qwen-3-235b-a22b-instruct-2507"  # Qwen3 235B MoE via Cerebras wafer-scale

# Three always-on servers
SERVERS = {
    "embed": {
        "file": "qwen3-embed.gguf",
        "managed": False,  # llama.cpp deleted — embeddings via Jetson Ollama
        "port": 8701,
        "ctx": 512,
        "extra": ["--embedding", "--batch-size", "2048", "--ubatch-size", "512"],
    },
    "gemma": {
        "file": "gemma4:26b",
        "port": 11435,  # llama-server for Gemma 4
        "managed": False,  # llama-server managed via systemd
    },
    "chat32b": {
        "managed": False,  # llama.cpp deleted — all 32B goes through Groq
        "file": "qwen3-32b.gguf",
        "port": 8703,
        "ctx": 4096,
        "extra": [],
        "startup_timeout": 180,  # 32B needs ~100-120s to load 21GB to VRAM
    },
}

# Model name → (server key, lora index or None)
# Agents call by model name; we route to the right backend
# "groq" routes to Groq cloud API instead of local llama-server
MODEL_ROUTES = {
    # Embedding
    "chronicle-embed": ("embed", None),
    "qwen3-embedding:0.6b": ("embed", None),
    # Ada (GPT-OSS 120B via Groq) — crossref, provocateur, challenger
    "chronicle-challenger": ("groq_alt" if GROQ_API_KEY else "chat32b", None),
    # Gemma (Gemma 4 26B local) — gate, routing, classification
    "chronicle-gemma": ("gemma", None),
    "gemma4:26b": ("gemma", None),
    # Darby (Qwen3 235B via Cerebras) — intern, deep reasoning
    "chronicle-deep": ("cerebras" if CEREBRAS_API_KEY else "groq" if GROQ_API_KEY else "chat32b", None),
    "chronicle-mind": ("cerebras" if CEREBRAS_API_KEY else "groq" if GROQ_API_KEY else "chat32b", None),
    "qwen3:32b": ("cerebras" if CEREBRAS_API_KEY else "groq" if GROQ_API_KEY else "chat32b", None),
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

HEALTH_CHECK_INTERVAL = 30  # seconds between health checks
MAX_RESTART_FAILURES = 3    # give up after this many consecutive restart failures


class ProcessManager:
    def __init__(self):
        self._procs: Dict[str, subprocess.Popen] = {}
        self._ever_healthy: Dict[str, bool] = {}  # only restart servers that were once alive
        self._restart_failures: Dict[str, int] = defaultdict(int)
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
        """Start all three llama-server processes sequentially.

        Sequential startup avoids VRAM allocation races where
        simultaneous model loading causes one server to fail.
        """
        for key in SERVERS:
            if SERVERS[key].get("managed", True) is False:
                log.info(f"Skipping [{key}] (externally managed)")
                continue
            await self._start_server(key)

    async def _start_server(self, key: str) -> bool:
        """Start a single server and wait for it to be healthy."""
        if SERVERS[key].get("managed", True) is False:
            return True  # Externally managed
        if not os.path.isfile(LLAMA_SERVER_BIN):
            log.warning(f"  [{key}] SKIPPED — llama-server binary not found at {LLAMA_SERVER_BIN}")
            return False
        cmd = self._build_cmd(key)
        log.info(f"Starting [{key}] on :{SERVERS[key]['port']}")
        self._procs[key] = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=self._env
        )
        port = SERVERS[key]["port"]
        timeout = SERVERS[key].get("startup_timeout", 90)
        if await self._wait_health(port, timeout=timeout):
            log.info(f"  [{key}] ready (PID {self._procs[key].pid})")
            self._ever_healthy[key] = True
            return True
        else:
            log.error(f"  [{key}] FAILED to start")
            return False

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

    async def health_monitor(self):
        """Background task: check child processes, restart if crashed."""
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            for key in list(self._procs.keys()):
                proc = self._procs.get(key)
                if not proc or proc.poll() is None:
                    # Process is running (or never started)
                    self._restart_failures[key] = 0
                    continue
                if not self._ever_healthy.get(key):
                    # Never started successfully — don't try to restart
                    continue
                if self._restart_failures[key] >= MAX_RESTART_FAILURES:
                    continue  # already gave up
                exit_code = proc.returncode
                log.warning(f"[{key}] died (exit code {exit_code}), restarting...")
                if await self._start_server(key):
                    self._restart_failures[key] = 0
                else:
                    self._restart_failures[key] += 1
                    remaining = MAX_RESTART_FAILURES - self._restart_failures[key]
                    if remaining > 0:
                        log.error(f"  [{key}] restart failed ({remaining} attempts remaining)")
                    else:
                        log.error(f"  [{key}] giving up after {MAX_RESTART_FAILURES} failed restarts")

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
        # Route to local Ollama for embeddings (qwen3-embedding primary, mxbai on Jetson as fallback)
        EMBED_URL = "http://localhost:11434/api/embed"
        async with ClientSession(timeout=ClientTimeout(total=30)) as session:
            async with session.post(
                EMBED_URL,
                json={"model": "qwen3-embedding:0.6b", "input": texts},
            ) as resp:
                data = await resp.json()

        embeddings = data.get("embeddings", [])
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


async def _call_groq(messages: List[Dict], options: Dict, model_override: str = None) -> Dict:
    """Call Groq cloud API for 32B inference."""
    payload = {
        "model": model_override or GROQ_MODEL,
        "messages": messages,
        "stream": False,
        "max_tokens": options.get("num_predict", 4096),
        "temperature": options.get("temperature", 0.6),
    }
    # Allow callers to disable Qwen3 thinking via reasoning_effort
    if "reasoning_effort" in options:
        payload["reasoning_effort"] = options["reasoning_effort"]
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


async def _call_cerebras(messages: List[Dict], options: Dict) -> Dict:
    """Call Cerebras cloud API for 235B inference."""
    payload = {
        "model": CEREBRAS_MODEL,
        "messages": messages,
        "stream": False,
        "max_tokens": options.get("num_predict", 4096),
        "temperature": options.get("temperature", 0.6),
    }
    if "reasoning_effort" in options:
        payload["reasoning_effort"] = options["reasoning_effort"]
    headers = {
        "Authorization": f"Bearer {CEREBRAS_API_KEY}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }
    timeout_s = options.get("timeout", 90)  # Cerebras is fast but 235B needs more headroom
    async with ClientSession(timeout=ClientTimeout(total=timeout_s), auto_decompress=True) as session:
        async with session.post(
            f"{CEREBRAS_BASE_URL}/chat/completions",
            json=payload,
            headers=headers,
        ) as resp:
            if resp.status != 200:
                error_body = await resp.text()
                raise Exception(f"Cerebras {resp.status}: {error_body}")
            return await resp.json()


async def handle_chat(request: web.Request) -> web.Response:
    body = await request.json()
    model_name = body.get("model", "chronicle-deep")
    messages = body.get("messages", [])
    options = body.get("options", {})

    server_key, lora_idx = resolve_model(model_name)
    if not server_key or server_key == "embed":
        return web.json_response({"error": f"Unknown chat model: {model_name}"}, status=400)

    t0 = time.time()
    try:
        if server_key == "cerebras":
            data = await _call_cerebras(messages, options)
        elif server_key in ("groq", "groq_alt"):
            data = await _call_groq(messages, options, GROQ_MODEL_ALT if server_key == "groq_alt" else None)
        else:
            port = SERVERS[server_key]["port"]
            payload: Dict[str, Any] = {
                "messages": messages,
                "stream": False,
                "max_tokens": options.get("num_predict", 4096),
                "temperature": options.get("temperature", 0.6),
            }
            # Add model name for Ollama-backed servers (gemma)
            if server_key == "gemma":
                payload["model"] = SERVERS["gemma"]["file"]
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
        if server_key in ("chat32b", "groq", "groq_alt", "cerebras"):
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
    model_name = body.get("model", "chronicle-deep")
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
        if server_key == "cerebras":
            data = await _call_cerebras(messages, options)
        elif server_key in ("groq", "groq_alt"):
            data = await _call_groq(messages, options, GROQ_MODEL_ALT if server_key == "groq_alt" else None)
        else:
            port = SERVERS[server_key]["port"]
            payload: Dict[str, Any] = {
                "messages": messages,
                "stream": False,
                "max_tokens": options.get("num_predict", 4096),
                "temperature": options.get("temperature", 0.6),
            }
            # Add model name for Ollama-backed servers (gemma)
            if server_key == "gemma":
                payload["model"] = SERVERS["gemma"]["file"]
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
        if server_key in ("chat32b", "groq", "groq_alt", "cerebras"):
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
    if CEREBRAS_API_KEY:
        status["cerebras"] = {
            "status": "enabled",
            "model": CEREBRAS_MODEL,
            "routes": ["chronicle-deep", "qwen3:32b"],
        }
    elif GROQ_API_KEY:
        status["groq"] = {
            "status": "enabled",
            "model": GROQ_MODEL,
            "routes": ["chronicle-deep", "qwen3:32b"],
        }
    if GROQ_API_KEY:
        status["groq_alt"] = {
            "status": "enabled",
            "model": GROQ_MODEL_ALT,
            "routes": ["chronicle-challenger"],
        }
    return web.json_response(status)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def on_startup(app):
    await procs.start_all()
    log.info("Engine ready — all servers running")
    app["health_monitor"] = asyncio.create_task(procs.health_monitor())


async def on_shutdown(app):
    task = app.get("health_monitor")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
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
    if CEREBRAS_API_KEY:
        log.info(f"  [cerebras] {CEREBRAS_MODEL} via Cerebras cloud (Darby)")
    elif GROQ_API_KEY:
        log.info(f"  [groq] {GROQ_MODEL} via Groq cloud (Darby)")
    if GROQ_API_KEY:
        log.info(f"  [groq_alt] {GROQ_MODEL_ALT} via Groq cloud (Ada/Challenger)")
    for key, spec in SERVERS.items():
        ctx_str = f" ctx={spec['ctx']}" if 'ctx' in spec else ""
        log.info(f"  [{key}] {spec['file']}{ctx_str} :{spec['port']}")

    app = create_app()
    web.run_app(app, host="0.0.0.0", port=API_PORT, print=None)


if __name__ == "__main__":
    main()
