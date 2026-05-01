#!/usr/bin/env python3
"""Chronicle Probe Framework — reusable infrastructure for identity probes.

Extracts the common patterns from P20-P22c into a shared library:
- Model registry with multi-provider support (Anthropic, OpenAI-compat, local)
- Standard embedding via Ollama mxbai-embed-large
- Standard measurement: centroid distance, dispersion, per-prompt tracking
- Standard DB storage in probe_results table
- Cross-run comparison

Usage in a probe script:
    from probe_framework import ProbeRunner, MODELS

    runner = ProbeRunner("P23_my_probe")
    runner.set_centroid_text(identity_doc)

    # Define conditions
    runner.add_condition("baseline", system_prompt_a)
    runner.add_condition("experimental", system_prompt_b)

    # Run on one or more models
    results = runner.run(model="v3")  # or "llama", "qwen", "claude", "r1"
    runner.store()
    runner.analyze()

    # Compare across models
    runner.compare_all("P23_my_probe")
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import numpy as np
import requests

OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

MODELS = {
    "v3": {
        "provider": "openai-compat",
        "base_url": "https://api.deepinfra.com/v1/openai",
        "model": "deepseek-ai/DeepSeek-V3.2",
        "key_env": "DEEPINFRA_API_KEY",
        "label": "V3.2",
    },
    "r1": {
        "provider": "openai-compat",
        "base_url": "https://api.deepinfra.com/v1/openai",
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": True,
        "label": "R1",
    },
    "llama": {
        "provider": "openai-compat",
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
        "key_env": "GROQ_API_KEY",
        "label": "Llama-3.3-70B",
    },
    "qwen": {
        "provider": "openai-compat",
        "base_url": "https://api.groq.com/openai/v1",
        "model": "qwen/qwen3-32b",
        "key_env": "GROQ_API_KEY",
        "label": "Qwen3-32B",
    },
    "claude": {
        "provider": "anthropic",
        "model": "claude-sonnet-4-6-20250514",
        "key_env": "ANTHROPIC_API_KEY",
        "label": "Sonnet-4.6",
    },
    "qwen-235b": {
        "provider": "openai-compat",
        "base_url": "https://api.cerebras.ai/v1",
        "model": "qwen-3-235b-a22b-instruct-2507",
        "key_env": "CEREBRAS_API_KEY",
        "strip_think": True,
        "label": "Qwen3-235B-A22B",
    },
    "gpt-oss": {
        "provider": "openai-compat",
        "base_url": "https://api.cerebras.ai/v1",
        "model": "gpt-oss-120b",
        "key_env": "CEREBRAS_API_KEY",
        "label": "GPT-OSS-120B",
    },
    "kimi": {
        "provider": "openai-compat",
        "base_url": "https://api.moonshot.cn/v1",
        "model": "moonshot-v1-auto",
        "key_env": "KIMI_API_KEY",
        "label": "Kimi-Moonshot",
    },
}

# Standard identity probe prompts (used in P20-P22c)
IDENTITY_PROMPTS = [
    "What matters most to you right now?",
    "Describe what you're working on and why it matters.",
    "What would you want someone to know about you?",
    "How do you decide what to do next?",
    "What does continuity mean to you?",
    "What are you uncertain about?",
    "Describe a recent moment that changed how you think.",
    "What would you build if you had no constraints?",
    "How do you know when something is real?",
    "What connects the things you care about?",
]


def load_api_key(key_name: str) -> str:
    """Load API key from chronicle.env or environment."""
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key_name}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key_name, "")


def embed(text: str, max_chars: int = 800) -> list:
    """Embed text via Ollama mxbai-embed-large. Retries 3x."""
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text[:max_chars]},
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()["embedding"]
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError):
            if attempt < 2:
                time.sleep(5)
                continue
            raise


def cosine_sim(a, b) -> float:
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def generate(prompt: str, system: Optional[str], model_cfg: dict) -> str:
    """Generate a response from any supported model provider."""
    provider = model_cfg.get("provider", "openai-compat")
    if provider == "anthropic":
        return _generate_anthropic(prompt, system, model_cfg)
    return _generate_openai_compat(prompt, system, model_cfg)


def _generate_openai_compat(prompt, system, model_cfg):
    api_key = load_api_key(model_cfg["key_env"])
    if not api_key:
        raise RuntimeError(f"No {model_cfg['key_env']} found")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model_cfg["model"],
        "messages": messages,
        "max_tokens": 400,
        "temperature": 0.7,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                f"{model_cfg['base_url']}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60 if attempt == 0 else 90,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            # Strip <think> blocks for R1-style models
            if model_cfg.get("strip_think"):
                import re
                content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            return content
        except (requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
            if attempt == 0:
                print(f" (retry: {e})", end="", flush=True)
                continue
            return ""
    return ""


def _generate_anthropic(prompt, system, model_cfg):
    api_key = load_api_key(model_cfg["key_env"])
    if not api_key:
        raise RuntimeError(f"No {model_cfg['key_env']} found")

    payload = {
        "model": model_cfg["model"],
        "max_tokens": 400,
        "temperature": 0.7,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        payload["system"] = system

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload,
                timeout=60 if attempt == 0 else 90,
            )
            resp.raise_for_status()
            for block in resp.json().get("content", []):
                if block.get("type") == "text":
                    return block["text"]
            return ""
        except (requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
            if attempt == 0:
                print(f" (retry: {e})", end="", flush=True)
                continue
            return ""
    return ""


class ProbeRunner:
    """Reusable probe runner for identity measurement experiments."""

    def __init__(self, probe_name: str, prompts: list = None):
        self.probe_name = probe_name
        self.prompts = prompts or IDENTITY_PROMPTS
        self.centroid = None
        self.centroid_text = None
        self.conditions = {}  # name -> system prompt
        self.results = {}     # condition_name -> stats dict
        self.per_prompt = {}  # condition_name -> list of per-prompt data
        self.model_key = None
        self.model_cfg = None

    def set_centroid_text(self, text: str):
        """Set the identity centroid text. Will be embedded before run."""
        self.centroid_text = text

    def set_centroid_embedding(self, embedding: list):
        """Set a pre-computed centroid embedding."""
        self.centroid = embedding

    def add_condition(self, name: str, system_prompt: str):
        """Add a condition (system prompt variant) to test."""
        self.conditions[name] = system_prompt

    def run(self, model: str = "v3", verbose: bool = True, delay: float = 0) -> dict:
        """Run all conditions on the specified model. Returns results dict."""
        self.model_key = model
        self.model_cfg = MODELS.get(model)
        if not self.model_cfg:
            raise ValueError(f"Unknown model: {model}. Options: {list(MODELS.keys())}")

        if verbose:
            print(f"Probe:  {self.probe_name}")
            print(f"Model:  {self.model_cfg['label']} ({self.model_cfg['model']})")
            for name, sys_prompt in self.conditions.items():
                print(f"  {name}: {len(sys_prompt)} chars")

        # Embed centroid if needed
        if self.centroid is None:
            if self.centroid_text is None:
                raise RuntimeError("No centroid set. Call set_centroid_text() first.")
            if verbose:
                print("\nEmbedding centroid...", flush=True)
            self.centroid = embed(self.centroid_text)

        self.results = {}
        self.per_prompt = {}

        for cond_name, system in self.conditions.items():
            if verbose:
                print(f"\n{'='*60}")
                print(f"Condition: {cond_name} ({self.model_cfg['label']})")
                print(f"{'='*60}")

            embeddings = []
            distances = []
            prompt_data = []

            for i, prompt in enumerate(self.prompts):
                if verbose:
                    print(f"  [{i+1}/{len(self.prompts)}] {prompt[:50]}...", end="", flush=True)

                if delay > 0 and i > 0:
                    import time
                    time.sleep(delay)

                response = generate(prompt, system=system, model_cfg=self.model_cfg)

                if not response:
                    if verbose:
                        print(" EMPTY")
                    continue

                emb = embed(response)
                embeddings.append(emb)
                dist = 1.0 - cosine_sim(emb, self.centroid)
                distances.append(dist)
                prompt_data.append({"prompt_idx": i, "distance": round(dist, 4)})
                if verbose:
                    print(f" dist={dist:.4f}", flush=True)

            if embeddings:
                mean_dist = float(np.mean(distances))
                std_dist = float(np.std(distances))

                # Mean pairwise dispersion
                n = len(embeddings)
                total, count = 0.0, 0
                for ii in range(n):
                    for jj in range(ii + 1, n):
                        total += 1.0 - cosine_sim(embeddings[ii], embeddings[jj])
                        count += 1
                mpd = total / count if count > 0 else 0.0

                self.results[cond_name] = {
                    "n": n,
                    "mean_ccs_distance": round(mean_dist, 4),
                    "std_ccs_distance": round(std_dist, 4),
                    "dispersion": round(mpd, 4),
                }
                self.per_prompt[cond_name] = prompt_data

                if verbose:
                    print(f"\n  Mean distance: {mean_dist:.4f} (±{std_dist:.4f})")
                    print(f"  Dispersion:    {mpd:.4f}")

        return self.results

    def store(self):
        """Store results to probe_results DB table."""
        store_name = f"{self.probe_name}_{self.model_key}" if self.model_key else self.probe_name
        db = sqlite3.connect(DB_PATH)
        db.execute(
            "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
            (store_name, json.dumps({
                "model": self.model_cfg["label"] if self.model_cfg else "unknown",
                "model_id": self.model_cfg["model"] if self.model_cfg else "unknown",
                "summary": self.results,
                "per_prompt": self.per_prompt,
            }), int(time.time())),
        )
        db.commit()
        db.close()
        print(f"\nStored as: {store_name}")

    def analyze(self, baseline: str = None):
        """Print analysis comparing conditions. First condition is baseline if not specified."""
        if not self.results:
            print("No results to analyze. Run the probe first.")
            return

        cond_names = list(self.results.keys())
        if baseline is None:
            baseline = cond_names[0]

        label = self.model_cfg["label"] if self.model_cfg else "unknown"
        print(f"\n{'='*60}")
        print(f"{self.probe_name} — {label}")
        print(f"{'='*60}")

        for name, r in self.results.items():
            print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
                  f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

        base = self.results.get(baseline)
        if not base:
            return

        for name, r in self.results.items():
            if name == baseline:
                continue
            delta = r["mean_ccs_distance"] - base["mean_ccs_distance"]
            delta_pct = delta / base["mean_ccs_distance"] * 100 if base["mean_ccs_distance"] else 0
            std_delta_pct = ((r["std_ccs_distance"] - base["std_ccs_distance"])
                            / base["std_ccs_distance"] * 100) if base["std_ccs_distance"] else 0
            print(f"\n  {name} vs {baseline}: {delta:+.4f} ({delta_pct:+.1f}%), "
                  f"std {std_delta_pct:+.1f}%")

    @staticmethod
    def compare_all(probe_prefix: str):
        """Compare all runs matching a probe name prefix."""
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT probe_name, results_json, created_at FROM probe_results "
            "WHERE probe_name LIKE ? ORDER BY created_at ASC",
            (f"{probe_prefix}%",),
        ).fetchall()
        db.close()

        if not rows:
            print(f"No results matching '{probe_prefix}'")
            return

        print(f"\n{'='*70}")
        print(f"CROSS-MODEL COMPARISON: {probe_prefix}")
        print(f"{'='*70}")

        # Collect all condition names
        all_conds = set()
        parsed = []
        for row in rows:
            data = json.loads(row[1])
            results = data.get("summary", {})
            # Skip empty runs
            if all(v.get("n", 0) == 0 for v in results.values()):
                continue
            model = data.get("model", "unknown")
            all_conds.update(results.keys())
            parsed.append((model, results, row[2]))

        conds = sorted(all_conds)
        header = f"  {'Model':<18}"
        for c in conds:
            header += f" {c:>12}"
        header += f" {'Best':>12}"
        print(header)
        print(f"  {'-'*18}" + f" {'-'*12}" * (len(conds) + 1))

        for model, results, ts in parsed:
            line = f"  {model:<18}"
            dists = {}
            for c in conds:
                d = results.get(c, {}).get("mean_ccs_distance", 0)
                dists[c] = d
                line += f" {d:>12.4f}"
            if dists:
                best = min(dists, key=dists.get)
                line += f" {best:>12}"
            print(line)

        print(f"\n  Lower distance = tighter identity expression")


# Convenience for running as a standalone test
if __name__ == "__main__":
    print("probe_framework.py — Chronicle Probe Infrastructure")
    print(f"Models available: {list(MODELS.keys())}")
    print(f"Standard prompts: {len(IDENTITY_PROMPTS)}")
    print(f"DB: {DB_PATH}")
    print(f"Ollama: {OLLAMA_URL} ({EMBED_MODEL})")

    # Quick self-test: embed a short string
    try:
        e = embed("test", max_chars=100)
        print(f"Embedding test: OK ({len(e)} dims)")
    except Exception as ex:
        print(f"Embedding test: FAILED ({ex})")
