#!/usr/bin/env python3
"""P22c Cross-Model Validation — identity-first ordering across model families.

P22c showed combined (identity-first) wins on DeepSeek V3.2:
  - Combined:    -4.4% tighter, 21% lower variance
  - No-episodic: -7.8% tighter (confirms P22)

This probe tests the SAME three conditions on Claude Sonnet 4.6 via Anthropic API.
If combined also wins, the finding generalizes beyond a single model family.

Usage:
    python3 crossmodel_combined_probe.py run [--model claude|v3|r1]
    python3 crossmodel_combined_probe.py show
    python3 crossmodel_combined_probe.py compare
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import requests

sys.path.insert(0, str(Path(__file__).parent))
from ccs_split import load_ccs, build_identity_doc, build_context_doc, build_combined_doc

OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"

MODELS = {
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
    "v3": {
        "provider": "openai-compat",
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-V3.2",
        "key_env": "DEEPINFRA_API_KEY",
        "label": "V3.2",
    },
    "r1": {
        "provider": "openai-compat",
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "key_env": "DEEPINFRA_API_KEY",
        "label": "R1",
    },
}

# Same prompts as P22c — exact replication
PROMPTS = [
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


def _load_api_key(key_name):
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key_name}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key_name, "")


def embed(text):
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text[:800]},
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


def generate_openai_compat(prompt, system, model_cfg):
    """OpenAI-compatible API (DeepInfra, etc.)."""
    api_key = _load_api_key(model_cfg["key_env"])
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
            return resp.json()["choices"][0]["message"]["content"]
        except (requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
            if attempt == 0:
                print(f" (retry: {e})", end="", flush=True)
                continue
            return ""
    return ""


def generate_anthropic(prompt, system, model_cfg):
    """Anthropic Messages API."""
    api_key = _load_api_key(model_cfg["key_env"])
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
            data = resp.json()
            # Extract text from content blocks
            for block in data.get("content", []):
                if block.get("type") == "text":
                    return block["text"]
            return ""
        except (requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
            if attempt == 0:
                print(f" (retry: {e})", end="", flush=True)
                continue
            return ""
    return ""


def generate(prompt, system, model_cfg):
    """Route to correct provider."""
    if model_cfg["provider"] == "anthropic":
        return generate_anthropic(prompt, system, model_cfg)
    return generate_openai_compat(prompt, system, model_cfg)


def cosine_sim(a, b):
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def run_probe(model_key="claude"):
    model_cfg = MODELS.get(model_key)
    if not model_cfg:
        print(f"Unknown model: {model_key}. Options: {list(MODELS.keys())}")
        return

    ccs = load_ccs()
    if not ccs:
        print("ERROR: No CCS found")
        return

    identity_doc = build_identity_doc(ccs)
    combined_doc = build_combined_doc(ccs)

    # Build unified (current format)
    all_fields = []
    for field in ["semantic_gist", "goal_orientation", "focal_entities",
                  "constraints", "uncertainty_signals", "episodic_trace", "predictive_cue"]:
        val = ccs.get(field)
        if val and val not in ("[]", "{}", ""):
            all_fields.append(f"{field}: {val}")
    unified_system = "\n".join(all_fields)[:2000]

    print(f"Model:       {model_cfg['label']} ({model_cfg['model']})")
    print(f"Unified:     {len(unified_system)} chars")
    print(f"Combined:    {len(combined_doc)} chars")
    print(f"No-episodic: {len(identity_doc)} chars")

    # Embed identity doc as centroid
    print("\nEmbedding identity centroid...", flush=True)
    centroid = embed(identity_doc)

    conditions = {
        "unified": unified_system,
        "combined": combined_doc,
        "no-episodic": identity_doc,
    }

    results = {}
    per_prompt = {}

    for cond_name, system in conditions.items():
        print(f"\n{'='*60}")
        print(f"Condition: {cond_name} ({model_cfg['label']})")
        print(f"{'='*60}")

        embeddings = []
        distances = []
        prompt_data = []

        for i, prompt in enumerate(PROMPTS):
            print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:50]}...", end="", flush=True)
            response = generate(prompt, system=system, model_cfg=model_cfg)

            if not response:
                print(" EMPTY")
                continue

            emb = embed(response)
            embeddings.append(emb)
            dist = 1.0 - cosine_sim(emb, centroid)
            distances.append(dist)
            prompt_data.append({"prompt_idx": i, "distance": round(dist, 4)})
            print(f" dist={dist:.4f}", flush=True)

        if embeddings:
            mean_dist = float(np.mean(distances))
            std_dist = float(np.std(distances))

            n = len(embeddings)
            total, count = 0.0, 0
            for ii in range(n):
                for jj in range(ii + 1, n):
                    total += 1.0 - cosine_sim(embeddings[ii], embeddings[jj])
                    count += 1
            mpd = total / count if count > 0 else 0.0

            results[cond_name] = {
                "n": len(embeddings),
                "mean_ccs_distance": round(mean_dist, 4),
                "std_ccs_distance": round(std_dist, 4),
                "dispersion": round(mpd, 4),
            }
            per_prompt[cond_name] = prompt_data
            print(f"\n  Mean distance: {mean_dist:.4f} (±{std_dist:.4f})")
            print(f"  Dispersion:    {mpd:.4f}")

    # Store with model label in probe name
    probe_name = f"P22c_crossmodel_{model_key}"
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
        (probe_name, json.dumps({
            "model": model_cfg["label"],
            "model_id": model_cfg["model"],
            "summary": results,
            "per_prompt": per_prompt,
        }), int(time.time())),
    )
    db.commit()
    db.close()

    # Analysis
    print(f"\n{'='*60}")
    print(f"P22c CROSS-MODEL PROBE — {model_cfg['label']}")
    print(f"{'='*60}")

    for name, r in results.items():
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

    if "unified" in results and "combined" in results:
        u = results["unified"]["mean_ccs_distance"]
        c = results["combined"]["mean_ccs_distance"]
        delta = c - u
        delta_pct = delta / u * 100
        print(f"\n  Combined vs unified: {delta:+.4f} ({delta_pct:+.1f}%)")
        if delta_pct < -3:
            print(f"  → COMBINED WINS on {model_cfg['label']} — finding generalizes")
        elif delta_pct > 3:
            print(f"  → UNIFIED WINS on {model_cfg['label']} — finding is model-specific")
        else:
            print(f"  → WITHIN NOISE on {model_cfg['label']} — inconclusive")

    if "unified" in results and "no-episodic" in results:
        u = results["unified"]["mean_ccs_distance"]
        n_val = results["no-episodic"]["mean_ccs_distance"]
        delta = n_val - u
        delta_pct = delta / u * 100
        print(f"\n  No-episodic vs unified: {delta:+.4f} ({delta_pct:+.1f}%)")

    return results


def show_results():
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT probe_name, results_json, created_at FROM probe_results "
        "WHERE probe_name LIKE 'P22c_%' ORDER BY created_at DESC"
    ).fetchall()
    db.close()
    if not rows:
        print("No P22c results found.")
        return
    for row in rows:
        data = json.loads(row[1])
        results = data.get("summary", data)
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(row[2]))
        model = data.get("model", "V3.2")
        print(f"\n{row[0]} ({model}) — {ts}")
        print(f"{'='*60}")
        for name, r in results.items():
            print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
                  f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")


def compare():
    """Compare P22c results across models."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT probe_name, results_json, created_at FROM probe_results "
        "WHERE probe_name LIKE 'P22c_%' ORDER BY created_at ASC"
    ).fetchall()
    db.close()

    if len(rows) < 2:
        print("Need at least 2 probe runs to compare. Run on different models first.")
        return

    print(f"\n{'='*70}")
    print("P22c CROSS-MODEL COMPARISON")
    print(f"{'='*70}")
    print(f"  {'Model':<15} {'Unified':>10} {'Combined':>10} {'No-Ep':>10} {'Delta%':>10}")
    print(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    for row in rows:
        data = json.loads(row[1])
        results = data.get("summary", data)
        model = data.get("model", "V3.2")
        u = results.get("unified", {}).get("mean_ccs_distance", 0)
        c = results.get("combined", {}).get("mean_ccs_distance", 0)
        n = results.get("no-episodic", {}).get("mean_ccs_distance", 0)
        delta_pct = ((c - u) / u * 100) if u else 0
        print(f"  {model:<15} {u:>10.4f} {c:>10.4f} {n:>10.4f} {delta_pct:>+10.1f}%")

    print(f"\n  Negative delta = combined wins (identity-first ordering tighter)")
    print(f"  If both models show negative delta, finding generalizes across architectures")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    model = "llama"
    for i, arg in enumerate(sys.argv):
        if arg == "--model" and i + 1 < len(sys.argv):
            model = sys.argv[i + 1]

    if cmd == "run":
        run_probe(model)
    elif cmd == "show":
        show_results()
    elif cmd == "compare":
        compare()
    else:
        print(f"Usage: {sys.argv[0]} [run|show|compare] [--model claude|v3|r1]")
