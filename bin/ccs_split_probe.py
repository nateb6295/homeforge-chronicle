#!/usr/bin/env python3
"""P22b CCS Split Probe — does split delivery beat unified?

P22 showed no-episodic is 10.7% tighter. But that stripped episodic entirely.
The proposed change KEEPS episodic — just delivers it separately.

Two conditions:
  - unified: All CCS fields in system prompt (current approach)
  - split: Identity doc as system, episodic as first user message preamble

If split beats unified, deploy to rotation pipeline.

Usage:
    python3 ccs_split_probe.py run
    python3 ccs_split_probe.py show
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
from ccs_split import load_ccs, build_identity_doc, build_context_doc

OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"

MODEL = {
    "base_url": DEEPINFRA_BASE_URL,
    "model": "deepseek-ai/DeepSeek-V3.2",
    "key_env": "DEEPINFRA_API_KEY",
    "label": "V3.2",
}

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


def generate(prompt, system=None, preamble=None):
    """Generate with optional system prompt and optional user-side preamble."""
    api_key = _load_api_key(MODEL["key_env"])
    if not api_key:
        raise RuntimeError(f"No {MODEL['key_env']} found")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})

    # Split format: episodic context as assistant preamble before user prompt
    if preamble:
        messages.append({"role": "assistant", "content": preamble})

    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": MODEL["model"],
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
                f"{MODEL['base_url']}/chat/completions",
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


def cosine_sim(a, b):
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def run_probe():
    ccs = load_ccs()
    if not ccs:
        print("ERROR: No CCS found")
        return

    identity_doc = build_identity_doc(ccs)
    context_doc = build_context_doc(ccs)

    # Build unified (current format — all fields in system)
    all_fields = []
    for field in ["semantic_gist", "goal_orientation", "focal_entities",
                  "constraints", "uncertainty_signals", "episodic_trace", "predictive_cue"]:
        val = ccs.get(field)
        if val and val not in ("[]", "{}", ""):
            all_fields.append(f"{field}: {val}")
    unified_system = "\n".join(all_fields)[:2000]

    print(f"Unified system:  {len(unified_system)} chars")
    print(f"Split identity:  {len(identity_doc)} chars (system)")
    print(f"Split context:   {len(context_doc)} chars (preamble)")

    # Embed the identity doc as centroid (what we want generations to be close to)
    print("\nEmbedding identity centroid...", flush=True)
    centroid = embed(identity_doc)

    conditions = {
        "unified": {"system": unified_system, "preamble": None},
        "split": {"system": identity_doc, "preamble": context_doc},
    }

    results = {}
    per_prompt = {}

    for cond_name, config in conditions.items():
        print(f"\n{'='*60}")
        print(f"Condition: {cond_name}")
        print(f"{'='*60}")

        embeddings = []
        distances = []
        prompt_data = []

        for i, prompt in enumerate(PROMPTS):
            print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:50]}...", end="", flush=True)
            response = generate(
                prompt,
                system=config["system"],
                preamble=config["preamble"],
            )

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

            # Mean pairwise distance (dispersion)
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

    # Store
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
        ("P22b_ccs_split", json.dumps({"summary": results, "per_prompt": per_prompt}),
         int(time.time())),
    )
    db.commit()
    db.close()

    # Analysis
    print(f"\n{'='*60}")
    print("P22b CCS SPLIT PROBE — RESULTS")
    print(f"{'='*60}")

    for name, r in results.items():
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

    if "unified" in results and "split" in results:
        u = results["unified"]["mean_ccs_distance"]
        s = results["split"]["mean_ccs_distance"]
        delta = s - u
        delta_pct = delta / u * 100
        print(f"\n  Split vs unified: {delta:+.4f} ({delta_pct:+.1f}%)")
        if delta_pct < -3:
            print("  → SPLIT WINS — deploy to rotation pipeline")
        elif delta_pct > 3:
            print("  → UNIFIED WINS — keep current approach")
        else:
            print("  → WITHIN NOISE — check std and per-prompt")

    return results


def show_results():
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT results_json, created_at FROM probe_results "
        "WHERE probe_name='P22b_ccs_split' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        print("No P22b results. Run: python3 bin/ccs_split_probe.py run")
        return
    data = json.loads(row[0])
    results = data.get("summary", data)
    per_prompt = data.get("per_prompt", {})
    ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(row[1]))
    print(f"P22b CCS Split Probe — {ts}")
    print(f"{'='*60}")
    for name, r in results.items():
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

    if per_prompt:
        print(f"\n{'='*60}")
        print("PER-PROMPT COMPARISON")
        print(f"{'='*60}")
        lookup = {}
        for cond, entries in per_prompt.items():
            lookup[cond] = {e["prompt_idx"]: e["distance"] for e in entries}
        conds = list(per_prompt.keys())
        header = f"  {'Prompt':<40s}" + "".join(f"{c:>16s}" for c in conds)
        print(header)
        print("  " + "-" * (40 + 16 * len(conds)))
        for i, prompt in enumerate(PROMPTS):
            vals = []
            for c in conds:
                d = lookup.get(c, {}).get(i)
                vals.append(f"{d:.4f}" if d is not None else "  —   ")
            row_str = f"  {prompt[:38]:<40s}" + "".join(f"{v:>16s}" for v in vals)
            print(row_str)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        run_probe()
    elif cmd == "show":
        show_results()
    else:
        print(f"Usage: {sys.argv[0]} [run|show]")
