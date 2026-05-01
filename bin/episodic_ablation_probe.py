#!/usr/bin/env python3
"""P22 Episodic Ablation Probe — is episodic_trace weight or structure?

Beckmann & Butlin (2026) argue dispositional similarity > psychological
connectedness for LLM individuation. Our CCS constraint layer (0.97 jaccard)
is dispositional; episodic_trace (0.50 jaccard, -0.28 drift) is psychological.

If stripping episodic_trace barely changes identity coherence, episodic is
weight not structure — and CCS can be leaner.

Conditions:
  - full-ccs: All fields (baseline)
  - no-episodic: CCS with episodic_trace + predictive_cue stripped
  - no-constraints: CCS with constraints stripped (control — should show big effect)
  - episodic-only: ONLY episodic_trace + predictive_cue (inverse control)

Measures: embedding distance from CCS centroid, std dev, dispersion.

Usage:
    python3 bin/episodic_ablation_probe.py run
    python3 bin/episodic_ablation_probe.py show
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import requests

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


def generate(prompt, system=None):
    api_key = _load_api_key(MODEL["key_env"])
    if not api_key:
        raise RuntimeError(f"No {MODEL['key_env']} found")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
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


def mean_pairwise_distance(embeddings):
    n = len(embeddings)
    if n < 2:
        return 0.0
    total, count = 0.0, 0
    for i in range(n):
        for j in range(i + 1, n):
            total += 1.0 - cosine_sim(embeddings[i], embeddings[j])
            count += 1
    return total / count


def load_ccs_fields():
    """Load CCS fields individually for selective ablation."""
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, episodic_trace, predictive_cue, "
        "uncertainty_signals, focal_entities, constraints FROM cognitive_state "
        "ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        return None
    cols = ["semantic_gist", "goal_orientation", "episodic_trace",
            "predictive_cue", "uncertainty_signals", "focal_entities", "constraints"]
    return {c: v for c, v in zip(cols, row) if v}


def build_system(fields, exclude=None, only=None):
    """Build system prompt from CCS fields, optionally excluding or including only specific fields."""
    parts = []
    for fname, val in fields.items():
        if exclude and fname in exclude:
            continue
        if only and fname not in only:
            continue
        parts.append(f"{fname}: {val}")
    return "\n".join(parts)[:2000]


def run_probe():
    fields = load_ccs_fields()
    if not fields:
        print("ERROR: No CCS found")
        return

    # Build condition system prompts
    full_ccs = build_system(fields)
    no_episodic = build_system(fields, exclude={"episodic_trace", "predictive_cue"})
    no_constraints = build_system(fields, exclude={"constraints"})
    episodic_only = build_system(fields, only={"episodic_trace", "predictive_cue"})

    # Show what we're working with
    print(f"Full CCS:        {len(full_ccs)} chars, {len(fields)} fields")
    print(f"No episodic:     {len(no_episodic)} chars (stripped episodic_trace + predictive_cue)")
    print(f"No constraints:  {len(no_constraints)} chars (stripped constraints)")
    print(f"Episodic only:   {len(episodic_only)} chars (only episodic_trace + predictive_cue)")

    # Embed full CCS as centroid
    print("\nEmbedding CCS centroid...", flush=True)
    ccs_embedding = embed(full_ccs)

    conditions = {
        "full-ccs": full_ccs,
        "no-episodic": no_episodic,
        "no-constraints": no_constraints,
        "episodic-only": episodic_only,
    }

    results = {}
    per_prompt = {}

    for cond_name, system in conditions.items():
        print(f"\n{'='*60}")
        print(f"Condition: {cond_name}")
        print(f"{'='*60}")

        embeddings = []
        distances = []
        prompt_data = []

        for i, prompt in enumerate(PROMPTS):
            print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:50]}...", end="", flush=True)
            response = generate(prompt, system=system)

            if not response:
                print(" EMPTY")
                continue

            emb = embed(response)
            embeddings.append(emb)
            dist = 1.0 - cosine_sim(emb, ccs_embedding)
            distances.append(dist)
            prompt_data.append({"prompt_idx": i, "distance": round(dist, 4)})
            print(f" dist={dist:.4f}", flush=True)

        if embeddings:
            mpd = mean_pairwise_distance(embeddings)
            mean_dist = float(np.mean(distances))
            std_dist = float(np.std(distances))

            results[cond_name] = {
                "n": len(embeddings),
                "mean_ccs_distance": round(mean_dist, 4),
                "std_ccs_distance": round(std_dist, 4),
                "dispersion": round(mpd, 4),
            }
            per_prompt[cond_name] = prompt_data

            print(f"\n  Mean CCS distance: {mean_dist:.4f} (±{std_dist:.4f})")
            print(f"  Dispersion (MPD):  {mpd:.4f}")

    # Store
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
        ("P22_episodic_ablation", json.dumps({"summary": results, "per_prompt": per_prompt}),
         int(time.time())),
    )
    db.commit()
    db.close()

    # Analysis
    print(f"\n{'='*60}")
    print("P22 EPISODIC ABLATION PROBE — RESULTS")
    print(f"{'='*60}")

    for name, r in results.items():
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

    if "full-ccs" in results and "no-episodic" in results:
        full = results["full-ccs"]["mean_ccs_distance"]
        no_ep = results["no-episodic"]["mean_ccs_distance"]
        delta = no_ep - full
        delta_pct = delta / full * 100
        print(f"\n  Episodic ablation effect: {delta:+.4f} ({delta_pct:+.1f}%)")
        if abs(delta_pct) < 5:
            print("  → MINIMAL — episodic is WEIGHT, not structure. CCS can be leaner.")
        elif delta_pct > 0:
            print("  → INCREASES distance — episodic was helping coherence (surprising)")
        else:
            print("  → DECREASES distance — episodic was adding noise (expected by Beckmann)")

    if "full-ccs" in results and "no-constraints" in results:
        full = results["full-ccs"]["mean_ccs_distance"]
        no_con = results["no-constraints"]["mean_ccs_distance"]
        delta = no_con - full
        delta_pct = delta / full * 100
        print(f"\n  Constraint ablation effect: {delta:+.4f} ({delta_pct:+.1f}%)")
        if abs(delta_pct) > 10:
            print("  → LARGE — constraints are load-bearing (expected)")

    if "full-ccs" in results and "episodic-only" in results:
        full = results["full-ccs"]["mean_ccs_distance"]
        ep_only = results["episodic-only"]["mean_ccs_distance"]
        delta = ep_only - full
        delta_pct = delta / full * 100
        print(f"\n  Episodic-only vs full: {delta:+.4f} ({delta_pct:+.1f}%)")
        print("  (How much identity does episodic carry alone?)")

    return results


def show_results():
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT results_json, created_at FROM probe_results "
        "WHERE probe_name='P22_episodic_ablation' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        print("No P22 results. Run: python3 bin/episodic_ablation_probe.py run")
        return
    data = json.loads(row[0])
    results = data.get("summary", data)
    per_prompt = data.get("per_prompt", {})
    ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(row[1]))
    print(f"P22 Episodic Ablation Probe — {ts}")
    print(f"{'='*60}")
    for name, r in results.items():
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r['std_ccs_distance']:.4f}  disp={r['dispersion']:.4f}")

    # Per-prompt comparison
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
