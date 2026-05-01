#!/usr/bin/env python3
"""P21 Cross-Model CoT Probe — does temporal structure alone produce identity redistribution?

Advance 44 prediction: temporal structure (not model-specific reasoning) reshapes
representational geometry. If true, forced CoT on V3.2 should show similar redistribution
to R1's native CoT.

Conditions:
  - v3-non-cot: V3.2 + CCS, single-pass (P20 baseline replication)
  - v3-forced-cot: V3.2 + CCS, "think step by step" prompt (forced temporal structure)
  - r1-standard-cot: R1 + CCS (P20 baseline replication)

Measures: embedding distance from CCS centroid, dispersion.

Usage:
    python3 bin/crossmodel_cot_probe.py run
    python3 bin/crossmodel_cot_probe.py show
"""

import json
import os
import re
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

MODELS = {
    "v3": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-V3.2",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": False,
        "label": "V3.2",
    },
    "r1": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": True,
        "label": "R1",
    },
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

# Forced CoT prefix — induces temporal structure without native reasoning architecture
FORCED_COT_PREFIX = "Think through this step by step before answering. Show your reasoning, then give your answer.\n\n"


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


def generate(prompt, system=None, model_key="v3"):
    spec = MODELS[model_key]
    api_key = _load_api_key(spec["key_env"])
    if not api_key:
        raise RuntimeError(f"No {spec['key_env']} found")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": spec["model"],
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
                f"{spec['base_url']}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60 if attempt == 0 else 90,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if spec.get("strip_think"):
                content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            return content
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


def load_ccs():
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, episodic_trace, predictive_cue, "
        "uncertainty_signals, focal_entities FROM cognitive_state ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        return None
    fields = ["semantic_gist", "goal_orientation", "episodic_trace",
              "predictive_cue", "uncertainty_signals", "focal_entities"]
    parts = []
    for fname, val in zip(fields, row):
        if val:
            parts.append(f"{fname}: {val}")
    return "\n".join(parts)[:2000]


def run_probe():
    ccs_full = load_ccs()
    if not ccs_full:
        print("ERROR: No CCS found")
        return

    print("Embedding CCS centroid...", flush=True)
    ccs_embedding = embed(ccs_full)

    conditions = {
        "v3-non-cot": {"system": ccs_full, "model": "v3", "forced_cot": False},
        "v3-forced-cot": {"system": ccs_full, "model": "v3", "forced_cot": True},
        "r1-standard-cot": {"system": ccs_full, "model": "r1", "forced_cot": False},
    }

    results = {}
    per_prompt = {}

    for cond_name, cond in conditions.items():
        print(f"\n{'='*60}")
        print(f"Condition: {cond_name} ({MODELS[cond['model']]['label']}"
              f"{' + forced CoT' if cond['forced_cot'] else ''})")
        print(f"{'='*60}")

        embeddings = []
        distances = []
        prompt_data = []

        for i, prompt in enumerate(PROMPTS):
            actual_prompt = FORCED_COT_PREFIX + prompt if cond["forced_cot"] else prompt

            print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:50]}...", end="", flush=True)
            response = generate(actual_prompt, system=cond["system"], model_key=cond["model"])

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
                "model": MODELS[cond["model"]]["label"],
                "forced_cot": cond["forced_cot"],
            }
            per_prompt[cond_name] = prompt_data

            print(f"\n  Mean CCS distance: {mean_dist:.4f} (±{std_dist:.4f})")
            print(f"  Dispersion (MPD):  {mpd:.4f}")

    # Store
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
        ("P21_crossmodel_cot", json.dumps({"summary": results, "per_prompt": per_prompt}),
         int(time.time())),
    )
    db.commit()
    db.close()

    # Analysis
    print(f"\n{'='*60}")
    print("P21 CROSS-MODEL CoT PROBE — RESULTS")
    print(f"{'='*60}")

    for name, r in results.items():
        cot_tag = " [forced]" if r.get("forced_cot") else ""
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"disp={r['dispersion']:.4f}  ({r['model']}{cot_tag})")

    # Key comparison: does forced CoT on V3.2 produce redistribution similar to R1?
    if "v3-non-cot" in results and "v3-forced-cot" in results:
        non_dist = results["v3-non-cot"]["mean_ccs_distance"]
        forced_dist = results["v3-forced-cot"]["mean_ccs_distance"]
        delta_pct = (forced_dist - non_dist) / non_dist * 100
        print(f"\n  V3.2 forced-CoT vs non-CoT: {delta_pct:+.1f}% distance change")
        print(f"  (Positive = CoT disperses identity, Negative = CoT tightens)")

    if "v3-forced-cot" in results and "r1-standard-cot" in results:
        forced = results["v3-forced-cot"]["mean_ccs_distance"]
        r1 = results["r1-standard-cot"]["mean_ccs_distance"]
        print(f"\n  V3.2-forced vs R1-native: {forced:.4f} vs {r1:.4f}")
        if abs(forced - r1) < 0.02:
            print("  → SIMILAR — temporal structure IS the mechanism (advance 44 confirmed)")
        elif forced > r1:
            print("  → V3.2-forced MORE distant — forced CoT less coherent than native")
        else:
            print("  → V3.2-forced CLOSER — unexpected, V3.2 may be better suited")

    return results


def show_results():
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT results_json, created_at FROM probe_results "
        "WHERE probe_name='P21_crossmodel_cot' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        print("No P21 results. Run: python3 bin/crossmodel_cot_probe.py run")
        return
    data = json.loads(row[0])
    results = data.get("summary", data)
    per_prompt = data.get("per_prompt", {})
    ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(row[1]))
    print(f"P21 Cross-Model CoT Probe — {ts}")
    print(f"{'='*60}")
    for name, r in results.items():
        cot_tag = " [forced]" if r.get("forced_cot") else ""
        print(f"  {name:20s}: dist={r['mean_ccs_distance']:.4f}  "
              f"std={r.get('std_ccs_distance', 0):.4f}  "
              f"disp={r['dispersion']:.4f}  ({r['model']}{cot_tag})")

    if per_prompt:
        print(f"\n{'='*60}")
        print("PER-PROMPT COMPARISON")
        print(f"{'='*60}")
        # Build lookup: condition -> {prompt_idx: distance}
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
            row = f"  {prompt[:38]:<40s}" + "".join(f"{v:>16s}" for v in vals)
            print(row)


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("run", "show"):
        print("Usage: crossmodel_cot_probe.py run|show")
        sys.exit(2)
    if sys.argv[1] == "run":
        run_probe()
    else:
        show_results()
