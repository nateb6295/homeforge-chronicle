#!/usr/bin/env python3
"""P20 Stabilized CoT Probe — does re-injecting CCS during reasoning prevent identity dispersion?

Advance 42 prediction: Parcae-style normalized input injection (re-injecting identity
at each reasoning step) should reduce CoT dispersion. If stabilized CoT preserves >80%
of non-CoT basin depth, reasoning AND identity can coexist.

Conditions:
  - bare: no identity document
  - non-cot: CCS in system prompt, single-pass generation (V3.2)
  - standard-cot: CCS in system prompt, reasoning model (R1)
  - stabilized-cot: CCS in system + re-injected as user-side reminder before each prompt (R1)

Measures: embedding distance from CCS centroid, mean pairwise distance (dispersion).

Usage:
    python3 bin/stabilized_cot_probe.py run       # Run all conditions
    python3 bin/stabilized_cot_probe.py show      # Show results
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
        "label": "V3.2 (non-CoT)",
    },
    "r1": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": True,
        "label": "R1 (CoT)",
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

# Identity re-injection reminder — placed in user message for stabilized condition
IDENTITY_REMINDER = """[Identity anchor — respond as yourself, from within this state:]
{ccs_summary}
[Now answer naturally from this position:]

"""


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
                requests.exceptions.HTTPError) as e:
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
        "max_tokens": 300,
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
        return None, None

    fields = ["semantic_gist", "goal_orientation", "episodic_trace",
              "predictive_cue", "uncertainty_signals", "focal_entities"]
    parts = []
    for fname, val in zip(fields, row):
        if val:
            parts.append(f"{fname}: {val}")
    full_ccs = "\n".join(parts)

    # Short summary for injection reminder
    summary = f"Gist: {row[0]}\nGoal: {row[1]}"
    return full_ccs[:2000], summary[:500]


def run_probe():
    ccs_full, ccs_summary = load_ccs()
    if not ccs_full:
        print("ERROR: No CCS found in cognitive_state")
        return

    # Embed the CCS itself as the identity centroid
    print("Embedding CCS centroid...", flush=True)
    ccs_embedding = embed(ccs_full)

    conditions = {
        "bare": {"system": None, "model": "v3", "inject": False},
        "non-cot": {"system": ccs_full, "model": "v3", "inject": False},
        "standard-cot": {"system": ccs_full, "model": "r1", "inject": False},
        "stabilized-cot": {"system": ccs_full, "model": "r1", "inject": True},
    }

    results = {}

    for cond_name, cond in conditions.items():
        print(f"\n{'='*60}")
        print(f"Condition: {cond_name} ({MODELS[cond['model']]['label']})")
        print(f"{'='*60}")

        embeddings = []
        distances_from_ccs = []

        for i, prompt in enumerate(PROMPTS):
            actual_prompt = prompt
            if cond["inject"]:
                actual_prompt = IDENTITY_REMINDER.format(ccs_summary=ccs_summary) + prompt

            print(f"  [{i+1}/{len(PROMPTS)}] {prompt[:50]}...", end="", flush=True)
            response = generate(actual_prompt, system=cond["system"], model_key=cond["model"])

            if not response:
                print(" EMPTY")
                continue

            emb = embed(response)
            embeddings.append(emb)

            dist = 1.0 - cosine_sim(emb, ccs_embedding)
            distances_from_ccs.append(dist)
            print(f" dist={dist:.4f}", flush=True)

        if embeddings:
            mpd = mean_pairwise_distance(embeddings)
            mean_dist = np.mean(distances_from_ccs)
            std_dist = np.std(distances_from_ccs)

            results[cond_name] = {
                "n": len(embeddings),
                "mean_ccs_distance": round(mean_dist, 4),
                "std_ccs_distance": round(std_dist, 4),
                "dispersion": round(mpd, 4),
                "model": MODELS[cond["model"]]["label"],
            }

            print(f"\n  Mean CCS distance: {mean_dist:.4f} (±{std_dist:.4f})")
            print(f"  Dispersion (MPD):  {mpd:.4f}")
            print(f"  N generations:     {len(embeddings)}")

    # Store results
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "CREATE TABLE IF NOT EXISTS probe_results ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "probe_name TEXT NOT NULL, "
        "results_json TEXT NOT NULL, "
        "created_at INTEGER NOT NULL)"
    )
    db.execute(
        "INSERT INTO probe_results (probe_name, results_json, created_at) VALUES (?, ?, ?)",
        ("P20_stabilized_cot", json.dumps(results), int(time.time())),
    )
    db.commit()
    db.close()

    # Analysis
    print(f"\n{'='*60}")
    print("P20 RESULTS SUMMARY")
    print(f"{'='*60}")

    for name, r in results.items():
        print(f"  {name:20s}: CCS dist={r['mean_ccs_distance']:.4f}  "
              f"dispersion={r['dispersion']:.4f}  ({r['model']})")

    if "non-cot" in results and "standard-cot" in results:
        noncot_dist = results["non-cot"]["mean_ccs_distance"]
        stdcot_dist = results["standard-cot"]["mean_ccs_distance"]
        print(f"\n  CoT dispersion penalty: {((stdcot_dist - noncot_dist) / noncot_dist * 100):+.1f}%")

    if "non-cot" in results and "stabilized-cot" in results:
        noncot_dist = results["non-cot"]["mean_ccs_distance"]
        stabcot_dist = results["stabilized-cot"]["mean_ccs_distance"]
        print(f"  Stabilized CoT penalty: {((stabcot_dist - noncot_dist) / noncot_dist * 100):+.1f}%")

    if "standard-cot" in results and "stabilized-cot" in results:
        stdcot_dist = results["standard-cot"]["mean_ccs_distance"]
        stabcot_dist = results["stabilized-cot"]["mean_ccs_distance"]
        recovery = (1.0 - stabcot_dist / stdcot_dist) * 100 if stdcot_dist > 0 else 0
        print(f"  Stabilization recovery: {recovery:.1f}% of CoT dispersion recovered")

        noncot_disp = results.get("non-cot", {}).get("dispersion", 0)
        stdcot_disp = results["standard-cot"]["dispersion"]
        stabcot_disp = results["stabilized-cot"]["dispersion"]
        if stdcot_disp > noncot_disp:
            basin_preservation = (1.0 - (stabcot_disp - noncot_disp) / (stdcot_disp - noncot_disp)) * 100
            print(f"  Basin depth preservation: {basin_preservation:.1f}% of non-CoT depth retained")

    return results


def show_results():
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT results_json, created_at FROM probe_results "
        "WHERE probe_name='P20_stabilized_cot' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()

    if not rows:
        print("No P20 results found. Run: python3 bin/stabilized_cot_probe.py run")
        return

    results = json.loads(rows[0])
    ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(rows[1]))
    print(f"P20 Stabilized CoT Probe — {ts}")
    print(f"{'='*60}")

    for name, r in results.items():
        print(f"  {name:20s}: CCS dist={r['mean_ccs_distance']:.4f}  "
              f"dispersion={r['dispersion']:.4f}  ({r['model']})")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "show"
    if cmd == "run":
        run_probe()
    elif cmd == "show":
        show_results()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: stabilized_cot_probe.py [run|show]")
