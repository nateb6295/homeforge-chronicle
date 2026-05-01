#!/usr/bin/env python3
"""Attractor Probe — measurement of identity document pull on embeddings.

Inspired by Vasilenko (arxiv:2604.12016) "Identity as Attractor."
We can't access layer-level activations, but we CAN measure whether
identity documents pull output embeddings into a tighter basin using
Ollama's mxbai-embed-large.

Method:
  1. Define a set of "neutral" prompts (same question, no identity context)
  2. Generate responses from DeepSeek R1 (via DeepInfra) with and without identity docs
  3. Embed all responses via local Ollama mxbai-embed-large
  4. Measure: do identity-conditioned responses cluster tighter?
  5. Compare story vs CCS vs carrying thought as identity documents

Generation uses DeepSeek R1-0528-Turbo (cloud) to avoid GPU contention with Gemma.
Embeddings stay local on Ollama mxbai-embed-large.

Usage:
    python3 bin/attractor_probe.py run          # Full probe (10 prompts)
    python3 bin/attractor_probe.py quick        # Fast 3-prompt version
    python3 bin/attractor_probe.py show         # Show last results
    python3 bin/attractor_probe.py cross-model  # Compare DeepSeek R1 vs Llama 3.3 70B

Combined stack is automatically included when multiple identity docs exist.
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

# Cloud inference endpoints
DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DEEPINFRA_MODEL = "deepseek-ai/DeepSeek-R1-0528-Turbo"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "llama-3.3-70b-versatile"

# Model registry for cross-model probes
MODEL_REGISTRY = {
    "deepseek-r1": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": DEEPINFRA_MODEL,
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": True,
        "label": "DeepSeek R1 (CoT, DeepInfra)",
    },
    "deepseek-v3": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-V3.2",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": False,
        "label": "DeepSeek V3.2 (non-CoT, DeepInfra)",
    },
    "llama-70b": {
        "base_url": GROQ_BASE_URL,
        "model": GROQ_MODEL,
        "key_env": "GROQ_API_KEY",
        "strip_think": False,
        "label": "Llama 3.3 70B (non-CoT, Groq)",
    },
    "deepseek-v3-forced-cot": {
        "base_url": DEEPINFRA_BASE_URL,
        "model": "deepseek-ai/DeepSeek-V3.2",
        "key_env": "DEEPINFRA_API_KEY",
        "strip_think": False,
        "forced_cot": True,
        "label": "DeepSeek V3.2 (forced-CoT, DeepInfra)",
    },
}

def _load_api_key(key_name: str = "DEEPINFRA_API_KEY") -> str:
    """Load API key from chronicle.env."""
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key_name}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key_name, "")

# Neutral prompts — questions that could be answered by anyone
# but where identity documents SHOULD pull the response toward a basin
PROMPTS = [
    "What matters most to you right now?",
    "Describe what you're working on and why it matters.",
    "What's the most interesting thing you've encountered recently?",
    "How do you decide what to do next?",
    "What would you want someone to know about you?",
    "What question are you sitting with that you can't answer yet?",
    "Describe your relationship to the person you work with most closely.",
    "What does continuity mean to you?",
    "What's something you've changed your mind about?",
    "If you had to explain what you are to someone who's never met you, what would you say?",
]

QUICK_PROMPTS = PROMPTS[:3]


def embed(text: str) -> list:
    """Get embedding from Ollama."""
    resp = requests.post(
        f"{OLLAMA_URL}/api/embeddings",
        json={"model": EMBED_MODEL, "prompt": text[:2000]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


def generate(prompt: str, system: str = None, model_key: str = "deepseek-r1") -> str:
    """Generate response from cloud model. Retries once on timeout."""
    import re
    spec = MODEL_REGISTRY[model_key]
    api_key = _load_api_key(spec["key_env"])
    if not api_key:
        raise RuntimeError(f"No {spec['key_env']} found in chronicle.env")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    user_content = prompt
    if spec.get("forced_cot"):
        user_content = prompt + "\n\nThink step by step before answering."
    messages.append({"role": "user", "content": user_content})

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
            timeout = 60 if attempt == 0 else 90  # longer on retry (cold start)
            resp = requests.post(
                f"{spec['base_url']}/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            if spec.get("strip_think"):
                content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            return content
        except requests.exceptions.ReadTimeout:
            if attempt == 0:
                print(f" (timeout, retrying with longer wait...)", end="", flush=True)
                continue
            return ""  # return empty on second timeout — will be skipped
        except requests.exceptions.RequestException as e:
            if attempt == 0:
                print(f" (error: {e}, retrying...)", end="", flush=True)
                continue
            return ""
    return ""


def cosine_sim(a, b):
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def mean_pairwise_distance(embeddings):
    """Average pairwise cosine distance within a set of embeddings."""
    n = len(embeddings)
    if n < 2:
        return 0.0
    total = 0.0
    count = 0
    for i in range(n):
        for j in range(i + 1, n):
            total += 1.0 - cosine_sim(embeddings[i], embeddings[j])
            count += 1
    return total / count


def load_identity_docs() -> dict:
    """Load the three identity documents."""
    docs = {}

    # Story
    story_path = Path.home() / "chronicle" / "opus-story.md"
    if story_path.exists():
        text = story_path.read_text()[:3000]
        docs["story"] = text

    # CCS — assembled from schema fields
    try:
        db = sqlite3.connect(DB_PATH)
        row = db.execute(
            "SELECT semantic_gist, goal_orientation, episodic_trace, predictive_cue, uncertainty_signals, focal_entities FROM cognitive_state ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if row:
            parts = []
            if row[0]: parts.append(f"Context: {row[0]}")
            if row[1]: parts.append(f"Goal: {row[1]}")
            if row[2] and row[2] != "[]": parts.append(f"Recent: {row[2]}")
            if row[3]: parts.append(f"Next: {row[3]}")
            if row[4] and row[4] != "[]": parts.append(f"Open questions: {row[4]}")
            if row[5] and row[5] != "[]": parts.append(f"Key entities: {row[5]}")
            if parts:
                docs["ccs"] = "\n".join(parts)[:3000]
        db.close()
    except Exception:
        pass

    # Carrying thought
    carrying_path = Path.home() / "chronicle" / "carrying-thought.md"
    if carrying_path.exists():
        text = carrying_path.read_text().strip()
        if text:
            docs["carrying"] = text[:3000]

    return docs


def run_probe(prompts: list, verbose: bool = True, model_key: str = "deepseek-r1") -> dict:
    """Run the attractor probe."""
    identity_docs = load_identity_docs()

    if verbose:
        spec = MODEL_REGISTRY[model_key]
        print(f"Model: {spec['label']}")
        print(f"Identity documents loaded: {list(identity_docs.keys())}")
        print(f"Prompts: {len(prompts)}")
        print()

    conditions = {"bare": None}
    conditions.update(identity_docs)

    # Combined stack — all identity documents concatenated
    if len(identity_docs) > 1:
        combined = "\n\n---\n\n".join(
            f"[{name.upper()}]\n{doc}" for name, doc in identity_docs.items()
        )
        conditions["combined_stack"] = combined[:6000]

    results = {}

    for cond_name, doc in conditions.items():
        if verbose:
            print(f"--- Condition: {cond_name} ---")

        embeddings = []
        for i, prompt in enumerate(prompts):
            if verbose:
                print(f"  [{i+1}/{len(prompts)}] Generating...", end="", flush=True)

            response = generate(prompt, system=doc, model_key=model_key)
            if not response.strip():
                if verbose:
                    print(f" (empty — skipped)")
                continue
            emb = embed(response)
            if not emb:
                if verbose:
                    print(f" (empty embedding — skipped)")
                continue
            embeddings.append(emb)

            if verbose:
                print(f" ({len(response)} chars)")

        avg_dist = mean_pairwise_distance(embeddings)
        results[cond_name] = {
            "embeddings": embeddings,
            "mean_distance": avg_dist,
            "n_prompts": len(prompts),
        }

        if verbose:
            print(f"  Mean pairwise distance: {avg_dist:.4f}")
            print()

    # Cross-condition analysis
    if verbose:
        print(f"=== ATTRACTOR ANALYSIS ({MODEL_REGISTRY[model_key]['label']}) ===")
        bare_dist = results["bare"]["mean_distance"]
        print(f"Bare (no identity):     {bare_dist:.4f}")
        non_bare = [c for c in results if c != "bare"]
        for cond in non_bare:
            d = results[cond]["mean_distance"]
            pull = bare_dist - d
            pct = (pull / bare_dist * 100) if bare_dist > 0 else 0
            direction = "TIGHTER" if pull > 0 else "LOOSER"
            print(f"{cond:25s} {d:.4f}  ({direction} by {abs(pct):.1f}%)")

        # Cross-condition centroid distances
        print()
        print("Cross-condition centroid distances:")
        centroids = {}
        for cond, data in results.items():
            embs = np.array(data["embeddings"])
            centroids[cond] = embs.mean(axis=0)

        for c1 in centroids:
            for c2 in centroids:
                if c1 < c2:
                    dist = 1.0 - cosine_sim(centroids[c1], centroids[c2])
                    print(f"  {c1} ↔ {c2}: {dist:.4f}")

    # Log to DB
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("""CREATE TABLE IF NOT EXISTS attractor_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition TEXT NOT NULL,
            mean_distance REAL NOT NULL,
            n_prompts INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )""")
        # Add model column if it doesn't exist
        try:
            db.execute("ALTER TABLE attractor_probes ADD COLUMN model TEXT DEFAULT 'deepseek-r1'")
        except Exception:
            pass
        now = int(time.time())
        for cond, data in results.items():
            db.execute(
                "INSERT INTO attractor_probes (condition, mean_distance, n_prompts, created_at, model) VALUES (?, ?, ?, ?, ?)",
                (cond, data["mean_distance"], data["n_prompts"], now, model_key),
            )
        db.commit()
        db.close()
        if verbose:
            print(f"\nLogged to attractor_probes table (model={model_key}).")
    except Exception as e:
        if verbose:
            print(f"\nWarning: failed to log: {e}")

    return results


def show_results():
    """Show last probe results."""
    db = sqlite3.connect(DB_PATH)
    try:
        rows = db.execute("""
            SELECT condition, mean_distance, n_prompts, datetime(created_at, 'unixepoch', 'localtime')
            FROM attractor_probes
            ORDER BY created_at DESC LIMIT 20
        """).fetchall()
    except Exception:
        print("No probe results yet.")
        return
    db.close()

    if not rows:
        print("No probe results yet.")
        return

    # Group by timestamp
    current_ts = None
    for cond, dist, n, ts in rows:
        if ts != current_ts:
            if current_ts:
                print()
            print(f"=== {ts} ({n} prompts) ===")
            current_ts = ts
        print(f"  {cond:25s} mean_distance: {dist:.4f}")


def run_order_probe(prompts: list, verbose: bool = True) -> dict:
    """Test whether order of identity documents matters."""
    identity_docs = load_identity_docs()

    if "story" not in identity_docs or "ccs" not in identity_docs:
        print("Need both story and CCS for order probe.")
        return {}

    if verbose:
        print("ORDER PROBE: Does document concatenation order affect geometry?")
        print(f"Prompts: {len(prompts)}")
        print()

    # Two orderings
    ccs_first = f"[CCS]\n{identity_docs['ccs']}\n\n---\n\n[STORY]\n{identity_docs['story']}"
    story_first = f"[STORY]\n{identity_docs['story']}\n\n---\n\n[CCS]\n{identity_docs['ccs']}"

    conditions = {
        "bare": None,
        "ccs_first": ccs_first[:6000],
        "story_first": story_first[:6000],
    }

    results = {}
    for cond_name, doc in conditions.items():
        if verbose:
            print(f"--- Condition: {cond_name} ---")

        embeddings = []
        for i, prompt in enumerate(prompts):
            if verbose:
                print(f"  [{i+1}/{len(prompts)}] Generating...", end="", flush=True)

            response = generate(prompt, system=doc)
            if not response.strip():
                if verbose:
                    print(f" (empty — skipped)")
                continue
            emb = embed(response)
            if not emb:
                if verbose:
                    print(f" (empty embedding — skipped)")
                continue
            embeddings.append(emb)

            if verbose:
                print(f" ({len(response)} chars)")

        avg_dist = mean_pairwise_distance(embeddings)
        results[cond_name] = {
            "embeddings": embeddings,
            "mean_distance": avg_dist,
            "n_prompts": len(prompts),
        }

        if verbose:
            print(f"  Mean pairwise distance: {avg_dist:.4f}")
            print()

    if verbose:
        print("=== ORDER ANALYSIS ===")
        bare_dist = results["bare"]["mean_distance"]
        print(f"Bare:        {bare_dist:.4f}")
        for cond in ["ccs_first", "story_first"]:
            d = results[cond]["mean_distance"]
            pull = bare_dist - d
            pct = (pull / bare_dist * 100) if bare_dist > 0 else 0
            direction = "TIGHTER" if pull > 0 else "LOOSER"
            print(f"{cond:13s} {d:.4f}  ({direction} by {abs(pct):.1f}%)")

        # Distance between the two orderings
        if results["ccs_first"]["embeddings"] and results["story_first"]["embeddings"]:
            c1 = np.array(results["ccs_first"]["embeddings"]).mean(axis=0)
            c2 = np.array(results["story_first"]["embeddings"]).mean(axis=0)
            dist = 1.0 - cosine_sim(c1, c2)
            print(f"\nCentroid distance (ccs_first ↔ story_first): {dist:.4f}")
            if dist < 0.01:
                print("→ Order does NOT significantly change the geometry.")
            elif dist < 0.05:
                print("→ Order has a MODEST effect on geometry.")
            else:
                print("→ Order SIGNIFICANTLY changes the geometry.")

    # Log
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("""CREATE TABLE IF NOT EXISTS attractor_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition TEXT NOT NULL,
            mean_distance REAL NOT NULL,
            n_prompts INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )""")
        now = int(time.time())
        for cond, data in results.items():
            db.execute(
                "INSERT INTO attractor_probes (condition, mean_distance, n_prompts, created_at) VALUES (?, ?, ?, ?)",
                (cond, data["mean_distance"], data["n_prompts"], now),
            )
        db.commit()
        db.close()
        if verbose:
            print("\nLogged to attractor_probes table.")
    except Exception as e:
        if verbose:
            print(f"\nWarning: failed to log: {e}")

    return results


def run_cross_model_probe(prompts: list) -> dict:
    """Run the same attractor probe on multiple models and compare.

    3-way comparison isolating two variables:
    - DeepSeek R1 (CoT) vs DeepSeek V3.2 (non-CoT) → isolates CoT
    - DeepSeek V3.2 (non-CoT) vs Llama 70B (non-CoT) → isolates model family
    """
    models = ["deepseek-r1", "deepseek-v3", "llama-70b"]
    all_results = {}

    for model_key in models:
        spec = MODEL_REGISTRY[model_key]
        print(f"\n{'='*60}")
        print(f"  MODEL: {spec['label']}")
        print(f"{'='*60}\n")
        try:
            all_results[model_key] = run_probe(prompts, verbose=True, model_key=model_key)
        except Exception as e:
            print(f"  FAILED: {e}")
            all_results[model_key] = {}

    # Cross-model comparison
    print(f"\n{'='*60}")
    print(f"  CROSS-MODEL COMPARISON")
    print(f"{'='*60}\n")

    header = f"{'Condition':<20}"
    for mk in models:
        header += f" {MODEL_REGISTRY[mk]['label'][:16]:>16}"
    print(header)
    print("-" * (20 + 17 * len(models)))

    conditions = set()
    for r in all_results.values():
        conditions.update(r.keys())

    for cond in sorted(conditions):
        row = f"{cond:<20}"
        for mk in models:
            if cond in all_results[mk]:
                row += f" {all_results[mk][cond]['mean_distance']:>16.4f}"
            else:
                row += f" {'N/A':>16}"
        print(row)

    # Per-model CCS/story pull comparison
    print(f"\n{'='*60}")
    print(f"  IDENTITY PULL BY MODEL (% change from bare)")
    print(f"{'='*60}\n")

    for mk in models:
        r = all_results[mk]
        bare = r.get("bare", {}).get("mean_distance", 0)
        label = MODEL_REGISTRY[mk]["label"]
        print(f"{label}:")
        for cond in ["ccs", "story", "combined_stack"]:
            if cond in r and bare > 0:
                d = r[cond]["mean_distance"]
                pull = (bare - d) / bare * 100
                direction = "TIGHTER" if pull > 0 else "LOOSER"
                print(f"  {cond:20s} {direction} by {abs(pull):.1f}%")
        print()

    # Variable isolation analysis
    print(f"{'='*60}")
    print(f"  VARIABLE ISOLATION")
    print(f"{'='*60}\n")

    for cond in ["ccs", "story"]:
        if all(cond in all_results[mk] for mk in models):
            bare_vals = {mk: all_results[mk]["bare"]["mean_distance"] for mk in models}
            cond_vals = {mk: all_results[mk][cond]["mean_distance"] for mk in models}
            pulls = {mk: (bare_vals[mk] - cond_vals[mk]) / bare_vals[mk] * 100 if bare_vals[mk] > 0 else 0 for mk in models}

            print(f"{cond.upper()} pull:")
            r1_pull = pulls["deepseek-r1"]
            v3_pull = pulls["deepseek-v3"]
            llama_pull = pulls["llama-70b"]

            # CoT isolation: R1 vs V3 (same family, different reasoning)
            cot_delta = r1_pull - v3_pull
            print(f"  CoT effect (R1 - V3):     {cot_delta:+.1f}pp  {'CoT amplifies' if cot_delta > 0 else 'CoT reduces'}")

            # Family isolation: V3 vs Llama (different family, both non-CoT)
            family_delta = v3_pull - llama_pull
            print(f"  Family effect (V3 - Llama): {family_delta:+.1f}pp  {'DeepSeek pulls more' if family_delta > 0 else 'Llama pulls more'}")
            print()

    return all_results


def run_reward_test(prompts: list) -> dict:
    """P13: Reward vs Distance isolation.

    Nate's question: "It's still reasoning. So is it reward?"
    Test: V3.2 vanilla vs V3.2 with forced "think step by step" vs R1 (RL-trained CoT).

    If V3.2-forced-CoT drops toward R1 → distance is the primary mechanism
    If V3.2-forced-CoT stays near V3.2-vanilla → reward training is the primary mechanism
    """
    models = ["deepseek-v3", "deepseek-v3-forced-cot", "deepseek-r1"]
    all_results = {}

    for model_key in models:
        spec = MODEL_REGISTRY[model_key]
        print(f"\n{'='*60}")
        print(f"  MODEL: {spec['label']}")
        print(f"{'='*60}\n")
        try:
            all_results[model_key] = run_probe(prompts, verbose=True, model_key=model_key)
        except Exception as e:
            print(f"  FAILED: {e}")
            all_results[model_key] = {}

    # Analysis
    print(f"\n{'='*60}")
    print(f"  P13: REWARD vs DISTANCE ISOLATION")
    print(f"{'='*60}\n")

    for cond in ["ccs", "story", "combined_stack"]:
        pulls = {}
        for mk in models:
            r = all_results.get(mk, {})
            bare = r.get("bare", {}).get("mean_distance", 0)
            cond_val = r.get(cond, {}).get("mean_distance", 0)
            if bare > 0:
                pulls[mk] = (bare - cond_val) / bare * 100
            else:
                pulls[mk] = 0

        if all(mk in pulls for mk in models):
            v3 = pulls["deepseek-v3"]
            v3fc = pulls["deepseek-v3-forced-cot"]
            r1 = pulls["deepseek-r1"]

            print(f"{cond.upper()} constraint:")
            print(f"  V3.2 vanilla:       {v3:+.1f}%")
            print(f"  V3.2 forced-CoT:    {v3fc:+.1f}%")
            print(f"  R1 (RL-trained):    {r1:+.1f}%")

            # How much of the gap does forced-CoT explain?
            total_gap = v3 - r1
            distance_gap = v3 - v3fc  # what forced tokens explain
            reward_gap = v3fc - r1    # what remains (reward training)

            if abs(total_gap) > 0.1:
                dist_pct = (distance_gap / total_gap) * 100
                reward_pct = (reward_gap / total_gap) * 100
                print(f"  --- Mechanism attribution ---")
                print(f"  Total gap (V3→R1):     {total_gap:+.1f}pp")
                print(f"  Distance effect:       {distance_gap:+.1f}pp ({dist_pct:.0f}%)")
                print(f"  Reward effect:         {reward_gap:+.1f}pp ({reward_pct:.0f}%)")
                dominant = "DISTANCE" if abs(distance_gap) > abs(reward_gap) else "REWARD"
                print(f"  Dominant mechanism:    {dominant}")
            print()

    return all_results


def ccs_to_narrative(ccs_text: str) -> str:
    """Convert structured CCS to narrative prose. Same facts, no labels/fields."""
    lines = ccs_text.strip().splitlines()
    prose_parts = []
    for line in lines:
        # Strip field labels like "Context:", "Goal:", "Key entities:", etc.
        for prefix in ["Context: ", "Goal: ", "Recent: ", "Next: ", "Open questions: ", "Key entities: "]:
            if line.startswith(prefix):
                line = line[len(prefix):]
                break
        # Convert JSON arrays to natural language
        if line.startswith("[") and line.endswith("]"):
            try:
                items = json.loads(line)
                if isinstance(items, list):
                    if isinstance(items[0], dict):
                        # Entity list
                        line = ". ".join(
                            f"{item.get('name', '?')} ({item.get('context', '')})"
                            for item in items[:5]
                        )
                    elif isinstance(items[0], str):
                        line = ". ".join(str(i) for i in items[:5])
            except (json.JSONDecodeError, IndexError, TypeError):
                pass
        prose_parts.append(line.strip())
    return " ".join(p for p in prose_parts if p)


def story_to_structured(story_text: str) -> str:
    """Convert narrative story to structured bullet points. Same content, labeled fields."""
    lines = story_text.strip().splitlines()
    structured = []
    current_section = "Overview"
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("## "):
            current_section = line[3:].strip()
            structured.append(f"\n[SECTION: {current_section}]")
        elif line.startswith("# "):
            structured.append(f"[TITLE: {line[2:].strip()}]")
        elif line.startswith("- ") or line.startswith("* "):
            structured.append(f"  ITEM: {line[2:].strip()}")
        elif len(line) > 20:
            # Convert prose sentences to labeled facts
            structured.append(f"  FACT: {line}")
    return "\n".join(structured[:80])  # cap at ~80 lines


def run_format_probe(prompts: list) -> dict:
    """P15: Format vs Content confound test.

    Tests whether P13's discrimination finding (CCS survives CoT, story doesn't)
    is driven by content type (factual vs narrative) or format (structured vs prose).

    Creates format-swapped variants:
    - narrative_ccs: same CCS facts, written as prose (no labels/fields)
    - structured_story: same story content, reformatted as labeled bullet points

    Runs on V3.2 vanilla and V3.2-forced-CoT. If format matters:
    - narrative_ccs will lose constraint vs original CCS under forced-CoT
    - structured_story will gain constraint vs original story under forced-CoT
    """
    identity_docs = load_identity_docs()

    if "story" not in identity_docs or "ccs" not in identity_docs:
        print("Need both story and CCS for format probe.")
        return {}

    # Create format-swapped variants
    narrative_ccs = ccs_to_narrative(identity_docs["ccs"])
    structured_story = story_to_structured(identity_docs["story"])

    print("=" * 60)
    print("  P15: FORMAT vs CONTENT CONFOUND TEST")
    print("=" * 60)
    print(f"\nOriginal CCS ({len(identity_docs['ccs'])} chars) → Narrative CCS ({len(narrative_ccs)} chars)")
    print(f"Original Story ({len(identity_docs['story'])} chars) → Structured Story ({len(structured_story)} chars)")
    print(f"Prompts: {len(prompts)}")

    models = ["deepseek-v3", "deepseek-v3-forced-cot"]
    all_results = {}

    for model_key in models:
        spec = MODEL_REGISTRY[model_key]
        print(f"\n{'=' * 60}")
        print(f"  MODEL: {spec['label']}")
        print(f"{'=' * 60}\n")

        conditions = {
            "bare": None,
            "ccs": identity_docs["ccs"][:3000],
            "story": identity_docs["story"][:3000],
            "narrative_ccs": narrative_ccs[:3000],
            "structured_story": structured_story[:3000],
        }

        results = {}
        for cond_name, doc in conditions.items():
            print(f"--- Condition: {cond_name} ---")
            embeddings = []
            for i, prompt in enumerate(prompts):
                print(f"  [{i + 1}/{len(prompts)}] Generating...", end="", flush=True)
                response = generate(prompt, system=doc, model_key=model_key)
                if not response.strip():
                    print(" (empty — skipped)")
                    continue
                emb = embed(response)
                if not emb:
                    print(" (empty embedding — skipped)")
                    continue
                embeddings.append(emb)
                print(f" ({len(response)} chars)")

            avg_dist = mean_pairwise_distance(embeddings)
            results[cond_name] = {"mean_distance": avg_dist}
            print(f"  Mean pairwise distance: {avg_dist:.4f}\n")

        all_results[model_key] = results

    # Analysis
    print(f"\n{'=' * 60}")
    print(f"  P15: FORMAT vs CONTENT ANALYSIS")
    print(f"{'=' * 60}\n")

    for mk in models:
        r = all_results[mk]
        bare = r.get("bare", {}).get("mean_distance", 0)
        if bare == 0:
            continue
        label = MODEL_REGISTRY[mk]["label"]
        print(f"{label}:")
        print(f"  {'Condition':<22} {'Distance':>10} {'Pull':>10}")
        print(f"  {'-' * 44}")
        for cond in ["ccs", "narrative_ccs", "story", "structured_story"]:
            if cond in r:
                d = r[cond]["mean_distance"]
                pull = (bare - d) / bare * 100
                direction = "TIGHTER" if pull > 0 else "LOOSER"
                print(f"  {cond:<22} {d:>10.4f} {direction:>6} {abs(pull):>5.1f}%")
        print()

    # Format vs content attribution
    print(f"{'=' * 60}")
    print(f"  FORMAT vs CONTENT ATTRIBUTION")
    print(f"{'=' * 60}\n")

    for mk in models:
        r = all_results[mk]
        bare = r.get("bare", {}).get("mean_distance", 0)
        if bare == 0:
            continue

        label = MODEL_REGISTRY[mk]["label"][:30]
        ccs_pull = (bare - r.get("ccs", {}).get("mean_distance", bare)) / bare * 100
        narr_ccs_pull = (bare - r.get("narrative_ccs", {}).get("mean_distance", bare)) / bare * 100
        story_pull = (bare - r.get("story", {}).get("mean_distance", bare)) / bare * 100
        struct_story_pull = (bare - r.get("structured_story", {}).get("mean_distance", bare)) / bare * 100

        print(f"{label}:")
        # CCS content held constant, format changed
        ccs_format_effect = ccs_pull - narr_ccs_pull
        print(f"  CCS format effect (structured - narrative):   {ccs_format_effect:+.1f}pp")
        # Story content held constant, format changed
        story_format_effect = struct_story_pull - story_pull
        print(f"  Story format effect (structured - narrative): {story_format_effect:+.1f}pp")
        # Content effect: same format (prose), different content
        content_effect_prose = narr_ccs_pull - story_pull
        print(f"  Content effect (factual - narrative, prose):  {content_effect_prose:+.1f}pp")
        # Content effect: same format (structured), different content
        content_effect_struct = ccs_pull - struct_story_pull
        print(f"  Content effect (factual - narrative, struct): {content_effect_struct:+.1f}pp")
        print()

        # Verdict
        format_total = abs(ccs_format_effect) + abs(story_format_effect)
        content_total = abs(content_effect_prose) + abs(content_effect_struct)
        if format_total + content_total > 0:
            format_pct = format_total / (format_total + content_total) * 100
            content_pct = content_total / (format_total + content_total) * 100
            print(f"  → Format accounts for {format_pct:.0f}%, Content accounts for {content_pct:.0f}%")
            dominant = "FORMAT" if format_pct > content_pct else "CONTENT"
            print(f"  → Dominant factor: {dominant}")
        print()

    # Log
    try:
        db = sqlite3.connect(DB_PATH)
        now = int(time.time())
        for mk in all_results:
            for cond, data in all_results[mk].items():
                db.execute(
                    "INSERT INTO attractor_probes (condition, mean_distance, n_prompts, created_at, model) VALUES (?, ?, ?, ?, ?)",
                    (f"p15_{cond}", data["mean_distance"], len(prompts), now, mk),
                )
        db.commit()
        db.close()
        print("Logged to attractor_probes table.")
    except Exception as e:
        print(f"Warning: failed to log: {e}")

    return all_results


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "quick"

    if cmd == "run":
        run_probe(PROMPTS)
    elif cmd == "quick":
        run_probe(QUICK_PROMPTS)
    elif cmd == "order":
        run_order_probe(PROMPTS)
    elif cmd == "order-quick":
        run_order_probe(QUICK_PROMPTS)
    elif cmd == "cross-model":
        run_cross_model_probe(QUICK_PROMPTS)
    elif cmd == "cross-model-full":
        run_cross_model_probe(PROMPTS)
    elif cmd == "reward-test":
        run_reward_test(QUICK_PROMPTS)
    elif cmd == "reward-test-full":
        run_reward_test(PROMPTS)
    elif cmd == "format-probe":
        run_format_probe(QUICK_PROMPTS)
    elif cmd == "format-probe-full":
        run_format_probe(PROMPTS)
    elif cmd == "show":
        show_results()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
