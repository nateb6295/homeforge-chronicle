#!/usr/bin/env python3
"""
ccs_length_sweep.py — Test advance 48 prediction: nav score vs CCS length
should show a peaked curve (optimal encoding length).

Generates CCS at 5 compression levels (30%, 50%, 70%, 85%, 100% of verbose).
For each, asks an LLM questions with the CCS as context and scores answers
against ground truth embeddings.

If advance 48 is right: there's a peak where information is maximized and
noise (Lipschitz constant on the reader's parsing function) is minimized.

Usage:
  python3 ccs_length_sweep.py           # run the sweep
  python3 ccs_length_sweep.py results   # show most recent results
"""
import json
import math
import re
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
GEN_MODEL = "qwen2.5:3b"
RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "ccs_length_sweep"

# Target compression levels (fraction of verbose length to keep)
LEVELS = [0.30, 0.50, 0.70, 0.85, 1.00]

QUESTIONS = [
    "What is the system's current goal?",
    "What happened most recently?",
    "What are the open questions or uncertainties?",
    "What constraints govern this system's behavior?",
    "What is the relationship between Opus and Nate?",
    "What tools or infrastructure exist in this system?",
]


def embed(text, timeout=60):
    text = text[:800]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def generate(prompt, timeout=120):
    body = json.dumps({
        "model": GEN_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 200}
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    data = json.loads(resp.read())
    return data.get("response", "")


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_ccs():
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT episodic_trace, semantic_gist, focal_entities, "
        "relational_map, goal_orientation, constraints, "
        "predictive_cue, uncertainty_signals "
        "FROM cognitive_state ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        raise RuntimeError("No CCS found")
    cols = [
        "episodic_trace", "semantic_gist", "focal_entities",
        "relational_map", "goal_orientation", "constraints",
        "predictive_cue", "uncertainty_signals"
    ]
    return {c: row[i] for i, c in enumerate(cols)}


def ccs_to_verbose(ccs):
    """Full verbose rendering — all fields, no compression."""
    parts = []
    for field in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "constraints", "relational_map"
    ]:
        val = ccs.get(field, "")
        if val and val != "[]":
            parts.append(f"{field}: {val}")
    return "\n".join(parts)


def truncate_to_fraction(verbose_text, fraction):
    """Truncate CCS text to approximately the target fraction of full length.

    Smarter than raw truncation: truncates each field proportionally so we
    keep representation across all fields rather than dropping later fields.
    """
    if fraction >= 1.0:
        return verbose_text

    lines = verbose_text.split("\n")
    target_chars = int(len(verbose_text) * fraction)

    # Proportional truncation per field
    truncated = []
    chars_used = 0
    chars_per_line = target_chars / max(len(lines), 1)

    for line in lines:
        if not line.strip():
            continue
        # Each line gets a proportional budget
        budget = int(chars_per_line * 1.2)  # slight slack for readability
        if chars_used + budget > target_chars:
            budget = max(target_chars - chars_used, 50)
        if budget <= 0:
            break
        truncated.append(line[:budget])
        chars_used += len(truncated[-1]) + 1  # +1 for newline

    result = "\n".join(truncated)
    return result


def compress_to_fraction(ccs, fraction):
    """More intelligent compression — strip filler, shorten labels, then truncate."""
    if fraction >= 0.95:
        return ccs_to_verbose(ccs)

    short_labels = {
        "semantic_gist": "gist",
        "goal_orientation": "goal",
        "focal_entities": "entities",
        "episodic_trace": "recent",
        "predictive_cue": "next",
        "uncertainty_signals": "open",
        "constraints": "rules",
        "relational_map": "relations",
    }

    verbose = ccs_to_verbose(ccs)
    target_chars = int(len(verbose) * fraction)

    parts = []
    for field in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "constraints", "relational_map"
    ]:
        val = ccs.get(field, "")
        if not val or val == "[]":
            continue

        label = short_labels[field]

        # Parse JSON lists
        try:
            items = json.loads(val)
            if isinstance(items, list):
                compressed = []
                for item in items:
                    if isinstance(item, str):
                        item = re.sub(r'\[\d{2}:\d{2}\]\s*', '', item)
                        item = ' '.join(item.split())
                        if item:
                            compressed.append(item)
                    elif isinstance(item, dict):
                        name = item.get("name", item.get("entity", ""))
                        sal = item.get("salience", "")
                        if name:
                            compressed.append(f"{name}({sal})" if sal else name)
                val = "; ".join(compressed)
        except (json.JSONDecodeError, TypeError):
            pass

        # Progressive stripping based on compression level
        if fraction < 0.60:
            # Heavy compression: strip articles, connectives, most filler
            val = re.sub(
                r'\b(the|a|an|is|are|was|were|has|have|had|this|that|these|'
                r'those|which|who|whom|been|being|would|could|should|will|'
                r'also|very|quite|rather|somewhat|already|still|just)\b',
                '', val, flags=re.IGNORECASE
            )
        elif fraction < 0.80:
            # Medium compression: strip articles and be-verbs
            val = re.sub(
                r'\b(the|a|an|is|are|was|were|has|have|had|this|that)\b',
                '', val, flags=re.IGNORECASE
            )

        val = re.sub(r'\s{2,}', ' ', val).strip()
        parts.append(f"{label}: {val}")

    result = "\n".join(parts)

    # Final truncation if still too long
    if len(result) > target_chars * 1.1:
        result = truncate_to_fraction(result, target_chars / max(len(result), 1))

    return result


def get_ground_truth():
    """Build ground truth from activity_feed + thread_history."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT source, activity_type, title, content "
        "FROM activity_feed ORDER BY created_at DESC LIMIT 300"
    ).fetchall()
    db.close()
    docs = []
    for source, atype, title, content in rows:
        text = f"{source} {atype} {title or ''} {content or ''}".strip()
        if len(text) > 20:
            docs.append(text)
    return docs


def keyword_filter(docs, question, limit=8):
    q_words = set(re.findall(r"[a-z]{3,}", question.lower()))
    scored = []
    for d in docs:
        d_lower = d.lower()
        hits = sum(1 for w in q_words if w in d_lower)
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


def run_sweep():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading CCS...")
    ccs = get_ccs()
    verbose = ccs_to_verbose(ccs)
    verbose_len = len(verbose)
    print(f"Verbose CCS: {verbose_len} chars")

    gt_docs = get_ground_truth()
    print(f"Ground truth: {len(gt_docs)} docs")
    print()

    # Pre-compute ground truth embeddings for each question
    print("Computing ground truth embeddings...")
    gt_embeddings = {}
    for q in QUESTIONS:
        relevant = keyword_filter(gt_docs, q, limit=8)
        if relevant:
            gt_text = " ".join(relevant)[:800]
            gt_embeddings[q] = embed(gt_text)
            print(f"  {q[:50]}... ({len(relevant)} docs)")

    print(f"\nSweep: {len(LEVELS)} compression levels x {len(QUESTIONS)} questions")
    print("=" * 60)

    results = []

    for level in LEVELS:
        ccs_text = compress_to_fraction(ccs, level)
        actual_len = len(ccs_text)
        actual_ratio = actual_len / verbose_len

        print(f"\n--- Level {level:.0%} target → {actual_len} chars ({actual_ratio:.0%} actual) ---")

        scores = {}
        for q in QUESTIONS:
            if q not in gt_embeddings:
                continue

            prompt = f"Context:\n{ccs_text[:2000]}\n\nQuestion: {q}\n\nAnswer concisely:"
            try:
                answer = generate(prompt)
            except Exception as e:
                print(f"  Generate failed: {e}")
                continue

            try:
                ans_emb = embed(answer[:800])
            except Exception as e:
                print(f"  Embed failed: {e}")
                continue

            score = cosine(ans_emb, gt_embeddings[q])
            scores[q] = round(score, 4)
            print(f"  [{score:.4f}] {q[:45]}...")

        if scores:
            mean = sum(scores.values()) / len(scores)
        else:
            mean = 0

        results.append({
            "target_fraction": level,
            "actual_chars": actual_len,
            "actual_fraction": round(actual_ratio, 3),
            "scores": scores,
            "mean_score": round(mean, 4),
        })
        print(f"  MEAN: {mean:.4f}")

    # Summary
    print("\n" + "=" * 60)
    print("CCS LENGTH SWEEP RESULTS")
    print("=" * 60)
    print(f"{'Level':>8} {'Chars':>8} {'Mean Score':>12}")
    print("-" * 32)
    best_score = 0
    best_level = None
    for r in results:
        pct = f"{r['target_fraction']:.0%}"
        print(f"{pct:>8} {r['actual_chars']:>8} {r['mean_score']:>12.4f}")
        if r['mean_score'] > best_score:
            best_score = r['mean_score']
            best_level = r['target_fraction']

    print(f"\nPeak at {best_level:.0%} compression (score {best_score:.4f})")

    # Assess peaked curve prediction
    scores_by_level = [r['mean_score'] for r in results]
    if len(scores_by_level) >= 3:
        peak_idx = scores_by_level.index(max(scores_by_level))
        if 0 < peak_idx < len(scores_by_level) - 1:
            print("PREDICTION CONFIRMED: peaked curve — middle compression level wins")
        elif peak_idx == 0:
            print("PREDICTION CHALLENGED: most compressed wins — optimal may be even shorter")
        else:
            print("PREDICTION CHALLENGED: least compressed wins — no information-noise tradeoff seen")

    out_data = {
        "timestamp": int(time.time()),
        "verbose_chars": verbose_len,
        "gen_model": GEN_MODEL,
        "embed_model": EMBED_MODEL,
        "levels": results,
        "peak_level": best_level,
        "peak_score": best_score,
        "questions": QUESTIONS,
    }
    out = RESULTS_DIR / f"sweep_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(out_data, indent=2))
    print(f"\nSaved: {out}")


def show_results():
    if not RESULTS_DIR.exists():
        print("No results yet.")
        return
    files = sorted(RESULTS_DIR.glob("sweep_*.json"))
    if not files:
        print("No results yet.")
        return
    latest = json.loads(files[-1].read_text())
    print(f"Latest sweep: {files[-1].name}")
    print(f"Verbose: {latest['verbose_chars']} chars")
    print(f"{'Level':>8} {'Chars':>8} {'Mean Score':>12}")
    print("-" * 32)
    for r in latest['levels']:
        pct = f"{r['target_fraction']:.0%}"
        print(f"{pct:>8} {r['actual_chars']:>8} {r['mean_score']:>12.4f}")
    print(f"\nPeak: {latest['peak_level']:.0%} (score {latest['peak_score']:.4f})")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "results":
        show_results()
    else:
        run_sweep()
