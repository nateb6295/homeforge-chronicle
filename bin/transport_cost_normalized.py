#!/usr/bin/env python3
"""Transport cost normalization: per-token embedding distance for structural vs reflexive fields.

Tests whether the 0.03 gap between structural (0.136) and reflexive (0.108)
transport cost is a real rate difference or a dimensional artifact from
structural fields having more tokens.

If normalized costs converge → artifact. If gap persists → real rate difference.
"""

import json
import os
import sqlite3
import sys
import time
import requests
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"


def get_ccs_history():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    db.close()
    return rows


def serialize_structural(ccs):
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(g[:500])
    fe = ccs.get("focal_entities", [])
    if fe:
        ents = [f"{x.get('name','')} ({x.get('type','')}, {x.get('salience',0):.1f}): {x.get('context','')[:80]}"
                for x in fe[:10]]
        parts.append(" | ".join(ents))
    c = ccs.get("constraints", [])
    if c:
        parts.append("; ".join(str(x)[:100] for x in c[:5]))
    e = ccs.get("episodic_trace", [])
    if e:
        parts.append("; ".join(str(x)[:150] for x in e[:5]))
    rm = ccs.get("relational_map", {})
    if rm:
        arcs = [f"{k}: {v[:80]}" for k, v in list(rm.items())[:5]]
        parts.append("; ".join(arcs))
    return "\n".join(parts)


def serialize_reflexive(ccs):
    parts = []
    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(go[:300])
    pc = ccs.get("predictive_cue", "")
    if pc:
        parts.append(pc[:200])
    u = ccs.get("uncertainty_signals", [])
    if u:
        descs = [x.get("description", "")[:100] if isinstance(x, dict) else str(x)[:100] for x in u[:3]]
        parts.append("; ".join(descs))
    return "\n".join(parts)


def approx_tokens(text):
    return len(text.split())


def get_embedding(text, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text[:800]},
                timeout=120,
            )
            data = resp.json()
            if "embedding" in data:
                return np.array(data["embedding"])
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            print(f"  Embedding error: {e}", file=sys.stderr)
            return None


def cosine_distance(a, b):
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 1.0
    return 1.0 - (dot / norm)


def main():
    rows = get_ccs_history()
    n = len(rows)
    print(f"CCS history: {n} snapshots")

    structural_embs = []
    reflexive_embs = []
    structural_tokens = []
    reflexive_tokens = []
    timestamps = []
    ids = []
    skipped = 0

    for i, (rid, snap, ts, trigger) in enumerate(rows):
        ccs = json.loads(snap)
        s_text = serialize_structural(ccs)
        r_text = serialize_reflexive(ccs)

        s_tok = approx_tokens(s_text)
        r_tok = approx_tokens(r_text)

        if s_tok < 5 or r_tok < 5:
            skipped += 1
            continue

        s_emb = get_embedding(s_text)
        r_emb = get_embedding(r_text)

        if s_emb is not None and r_emb is not None:
            structural_embs.append(s_emb)
            reflexive_embs.append(r_emb)
            structural_tokens.append(s_tok)
            reflexive_tokens.append(r_tok)
            timestamps.append(ts)
            ids.append(rid)

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n} processed")

    ne = len(structural_embs)
    print(f"\n{ne}/{n} pairs embedded ({skipped} skipped for low token count)")

    if ne < 2:
        print("Not enough embeddings")
        return

    s_tokens_arr = np.array(structural_tokens)
    r_tokens_arr = np.array(reflexive_tokens)
    print(f"\nToken counts:")
    print(f"  Structural: mean={s_tokens_arr.mean():.1f}, std={s_tokens_arr.std():.1f}, "
          f"min={s_tokens_arr.min()}, max={s_tokens_arr.max()}")
    print(f"  Reflexive:  mean={r_tokens_arr.mean():.1f}, std={r_tokens_arr.std():.1f}, "
          f"min={r_tokens_arr.min()}, max={r_tokens_arr.max()}")
    print(f"  Ratio (structural/reflexive): {s_tokens_arr.mean() / r_tokens_arr.mean():.2f}x")

    # Raw transport costs
    s_raw = []
    r_raw = []
    for i in range(1, ne):
        s_raw.append(cosine_distance(structural_embs[i-1], structural_embs[i]))
        r_raw.append(cosine_distance(reflexive_embs[i-1], reflexive_embs[i]))

    s_raw_arr = np.array(s_raw)
    r_raw_arr = np.array(r_raw)

    print(f"\n{'='*60}")
    print("RAW TRANSPORT COSTS (replication check)")
    print(f"{'='*60}")
    print(f"  Structural: mean={s_raw_arr.mean():.4f}, std={s_raw_arr.std():.4f}")
    print(f"  Reflexive:  mean={r_raw_arr.mean():.4f}, std={r_raw_arr.std():.4f}")
    print(f"  Gap: {s_raw_arr.mean() - r_raw_arr.mean():.4f}")
    print(f"  Correlation: {np.corrcoef(s_raw_arr, r_raw_arr)[0,1]:.4f}")

    # Normalized: cost per token (average tokens of the two snapshots in each pair)
    s_norm = []
    r_norm = []
    for i in range(1, ne):
        s_avg_tok = (structural_tokens[i-1] + structural_tokens[i]) / 2.0
        r_avg_tok = (reflexive_tokens[i-1] + reflexive_tokens[i]) / 2.0
        s_norm.append(s_raw[i-1] / s_avg_tok * 100)  # per-100-tokens
        r_norm.append(r_raw[i-1] / r_avg_tok * 100)

    s_norm_arr = np.array(s_norm)
    r_norm_arr = np.array(r_norm)

    print(f"\n{'='*60}")
    print("NORMALIZED TRANSPORT COSTS (per 100 tokens)")
    print(f"{'='*60}")
    print(f"  Structural: mean={s_norm_arr.mean():.4f}, std={s_norm_arr.std():.4f}")
    print(f"  Reflexive:  mean={r_norm_arr.mean():.4f}, std={r_norm_arr.std():.4f}")
    print(f"  Gap: {s_norm_arr.mean() - r_norm_arr.mean():.4f}")

    raw_ratio = r_raw_arr.mean() / s_raw_arr.mean()
    norm_ratio = r_norm_arr.mean() / s_norm_arr.mean()
    print(f"\n  Raw ratio (reflexive/structural): {raw_ratio:.4f}")
    print(f"  Normalized ratio: {norm_ratio:.4f}")

    if norm_ratio > 0.95:
        verdict = "CONVERGED — gap was dimensional artifact"
    elif norm_ratio > raw_ratio:
        verdict = "PARTIALLY EXPLAINED — normalization closes some of the gap"
    else:
        verdict = "NOT EXPLAINED — gap persists or widens after normalization"
    print(f"\n  VERDICT: {verdict}")

    # Per-step comparison: which normalization direction shifts
    flipped = 0
    for i in range(len(s_raw)):
        raw_s_bigger = s_raw[i] > r_raw[i]
        norm_s_bigger = s_norm[i] > r_norm[i]
        if raw_s_bigger != norm_s_bigger:
            flipped += 1
    print(f"\n  Steps where normalization flips which field has higher cost: {flipped}/{len(s_raw)}")

    # Token variance correlation with cost
    s_tok_diffs = []
    r_tok_diffs = []
    for i in range(1, ne):
        s_tok_diffs.append(abs(structural_tokens[i] - structural_tokens[i-1]))
        r_tok_diffs.append(abs(reflexive_tokens[i] - reflexive_tokens[i-1]))

    s_tok_corr = np.corrcoef(s_tok_diffs, s_raw)[0,1]
    r_tok_corr = np.corrcoef(r_tok_diffs, r_raw)[0,1]
    print(f"\n  Token-change vs cost correlation:")
    print(f"    Structural: r={s_tok_corr:.4f}")
    print(f"    Reflexive:  r={r_tok_corr:.4f}")

    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "n_pairs": ne,
        "token_counts": {
            "structural_mean": float(s_tokens_arr.mean()),
            "structural_std": float(s_tokens_arr.std()),
            "reflexive_mean": float(r_tokens_arr.mean()),
            "reflexive_std": float(r_tokens_arr.std()),
            "ratio": float(s_tokens_arr.mean() / r_tokens_arr.mean()),
        },
        "raw": {
            "structural_mean": float(s_raw_arr.mean()),
            "reflexive_mean": float(r_raw_arr.mean()),
            "gap": float(s_raw_arr.mean() - r_raw_arr.mean()),
            "correlation": float(np.corrcoef(s_raw_arr, r_raw_arr)[0,1]),
        },
        "normalized_per_100tok": {
            "structural_mean": float(s_norm_arr.mean()),
            "reflexive_mean": float(r_norm_arr.mean()),
            "gap": float(s_norm_arr.mean() - r_norm_arr.mean()),
        },
        "raw_ratio": float(raw_ratio),
        "normalized_ratio": float(norm_ratio),
        "verdict": verdict,
        "flipped_steps": flipped,
        "token_cost_correlation": {
            "structural": float(s_tok_corr),
            "reflexive": float(r_tok_corr),
        },
        "per_step_raw_structural": [float(x) for x in s_raw],
        "per_step_raw_reflexive": [float(x) for x in r_raw],
        "per_step_norm_structural": [float(x) for x in s_norm],
        "per_step_norm_reflexive": [float(x) for x in r_norm],
        "per_step_structural_tokens": [int(x) for x in structural_tokens],
        "per_step_reflexive_tokens": [int(x) for x in reflexive_tokens],
    }
    outpath = os.path.expanduser("~/chronicle/data/transport_cost_normalized.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
