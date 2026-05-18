#!/usr/bin/env python3
"""Goal churn analysis — quantifies goal_orientation stability across CCS history.

For #324 (compositionality measurement): if goal_orientation shifts between two
CCS snapshots being compared, compositionality measurement is contaminated.
This script identifies which version pairs are safe to compare (same goal regime)
vs which straddle a goal shift.

Also measures: entity stickiness, gist drift, episodic turnover — the temporal
hypotheses named 2026-05-13.
"""
import json
import sqlite3
import sys
import time
import numpy as np
import requests

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
SHIFT_THRESHOLD = 0.05


def get_embedding(text):
    resp = requests.post(
        EMBED_URL,
        json={"model": "nomic-embed-text", "prompt": text[:4000]},
        timeout=60,
    )
    resp.raise_for_status()
    return np.array(resp.json()["embedding"])


def cosine_sim(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def load_history(limit=50):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    db.close()
    versions = []
    for id_, snap_str in rows:
        try:
            snap = json.loads(snap_str)
            snap["_db_id"] = id_
            versions.append(snap)
        except (json.JSONDecodeError, TypeError):
            continue
    versions.reverse()
    return versions


def entity_set(ccs):
    fe = ccs.get("focal_entities", [])
    names = set()
    for e in fe:
        if isinstance(e, dict):
            names.add(e.get("name", ""))
        else:
            names.add(str(e)[:30])
    names.discard("")
    return names


def jaccard(a, b):
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    versions = load_history(limit)
    print(f"=== Goal Churn Analysis — {len(versions)} CCS versions ===\n")

    if len(versions) < 2:
        print("Need at least 2 versions.")
        return

    # Embed goals and gists
    print("Embedding goal_orientation and semantic_gist fields...")
    goal_embs = []
    gist_embs = []
    for v in versions:
        goal = v.get("goal_orientation", "") or "(no goal)"
        gist = v.get("semantic_gist", "") or "(no gist)"
        goal_embs.append(get_embedding(goal))
        gist_embs.append(get_embedding(gist))

    # Consecutive field similarities
    goal_sims = []
    gist_sims = []
    entity_sims = []
    for i in range(len(versions) - 1):
        gs = cosine_sim(goal_embs[i], goal_embs[i + 1])
        gts = cosine_sim(gist_embs[i], gist_embs[i + 1])
        es = jaccard(entity_set(versions[i]), entity_set(versions[i + 1]))
        goal_sims.append(gs)
        gist_sims.append(gts)
        entity_sims.append(es)

    print(f"\n--- Field Stability (consecutive CCS pairs) ---\n")
    print(f"{'Field':25s} {'Mean Sim':>10s} {'Min':>8s} {'Std':>8s} {'Shifts':>8s}")
    print("-" * 60)
    for name, sims in [("goal_orientation", goal_sims), ("semantic_gist", gist_sims), ("focal_entities (Jaccard)", entity_sims)]:
        shifts = sum(1 for s in sims if (1 - s) > SHIFT_THRESHOLD)
        print(f"{name:25s} {np.mean(sims):10.4f} {np.min(sims):8.4f} {np.std(sims):8.4f} {shifts:5d}/{len(sims)}")

    # Identify goal regimes (runs of similar goals)
    print(f"\n--- Goal Regimes (shift threshold = {SHIFT_THRESHOLD}) ---\n")
    regimes = []
    regime_start = 0
    for i, sim in enumerate(goal_sims):
        drift = 1 - sim
        if drift > SHIFT_THRESHOLD:
            regimes.append((regime_start, i))
            regime_start = i + 1
    regimes.append((regime_start, len(versions) - 1))

    for ri, (start, end) in enumerate(regimes):
        ver_start = versions[start].get("version", "?")
        ver_end = versions[end].get("version", "?")
        goal_preview = (versions[start].get("goal_orientation", "") or "")[:80]
        print(f"  Regime {ri}: v{ver_start}–v{ver_end} ({end - start + 1} versions)")
        print(f"    Goal: {goal_preview}")

    # Episodic turnover
    print(f"\n--- Episodic Trace Turnover ---\n")
    ep_turnovers = []
    for i in range(len(versions) - 1):
        ep_a = set(str(e)[:60] for e in (versions[i].get("episodic_trace", []) or []))
        ep_b = set(str(e)[:60] for e in (versions[i + 1].get("episodic_trace", []) or []))
        if ep_a or ep_b:
            j = jaccard(ep_a, ep_b)
            ep_turnovers.append(1 - j)
    if ep_turnovers:
        print(f"  Mean turnover: {np.mean(ep_turnovers):.4f}")
        print(f"  Max turnover:  {np.max(ep_turnovers):.4f}")
        print(f"  Min turnover:  {np.min(ep_turnovers):.4f}")

    # Summary for #324
    print(f"\n=== #324 Compositionality Guidance ===\n")
    print(f"Safe comparison pairs (within same goal regime):")
    safe_pairs = 0
    for start, end in regimes:
        n = end - start + 1
        pairs = n * (n - 1) // 2
        safe_pairs += pairs
        if pairs > 0:
            ver_s = versions[start].get("version", "?")
            ver_e = versions[end].get("version", "?")
            print(f"  v{ver_s}–v{ver_e}: {pairs} pairs")
    total_pairs = len(versions) * (len(versions) - 1) // 2
    print(f"\n  {safe_pairs}/{total_pairs} pairs are goal-regime-safe ({safe_pairs/total_pairs*100:.0f}%)")

    # Save results
    result = {
        "timestamp": time.time(),
        "n_versions": len(versions),
        "goal_mean_sim": round(float(np.mean(goal_sims)), 4),
        "gist_mean_sim": round(float(np.mean(gist_sims)), 4),
        "entity_mean_jaccard": round(float(np.mean(entity_sims)), 4),
        "goal_shifts": sum(1 for s in goal_sims if (1 - s) > SHIFT_THRESHOLD),
        "n_regimes": len(regimes),
        "safe_pair_pct": round(safe_pairs / total_pairs * 100, 1) if total_pairs > 0 else 0,
        "ep_mean_turnover": round(float(np.mean(ep_turnovers)), 4) if ep_turnovers else None,
    }
    out_path = "/home/nate-agx/chronicle/data/goal_churn_analysis.jsonl"
    with open(out_path, "a") as f:
        f.write(json.dumps(result) + "\n")
    print(f"\n  Results appended to {out_path}")


if __name__ == "__main__":
    main()
