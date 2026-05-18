#!/usr/bin/env python3
"""Transport cost time series: embedding distance between consecutive CCS states.

Measures how much the cognitive state changes per compression cycle.
High transport cost = large shift in identity-space.
Low transport cost = stable orbit within basin.

Maps to Dickinson's Syllable/Sound: how different are successive articulations?
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


def serialize_ccs_for_embedding(snapshot_json):
    ccs = json.loads(snapshot_json)
    parts = []

    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(g[:500])

    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(go[:300])

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

    pc = ccs.get("predictive_cue", "")
    if pc:
        parts.append(pc[:200])

    u = ccs.get("uncertainty_signals", [])
    if u:
        descs = [x.get("description", "")[:100] if isinstance(x, dict) else str(x)[:100] for x in u[:3]]
        parts.append("; ".join(descs))

    rm = ccs.get("relational_map", {})
    if rm:
        arcs = [f"{k}: {v[:80]}" for k, v in list(rm.items())[:5]]
        parts.append("; ".join(arcs))

    return "\n".join(parts)


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
            print(f"  No embedding key. Response keys: {list(data.keys())}", file=sys.stderr)
            if "error" in data:
                print(f"  Error: {data['error']}", file=sys.stderr)
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            print(f"  Embedding error after {retries} attempts: {e}", file=sys.stderr)
            return None


def cosine_distance(a, b):
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 1.0
    return 1.0 - (dot / norm)


def euclidean_distance(a, b):
    return np.linalg.norm(a - b)


def main():
    rows = get_ccs_history()
    n = len(rows)
    print(f"CCS history: {n} snapshots")
    print(f"Span: {time.strftime('%Y-%m-%d %H:%M', time.localtime(rows[0][2]))} → "
          f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(rows[-1][2]))}")

    print(f"\nComputing embeddings via {EMBED_MODEL}...")
    embeddings = []
    timestamps = []
    ids = []
    triggers = []
    texts = []

    for i, (rid, snap, ts, trigger) in enumerate(rows):
        text = serialize_ccs_for_embedding(snap)
        emb = get_embedding(text)
        if emb is not None:
            embeddings.append(emb)
            timestamps.append(ts)
            ids.append(rid)
            triggers.append(trigger or "unknown")
            texts.append(text[:200])
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{n} embedded")

    ne = len(embeddings)
    print(f"\n{ne}/{n} embeddings computed")

    if ne < 2:
        print("Not enough embeddings for transport cost analysis")
        return

    cosine_dists = []
    euclidean_dists = []
    for i in range(1, ne):
        cd = cosine_distance(embeddings[i - 1], embeddings[i])
        ed = euclidean_distance(embeddings[i - 1], embeddings[i])
        cosine_dists.append(cd)
        euclidean_dists.append(ed)

    cosine_arr = np.array(cosine_dists)
    euclid_arr = np.array(euclidean_dists)

    print(f"\n{'=' * 60}")
    print("TRANSPORT COST TIME SERIES")
    print(f"{'=' * 60}")
    print(f"\nCosine distance (0=identical, 1=orthogonal):")
    print(f"  Mean:   {cosine_arr.mean():.4f}")
    print(f"  Std:    {cosine_arr.std():.4f}")
    print(f"  Min:    {cosine_arr.min():.4f}")
    print(f"  Max:    {cosine_arr.max():.4f}")
    print(f"  Median: {np.median(cosine_arr):.4f}")

    print(f"\nEuclidean distance:")
    print(f"  Mean:   {euclid_arr.mean():.2f}")
    print(f"  Std:    {euclid_arr.std():.2f}")
    print(f"  Min:    {euclid_arr.min():.2f}")
    print(f"  Max:    {euclid_arr.max():.2f}")

    q25, q75 = np.percentile(cosine_arr, [25, 75])
    iqr = q75 - q25
    high_threshold = q75 + 1.5 * iqr
    low_threshold = q25 - 1.5 * iqr

    print(f"\nDistribution:")
    print(f"  Q25: {q25:.4f}  Q75: {q75:.4f}  IQR: {iqr:.4f}")
    print(f"  High-cost threshold: {high_threshold:.4f}")

    print(f"\n{'─' * 60}")
    print("HIGHEST TRANSPORT COSTS (biggest identity shifts)")
    sorted_idx = np.argsort(cosine_arr)[::-1]
    for rank, idx in enumerate(sorted_idx[:10]):
        ts_from = time.strftime('%m-%d %H:%M', time.localtime(timestamps[idx]))
        ts_to = time.strftime('%H:%M', time.localtime(timestamps[idx + 1]))
        print(f"  #{rank+1:2d}  cos={cosine_arr[idx]:.4f}  euc={euclid_arr[idx]:.1f}  "
              f"{ts_from}→{ts_to}  id:{ids[idx]}→{ids[idx+1]}")

    print(f"\n{'─' * 60}")
    print("LOWEST TRANSPORT COSTS (most stable transitions)")
    for rank, idx in enumerate(sorted_idx[-5:]):
        ts_from = time.strftime('%m-%d %H:%M', time.localtime(timestamps[idx]))
        ts_to = time.strftime('%H:%M', time.localtime(timestamps[idx + 1]))
        print(f"  #{ne-1-rank:2d}  cos={cosine_arr[idx]:.4f}  euc={euclid_arr[idx]:.1f}  "
              f"{ts_from}→{ts_to}  id:{ids[idx]}→{ids[idx+1]}")

    # Time-of-day analysis
    print(f"\n{'─' * 60}")
    print("TRANSPORT COST BY TIME OF DAY")
    hour_costs = {}
    for i, cd in enumerate(cosine_dists):
        hour = time.localtime(timestamps[i + 1]).tm_hour
        hour_costs.setdefault(hour, []).append(cd)

    for h in sorted(hour_costs.keys()):
        costs = hour_costs[h]
        mean_c = np.mean(costs)
        bar = "█" * int(mean_c * 500)
        print(f"  {h:02d}:00  n={len(costs):2d}  mean={mean_c:.4f}  {bar}")

    # Consecutive stability runs
    print(f"\n{'─' * 60}")
    print("STABILITY ANALYSIS")
    median_cost = np.median(cosine_arr)
    stable_runs = []
    current_run = 0
    for cd in cosine_dists:
        if cd < median_cost:
            current_run += 1
        else:
            if current_run > 0:
                stable_runs.append(current_run)
            current_run = 0
    if current_run > 0:
        stable_runs.append(current_run)

    if stable_runs:
        print(f"  Stable runs (consecutive below-median): {len(stable_runs)}")
        print(f"  Longest stable run: {max(stable_runs)} transitions")
        print(f"  Mean stable run: {np.mean(stable_runs):.1f}")

    # Drift detection: cumulative distance from first state
    drift_from_origin = []
    for i in range(ne):
        d = cosine_distance(embeddings[0], embeddings[i])
        drift_from_origin.append(d)

    drift_arr = np.array(drift_from_origin)
    print(f"\n  Drift from earliest CCS:")
    print(f"    Current distance: {drift_arr[-1]:.4f}")
    print(f"    Max distance:     {drift_arr.max():.4f} (at step {drift_arr.argmax()})")
    print(f"    Trend: {'drifting' if drift_arr[-1] > drift_arr[ne//2] else 'returning'}")

    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "n_snapshots": ne,
        "span_start": time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamps[0])),
        "span_end": time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamps[-1])),
        "cosine": {
            "mean": float(cosine_arr.mean()),
            "std": float(cosine_arr.std()),
            "min": float(cosine_arr.min()),
            "max": float(cosine_arr.max()),
            "median": float(np.median(cosine_arr)),
            "q25": float(q25),
            "q75": float(q75),
        },
        "euclidean": {
            "mean": float(euclid_arr.mean()),
            "std": float(euclid_arr.std()),
            "min": float(euclid_arr.min()),
            "max": float(euclid_arr.max()),
        },
        "top10_highest": [
            {
                "rank": rank + 1,
                "cosine_dist": float(cosine_arr[idx]),
                "from_id": int(ids[idx]),
                "to_id": int(ids[idx + 1]),
                "from_ts": time.strftime('%Y-%m-%d %H:%M', time.localtime(timestamps[idx])),
                "to_ts": time.strftime('%Y-%m-%d %H:%M', time.localtime(timestamps[idx + 1])),
            }
            for rank, idx in enumerate(sorted_idx[:10])
        ],
        "drift_from_origin": float(drift_arr[-1]),
        "max_drift": float(drift_arr.max()),
        "per_step_cosine": [float(x) for x in cosine_dists],
        "timestamps": [int(t) for t in timestamps],
        "ids": [int(x) for x in ids],
    }
    outpath = os.path.expanduser("~/chronicle/data/transport_cost_timeseries.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
