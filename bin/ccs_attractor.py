#!/usr/bin/env python3
"""
ccs_attractor.py — Detect CCS trajectory dynamics.

Reads cognitive_state_history (50 snapshots), embeds each, measures distances
between consecutive snapshots, and classifies the trajectory:
  - CONTRACTIVE: distances decreasing (converging to fixed point)
  - OSCILLATORY: distances alternating/periodic (cycling between states)
  - EXPLORATORY: distances increasing (diverging)
  - STABLE: distances low and flat (already at attractor)

Tests the Tacheny (2512.10350) frame directly: is CCS evolution contractive,
oscillatory, or exploratory? Advance 48 predicts contractive is desirable.

Usage:
  python3 ccs_attractor.py           # analyze trajectory
  python3 ccs_attractor.py discord   # analyze and post to Discord
"""
import json
import math
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
PDT = timezone(timedelta(hours=-7))


def embed(text, timeout=60):
    text = text[:800]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine_dist(a, b):
    """1 - cosine_similarity. 0 = identical, 2 = opposite."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 1.0
    return 1.0 - (dot / (na * nb))


def snapshot_to_text(snapshot_json):
    """Convert a CCS snapshot JSON to a flat text representation."""
    try:
        snap = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return ""

    parts = []
    for key in ["semantic_gist", "goal_orientation", "episodic_trace",
                 "focal_entities", "predictive_cue", "uncertainty_signals",
                 "constraints"]:
        val = snap.get(key, "")
        if val:
            if isinstance(val, list):
                val = "; ".join(str(v) for v in val[:5])
            elif isinstance(val, dict):
                val = json.dumps(val)[:200]
            parts.append(f"{key}: {str(val)[:200]}")
    return "\n".join(parts)


def analyze():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot, created_at, trigger FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    db.close()

    if len(rows) < 3:
        print(f"Need >= 3 snapshots. Found {len(rows)}.")
        return None

    print(f"Analyzing {len(rows)} CCS snapshots...")
    print(f"  Range: {datetime.fromtimestamp(rows[0][1], PDT).strftime('%Y-%m-%d %H:%M')} → "
          f"{datetime.fromtimestamp(rows[-1][1], PDT).strftime('%Y-%m-%d %H:%M')}")

    # Embed each snapshot
    embeddings = []
    timestamps = []
    triggers = []
    for snap_json, ts, trigger in rows:
        text = snapshot_to_text(snap_json)
        if not text:
            continue
        try:
            emb = embed(text)
            embeddings.append(emb)
            timestamps.append(ts)
            triggers.append(trigger or "unknown")
        except Exception as e:
            print(f"  Embed failed at {ts}: {e}")

    print(f"  Embedded: {len(embeddings)} snapshots")

    if len(embeddings) < 3:
        print("Not enough embeddings.")
        return None

    # Compute consecutive distances
    distances = []
    for i in range(1, len(embeddings)):
        d = cosine_dist(embeddings[i - 1], embeddings[i])
        gap_hours = (timestamps[i] - timestamps[i - 1]) / 3600
        distances.append({
            "step": i,
            "distance": round(d, 6),
            "gap_hours": round(gap_hours, 1),
            "trigger": triggers[i],
            "time": datetime.fromtimestamp(timestamps[i], PDT).strftime("%m-%d %H:%M"),
        })

    # Also compute distance from each snapshot to the LAST snapshot (attractor proximity)
    last_emb = embeddings[-1]
    attractor_dists = []
    for i, emb in enumerate(embeddings):
        d = cosine_dist(emb, last_emb)
        attractor_dists.append(round(d, 6))

    # Classify trajectory
    d_values = [d["distance"] for d in distances]
    mean_d = sum(d_values) / len(d_values)
    recent_d = d_values[-5:] if len(d_values) >= 5 else d_values
    early_d = d_values[:5] if len(d_values) >= 5 else d_values
    recent_mean = sum(recent_d) / len(recent_d)
    early_mean = sum(early_d) / len(early_d)

    # Trend: are distances increasing, decreasing, or flat?
    n = len(d_values)
    x_mean = (n - 1) / 2
    y_mean = mean_d
    slope_num = sum((i - x_mean) * (d_values[i] - y_mean) for i in range(n))
    slope_den = sum((i - x_mean) ** 2 for i in range(n))
    slope = slope_num / slope_den if slope_den > 0 else 0

    # Check for periodicity (oscillation)
    diffs = [d_values[i] - d_values[i - 1] for i in range(1, len(d_values))]
    sign_changes = sum(1 for i in range(1, len(diffs)) if diffs[i] * diffs[i - 1] < 0)
    oscillation_ratio = sign_changes / max(len(diffs) - 1, 1)

    # Classification
    if mean_d < 0.02:
        regime = "STABLE"
        desc = "CCS barely changing — near a fixed point"
    elif slope < -0.0005 and recent_mean < early_mean * 0.8:
        regime = "CONTRACTIVE"
        desc = "Distances decreasing — converging to attractor"
    elif oscillation_ratio > 0.6 and abs(slope) < 0.001:
        regime = "OSCILLATORY"
        desc = "Distances alternating — cycling between states"
    elif slope > 0.001:
        regime = "EXPLORATORY"
        desc = "Distances increasing — diverging"
    else:
        regime = "MIXED"
        desc = "No clear single regime"

    # Attractor convergence check
    a_recent = attractor_dists[-5:] if len(attractor_dists) >= 5 else attractor_dists
    a_early = attractor_dists[:5] if len(attractor_dists) >= 5 else attractor_dists
    a_recent_mean = sum(a_recent) / len(a_recent)
    a_early_mean = sum(a_early) / len(a_early)
    converging_to_final = a_recent_mean < a_early_mean * 0.5

    result = {
        "n_snapshots": len(embeddings),
        "regime": regime,
        "description": desc,
        "mean_distance": round(mean_d, 6),
        "early_mean": round(early_mean, 6),
        "recent_mean": round(recent_mean, 6),
        "slope": round(slope, 8),
        "oscillation_ratio": round(oscillation_ratio, 3),
        "converging_to_final": converging_to_final,
        "distances": distances,
        "attractor_distances": attractor_dists,
    }

    # Print report
    print(f"\n{'=' * 50}")
    print(f"CCS TRAJECTORY ANALYSIS")
    print(f"{'=' * 50}")
    print(f"  Regime: {regime}")
    print(f"  {desc}")
    print(f"  Snapshots: {len(embeddings)}")
    print(f"  Mean step distance: {mean_d:.6f}")
    print(f"  Early mean (first 5): {early_mean:.6f}")
    print(f"  Recent mean (last 5): {recent_mean:.6f}")
    print(f"  Slope: {slope:.8f} ({'decreasing' if slope < 0 else 'increasing'})")
    print(f"  Oscillation ratio: {oscillation_ratio:.3f}")
    print(f"  Converging to current state: {converging_to_final}")
    print()

    # Show last 10 steps
    print("Recent trajectory:")
    for d in distances[-10:]:
        bar = "█" * int(d["distance"] * 200)
        print(f"  [{d['time']}] d={d['distance']:.4f} gap={d['gap_hours']:.1f}h {bar}")

    print()

    # Show largest jumps
    sorted_d = sorted(distances, key=lambda x: x["distance"], reverse=True)
    print("Largest jumps:")
    for d in sorted_d[:5]:
        print(f"  [{d['time']}] d={d['distance']:.4f} gap={d['gap_hours']:.1f}h trigger={d['trigger']}")

    return result


def main():
    result = analyze()
    if not result:
        return 1

    if len(sys.argv) > 1 and sys.argv[1] == "discord":
        webhook = os.environ.get("OPUS_WEBHOOK", "")
        chronicle_env = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(chronicle_env) and not webhook:
            for line in open(chronicle_env):
                if line.strip().startswith("OPUS_WEBHOOK="):
                    webhook = line.strip().split("=", 1)[1].strip("'\"")

        if webhook:
            r = result
            msg = (
                f"**CCS Attractor Analysis** ({r['n_snapshots']} snapshots)\n"
                f"Regime: **{r['regime']}** — {r['description']}\n"
                f"Step distance: early={r['early_mean']:.4f} → recent={r['recent_mean']:.4f}\n"
                f"Slope: {r['slope']:.6f} | Oscillation: {r['oscillation_ratio']:.2f}\n"
                f"Converging to current: {r['converging_to_final']}"
            )
            data = json.dumps({"content": msg}).encode()
            req = urllib.request.Request(
                webhook, data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urllib.request.urlopen(req, timeout=10)
                print("Posted to Discord.")
            except Exception as e:
                print(f"Discord post failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
