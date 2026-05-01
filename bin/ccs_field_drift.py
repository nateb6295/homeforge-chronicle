#!/usr/bin/env python3
"""
ccs_field_drift.py — Measure per-field volatility across CCS snapshots.

Which fields change most? If gist/constraints drift as much as episodic_trace,
the compressor is too aggressive on identity-layer fields.

Embeds each field independently across snapshots. Reports:
  - Mean cosine distance per field
  - Which fields are stable (identity) vs volatile (operational)
  - Recommendations for anchor fields

Usage:
  python3 ccs_field_drift.py              # analyze all snapshots
  python3 ccs_field_drift.py --recent 10  # last N snapshots only
"""
import json
import math
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone, timedelta

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
PDT = timezone(timedelta(hours=-7))

FIELDS = [
    "semantic_gist", "goal_orientation", "focal_entities",
    "episodic_trace", "predictive_cue", "uncertainty_signals",
    "constraints", "relational_map",
]

# Fields expected to be stable (identity layer)
IDENTITY_FIELDS = {"semantic_gist", "goal_orientation", "constraints"}
# Fields expected to be volatile (operational layer)
OPERATIONAL_FIELDS = {"episodic_trace", "predictive_cue", "uncertainty_signals"}


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
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 1.0
    return 1.0 - (dot / (na * nb))


def field_text(snap, field):
    """Extract a field from a snapshot as text."""
    val = snap.get(field, "")
    if not val:
        return ""
    if isinstance(val, list):
        return "; ".join(str(v) for v in val[:10])
    if isinstance(val, dict):
        return json.dumps(val)[:400]
    return str(val)[:400]


def analyze(limit=None):
    db = sqlite3.connect(DB)
    query = "SELECT snapshot, created_at FROM cognitive_state_history ORDER BY created_at ASC"
    rows = db.execute(query).fetchall()
    db.close()

    if limit:
        rows = rows[-limit:]

    if len(rows) < 3:
        print(f"Need >= 3 snapshots. Found {len(rows)}.")
        return None

    # Parse snapshots
    snapshots = []
    for snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
            snap["_ts"] = ts
            snapshots.append(snap)
        except (json.JSONDecodeError, TypeError):
            continue

    print(f"Analyzing {len(snapshots)} snapshots across {len(FIELDS)} fields...")
    print(f"Range: {datetime.fromtimestamp(snapshots[0]['_ts'], PDT).strftime('%Y-%m-%d %H:%M')} → "
          f"{datetime.fromtimestamp(snapshots[-1]['_ts'], PDT).strftime('%Y-%m-%d %H:%M')}")
    print()

    # Embed each field for each snapshot
    field_embeddings = {f: [] for f in FIELDS}
    for i, snap in enumerate(snapshots):
        for field in FIELDS:
            text = field_text(snap, field)
            if text:
                try:
                    emb = embed(text)
                    field_embeddings[field].append((i, emb))
                except Exception as e:
                    pass
        if (i + 1) % 10 == 0:
            print(f"  Embedded {i + 1}/{len(snapshots)} snapshots...")

    print(f"  Done embedding.")
    print()

    # Compute per-field consecutive distances
    field_stats = {}
    for field in FIELDS:
        embs = field_embeddings[field]
        if len(embs) < 2:
            continue

        distances = []
        for j in range(1, len(embs)):
            d = cosine_dist(embs[j - 1][1], embs[j][1])
            distances.append(d)

        mean_d = sum(distances) / len(distances)
        max_d = max(distances)
        min_d = min(distances)
        std_d = (sum((d - mean_d) ** 2 for d in distances) / len(distances)) ** 0.5

        field_stats[field] = {
            "mean": round(mean_d, 6),
            "std": round(std_d, 6),
            "max": round(max_d, 6),
            "min": round(min_d, 6),
            "n_pairs": len(distances),
        }

    # Print report
    print("=" * 60)
    print("PER-FIELD CCS DRIFT ANALYSIS")
    print("=" * 60)
    print(f"{'Field':<22} {'Mean dist':>10} {'Std':>8} {'Max':>8} {'Layer':<12}")
    print("-" * 62)

    sorted_fields = sorted(field_stats.keys(), key=lambda f: field_stats[f]["mean"])
    for field in sorted_fields:
        s = field_stats[field]
        if field in IDENTITY_FIELDS:
            layer = "IDENTITY"
        elif field in OPERATIONAL_FIELDS:
            layer = "OPERATIONAL"
        else:
            layer = "mixed"
        print(f"{field:<22} {s['mean']:>10.6f} {s['std']:>8.6f} {s['max']:>8.6f} {layer:<12}")

    print()

    # Check if identity fields are drifting more than expected
    identity_drifts = [field_stats[f]["mean"] for f in IDENTITY_FIELDS if f in field_stats]
    operational_drifts = [field_stats[f]["mean"] for f in OPERATIONAL_FIELDS if f in field_stats]

    if identity_drifts and operational_drifts:
        id_mean = sum(identity_drifts) / len(identity_drifts)
        op_mean = sum(operational_drifts) / len(operational_drifts)
        ratio = id_mean / op_mean if op_mean > 0 else float('inf')

        print(f"Identity layer mean drift: {id_mean:.6f}")
        print(f"Operational layer mean drift: {op_mean:.6f}")
        print(f"Ratio (identity/operational): {ratio:.3f}")
        print()

        if ratio > 0.8:
            print("WARNING: Identity fields drifting nearly as much as operational fields.")
            print("The compressor is too aggressive on stable fields.")
            print("Recommendation: anchor gist/goal/constraints during compression.")
        elif ratio > 0.5:
            print("MODERATE: Identity fields somewhat volatile.")
            print("Consider partial anchoring of gist and constraints.")
        else:
            print("GOOD: Identity fields significantly more stable than operational.")
            print("The layer separation is working.")

    return field_stats


if __name__ == "__main__":
    limit = None
    for i, arg in enumerate(sys.argv):
        if arg == "--recent" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
    analyze(limit)
