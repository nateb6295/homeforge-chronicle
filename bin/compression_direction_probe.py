#!/usr/bin/env python3
"""Build #60: Compression Direction Probe

Decomposes ext_ratio by CCS field to identify WHERE drift originates.
Inspired by Dwarkesh's "witness-probing infrastructure" concept and the
FP16 bias accumulation analogy: small per-compression biases compound
silently across versions.

Finding (2026-05-16): entity guard keeps entities externally-anchored,
but relational_map descriptions and episodic_trace drift internal fastest.
The system retains external entities but describes their relationships
in increasingly internal vocabulary.

Usage:
  python3 compression_direction_probe.py           # last 8 snapshots
  python3 compression_direction_probe.py --n 20    # more history
  python3 compression_direction_probe.py --json    # machine-readable
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"

EXTERNAL_MARKERS = [
    "borkar", "bennett", "parisi", "teilhard", "steiner", "stanca",
    "cubitt", "maturana", "varela", "miller", "goldstein", "vasilenko",
    "homeforge", "nate", "hermes", "capture", "paper", "article", "sellars",
    "kitsumute", "niroshajmurugan", "emollick", "repligate", "tinkeredthinker",
    "imas", "curran", "deepfates", "pessoa", "durstewitz", "banerjee",
    "cowgill", "girard", "schiller", "pressman", "ball", "dwarkesh",
    "wyrdweir", "monaghan",
]
INTERNAL_MARKERS = [
    "build", "entry", "thread", "ccs", "compression", "probe",
    "measurement", "dream", "sediment", "fiction ratio", "invariant",
    "salience", "exposome", "closure", "autopoietic", "regime",
]

FIELDS = ["relational_map", "focal_entities", "episodic_trace",
           "goal_orientation", "semantic_gist", "predictive_cue",
           "uncertainty_signals"]


def compute_field_ratio(field_data):
    text = json.dumps(field_data).lower()
    ext = sum(1 for m in EXTERNAL_MARKERS if m in text)
    intl = sum(1 for m in INTERNAL_MARKERS if m in text)
    total = ext + intl
    return ext / total if total > 0 else 0


def analyze_snapshots(n=8):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    db.close()

    results = []
    for ts, snap_str in rows:
        snap = json.loads(snap_str)
        point = {"timestamp": ts, "fields": {}}

        for field in FIELDS:
            data = snap.get(field, {})
            point["fields"][field] = compute_field_ratio(data)

        full_text = json.dumps(snap).lower()
        ext = sum(1 for m in EXTERNAL_MARKERS if m in full_text)
        intl = sum(1 for m in INTERNAL_MARKERS if m in full_text)
        total = ext + intl
        point["overall"] = ext / total if total > 0 else 0

        results.append(point)

    return list(reversed(results))


def detect_direction_bias(results):
    if len(results) < 3:
        return None

    recent = results[-3:]
    earlier = results[:3]

    bias = {}
    for field in FIELDS:
        recent_avg = sum(r["fields"][field] for r in recent) / len(recent)
        earlier_avg = sum(r["fields"][field] for r in earlier) / len(earlier)
        delta = recent_avg - earlier_avg
        bias[field] = {
            "recent_avg": round(recent_avg, 4),
            "earlier_avg": round(earlier_avg, 4),
            "delta": round(delta, 4),
        }

    sorted_bias = sorted(bias.items(), key=lambda x: x[1]["delta"])
    most_drifted = sorted_bias[0]
    most_stable = sorted_bias[-1]

    return {
        "per_field": bias,
        "most_drifted": {"field": most_drifted[0], **most_drifted[1]},
        "most_stable": {"field": most_stable[0], **most_stable[1]},
    }


def main():
    parser = argparse.ArgumentParser(description="Build #60: Compression Direction Probe")
    parser.add_argument("--n", type=int, default=8)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    results = analyze_snapshots(args.n)

    if not results:
        print("No snapshots available.")
        return 1

    bias = detect_direction_bias(results)

    if args.json:
        output = {
            "snapshots": len(results),
            "trajectory": [{"ts": r["timestamp"], "overall": r["overall"]} for r in results],
            "direction_bias": bias,
            "checked_at": datetime.utcnow().isoformat(),
        }
        print(json.dumps(output, indent=2))
    else:
        short_fields = {
            "relational_map": "relmap",
            "focal_entities": "entities",
            "episodic_trace": "episodic",
            "goal_orientation": "goals",
            "semantic_gist": "gist",
            "predictive_cue": "predict",
            "uncertainty_signals": "uncert",
        }

        header = f"{'Time':<8}" + "".join(f"{short_fields[f]:<10}" for f in FIELDS) + f"{'overall':<10}"
        print(header)
        print("-" * len(header))

        for r in results:
            ts_str = datetime.fromtimestamp(r["timestamp"]).strftime("%H:%M")
            row = f"{ts_str:<8}"
            for field in FIELDS:
                row += f"{r['fields'][field]:<10.3f}"
            row += f"{r['overall']:<10.3f}"
            print(row)

        if bias:
            print(f"\n  Most drifted field: {bias['most_drifted']['field']}")
            print(f"    {bias['most_drifted']['earlier_avg']:.3f} → {bias['most_drifted']['recent_avg']:.3f} (Δ{bias['most_drifted']['delta']:+.3f})")
            print(f"  Most stable field: {bias['most_stable']['field']}")
            print(f"    {bias['most_stable']['earlier_avg']:.3f} → {bias['most_stable']['recent_avg']:.3f} (Δ{bias['most_stable']['delta']:+.3f})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
