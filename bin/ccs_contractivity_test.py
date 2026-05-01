#!/usr/bin/env python3
"""CCS Contractivity Test — advance 55 falsifiable prediction.

If the CCS compressor is contractive (like looped transformer architectures),
running it twice on the same session context should produce minimal change
on the second pass. If the second compression substantially changes the CCS,
the compressor is exploratory, not contractive.

Usage:
  ccs_contractivity_test.py run     # Run double-compression and compare
  ccs_contractivity_test.py history # Show historical CCS-to-CCS similarity

The 'run' command calls compress_cognitive_state twice with the same context
and measures the delta. USE AT ROTATION BOUNDARY ONLY — each call writes
to the DB.

The 'history' command is read-only: compares consecutive CCS snapshots
to estimate how contractive the compressor has been historically.
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def jaccard(a, b):
    """Word-level Jaccard similarity."""
    sa = set(a.lower().split())
    sb = set(b.lower().split())
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def field_similarity(snap_a, snap_b):
    """Compare two CCS snapshots field by field."""
    fields = {
        "semantic_gist": ("gist", str),
        "goal_orientation": ("goal", str),
        "predictive_cue": ("cue", str),
        "focal_entities": ("entities", list),
        "constraints": ("constraints", list),
        "episodic_trace": ("episodic", list),
    }

    results = {}
    for field, (label, ftype) in fields.items():
        a_val = snap_a.get(field, "" if ftype == str else [])
        b_val = snap_b.get(field, "" if ftype == str else [])

        if ftype == str:
            sim = jaccard(str(a_val), str(b_val))
        elif ftype == list:
            # For lists, compare JSON representations
            a_str = json.dumps(a_val, sort_keys=True) if a_val else ""
            b_str = json.dumps(b_val, sort_keys=True) if b_val else ""
            sim = jaccard(a_str, b_str)

        results[label] = sim

    return results


def cmd_history():
    """Show historical CCS-to-CCS similarity between consecutive snapshots."""
    conn = sqlite3.connect(DB_PATH, timeout=5)
    rows = conn.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id"
    ).fetchall()
    conn.close()

    if len(rows) < 2:
        print("Need >=2 CCS snapshots. Have {}.".format(len(rows)))
        return

    print("CCS Contractivity History — {} snapshots\n".format(len(rows)))
    print("{:>4} {:>12} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}".format(
        "ID", "Date", "gist", "goal", "cue", "ents", "const", "epis"))
    print("{} {} {} {} {} {} {} {}".format(
        "-"*4, "-"*12, "-"*6, "-"*6, "-"*6, "-"*6, "-"*6, "-"*6))

    sims_by_field = {k: [] for k in ["gist", "goal", "cue", "entities", "constraints", "episodic"]}

    for i in range(1, len(rows)):
        prev_snap = json.loads(rows[i-1][1])
        curr_snap = json.loads(rows[i][1])
        curr_id = rows[i][0]
        curr_ts = datetime.fromtimestamp(rows[i][2], PDT).strftime("%m-%d %H:%M")

        sims = field_similarity(prev_snap, curr_snap)

        for k, v in sims.items():
            sims_by_field[k].append(v)

        print("{:>4} {:>12} {:>6.3f} {:>6.3f} {:>6.3f} {:>6.3f} {:>6.3f} {:>6.3f}".format(
            curr_id, curr_ts,
            sims["gist"], sims["goal"], sims["cue"],
            sims["entities"], sims["constraints"], sims["episodic"]))

    print()
    print("Field means (higher = more contractive):")
    for field, vals in sims_by_field.items():
        if vals:
            mean = sum(vals) / len(vals)
            # Last 10 vs first 10
            early = vals[:10] if len(vals) >= 10 else vals
            late = vals[-10:] if len(vals) >= 10 else vals
            early_mean = sum(early) / len(early)
            late_mean = sum(late) / len(late)
            trend = late_mean - early_mean
            arrow = "+" if trend > 0 else ""
            print("  {:>12}: mean={:.3f}  early={:.3f}  late={:.3f}  trend={}{:.3f}".format(
                field, mean, early_mean, late_mean, arrow, trend))

    # Overall contractivity score
    all_late = []
    for vals in sims_by_field.values():
        if len(vals) >= 5:
            all_late.extend(vals[-5:])
    if all_late:
        overall = sum(all_late) / len(all_late)
        if overall > 0.7:
            regime = "CONTRACTIVE"
        elif overall > 0.4:
            regime = "MIXED"
        else:
            regime = "EXPLORATORY"
        print("\nLast-5 overall similarity: {:.3f} — {}".format(overall, regime))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]

    if cmd == "history":
        cmd_history()
    elif cmd == "run":
        print("Double-compression test requires MCP compress_cognitive_state.")
        print("Run at rotation boundary only — each call writes to DB.")
        print("Protocol:")
        print("  1. compress_cognitive_state with session context")
        print("  2. Read the resulting CCS")
        print("  3. compress_cognitive_state again with same context")
        print("  4. Compare CCS v1 and v2 using field_similarity()")
        print("  5. High similarity (>0.8) = contractive. Low (<0.5) = exploratory.")
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
