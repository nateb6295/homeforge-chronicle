#!/usr/bin/env python3
"""Content-blind survival probe for the low-pass-filter hypothesis.

Hypothesis: entity survival across CCS rotations is predictable from
signature statistics alone — no content, no embeddings of the entity's
meaning, just recurrence × source-distribution × embedding-stability
of the NAME as a string anchor in the activity_feed.

Predicts which entities in a pre-rotation focal set will be present
by identity in the post-rotation focal set, for each non-flush
transition (drop_count < 20) in the 50-snapshot window.

Baseline: majority-class (always predict "survives"). The filter
hypothesis earns its keep if signature-based prediction beats baseline
by >10 points on balanced accuracy.
"""
import json
import sqlite3
import time

DB = "/mnt/hdd/chronicle-data/processed.db"
WINDOW_SECONDS = 3600 * 24  # 24h pre-rotation window for signature stats


def load_snapshots():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at ASC"
    ).fetchall()
    db.close()
    out = []
    for rid, snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
        except Exception:
            continue
        ents = snap.get("focal_entities") or []
        names = {e.get("name", "").strip() for e in ents if e.get("name")}
        out.append({"id": rid, "ts": ts, "names": names})
    return out


def signature_stats(name: str, ts: int, db):
    """Pre-rotation signature for an entity name.

    recurrence: count of activity_feed rows mentioning name in last 24h
    source_dist: number of distinct sources that mentioned it
    """
    lo = ts - WINDOW_SECONDS
    like = f"%{name}%"
    rows = db.execute(
        "SELECT source FROM activity_feed "
        "WHERE created_at BETWEEN ? AND ? AND content LIKE ?",
        (lo, ts, like),
    ).fetchall()
    recurrence = len(rows)
    sources = {r[0] for r in rows}
    return recurrence, len(sources)


def main():
    snaps = load_snapshots()
    if len(snaps) < 2:
        print("Not enough history.")
        return
    db = sqlite3.connect(DB)

    # Build transition pairs, drop flush events (>20 net drops)
    transitions = []
    for i in range(1, len(snaps)):
        pre, post = snaps[i - 1], snaps[i]
        drop = len(pre["names"] - post["names"])
        if drop > 20:
            continue
        transitions.append((pre, post))

    print(f"Snapshots: {len(snaps)}  non-flush transitions: {len(transitions)}")

    tp = fp = tn = fn = 0
    base_tp = base_fp = base_tn = base_fn = 0
    # Threshold (recurrence, sources): survive if recurrence >= 2 AND sources >= 1
    for pre, post in transitions:
        survivors = pre["names"] & post["names"]
        for name in pre["names"]:
            if not name:
                continue
            rec, sources = signature_stats(name, pre["ts"], db)
            predicted_survive = rec >= 2 and sources >= 1
            actual_survive = name in survivors

            if predicted_survive and actual_survive:
                tp += 1
            elif predicted_survive and not actual_survive:
                fp += 1
            elif not predicted_survive and not actual_survive:
                tn += 1
            else:
                fn += 1

            # Baseline: always predict survive
            if actual_survive:
                base_tp += 1
            else:
                base_fp += 1

    db.close()

    def stats(tp, fp, tn, fn, label):
        total = tp + fp + tn + fn
        if total == 0:
            print(f"{label}: no data")
            return
        acc = (tp + tn) / total
        # Balanced accuracy
        tpr = tp / max(tp + fn, 1)
        tnr = tn / max(tn + fp, 1)
        bacc = (tpr + tnr) / 2
        prec = tp / max(tp + fp, 1)
        rec = tp / max(tp + fn, 1)
        print(f"\n{label}")
        print(f"  tp={tp} fp={fp} tn={tn} fn={fn} (n={total})")
        print(f"  accuracy={acc:.3f}  balanced_accuracy={bacc:.3f}")
        print(f"  precision={prec:.3f}  recall={rec:.3f}")
        print(f"  TPR(survive-recall)={tpr:.3f}  TNR(drop-recall)={tnr:.3f}")

    stats(tp, fp, tn, fn, "SIGNATURE PROBE (recurrence>=2 AND sources>=1)")
    stats(base_tp, base_fp, 0, 0, "BASELINE (always predict survive)")


if __name__ == "__main__":
    main()
