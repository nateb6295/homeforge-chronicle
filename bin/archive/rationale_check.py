#!/usr/bin/env python3
"""Rationale Checker — Validate prediction reasoning, not just outcomes.

Darby's question: "Are we capturing calibration drift over time, or just
accuracy at snapshot? A prediction can be right for the wrong reason."

When a prediction resolves, this checks whether the stated rationale
(the adjustment reasons) actually aligned with what happened. A prediction
scored 'correct' whose rationale diverged from reality is a map-territory
failure — the outcome matched but the map was wrong.

Usage:
    python3 rationale_check.py audit              # audit scored predictions
    python3 rationale_check.py drift               # calibration drift over time
    python3 rationale_check.py trail PRED_ID       # show reasoning trail for one prediction

Build #87. Darby's calibration drift question, answered.
"""

import os
import sys
import json
import sqlite3
import time
from datetime import datetime

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def _get_scored_predictions() -> list:
    """Get predictions that have been scored."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, claim, confidence, status, outcome, score_notes, created_at, scored_at "
        "FROM prediction_track WHERE status = 'scored' ORDER BY scored_at DESC"
    ).fetchall()
    db.close()
    return [{"id": r[0], "claim": r[1], "confidence": r[2], "status": r[3],
             "outcome": r[4], "score_notes": r[5],
             "created_at": r[6], "scored_at": r[7]} for r in rows]


def _get_adjustment_trail(pred_id: int) -> list:
    """Get all confidence adjustments for a prediction."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT old_confidence, new_confidence, reason, source, created_at "
        "FROM prediction_adjustments WHERE prediction_id = ? ORDER BY created_at",
        (pred_id,)
    ).fetchall()
    db.close()
    return [{"old": r[0], "new": r[1], "reason": r[2], "source": r[3],
             "created_at": r[4]} for r in rows]


def _get_open_predictions() -> list:
    """Get open predictions with their current confidence."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, claim, confidence, deadline, created_at "
        "FROM prediction_track WHERE status = 'open' ORDER BY deadline"
    ).fetchall()
    db.close()
    return [{"id": r[0], "claim": r[1], "confidence": r[2],
             "deadline": r[3], "created_at": r[4]} for r in rows]


def trail(pred_id: int):
    """Show the full reasoning trail for a prediction."""
    db = sqlite3.connect(DB_PATH)
    pred = db.execute(
        "SELECT id, claim, confidence, status, outcome, score_notes, deadline "
        "FROM prediction_track WHERE id = ?", (pred_id,)
    ).fetchone()
    db.close()

    if not pred:
        print(f"Prediction #{pred_id} not found.")
        return

    adjustments = _get_adjustment_trail(pred_id)

    print(f"\n{'='*60}")
    print(f"Prediction #{pred[0]}: {pred[1][:80]}")
    print(f"Status: {pred[3]}  |  Current confidence: {pred[2]}  |  Deadline: {pred[6]}")
    if pred[4]:
        print(f"Outcome: {pred[4]}")
    if pred[5]:
        print(f"Score notes: {pred[5][:200]}")
    print(f"{'='*60}")

    if not adjustments:
        print("  No confidence adjustments recorded.")
        return

    print(f"\nReasoning Trail ({len(adjustments)} adjustments):")
    for i, adj in enumerate(adjustments):
        ts = datetime.fromtimestamp(adj["created_at"]).strftime("%m-%d %H:%M")
        direction = "↑" if adj["new"] > adj["old"] else "↓" if adj["new"] < adj["old"] else "→"
        print(f"  {i+1}. [{ts}] {adj['old']:.2f} {direction} {adj['new']:.2f} ({adj['source']})")
        reason = adj["reason"][:120] if adj["reason"] else "(no reason)"
        print(f"     {reason}")

    # Confidence trajectory
    confs = [adjustments[0]["old"]] + [a["new"] for a in adjustments]
    total_drift = confs[-1] - confs[0]
    reversals = sum(1 for i in range(2, len(confs))
                    if (confs[i] - confs[i-1]) * (confs[i-1] - confs[i-2]) < 0)

    print(f"\n  Trajectory: {confs[0]:.2f} → {confs[-1]:.2f} (net drift: {total_drift:+.2f})")
    print(f"  Reversals: {reversals}  |  Adjustments: {len(adjustments)}")
    if reversals > len(adjustments) * 0.5:
        print(f"  ⚠ High reversal rate — reasoning may be oscillating")


def drift():
    """Measure calibration drift — are confidence levels tracking reality?"""
    scored = _get_scored_predictions()
    open_preds = _get_open_predictions()

    print(f"\nCalibration Drift Report")
    print(f"{'='*50}")

    if scored:
        print(f"\nScored predictions ({len(scored)}):")
        correct = sum(1 for p in scored if p["outcome"] and "correct" in p["outcome"].lower())
        incorrect = sum(1 for p in scored if p["outcome"] and "incorrect" in p["outcome"].lower())
        avg_conf = sum(p["confidence"] for p in scored) / len(scored) if scored else 0

        print(f"  Correct: {correct}  |  Incorrect: {incorrect}  |  Total: {len(scored)}")
        print(f"  Average confidence at scoring: {avg_conf:.2f}")
        if correct + incorrect > 0:
            accuracy = correct / (correct + incorrect)
            print(f"  Accuracy: {accuracy:.1%}")
            cal_gap = avg_conf - accuracy
            print(f"  Calibration gap: {cal_gap:+.2f} ({'overconfident' if cal_gap > 0 else 'underconfident'})")

        # Check each scored prediction's adjustment trail
        print(f"\n  Reasoning consistency:")
        for p in scored:
            adjustments = _get_adjustment_trail(p["id"])
            if adjustments:
                reversals = 0
                confs = [adjustments[0]["old"]] + [a["new"] for a in adjustments]
                for i in range(2, len(confs)):
                    if (confs[i] - confs[i-1]) * (confs[i-1] - confs[i-2]) < 0:
                        reversals += 1
                status = "⚠ oscillating" if reversals > 1 else "✓ consistent"
                print(f"    #{p['id']:>2}: {p['claim'][:50]:50} {status} ({len(adjustments)} adj, {reversals} rev)")
            else:
                print(f"    #{p['id']:>2}: {p['claim'][:50]:50} (no adjustments)")
    else:
        print("  No scored predictions yet.")

    if open_preds:
        print(f"\nOpen predictions ({len(open_preds)}):")
        for p in open_preds:
            adjustments = _get_adjustment_trail(p["id"])
            adj_count = len(adjustments)
            confs = [adjustments[0]["old"]] + [a["new"] for a in adjustments] if adjustments else [p["confidence"]]
            total_drift = confs[-1] - confs[0] if len(confs) > 1 else 0
            print(f"    #{p['id']:>2}: conf={p['confidence']:.2f}  drift={total_drift:+.2f}  "
                  f"adj={adj_count}  deadline={p['deadline'][:10]}  {p['claim'][:45]}")


def audit():
    """Audit scored predictions for rationale-outcome alignment."""
    scored = _get_scored_predictions()

    if not scored:
        print("No scored predictions to audit.")
        return

    print(f"\nRationale Audit — {len(scored)} scored predictions")
    print(f"{'='*60}")

    for p in scored:
        adjustments = _get_adjustment_trail(p["id"])
        print(f"\n  #{p['id']}: {p['claim'][:70]}")
        print(f"  Outcome: {p['outcome'] or 'not recorded'}")
        print(f"  Final confidence: {p['confidence']}")

        if adjustments:
            # Show final reasoning vs outcome
            last_reason = adjustments[-1]["reason"][:150] if adjustments[-1]["reason"] else "(none)"
            print(f"  Last rationale: {last_reason}")

            # Flag if reasoning direction contradicted outcome
            final_direction = adjustments[-1]["new"] - adjustments[-1]["old"]
            if p["outcome"]:
                if "correct" in p["outcome"].lower() and final_direction < -0.1:
                    print(f"  ⚠ Last adjustment was DOWNWARD but outcome was correct — right for wrong reason?")
                elif "incorrect" in p["outcome"].lower() and final_direction > 0.1:
                    print(f"  ⚠ Last adjustment was UPWARD but outcome was incorrect — false confidence")
        else:
            print(f"  (no adjustment trail)")


def surprise():
    """Find high-confidence failures — Darby's overfit signal.

    A 'surprise' is a scored prediction where confidence was high (>0.7)
    but the outcome was incorrect. These are the most informative failures:
    the system was confident AND wrong.
    """
    scored = _get_scored_predictions()
    if not scored:
        print("No scored predictions yet.")
        return

    surprises = []
    for p in scored:
        if not p["outcome"]:
            continue
        is_wrong = "incorrect" in p["outcome"].lower() or "wrong" in p["outcome"].lower()
        if is_wrong and p["confidence"] >= 0.7:
            surprises.append(p)

    # Also check: predictions where last adjustment was UP but outcome was incorrect
    false_confidence = []
    for p in scored:
        if not p["outcome"]:
            continue
        is_wrong = "incorrect" in p["outcome"].lower() or "wrong" in p["outcome"].lower()
        if not is_wrong:
            continue
        adjustments = _get_adjustment_trail(p["id"])
        if adjustments and adjustments[-1]["new"] > adjustments[-1]["old"]:
            false_confidence.append({
                "prediction": p,
                "last_adj": adjustments[-1],
            })

    print(f"\nSurprise Report — Darby's overfit detector")
    print(f"{'='*50}")
    print(f"Scored: {len(scored)}  |  High-confidence failures: {len(surprises)}  |  False-confidence: {len(false_confidence)}")

    if surprises:
        print(f"\n⚠ High-confidence failures (conf >= 0.7, outcome incorrect):")
        for p in surprises:
            print(f"  #{p['id']}: conf={p['confidence']:.2f}  {p['claim'][:60]}")
            print(f"    Outcome: {p['outcome'][:100]}")
            trail_adj = _get_adjustment_trail(p["id"])
            if trail_adj:
                confs = [trail_adj[0]["old"]] + [a["new"] for a in trail_adj]
                print(f"    Trail: {' → '.join(f'{c:.2f}' for c in confs)}")
    else:
        print("\n✓ No high-confidence failures. System not overfit (yet).")

    if false_confidence:
        print(f"\n⚠ False confidence (last adjustment UP, outcome wrong):")
        for fc in false_confidence:
            p = fc["prediction"]
            adj = fc["last_adj"]
            print(f"  #{p['id']}: {adj['old']:.2f} → {adj['new']:.2f} then WRONG")
            print(f"    Last reason: {adj['reason'][:100]}")
    elif scored:
        print("✓ No false-confidence patterns.")

    # Category clustering for future use
    if len(scored) >= 5:
        from collections import Counter
        categories = Counter()
        wrong_categories = Counter()
        db = sqlite3.connect(DB_PATH)
        for p in scored:
            cat = db.execute(
                "SELECT category FROM prediction_track WHERE id = ?", (p["id"],)
            ).fetchone()
            cat_name = cat[0] if cat else "general"
            categories[cat_name] += 1
            is_wrong = p["outcome"] and ("incorrect" in p["outcome"].lower() or "wrong" in p["outcome"].lower())
            if is_wrong:
                wrong_categories[cat_name] += 1
        db.close()

        if wrong_categories:
            print(f"\n  Failure by category:")
            for cat, count in wrong_categories.most_common():
                total = categories[cat]
                print(f"    {cat}: {count}/{total} wrong ({count/total:.0%})")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  rationale_check.py audit              # audit scored predictions")
        print("  rationale_check.py drift               # calibration drift report")
        print("  rationale_check.py surprise            # high-confidence failures (Darby's overfit detector)")
        print("  rationale_check.py trail PRED_ID       # reasoning trail for one prediction")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "audit":
        audit()
    elif cmd == "drift":
        drift()
    elif cmd == "surprise":
        surprise()
    elif cmd == "trail":
        if len(sys.argv) < 3:
            print("Usage: rationale_check.py trail PRED_ID")
            sys.exit(1)
        trail(int(sys.argv[2]))
    else:
        print(f"Unknown command: {cmd}")
