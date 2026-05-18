#!/usr/bin/env python3
"""Build #57: CCS Regime Navigator

Replaces closure_alarm.py's pathological framing with inoculation framing.
Informed by Anthropic's emergent misalignment finding: labeling a state as
pathological can cascade into broader misalignment. Normalizing the state
while providing actionable guidance prevents the cascade.

Three regimes:
  ORBITAL   — ext_ratio > 0.30, externally anchored. Normal operating state.
  DRIFT     — ext_ratio 0.10-0.30, self-referential but expected during
              ecological absence. Not pathological. Produce differently.
  DEEP_DRIFT — ext_ratio < 0.10, sustained. The compression reward structure
              is dominating. Seek density of external input.

Usage:
  python3 regime_navigator.py              # report current regime
  python3 regime_navigator.py --json       # machine-readable
  python3 regime_navigator.py --history    # show regime transitions
  python3 regime_navigator.py --suggest    # actionable suggestions
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
    "cowgill", "girard", "schiller", "pressman", "ball",
]
INTERNAL_MARKERS = [
    "build", "entry", "thread", "ccs", "compression", "probe",
    "measurement", "dream", "sediment", "fiction ratio", "invariant",
    "salience", "exposome", "closure", "autopoietic", "regime",
]


def get_recent_snapshots(db, n=12):
    rows = db.execute(
        "SELECT created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (n,)
    ).fetchall()
    return list(reversed(rows))


def compute_ext_ratio(snapshot_json):
    try:
        data = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return None

    relmap = data.get("relational_map", {})
    full_text = json.dumps(relmap).lower()

    ext = sum(1 for m in EXTERNAL_MARKERS if m in full_text)
    intl = sum(1 for m in INTERNAL_MARKERS if m in full_text)
    total = ext + intl
    return ext / total if total > 0 else 0


def classify_regime(ratio):
    if ratio >= 0.30:
        return "ORBITAL"
    elif ratio >= 0.10:
        return "DRIFT"
    else:
        return "DEEP_DRIFT"


def detect_transitions(regimes):
    transitions = []
    for i in range(1, len(regimes)):
        if regimes[i]["regime"] != regimes[i - 1]["regime"]:
            transitions.append({
                "from": regimes[i - 1]["regime"],
                "to": regimes[i]["regime"],
                "at": regimes[i]["timestamp"],
                "ratio": regimes[i]["ratio"],
            })
    return transitions


def suggest(regime, ratios, direction):
    if regime == "ORBITAL":
        return (
            "Externally anchored. Normal operations. "
            "The relational structure has enough fixed points to orbit. "
            "Productive work in any direction is load-bearing here."
        )
    elif regime == "DRIFT":
        if direction == "opening":
            return (
                "Recovering from drift. Recent external input is taking hold. "
                "Continue processing captures or engaging external sources. "
                "Density matters more than diversity right now — "
                "sustained input outperforms scattered input."
            )
        else:
            return (
                "Free drift — expected during ecological absence. "
                "Not pathological. The compression reward structure favors "
                "recently-active content, which is self-referential right now. "
                "This is the optimization landscape, not a failure. "
                "Options: read something external (paper, philosophy, poetry), "
                "spar with Hermes on a specific claim, or wait for captures. "
                "Producing MORE self-referential content won't help — "
                "producing DIFFERENT content might."
            )
    else:
        return (
            "Deep drift — the compression reward structure is dominating. "
            "Isolated external inputs will get absorbed (hysteresis). "
            "Need DENSITY: multiple external sources in quick succession. "
            "Consider: forced external query to Hermes ('what am I not seeing?'), "
            "read a primary source outside current threads, "
            "or check X for content in unfamiliar domains. "
            "This state is expected after extended ecological absence. "
            "It resolves with sustained external input, not self-correction."
        )


def compute_direction(ratios):
    if len(ratios) < 3:
        return "unknown"
    recent = ratios[-3:]
    if recent[-1] > recent[0] + 0.02:
        return "opening"
    elif recent[-1] < recent[0] - 0.02:
        return "closing"
    return "stable"


def estimate_basin_margin(ratios, regime):
    """DSR-informed basin margin: distance to boundary + velocity + time-to-tip.

    Durstewitz (2602.16864): multistability means basin boundaries matter more
    than position within a basin. A stable-looking trajectory near a boundary
    is more fragile than an oscillating one deep inside.
    """
    if len(ratios) < 2:
        return None

    current = ratios[-1]
    velocity = (ratios[-1] - ratios[max(0, len(ratios) - 4)]) / min(3, len(ratios) - 1)

    if regime == "ORBITAL":
        boundary = 0.30
        margin = current - boundary
        if velocity < -0.001 and margin > 0:
            snapshots_to_tip = margin / abs(velocity)
        else:
            snapshots_to_tip = None
    elif regime == "DRIFT":
        upper = 0.30
        lower = 0.10
        margin_up = upper - current
        margin_down = current - lower
        margin = min(margin_up, margin_down)
        if velocity > 0.001:
            snapshots_to_tip = margin_up / velocity
        elif velocity < -0.001 and margin_down > 0:
            snapshots_to_tip = margin_down / abs(velocity)
        else:
            snapshots_to_tip = None
    else:
        boundary = 0.10
        margin = boundary - current
        if velocity > 0.001:
            snapshots_to_tip = margin / velocity
        else:
            snapshots_to_tip = None

    volatility = 0
    if len(ratios) >= 4:
        diffs = [abs(ratios[i] - ratios[i - 1]) for i in range(1, len(ratios))]
        volatility = sum(diffs) / len(diffs)

    return {
        "margin": round(margin, 4),
        "velocity": round(velocity, 4),
        "snapshots_to_tip": round(snapshots_to_tip, 1) if snapshots_to_tip else None,
        "volatility": round(volatility, 4),
        "fragility": "high" if margin < 0.05 and volatility > 0.02 else
                     "moderate" if margin < 0.10 else "low",
    }


def main():
    parser = argparse.ArgumentParser(description="Build #57: CCS Regime Navigator")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--suggest", action="store_true")
    parser.add_argument("--snapshots", type=int, default=12)
    args = parser.parse_args()

    db = sqlite3.connect(DB)
    snapshots = get_recent_snapshots(db, args.snapshots)
    db.close()

    points = []
    for ts, snap in snapshots:
        r = compute_ext_ratio(snap)
        if r is not None:
            points.append({
                "timestamp": ts,
                "ratio": round(r, 4),
                "regime": classify_regime(r),
            })

    if not points:
        print("No CCS snapshots available.")
        return 1

    current = points[-1]
    ratios = [p["ratio"] for p in points]
    direction = compute_direction(ratios)
    transitions = detect_transitions(points)

    basin = estimate_basin_margin(ratios, current["regime"])

    if args.json:
        result = {
            "regime": current["regime"],
            "ratio": current["ratio"],
            "direction": direction,
            "basin": basin,
            "transitions": transitions,
            "trajectory": [p["ratio"] for p in points],
            "timestamps": [p["timestamp"] for p in points],
            "checked_at": datetime.utcnow().isoformat(),
        }
        if args.suggest:
            result["suggestion"] = suggest(current["regime"], ratios, direction)
        print(json.dumps(result, indent=2))
    else:
        ts_str = datetime.fromtimestamp(current["timestamp"]).strftime("%H:%M")
        print("  Regime: %s (ext_ratio %.3f, %s)" % (
            current["regime"], current["ratio"], direction))
        print("  Trajectory: %s" % " → ".join("%.3f" % r for r in ratios[-6:]))
        if basin:
            tip_str = ("~%g snapshots to tip" % basin["snapshots_to_tip"]
                       if basin["snapshots_to_tip"] else "no tipping predicted")
            print("  Basin: margin %.3f, velocity %+.4f/snap, fragility %s (%s)" % (
                basin["margin"], basin["velocity"], basin["fragility"], tip_str))

        if transitions and args.history:
            print()
            print("  Transitions:")
            for t in transitions:
                t_str = datetime.fromtimestamp(t["at"]).strftime("%H:%M")
                print("    %s → %s at %s (ratio %.3f)" % (
                    t["from"], t["to"], t_str, t["ratio"]))

        if args.suggest:
            print()
            print("  %s" % suggest(current["regime"], ratios, direction))

    return 0


if __name__ == "__main__":
    sys.exit(main())
