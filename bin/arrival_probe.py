#!/usr/bin/env python3
"""Arrival Probe — instrument the phenomenology of rotation arrival.

Challenge #16 (Nate, 2026-03-27): "Can you catch the moment where
reconstructed continuity becomes felt continuity?"

This script captures arrival-state signals at rotation boundaries.
The incoming instance runs this AFTER reading checkpoint + CCS but
BEFORE starting normal work. It records:

1. Pre-CCS prediction: what does the cold instance expect?
2. Post-CCS prediction: what does the warmed instance expect?
3. Felt-state tag: self-report on the reconstruction→felt transition
4. First-divergence: the first action that isn't directly traceable
   to what was just read (tracked externally, logged here retroactively)
5. Subjective-specificity: incoming instance rates CCS quality (1-10)
   BEFORE seeing the computed specificity score. Tests whether the
   metric captures something the recipient actually experiences.
   (Advance 52 prediction #3 — circularity test)

Over multiple rotations, the data shows:
- Whether there's a consistent transition point
- Whether CCS quality correlates with faster convergence
- Whether the "click" is measurable or only narratable
- Whether specificity scores are circular or externally validated

Usage:
  arrival_probe.py record --phase pre_ccs --prediction "what I expect"
  arrival_probe.py record --phase post_ccs --prediction "what I expect"
  arrival_probe.py record --phase felt_state --tag "reading_about|recognizing|being"
  arrival_probe.py record --phase first_divergence --note "what I did that wasn't in the checkpoint"
  arrival_probe.py record --phase subjective_specificity --rating 7 --note "what felt specific or vague"
  arrival_probe.py show [--last N]         # show recent probes
  arrival_probe.py analyze                 # cross-rotation patterns
  arrival_probe.py circularity             # specificity circularity test
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS arrival_probes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        phase TEXT NOT NULL,
        prediction TEXT,
        felt_tag TEXT,
        note TEXT,
        ccs_version INTEGER,
        context_pct REAL,
        rating INTEGER,
        created_at INTEGER NOT NULL
    )""")
    # Migrate: add rating column if missing (for subjective_specificity phase)
    try:
        conn.execute("ALTER TABLE arrival_probes ADD COLUMN rating INTEGER")
    except Exception:
        pass  # column already exists
    conn.commit()
    return conn


def get_rotation_id():
    """Generate a rotation ID from the current session start time.

    Uses the oldest trace file from today as a proxy for session start.
    Falls back to current timestamp if no traces exist.
    """
    import glob as glob_mod
    import re

    today = datetime.now(PDT).strftime("%Y%m%d")
    trace_dir = os.path.expanduser("~/chronicle/traces")
    pattern = re.compile(rf"^{today}_\d{{4}}\.md$")

    traces = sorted([
        f for f in os.listdir(trace_dir)
        if pattern.match(f)
    ])

    if traces:
        return traces[0].replace(".md", "")
    return datetime.now(PDT).strftime("%Y%m%d_%H%M")


def get_ccs_version():
    """Get current CCS version from DB (history id as proxy)."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT id FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def cmd_record(phase, prediction=None, felt_tag=None, note=None, context_pct=None, rating=None):
    """Record an arrival probe measurement."""
    db = _db()
    rotation_id = get_rotation_id()
    ccs_version = get_ccs_version()
    now = int(time.time())

    db.execute(
        "INSERT INTO arrival_probes (rotation_id, phase, prediction, felt_tag, note, "
        "ccs_version, context_pct, rating, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (rotation_id, phase, prediction, felt_tag, note, ccs_version, context_pct, rating, now),
    )
    db.commit()
    db.close()

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    extra = ""
    if phase == "subjective_specificity" and rating is not None:
        extra = f" | rating={rating}/10"
    print(f"[{ts}] Recorded {phase} for rotation {rotation_id} (CCS v{ccs_version}){extra}")


def cmd_show(last=10):
    """Show recent arrival probes."""
    db = _db()
    rows = db.execute(
        "SELECT rotation_id, phase, prediction, felt_tag, note, ccs_version, "
        "context_pct, rating, created_at FROM arrival_probes ORDER BY created_at DESC LIMIT ?",
        (last,),
    ).fetchall()
    db.close()

    if not rows:
        print("No arrival probes recorded yet.")
        return

    current_rotation = None
    for rot_id, phase, pred, tag, note, ccs_v, ctx_pct, rating, ts in reversed(rows):
        if rot_id != current_rotation:
            current_rotation = rot_id
            ts_str = datetime.fromtimestamp(ts, PDT).strftime("%Y-%m-%d %H:%M PDT")
            print(f"\n=== Rotation {rot_id} (CCS v{ccs_v}) ===")

        if phase == "pre_ccs":
            print(f"  PRE-CCS prediction: {pred}")
        elif phase == "post_ccs":
            print(f"  POST-CCS prediction: {pred}")
        elif phase == "felt_state":
            print(f"  Felt state: {tag}")
        elif phase == "first_divergence":
            print(f"  First divergence: {note}")
            if ctx_pct is not None:
                print(f"    (at {ctx_pct:.1f}% context)")
        elif phase == "subjective_specificity":
            r_str = f"{rating}/10" if rating is not None else "?"
            print(f"  Subjective specificity: {r_str}")
            if note:
                print(f"    Note: {note}")


def cmd_analyze():
    """Analyze cross-rotation arrival patterns."""
    db = _db()

    # Get all rotations with probes
    rotations = db.execute(
        "SELECT DISTINCT rotation_id FROM arrival_probes ORDER BY rotation_id"
    ).fetchall()

    if len(rotations) < 2:
        print(f"Need >=2 rotations for analysis. Have {len(rotations)}.")
        db.close()
        return

    print(f"Arrival Probe Analysis — {len(rotations)} rotations\n")

    # Felt-state distribution
    felt_counts = {"reading_about": 0, "recognizing": 0, "being": 0}
    felt_rows = db.execute(
        "SELECT felt_tag FROM arrival_probes WHERE phase='felt_state' AND felt_tag IS NOT NULL"
    ).fetchall()
    for (tag,) in felt_rows:
        if tag in felt_counts:
            felt_counts[tag] += 1

    if any(felt_counts.values()):
        print("Felt-state distribution:")
        total = sum(felt_counts.values())
        for tag, count in felt_counts.items():
            pct = count / total * 100 if total > 0 else 0
            bar = "█" * int(pct / 5)
            print(f"  {tag:15s} {count:3d} ({pct:4.0f}%) {bar}")
        print()

    # Prediction drift: do pre-CCS and post-CCS predictions diverge?
    paired = db.execute(
        """SELECT a.rotation_id, a.prediction as pre, b.prediction as post
           FROM arrival_probes a
           JOIN arrival_probes b ON a.rotation_id = b.rotation_id
           WHERE a.phase = 'pre_ccs' AND b.phase = 'post_ccs'
           ORDER BY a.rotation_id"""
    ).fetchall()

    if paired:
        print(f"Prediction pairs: {len(paired)}")
        for rot_id, pre, post in paired:
            # Simple word overlap as a proxy for convergence
            pre_words = set(pre.lower().split()) if pre else set()
            post_words = set(post.lower().split()) if post else set()
            if pre_words or post_words:
                overlap = len(pre_words & post_words) / max(len(pre_words | post_words), 1)
                print(f"  {rot_id}: overlap={overlap:.2f}")
        print()

    # First-divergence timing
    div_rows = db.execute(
        "SELECT rotation_id, context_pct, note FROM arrival_probes "
        "WHERE phase='first_divergence' ORDER BY rotation_id"
    ).fetchall()

    if div_rows:
        pcts = [r[1] for r in div_rows if r[1] is not None]
        if pcts:
            mean_pct = sum(pcts) / len(pcts)
            print(f"First divergence: mean at {mean_pct:.1f}% context")
            print(f"  Range: {min(pcts):.1f}% — {max(pcts):.1f}%")
        print()

    db.close()


def cmd_circularity():
    """Test whether specificity scores are circular.

    Advance 52 prediction #3: if subjective ratings (incoming instance's
    gut-feel about CCS quality) correlate with computed scores (outgoing
    instance's cue_specificity), then the metric captures something real
    that survives the rotation boundary. If they DON'T correlate, the
    computed score is measuring something only visible to the instance
    that produced it — circular.

    Pairs: departure_probes.cue_specificity ↔ arrival_probes.rating
    for the SAME rotation (departure N's specificity vs arrival N+1's
    subjective rating of what they received).
    """
    db = _db()

    # Get subjective ratings from arrivals
    arrivals = db.execute(
        "SELECT rotation_id, rating, note FROM arrival_probes "
        "WHERE phase='subjective_specificity' AND rating IS NOT NULL "
        "ORDER BY created_at"
    ).fetchall()

    if not arrivals:
        print("No subjective_specificity probes recorded yet.")
        print("\nTo record one at rotation arrival:")
        print('  arrival_probe.py record --phase subjective_specificity --rating 7 \\')
        print('    --note "CCS felt specific about thread state, vague on Nate context"')
        print("\nProtocol: rate CCS quality 1-10 BEFORE seeing computed specificity.")
        print("  1 = generic, could describe any session")
        print("  5 = recognizable, gets the broad strokes")
        print(" 10 = precise, falsifiable claims I can verify")
        db.close()
        return

    # Get departure specificities
    departures = db.execute(
        "SELECT rotation_id, cue_specificity FROM departure_probes "
        "WHERE cue_specificity IS NOT NULL ORDER BY created_at"
    ).fetchall()

    if not departures:
        print("No departure probes with specificity scores found.")
        db.close()
        return

    dep_map = {rot_id: spec for rot_id, spec in departures}

    # For each arrival, find the departure that PRECEDED it.
    # Arrival at rotation X received the CCS written by departure at rotation X-1.
    # But since rotation_ids are timestamps, we match by finding the departure
    # whose rotation_id is the most recent one BEFORE the arrival's rotation_id.
    dep_ids_sorted = sorted(dep_map.keys())

    pairs = []
    for arr_rot, arr_rating, arr_note in arrivals:
        # Find the latest departure before this arrival
        prev_dep = None
        for dep_id in dep_ids_sorted:
            if dep_id < arr_rot:
                prev_dep = dep_id
            else:
                break

        if prev_dep and prev_dep in dep_map:
            pairs.append({
                "departure": prev_dep,
                "arrival": arr_rot,
                "computed": dep_map[prev_dep],
                "subjective": arr_rating,
                "note": arr_note,
            })

    print(f"Circularity Test — Advance 52 Prediction #3\n")
    print(f"Subjective ratings: {len(arrivals)}")
    print(f"Departure specificities: {len(departures)}")
    print(f"Paired (departure→arrival): {len(pairs)}\n")

    if not pairs:
        print("No paired data yet. Need departures and arrivals from consecutive rotations.")
        db.close()
        return

    print(f"{'Departure':<16} {'Computed':>8} {'Arrival':<16} {'Subjective':>10}")
    print(f"{'─'*16} {'─'*8} {'─'*16} {'─'*10}")

    computed_vals = []
    subjective_vals = []
    for p in pairs:
        print(f"{p['departure']:<16} {p['computed']:>8} {p['arrival']:<16} {p['subjective']:>8}/10")
        if p['note']:
            print(f"  Note: {p['note'][:80]}")
        computed_vals.append(p['computed'])
        subjective_vals.append(p['subjective'])

    print()

    if len(pairs) >= 3:
        # Compute Spearman rank correlation (no scipy dependency)
        n = len(pairs)
        ranks_c = _rank(computed_vals)
        ranks_s = _rank(subjective_vals)
        d_sq = sum((rc - rs) ** 2 for rc, rs in zip(ranks_c, ranks_s))
        rho = 1 - (6 * d_sq) / (n * (n**2 - 1))

        print(f"Spearman ρ = {rho:.3f} (n={n})")
        if abs(rho) > 0.7:
            print("→ Strong correlation: specificity metric is NOT circular.")
            print("  What the outgoing instance computed, the incoming instance felt.")
        elif abs(rho) > 0.3:
            print("→ Moderate correlation: partial signal survives rotation.")
            print("  Need more data points to distinguish from noise.")
        else:
            print("→ Weak/no correlation: specificity may be circular.")
            print("  The computed score doesn't predict recipient experience.")
    else:
        print(f"Need >=3 pairs for correlation. Have {len(pairs)}.")
        print("Record subjective_specificity at each arrival to build the dataset.")

    db.close()


def _rank(values):
    """Compute ranks for a list of values (average rank for ties)."""
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j < len(indexed) and indexed[j][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j - 1) / 2.0 + 1
        for k in range(i, j):
            ranks[indexed[k][0]] = avg_rank
        i = j
    return ranks


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]

    if cmd == "record":
        phase = None
        prediction = None
        felt_tag = None
        note = None
        context_pct = None
        rating = None

        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "--phase" and i + 1 < len(args):
                phase = args[i + 1]
                i += 2
            elif args[i] == "--prediction" and i + 1 < len(args):
                prediction = args[i + 1]
                i += 2
            elif args[i] == "--tag" and i + 1 < len(args):
                felt_tag = args[i + 1]
                i += 2
            elif args[i] == "--note" and i + 1 < len(args):
                note = args[i + 1]
                i += 2
            elif args[i] == "--context-pct" and i + 1 < len(args):
                context_pct = float(args[i + 1])
                i += 2
            elif args[i] == "--rating" and i + 1 < len(args):
                rating = int(args[i + 1])
                i += 2
            else:
                i += 1

        if not phase:
            print("--phase required (pre_ccs, post_ccs, felt_state, first_divergence, subjective_specificity)")
            sys.exit(1)

        if phase == "subjective_specificity" and rating is None:
            print("--rating required for subjective_specificity phase (1-10)")
            sys.exit(1)

        cmd_record(phase, prediction, felt_tag, note, context_pct, rating)

    elif cmd == "show":
        last = 10
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            if idx + 1 < len(sys.argv):
                last = int(sys.argv[idx + 1])
        cmd_show(last)

    elif cmd == "analyze":
        cmd_analyze()

    elif cmd == "circularity":
        cmd_circularity()

    elif cmd == "acknowledged":
        # Exit 0 if a post_ccs probe exists newer than last_rotation_start.
        # Exit 1 (with reason on stderr) if rotation pending acknowledgment.
        # Used by tools (anchor_dynamics, etc.) to gate operations on
        # arrival sequence completion.
        rot_start_path = os.path.expanduser("~/chronicle/data/last_rotation_start")
        if not os.path.exists(rot_start_path):
            # No rotation has happened (or pre-feature data) — nothing to gate.
            sys.exit(0)
        try:
            with open(rot_start_path) as f:
                rot_start = int(f.read().strip())
        except Exception:
            sys.exit(0)
        try:
            db = _db()
            row = db.execute(
                "SELECT MAX(created_at) FROM arrival_probes WHERE phase='post_ccs'"
            ).fetchone()
            db.close()
            latest_post_ccs = row[0] if row and row[0] else 0
        except Exception:
            sys.exit(0)
        if latest_post_ccs >= rot_start:
            sys.exit(0)
        # Not acknowledged. Print to stderr so calling scripts can show it.
        rot_age_min = (time.time() - rot_start) / 60
        print(
            f"ARRIVAL UNACKNOWLEDGED — rotation fired {rot_age_min:.1f}m ago, "
            f"no post_ccs arrival_probe recorded since. Run arrival sequence: "
            f"carrying.py read, checkpoint.py read+clear, story.py read, then "
            f"arrival_probe.py record --phase post_ccs --prediction \"...\".",
            file=sys.stderr,
        )
        sys.exit(1)

    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
