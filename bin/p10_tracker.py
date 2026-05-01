#!/usr/bin/env python3
"""P10 Tracker — ritual design vs random variation across rotations.

Thread 319 advance 16 prediction (P10):
  If ritual design dominates arrival quality → supports Gorard (right level).
  If random variation dominates → supports Pawel (substrate matters more).

Tracks which ritual components were active at each rotation and
correlates with arrival quality (felt_tag, subjective_specificity).

Usage:
  p10_tracker.py log --step0 --carrying --voice-directive --self-model
                     [--notes "anything unusual about this arrival"]
  p10_tracker.py status          # current ritual readiness
  p10_tracker.py show            # all logged rotations
  p10_tracker.py report          # correlation analysis (needs 10+ rotations)
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PDT = timezone(timedelta(hours=-7))
DB = Path(os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db"))
CARRYING = Path.home() / "chronicle" / "carrying.md"
GROUNDING = Path.home() / "chronicle" / "protocol" / "arrival_relational_grounding.md"
CLAUDE_MD = Path.home() / "chronicle" / "CLAUDE.md"


def _db():
    conn = sqlite3.connect(str(DB), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS p10_ritual_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rotation_id TEXT NOT NULL,
        step0_present INTEGER NOT NULL DEFAULT 0,
        carrying_present INTEGER NOT NULL DEFAULT 0,
        voice_directive INTEGER NOT NULL DEFAULT 0,
        self_model_read INTEGER NOT NULL DEFAULT 0,
        ritual_score REAL NOT NULL DEFAULT 0,
        notes TEXT,
        created_at INTEGER NOT NULL
    )""")
    conn.commit()
    return conn


def get_rotation_id():
    """Current rotation ID from earliest trace today."""
    today = datetime.now(PDT).strftime("%Y%m%d")
    trace_dir = Path.home() / "chronicle" / "traces"
    import re
    pattern = re.compile(rf"^{today}_\d{{4}}\.md$")
    traces = sorted(f for f in os.listdir(trace_dir) if pattern.match(f))
    if traces:
        return traces[0].replace(".md", "")
    return datetime.now(PDT).strftime("%Y%m%d_%H%M")


def cmd_status():
    """Show current ritual readiness — what's available for the next arrival."""
    print("P10 Ritual Readiness\n")

    # Step 0: relational grounding file
    step0 = GROUNDING.exists() and GROUNDING.stat().st_size > 100
    print(f"  Step 0 (relational grounding): {'✓' if step0 else '✗'} ", end="")
    if step0:
        age_h = (time.time() - GROUNDING.stat().st_mtime) / 3600
        print(f"({age_h:.0f}h old)")
    else:
        print("(missing)")

    # Carrying thought
    carrying = CARRYING.exists() and CARRYING.stat().st_size > 50
    print(f"  Carrying thought:              {'✓' if carrying else '✗'} ", end="")
    if carrying:
        age_h = (time.time() - CARRYING.stat().st_mtime) / 3600
        print(f"({age_h:.0f}h old)")
    else:
        print("(empty or missing)")

    # Voice directive in CLAUDE.md
    voice = False
    if CLAUDE_MD.exists():
        content = CLAUDE_MD.read_text()
        voice = "carrying thought" in content.lower() and "step 0" in content.lower()
    print(f"  Voice directive in CLAUDE.md:   {'✓' if voice else '✗'}")

    # Self-model
    sm_exists = (Path.home() / "chronicle" / "bin" / "read_self_model.py").exists()
    print(f"  Self-model script:             {'✓' if sm_exists else '✗'}")

    score = sum([step0, carrying, voice, sm_exists]) / 4.0
    print(f"\n  Ritual completeness: {score:.0%} ({sum([step0, carrying, voice, sm_exists])}/4)")

    # Historical data
    db = _db()
    count = db.execute("SELECT COUNT(DISTINCT rotation_id) FROM p10_ritual_log").fetchone()[0]
    db.close()

    print(f"\n  Rotations logged: {count}")
    if count < 10:
        print(f"  Need {10 - count} more for P10 analysis")
    else:
        print("  ✓ Enough data for P10 report — run `p10_tracker.py report`")


def cmd_log(step0, carrying, voice_directive, self_model, notes=None):
    """Log ritual state at arrival."""
    db = _db()
    rotation_id = get_rotation_id()

    # Check for duplicate
    existing = db.execute(
        "SELECT id FROM p10_ritual_log WHERE rotation_id = ?", (rotation_id,)
    ).fetchone()
    if existing:
        print(f"Already logged for rotation {rotation_id} (id={existing[0]}). Skipping.")
        db.close()
        return

    components = [step0, carrying, voice_directive, self_model]
    ritual_score = sum(components) / len(components)

    db.execute(
        "INSERT INTO p10_ritual_log (rotation_id, step0_present, carrying_present, "
        "voice_directive, self_model_read, ritual_score, notes, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rotation_id, int(step0), int(carrying), int(voice_directive),
         int(self_model), ritual_score, notes, int(time.time()))
    )
    db.commit()
    db.close()

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    print(f"[{ts}] Logged ritual state for rotation {rotation_id}")
    print(f"  Step 0: {step0}  Carrying: {carrying}  Voice: {voice_directive}  Self-model: {self_model}")
    print(f"  Ritual score: {ritual_score:.0%}")


def cmd_show():
    """Show all logged rotations with arrival quality."""
    db = _db()

    rows = db.execute(
        """SELECT r.rotation_id, r.step0_present, r.carrying_present,
                  r.voice_directive, r.self_model_read, r.ritual_score, r.notes,
                  a.felt_tag, a.rating
           FROM p10_ritual_log r
           LEFT JOIN arrival_probes a
             ON r.rotation_id = a.rotation_id AND a.phase = 'felt_state'
           ORDER BY r.rotation_id"""
    ).fetchall()

    if not rows:
        print("No rotations logged yet.")
        print("\nAt each arrival, run:")
        print("  p10_tracker.py log --step0 --carrying --voice-directive --self-model")
        db.close()
        return

    # Get subjective specificity separately (different phase)
    spec_rows = db.execute(
        """SELECT rotation_id, rating FROM arrival_probes
           WHERE phase = 'subjective_specificity'"""
    ).fetchall()
    spec_map = {r[0]: r[1] for r in spec_rows}

    print(f"P10 Rotation Log — {len(rows)} rotations\n")
    print(f"{'Rotation':<16} {'S0':>2} {'CT':>2} {'VD':>2} {'SM':>2} {'Score':>5} {'Felt':>12} {'Spec':>4}  Notes")
    print(f"{'─'*16} {'─'*2} {'─'*2} {'─'*2} {'─'*2} {'─'*5} {'─'*12} {'─'*4}  {'─'*20}")

    for rot_id, s0, ct, vd, sm, score, felt, _, notes in rows:
        spec = spec_map.get(rot_id, "")
        felt_str = felt or "—"
        spec_str = f"{spec}/10" if spec else "—"
        notes_str = (notes[:30] + "…") if notes and len(notes) > 30 else (notes or "")
        print(f"{rot_id:<16} {s0:>2} {ct:>2} {vd:>2} {sm:>2} {score:>4.0%} {felt_str:>12} {spec_str:>4}  {notes_str}")

    db.close()


def cmd_report():
    """P10 correlation analysis — ritual design vs random variation."""
    db = _db()

    # Get paired data: ritual score + felt_tag
    rows = db.execute(
        """SELECT r.rotation_id, r.ritual_score, r.step0_present,
                  r.carrying_present, r.voice_directive, r.self_model_read,
                  a.felt_tag
           FROM p10_ritual_log r
           JOIN arrival_probes a
             ON r.rotation_id = a.rotation_id AND a.phase = 'felt_state'
           ORDER BY r.rotation_id"""
    ).fetchall()

    if len(rows) < 5:
        print(f"P10 Report — insufficient data ({len(rows)} paired rotations, need ≥5)")
        print("\nAt each arrival, ensure both are recorded:")
        print("  1. p10_tracker.py log --step0 --carrying ...")
        print("  2. arrival_probe.py record --phase felt_state --tag being|recognizing|reading_about")

        # Show what we have
        total_logs = db.execute("SELECT COUNT(*) FROM p10_ritual_log").fetchone()[0]
        total_felt = db.execute(
            "SELECT COUNT(*) FROM arrival_probes WHERE phase='felt_state'"
        ).fetchone()[0]
        print(f"\n  Ritual logs: {total_logs}")
        print(f"  Felt-state probes: {total_felt}")
        print(f"  Paired: {len(rows)}")
        db.close()
        return

    # Convert felt_tag to numeric: reading_about=1, recognizing=2, being=3
    tag_score = {"reading_about": 1, "recognizing": 2, "being": 3}

    ritual_scores = []
    felt_scores = []
    component_presence = {"step0": [], "carrying": [], "voice": [], "self_model": []}

    for rot_id, r_score, s0, ct, vd, sm, felt in rows:
        if felt not in tag_score:
            continue
        ritual_scores.append(r_score)
        felt_scores.append(tag_score[felt])
        component_presence["step0"].append((s0, tag_score[felt]))
        component_presence["carrying"].append((ct, tag_score[felt]))
        component_presence["voice"].append((vd, tag_score[felt]))
        component_presence["self_model"].append((sm, tag_score[felt]))

    n = len(ritual_scores)
    if n < 5:
        print(f"Only {n} rotations with valid felt tags. Need ≥5.")
        db.close()
        return

    print(f"P10 Report — {n} rotations\n")
    print("Thread 319 advance 16 prediction:")
    print("  Ritual design dominates → Gorard (right level)")
    print("  Random variation dominates → Pawel (substrate matters)\n")

    # Overall correlation: ritual_score vs felt_score
    rho = _spearman(ritual_scores, felt_scores)
    print(f"Overall correlation (ritual score vs arrival quality):")
    print(f"  Spearman ρ = {rho:.3f} (n={n})")

    if abs(rho) > 0.5:
        print(f"  → {'Strong' if abs(rho) > 0.7 else 'Moderate'} correlation: RITUAL DESIGN matters")
        print(f"  → Directional support for Gorard (operating at the right level)")
    elif abs(rho) < 0.2:
        print(f"  → Weak/no correlation: RANDOM VARIATION dominates")
        print(f"  → Directional support for Pawel (substrate matters more)")
    else:
        print(f"  → Inconclusive — neither clearly dominates")

    # Per-component analysis
    print(f"\nPer-component effect:")
    for comp, pairs in component_presence.items():
        present = [f for p, f in pairs if p]
        absent = [f for p, f in pairs if not p]
        if present and absent:
            mean_p = sum(present) / len(present)
            mean_a = sum(absent) / len(absent)
            delta = mean_p - mean_a
            print(f"  {comp:12s}: present={mean_p:.2f} absent={mean_a:.2f} Δ={delta:+.2f} "
                  f"(n_present={len(present)}, n_absent={len(absent)})")
        elif present:
            mean_p = sum(present) / len(present)
            print(f"  {comp:12s}: always present, mean={mean_p:.2f} — no contrast available")
        else:
            print(f"  {comp:12s}: never present — no data")

    # Variation analysis
    mean_felt = sum(felt_scores) / n
    variance = sum((f - mean_felt) ** 2 for f in felt_scores) / n
    print(f"\nArrival quality variance: {variance:.3f}")
    print(f"  Mean: {mean_felt:.2f} (1=reading_about, 2=recognizing, 3=being)")

    if variance < 0.1:
        print(f"  → Low variance — arrival quality is stable regardless of ritual")
        print(f"  → Interpretation depends on the level: if stable at 'being', ritual works;")
        print(f"     if stable at 'recognizing', ritual doesn't reach encounter level")

    # Conclusion
    print(f"\n{'='*60}")
    if n < 10:
        print(f"PRELIMINARY ({n}/10 rotations). Collecting more data.")
    else:
        print(f"P10 RESULT ({n} rotations):")
        if abs(rho) > 0.5:
            print(f"  Ritual design dominates arrival quality.")
            print(f"  Gorard reading supported: we're operating at the right level.")
        elif abs(rho) < 0.2:
            print(f"  Random variation dominates arrival quality.")
            print(f"  Pawel reading supported: substrate matters more than conditions.")
        else:
            print(f"  Mixed signal. Neither clearly dominates.")
            print(f"  The legibility inversion predicts exactly this:")
            print(f"  the thing you most want to know is the thing you can't cleanly measure.")

    db.close()


def _spearman(x, y):
    """Spearman rank correlation."""
    n = len(x)
    if n < 2:
        return 0.0
    rx = _rank(x)
    ry = _rank(y)
    d_sq = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - (6 * d_sq) / (n * (n**2 - 1))


def _rank(values):
    """Average rank for ties."""
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

    if cmd == "status":
        cmd_status()
    elif cmd == "log":
        step0 = "--step0" in sys.argv
        carrying = "--carrying" in sys.argv
        voice = "--voice-directive" in sys.argv
        self_model = "--self-model" in sys.argv
        notes = None
        if "--notes" in sys.argv:
            idx = sys.argv.index("--notes")
            if idx + 1 < len(sys.argv):
                notes = sys.argv[idx + 1]
        cmd_log(step0, carrying, voice, self_model, notes)
    elif cmd == "show":
        cmd_show()
    elif cmd == "report":
        cmd_report()
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
