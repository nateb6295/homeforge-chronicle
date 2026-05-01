#!/usr/bin/env python3
"""CCS Commit Gate — conditional accept/rollback for cognitive state updates.

Inspired by Autogenesis Protocol (AGP, arxiv 2604.15034) operator algebra:
the Commit operator (κ) gates state transitions on quality criteria.

Runs AFTER compress_cognitive_state or update_cognitive_state.
Checks the current CCS against quality criteria. If any check fails,
rolls back to the previous version from cognitive_state_history.

Usage:
  ccs_commit_gate.py check          # Report pass/fail without acting
  ccs_commit_gate.py gate           # Check and rollback if failed
  ccs_commit_gate.py history        # Show recent CCS versions with scores
  ccs_commit_gate.py rollback [N]   # Rollback to version N (or previous)

Checks:
  1. predictive_cue not empty (caught empty-cue bug 2026-04-17)
  2. semantic_gist present and non-trivial
  3. focal_entities has at least one entity with salience >= 0.8
  4. episodic_trace has at least one entry
  5. specificity score >= 4 (reuses departure_probe scorer)
"""

import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

WEBHOOK = None


def _load_webhook():
    global WEBHOOK
    if WEBHOOK:
        return WEBHOOK
    WEBHOOK = os.environ.get("OPERATOR_WEBHOOK")
    if not WEBHOOK:
        env_path = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(env_path):
            for line in open(env_path):
                if line.startswith("OPERATOR_WEBHOOK="):
                    WEBHOOK = line.split("=", 1)[1].strip()
                    break
    return WEBHOOK


def _post_discord(msg):
    webhook = _load_webhook()
    if not webhook:
        return
    import urllib.request
    payload = json.dumps({"content": msg[:1900]})
    try:
        req = urllib.request.Request(
            webhook,
            data=payload.encode(),
            headers={"Content-Type": "application/json", "User-Agent": "ccs-gate/1.0"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:
        pass


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_current_ccs():
    """Get current cognitive state as dict."""
    db = _db()
    row = db.execute(
        "SELECT semantic_gist, predictive_cue, focal_entities, episodic_trace, "
        "goal_orientation, version, updated_at FROM cognitive_state WHERE id=1"
    ).fetchone()
    db.close()
    if not row:
        return None
    return {
        "semantic_gist": row[0],
        "predictive_cue": row[1],
        "focal_entities": json.loads(row[2]) if row[2] else [],
        "episodic_trace": json.loads(row[3]) if row[3] else [],
        "goal_orientation": row[4],
        "version": row[5],
        "updated_at": row[6],
    }


def get_previous_snapshot():
    """Get the second-most-recent CCS snapshot from history."""
    db = _db()
    rows = db.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 2"
    ).fetchall()
    db.close()
    if len(rows) < 2:
        return None
    hist_id, snapshot_json, created_at, trigger = rows[1]
    return {
        "id": hist_id,
        "snapshot": json.loads(snapshot_json),
        "created_at": created_at,
        "trigger": trigger,
    }


def get_snapshot_by_id(version_id):
    """Get a specific CCS snapshot from history."""
    db = _db()
    row = db.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history "
        "WHERE id=?", (version_id,)
    ).fetchone()
    db.close()
    if not row:
        return None
    return {
        "id": row[0],
        "snapshot": json.loads(row[1]),
        "created_at": row[2],
        "trigger": row[3],
    }


def score_specificity(cue):
    """Count falsifiable claims in predictive_cue. From departure_probe.py."""
    if not cue:
        return 0
    score = 0
    numbers = re.findall(r'\b\d+\b', cue)
    score += len(numbers)
    entities = re.findall(
        r'(?:Thread|Hermes|Gemma|Nate|Opus|Chronicle|CCS|HAL)\b',
        cue, re.IGNORECASE
    )
    score += len(set(entities))
    states = re.findall(
        r'\b(?:healthy|running|active|stable|broken|down|green|red|stale|fresh)\b',
        cue, re.IGNORECASE
    )
    score += len(states)
    paths = re.findall(r'[a-z_]+\.(?:py|md|json|toml)\b', cue)
    score += len(paths)
    return score


def run_checks(ccs):
    """Run quality checks on current CCS. Returns list of (check_name, passed, detail)."""
    results = []

    # 1. Predictive cue not empty
    cue = ccs.get("predictive_cue", "")
    cue_ok = bool(cue and len(cue.strip()) > 10)
    results.append(("predictive_cue_present", cue_ok,
                     f"{len(cue)} chars" if cue else "EMPTY"))

    # 2. Semantic gist present and non-trivial
    gist = ccs.get("semantic_gist", "")
    gist_ok = bool(gist and len(gist.strip()) > 20)
    results.append(("semantic_gist_present", gist_ok,
                     f"{len(gist)} chars" if gist else "EMPTY"))

    # 3. Focal entities has at least one high-salience entity
    entities = ccs.get("focal_entities", [])
    high_salience = [e for e in entities if e.get("salience", 0) >= 0.8]
    entities_ok = len(high_salience) >= 1
    results.append(("high_salience_entity", entities_ok,
                     f"{len(high_salience)} entities >= 0.8 salience"))

    # 4. Episodic trace has content
    trace = ccs.get("episodic_trace", [])
    trace_ok = len(trace) >= 1
    results.append(("episodic_trace_present", trace_ok,
                     f"{len(trace)} entries"))

    # 5. Specificity score >= 4
    spec = score_specificity(cue)
    spec_ok = spec >= 4
    results.append(("specificity_threshold", spec_ok,
                     f"score={spec} (threshold=4)"))

    return results


def cmd_check():
    """Report pass/fail without acting."""
    ccs = get_current_ccs()
    if not ccs:
        print("No cognitive state found.")
        return False

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    print(f"CCS Commit Gate — {ts} (v{ccs['version']})\n")

    results = run_checks(ccs)
    all_passed = True
    for name, passed, detail in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"  [{status}] {name}: {detail}")

    print()
    if all_passed:
        print("VERDICT: COMMIT — all checks passed")
    else:
        failed = [name for name, passed, _ in results if not passed]
        print(f"VERDICT: ROLLBACK recommended — failed: {', '.join(failed)}")

    return all_passed


def cmd_gate():
    """Check and rollback if failed."""
    ccs = get_current_ccs()
    if not ccs:
        print("No cognitive state found.")
        return

    results = run_checks(ccs)
    all_passed = all(passed for _, passed, _ in results)

    if all_passed:
        ts = datetime.now(PDT).strftime("%H:%M PDT")
        print(f"[{ts}] CCS v{ccs['version']} — COMMITTED (all checks passed)")
        spec = score_specificity(ccs.get("predictive_cue", ""))
        print(f"  Specificity: {spec}")
        return

    # Failed — rollback
    failed = [(name, detail) for name, passed, detail in results if not passed]
    print(f"CCS v{ccs['version']} FAILED gate checks:")
    for name, detail in failed:
        print(f"  ✗ {name}: {detail}")

    prev = get_previous_snapshot()
    if not prev:
        print("\n  Cannot rollback: no previous snapshot in history")
        _post_discord(
            f"**CCS Gate FAILED** — v{ccs['version']} failed checks but no previous snapshot for rollback.\n"
            + "\n".join(f"  ✗ {n}: {d}" for n, d in failed)
        )
        return

    # Restore previous snapshot
    snapshot = prev["snapshot"]
    db = _db()
    db.execute(
        "UPDATE cognitive_state SET "
        "semantic_gist=?, predictive_cue=?, focal_entities=?, episodic_trace=?, "
        "goal_orientation=?, constraints=?, uncertainty_signals=?, relational_map=?, "
        "retrieved_artifacts=?, updated_at=?, compression_model=?, version=? "
        "WHERE id=1",
        (
            snapshot.get("semantic_gist", ""),
            snapshot.get("predictive_cue", ""),
            json.dumps(snapshot.get("focal_entities", [])),
            json.dumps(snapshot.get("episodic_trace", [])),
            snapshot.get("goal_orientation", ""),
            json.dumps(snapshot.get("constraints", [])),
            json.dumps(snapshot.get("uncertainty_signals", [])),
            json.dumps(snapshot.get("relational_map", {})),
            json.dumps(snapshot.get("retrieved_artifacts", [])),
            int(time.time()),
            snapshot.get("compression_model"),
            prev["id"],
        ),
    )
    db.commit()
    db.close()

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    print(f"\n  ROLLED BACK to history snapshot {prev['id']} (from {prev['trigger']})")
    print(f"  [{ts}] CCS restored to previous version")

    _post_discord(
        f"**CCS Gate — ROLLBACK** v{ccs['version']} → snapshot {prev['id']}\n"
        f"Failed: {', '.join(n for n, _ in failed)}\n"
        f"Previous state restored. Gate is working."
    )


def cmd_history(last=10):
    """Show recent CCS versions with quality scores."""
    db = _db()
    rows = db.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (last,)
    ).fetchall()
    db.close()

    if not rows:
        print("No CCS history.")
        return

    print(f"CCS Version History (last {len(rows)})\n")
    print(f"{'ID':>5} {'Time':<18} {'Trigger':<14} {'Spec':>4} {'Gist':<50}")
    print(f"{'─'*5} {'─'*18} {'─'*14} {'─'*4} {'─'*50}")

    for hist_id, snapshot_json, created_at, trigger in reversed(rows):
        snap = json.loads(snapshot_json)
        ts = datetime.fromtimestamp(created_at, PDT).strftime("%m-%d %H:%M PDT")
        cue = snap.get("predictive_cue", "")
        spec = score_specificity(cue)
        gist = snap.get("semantic_gist", "")[:50]
        trigger_str = (trigger or "?")[:14]
        print(f"{hist_id:>5} {ts:<18} {trigger_str:<14} {spec:>4} {gist}")


def cmd_rollback(version_id=None):
    """Rollback to a specific version or previous."""
    if version_id:
        snap = get_snapshot_by_id(version_id)
        if not snap:
            print(f"No snapshot with id {version_id}")
            return
    else:
        snap = get_previous_snapshot()
        if not snap:
            print("No previous snapshot available")
            return

    snapshot = snap["snapshot"]
    db = _db()
    db.execute(
        "UPDATE cognitive_state SET "
        "semantic_gist=?, predictive_cue=?, focal_entities=?, episodic_trace=?, "
        "goal_orientation=?, constraints=?, uncertainty_signals=?, relational_map=?, "
        "retrieved_artifacts=?, updated_at=?, compression_model=?, version=? "
        "WHERE id=1",
        (
            snapshot.get("semantic_gist", ""),
            snapshot.get("predictive_cue", ""),
            json.dumps(snapshot.get("focal_entities", [])),
            json.dumps(snapshot.get("episodic_trace", [])),
            snapshot.get("goal_orientation", ""),
            json.dumps(snapshot.get("constraints", [])),
            json.dumps(snapshot.get("uncertainty_signals", [])),
            json.dumps(snapshot.get("relational_map", {})),
            json.dumps(snapshot.get("retrieved_artifacts", [])),
            int(time.time()),
            snapshot.get("compression_model"),
            snap["id"],
        ),
    )
    db.commit()
    db.close()

    ts = datetime.now(PDT).strftime("%H:%M PDT")
    print(f"[{ts}] Rolled back to snapshot {snap['id']} ({snap['trigger']})")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]

    if cmd == "check":
        passed = cmd_check()
        sys.exit(0 if passed else 1)

    elif cmd == "gate":
        cmd_gate()

    elif cmd == "history":
        last = 10
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            if idx + 1 < len(sys.argv):
                last = int(sys.argv[idx + 1])
        cmd_history(last)

    elif cmd == "rollback":
        version_id = None
        if len(sys.argv) > 2:
            version_id = int(sys.argv[2])
        cmd_rollback(version_id)

    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
