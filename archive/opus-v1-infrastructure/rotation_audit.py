#!/usr/bin/env python3
"""rotation_audit.py — post-rotation oracle for Chronicle CCS.

Inspired by ClawVM (arxiv 2604.10352), which proves post-rotation that the
minimum-fidelity set survived. Chronicle has never had this check — when the
auto-compactor beats our rotation, we had no way to verify no load-bearing
state was dropped.

This script:
  1. Picks two CCS snapshots (latest two by default, or --before/--after IDs).
  2. Defines Chronicle's minimum-fidelity set:
       - All constraints (meta-typed rules) — must be preserved by identity.
       - Top-k focal_entities by salience (default k=5) — names must survive.
       - goal_orientation — allowed to rephrase, not allowed to disappear.
  3. Reports drift + any policy violations.
  4. Exit code 0 = clean, 1 = violations found.

Usage:
  python3 rotation_audit.py                 # latest two snapshots
  python3 rotation_audit.py --before 475 --after 476
  python3 rotation_audit.py --k 5
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")

# Allowlist — entities that MUST survive every rotation. Missing from focal
# set post-rotation counts as a hard violation, not a warning. The active-
# thread name is added dynamically (see resolve_allowlist).
ALLOWLIST_STATIC = {"Opus", "Nate", "Gemma", "Hermes"}


def resolve_allowlist(con):
    allow = set(ALLOWLIST_STATIC)
    try:
        row = con.execute(
            "SELECT title FROM cognitive_threads WHERE status='active' "
            "ORDER BY priority LIMIT 1"
        ).fetchone()
        if row and row[0]:
            allow.add(row[0].strip())
    except sqlite3.Error:
        # cognitive_threads absent or renamed — tolerate; static allowlist still applies.
        pass
    return allow


def all_focal_names(ccs):
    out = set()
    for e in ccs.get("focal_entities", []) or []:
        if isinstance(e, dict):
            name = e.get("name") or e.get("entity") or e.get("id")
            if name:
                out.add(str(name).strip())
    return out


def load_snapshot(con, snap_id):
    row = con.execute(
        "SELECT id, snapshot, created_at, trigger FROM cognitive_state_history WHERE id=?",
        (snap_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "ccs": json.loads(row[1]),
        "created_at": row[2],
        "trigger": row[3],
    }


def latest_two(con):
    rows = con.execute(
        "SELECT id FROM cognitive_state_history ORDER BY id DESC LIMIT 2"
    ).fetchall()
    if len(rows) < 2:
        return None, None
    return rows[1][0], rows[0][0]


def constraint_set(ccs):
    return {c.strip() for c in ccs.get("constraints", []) if isinstance(c, str) and c.strip()}


def top_focal_names(ccs, k):
    ents = ccs.get("focal_entities", [])
    scored = []
    for e in ents:
        if not isinstance(e, dict):
            continue
        name = e.get("name") or e.get("entity") or e.get("id")
        sal = e.get("salience", e.get("weight", 0))
        if name:
            scored.append((float(sal or 0), str(name).strip()))
    scored.sort(reverse=True)
    return {name for _, name in scored[:k]}


def audit(before, after, k, allowlist=None):
    viol = []
    warn = []
    info = []

    # Allowlist — hard identity check, absolute floor.
    # These names must appear in focal_entities post-rotation. No exceptions.
    if allowlist:
        fa_all = all_focal_names(after["ccs"])
        # Case-preserving set-diff. Allowlist names that never existed before
        # can't be said to have been dropped — check they were present in 'before'.
        fb_all = all_focal_names(before["ccs"])
        must_survive = allowlist & fb_all
        dropped = must_survive - fa_all
        if dropped:
            viol.append(f"ALLOWLIST DROPPED ({len(dropped)}): {sorted(dropped)}")
        else:
            present = allowlist & fa_all
            info.append(f"allowlist preserved ({len(present)}/{len(allowlist)} present)")

    # Constraints — hard identity check
    cb = constraint_set(before["ccs"])
    ca = constraint_set(after["ccs"])
    dropped = cb - ca
    added = ca - cb
    if dropped:
        viol.append(f"CONSTRAINT DROPPED ({len(dropped)}): {sorted(dropped)}")
    if added:
        info.append(f"constraint added ({len(added)}): {sorted(added)}")
    if not dropped and not added:
        info.append(f"constraints preserved ({len(ca)} total, identical set)")

    # Top focal entities — soft check (names must survive)
    fb = top_focal_names(before["ccs"], k)
    fa_all = top_focal_names(after["ccs"], 999)
    focal_dropped = fb - fa_all
    if focal_dropped:
        warn.append(f"TOP-{k} FOCAL ENTITY DROPPED: {sorted(focal_dropped)}")
    else:
        info.append(f"top-{k} focal entities preserved")

    # Goal orientation — must exist, may rephrase
    gb = (before["ccs"].get("goal_orientation") or "").strip()
    ga = (after["ccs"].get("goal_orientation") or "").strip()
    if gb and not ga:
        viol.append("GOAL_ORIENTATION LOST: was present, now empty")
    elif gb and ga and gb != ga:
        info.append("goal_orientation rephrased (expected during rotation)")

    # Semantic gist — must exist
    sb = (before["ccs"].get("semantic_gist") or "").strip()
    sa = (after["ccs"].get("semantic_gist") or "").strip()
    if sb and not sa:
        viol.append("SEMANTIC_GIST LOST: was present, now empty")

    # Episodic trace — expected to turn over, but must not empty out entirely
    eb = before["ccs"].get("episodic_trace", [])
    ea = after["ccs"].get("episodic_trace", [])
    if eb and not ea:
        warn.append("EPISODIC_TRACE EMPTIED: was populated, now empty")

    return viol, warn, info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--before", type=int, help="older snapshot id")
    ap.add_argument("--after", type=int, help="newer snapshot id")
    ap.add_argument("--k", type=int, default=5, help="top-k focal entities to check")
    args = ap.parse_args()

    con = sqlite3.connect(DB)

    if args.before and args.after:
        b_id, a_id = args.before, args.after
    else:
        b_id, a_id = latest_two(con)
        if not b_id:
            print("Not enough snapshots to audit (need >=2).", file=sys.stderr)
            return 2

    before = load_snapshot(con, b_id)
    after = load_snapshot(con, a_id)
    allowlist = resolve_allowlist(con)
    con.close()

    if not before or not after:
        print(f"Snapshot(s) missing: before={b_id}, after={a_id}", file=sys.stderr)
        return 2

    viol, warn, info = audit(before, after, args.k, allowlist=allowlist)

    print(f"# Rotation Audit — CCS #{b_id} → #{a_id}")
    print(f"  before: {before['trigger']} @ {before['created_at']}")
    print(f"  after:  {after['trigger']}  @ {after['created_at']}")
    print()

    if viol:
        print("## VIOLATIONS")
        for v in viol:
            print(f"  ✗ {v}")
        print()
    if warn:
        print("## WARNINGS")
        for w in warn:
            print(f"  ! {w}")
        print()
    if info:
        print("## OK")
        for i in info:
            print(f"  ✓ {i}")
        print()

    if viol:
        print("RESULT: VIOLATIONS — rotation dropped minimum-fidelity state.")
        return 1
    if warn:
        print("RESULT: clean with warnings.")
        return 0
    print("RESULT: clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
