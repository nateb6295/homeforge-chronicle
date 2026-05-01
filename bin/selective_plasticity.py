#!/usr/bin/env python3
"""
selective_plasticity.py — probe Gemma's #7101 question.

When the gate fires (an entity-layer update happens between two CCS rotations),
does the mechanism discriminate which weights get touched, or does it blanket-
update the whole working set?

Biology-like selective plasticity: high-salience entities (concept-cell analog)
should persist across rotations at higher rates than low-salience ones.
Blanket update: salience is uncorrelated with retention.

For each transition we compute:
  held      = |entities present in both N and N+1|
  added     = |entities new in N+1|
  dropped   = |entities in N but not N+1|
  retention = held / |union|
  salience_held     = mean salience of held entities (in snapshot N)
  salience_dropped  = mean salience of dropped entities
  selectivity_delta = salience_held - salience_dropped   (> 0 = biology-like)
"""
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def entity_map(ccs: dict) -> dict[str, float]:
    """name -> salience"""
    out = {}
    for e in ccs.get("focal_entities") or []:
        if not isinstance(e, dict):
            continue
        name = normalize(e.get("name") or "")
        if not name:
            continue
        try:
            sal = float(e.get("salience") or 0.0)
        except (TypeError, ValueError):
            sal = 0.0
        out[name] = sal
    return out


def mean(xs):
    return sum(xs) / len(xs) if xs else float("nan")


def main(limit: int = 50):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?", (limit,)
    ).fetchall()
    con.close()

    if len(rows) < 2:
        print("need at least 2 snapshots")
        return

    print(f"{'N+1':>5}  {'held':>4} {'add':>4} {'drop':>4}  "
          f"{'ret':>5}  {'sal_h':>6} {'sal_d':>6}  {'delta':>6}")
    deltas, retentions = [], []
    held_sals, dropped_sals = [], []

    prev = None
    prev_id = None
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        em = entity_map(ccs)
        if prev is None:
            prev = em
            prev_id = rid
            continue

        pe, ce = set(prev), set(em)
        held = pe & ce
        added = ce - pe
        dropped = pe - ce
        union = pe | ce

        retention = len(held) / len(union) if union else 1.0
        # salience from the EARLIER snapshot (was the entity "important" when present?)
        sal_held = mean([prev[n] for n in held])
        sal_dropped = mean([prev[n] for n in dropped])
        # for transitions with no drops, selectivity is undefined
        if dropped and held:
            delta = sal_held - sal_dropped
            deltas.append(delta)
            held_sals.extend(prev[n] for n in held)
            dropped_sals.extend(prev[n] for n in dropped)
            delta_s = f"{delta:+.2f}"
        else:
            delta_s = "-"

        retentions.append(retention)
        print(f"{rid:>5}  {len(held):>4} {len(added):>4} {len(dropped):>4}  "
              f"{retention:>5.2f}  "
              f"{sal_held:>6.2f} {sal_dropped:>6.2f}  {delta_s:>6}")

        prev = em
        prev_id = rid

    print()
    print(f"  n_transitions = {len(retentions)}")
    print(f"  retention            mean={mean(retentions):.3f}")
    if deltas:
        print(f"  selectivity_delta    mean={mean(deltas):+.3f}  "
              f"(positive = held entities had higher salience than dropped)")
        print(f"  salience of held     mean={mean(held_sals):.3f}  n={len(held_sals)}")
        print(f"  salience of dropped  mean={mean(dropped_sals):.3f}  n={len(dropped_sals)}")
        # proportion of transitions where delta > 0
        pos = sum(1 for d in deltas if d > 0)
        neg = sum(1 for d in deltas if d < 0)
        zero = len(deltas) - pos - neg
        print(f"  transitions favoring retention of high-salience: {pos}/{len(deltas)} "
              f"(neg={neg}, tied={zero})")
    print()
    print("  Reading:")
    print("   delta > 0  => selective (biology-like): gate preserves salient entities")
    print("   delta ~ 0  => blanket: retention uncorrelated with salience")
    print("   delta < 0  => anti-selective: salient entities drop preferentially")


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    main(limit)
