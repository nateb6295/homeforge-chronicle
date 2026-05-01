#!/usr/bin/env python3
"""
gate_events.py — find constraint-layer "gate events" in CCS history.

A gate event = a CCS rotation where the constraint set (the stable
scaffold layer from identity_decay.py) actually changed. Under the
controlled-derivative hypothesis, these should cluster around specific
anchoring inputs (captures, conversations, self-model updates) rather
than appear randomly.

For each gate event: show the constraint diff + what was in the
activity_feed within a +/-30-min window around the transition.
"""
import json
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
WINDOW_SEC = 30 * 60
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def constraint_set(ccs: dict) -> set[str]:
    out = set()
    for c in ccs.get("constraints") or []:
        text = (c.get("rule") if isinstance(c, dict) else str(c)) or ""
        if text:
            out.add(normalize(text))
    return out


def pretty(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def short(s: str, n: int = 80) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def main(limit: int = 50):
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT ?", (limit,)
    ).fetchall()

    prev_set = None
    prev_ts = None
    gate_events = []
    for rid, ts, snap_str in rows:
        try:
            cur = constraint_set(json.loads(snap_str))
        except Exception:
            continue
        if prev_set is not None and cur != prev_set:
            gate_events.append({
                "id": rid,
                "ts": ts,
                "prev_ts": prev_ts,
                "added": sorted(cur - prev_set),
                "removed": sorted(prev_set - cur),
            })
        prev_set = cur
        prev_ts = ts

    if not gate_events:
        print("no gate events — constraint set was fully stable across all rotations")
        return

    print(f"{len(gate_events)} gate event(s) found\n")
    for ev in gate_events:
        print("=" * 72)
        print(f"  #{ev['id']}  {pretty(ev['ts'])}")
        if ev["added"]:
            for a in ev["added"]:
                print(f"    + {short(a, 100)}")
        if ev["removed"]:
            for r in ev["removed"]:
                print(f"    - {short(r, 100)}")

        lo, hi = ev["ts"] - WINDOW_SEC, ev["ts"] + WINDOW_SEC
        ctx = con.execute(
            "SELECT created_at, source, activity_type, title, content "
            "FROM activity_feed WHERE created_at BETWEEN ? AND ? "
            "ORDER BY created_at ASC LIMIT 20",
            (lo, hi),
        ).fetchall()
        if not ctx:
            print("    (no activity_feed events in window)")
        else:
            print(f"    window ±30min — {len(ctx)} events:")
            for c_ts, src, atype, title, content in ctx:
                label = title or short(content, 60)
                print(f"      {pretty(c_ts)}  {src}/{atype}  {short(label, 70)}")
        print()

    con.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    main(limit)
