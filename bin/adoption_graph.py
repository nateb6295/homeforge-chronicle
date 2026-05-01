#!/usr/bin/env python3
"""Adoption Graph — walk the adoption_edges lattice.

Usage:
  adoption_graph.py hubs [--limit N] [--lane XRP-ADOPT|ICP-ADOPT]
  adoption_graph.py neighbors <entity>
  adoption_graph.py geo <country>
  adoption_graph.py edges [--since 2h|24h|7d]
  adoption_graph.py stats
"""
import argparse
import sqlite3
import sys
import time
from collections import Counter


DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _parse_since(s):
    if not s:
        return 0
    units = {"h": 3600, "d": 86400, "w": 604800}
    try:
        num = int(s[:-1])
        unit = s[-1]
        return int(time.time()) - num * units.get(unit, 3600)
    except Exception:
        return 0


def cmd_hubs(args):
    """Most-connected entities — count distinct (predicate, counterpart) pairs."""
    conn = _conn()
    where = ""
    params = []
    if args.lane:
        where = "WHERE lane = ?"
        params.append(args.lane)
    rows = conn.execute(f"""
        SELECT ent, COUNT(DISTINCT sig) AS degree FROM (
            SELECT subject AS ent, predicate || '|' || object AS sig FROM adoption_edges {where}
            UNION ALL
            SELECT object AS ent, predicate || '|' || subject AS sig FROM adoption_edges {where}
        ) GROUP BY ent ORDER BY degree DESC LIMIT ?
    """, params + params + [args.limit]).fetchall()
    print(f"HUBS{' ('+args.lane+')' if args.lane else ''} — top {len(rows)}:")
    for r in rows:
        print(f"  {r['ent']}: degree {r['degree']}")


def cmd_neighbors(args):
    conn = _conn()
    rows = conn.execute("""
        SELECT predicate, object AS counterpart, 'out' AS direction, COUNT(*) AS n
          FROM adoption_edges WHERE subject = ?
         GROUP BY predicate, object
        UNION ALL
        SELECT predicate, subject AS counterpart, 'in' AS direction, COUNT(*) AS n
          FROM adoption_edges WHERE object = ?
         GROUP BY predicate, subject
        ORDER BY n DESC
    """, (args.entity, args.entity)).fetchall()
    if not rows:
        print(f"No edges for '{args.entity}'. Try `hubs` to see known entities.")
        return
    print(f"NEIGHBORS of '{args.entity}':")
    for r in rows:
        arrow = "→" if r["direction"] == "out" else "←"
        src = f"(×{r['n']})" if r["n"] > 1 else ""
        print(f"  {arrow} [{r['predicate']}] {r['counterpart']} {src}")


def cmd_geo(args):
    conn = _conn()
    rows = conn.execute("""
        SELECT DISTINCT subject FROM adoption_edges
         WHERE predicate = 'operates_in' AND object = ?
    """, (args.country,)).fetchall()
    if not rows:
        print(f"No entities tagged as operating in '{args.country}'.")
        return
    print(f"ENTITIES IN '{args.country}':")
    for r in rows:
        print(f"  - {r['subject']}")
    # Also show recent source titles that mentioned this geo
    stories = conn.execute("""
        SELECT DISTINCT source_title FROM adoption_edges
         WHERE geo = ? ORDER BY created_at DESC LIMIT 5
    """, (args.country,)).fetchall()
    if stories:
        print(f"\nRECENT STORIES:")
        for s in stories:
            print(f"  - {s['source_title']}")


def cmd_edges(args):
    conn = _conn()
    cutoff = _parse_since(args.since)
    rows = conn.execute("""
        SELECT subject, predicate, object, COUNT(*) AS n, MAX(created_at) AS ts
          FROM adoption_edges
         WHERE created_at >= ?
         GROUP BY subject, predicate, object
         ORDER BY n DESC, ts DESC
    """, (cutoff,)).fetchall()
    print(f"EDGES (since {args.since or 'forever'}) — {len(rows)} unique:")
    for r in rows:
        mult = f"  [×{r['n']}]" if r["n"] > 1 else ""
        print(f"  ({r['subject']}) -[{r['predicate']}]-> ({r['object']}){mult}")


def cmd_timeline(args):
    """Edge accumulation over time — weekly bins, ASCII sparkline per lane."""
    conn = _conn()
    now = int(time.time())
    weeks = args.weeks
    bins = [0] * weeks
    by_lane = {"XRP-ADOPT": [0] * weeks, "ICP-ADOPT": [0] * weeks}
    rows = conn.execute(
        "SELECT lane, created_at FROM adoption_edges WHERE created_at > ?",
        (now - weeks * 604800,),
    ).fetchall()
    for r in rows:
        age_weeks = (now - r["created_at"]) // 604800
        idx = weeks - 1 - age_weeks
        if 0 <= idx < weeks:
            bins[idx] += 1
            if r["lane"] in by_lane:
                by_lane[r["lane"]][idx] += 1

    def spark(series):
        if not any(series):
            return " " * len(series)
        chars = " ▁▂▃▄▅▆▇█"
        mx = max(series)
        return "".join(chars[min(8, int(v * 8 / mx) if mx else 0)] for v in series)

    print(f"ADOPTION TIMELINE — {weeks} weeks, newest on right")
    print(f"  ALL        |{spark(bins)}| total={sum(bins)}")
    for lane, series in by_lane.items():
        print(f"  {lane:10} |{spark(series)}| total={sum(series)}")
    # Summary: first/last non-zero week
    first_nz = next((i for i, v in enumerate(bins) if v), None)
    if first_nz is not None:
        weeks_ago = weeks - 1 - first_nz
        print(f"  earliest signal: {weeks_ago}w ago")
    # Month bucketing for a quick human-readable view
    months_back = min(12, weeks // 4)
    print(f"\n  Monthly buckets (last {months_back}):")
    for m in range(months_back, 0, -1):
        start_i = max(0, weeks - m * 4)
        end_i = weeks - (m - 1) * 4
        total_m = sum(bins[start_i:end_i])
        xrp_m = sum(by_lane["XRP-ADOPT"][start_i:end_i])
        icp_m = sum(by_lane["ICP-ADOPT"][start_i:end_i])
        print(f"    ~{m}mo ago: total={total_m:4d}  XRP={xrp_m:4d}  ICP={icp_m:3d}")


def cmd_stats(args):
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) AS n FROM adoption_edges").fetchone()["n"]
    unique = conn.execute("""
        SELECT COUNT(*) AS n FROM (SELECT DISTINCT subject, predicate, object FROM adoption_edges)
    """).fetchone()["n"]
    by_lane = conn.execute("SELECT lane, COUNT(*) AS n FROM adoption_edges GROUP BY lane").fetchall()
    top_preds = conn.execute("""
        SELECT predicate, COUNT(*) AS n FROM adoption_edges GROUP BY predicate ORDER BY n DESC LIMIT 6
    """).fetchall()
    print(f"ADOPTION GRAPH STATS")
    print(f"  total edges: {total}")
    print(f"  unique (s,p,o): {unique}")
    print(f"  by lane: " + ", ".join(f"{r['lane']}={r['n']}" for r in by_lane))
    print(f"  top predicates: " + ", ".join(f"{r['predicate']}({r['n']})" for r in top_preds))


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("hubs")
    sp.add_argument("--limit", type=int, default=10)
    sp.add_argument("--lane", choices=["XRP-ADOPT", "ICP-ADOPT"])
    sp.set_defaults(func=cmd_hubs)

    sp = sub.add_parser("neighbors")
    sp.add_argument("entity")
    sp.set_defaults(func=cmd_neighbors)

    sp = sub.add_parser("geo")
    sp.add_argument("country")
    sp.set_defaults(func=cmd_geo)

    sp = sub.add_parser("edges")
    sp.add_argument("--since", default="")
    sp.set_defaults(func=cmd_edges)

    sp = sub.add_parser("stats")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("timeline")
    sp.add_argument("--weeks", type=int, default=52)
    sp.set_defaults(func=cmd_timeline)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
