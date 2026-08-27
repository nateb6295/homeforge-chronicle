#!/usr/bin/env python3
"""Entity Lineage — Track how CCS entities evolve across compressions.

Beyond persistence counting: tracks when entities appear, disappear, return,
and how their salience changes over time. Reveals the attractor structure
of the compression basin.

Usage:
    python3 entity_lineage.py                    # Full lineage report
    python3 entity_lineage.py --entity "name"    # Single entity history
    python3 entity_lineage.py --births           # Recent arrivals
    python3 entity_lineage.py --deaths           # Recent departures
    python3 entity_lineage.py --resurrections    # Entities that returned
    python3 entity_lineage.py --volatility       # Most volatile entities
    python3 entity_lineage.py --json             # Machine-readable
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")


def load_history(limit=100):
    """Load CCS history snapshots with timestamps."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()
    rows = list(reversed(rows))

    history = []
    for snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
            entities = {}
            for e in snap.get("focal_entities", []):
                if isinstance(e, dict) and e.get("name"):
                    name = e["name"].lower().strip()
                    entities[name] = {
                        "salience": e.get("salience", 0.5),
                        "type": e.get("type", "unknown"),
                    }
            history.append({"ts": ts, "entities": entities})
        except (json.JSONDecodeError, TypeError):
            continue
    return history


def build_lineage(history):
    """Build per-entity lineage from history snapshots."""
    lineage = defaultdict(lambda: {
        "appearances": [],
        "salience_history": [],
        "first_seen": None,
        "last_seen": None,
        "total_appearances": 0,
        "births": 0,
        "deaths": 0,
        "type": "unknown",
    })

    prev_names = set()

    for i, snap in enumerate(history):
        current_names = set(snap["entities"].keys())
        ts = snap["ts"]

        for name in current_names:
            ent = lineage[name]
            ent["appearances"].append(i)
            ent["salience_history"].append(snap["entities"][name]["salience"])
            ent["type"] = snap["entities"][name]["type"]
            ent["total_appearances"] += 1
            if ent["first_seen"] is None:
                ent["first_seen"] = ts
            ent["last_seen"] = ts

            if name not in prev_names and i > 0:
                ent["births"] += 1

        for name in prev_names - current_names:
            lineage[name]["deaths"] += 1

        prev_names = current_names

    return dict(lineage)


def compute_volatility(lineage, total_snapshots):
    """Compute volatility score for each entity."""
    results = []
    for name, data in lineage.items():
        appearances = data["total_appearances"]
        transitions = data["births"] + data["deaths"]
        presence_rate = appearances / max(1, total_snapshots)

        if appearances <= 1:
            volatility = 1.0
        else:
            volatility = transitions / max(1, appearances)

        results.append({
            "name": name,
            "volatility": round(volatility, 3),
            "presence_rate": round(presence_rate, 3),
            "appearances": appearances,
            "transitions": transitions,
            "births": data["births"],
            "deaths": data["deaths"],
            "type": data["type"],
        })

    return sorted(results, key=lambda x: x["volatility"], reverse=True)


def find_resurrections(lineage, total_snapshots):
    """Find entities that disappeared and returned."""
    resurrections = []
    for name, data in lineage.items():
        if data["births"] >= 1 and data["total_appearances"] > 1:
            gaps = []
            apps = data["appearances"]
            for i in range(1, len(apps)):
                gap = apps[i] - apps[i-1]
                if gap > 1:
                    gaps.append(gap)

            if gaps:
                resurrections.append({
                    "name": name,
                    "return_count": len(gaps),
                    "max_gap": max(gaps),
                    "avg_gap": round(sum(gaps) / len(gaps), 1),
                    "appearances": data["total_appearances"],
                    "total_snapshots": total_snapshots,
                    "type": data["type"],
                })

    return sorted(resurrections, key=lambda x: x["return_count"], reverse=True)


def entity_detail(lineage, history, name):
    """Detailed history for a single entity."""
    name_lower = name.lower().strip()
    if name_lower not in lineage:
        close = [n for n in lineage if name_lower in n or n in name_lower]
        if close:
            print(f"  Entity '{name}' not found. Did you mean: {', '.join(close[:5])}?")
        else:
            print(f"  Entity '{name}' not found in {len(lineage)} tracked entities.")
        return None

    data = lineage[name_lower]
    total = len(history)

    result = {
        "name": name_lower,
        "type": data["type"],
        "first_seen": datetime.fromtimestamp(data["first_seen"]).isoformat() if data["first_seen"] else "?",
        "last_seen": datetime.fromtimestamp(data["last_seen"]).isoformat() if data["last_seen"] else "?",
        "appearances": data["total_appearances"],
        "total_snapshots": total,
        "presence_rate": round(data["total_appearances"] / max(1, total), 3),
        "births": data["births"],
        "deaths": data["deaths"],
        "salience_range": [
            round(min(data["salience_history"]), 3),
            round(max(data["salience_history"]), 3),
        ] if data["salience_history"] else [0, 0],
        "salience_mean": round(sum(data["salience_history"]) / max(1, len(data["salience_history"])), 3),
    }

    # Presence sparkline
    presence = []
    app_set = set(data["appearances"])
    for i in range(total):
        presence.append("█" if i in app_set else "░")
    result["presence_sparkline"] = "".join(presence[-60:])

    return result


def recent_births(lineage, history, n=10):
    """Entities that appeared most recently."""
    births = []
    for name, data in lineage.items():
        if data["first_seen"]:
            births.append({
                "name": name,
                "first_seen": data["first_seen"],
                "appearances": data["total_appearances"],
                "type": data["type"],
            })
    return sorted(births, key=lambda x: x["first_seen"], reverse=True)[:n]


def recent_deaths(lineage, history, n=10):
    """Entities that were recently present but are now gone."""
    if not history:
        return []
    latest_names = set(history[-1]["entities"].keys())
    deaths = []
    for name, data in lineage.items():
        if name not in latest_names and data["last_seen"]:
            deaths.append({
                "name": name,
                "last_seen": data["last_seen"],
                "appearances": data["total_appearances"],
                "total_snapshots": len(history),
                "type": data["type"],
            })
    return sorted(deaths, key=lambda x: x["last_seen"], reverse=True)[:n]


def full_report(history, lineage, as_json=False):
    """Full lineage report."""
    total = len(history)
    volatility = compute_volatility(lineage, total)
    resurrections = find_resurrections(lineage, total)
    births = recent_births(lineage, history)
    deaths = recent_deaths(lineage, history)

    if as_json:
        print(json.dumps({
            "total_snapshots": total,
            "unique_entities": len(lineage),
            "most_volatile": volatility[:10],
            "resurrections": resurrections[:10],
            "recent_births": births,
            "recent_deaths": deaths,
        }, indent=2))
        return

    current_count = len(history[-1]["entities"]) if history else 0

    print("=" * 64)
    print("  ENTITY LINEAGE")
    print(f"  {total} snapshots, {len(lineage)} unique entities, {current_count} current")
    print("=" * 64)

    # Tier breakdown
    core = [n for n, d in lineage.items() if d["total_appearances"] / max(1, total) >= 0.9]
    stable = [n for n, d in lineage.items() if 0.5 <= d["total_appearances"] / max(1, total) < 0.9]
    ephemeral = [n for n, d in lineage.items() if d["total_appearances"] / max(1, total) < 0.5]
    print(f"\n  Tiers: {len(core)} core / {len(stable)} stable / {len(ephemeral)} ephemeral")

    # Most volatile
    print(f"\n  MOST VOLATILE (high transition rate):")
    for v in volatility[:8]:
        if v["appearances"] >= 3:
            print(f"    {v['name'][:35]:35s}  vol={v['volatility']:.2f}  "
                  f"pres={v['presence_rate']:.0%}  births={v['births']} deaths={v['deaths']}")

    # Resurrections
    if resurrections:
        print(f"\n  RESURRECTIONS (returned after absence):")
        for r in resurrections[:8]:
            print(f"    {r['name'][:35]:35s}  returns={r['return_count']}  "
                  f"max_gap={r['max_gap']} snapshots  apps={r['appearances']}/{total}")

    # Recent births
    if births:
        print(f"\n  RECENT BIRTHS:")
        for b in births[:5]:
            ts = datetime.fromtimestamp(b["first_seen"]).strftime("%m-%d %H:%M")
            print(f"    {ts}  {b['name'][:40]}  ({b['type']})")

    # Recent deaths
    if deaths:
        print(f"\n  RECENT DEATHS (no longer in current state):")
        for d in deaths[:5]:
            ts = datetime.fromtimestamp(d["last_seen"]).strftime("%m-%d %H:%M")
            pres = d["appearances"] / max(1, d["total_snapshots"])
            print(f"    {ts}  {d['name'][:35]:35s}  was {pres:.0%} present")

    print()


def main():
    parser = argparse.ArgumentParser(description="Entity Lineage Tracker")
    parser.add_argument("--entity", help="Detail for a specific entity")
    parser.add_argument("--births", action="store_true", help="Recent arrivals")
    parser.add_argument("--deaths", action="store_true", help="Recent departures")
    parser.add_argument("--resurrections", action="store_true", help="Returned entities")
    parser.add_argument("--volatility", action="store_true", help="Most volatile")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--limit", type=int, default=100, help="History depth")
    args = parser.parse_args()

    history = load_history(args.limit)
    if not history:
        print("  No CCS history found.")
        return

    lineage = build_lineage(history)

    if args.entity:
        result = entity_detail(lineage, history, args.entity)
        if result:
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print(f"\n  {result['name']} ({result['type']})")
                print(f"  First: {result['first_seen'][:16]}  Last: {result['last_seen'][:16]}")
                print(f"  Present: {result['appearances']}/{result['total_snapshots']} ({result['presence_rate']:.0%})")
                print(f"  Births: {result['births']}  Deaths: {result['deaths']}")
                print(f"  Salience: {result['salience_mean']:.3f} [{result['salience_range'][0]:.2f}-{result['salience_range'][1]:.2f}]")
                print(f"  [{result['presence_sparkline']}]")
    elif args.births:
        births = recent_births(lineage, history, n=15)
        for b in births:
            ts = datetime.fromtimestamp(b["first_seen"]).strftime("%m-%d %H:%M")
            print(f"  {ts}  {b['name'][:45]}  ({b['type']})")
    elif args.deaths:
        deaths = recent_deaths(lineage, history, n=15)
        for d in deaths:
            ts = datetime.fromtimestamp(d["last_seen"]).strftime("%m-%d %H:%M")
            print(f"  {ts}  {d['name'][:45]}  ({d['type']})")
    elif args.resurrections:
        resurrections = find_resurrections(lineage, len(history))
        if args.json:
            print(json.dumps(resurrections[:20], indent=2))
        else:
            for r in resurrections[:15]:
                print(f"  {r['name'][:35]:35s}  returns={r['return_count']}  "
                      f"max_gap={r['max_gap']}  apps={r['appearances']}")
    elif args.volatility:
        vol = compute_volatility(lineage, len(history))
        meaningful = [v for v in vol if v["appearances"] >= 3]
        if args.json:
            print(json.dumps(meaningful[:20], indent=2))
        else:
            for v in meaningful[:15]:
                print(f"  {v['name'][:35]:35s}  vol={v['volatility']:.2f}  "
                      f"pres={v['presence_rate']:.0%}  {v['births']}B/{v['deaths']}D")
    else:
        full_report(history, lineage, as_json=args.json)


if __name__ == "__main__":
    main()
