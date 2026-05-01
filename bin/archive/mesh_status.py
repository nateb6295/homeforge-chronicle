#!/usr/bin/env python3
"""Mesh Status — Build #139

One-shot mesh health dashboard. Combines coupling health, source quality,
agent status, and pipeline velocity into a single readable view.

Usage:
    python3 mesh_status.py           # Full dashboard
    python3 mesh_status.py discord   # Post to Discord
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

OPUS_WEBHOOK = (
    "REDACTED_WEBHOOK_USE_ENV"
    "2hZYzQQcyDEVD0A9UQqJsHlnV9D1m-6AfwNCnNWxGUC_8A0-ViX2dRVkBHF17_b2oDxJ"
)


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def agent_health():
    """Check which agents are running."""
    agents = [
        "chronicle-gemma", "chronicle-intern", "chronicle-crossref",
        "chronicle-provocateur", "chronicle-sentinel", "chronicle-feeds",
        "chronicle-engine", "chronicle-scribe", "chronicle-hal",
    ]
    results = {}
    for agent in agents:
        try:
            r = subprocess.run(
                ["systemctl", "--user", "is-active", agent],
                capture_output=True, text=True, timeout=5
            )
            results[agent.replace("chronicle-", "")] = r.stdout.strip() == "active"
        except Exception:
            results[agent.replace("chronicle-", "")] = False
    return results


def pipeline_velocity(hours=2):
    """Measure pipeline throughput."""
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    counts = db.execute(
        "SELECT activity_type, COUNT(*) FROM activity_feed "
        "WHERE source='intern' AND created_at > ? "
        "GROUP BY activity_type ORDER BY COUNT(*) DESC",
        (cutoff,)
    ).fetchall()

    total_briefs = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
        (cutoff,)
    ).fetchone()[0]

    total_captures = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE '%operator:capture%' AND created_at > ?",
        (cutoff,)
    ).fetchone()[0]

    crossref = db.execute(
        "SELECT COUNT(*) FROM crossref_connections WHERE created_at > ?",
        (cutoff,)
    ).fetchone()[0]

    db.close()

    return {
        "briefs": total_briefs,
        "captures": total_captures,
        "crossref": crossref,
        "per_hour": round(total_briefs / max(hours, 0.1), 1),
        "breakdown": dict(counts),
    }


def source_quality(hours=6):
    """Brief quality by source path."""
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    rows = db.execute(
        "SELECT json_extract(metadata, '$.source_path') as src, "
        "COUNT(*) as cnt, "
        "ROUND(AVG(json_extract(metadata, '$.argument_depth')), 2) as depth, "
        "ROUND(AVG(length(content)), 0) as avg_len "
        "FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' "
        "AND created_at > ? AND json_extract(metadata, '$.source_path') IS NOT NULL "
        "GROUP BY src ORDER BY cnt DESC LIMIT 8",
        (cutoff,)
    ).fetchall()

    db.close()
    return [{"source": r[0], "count": r[1], "depth": r[2], "length": r[3]} for r in rows]


def novelty_ratio(hours=2):
    """Quick novelty measurement."""
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    briefs = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
        (cutoff,)
    ).fetchall()
    db.close()

    if not briefs:
        return 0.0

    novel_markers = [
        r"transfer hypothesis",
        r"this (?:suggests|implies|means|reveals|changes|shows)",
        r"the (?:key|core|real|actual|bigger|deeper) (?:insight|shift|change|move|question)",
        r"not (?:just|merely|simply)",
        r"what (?:this|it) actually",
        r"connects to",
    ]

    relay_markers = [
        r"^a (?:new|recent) (?:study|paper|article|report) (?:shows|finds|reveals|demonstrates|suggests)",
        r"^researchers (?:have|at)",
        r"^according to",
    ]

    novel = 0
    for (content,) in briefs:
        cl = content.lower()
        nh = sum(1 for m in novel_markers if re.search(m, cl))
        rh = sum(1 for m in relay_markers if re.search(m, cl))
        if nh >= 2 and len(content) > 400:
            novel += 1
        elif rh > 0 and nh < 2:
            pass
        else:
            novel += 0.5

    return round(novel / len(briefs), 3)


def fab_rate():
    """Read current fabrication rate from health signal."""
    try:
        path = os.path.expanduser("~/chronicle/spot_check_health.json")
        with open(path) as f:
            data = json.load(f)
        return data.get("fab_rate", 0), data.get("constraint_level", 0)
    except Exception:
        return 0, 0


def full_dashboard():
    """Print the complete mesh status dashboard."""
    print("═══════════════════════════════════════════════")
    print("  MESH STATUS DASHBOARD")
    print("═══════════════════════════════════════════════\n")

    # Agents
    agents = agent_health()
    green = sum(1 for v in agents.values() if v)
    total = len(agents)
    status_str = " ".join(
        f"{'●' if v else '○'}{k[:3]}" for k, v in agents.items()
    )
    print(f"AGENTS: {green}/{total} green  {status_str}\n")

    # Pipeline
    vel = pipeline_velocity()
    print(f"PIPELINE (2h):")
    print(f"  Briefs: {vel['briefs']} ({vel['per_hour']}/hr)")
    print(f"  Captures: {vel['captures']}")
    print(f"  Crossref: {vel['crossref']} connections\n")

    # Quality
    fr, cl = fab_rate()
    nr = novelty_ratio()
    print(f"QUALITY:")
    print(f"  Novelty ratio:    {nr:.0%}")
    print(f"  Fabrication rate: {fr:.0%} (constraint level {cl})")

    # Thermostat state
    if cl >= 2:
        print(f"  Thermostat:       TIGHT (high fab)")
    elif cl >= 1:
        print(f"  Thermostat:       WARM (elevated fab)")
    elif nr < 0.4:
        print(f"  Thermostat:       ENCOURAGING (low novelty)")
    else:
        print(f"  Thermostat:       BALANCED")
    print()

    # Source quality
    sq = source_quality()
    if sq:
        print(f"SOURCE QUALITY (6h):")
        for s in sq[:6]:
            name = s["source"].replace("operator:capture:", "capture")
            name = name.replace("feed-explore:", "")
            name = name.replace("seeker:algo:", "algo:")
            print(f"  {name:<20} {s['count']:>3} briefs  depth={s['depth']:<5}  len={s['length']:.0f}")
        print()

    # Overall
    mesh_state = "ADAPTING" if nr > 0.6 and vel["crossref"] > 5 else \
                 "COMPROMISING" if nr < 0.3 and vel["crossref"] < 2 else \
                 "STABLE"
    print(f"═══ MESH STATE: {mesh_state} ═══")

    return {
        "agents_green": green,
        "briefs": vel["briefs"],
        "novelty": nr,
        "fab_rate": fr,
        "mesh_state": mesh_state,
    }


def post_to_discord(data):
    """Post compact status to Discord."""
    import requests
    msg = (
        f"**Mesh Status** | {data['agents_green']}/9 agents | "
        f"{data['briefs']} briefs/2h | "
        f"novelty {data['novelty']:.0%} | "
        f"fab {data['fab_rate']:.0%} | "
        f"**{data['mesh_state']}**"
    )
    try:
        requests.post(OPUS_WEBHOOK, json={"content": msg}, timeout=10)
    except Exception:
        pass


def main():
    data = full_dashboard()
    if len(sys.argv) > 1 and sys.argv[1] == "discord":
        post_to_discord(data)


if __name__ == "__main__":
    main()
