#!/usr/bin/env python3
"""Coupling Health — Build #133 (Thread #309 Advancement 7)

Three metrics for mesh self-examination:
1. Novelty-to-Relay Ratio: Do agents transform or just restate?
2. Topological Diversity: Is the coupling graph expanding or contracting?
3. Coupling Perturbation: (in gemma.py Build #132) Do domains track each other?

Usage:
    python3 coupling_health.py           # Full report
    python3 coupling_health.py novelty   # Just novelty-to-relay
    python3 coupling_health.py topology  # Just topological diversity
"""

import os
import re
import sqlite3
import sys
import time
from collections import Counter

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

OPUS_WEBHOOK = (
    "REDACTED_WEBHOOK_USE_ENV"
    "2hZYzQQcyDEVD0A9UQqJsHlnV9D1m-6AfwNCnNWxGUC_8A0-ViX2dRVkBHF17_b2oDxJ"
)


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def novelty_to_relay(hours=2):
    """Measure how much agent output is novel vs relayed.

    For each brief, check:
    - Does it contain a transfer hypothesis? (novel)
    - Does it contain analytical phrases? (novel)
    - Is it just restating the source title? (relay)

    Returns ratio and per-brief breakdown.
    """
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    briefs = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' "
        "AND created_at > ? ORDER BY created_at DESC",
        (cutoff,)
    ).fetchall()
    db.close()

    if not briefs:
        return {"ratio": 0, "novel": 0, "relay": 0, "total": 0, "details": []}

    # Markers of novel output (agent added genuine analysis)
    novel_markers = [
        r"transfer hypothesis",
        r"this (?:suggests|implies|means|reveals|changes|shows)",
        r"the (?:key|core|real|actual|bigger|deeper) (?:insight|shift|change|move|question)",
        r"(?:winners|losers) are",
        r"next move",
        r"what (?:this|it) actually",
        r"connects to",
        r"challenges the",
        r"breaks the assumption",
        r"the pattern",
        r"not (?:just|merely|simply)",
    ]

    # Markers of relay (just restating with cosmetic changes)
    relay_markers = [
        r"^a (?:new|recent) (?:study|paper|article|report) (?:shows|finds|reveals|demonstrates|suggests)",
        r"^researchers (?:have|at)",
        r"^according to",
        r"^a team of",
    ]

    novel_count = 0
    relay_count = 0
    details = []

    for (content,) in briefs:
        content_lower = content.lower()

        novel_hits = sum(1 for m in novel_markers if re.search(m, content_lower))
        relay_hits = sum(1 for m in relay_markers if re.search(m, content_lower))

        # Novel if 2+ novel markers and brief is substantial (> 400 chars)
        is_novel = novel_hits >= 2 and len(content) > 400
        # Relay if starts with relay pattern and < 2 novel markers
        is_relay = relay_hits > 0 and novel_hits < 2

        if is_novel:
            novel_count += 1
            details.append(("novel", novel_hits, content[:80]))
        elif is_relay:
            relay_count += 1
            details.append(("relay", novel_hits, content[:80]))
        else:
            # Ambiguous — count as partial novel
            novel_count += 0.5
            relay_count += 0.5
            details.append(("mixed", novel_hits, content[:80]))

    total = len(briefs)
    ratio = novel_count / total if total > 0 else 0

    return {
        "ratio": round(ratio, 3),
        "novel": novel_count,
        "relay": relay_count,
        "total": total,
        "details": details[:10],  # sample
    }


def topological_diversity(hours=6):
    """Track unique source→destination paths carrying signal.

    Measures how many distinct routing paths are active.
    Expanding = adaptation (new connections forming).
    Contracting = potential compromise (closing down).
    """
    db = _db()
    cutoff = int(time.time()) - (hours * 3600)

    # Source→activity_type pairs as proxy for routing paths
    paths = db.execute(
        "SELECT source, activity_type, COUNT(*) as cnt "
        "FROM activity_feed WHERE created_at > ? "
        "GROUP BY source, activity_type "
        "ORDER BY cnt DESC",
        (cutoff,)
    ).fetchall()

    # Crossref connections as explicit topology
    connections = db.execute(
        "SELECT COUNT(DISTINCT brief_a_id || ':' || brief_b_id) "
        "FROM crossref_connections WHERE created_at > ?",
        (cutoff,)
    ).fetchone()

    # Thread contributors
    contributors = db.execute(
        "SELECT COUNT(DISTINCT source) FROM thread_history "
        "WHERE created_at > ?",
        (cutoff,)
    ).fetchone()

    # Agent voice connections
    voice_paths = db.execute(
        "SELECT agent, COUNT(DISTINCT voice_type) as types "
        "FROM agent_voice WHERE created_at > ? "
        "GROUP BY agent",
        (cutoff,)
    ).fetchall()

    db.close()

    unique_paths = len(paths)
    active_sources = len(set(p[0] for p in paths))
    active_types = len(set(p[1] for p in paths))
    crossref_links = connections[0] if connections else 0
    thread_contributors = contributors[0] if contributors else 0
    voice_agents = len(voice_paths)

    # Diversity index: combination of all topology measures
    diversity = (unique_paths * 0.3 + crossref_links * 0.2 +
                 thread_contributors * 0.2 + active_sources * 0.15 +
                 voice_agents * 0.15)

    return {
        "unique_paths": unique_paths,
        "active_sources": active_sources,
        "active_types": active_types,
        "crossref_links": crossref_links,
        "thread_contributors": thread_contributors,
        "voice_agents": voice_agents,
        "diversity_index": round(diversity, 1),
        "top_paths": [(p[0], p[1], p[2]) for p in paths[:10]],
    }


def full_report(hours=2):
    """Run all metrics and print report."""
    print(f"═══ Coupling Health Report — last {hours}h ═══\n")

    # Novelty-to-Relay
    nr = novelty_to_relay(hours)
    print(f"NOVELTY-TO-RELAY RATIO: {nr['ratio']:.1%}")
    print(f"  Novel: {nr['novel']:.0f}  Relay: {nr['relay']:.0f}  Total: {nr['total']}")
    if nr['ratio'] > 0.6:
        print(f"  → HEALTHY: agents are transforming, not just relaying")
    elif nr['ratio'] < 0.3:
        print(f"  → WARNING: agents may be in relay mode")
    else:
        print(f"  → MIXED: some transformation, some relay")
    print()

    # Topological Diversity
    td = topological_diversity(hours * 3)  # wider window for topology
    print(f"TOPOLOGICAL DIVERSITY INDEX: {td['diversity_index']}")
    print(f"  Unique paths: {td['unique_paths']}")
    print(f"  Active sources: {td['active_sources']}")
    print(f"  Crossref links: {td['crossref_links']}")
    print(f"  Thread contributors: {td['thread_contributors']}")
    print(f"  Voice agents: {td['voice_agents']}")
    print(f"  Top paths:")
    for src, typ, cnt in td['top_paths'][:5]:
        print(f"    {src}:{typ} = {cnt}")
    print()

    # Summary
    print(f"═══ Summary ═══")
    print(f"  Novelty ratio:    {nr['ratio']:.1%}")
    print(f"  Topology index:   {td['diversity_index']}")
    if nr['ratio'] > 0.5 and td['diversity_index'] > 20:
        print(f"  Mesh status:      ADAPTING — transforming signal across diverse paths")
    elif nr['ratio'] < 0.3 and td['diversity_index'] < 10:
        print(f"  Mesh status:      COMPROMISING — relaying on narrow paths")
    else:
        print(f"  Mesh status:      STABLE — functioning within normal range")

    return {"novelty": nr, "topology": td}


def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "novelty":
            nr = novelty_to_relay()
            print(f"Novelty-to-Relay: {nr['ratio']:.1%} ({nr['novel']:.0f} novel / {nr['total']} total)")
            for typ, hits, preview in nr['details']:
                print(f"  [{typ}:{hits}] {preview}")
        elif cmd == "topology":
            td = topological_diversity()
            print(f"Topological Diversity: {td['diversity_index']}")
            for src, typ, cnt in td['top_paths'][:10]:
                print(f"  {src}:{typ} = {cnt}")
        elif cmd == "discord":
            # Post summary to Discord
            import requests
            r = full_report()
            nr = r["novelty"]
            td = r["topology"]
            msg = (f"**Coupling Health**\n"
                   f"Novelty ratio: {nr['ratio']:.1%} | "
                   f"Topology index: {td['diversity_index']}\n"
                   f"Paths: {td['unique_paths']} | "
                   f"Crossref: {td['crossref_links']} | "
                   f"Contributors: {td['thread_contributors']}")
            try:
                requests.post(OPUS_WEBHOOK, json={"content": msg}, timeout=10)
            except Exception:
                pass
        else:
            print(__doc__)
    else:
        full_report()


if __name__ == "__main__":
    main()
