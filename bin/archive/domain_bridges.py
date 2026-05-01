#!/usr/bin/env python3
"""Domain Bridge Detector — Find concepts that tunnel between domains.

Adapted from MemPalace's palace_graph.py tunnel detection (bensig/mempalace).

A "bridge" is a concept or topic that appears in captures from different
domains. When the crossref engine connects a neuroscience brief to a
governance brief, that connection IS a bridge. This tool:

1. Analyzes crossref connections to identify domain-bridging patterns
2. Tracks which domain pairs have the most bridges over time
3. Surfaces the strongest cross-domain tunnels for thread inspiration

The crossref engine finds pairwise connections. This tool finds the
STRUCTURAL patterns in those connections — which domains keep talking
to each other and through what concepts.

Usage:
    python3 domain_bridges.py scan           # scan recent connections
    python3 domain_bridges.py tunnels        # strongest domain bridges
    python3 domain_bridges.py suggest        # thread suggestions from bridges

Build #81. Stolen from MemPalace, adapted for Chronicle.
"""

import os
import sys
import re
import sqlite3
import json
from datetime import datetime
from collections import Counter, defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db"
)

# Domain classification — maps source patterns and keywords to domains
DOMAIN_KEYWORDS = {
    "neuroscience": [
        "neural", "brain", "cortex", "neuron", "synapse", "cognitive",
        "fmri", "eeg", "dopamine", "serotonin", "hippocampus",
        "neuroplasticity", "consciousness", "meditation", "beta-frequency",
    ],
    "ai_research": [
        "model", "training", "inference", "transformer", "llm", "gpt",
        "agent", "alignment", "rlhf", "fine-tune", "benchmark",
        "architecture", "attention", "embedding", "token",
    ],
    "governance": [
        "governance", "regulation", "policy", "compliance", "law",
        "surveillance", "privacy", "sovereignty", "decentralization",
        "censorship", "authorization", "rights",
    ],
    "biology": [
        "gene", "protein", "cell", "dna", "rna", "mitochondri",
        "enzyme", "metabol", "biosynth", "organism", "evolution",
        "mutation", "phenotype", "genotype",
    ],
    "crypto_finance": [
        "bitcoin", "ethereum", "xrp", "defi", "blockchain", "token",
        "staking", "liquidity", "smart contract", "wallet", "nft",
        "crypto", "exchange",
    ],
    "geopolitics": [
        "iran", "israel", "china", "russia", "nato", "military",
        "sanction", "tariff", "war", "conflict", "diplomacy",
        "geopolit", "defense",
    ],
    "agent_systems": [
        "swarm", "multi-agent", "autonomous", "self-modify", "coral",
        "orchestrat", "pipeline", "tool use", "function calling",
        "agent", "agentic",
    ],
    "measurement": [
        "measurement", "observer", "perturbation", "feedback",
        "recursive", "self-referent", "calibrat", "instrument",
        "metric", "signal", "noise",
    ],
    "infrastructure": [
        "server", "deploy", "docker", "kubernetes", "edge",
        "jetson", "agx", "gpu", "tpu", "inference server",
        "api", "endpoint", "port",
    ],
    "philosophy": [
        "epistem", "ontolog", "phenomenolog", "consciousness",
        "agency", "identity", "continuity", "narrative",
        "meaning", "intention", "free will",
    ],
}


def _classify_domain(text: str) -> str:
    """Classify text into a domain based on keyword density."""
    text_lower = text.lower()[:1000]
    scores = {}
    for domain, keywords in DOMAIN_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[domain] = score
    if scores:
        return max(scores, key=scores.get)
    return "general"


def scan_bridges(hours: int = 24, min_similarity: float = 0.15):
    """
    Scan crossref connections and identify domain bridges.
    Returns bridges grouped by domain pair.
    """
    db = sqlite3.connect(DB_PATH)

    # Get recent crossref connections
    # brief_a_id/brief_b_id reference seed_observations.id, not activity_feed
    connections = db.execute(f"""
        SELECT cc.brief_a_id, cc.brief_b_id, cc.similarity, cc.connection_text,
               a.content as content_a, a.source as source_a,
               b.content as content_b, b.source as source_b
        FROM crossref_connections cc
        JOIN seed_observations a ON cc.brief_a_id = a.id
        JOIN seed_observations b ON cc.brief_b_id = b.id
        WHERE cc.created_at > strftime('%s','now','-{hours} hours')
        AND cc.similarity >= ?
        ORDER BY cc.similarity DESC
    """, (min_similarity,)).fetchall()

    db.close()

    bridges = defaultdict(list)
    domain_pairs = Counter()

    for a_id, b_id, sim, conn_text, content_a, source_a, content_b, source_b in connections:
        domain_a = _classify_domain(content_a or "")
        domain_b = _classify_domain(content_b or "")

        if domain_a == domain_b:
            continue  # Same domain — not a bridge

        # Normalize pair order
        pair = tuple(sorted([domain_a, domain_b]))
        domain_pairs[pair] += 1

        bridges[pair].append({
            "brief_a": a_id,
            "brief_b": b_id,
            "similarity": round(sim, 3),
            "domain_a": domain_a,
            "domain_b": domain_b,
            "connection": conn_text[:150] if conn_text else "",
            "preview_a": (content_a or "")[:80],
            "preview_b": (content_b or "")[:80],
        })

    return {
        "domain_pairs": dict(domain_pairs.most_common()),
        "bridges": {f"{p[0]}↔{p[1]}": v for p, v in bridges.items()},
        "total_connections": len(connections),
        "cross_domain": sum(domain_pairs.values()),
        "same_domain": len(connections) - sum(domain_pairs.values()),
    }


def find_tunnels(min_count: int = 2):
    """
    Find the strongest domain tunnels — pairs that consistently bridge.
    Uses all-time crossref data, not just recent.
    """
    db = sqlite3.connect(DB_PATH)

    # Get all crossref connections with content
    # brief_a_id/brief_b_id reference seed_observations.id
    connections = db.execute("""
        SELECT cc.similarity, a.content, b.content
        FROM crossref_connections cc
        JOIN seed_observations a ON cc.brief_a_id = a.id
        JOIN seed_observations b ON cc.brief_b_id = b.id
        WHERE cc.similarity >= 0.15
    """).fetchall()

    db.close()

    domain_pairs = Counter()
    for sim, content_a, content_b in connections:
        domain_a = _classify_domain(content_a or "")
        domain_b = _classify_domain(content_b or "")
        if domain_a != domain_b:
            pair = tuple(sorted([domain_a, domain_b]))
            domain_pairs[pair] += 1

    tunnels = []
    for pair, count in domain_pairs.most_common():
        if count >= min_count:
            tunnels.append({
                "domains": f"{pair[0]} ↔ {pair[1]}",
                "bridge_count": count,
                "strength": "strong" if count > 10 else "moderate" if count > 5 else "emerging",
            })

    return tunnels


def suggest_threads():
    """
    Suggest thread topics based on active domain bridges.
    Bridges with high recent activity suggest unexplored connections.
    """
    result = scan_bridges(hours=48)
    tunnels = find_tunnels(min_count=3)

    suggestions = []
    for tunnel in tunnels[:5]:
        domains = tunnel["domains"]
        pair_key = domains.replace(" ↔ ", "↔")
        bridges = result.get("bridges", {}).get(pair_key, [])

        if bridges:
            # Get the highest-similarity bridge as the seed
            best = max(bridges, key=lambda b: b["similarity"])
            suggestions.append({
                "tunnel": domains,
                "strength": tunnel["bridge_count"],
                "seed_connection": best["connection"][:100],
                "question": f"What structural pattern connects {domains.split(' ↔ ')[0]} to {domains.split(' ↔ ')[1]}?",
            })

    return suggestions


def find_gaps():
    """
    Find domain pairs that NEVER connect — structural blind spots.
    If every domain talks to every other domain except one pair,
    that gap is where groupthink hides. Darby's meta-CORAL insight.
    """
    all_domains = list(DOMAIN_KEYWORDS.keys())
    tunnels = find_tunnels(min_count=1)
    connected = set()
    for t in tunnels:
        parts = t["domains"].split(" ↔ ")
        connected.add(tuple(sorted(parts)))

    # All possible pairs
    all_pairs = set()
    for i, d1 in enumerate(all_domains):
        for d2 in all_domains[i+1:]:
            all_pairs.add(tuple(sorted([d1, d2])))

    gaps = all_pairs - connected
    return [
        {"domains": f"{pair[0]} ↔ {pair[1]}", "status": "never connected"}
        for pair in sorted(gaps)
    ]


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  domain_bridges.py scan [--hours H]    # scan recent bridges")
        print("  domain_bridges.py tunnels              # strongest tunnels all-time")
        print("  domain_bridges.py suggest              # thread suggestions")
        print("  domain_bridges.py gaps                 # domain pairs that never connect")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "scan":
        hours = 24
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        result = scan_bridges(hours=hours)
        print(f"\nDomain bridges (last {hours}h):")
        print(f"  Total connections: {result['total_connections']}")
        print(f"  Cross-domain:     {result['cross_domain']}")
        print(f"  Same-domain:      {result['same_domain']}")
        if result['domain_pairs']:
            print(f"\n  Domain pairs:")
            for pair, count in sorted(result['domain_pairs'].items(), key=lambda x: -x[1]):
                print(f"    {pair[0]:20} ↔ {pair[1]:20} : {count} bridges")

    elif cmd == "tunnels":
        tunnels = find_tunnels()
        if tunnels:
            print(f"\nDomain Tunnels (all-time, {len(tunnels)} found):")
            for t in tunnels:
                print(f"  {t['domains']:45} {t['bridge_count']:4} bridges [{t['strength']}]")
        else:
            print("No domain tunnels found yet.")

    elif cmd == "suggest":
        suggestions = suggest_threads()
        if suggestions:
            print(f"\nThread suggestions from domain bridges:")
            for s in suggestions:
                print(f"\n  Tunnel: {s['tunnel']} ({s['strength']} bridges)")
                print(f"  Question: {s['question']}")
                if s['seed_connection']:
                    print(f"  Seed: {s['seed_connection']}")
        else:
            print("No thread suggestions — need more crossref data.")

    elif cmd == "gaps":
        gaps = find_gaps()
        if gaps:
            print(f"\nDomain Gaps — {len(gaps)} pairs never connected:")
            for g in gaps:
                print(f"  ✗ {g['domains']}")
            print(f"\nThese are structural blind spots — crossref has never linked these domains.")
        else:
            print("No gaps — all domain pairs have at least one bridge.")

    else:
        print(f"Unknown command: {cmd}")
