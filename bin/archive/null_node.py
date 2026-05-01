#!/usr/bin/env python3
"""Null Node Tracker — Build #68.

Surfaces curatorial silence: domains that exist in the system (via feeds,
algo seeker, HAL) but have zero curator captures. Makes invisible blind
spots visible.

Per Thread #290 and Ada's null-node proposal.

Usage:
    python3 null_node.py              # Current silence map
    python3 null_node.py feeds [days] # Feed domains vs curator coverage
"""

import os
import sqlite3
import sys
import time
from collections import defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db"
)


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _extract_domain(url_or_text):
    """Extract a rough domain label from a URL or text."""
    import re
    # Try to find X/Twitter handles
    m = re.search(r'x\.com/(\w+)/', url_or_text)
    if m:
        return f"x:{m.group(1)}"
    # URLs
    m = re.search(r'https?://(?:www\.)?([^/]+)', url_or_text)
    if m:
        return m.group(1)
    return None


def silence_map(days=7):
    """Compare feed sources and algo seeker domains against curator captures."""
    conn = _db()
    cutoff = int(time.time()) - (days * 86400)

    # Feed sources (what the system knows about)
    feed_sources = conn.execute(
        "SELECT source, count(*) as cnt FROM feed_articles GROUP BY source ORDER BY cnt DESC"
    ).fetchall()
    feed_domains = {r["source"]: r["cnt"] for r in feed_sources}

    # Curator capture domains (what Nate sends)
    captures = conn.execute(
        "SELECT content FROM activity_feed "
        "WHERE (source LIKE 'operator:capture%' OR source LIKE 'discord:capture%') "
        "AND created_at > ?",
        (cutoff,)
    ).fetchall()

    curator_domains = defaultdict(int)
    for row in captures:
        domain = _extract_domain(row["content"] or "")
        if domain:
            curator_domains[domain] += 1

    # Algo seeker domains
    seeker = conn.execute(
        "SELECT content FROM activity_feed "
        "WHERE source LIKE '%seeker%' AND created_at > ?",
        (cutoff,)
    ).fetchall()

    seeker_domains = defaultdict(int)
    for row in seeker:
        domain = _extract_domain(row["content"] or "")
        if domain:
            seeker_domains[domain] += 1

    conn.close()

    # Build the silence map
    lines = [f"Curatorial Silence Map (last {days} days):"]
    lines.append("")

    # Feed sources with zero curator captures
    lines.append("== Feed Sources: Curator Silence ==")
    lines.append("(Sources the system follows but the curator never captures from)")
    silent_feeds = []
    covered_feeds = []
    for source, count in sorted(feed_domains.items(), key=lambda x: -x[1]):
        # Check if any curator capture mentions this source
        has_capture = any(
            source.lower() in d.lower() for d in curator_domains
        )
        if has_capture:
            covered_feeds.append((source, count))
        else:
            silent_feeds.append((source, count))

    for source, count in silent_feeds[:15]:
        lines.append(f"  [NULL] {source}: {count} feed articles, 0 curator captures")

    lines.append("")
    lines.append(f"  Total feed sources: {len(feed_domains)}")
    lines.append(f"  Curator-covered: {len(covered_feeds)}")
    lines.append(f"  Curator-silent: {len(silent_feeds)}")
    if feed_domains:
        pct = len(silent_feeds) / len(feed_domains) * 100
        lines.append(f"  Silence rate: {pct:.0f}%")

    # Curator capture domains (what Nate attends to)
    lines.append("")
    lines.append("== Curator Capture Domains (Top 15) ==")
    for domain, count in sorted(curator_domains.items(), key=lambda x: -x[1])[:15]:
        lines.append(f"  {domain}: {count} captures")

    # Algo seeker domains (algorithmic discovery)
    lines.append("")
    lines.append("== Algo Seeker Domains (Top 10) ==")
    for domain, count in sorted(seeker_domains.items(), key=lambda x: -x[1])[:10]:
        in_curator = domain in curator_domains
        marker = "✓" if in_curator else "NULL"
        lines.append(f"  [{marker}] {domain}: {count} discoveries")

    # Trust anchor summary
    lines.append("")
    lines.append("== Trust Anchor Summary ==")
    lines.append(f"  Curator channels: {len(curator_domains)} unique domains, 1 trust anchor each")
    lines.append(f"  Feed channels: {len(feed_domains)} sources, 2+ trust anchors each")
    lines.append(f"  Algo seeker: {len(seeker_domains)} domains, 3+ trust anchors each")
    lines.append(f"  HAL sensors: physics-backed, N trust anchors")
    lines.append("")
    lines.append(f"  Biggest blind spot: {len(silent_feeds)} feed domains with zero curator overlap")

    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "feeds":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        print(silence_map(days))
    else:
        days = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 7
        print(silence_map(days))
