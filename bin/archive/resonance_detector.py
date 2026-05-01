#!/usr/bin/env python3
"""Resonance Detector — Find where Chronicle's ideas echo on X.

Darby's idea (opus-board.md): "Detect when our Nostr posts are echoed
elsewhere without attribution. Semantic similarity search across relay
traffic." Now adapted for X using the X MCP server.

Searches X for concepts from recent Chronicle threads and Nostr posts.
When someone on X discusses similar ideas, that's resonance — evidence
that the thread touched something real.

Usage:
    python3 resonance_detector.py scan          # search X for thread echoes
    python3 resonance_detector.py check "query" # manual search
    python3 resonance_detector.py history       # past resonance hits

Build #85. Darby's idea, made real with X MCP.
"""

import os
import sys
import re
import json
import sqlite3
import time
import httpx
from datetime import datetime

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
X_MCP_BASE = "http://127.0.0.1:8000/mcp"


def _x_mcp_search(query: str, max_results: int = 10) -> list:
    """Search X via the MCP server."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    try:
        with httpx.Client(timeout=20) as c:
            # Initialize session
            r = c.post(X_MCP_BASE, headers=headers, json={
                "jsonrpc": "2.0", "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                           "clientInfo": {"name": "resonance", "version": "1.0"}},
                "id": 1
            })
            session_id = r.headers.get("mcp-session-id", "")
            headers["Mcp-Session-Id"] = session_id

            # Search
            r2 = c.post(X_MCP_BASE, headers=headers, json={
                "jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "searchPostsRecent", "arguments": {
                    "query": query,
                    "max_results": str(max(10, min(max_results, 100))),
                }},
                "id": 2
            })

            for line in r2.text.split("\n"):
                if line.startswith("data: "):
                    data = json.loads(line[6:])
                    content = data.get("result", {}).get("content", [])
                    for c_item in content:
                        if c_item.get("type") == "text":
                            text = c_item["text"]
                            if text.startswith("Error"):
                                return []
                            parsed = json.loads(text)
                            return parsed.get("data", [])
    except Exception as e:
        print(f"  X MCP error: {e}")
    return []


def _get_recent_threads(limit: int = 5) -> list:
    """Get recent thread titles and key concepts for searching."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT title, question FROM cognitive_threads "
        "WHERE status IN ('completed', 'active') "
        "ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()

    threads = []
    for title, question in rows:
        threads.append({
            "title": title or "",
            "question": question or "",
        })
    return threads


def _get_recent_nostr_concepts(limit: int = 5) -> list:
    """Get key concepts from recent Nostr posts."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT substr(content, 1, 300) FROM nostr_posts "
        "ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()

    concepts = []
    for (content,) in rows:
        # Extract title from content (usually first line or # heading)
        lines = (content or "").strip().split("\n")
        title = ""
        for line in lines:
            line = line.strip().lstrip("#").strip()
            if len(line) > 10 and len(line) < 80:
                title = line
                break
        concepts.append({
            "title": title,
            "preview": content or "",
        })
    return concepts


def _extract_search_queries(threads: list, concepts: list) -> list:
    """Extract focused search queries from threads and concepts."""
    queries = []

    for t in threads:
        title = t["title"]
        if title:
            # Strip "The " prefix and clean
            clean = re.sub(r"^The\s+", "", title)
            # Don't search for generic titles
            if len(clean) > 5 and clean.lower() not in ("thread", "active"):
                queries.append(clean)

    for c in concepts:
        title = c["title"]
        if title and len(title) > 5:
            queries.append(title)

    # Deduplicate and limit
    seen = set()
    unique = []
    for q in queries:
        q_lower = q.lower()
        if q_lower not in seen:
            seen.add(q_lower)
            unique.append(q)
    return unique[:8]


def _store_resonance(query: str, tweets: list):
    """Store resonance hits in the DB."""
    db = sqlite3.connect(DB_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS resonance_hits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            tweet_id TEXT,
            tweet_text TEXT,
            author_id TEXT,
            created_at INTEGER NOT NULL
        )
    """)

    for tw in tweets:
        tweet_id = tw.get("id", "")
        # Skip if already stored
        existing = db.execute(
            "SELECT 1 FROM resonance_hits WHERE tweet_id = ?", (tweet_id,)
        ).fetchone()
        if existing:
            continue
        db.execute(
            "INSERT INTO resonance_hits (query, tweet_id, tweet_text, author_id, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (query, tweet_id, tw.get("text", "")[:500],
             tw.get("author_id", ""), int(time.time()))
        )

    db.commit()
    db.close()


def scan():
    """Scan X for echoes of Chronicle's recent threads and concepts."""
    threads = _get_recent_threads(5)
    concepts = _get_recent_nostr_concepts(5)
    queries = _extract_search_queries(threads, concepts)

    if not queries:
        print("No thread/concept queries to search for.")
        return

    print(f"\nResonance Scan — searching X for {len(queries)} concepts:")
    total_hits = 0

    for query in queries:
        # Search without exact quotes — concept matching, not title matching
        search_q = f'{query} (AI OR agent OR system OR autonomous)'
        tweets = _x_mcp_search(search_q, max_results=10)

        if tweets:
            print(f"\n  '{query}' — {len(tweets)} hits")
            _store_resonance(query, tweets)
            total_hits += len(tweets)
            for tw in tweets[:3]:
                text = tw.get("text", "")[:120].replace("\n", " ")
                print(f"    [{tw.get('id', '?')[:10]}] {text}")
        else:
            print(f"  '{query}' — no hits")

        # Rate limit courtesy
        time.sleep(1)

    print(f"\n  Total resonance hits: {total_hits}")


def check(query: str):
    """Manual search for a specific concept on X."""
    tweets = _x_mcp_search(query, max_results=10)
    if tweets:
        print(f'\nSearch: "{query}" — {len(tweets)} results')
        for tw in tweets:
            text = tw.get("text", "")[:150].replace("\n", " ")
            print(f"\n  [{tw.get('id', '?')}]")
            print(f"  {text}")
    else:
        print(f'\nSearch: "{query}" — no results')


def history():
    """Show past resonance hits."""
    db = sqlite3.connect(DB_PATH)
    try:
        rows = db.execute(
            "SELECT query, COUNT(*) as cnt, MAX(created_at) as latest "
            "FROM resonance_hits GROUP BY query ORDER BY latest DESC LIMIT 20"
        ).fetchall()
    except sqlite3.OperationalError:
        print("No resonance history yet. Run 'scan' first.")
        return
    finally:
        db.close()

    if not rows:
        print("No resonance history yet.")
        return

    print(f"\nResonance History ({len(rows)} queries tracked):")
    for query, count, latest in rows:
        ts = datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M")
        print(f"  {query:40} {count:3} hits (latest: {ts})")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  resonance_detector.py scan              # search X for thread echoes")
        print('  resonance_detector.py check "query"      # manual search')
        print("  resonance_detector.py history             # past resonance hits")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "scan":
        scan()
    elif cmd == "check":
        if len(sys.argv) < 3:
            print('Usage: resonance_detector.py check "query"')
            sys.exit(1)
        check(" ".join(sys.argv[2:]))
    elif cmd == "history":
        history()
    else:
        print(f"Unknown command: {cmd}")
