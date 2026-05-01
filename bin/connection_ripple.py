#!/usr/bin/env python3
"""Connection Ripple — maps capture-to-capture relationships via embeddings.

The Gemma bridge scores captures against the active thread (capture→thread).
This script maps the topology BETWEEN captures using embedding similarity:
which captures converge, which are independent, what clusters form.

Uses mxbai-embed-large via Ollama for fast pairwise similarity.
Stores connections in the DB. Produces a readable map for Discord.

Usage:
  connection_ripple.py run [--window HOURS]   # Score recent captures, store connections
  connection_ripple.py show [--window HOURS]  # Show connection map
  connection_ripple.py discord [--window HOURS] # Post map to Discord
"""

import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from itertools import combinations

import requests

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"

# Cosine similarity → 0-10 score mapping
SIM_THRESHOLDS = [
    (0.82, 10), (0.78, 9), (0.74, 8), (0.70, 7),
    (0.66, 6), (0.62, 5), (0.58, 4), (0.54, 3),
    (0.48, 2), (0.40, 1),
]


def sim_to_score(sim):
    for threshold, score in SIM_THRESHOLDS:
        if sim >= threshold:
            return score
    return 0


def cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def embed(text):
    """Get embedding via Ollama."""
    resp = requests.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": text[:500]},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["embeddings"][0]


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS capture_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        capture_a_id INTEGER NOT NULL,
        capture_b_id INTEGER NOT NULL,
        score INTEGER NOT NULL,
        similarity REAL,
        created_at INTEGER NOT NULL,
        UNIQUE(capture_a_id, capture_b_id)
    )""")
    conn.commit()
    return conn


def get_recent_captures(db, hours=24):
    """Get captures from the last N hours."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT id, title, content, source, created_at FROM activity_feed "
        "WHERE activity_type = 'capture' AND created_at >= ? "
        "ORDER BY created_at ASC",
        (cutoff,),
    ).fetchall()
    captures = []
    for row_id, title, content, source, ts in rows:
        raw_content = (content or "")
        # Extract @Author: first sentence from content
        match = re.search(r'@([^:]+):\s*(.+?)(?:\n|$)', raw_content)
        if match:
            author = match.group(1).strip()[:20]
            snippet = match.group(2).strip()[:60]
            label = f"{author}: {snippet}"
        else:
            # Strip Discord/URL prefixes
            label = re.sub(r'^\[Discord #\w+\]\s*\w+:\s*', '', raw_content).strip()
            label = re.sub(r'^https?://\S+\s*', '', label).strip()[:80]
            if not label:
                label = (title or "unknown")[:80]
        # Extract author handle for diversity tracking
        author_match = re.search(r'@(\w+)', raw_content)
        author = author_match.group(1).lower() if author_match else "unknown"
        captures.append({
            "id": row_id,
            "label": label,
            "content": (content or "")[:500],
            "time": datetime.fromtimestamp(ts, PDT).strftime("%H:%M"),
            "author": author,
        })
    return captures


def cmd_run(hours=24):
    """Embed captures, compute pairwise similarity, store connections."""
    db = _db()
    captures = get_recent_captures(db, hours)

    if len(captures) < 2:
        print(f"Need >=2 captures. Found {len(captures)}.")
        db.close()
        return 0

    # Check which pairs already scored
    pairs_needed = []
    for a, b in combinations(captures, 2):
        existing = db.execute(
            "SELECT id FROM capture_connections WHERE capture_a_id=? AND capture_b_id=?",
            (a["id"], b["id"]),
        ).fetchone()
        if not existing:
            pairs_needed.append((a, b))

    if not pairs_needed:
        print(f"{len(captures)} captures, all pairs already scored.")
        db.close()
        return 0

    # Get unique capture IDs that need embeddings
    needs_embed = set()
    for a, b in pairs_needed:
        needs_embed.add(a["id"])
        needs_embed.add(b["id"])

    print(f"Embedding {len(needs_embed)} captures...")
    embeddings = {}
    cap_map = {c["id"]: c for c in captures}
    for cid in needs_embed:
        cap = cap_map[cid]
        try:
            embeddings[cid] = embed(cap["content"])
        except Exception as e:
            print(f"  Embed failed for {cid}: {e}", file=sys.stderr)
            continue

    print(f"Scoring {len(pairs_needed)} pairs...")
    now = int(time.time())
    connected = 0

    for a, b in pairs_needed:
        if a["id"] not in embeddings or b["id"] not in embeddings:
            continue
        sim = cosine_sim(embeddings[a["id"]], embeddings[b["id"]])
        score = sim_to_score(sim)
        try:
            db.execute(
                "INSERT INTO capture_connections (capture_a_id, capture_b_id, score, similarity, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (a["id"], b["id"], score, round(sim, 4), now),
            )
        except sqlite3.IntegrityError:
            pass
        if score >= 6:
            connected += 1
            print(f"  [{score}] (sim={sim:.3f}) {a['label'][:40]} <-> {b['label'][:40]}")

    db.commit()
    db.close()
    total = len(pairs_needed)
    print(f"\nScored {total} pairs. {connected} connections (>=6).")
    return 0


def cmd_show(hours=24):
    """Show the connection map for recent captures."""
    db = _db()
    captures = get_recent_captures(db, hours)
    if not captures:
        print("No captures in window.")
        db.close()
        return 0

    cap_ids = [c["id"] for c in captures]
    cap_map = {c["id"]: c for c in captures}

    placeholders = ",".join("?" * len(cap_ids))
    connections = db.execute(
        f"SELECT capture_a_id, capture_b_id, score, similarity FROM capture_connections "
        f"WHERE capture_a_id IN ({placeholders}) AND capture_b_id IN ({placeholders}) "
        f"AND score >= 6 ORDER BY score DESC",
        cap_ids + cap_ids,
    ).fetchall()
    db.close()

    # Build adjacency
    adj = {}
    for a_id, b_id, score, sim in connections:
        adj.setdefault(a_id, []).append((b_id, score, sim))
        adj.setdefault(b_id, []).append((a_id, score, sim))

    # Cluster detection via connected components at score >= 8 (cosine >= 0.74)
    # Lower thresholds collapse everything — mxbai-embed-large puts all
    # AI/neuro/consciousness content close in embedding space
    CLUSTER_THRESHOLD = 8
    visited = set()
    clusters = []
    for cap in captures:
        cid = cap["id"]
        if cid in visited:
            continue
        cluster = []
        stack = [cid]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            cluster.append(node)
            for neighbor, score, _ in adj.get(node, []):
                if score >= CLUSTER_THRESHOLD and neighbor not in visited:
                    stack.append(neighbor)
        clusters.append(cluster)

    # Sort clusters by size descending
    clusters.sort(key=len, reverse=True)

    print(f"Connection Map — last {hours}h")
    print(f"{len(captures)} captures, {len(connections)} connections (>=6)")
    print()

    cluster_num = 0
    for cluster in clusters:
        if len(cluster) > 1:
            cluster_num += 1
            cset = set(cluster)

            # Author diversity
            authors = set()
            for cid in cluster:
                cap = cap_map.get(cid)
                if cap:
                    authors.add(cap.get("author", "unknown"))
            authors.discard("unknown")
            diversity = len(authors)

            # Cluster quality — internal edge analysis
            internal_sims = []
            best_sim = 0
            best_pair = None
            for a_id, b_id, score, sim in connections:
                if a_id in cset and b_id in cset:
                    internal_sims.append(sim or 0)
                    if (sim or 0) > best_sim:
                        best_sim = sim or 0
                        best_pair = (a_id, b_id, score)

            if internal_sims:
                mean_sim = sum(internal_sims) / len(internal_sims)
                strong = sum(1 for s in internal_sims if s >= 0.82)
                strong_pct = strong / len(internal_sims)
                quality = "tight" if strong_pct >= 0.40 else "loose"
            else:
                mean_sim = 0
                strong_pct = 0
                quality = "?"

            div_tag = ""
            if diversity >= 3:
                div_tag = f" ★ CONVERGENCE ({quality})"
            elif diversity >= 2:
                div_tag = f" · cross-author ({quality})"
            else:
                div_tag = f" ({quality})"

            header = f"Cluster {cluster_num} ({len(cluster)} captures, {diversity} authors{div_tag}):"
            if internal_sims:
                header += f" avg={mean_sim:.3f}, {int(strong_pct*100)}% strong"
            print(header)

            for cid in cluster:
                cap = cap_map.get(cid)
                if cap:
                    print(f"  [{cap['time']}] {cap['label'][:70]}")
            if best_pair:
                a_l = cap_map.get(best_pair[0], {}).get("label", "?")[:25]
                b_l = cap_map.get(best_pair[1], {}).get("label", "?")[:25]
                print(f"  Strongest: {a_l} <-> {b_l} (sim={best_sim:.3f})")
            if diversity >= 2:
                print(f"  Authors: {', '.join(sorted(authors))}")
            print()

    isolates = [c for c in clusters if len(c) == 1]
    if isolates:
        print(f"Independent ({len(isolates)}):")
        for cluster in isolates:
            cid = cluster[0]
            cap = cap_map.get(cid)
            if cap:
                print(f"  [{cap['time']}] {cap['label'][:70]}")
    print()
    return 0


def cmd_discord(hours=24):
    """Post connection map to Discord."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        cmd_show(hours)

    text = buf.getvalue()
    if not text.strip():
        return 0

    chronicle_env = os.path.expanduser("~/chronicle/chronicle.env")
    webhook = ""
    if os.path.exists(chronicle_env):
        for line in open(chronicle_env):
            if line.strip().startswith("OPERATOR_WEBHOOK="):
                webhook = line.strip().split("=", 1)[1].strip("'\"")

    if not webhook:
        print("No OPERATOR_WEBHOOK found", file=sys.stderr)
        return 1

    msg = f"**Connection Ripple**\n```\n{text[:1700]}\n```"
    try:
        requests.post(webhook, json={"content": msg}, timeout=10)
        print("Posted to Discord.")
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]
    hours = 24
    for i, arg in enumerate(sys.argv):
        if arg == "--window" and i + 1 < len(sys.argv):
            hours = int(sys.argv[i + 1])

    if cmd == "run":
        sys.exit(cmd_run(hours))
    elif cmd == "show":
        sys.exit(cmd_show(hours))
    elif cmd == "discord":
        sys.exit(cmd_discord(hours))
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
