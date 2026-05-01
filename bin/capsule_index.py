#!/usr/bin/env python3
"""Capsule index — build a local conversation thread map from canister data.

The HTTP API doesn't expose conversation_id, so this uses dfx to sample
capsules and build a mapping of conversation threads.

Usage:
    capsule_index.py build [--step N]  # Sample every Nth capsule (default 10)
    capsule_index.py status            # Show index status
    capsule_index.py lookup ID         # Find conversation for a capsule ID
    capsule_index.py threads           # List all mapped conversations
    capsule_index.py thread CONV_ID    # Show capsules in a conversation

The index is stored in the conversation_threads table in processed.db.
Sampling fills in the map — denser sampling = more complete index.
"""

import subprocess
import sqlite3
import sys
import os
import re
import time
from collections import defaultdict
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DFX = os.path.expanduser("~/.local/share/dfx/bin/dfx")
NETWORK = "ic"
BACKEND = "fqqku-bqaaa-aaaai-q4wha-cai"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}", flush=True)


def dfx_env():
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    return env


def get_capsule_conv_id(capsule_id):
    """Fetch conversation_id for a capsule via dfx."""
    try:
        result = subprocess.run(
            [DFX, "canister", "--network", NETWORK, "call", BACKEND,
             "get_capsule", f"({capsule_id} : nat64)"],
            capture_output=True, text=True, timeout=15, env=dfx_env()
        )
        if "(null)" in result.stdout:
            return None, None, None

        conv_match = re.search(r'conversation_id = "([^"]+)"', result.stdout)
        topic_match = re.search(r'topic = opt "([^"]+)"', result.stdout)
        ts_match = re.search(r'timestamp = opt "([^"]+)"', result.stdout)

        conv_id = conv_match.group(1) if conv_match else None
        topic = topic_match.group(1) if topic_match else None
        timestamp = ts_match.group(1) if ts_match else None

        return conv_id, topic, timestamp
    except Exception:
        return None, None, None


def ensure_table(db):
    db.execute("""CREATE TABLE IF NOT EXISTS conversation_threads (
        capsule_id INTEGER PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        topic TEXT,
        timestamp TEXT,
        indexed_at INTEGER NOT NULL
    )""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_conv_threads_conv
        ON conversation_threads(conversation_id)""")
    db.commit()


def build_index(db, step=10, max_id=None):
    """Sample capsules and build conversation thread index."""
    ensure_table(db)

    # Find what we already have indexed
    existing = set(
        r[0] for r in db.execute("SELECT capsule_id FROM conversation_threads").fetchall()
    )
    log(f"Existing index: {len(existing)} capsules")

    # Find max capsule ID
    if max_id is None:
        row = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()
        max_id = row[0] if row and row[0] else 26500

    # Build sample points (skip already indexed)
    sample_points = [i for i in range(1, max_id + 1, step) if i not in existing]
    log(f"Sampling {len(sample_points)} points (step={step}, max={max_id})")

    indexed = 0
    batch = []
    now = int(time.time())

    for i, capsule_id in enumerate(sample_points):
        conv_id, topic, timestamp = get_capsule_conv_id(capsule_id)

        if conv_id:
            batch.append((capsule_id, conv_id, topic, timestamp, now))
            indexed += 1

        # Commit in batches of 20
        if len(batch) >= 20:
            db.executemany(
                "INSERT OR REPLACE INTO conversation_threads "
                "(capsule_id, conversation_id, topic, timestamp, indexed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                batch
            )
            db.commit()
            batch = []

        if (i + 1) % 50 == 0:
            log(f"Progress: {i + 1}/{len(sample_points)} sampled, {indexed} indexed")

    # Final batch
    if batch:
        db.executemany(
            "INSERT OR REPLACE INTO conversation_threads "
            "(capsule_id, conversation_id, topic, timestamp, indexed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            batch
        )
        db.commit()

    log(f"Done. Indexed {indexed} new capsules.")
    return indexed


def show_status(db):
    ensure_table(db)
    total = db.execute("SELECT COUNT(*) FROM conversation_threads").fetchone()[0]
    convs = db.execute("SELECT COUNT(DISTINCT conversation_id) FROM conversation_threads").fetchone()[0]
    min_id = db.execute("SELECT MIN(capsule_id) FROM conversation_threads").fetchone()[0]
    max_id = db.execute("SELECT MAX(capsule_id) FROM conversation_threads").fetchone()[0]
    print(f"Conversation Thread Index:")
    print(f"  Indexed capsules: {total}")
    print(f"  Unique conversations: {convs}")
    print(f"  ID range: {min_id} - {max_id}")


def lookup(db, capsule_id):
    ensure_table(db)
    row = db.execute(
        "SELECT conversation_id, topic, timestamp FROM conversation_threads WHERE capsule_id = ?",
        (capsule_id,)
    ).fetchone()
    if row:
        print(f"  Capsule #{capsule_id}: conversation={row[0]}, topic={row[1]}, time={row[2]}")
        # Show other capsules in same conversation
        siblings = db.execute(
            "SELECT capsule_id, topic, timestamp FROM conversation_threads "
            "WHERE conversation_id = ? ORDER BY capsule_id",
            (row[0],)
        ).fetchall()
        if len(siblings) > 1:
            print(f"  Same conversation ({len(siblings)} indexed):")
            for s in siblings:
                marker = " ←" if s[0] == capsule_id else ""
                print(f"    #{s[0]:>5d}  {s[1] or '':40s}  {s[2] or ''}{marker}")
    else:
        print(f"  Capsule #{capsule_id} not in index. Run 'build' to index it.")


def list_threads(db):
    ensure_table(db)

    rows = db.execute(
        "SELECT conversation_id, COUNT(*) as cnt, MIN(capsule_id), MAX(capsule_id), "
        "MIN(timestamp), GROUP_CONCAT(DISTINCT topic) "
        "FROM conversation_threads "
        "GROUP BY conversation_id ORDER BY MIN(capsule_id)"
    ).fetchall()

    # Classify
    imports = []
    live = []
    for row in rows:
        conv_id, count, min_id, max_id, min_ts, topics = row
        if conv_id.startswith(("mcp-", "http-", "chronicle-", "arxiv-")):
            live.append(row)
        else:
            imports.append(row)

    if imports:
        print("── IMPORTED CONVERSATIONS ──")
        for conv_id, count, min_id, max_id, min_ts, topics in imports:
            topics_short = (topics or "")[:60]
            date = (min_ts or "")[:10]
            print(f"  {conv_id[:8]}..  #{min_id:>5d}-{max_id:<5d}  ({count:>3d} sampled)  {date:10s}  {topics_short}")

    if live:
        print(f"\n── LIVE CHRONICLE ({len(live)} sources) ──")
        for conv_id, count, min_id, max_id, min_ts, topics in live[:20]:
            conv_short = conv_id[:30]
            date = (min_ts or "")[:10]
            print(f"  {conv_short:32s}  #{min_id:>5d}-{max_id:<5d}  ({count:>3d})  {date}")
        if len(live) > 20:
            print(f"  ... and {len(live) - 20} more")

    print(f"\nTotal: {len(imports)} imported, {len(live)} live")


def show_thread(db, conv_prefix):
    ensure_table(db)
    rows = db.execute(
        "SELECT capsule_id, topic, timestamp FROM conversation_threads "
        "WHERE conversation_id LIKE ? ORDER BY capsule_id",
        (f"{conv_prefix}%",)
    ).fetchall()

    if not rows:
        print(f"  No capsules found with conversation_id starting '{conv_prefix}'")
        return

    conv_id = db.execute(
        "SELECT conversation_id FROM conversation_threads WHERE conversation_id LIKE ? LIMIT 1",
        (f"{conv_prefix}%",)
    ).fetchone()[0]

    print(f"  Conversation: {conv_id}")
    print(f"  Capsules: {len(rows)} indexed\n")

    for cid, topic, ts in rows:
        ts_str = f"  [{ts}]" if ts else ""
        print(f"    #{cid:>5d}  {topic or '':50s}{ts_str}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    db = sqlite3.connect(DB_PATH, timeout=10)

    if args[0] == "build":
        step = 10
        max_id = None
        if "--step" in args:
            idx = args.index("--step")
            if idx + 1 < len(args):
                step = int(args[idx + 1])
        if "--max" in args:
            idx = args.index("--max")
            if idx + 1 < len(args):
                max_id = int(args[idx + 1])
        build_index(db, step=step, max_id=max_id)
    elif args[0] == "status":
        show_status(db)
    elif args[0] == "lookup" and len(args) >= 2:
        lookup(db, int(args[1]))
    elif args[0] == "threads":
        list_threads(db)
    elif args[0] == "thread" and len(args) >= 2:
        show_thread(db, args[1])
    else:
        print(__doc__)

    db.close()
