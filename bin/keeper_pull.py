#!/usr/bin/env python3
"""Keeper Pull — Sync keeper clusters and connections to local SQLite.

Runs on a systemd timer (every 30 minutes). Pulls from the on-chain Keeper
canister via Stem and upserts into keeper_clusters_local / keeper_connections_local
tables for fast agent queries.

Usage:
    python3 keeper_pull.py
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime

# Add bin dir to path for stem import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stem import Stem

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
KEEPER_CANISTER = "mjoko-laaaa-aaaai-q7tlq-cai"
DFX_BIN = os.environ.get("DFX_BIN", "dfx")
DFX_IDENTITY = os.environ.get("CHRONICLE_IDENTITY", "chronicle-auto")


def _log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [keeper-pull] {msg}", flush=True)


def _now() -> int:
    return int(time.time())


def main():
    _log("Starting keeper pull...")

    stem = Stem(KEEPER_CANISTER, DFX_BIN, DFX_IDENTITY)
    conn = sqlite3.connect(DB_PATH, timeout=10)

    # Ensure tables exist
    conn.execute("""CREATE TABLE IF NOT EXISTS keeper_clusters_local (
        id INTEGER PRIMARY KEY, theme TEXT NOT NULL, capsule_ids TEXT NOT NULL,
        strength REAL NOT NULL, updated_at INTEGER NOT NULL, pulled_at INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS keeper_connections_local (
        id INTEGER PRIMARY KEY AUTOINCREMENT, capsule_a INTEGER NOT NULL,
        capsule_b INTEGER NOT NULL, similarity REAL NOT NULL,
        connection_text TEXT, pulled_at INTEGER NOT NULL
    )""")
    conn.commit()

    now = _now()
    clusters_pulled = 0
    connections_pulled = 0

    # Pull clusters
    try:
        clusters = stem.pull_clusters()
        if clusters:
            for c in clusters:
                conn.execute(
                    "INSERT OR REPLACE INTO keeper_clusters_local "
                    "(id, theme, capsule_ids, strength, updated_at, pulled_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (c["id"], c["theme"], json.dumps(c["capsule_ids"]),
                     c["strength"], c["updated_at"], now),
                )
            conn.commit()
            clusters_pulled = len(clusters)
            _log(f"  Upserted {clusters_pulled} clusters")
        else:
            _log("  No clusters returned")
    except Exception as e:
        _log(f"  Cluster pull error: {e}")

    # Pull connections
    try:
        connections = stem.pull_connections(limit=10000)
        if connections:
            # Clear old and insert fresh (connections don't have stable IDs)
            conn.execute("DELETE FROM keeper_connections_local")
            for c in connections:
                conn.execute(
                    "INSERT INTO keeper_connections_local "
                    "(capsule_a, capsule_b, similarity, pulled_at) "
                    "VALUES (?, ?, ?, ?)",
                    (c["capsule_a"], c["capsule_b"], c["similarity"], now),
                )
            conn.commit()
            connections_pulled = len(connections)
            _log(f"  Inserted {connections_pulled} connections")
        else:
            _log("  No connections returned")
    except Exception as e:
        _log(f"  Connection pull error: {e}")

    # Log to activity_feed
    try:
        conn.execute(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("keeper-pull", "sync",
             f"Pulled {clusters_pulled} clusters, {connections_pulled} connections from keeper",
             now),
        )
        conn.commit()
    except Exception:
        pass

    conn.close()
    _log(f"Done. {clusters_pulled} clusters, {connections_pulled} connections.")


if __name__ == "__main__":
    main()
