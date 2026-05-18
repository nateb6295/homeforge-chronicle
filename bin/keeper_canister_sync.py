#!/usr/bin/env python3
"""keeper_canister_sync.py — Push local keeper connections to Keeper canister.

Maps capsule_graph rows to CausalEdge format and calls import_causal_edges
on the Keeper canister via dfx. Tracks last-synced ID to support incremental
pushes.

NOT on a cron. Run manually when durability profile changes.

Usage:
  keeper_canister_sync.py status          # Show local vs on-chain counts
  keeper_canister_sync.py push [--batch N] [--limit N] [--top N]
                                          # Push connections
  keeper_canister_sync.py push --top 10000  # Push strongest 10K only
  keeper_canister_sync.py push --dry      # Show what would be pushed
  keeper_canister_sync.py reset           # Reset sync cursor to 0
"""
import json
import os
import sqlite3
import subprocess
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
KEEPER_CANISTER = "mjoko-laaaa-aaaai-q7tlq-cai"
IDENTITY = "chronicle-auto"
BATCH_SIZE = 200  # Keeper sync guard MAX_SYNC_BATCH = 250, stay under
STATE_FILE = os.path.expanduser("~/chronicle/data/keeper_sync_state.json")


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_synced_id": 0, "total_pushed": 0, "last_push_at": 0}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_local_count():
    db = sqlite3.connect(DB_PATH)
    count = db.execute("SELECT COUNT(*) FROM capsule_graph").fetchone()[0]
    db.close()
    return count


def get_connections(since_id=0, limit=None, top_n=None):
    """Get connections to sync, ordered by ID (incremental) or quality (top-N)."""
    db = sqlite3.connect(DB_PATH)
    if top_n:
        rows = db.execute(
            "SELECT id, capsule_a, capsule_b, quality, similarity, relationship, created_at "
            "FROM capsule_graph ORDER BY quality DESC LIMIT ?",
            (top_n,)
        ).fetchall()
    else:
        query = (
            "SELECT id, capsule_a, capsule_b, quality, similarity, relationship, created_at "
            "FROM capsule_graph WHERE id > ? ORDER BY id ASC"
        )
        if limit:
            query += f" LIMIT {limit}"
        rows = db.execute(query, (since_id,)).fetchall()
    db.close()
    return rows


def rows_to_causal_edges(rows):
    """Map capsule_graph rows to CausalEdge Candid format."""
    edges = []
    for row in rows:
        _id, capsule_a, capsule_b, quality, similarity, relationship, created_at = row
        context = relationship if relationship else f"sim={similarity:.3f}"
        edges.append({
            "id": 0,  # assigned by canister
            "source_id": str(capsule_a),
            "target_id": str(capsule_b),
            "edge_type": "keeper_connection",
            "strength": round(quality, 4),
            "context": context[:200],
            "created_at": created_at or 0,
        })
    return edges


def edges_to_candid(edges):
    """Format edges as Candid text for dfx CLI."""
    parts = []
    for e in edges:
        ctx = e["context"].replace('"', '\\"')
        parts.append(
            f'record {{ id = {e["id"]} : nat64; '
            f'source_id = "{e["source_id"]}"; '
            f'target_id = "{e["target_id"]}"; '
            f'edge_type = "{e["edge_type"]}"; '
            f'strength = {e["strength"]} : float64; '
            f'context = "{ctx}"; '
            f'created_at = {e["created_at"]} : nat64 }}'
        )
    return f'(vec {{ {"; ".join(parts)} }})'


def call_import(candid_arg, dry=False):
    """Call import_causal_edges on Keeper canister."""
    if dry:
        return True, "dry run"
    try:
        result = subprocess.run(
            ["dfx", "canister", "--network", "ic", "--identity", IDENTITY,
             "call", KEEPER_CANISTER, "import_causal_edges", candid_arg],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, result.stderr.strip()[:300]
    except subprocess.TimeoutExpired:
        return False, "timeout (60s)"


def cmd_status():
    state = load_state()
    local = get_local_count()
    unsynced = 0
    if state["last_synced_id"] > 0:
        db = sqlite3.connect(DB_PATH)
        unsynced = db.execute(
            "SELECT COUNT(*) FROM capsule_graph WHERE id > ?",
            (state["last_synced_id"],)
        ).fetchone()[0]
        db.close()

    print(f"Local connections: {local:,}")
    print(f"Total pushed:      {state['total_pushed']:,}")
    print(f"Last synced ID:    {state['last_synced_id']}")
    print(f"Unsynced:          {unsynced:,}" if state["last_synced_id"] > 0 else f"Unsynced:          {local:,} (never synced)")
    if state["last_push_at"]:
        ago = (time.time() - state["last_push_at"]) / 3600
        print(f"Last push:         {ago:.1f}h ago")


def cmd_push(batch_size=200, limit=None, top_n=None, dry=False):
    state = load_state()

    if top_n:
        rows = get_connections(top_n=top_n)
        print(f"Pushing top {top_n} connections by quality ({len(rows)} found)")
    else:
        rows = get_connections(since_id=state["last_synced_id"], limit=limit)
        print(f"Pushing {len(rows)} connections since ID {state['last_synced_id']}")

    if not rows:
        print("Nothing to push.")
        return

    pushed = 0
    max_id = state["last_synced_id"]

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        edges = rows_to_causal_edges(batch)
        candid = edges_to_candid(edges)

        batch_max_id = max(r[0] for r in batch)
        print(f"  Batch {i // batch_size + 1}: {len(edges)} edges (IDs up to {batch_max_id})", end="")

        if dry:
            print(" [dry run]")
            pushed += len(edges)
            max_id = max(max_id, batch_max_id)
            continue

        ok, msg = call_import(candid)
        if ok:
            pushed += len(edges)
            max_id = max(max_id, batch_max_id)
            print(f" ✓ {msg}")
        else:
            print(f" ✗ {msg}")
            print("  Stopping on error.")
            break

    if not dry and pushed > 0:
        state["last_synced_id"] = max_id
        state["total_pushed"] += pushed
        state["last_push_at"] = int(time.time())
        save_state(state)

    print(f"\nPushed: {pushed:,} connections" + (" [dry run]" if dry else ""))


def cmd_reset():
    save_state({"last_synced_id": 0, "total_pushed": 0, "last_push_at": 0})
    print("Sync cursor reset to 0.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]

    batch_size = BATCH_SIZE
    limit = None
    top_n = None
    dry = "--dry" in sys.argv

    for i, arg in enumerate(sys.argv):
        if arg == "--batch" and i + 1 < len(sys.argv):
            batch_size = int(sys.argv[i + 1])
        elif arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
        elif arg == "--top" and i + 1 < len(sys.argv):
            top_n = int(sys.argv[i + 1])

    if cmd == "status":
        cmd_status()
    elif cmd == "push":
        cmd_push(batch_size=batch_size, limit=limit, top_n=top_n, dry=dry)
    elif cmd == "reset":
        cmd_reset()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
