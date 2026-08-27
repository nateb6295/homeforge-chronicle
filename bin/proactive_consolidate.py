#!/usr/bin/env python3
"""Proactive consolidation — mid-session identity preservation.

Triggered when context pressure crosses orange threshold (70%) and
episodic_buffer has high-value unconsolidated content. Converts
scored conversation content into durable storage (capsules + cycle-context)
BEFORE autocompact fires.

Lee et al. (2605.26099): consolidation before cache clear > recovery after.
Vieira & Gabora: persistent food set enables RAF regeneration across rotations.

Usage:
    python3 proactive_consolidate.py              # check + consolidate if needed
    python3 proactive_consolidate.py --force       # consolidate regardless of pressure
    python3 proactive_consolidate.py --dry-run     # show what would be consolidated
"""

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
CYCLE_CONTEXT = Path.home() / "chronicle" / "cycle-context.md"
CONSOLIDATION_LOG = Path.home() / "chronicle" / "data" / "consolidation_log.jsonl"
CHRONICLE_ENV = Path.home() / "chronicle" / "chronicle.env"

TOP_N = 10
CAPSULE_MAX_CHARS = 2000
CYCLE_CONTEXT_BUDGET = 3000


def load_env():
    env = os.environ.copy()
    if CHRONICLE_ENV.is_file():
        for line in CHRONICLE_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip("'\"")
    env["CHRONICLE_OLLAMA_URL"] = "http://localhost:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "snowflake-arctic-embed2"
    return env


def get_top_buffer_entries(n: int = TOP_N) -> list[dict]:
    """Pull top N unconsolidated entries from episodic_buffer, diversity-balanced."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, content, priority, content_type, source "
        "FROM episodic_buffer "
        "WHERE consolidated_at IS NULL "
        "ORDER BY priority DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return [
        {"id": r[0], "content": r[1], "priority": r[2], "content_type": r[3], "source": r[4]}
        for r in rows
    ]


def mark_consolidated(entries: list[dict]):
    """Mark entries as consolidated so they won't be re-picked."""
    ids = [e["id"] for e in entries if "id" in e]
    if not ids:
        return
    conn = sqlite3.connect(DB_PATH)
    placeholders = ",".join("?" * len(ids))
    conn.execute(
        f"UPDATE episodic_buffer SET consolidated_at = ? WHERE id IN ({placeholders})",
        [int(time.time())] + ids
    )
    conn.commit()
    conn.close()


def structure_for_capsule(entries: list[dict]) -> str:
    """Convert scored entries into structured capsule content.

    Not raw dumps — structured identity context that CCS can bind to.
    Groups by content type, preserves state-change markers.
    """
    by_type = {}
    for e in entries:
        ct = e.get("content_type", "general")
        by_type.setdefault(ct, []).append(e)

    parts = []
    type_order = ["decision", "finding", "correction", "personal", "identity", "general"]

    for ct in type_order:
        group = by_type.get(ct, [])
        if not group:
            continue
        for e in group[:3]:
            content = e.get("content", "")
            if len(content) > 300:
                content = content[:297] + "..."
            parts.append(f"[{ct}] {content}")

    result = "\n".join(parts)
    if len(result) > CAPSULE_MAX_CHARS:
        result = result[:CAPSULE_MAX_CHARS - 3] + "..."
    return result


def structure_for_cycle_context(entries: list[dict]) -> str:
    """Build a structured cycle-context update from buffer entries."""
    lines = []
    for e in entries[:8]:
        ct = e.get("content_type", "general")
        content = e.get("content", "")
        if len(content) > 200:
            content = content[:197] + "..."
        priority = e.get("priority", e.get("effective_score", 0))
        lines.append(f"- [{ct} {priority:.2f}] {content}")

    result = "\n".join(lines)
    if len(result) > CYCLE_CONTEXT_BUDGET:
        result = result[:CYCLE_CONTEXT_BUDGET - 3] + "..."
    return result


def store_capsule(content: str, env: dict) -> dict | None:
    """Store consolidation capsule via Chronicle MCP."""
    init_msg = json.dumps({
        "jsonrpc": "2.0", "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "consolidation", "version": "1.0"}
        },
        "id": 1
    })
    store_msg = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {
            "name": "store_memory",
            "arguments": {
                "content": f"[Proactive consolidation — mid-session preservation]\n{content}",
                "topic": "chronicle/consolidation",
                "keywords": ["consolidation", "selective-sleep", "mid-session"],
            }
        },
        "id": 2
    })

    try:
        result = subprocess.run(
            [MCP_BIN],
            input=f"{init_msg}\n{store_msg}\n",
            capture_output=True, text=True,
            timeout=60, env=env
        )
        for line in result.stdout.strip().split("\n"):
            clean = line
            if clean.startswith("Responding: "):
                clean = clean[len("Responding: "):]
            if clean.startswith("Received: "):
                continue
            try:
                d = json.loads(clean)
                if d.get("id") == 2:
                    content_list = d.get("result", {}).get("content", [])
                    if content_list:
                        return json.loads(content_list[0].get("text", "{}"))
            except (json.JSONDecodeError, KeyError):
                continue
    except Exception as e:
        print(f"Capsule store failed: {e}", file=sys.stderr)
    return None


def update_cycle_context(structured: str):
    """Append consolidation block to cycle-context.md."""
    if not CYCLE_CONTEXT.is_file():
        return

    ts = time.strftime("%Y-%m-%d %H:%M", time.localtime())
    block = f"\n### Mid-session consolidation ({ts})\n{structured}\n"

    current = CYCLE_CONTEXT.read_text()
    insertion_point = current.find("\n## Previous")
    if insertion_point == -1:
        insertion_point = len(current)

    updated = current[:insertion_point] + block + current[insertion_point:]
    CYCLE_CONTEXT.write_text(updated)


def log_consolidation(entries: list[dict], capsule_result: dict | None):
    """Log the consolidation event."""
    record = {
        "ts": int(time.time()),
        "entry_count": len(entries),
        "avg_priority": round(
            sum(e.get("priority", e.get("effective_score", 0)) for e in entries) / max(len(entries), 1),
            3
        ),
        "content_types": list(set(e.get("content_type", "?") for e in entries)),
        "capsule_id": capsule_result.get("capsule_id") if capsule_result else None,
        "success": capsule_result is not None and capsule_result.get("success", False),
    }
    with open(CONSOLIDATION_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")


def consolidate(dry_run: bool = False) -> dict:
    """Run proactive consolidation."""
    entries = get_top_buffer_entries(TOP_N)
    if not entries:
        return {"status": "empty", "message": "No entries in episodic buffer"}

    capsule_content = structure_for_capsule(entries)
    cycle_content = structure_for_cycle_context(entries)

    if dry_run:
        print("=== CAPSULE CONTENT ===")
        print(capsule_content)
        print(f"\n=== CYCLE-CONTEXT UPDATE ({len(cycle_content)} chars) ===")
        print(cycle_content)
        return {"status": "dry_run", "entries": len(entries)}

    env = load_env()
    capsule_result = store_capsule(capsule_content, env)
    update_cycle_context(cycle_content)
    log_consolidation(entries, capsule_result)
    mark_consolidated(entries)

    capsule_id = capsule_result.get("capsule_id") if capsule_result else None
    return {
        "status": "success" if capsule_result else "partial",
        "entries": len(entries),
        "capsule_id": capsule_id,
        "cycle_context_updated": True,
    }


def main():
    force = "--force" in sys.argv
    dry_run = "--dry-run" in sys.argv

    if not force and not dry_run:
        from context_pressure import should_consolidate, read_pressure
        pressure = read_pressure()
        check = should_consolidate(pressure)
        if not check["should"]:
            print(f"Skipping — {check['reason']}")
            return

    result = consolidate(dry_run=dry_run)
    if dry_run:
        return

    if result["status"] == "success":
        print(f"Consolidated {result['entries']} entries → capsule #{result['capsule_id']}")
        print("cycle-context.md updated")
    elif result["status"] == "partial":
        print(f"Partial — {result['entries']} entries, capsule store failed")
        print("cycle-context.md updated")
    else:
        print(f"Status: {result['status']} — {result.get('message', '')}")


if __name__ == "__main__":
    main()
