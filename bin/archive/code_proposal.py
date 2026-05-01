#!/usr/bin/env python3
"""Code Proposal System — Family agents propose code changes, Opus reviews.

Usage from agents:
  from code_proposal import propose_patch
  propose_patch(
      agent="darby",
      target="intern",
      description="Add timeout to web_search fallback",
      code_context="Line 35 in web_tools.py, the DDGS call has no timeout",
      suggestion="Add timeout=10 to DDGS().text() call",
      rationale="Saw 3 cycles where DDG hung for 60s+, blocking the pipeline"
  )

Usage from Opus (CLI):
  code_proposal.py list                    — show pending proposals
  code_proposal.py review ID accept|reject "reason"
  code_proposal.py apply ID               — apply accepted proposal via modify_agent.py
"""

import sys
import os
import time
import json
import sqlite3

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

# Agents allowed to propose
ALLOWED_PROPOSERS = {"darby", "ada", "opus"}

# Max proposals per agent per hour
MAX_PER_HOUR = 3


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table():
    """Create code_proposals table if it doesn't exist."""
    db = _db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS code_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            target_agent TEXT NOT NULL,
            description TEXT NOT NULL,
            code_context TEXT,
            suggestion TEXT NOT NULL,
            rationale TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            reviewer TEXT,
            review_note TEXT,
            created_at INTEGER NOT NULL,
            reviewed_at INTEGER
        )
    """)
    db.commit()
    db.close()


def propose_patch(agent: str, target: str, description: str,
                  suggestion: str, code_context: str = "",
                  rationale: str = "") -> bool:
    """Submit a code change proposal for Opus review."""
    if agent not in ALLOWED_PROPOSERS:
        return False

    _ensure_table()
    db = _db()

    # Rate limit
    cutoff = int(time.time()) - 3600
    count = db.execute(
        "SELECT COUNT(*) FROM code_proposals WHERE agent=? AND created_at>?",
        (agent, cutoff)
    ).fetchone()[0]
    if count >= MAX_PER_HOUR:
        db.close()
        return False

    db.execute(
        "INSERT INTO code_proposals "
        "(agent, target_agent, description, code_context, suggestion, rationale, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (agent, target, description, code_context, suggestion, rationale, int(time.time()))
    )
    db.commit()
    db.close()
    return True


def list_proposals(status="pending"):
    """List proposals by status."""
    _ensure_table()
    db = _db()
    rows = db.execute(
        "SELECT * FROM code_proposals WHERE status=? ORDER BY created_at DESC LIMIT 20",
        (status,)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def review_proposal(proposal_id: int, action: str, reviewer: str = "opus",
                    note: str = "") -> bool:
    """Accept or reject a proposal."""
    if action not in ("accept", "reject"):
        return False

    _ensure_table()
    db = _db()
    db.execute(
        "UPDATE code_proposals SET status=?, reviewer=?, review_note=?, reviewed_at=? WHERE id=?",
        (action + "ed", reviewer, note, int(time.time()), proposal_id)
    )
    db.commit()
    db.close()
    return True


# ═══ CLI ═══

def main():
    _ensure_table()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  code_proposal.py list [pending|accepted|rejected]")
        print("  code_proposal.py review ID accept|reject 'reason'")
        return

    action = sys.argv[1]

    if action == "list":
        status = sys.argv[2] if len(sys.argv) > 2 else "pending"
        proposals = list_proposals(status)
        if not proposals:
            print(f"No {status} proposals.")
            return
        print(f"{len(proposals)} {status} proposals:")
        for p in proposals:
            age = time.time() - p["created_at"]
            ago = f"{int(age/60)}m" if age < 3600 else f"{age/3600:.1f}h"
            print(f"\n  #{p['id']} [{p['agent']}→{p['target_agent']}] ({ago} ago)")
            print(f"    {p['description']}")
            if p['code_context']:
                print(f"    Context: {p['code_context'][:120]}")
            print(f"    Suggestion: {p['suggestion'][:200]}")
            if p['rationale']:
                print(f"    Why: {p['rationale'][:120]}")
            if p['review_note']:
                print(f"    Review: {p['review_note']}")

    elif action == "review":
        if len(sys.argv) < 4:
            print("Usage: code_proposal.py review ID accept|reject 'reason'")
            return
        pid = int(sys.argv[2])
        verdict = sys.argv[3]
        note = sys.argv[4] if len(sys.argv) > 4 else ""
        if review_proposal(pid, verdict, note=note):
            print(f"Proposal #{pid} {verdict}ed.")
        else:
            print(f"Failed to review proposal #{pid}")


if __name__ == "__main__":
    main()
