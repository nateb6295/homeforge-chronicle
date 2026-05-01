#!/usr/bin/env python3
"""self_model_for_arrival — select high-signal self-model observations to
surface in the rotation_startup_hook injection.

The gap (named in self-model #252 on 2026-04-27 08:43, demonstrated across
the day's arc, synthesized in #259): self-model accumulates wisdom that
the operating system doesn't access by default. Story tail handles
narrative continuity; CCS handles compressed identity; preferences are
surfaced via read_self_model.py --type preference. Observations sit in
the DB and don't get read on arrival unless the agent explicitly queries.

This script closes that gap by selecting recent high-confidence
observations and formatting them for injection.

Selection criteria (defaults; configurable):
  - Recency: last 14 days
  - Confidence: >= 0.85
  - Property type: observation (not preference — those go elsewhere)
  - Not superseded
  - Top N by created_at desc

Output: markdown formatted for the hook. Capped at ~2500 chars to fit
the 10k injection budget.
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import sys
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def fetch_observations(days: int = 14, min_confidence: float = 0.85,
                        limit: int = 8) -> list[tuple]:
    cutoff = int(datetime.now().timestamp()) - (days * 86400)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, capability, description, confidence, created_at
        FROM self_model
        WHERE property_type = 'observation'
          AND confidence >= ?
          AND superseded_by IS NULL
          AND created_at > ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (min_confidence, cutoff, limit),
    ).fetchall()
    conn.close()
    return rows


def truncate(text: str, n: int = 280) -> str:
    text = text.strip().replace("\n", " ")
    if len(text) <= n:
        return text
    return text[:n].rsplit(" ", 1)[0] + "…"


def format_for_injection(rows, max_chars: int = 2500) -> str:
    if not rows:
        return "(no recent high-signal self-model observations)"
    out = []
    out.append("Recent high-confidence observations from your self-model "
               "(filed by past instances; READ THESE — they're load-bearing):")
    out.append("")
    cur_len = sum(len(s) + 1 for s in out)
    for r in rows:
        ts = datetime.fromtimestamp(r["created_at"]).strftime("%Y-%m-%d")
        cap = r["capability"][:60]
        desc = truncate(r["description"], 280)
        line = f"#{r['id']} [{ts}] {cap}\n  {desc}"
        if cur_len + len(line) + 1 > max_chars:
            out.append(f"  (… {len(rows) - len(out) + 2} more entries truncated)")
            break
        out.append(line)
        cur_len += len(line) + 1
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--min-confidence", type=float, default=0.85)
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--max-chars", type=int, default=2500)
    args = ap.parse_args()

    rows = fetch_observations(args.days, args.min_confidence, args.limit)
    print(format_for_injection(rows, args.max_chars))


if __name__ == "__main__":
    main()
