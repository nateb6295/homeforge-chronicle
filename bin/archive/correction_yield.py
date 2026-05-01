#!/usr/bin/env python3
"""Correction Yield — Build #69.

Measures whether contested captures (those that generate corrections,
fabrication flags, or provocateur challenges) produce more downstream
learning than clean ones.

Thread #291 core question: "Is a system that only processes correct
inputs actually dumber than one that processes a mix?"

This tool answers empirically by tracing captures through the pipeline
and comparing downstream signal generation for contested vs clean inputs.

Usage:
    python3 correction_yield.py              # Full yield report
    python3 correction_yield.py summary      # One-line summary
    python3 correction_yield.py captures [N] # Per-capture breakdown (last N days)
"""

import os
import re
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


def _get_captures(conn, days=7):
    """Get all curator captures in the window."""
    cutoff = int(time.time()) - (days * 86400)
    rows = conn.execute(
        "SELECT id, content, created_at FROM activity_feed "
        "WHERE source LIKE 'discord:capture%' AND created_at > ? "
        "ORDER BY created_at ASC",
        (cutoff,)
    ).fetchall()
    return rows


def _get_briefs_for_capture(conn, capture_id, capture_ts):
    """Find briefs generated from a capture (within 10 min window after)."""
    window_start = capture_ts
    window_end = capture_ts + 600  # 10 min
    rows = conn.execute(
        "SELECT id, source, content, created_at FROM activity_feed "
        "WHERE activity_type = 'brief' AND created_at BETWEEN ? AND ? "
        "ORDER BY created_at ASC",
        (window_start, window_end)
    ).fetchall()
    return rows


def _get_fab_flags(conn, days=7):
    """Get spot check fabrication flags."""
    cutoff = int(time.time()) - (days * 86400)
    rows = conn.execute(
        "SELECT id, content, created_at FROM activity_feed "
        "WHERE source = 'spot_check' AND activity_type = 'audit' "
        "AND created_at > ? ORDER BY created_at ASC",
        (cutoff,)
    ).fetchall()
    # Parse out flagged brief IDs from spot check content
    flagged_ids = set()
    for row in rows:
        content = row["content"] or ""
        # Pattern: "LLM flag #NNNNN: FABRICATION"
        for m in re.finditer(r'flag #(\d+):\s*FABRICATION', content):
            flagged_ids.add(int(m.group(1)))
    return flagged_ids


def _get_seed_obs_for_briefs(conn, brief_ids, brief_timestamps):
    """Map activity_feed brief IDs to seed_observations IDs via timestamp+content."""
    seed_ids = set()
    for bf_id, bf_ts in zip(brief_ids, brief_timestamps):
        rows = conn.execute(
            "SELECT id FROM seed_observations "
            "WHERE source = 'activity:intern:brief' "
            "AND timestamp BETWEEN ? AND ?",
            (bf_ts - 60, bf_ts + 120)
        ).fetchall()
        for r in rows:
            seed_ids.add(r["id"])
    return seed_ids


def _get_crossref_connections(conn, brief_ids, brief_timestamps=None):
    """Count crossref connections involving these briefs.

    Crossref connections reference seed_observations IDs, not activity_feed IDs.
    We trace through the seed_observations table.
    """
    if not brief_ids:
        return 0, []
    # If we have timestamps, trace through seed_observations
    if brief_timestamps:
        seed_ids = _get_seed_obs_for_briefs(conn, brief_ids, brief_timestamps)
        if not seed_ids:
            return 0, []
        placeholders = ",".join("?" * len(seed_ids))
        rows = conn.execute(
            f"SELECT brief_a_id, brief_b_id, similarity, connection_text "
            f"FROM crossref_connections "
            f"WHERE brief_a_id IN ({placeholders}) OR brief_b_id IN ({placeholders})",
            list(seed_ids) + list(seed_ids)
        ).fetchall()
        return len(rows), rows
    # Fallback: direct lookup (won't match for newer data)
    placeholders = ",".join("?" * len(brief_ids))
    rows = conn.execute(
        f"SELECT brief_a_id, brief_b_id, similarity, connection_text "
        f"FROM crossref_connections "
        f"WHERE brief_a_id IN ({placeholders}) OR brief_b_id IN ({placeholders})",
        list(brief_ids) + list(brief_ids)
    ).fetchall()
    return len(rows), rows


def _get_voice_mentions(conn, capture_content, days=7):
    """Count agent voices that reference this capture's topic."""
    cutoff = int(time.time()) - (days * 86400)
    # Extract URL or key terms from capture
    url_match = re.search(r'https?://\S+', capture_content or "")
    if url_match:
        # Search for URL fragment in voices
        url = url_match.group()
        # Use domain or path fragment
        parts = url.split("/")
        if len(parts) > 3:
            search_term = parts[-1][:30] if parts[-1] else parts[-2][:30]
        else:
            search_term = url[:50]
        rows = conn.execute(
            "SELECT COUNT(*) as cnt FROM agent_voice "
            "WHERE content LIKE ? AND created_at > ?",
            (f"%{search_term}%", cutoff)
        ).fetchone()
        return rows["cnt"]
    return 0


def yield_report(days=7):
    """Full correction yield analysis."""
    conn = _db()
    captures = _get_captures(conn, days)
    fab_flags = _get_fab_flags(conn, days)

    total_captures = len(captures)
    if total_captures == 0:
        return "No captures in window."

    # Track per-capture metrics
    contested = []  # Captures with any correction signal
    clean = []      # Captures with no correction signal

    for cap in captures:
        cap_id = cap["id"]
        cap_ts = cap["created_at"]
        cap_content = cap["content"]

        briefs = _get_briefs_for_capture(conn, cap_id, cap_ts)
        brief_ids = [b["id"] for b in briefs]
        brief_timestamps = [b["created_at"] for b in briefs]

        # Check if any brief was fab-flagged
        is_fab_flagged = bool(brief_ids and fab_flags & set(brief_ids))

        # Count crossref connections (traces through seed_observations)
        cx_count, _ = _get_crossref_connections(
            conn, brief_ids, brief_timestamps
        )

        # Count voice mentions
        voice_count = _get_voice_mentions(conn, cap_content, days)

        # Downstream signal score: briefs + connections + voices
        downstream = len(briefs) + cx_count + voice_count

        entry = {
            "id": cap_id,
            "briefs": len(briefs),
            "crossref": cx_count,
            "voices": voice_count,
            "fab_flagged": is_fab_flagged,
            "downstream": downstream,
            "ts": cap_ts,
        }

        if is_fab_flagged or cx_count > 2:
            # Contested: had a correction event OR unusually high crossref
            contested.append(entry)
        else:
            clean.append(entry)

    # Compute averages
    avg_downstream_contested = (
        sum(e["downstream"] for e in contested) / len(contested)
        if contested else 0
    )
    avg_downstream_clean = (
        sum(e["downstream"] for e in clean) / len(clean)
        if clean else 0
    )
    avg_crossref_contested = (
        sum(e["crossref"] for e in contested) / len(contested)
        if contested else 0
    )
    avg_crossref_clean = (
        sum(e["crossref"] for e in clean) / len(clean)
        if clean else 0
    )

    # Correction yield ratio
    if avg_downstream_clean > 0:
        yield_ratio = avg_downstream_contested / avg_downstream_clean
    else:
        yield_ratio = float("inf") if avg_downstream_contested > 0 else 1.0

    lines = [
        f"CORRECTION YIELD — {days}d window, {total_captures} captures",
        f"",
        f"  Contested captures: {len(contested)} ({100*len(contested)/total_captures:.0f}%)",
        f"  Clean captures:     {len(clean)} ({100*len(clean)/total_captures:.0f}%)",
        f"",
        f"  Avg downstream (contested): {avg_downstream_contested:.1f}",
        f"  Avg downstream (clean):     {avg_downstream_clean:.1f}",
        f"  Yield ratio:                {yield_ratio:.2f}x",
        f"",
        f"  Avg crossref (contested):   {avg_crossref_contested:.1f}",
        f"  Avg crossref (clean):       {avg_crossref_clean:.1f}",
        f"",
    ]

    # Interpretation
    if yield_ratio > 1.2:
        lines.append(f"  ✦ Contested captures generate {yield_ratio:.1f}x more downstream signal.")
        lines.append(f"    Thread #291 thesis SUPPORTED: wrong inputs produce more learning.")
    elif yield_ratio > 0.8:
        lines.append(f"  ≈ Contested and clean captures generate similar signal.")
        lines.append(f"    Thread #291: wrongness neither helps nor hurts.")
    else:
        lines.append(f"  ✧ Clean captures generate more downstream signal.")
        lines.append(f"    Thread #291 thesis CHALLENGED: wrong inputs may be noise.")

    # Fab rate within captures
    fab_count = sum(1 for e in contested if e["fab_flagged"])
    lines.append(f"")
    lines.append(f"  Fab-flagged captures: {fab_count}")

    conn.close()
    return "\n".join(lines)


def summary(days=7):
    """One-line summary for dashboards."""
    conn = _db()
    captures = _get_captures(conn, days)
    fab_flags = _get_fab_flags(conn, days)
    total = len(captures)
    if total == 0:
        return "No captures."

    contested = 0
    for cap in captures:
        briefs = _get_briefs_for_capture(conn, cap["id"], cap["created_at"])
        brief_ids = [b["id"] for b in briefs]
        brief_ts = [b["created_at"] for b in briefs]
        is_fab = bool(brief_ids and fab_flags & set(brief_ids))
        cx, _ = _get_crossref_connections(conn, brief_ids, brief_ts)
        if is_fab or cx > 2:
            contested += 1

    conn.close()
    return f"Correction yield: {contested}/{total} contested ({100*contested/total:.0f}%), {days}d"


def per_capture(days=3):
    """Per-capture breakdown showing downstream signal chain."""
    conn = _db()
    captures = _get_captures(conn, days)
    fab_flags = _get_fab_flags(conn, days)

    lines = [f"PER-CAPTURE YIELD — {days}d, {len(captures)} captures\n"]

    for cap in captures[-20:]:  # Last 20
        cap_id = cap["id"]
        content = (cap["content"] or "")[:80]
        briefs = _get_briefs_for_capture(conn, cap_id, cap["created_at"])
        brief_ids = [b["id"] for b in briefs]
        brief_ts = [b["created_at"] for b in briefs]
        is_fab = bool(brief_ids and fab_flags & set(brief_ids))
        cx, _ = _get_crossref_connections(conn, brief_ids, brief_ts)
        voices = _get_voice_mentions(conn, cap["content"], days)
        downstream = len(briefs) + cx + voices

        marker = "⚡" if (is_fab or cx > 2) else "·"
        fab_tag = " [FAB]" if is_fab else ""
        lines.append(
            f"  {marker} #{cap_id}: {len(briefs)}b {cx}cx {voices}v "
            f"= {downstream} signal{fab_tag}  {content}"
        )

    conn.close()
    return "\n".join(lines)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    if cmd == "summary":
        print(summary())
    elif cmd == "captures":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        print(per_capture(days))
    else:
        days = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 7
        print(yield_report(days))
