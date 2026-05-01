#!/usr/bin/env python3
"""Adoption backfill — run the extractor over historical activity_feed rows.

Preserves the row's original created_at so the year-curve shape surfaces.
Uses the same _extract_edges / _find_geos logic as adoption_extractor
so the backfilled edges are shape-compatible with the live pipeline.

Usage:
  python3 adoption_backfill.py --days 7        # last 7d sample
  python3 adoption_backfill.py --days 365      # full year (the real lever)
  python3 adoption_backfill.py --days 7 --dry  # classify+extract, don't insert
"""
import sys
import sqlite3
import argparse
import importlib.util
from pathlib import Path

EXT_PATH = Path(__file__).parent / "adoption_extractor.py"
spec = importlib.util.spec_from_file_location("adoption_extractor", EXT_PATH)
ext = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ext)

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

# Minimum signal: must mention a chain term AND an adoption keyword.
CHAIN_TERMS = ("xrp", "ripple", "xrpl", "rlusd",
               "internet computer", " icp ", "dfinity")
ADOPTION_KW = ("partner", "partnership", "adopt", "integrat", "pilot",
               "launches", "launched", "deploy", "selects", "selected",
               "bank", "insurance", "insurer", "custodian", "custody",
               "cbdc", "institution", "institutional", "settlement",
               "on-chain", "onchain", "brings", "bring")

# Source lanes — how to classify which side of the lattice an item belongs to.
def _classify_lane(text):
    low = text.lower()
    xrp_hit = any(t in low for t in ("xrp", "ripple", "xrpl", "rlusd"))
    icp_hit = any(t in low for t in ("internet computer", " icp ", "dfinity"))
    if xrp_hit and not icp_hit:
        return "XRP-ADOPT"
    if icp_hit and not xrp_hit:
        return "ICP-ADOPT"
    if xrp_hit and icp_hit:
        return "XRP-ADOPT"  # prefer XRP lane when both; conservative
    return None


def _candidate(text):
    low = text.lower()
    chain = any(t in low for t in CHAIN_TERMS)
    kw = any(k in low for k in ADOPTION_KW)
    return chain and kw


def _title_from_row(title, content):
    if title and title.strip():
        return title.strip()
    first_line = (content or "").split("\n", 1)[0].strip()
    return first_line[:200]


def _summary_from_row(title, content):
    if not content:
        return ""
    # If title is a prefix of content, slice past it.
    if title and content.startswith(title):
        return content[len(title):].strip()
    return content[:2000]


def _url_from_row(source, activity_type, content, row_id):
    # Best effort — capture URLs often sit in content first line.
    import re
    m = re.search(r"https?://\S+", content or "")
    if m:
        return m.group(0)
    return f"backfill://{source}/{activity_type}/{row_id}"


def _store_historical(conn, edges, lane, geo, title, url, created_at):
    for e in edges:
        conn.execute(
            """INSERT OR IGNORE INTO adoption_edges
               (subject, predicate, object, lane, geo, source_title, source_url, confidence, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (e["subject"], e["predicate"], e["object"], lane, geo,
             title[:300], url, e["confidence"], created_at),
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ext._ensure_schema(conn)

    cutoff = f"strftime('%s','now')-{args.days}*86400"
    rows = conn.execute(
        f"""SELECT id, source, activity_type, title, content, created_at
            FROM activity_feed
            WHERE created_at > {cutoff}
              AND (content LIKE '%XRP%' OR content LIKE '%Ripple%'
                   OR content LIKE '%XRPL%' OR content LIKE '%Internet Computer%'
                   OR content LIKE '%ICP%')""",
    ).fetchall()

    scanned = len(rows)
    matched = 0
    edges_total = 0
    inserted_total = 0

    pre = conn.execute("SELECT COUNT(*) FROM adoption_edges").fetchone()[0]

    for rid, source, atype, title, content, created_at in rows:
        full = f"{title or ''} {content or ''}"
        if not _candidate(full):
            continue
        lane = _classify_lane(full)
        if not lane:
            continue
        matched += 1

        t = _title_from_row(title, content)
        s = _summary_from_row(title, content)
        url = _url_from_row(source, atype, content, rid)

        edges = ext._extract_edges(t, s, lane, url)
        edges_total += len(edges)

        if not edges:
            continue
        geos = ext._find_geos(f"{t} {s}")
        geo = geos[0] if geos else None

        if not args.dry:
            _store_historical(conn, edges, lane, geo, t, url, created_at)

    if not args.dry:
        conn.commit()

    post = conn.execute("SELECT COUNT(*) FROM adoption_edges").fetchone()[0]
    inserted_total = post - pre

    print(f"BACKFILL --days={args.days} (dry={args.dry})")
    print(f"  scanned candidate rows: {scanned}")
    print(f"  classified matches:     {matched}")
    print(f"  edges extracted:        {edges_total}")
    print(f"  edges inserted (new):   {inserted_total}")
    print(f"  total edges pre → post: {pre} → {post}")


if __name__ == "__main__":
    main()
