#!/usr/bin/env python3
"""
Activity-feed hash chain — SICF §6.3 fix #2 prototype.

Produces tamper-evident anchors over activity_feed rows. Two modes:

  tail       — hash the last N rows (default 500), print the hash
  snapshot   — append a (timestamp, max_id, row_count, tail_hash, prev_hash)
               record to ~/chronicle/activity_hashes.log. Each record links to
               the previous via prev_hash, so the log itself is a hash chain.

Run `snapshot` periodically (e.g., hourly cron). Any silent rewrite of
historical activity_feed rows will surface as a chain break the next time
we hash the same id range.

v0 scope: local append-only log. v1 pushes chain heads to canister so the
anchor is externally verifiable (SICF §6.3 full satisfaction).

Usage:
  activity_hash.py tail [--rows N]
  activity_hash.py snapshot [--rows N]
  activity_hash.py verify [--limit N]   # re-hash each logged range, confirm match
  activity_hash.py head                 # show latest snapshot
"""
import argparse
import hashlib
import os
import sqlite3
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
LOG = Path(os.path.expanduser("~/chronicle/activity_hashes.log"))
DEFAULT_ROWS = 500


def hash_tail(db, rows):
    cur = db.execute(
        "SELECT id, source, activity_type, content, created_at "
        "FROM activity_feed ORDER BY id DESC LIMIT ?",
        (rows,),
    )
    h = hashlib.sha256()
    max_id = None
    count = 0
    for row in cur:
        if max_id is None:
            max_id = row[0]
        count += 1
        line = f"{row[0]}\x1f{row[1]}\x1f{row[2]}\x1f{row[3]}\x1f{row[4]}".encode("utf-8", errors="replace")
        h.update(line)
        h.update(b"\x1e")
    return h.hexdigest(), max_id or 0, count


def latest_log_record():
    if not LOG.exists():
        return None
    last = None
    for line in LOG.read_text().splitlines():
        line = line.strip()
        if line:
            last = line
    if not last:
        return None
    parts = last.split("\t")
    if len(parts) != 5:
        return None
    return {
        "timestamp": int(parts[0]),
        "max_id": int(parts[1]),
        "row_count": int(parts[2]),
        "tail_hash": parts[3],
        "prev_hash": parts[4],
    }


def cmd_tail(rows):
    db = sqlite3.connect(DB)
    digest, max_id, count = hash_tail(db, rows)
    db.close()
    print(f"rows: {count} (max_id {max_id})")
    print(f"hash: {digest}")


def cmd_snapshot(rows):
    db = sqlite3.connect(DB)
    digest, max_id, count = hash_tail(db, rows)
    db.close()
    prev = latest_log_record()
    prev_hash = prev["tail_hash"] if prev else "GENESIS"
    record = f"{int(time.time())}\t{max_id}\t{count}\t{digest}\t{prev_hash}\n"
    with open(LOG, "a") as f:
        f.write(record)
    print(f"snapshot appended:")
    print(f"  max_id:    {max_id}")
    print(f"  row_count: {count}")
    print(f"  tail_hash: {digest}")
    print(f"  prev_hash: {prev_hash}")
    print(f"  log:       {LOG}")


def cmd_head():
    rec = latest_log_record()
    if not rec:
        print("no snapshots yet")
        return
    age = int(time.time()) - rec["timestamp"]
    print(f"latest snapshot ({age // 60}m old):")
    for k, v in rec.items():
        print(f"  {k}: {v}")


def cmd_verify(limit):
    """Re-hash each logged record's tail range and confirm still matches.
    NOTE: this only catches rewrites within the last N rows range at snapshot
    time — if someone rewrote rows older than that range, it's invisible here.
    Real tamper evidence needs per-range hashing in v1."""
    if not LOG.exists():
        print("no log to verify")
        return 0
    lines = LOG.read_text().splitlines()
    if limit and limit > 0:
        lines = lines[-limit:]
    db = sqlite3.connect(DB)
    ok = 0
    bad = 0
    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) != 5:
            continue
        ts, max_id, row_count, claimed, prev_hash = parts
        max_id = int(max_id)
        row_count = int(row_count)
        cur = db.execute(
            "SELECT id, source, activity_type, content, created_at "
            "FROM activity_feed WHERE id <= ? ORDER BY id DESC LIMIT ?",
            (max_id, row_count),
        )
        h = hashlib.sha256()
        for row in cur:
            line_bytes = f"{row[0]}\x1f{row[1]}\x1f{row[2]}\x1f{row[3]}\x1f{row[4]}".encode("utf-8", errors="replace")
            h.update(line_bytes)
            h.update(b"\x1e")
        actual = h.hexdigest()
        if actual == claimed:
            ok += 1
        else:
            bad += 1
            print(f"MISMATCH at ts={ts} max_id={max_id}: {claimed} vs {actual}")
    db.close()
    print(f"verified {ok}/{ok+bad} snapshots")
    return 0 if bad == 0 else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["tail", "snapshot", "verify", "head"])
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    if args.cmd == "tail":
        cmd_tail(args.rows)
    elif args.cmd == "snapshot":
        cmd_snapshot(args.rows)
    elif args.cmd == "verify":
        return cmd_verify(args.limit)
    elif args.cmd == "head":
        cmd_head()
    return 0


if __name__ == "__main__":
    sys.exit(main())
