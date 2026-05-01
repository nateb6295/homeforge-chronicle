#!/usr/bin/env python3
"""Fast semantic deduplication for knowledge capsules via SemHash.

Build #25: Replaces O(n²) pairwise embedding comparison with O(n log n)
ANN-based clustering using Model2Vec + Vicinity.

Usage:
    capsule_dedup.py scan [--topic TOPIC] [--threshold 0.85] [--include-logs]
    capsule_dedup.py apply [--topic TOPIC] [--threshold 0.85] [--include-logs]
    capsule_dedup.py stats

By default, bracket-prefix log-style records (e.g. [WALLET-TX], [FEED], [HA])
are EXCLUDED — template dominates the embedding and distinct events get flagged
as duplicates. Pass --include-logs only if you're sure.
"""
import os
import re
import sqlite3
import sys
import time

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DEFAULT_THRESHOLD = 0.85

LOG_PREFIX_RE = re.compile(r"^\s*\[[A-Z][A-Z0-9_\-]{1,30}\]")
HEARTBEAT_RE = re.compile(r"^(Sentinel|HAL|Gemma|Pulse):\s", re.IGNORECASE)
UNICODE_ESC_RE = re.compile(r"\\u[0-9a-fA-F]{4}")


def is_log_style(text):
    if not text:
        return True
    if LOG_PREFIX_RE.match(text):
        return True
    if HEARTBEAT_RE.match(text):
        return True
    escapes = len(UNICODE_ESC_RE.findall(text[:500]))
    if escapes >= 5:
        return True
    return False


def load_capsules(topic=None, include_logs=False):
    """Load active capsules, optionally filtered by topic."""
    db = sqlite3.connect(DB_PATH)
    if topic:
        rows = db.execute(
            "SELECT id, restatement FROM knowledge_capsules "
            "WHERE topic = ? AND superseded_at IS NULL AND consolidated_into IS NULL "
            "ORDER BY id ASC",
            (topic,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, restatement FROM knowledge_capsules "
            "WHERE superseded_at IS NULL AND consolidated_into IS NULL "
            "ORDER BY id ASC"
        ).fetchall()
    db.close()
    if not include_logs:
        rows = [r for r in rows if not is_log_style(r[1])]
    return rows


def find_duplicates(rows, threshold=DEFAULT_THRESHOLD):
    """Use SemHash to find duplicate capsules. Returns list of (dup_id, keep_id, sim)."""
    from semhash import SemHash

    ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]
    records = [{"text": t} for t in texts]
    text_to_id = {t: i for i, t in zip(ids, texts)}

    semhash = SemHash.from_records(records, columns=["text"])
    result = semhash.self_deduplicate(threshold=threshold)

    pairs = []
    for dup_rec in result.filtered:
        dup_text = dup_rec.record.get("text", "")
        dup_id = text_to_id.get(dup_text)
        if not dup_id or not dup_rec.duplicates:
            continue
        best_text, best_score = dup_rec.duplicates[0]
        best_str = best_text.get("text", "") if isinstance(best_text, dict) else str(best_text)
        best_id = text_to_id.get(best_str)
        if best_id and best_id != dup_id:
            pairs.append((dup_id, best_id, best_score))

    return pairs, len(result.selected), len(result.filtered)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    # Parse args
    topic = None
    threshold = DEFAULT_THRESHOLD
    include_logs = False
    for i, arg in enumerate(sys.argv[2:], 2):
        if arg == "--topic" and i + 1 < len(sys.argv):
            topic = sys.argv[i + 1]
        if arg == "--threshold" and i + 1 < len(sys.argv):
            threshold = float(sys.argv[i + 1])
        if arg == "--include-logs":
            include_logs = True

    if cmd == "scan":
        rows = load_capsules(topic, include_logs=include_logs)
        if len(rows) < 2:
            print(f"Only {len(rows)} capsules — nothing to deduplicate.")
            return

        print(f"Scanning {len(rows)} capsules{f' (topic: {topic})' if topic else ''}...")
        t0 = time.time()
        pairs, kept, filtered = find_duplicates(rows, threshold)
        elapsed = time.time() - t0

        print(f"Completed in {elapsed:.1f}s")
        print(f"Kept: {kept}, Duplicates: {filtered}")
        print(f"Reduction: {len(rows)} → {kept} ({filtered} redundant, {filtered/len(rows)*100:.1f}%)")
        if pairs:
            print(f"\nTop duplicates:")
            for dup_id, keep_id, sim in pairs[:15]:
                print(f"  [{dup_id}] → [{keep_id}] (sim={sim:.3f})")

    elif cmd == "apply":
        rows = load_capsules(topic, include_logs=include_logs)
        if len(rows) < 2:
            print(f"Only {len(rows)} capsules — nothing to deduplicate.")
            return

        print(f"Deduplicating {len(rows)} capsules{f' (topic: {topic})' if topic else ''}...")
        t0 = time.time()
        pairs, kept, filtered = find_duplicates(rows, threshold)
        elapsed = time.time() - t0
        print(f"Found {filtered} duplicates in {elapsed:.1f}s")

        if not pairs:
            print("No duplicates to supersede.")
            return

        db = sqlite3.connect(DB_PATH)
        now = int(time.time())
        applied = 0
        for dup_id, keep_id, sim in pairs:
            db.execute(
                "UPDATE knowledge_capsules SET superseded_at=?, superseded_by=? "
                "WHERE id=? AND superseded_at IS NULL",
                (now, keep_id, dup_id),
            )
            applied += 1
        db.commit()
        db.close()
        print(f"Superseded {applied} capsules")

    elif cmd == "stats":
        db = sqlite3.connect(DB_PATH)
        total = db.execute("SELECT count(*) FROM knowledge_capsules").fetchone()[0]
        active = db.execute(
            "SELECT count(*) FROM knowledge_capsules "
            "WHERE superseded_at IS NULL AND consolidated_into IS NULL"
        ).fetchone()[0]
        superseded = db.execute(
            "SELECT count(*) FROM knowledge_capsules WHERE superseded_at IS NOT NULL"
        ).fetchone()[0]
        consolidated = db.execute(
            "SELECT count(*) FROM knowledge_capsules WHERE consolidated_into IS NOT NULL"
        ).fetchone()[0]
        db.close()
        print(f"Capsule stats:")
        print(f"  Total:        {total}")
        print(f"  Active:       {active}")
        print(f"  Superseded:   {superseded}")
        print(f"  Consolidated: {consolidated}")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
