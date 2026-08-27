#!/usr/bin/env python3
"""Audit CONNECTIONS, not components.

Built 2026-08-25 from Nate's diagnosis: "loose connections, too many pieces
don't connect to each other."

Every failure found this week was two things that should agree and don't:
  - three parameters, each valid, never multiplied against each other
  - an accumulator reporting a gap from one clock and counts from another
  - an author field that disagrees with the content prefix in the same message
  - a prompt slot filled with text the prompt never frames
  - a threshold sitting below the floor value of its own score
  - a novelty term reading a schema field the writer stopped emitting

None are component faults. Each component is individually correct. A
"can this component report unknown" sweep found none of them, BY DESIGN —
it audits nodes, and these are all edges.

This audits edges. For every column in the database, who WRITES it and who
READS it, and do those sets make sense together?

  ORPHAN   : read by code, written by nobody   (the episodic_trace class)
  DEAD     : written by code, read by nobody   (work with no consumer)
  MIXED    : column holds more than one storage type (the created_at class)

Read-only. Reports; changes nothing.
"""
import glob, os, re, sqlite3, sys
from collections import defaultdict

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def columns_of_interest(db):
    """Columns in populated tables, excluding sqlite internals and FTS shadows."""
    out = {}
    for (t,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                           "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%'"):
        try:
            n = db.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        except Exception:
            continue
        if n == 0:
            continue
        cols = [r[1] for r in db.execute(f"PRAGMA table_info([{t}])")]
        out[t] = (n, cols)
    return out


def scan_code():
    """Crude but honest: which files mention which identifiers, and how."""
    writes, reads = defaultdict(set), defaultdict(set)
    for f in glob.glob("bin/*.py"):
        try:
            src = open(f, errors="ignore").read()
        except Exception:
            continue
        base = os.path.basename(f)
        for m in re.finditer(r"(INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE)\s+\[?(\w+)\]?", src, re.I):
            writes[m.group(2)].add(base)
        for m in re.finditer(r"FROM\s+\[?(\w+)\]?|JOIN\s+\[?(\w+)\]?", src, re.I):
            t = m.group(1) or m.group(2)
            if t:
                reads[t].add(base)
    return writes, reads


def mixed_types(db, table, col):
    try:
        rows = db.execute(f"SELECT typeof([{col}]) t, COUNT(*) FROM [{table}] "
                          f"GROUP BY t").fetchall()
    except Exception:
        return None
    real = [(t, n) for t, n in rows if t != "null"]
    return real if len(real) > 1 else None


def main():
    db = sqlite3.connect(DB)
    tables = columns_of_interest(db)
    writes, reads = scan_code()
    orphans, dead, mixed = [], [], []

    for t, (n, cols) in tables.items():
        w, r = writes.get(t, set()), reads.get(t, set())
        if r and not w:
            orphans.append((t, n, len(r)))
        if w and not r:
            dead.append((t, n, len(w)))
        for c in cols:
            m = mixed_types(db, t, c)
            if m:
                mixed.append((t, c, m))
    db.close()

    print(f"{len(tables)} populated tables audited (read-only)\n")
    print(f"MIXED STORAGE TYPES — one column, two kinds of value ({len(mixed)}):")
    for t, c, m in sorted(mixed, key=lambda x: -sum(n for _, n in x[2]))[:12]:
        detail = ", ".join(f"{ty}:{cnt:,}" for ty, cnt in m)
        minor = min(n for _, n in m)
        print(f"   {t}.{c:24} {detail}"
              + ("   <-- comparisons against this are unsound" if minor else ""))
    print(f"\nORPHAN TABLES — read by code, written by none ({len(orphans)}):")
    for t, n, r in sorted(orphans, key=lambda x: -x[1])[:8]:
        print(f"   {t:32} {n:>8,} rows, {r} readers, 0 writers")
    print(f"\nDEAD TABLES — written, never read ({len(dead)}):")
    for t, n, w in sorted(dead, key=lambda x: -x[1])[:8]:
        print(f"   {t:32} {n:>8,} rows, {w} writers, 0 readers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
