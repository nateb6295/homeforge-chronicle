#!/usr/bin/env python3
"""Snapshot WHO is writing capsules, by monotonic id — for the cadence test.

Built 2026-08-24 23:40 because I had written "check per-interval capsule
composition" into tomorrow's queue and built nothing capable of answering it.
An instruction you cannot follow is not an instruction.

Why it matters: ccs_adaptive gates compression on `capsules >= 30`. LoQwen's
10-minute pulse alone writes ~29 capsules per 3h. So the gate may be measuring
her heartbeat rather than work, and the resulting cadence variation would look
exactly like adaptive sensing. Distinguishing those needs composition per
interval, not just the interval length.

Uses id watermarks, never timestamps: knowledge_capsules.created_at is INTEGER
in 77,358 rows and TEXT in 85, and `timestamp` is NULL in 2,256 with two
incompatible formats. Ids cannot have a format.
"""
import json, os, sqlite3, sys, time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                   "data", "capsule_composition.jsonl")

# Anything matching these is an AUTOMATED heartbeat, not work.
AUTOMATED = ("loquwen", "vitals", "sentinel", "hal", "sync", "feed")


def snapshot():
    db = sqlite3.connect(DB, timeout=10)
    hi = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()[0] or 0
    prev = None
    if os.path.exists(LOG):
        for line in open(LOG):
            if line.strip():
                prev = json.loads(line)
    lo = prev["max_id"] if prev else hi
    rows = db.execute(
        "SELECT COALESCE(topic,'(none)'), COUNT(*) FROM knowledge_capsules "
        "WHERE id > ? AND id <= ? GROUP BY 1", (lo, hi)).fetchall()
    ver = db.execute("SELECT version FROM cognitive_state WHERE id=1").fetchone()
    last_comp = db.execute(
        "SELECT MAX(created_at) FROM cognitive_state_history").fetchone()[0] or 0
    db.close()

    auto = sum(n for t, n in rows if any(a in t.lower() for a in AUTOMATED))
    total = sum(n for _, n in rows)
    rec = {
        "ts": int(time.time()),
        "human_ts": time.strftime("%Y-%m-%d %H:%M"),
        "max_id": hi,
        "since_id": lo,
        "total": total,
        "automated": auto,
        "work": total - auto,
        "ccs_version": ver[0] if ver else None,
        "mins_since_compression": round((time.time() - last_comp) / 60, 1),
        "by_topic": dict(sorted(rows, key=lambda r: -r[1])[:8]),
    }
    with open(LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return rec


if __name__ == "__main__":
    r = snapshot()
    if "--json" in sys.argv:
        print(json.dumps(r, indent=2))
    else:
        print(f"[{r['human_ts']}] ids {r['since_id']}->{r['max_id']}  "
              f"total={r['total']}  automated={r['automated']}  WORK={r['work']}")
        print(f"  {r['mins_since_compression']:.0f} min since compression, "
              f"CCS v{r['ccs_version']}")
        if r["total"]:
            frac = r["automated"] / r["total"]
            print(f"  automated share: {frac:.0%}"
                  + ("   <-- the gate is measuring a heartbeat" if frac > 0.7 else ""))
        for t, n in r["by_topic"].items():
            print(f"    {n:>4}  {t}")
