#!/usr/bin/env python3
"""Capsule survival filter — does the claim still hold?

The essay's thesis: grounding is selection pressure, not storage. This tool
is the survival loop — eligible capsules get re-evaluated and either SURVIVE
(confidence stands, survived_at timestamp refreshes) or get DEMOTED (confidence
drops, row carries a demotion reason).

Signals feeding the survival score:
  + contradictions: penalty for each capsule_contradictions row where this
    capsule is the loser (applied or unapplied).
  + recall: bonus proportional to log(1 + distinct-days the capsule
    appears cited in activity_feed / thread_history text search).
  + age: mild decay so untouched old claims have to earn their keep.
  + confidence: current score is prior; survival can only adjust by
    DEMOTION_STEP per pass (bounded change, matches capsule_review.py).

v0 scope: compute, rank, and show — no automatic mutation.
`apply` is explicit and per-id, so the human stays on the cut.

Usage:
    capsule_survival.py compute                 # compute scores, write survival table
    capsule_survival.py stats                   # distribution
    capsule_survival.py rank --limit 20         # lowest survival score first
    capsule_survival.py show ID                 # full diagnostic on one capsule
    capsule_survival.py demote ID [--reason T]  # apply the demotion step
    capsule_survival.py survive ID              # refresh survived_at, no conf change
"""

import argparse
import json
import math
import os
import sqlite3
import sys
import time

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DEMOTION_STEP = 0.08
MIN_AGE_SECONDS = 72 * 3600
ELIGIBLE_TYPES = ("claim", "prediction")


def conn():
    c = sqlite3.connect(DB, timeout=60.0)
    c.execute("PRAGMA busy_timeout = 60000")
    c.row_factory = sqlite3.Row
    return c


def ensure_schema(c):
    c.execute("""
        CREATE TABLE IF NOT EXISTS capsule_survival (
            capsule_id      INTEGER PRIMARY KEY REFERENCES knowledge_capsules(id),
            score           REAL    NOT NULL,
            components      TEXT    NOT NULL,
            computed_at     INTEGER NOT NULL,
            survived_at     INTEGER,
            demoted_at      INTEGER,
            demotion_reason TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_survival_score ON capsule_survival(score)")
    c.commit()


def capsule_tokens(restatement):
    """Return a small set of tokens used for recall-search. Strips stopwords and
    template prefixes so the search is on the actual claim content."""
    txt = (restatement or "").strip()
    # Drop PREDICTION #N / [BRACKET] prefixes
    import re
    txt = re.sub(r"^\s*(PREDICTION #\d+[^:]*:|\[[A-Z][A-Z0-9_\-:]+\])\s*", "", txt)
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9]{4,}", txt.lower())
    stop = {"prediction", "capsule", "chronicle", "would", "should", "could",
            "about", "because", "through", "between", "before", "after",
            "which", "their", "these", "those", "there", "where", "while"}
    uniq = []
    seen = set()
    for w in words:
        if w in stop or w in seen:
            continue
        seen.add(w)
        uniq.append(w)
        if len(uniq) >= 5:
            break
    return uniq


def recall_signal(c, cap_id, tokens):
    """Count distinct days the claim's key tokens appear in activity_feed
    or thread_history AFTER the capsule was created. Cheap proxy for
    recall utility until memory_access_log is restored."""
    if not tokens:
        return 0
    row = c.execute("SELECT created_at FROM knowledge_capsules WHERE id=?",
                    (cap_id,)).fetchone()
    if not row:
        return 0
    since = row["created_at"]
    # FTS5 instead of ANDed leading-wildcard LIKEs (2026-08-25).
    #
    # This used to be `content LIKE '%tok%' AND content LIKE '%tok%' ...` over
    # all 160,651 activity_feed rows, once per capsule — unindexable, ~0.35s
    # each, 23 minutes for a full pass, and every background run was killed
    # before finishing. activity_fts is built once in ~7s and answers the same
    # question directly, since FTS5 ANDs bare terms by default.
    #
    # NOT identical semantics, and the difference matters: FTS matches whole
    # tokens with porter stemming, LIKE matched substrings. "relay" no longer
    # matches "relayed" as a substring but does match via stemming; it will no
    # longer match inside "correlays". Recall counts may shift slightly against
    # the April scores — that is a method change, not drift, and any
    # before/after comparison across it is invalid.
    match = " ".join(f'"{t}"' for t in tokens if t.isalnum())
    if not match:
        return 0
    params = [match, since]
    q = """
        SELECT DISTINCT date(created_at, 'unixepoch') d
          FROM activity_fts
         WHERE activity_fts MATCH ?
           AND created_at > ?
         LIMIT 30
    """
    try:
        rows = c.execute(q, params).fetchall()
    except sqlite3.OperationalError:
        return 0
    return len(rows)


def contradiction_signal(c, cap_id):
    """Count contradictions where this capsule is the loser.

    REPAIRED 2026-08-25. This read a `capsule_contradictions` join table that
    no longer exists — supersession moved onto knowledge_capsules itself as
    superseded_at / superseded_by, and capsule_survival was never updated. It
    has raised OperationalError on every `compute` since, which is almost
    certainly why the last scores in this table are dated 2026-04-15.

    The mapping: losing a contradiction is now recorded as being superseded.

    MEASURED, not assumed (2026-08-25, and the first version of this comment
    was wrong):
      - The double weight is UNREACHABLE. cmd_compute's eligibility filter
        requires `superseded_by IS NULL`, which is the exact condition for
        weight 2. No scored capsule can ever return it. Kept as dead-but-
        documented rather than deleted, so the next reader sees the constraint
        instead of rediscovering it.
      - The single weight reaches 37 of 5,494 eligible capsules — 0.67%.
        11,481 capsules carry superseded_at without a named winner, but almost
        none are claim/prediction type above the confidence floor.
    So this signal is real but nearly inert, and the survival score is in
    practice age_decay + recall_bonus. Do not describe a rank list as
    contradiction-aware; it is age-and-recall-aware with a rare tiebreak.
    """
    row = c.execute("""
        SELECT superseded_at, superseded_by
        FROM knowledge_capsules WHERE id = ?
    """, (cap_id,)).fetchone()
    if not row or row["superseded_at"] is None:
        return 0
    return 2 if row["superseded_by"] is not None else 1


def compute_score(c, cap):
    """Combine signals into a survival score in [-1, +1]. Positive = survives."""
    now = int(time.time())
    age_days = max(1, (now - cap["created_at"]) / 86400)
    age_decay = min(0.5, 0.1 * math.log(age_days))

    tokens = capsule_tokens(cap["restatement"])
    recall_days = recall_signal(c, cap["id"], tokens)
    recall_bonus = min(0.6, 0.15 * math.log(1 + recall_days))

    contra = contradiction_signal(c, cap["id"])
    contra_penalty = min(1.0, 0.3 * contra)

    conf_prior = (cap["confidence_score"] - 0.5) * 0.4  # gentle prior

    score = conf_prior + recall_bonus - contra_penalty - age_decay
    score = max(-1.0, min(1.0, score))

    return score, {
        "age_days": round(age_days, 1),
        "age_decay": round(age_decay, 3),
        "recall_days": recall_days,
        "recall_bonus": round(recall_bonus, 3),
        "contradictions": contra,
        "contra_penalty": round(contra_penalty, 3),
        "conf_prior": round(conf_prior, 3),
        "tokens": tokens,
    }


def cmd_compute(args):
    c = conn()
    ensure_schema(c)
    now = int(time.time())
    cutoff = now - MIN_AGE_SECONDS

    type_placeholders = ",".join("?" * len(ELIGIBLE_TYPES))

    # RESUMABLE, added 2026-08-25 after three runs died partway.
    #
    # Cost is dominated by recall_signal(), which scans activity_feed
    # (160,634 rows) with several leading-wildcard LIKE clauses — unindexable —
    # once per capsule. 5,494 capsules against that is on the order of 10^9 row
    # comparisons, and the process was killed every time before finishing.
    #
    # A killed run used to leave the table holding fresh rows beside
    # four-month-old ones, which `stats` then averaged into a single
    # distribution that looked like a measurement. That is the worst possible
    # failure: not an error, a plausible number.
    #
    # So: skip capsules already scored in this era. Repeated bounded runs now
    # CONVERGE instead of churning, --stale-days sets the era, and `stats`
    # refuses to report while epochs are mixed.
    stale_before = now - int(getattr(args, "stale_days", 7) * 86400)
    rows = c.execute(f"""
        SELECT k.id, k.restatement, k.confidence_score, k.memory_type, k.created_at
          FROM knowledge_capsules k
          LEFT JOIN capsule_survival s ON s.capsule_id = k.id
         WHERE k.superseded_by IS NULL
           AND k.memory_type IN ({type_placeholders})
           AND k.created_at <= ?
           AND k.confidence_score >= 0.5
           AND (s.computed_at IS NULL OR s.computed_at < ?)
         ORDER BY k.id
    """, (*ELIGIBLE_TYPES, cutoff, stale_before)).fetchall()

    if not rows:
        print("all eligible capsules already scored in this era — nothing to do",
              file=sys.stderr)
        return
    cap_n = getattr(args, "max", 0)
    if cap_n and len(rows) > cap_n:
        print(f"{len(rows)} stale; taking {cap_n} this pass (run again to continue)",
              file=sys.stderr)
        rows = rows[:cap_n]

    print(f"Scoring {len(rows)} eligible capsules...", file=sys.stderr)
    t0 = time.time()

    # PHASE 1 — compute, no write transaction open.
    #
    # This used to interleave: INSERT, then compute_score (2-3 SELECTs), then
    # INSERT, committing every 500. Because the SELECTs ran on the same
    # connection, they sat INSIDE the write transaction, so the DB stayed
    # write-locked across 500 scoring passes. On a box where the sentinel,
    # ccs_touch, capsule_sync and discord-to-capsule all write the same file,
    # that loses the lock race — two runs on 2026-08-25 died with
    # "database is locked" at 1,000 and 1,500 rows, each leaving the table a
    # mix of fresh and four-month-old rows that `stats` reports as one number.
    # A partial recompute is worse than no recompute: it looks complete.
    scored = []
    for i, cap in enumerate(rows, 1):
        score, components = compute_score(c, cap)
        scored.append((cap["id"], score, json.dumps(components), now))
        if i % 1000 == 0:
            print(f"  computed {i}/{len(rows)}...", file=sys.stderr)

    # PHASE 2 — write in short bursts, retrying the lock instead of dying.
    written = 0
    for start in range(0, len(scored), 200):
        chunk = scored[start:start + 200]
        for attempt in range(6):
            try:
                c.executemany("""
                    INSERT INTO capsule_survival (capsule_id, score, components, computed_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(capsule_id) DO UPDATE SET
                      score=excluded.score,
                      components=excluded.components,
                      computed_at=excluded.computed_at
                """, chunk)
                c.commit()
                written += len(chunk)
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == 5:
                    raise
                time.sleep(2 * (attempt + 1))
        else:
            raise RuntimeError("could not acquire write lock")

    elapsed = time.time() - t0
    print(f"scored {written} capsules in {elapsed:.1f}s", file=sys.stderr)
    if written != len(rows):
        print(f"WARNING: wrote {written} of {len(rows)} — table now mixes "
              f"epochs, treat stats as invalid until a clean run", file=sys.stderr)


def cmd_stats(_args):
    c = conn()
    ensure_schema(c)

    # Refuse to average across epochs. A half-finished recompute leaves fresh
    # rows next to months-old ones, and every summary over that mixture is a
    # number with no referent. On 2026-08-25 I reported one such distribution
    # to Nate as a finding before noticing.
    epochs = c.execute("""
        SELECT date(computed_at,'unixepoch') d, COUNT(*) n
          FROM capsule_survival GROUP BY d ORDER BY d
    """).fetchall()
    if len(epochs) > 1:
        span = [(e["d"], e["n"]) for e in epochs]
        print("REFUSING TO REPORT — scores span multiple compute epochs:")
        for d, n in span:
            print(f"    {d}   {n:,} rows")
        print("Any distribution over this mixture is meaningless. Finish the")
        print("recompute (`compute`, repeatedly — it resumes) and try again.")
        return 1

    total = c.execute("SELECT COUNT(*) n FROM capsule_survival").fetchone()["n"]
    if total == 0:
        print("no survival scores computed yet — run `compute` first")
        return

    buckets = [
        ("-1.0..-0.5  (critical)", -1.0, -0.5),
        ("-0.5..-0.2  (weak)",    -0.5, -0.2),
        ("-0.2..+0.2  (neutral)", -0.2,  0.2),
        ("+0.2..+0.5  (solid)",    0.2,  0.5),
        ("+0.5..+1.0  (strong)",   0.5,  1.001),
    ]
    print(f"{'bucket':<28} {'count':>7} {'pct':>6}")
    print("-" * 44)
    for label, lo, hi in buckets:
        n = c.execute("SELECT COUNT(*) n FROM capsule_survival WHERE score >= ? AND score < ?",
                      (lo, hi)).fetchone()["n"]
        pct = 100.0 * n / total
        print(f"{label:<28} {n:>7} {pct:>5.1f}%")
    print("-" * 44)
    print(f"{'total':<28} {total:>7}")

    demoted = c.execute("SELECT COUNT(*) n FROM capsule_survival WHERE demoted_at IS NOT NULL").fetchone()["n"]
    survived = c.execute("SELECT COUNT(*) n FROM capsule_survival WHERE survived_at IS NOT NULL").fetchone()["n"]
    print(f"\nhuman actions: survived={survived} demoted={demoted}")


def cmd_rank(args):
    c = conn()
    ensure_schema(c)
    rows = c.execute("""
        SELECT s.capsule_id, s.score, s.components, k.restatement, k.confidence_score,
               k.memory_type, datetime(k.created_at,'unixepoch','localtime') created_at_str
          FROM capsule_survival s
          JOIN knowledge_capsules k ON k.id = s.capsule_id
         WHERE s.survived_at IS NULL AND s.demoted_at IS NULL
         ORDER BY s.score ASC
         LIMIT ?
    """, (args.limit,)).fetchall()
    for r in rows:
        comp = json.loads(r["components"])
        print(f"[{r['capsule_id']:>5}] score={r['score']:+.3f} conf={r['confidence_score']:.2f} "
              f"type={r['memory_type']} age={comp['age_days']}d "
              f"recall={comp['recall_days']}d contra={comp['contradictions']}")
        first_line = r["restatement"].split("\n")[0][:140]
        print(f"         {first_line}")


def cmd_show(args):
    c = conn()
    ensure_schema(c)
    row = c.execute("""
        SELECT s.*, k.restatement, k.confidence_score, k.memory_type,
               datetime(k.created_at,'unixepoch','localtime') created_at_str
          FROM capsule_survival s
          JOIN knowledge_capsules k ON k.id = s.capsule_id
         WHERE s.capsule_id = ?
    """, (args.id,)).fetchone()
    if not row:
        print(f"no survival row for capsule {args.id} — run `compute` first")
        return
    comp = json.loads(row["components"])
    print(f"capsule {args.id}")
    print(f"  type       : {row['memory_type']}")
    print(f"  created    : {row['created_at_str']}")
    print(f"  confidence : {row['confidence_score']:.3f}")
    print(f"  score      : {row['score']:+.3f}")
    print(f"  components : {json.dumps(comp, indent=2)}")
    if row["survived_at"]:
        print(f"  SURVIVED @ {time.strftime('%Y-%m-%d %H:%M', time.localtime(row['survived_at']))}")
    if row["demoted_at"]:
        print(f"  DEMOTED @ {time.strftime('%Y-%m-%d %H:%M', time.localtime(row['demoted_at']))} — {row['demotion_reason']}")
    print(f"\n  restatement:")
    for line in row["restatement"].splitlines()[:10]:
        print(f"    {line}")


def cmd_survive(args):
    c = conn()
    ensure_schema(c)
    now = int(time.time())
    c.execute("UPDATE capsule_survival SET survived_at=?, demoted_at=NULL, demotion_reason=NULL "
              "WHERE capsule_id=?", (now, args.id))
    c.commit()
    print(f"capsule {args.id} marked SURVIVED (no confidence change)")


def cmd_demote(args):
    c = conn()
    ensure_schema(c)
    now = int(time.time())
    cap = c.execute("SELECT confidence_score FROM knowledge_capsules WHERE id=?",
                    (args.id,)).fetchone()
    if not cap:
        print(f"capsule {args.id} not found")
        return
    new_conf = max(0.0, cap["confidence_score"] - DEMOTION_STEP)
    c.execute("UPDATE knowledge_capsules SET confidence_score=? WHERE id=?",
              (new_conf, args.id))
    c.execute("UPDATE capsule_survival SET demoted_at=?, demotion_reason=?, survived_at=NULL "
              "WHERE capsule_id=?", (now, args.reason, args.id))
    c.commit()
    print(f"capsule {args.id} DEMOTED: conf {cap['confidence_score']:.3f} -> {new_conf:.3f}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    comp = sub.add_parser("compute", help="compute/refresh survival scores (resumable)")
    comp.add_argument("--stale-days", type=float, default=7,
                      help="rescore anything older than this many days (default 7)")
    comp.add_argument("--max", type=int, default=0,
                      help="stop after N capsules; run again to continue")
    sub.add_parser("stats", help="score distribution")

    rank = sub.add_parser("rank", help="lowest-scoring capsules first")
    rank.add_argument("--limit", type=int, default=20)

    show = sub.add_parser("show")
    show.add_argument("id", type=int)

    surv = sub.add_parser("survive")
    surv.add_argument("id", type=int)

    demote = sub.add_parser("demote")
    demote.add_argument("id", type=int)
    demote.add_argument("--reason", default="opus:survival_review")

    args = ap.parse_args()
    {"compute": cmd_compute, "stats": cmd_stats, "rank": cmd_rank,
     "show": cmd_show, "survive": cmd_survive, "demote": cmd_demote}[args.cmd](args)


if __name__ == "__main__":
    main()
