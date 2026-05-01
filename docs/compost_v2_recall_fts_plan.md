# Compost v2 — recall-signal FTS5 rebuild plan

Drafted 2026-04-15 01:15 PDT. Morning work. Do NOT execute during
active pipeline writes — the CREATE VIRTUAL TABLE + initial populate
on activity_feed (126k rows) will hold a write lock long enough to
back up captures.

## Why this is needed

Current `capsule_survival.recall_signal` in `bin/capsule_survival.py`:
- Tokenizes restatement to top-5 content words.
- Builds `LIKE ?` AND-chain — ALL 5 tokens must appear in one row.
- Searches only `activity_feed`, despite docstring mentioning `thread_history`.

Result: recall_days ≈ 0 for most capsules. Binary near-zero signal.
Can't calibrate a compost threshold from noise-floor data.

## What FTS5 fixes

- **k-of-n** via MATCH: `token1 OR token2 OR token3 OR token4 OR token5`
  with BM25 scoring weights rare tokens more (naturally filters
  filler like "model", "system").
- **Two-source search** (activity_feed + thread_history), union
  distinct days.
- **~1000× faster** per query than LIKE %sub% on 126k rows, so the
  compute pass goes from ~40 min to seconds-per-100-capsules.

## Schema migration (morning — lock window ~1–3 min)

```sql
-- activity_feed FTS
CREATE VIRTUAL TABLE activity_feed_fts USING fts5(
    content,
    content='activity_feed',
    content_rowid='id',
    tokenize='porter unicode61 remove_diacritics 1'
);

INSERT INTO activity_feed_fts (rowid, content)
SELECT id, content FROM activity_feed;

CREATE TRIGGER activity_feed_fts_insert AFTER INSERT ON activity_feed BEGIN
    INSERT INTO activity_feed_fts(rowid, content) VALUES (new.id, new.content);
END;
CREATE TRIGGER activity_feed_fts_delete AFTER DELETE ON activity_feed BEGIN
    INSERT INTO activity_feed_fts(activity_feed_fts, rowid, content)
    VALUES ('delete', old.id, old.content);
END;
CREATE TRIGGER activity_feed_fts_update AFTER UPDATE ON activity_feed BEGIN
    INSERT INTO activity_feed_fts(activity_feed_fts, rowid, content)
    VALUES ('delete', old.id, old.content);
    INSERT INTO activity_feed_fts(rowid, content) VALUES (new.id, new.content);
END;

-- thread_history FTS (same pattern, smaller table)
CREATE VIRTUAL TABLE thread_history_fts USING fts5(
    content,
    content='thread_history',
    content_rowid='id',
    tokenize='porter unicode61 remove_diacritics 1'
);

INSERT INTO thread_history_fts (rowid, content)
SELECT id, content FROM thread_history;
-- triggers analogous to above
```

Do the activity_feed populate in a transaction during a pipeline
pause window (gemma heartbeat still safe, but stop chronicle-feeds
briefly).

## Rewritten recall_signal (sketch)

```python
def recall_signal(c, cap_id, tokens):
    if not tokens:
        return 0
    row = c.execute("SELECT created_at FROM knowledge_capsules WHERE id=?",
                    (cap_id,)).fetchone()
    if not row:
        return 0
    since = row["created_at"]
    match_expr = " OR ".join(tokens)  # OR instead of AND-LIKE

    q = """
        WITH hits AS (
            SELECT af.created_at, bm25(activity_feed_fts) AS rank
              FROM activity_feed_fts
              JOIN activity_feed af ON af.id = activity_feed_fts.rowid
             WHERE activity_feed_fts MATCH ?
               AND af.created_at > ?
            UNION ALL
            SELECT th.created_at, bm25(thread_history_fts) AS rank
              FROM thread_history_fts
              JOIN thread_history th ON th.id = thread_history_fts.rowid
             WHERE thread_history_fts MATCH ?
               AND th.created_at > ?
        )
        SELECT COUNT(DISTINCT date(created_at, 'unixepoch')) AS days,
               AVG(rank)                                      AS avg_rank
          FROM hits
         WHERE rank < -2.0       -- BM25 is negative; more-negative = stronger match
         LIMIT 60
    """
    r = c.execute(q, (match_expr, since, match_expr, since)).fetchone()
    if not r or r["days"] is None:
        return 0
    return int(r["days"])
```

Two knobs to tune:
- `rank < -2.0` threshold (filters weak matches)
- `LIMIT 60` cap (cost control on very common-token capsules)

## Calibration plan

1. Build FTS tables (morning, off-peak).
2. Recompute survival for the ~5400 capsules.
3. Expect distribution to shift — current neutral bucket (98.6%) should
   spread into solid/strong buckets as recall actually registers.
4. Label 40 capsules by hand: 20 I'd keep, 20 I'd compost.
   Target: compost-label capsules land in the -0.5 to -0.2 weak
   bucket with few false positives above -0.2.
5. Set dormancy threshold from calibrated distribution, not guess.

## Safety

- `visible_in_retrieval=0` (dormancy) is reversible. Never DELETE.
- Triggers keep FTS in sync with writes; verify on a 10-row write
  test before trusting auto-incremental capsules.
- Keep the old LIKE path behind a `USE_FTS=0` env flag for a week
  so I can A/B the distributions.

## Next handoff

This doc + the existing `docs/compost_v2_spec.md` are the morning
starting point. After FTS + recalibration, compost v2 is ready to
ship behind a dry-run flag.
