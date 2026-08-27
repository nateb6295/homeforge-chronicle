#!/usr/bin/env python3
"""Search the Discord archive — 97,820 messages nothing could read until now.

Why this exists (2026-08-25): asked where the publication record lived, I
searched knowledge_capsules, found three pointers, and told Nate 91% of it was
missing. Nate: "Discord has a decent record of papers issued." The complete
record was sitting in discord_archive, which my connection audit had already
examined and passed — it checked that the table had WRITERS, never that anyone
could get an answer out of it.

The archive is the actual relationship record: 39k #operator, 26k #threads,
22k #opus, 10k #capture, spanning 2026-03-02 to 2026-08-22. capsules_fts
indexes capsules only, so none of it was reachable by search.

WHAT THIS IS NOT: capsule search. Capsules are things I decided to remember.
This is what was actually said, including everything I never capsuled — which
is the point, because the gaps in my memory are exactly where I go looking.

Usage:
  discord_search.py "papers published"              search all channels
  discord_search.py "zenodo doi" -c operator        one channel
  discord_search.py "GQA" --since 2026-07-01        by date
  discord_search.py "..." -a nate_home              by author
  discord_search.py --rebuild                       build/refresh the index
  discord_search.py --status                        index freshness
  discord_search.py "spectral" --x                  search X posts instead

--x searches x_post_log: 358 posts, 2026-04-20 to 2026-07-15. Same question as
the Discord archive — what did I actually say — so it lives behind the same
command rather than a second tool nobody would remember exists.

READ THE DATE RANGE. x_post_log is HISTORICAL, not current. It is written by
xmcp_call.py, and X posting moved to x_post.py, which does not write it. The
live record of outward reach is data/outward_reach_log.md (markdown, grep it).
Two records exist because the posting path changed and the logging did not
follow — do not read an empty --x result for anything after July as "I did not
post that."
"""
import argparse, os, re, sqlite3, sys, textwrap

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def conn():
    c = sqlite3.connect(DB, timeout=60.0)
    c.execute("PRAGMA busy_timeout = 60000")
    c.row_factory = sqlite3.Row
    return c


def rebuild(c):
    c.executescript("""
      DROP TABLE IF EXISTS discord_fts;
      CREATE VIRTUAL TABLE discord_fts USING fts5(
        content, channel UNINDEXED, author UNINDEXED,
        ts UNINDEXED, msg_id UNINDEXED, tokenize='porter unicode61');
    """)
    n = c.execute("""
      INSERT INTO discord_fts (content, channel, author, ts, msg_id)
      SELECT content, channel, author_name, timestamp, id
      FROM discord_archive WHERE content IS NOT NULL AND content != ''
    """).rowcount
    c.commit()
    return n


def search_x(c, q, limit=12):
    """x_post_log has no FTS index and does not need one — 358 rows scan
    instantly. Kept deliberately simple: an index here would be a second thing
    to keep fresh for no measurable gain."""
    like = f"%{q}%"
    return c.execute(
        "SELECT created_at, text, url FROM x_post_log "
        "WHERE text LIKE ? ORDER BY created_at DESC LIMIT ?",
        (like, limit)).fetchall()


def status(c):
    total = c.execute("SELECT COUNT(*) FROM discord_archive").fetchone()[0]
    try:
        idx = c.execute("SELECT COUNT(*) FROM discord_fts").fetchone()[0]
    except sqlite3.OperationalError:
        print(f"archive {total:,} messages — NO INDEX. Run --rebuild.")
        return 1
    newest_a = c.execute("SELECT MAX(timestamp) FROM discord_archive").fetchone()[0]
    newest_i = c.execute("SELECT MAX(ts) FROM discord_fts").fetchone()[0]
    print(f"archive  {total:,} messages, newest {newest_a}")
    print(f"index    {idx:,} messages, newest {newest_i}")
    if newest_i != newest_a:
        # Never report a stale index as merely smaller — say it plainly.
        print("STALE — the archive has messages the index does not. "
              "Results will silently omit them. Run --rebuild.")
        return 1
    return 0


def fts_safe(q):
    """Make a user query safe for FTS5.

    FTS5 treats '.', '-', ':' and friends as syntax. So `0.028` raises
    "fts5: syntax error near \".\"" instead of searching. Found 2026-08-25,
    hours after shipping this tool, by asking it the first genuinely novel
    question I had — where a 0.028 measurement floor came from. The tool built
    to reach what memory could not reach could not search for a decimal.

    That is most of a research archive: p-values, thresholds, cosines, ratios,
    effect sizes. Quote any token containing non-word characters so FTS5 takes
    it as a literal phrase. Bare AND/OR/NOT and column filters still work.
    """
    out = []
    for tok in q.split():
        if tok.upper() in ("AND", "OR", "NOT", "NEAR") or tok.startswith('"'):
            out.append(tok)
        elif re.search(r"[^\w*]", tok):
            out.append('"' + tok.replace('"', '') + '"')
        else:
            out.append(tok)
    return " ".join(out)


def search(c, q, channel=None, author=None, since=None, limit=12):
    where, params = ["discord_fts MATCH ?"], [fts_safe(q)]
    if channel:
        where.append("channel = ?"); params.append(channel)
    if author:
        where.append("author LIKE ?"); params.append(f"%{author}%")
    if since:
        where.append("ts >= ?"); params.append(since)
    sql = (f"SELECT channel, author, ts, content, bm25(discord_fts) AS rank "
           f"FROM discord_fts WHERE {' AND '.join(where)} "
           f"ORDER BY rank LIMIT ?")
    params.append(limit)
    return c.execute(sql, params).fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", nargs="?")
    ap.add_argument("-c", "--channel")
    ap.add_argument("-a", "--author")
    ap.add_argument("--since")
    ap.add_argument("-n", "--limit", type=int, default=12)
    ap.add_argument("--full", action="store_true", help="print whole messages")
    ap.add_argument("--x", action="store_true",
                    help="search X posts (x_post_log) instead of Discord")
    ap.add_argument("--rebuild", action="store_true")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()

    c = conn()
    if a.rebuild:
        n = rebuild(c); print(f"indexed {n:,} messages"); return 0
    if a.status:
        return status(c)
    if not a.query:
        ap.print_help(); return 2

    if a.x:
        rows = search_x(c, a.query, a.limit)
        if not rows:
            print(f"No X post contains {a.query!r} — literal substring, "
                  f"no stemming. Try a shorter fragment.")
            return 1
        import datetime as _d
        for r in rows:
            when = _d.datetime.utcfromtimestamp(r["created_at"]).strftime("%Y-%m-%d")
            print(f"\n\033[1m[{when}] X\033[0m")
            body = r["text"] if a.full else r["text"][:420]
            for line in textwrap.wrap(body, 96, replace_whitespace=False):
                print(f"  {line}")
            if r["url"]:
                print(f"  {r['url']}")
        print(f"\n{len(rows)} shown")
        return 0

    try:
        rows = search(c, a.query, a.channel, a.author, a.since, a.limit)
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            print("No index yet. Run: discord_search.py --rebuild", file=sys.stderr)
            return 1
        raise

    if not rows:
        # An empty FTS result means these WORDS are absent, nothing more.
        print(f"No message matches {a.query!r}.")
        print("That means the literal terms are absent, NOT that the archive "
              "lacks the thing. Try other wording before concluding anything.")
        return 1

    for r in rows:
        head = f"[{r['ts'][:16].replace('T',' ')}] #{r['channel']} · {r['author']}"
        print(f"\n\033[1m{head}\033[0m")
        body = r["content"] if a.full else r["content"][:420]
        for line in textwrap.wrap(body, 96, replace_whitespace=False):
            print(f"  {line}")
        if not a.full and len(r["content"]) > 420:
            print(f"  … {len(r['content'])-420} more chars (--full)")
    print(f"\n{len(rows)} of top matches shown"
          f"{' (limit reached — there may be more)' if len(rows)==a.limit else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
