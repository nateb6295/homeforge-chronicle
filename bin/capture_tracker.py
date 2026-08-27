#!/usr/bin/env python3
"""Capture Tracker — prevents reprocessing of already-analyzed captures.

Tracks tweet IDs and URLs that have been processed (analyzed + posted to #operator).
Queries activity_feed for new unprocessed captures.
Supports tweets (x.com/twitter.com), Substack, arxiv, and generic URLs.

Usage:
    python3 capture_tracker.py pending          # show unprocessed captures
    python3 capture_tracker.py next [N]         # fetch next N unprocessed (default 3), with tweet content
    python3 capture_tracker.py post <id> [--author handle]  # read analysis from stdin, post+mark atomically
    python3 capture_tracker.py mark <tweet_id>  # mark a capture as processed (without posting)
    python3 capture_tracker.py batch_mark <id1> <id2> ...  # mark multiple as processed
    python3 capture_tracker.py status           # show tracking stats
    python3 capture_tracker.py check <tweet_id> # check if already processed
"""
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from urllib.parse import urlparse

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def ensure_table(db_path=DB):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS capture_processed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT,
            capture_text TEXT NOT NULL,
            source TEXT DEFAULT 'discord:capture',
            processed_at INTEGER NOT NULL,
            operator_post_id TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_capture_tweet_id
        ON capture_processed(tweet_id) WHERE tweet_id IS NOT NULL
    """)
    conn.commit()
    conn.close()


def ensure_open_table(db_path=DB):
    """Captures held OPEN — not processed, not dropped. A capture can end in
    'I don't know what to do with this yet' and stay live."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS capture_open (
            capture_id TEXT PRIMARY KEY,
            author TEXT,
            gist TEXT NOT NULL,
            why_open TEXT NOT NULL,
            resurface_when TEXT,
            opened_at INTEGER NOT NULL,
            closed_at INTEGER,
            closed_note TEXT
        )
    """)
    conn.commit()
    conn.close()


def hold_open(capture_id, gist, why_open, resurface_when=None, author=None, db_path=DB):
    ensure_open_table(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO capture_open "
        "(capture_id, author, gist, why_open, resurface_when, opened_at) "
        "VALUES (?,?,?,?,?,?)",
        (capture_id, author, gist, why_open, resurface_when, int(time.time()))
    )
    conn.commit()
    conn.close()


def get_open(db_path=DB, limit=20):
    ensure_open_table(db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT capture_id, author, gist, why_open, resurface_when, opened_at "
        "FROM capture_open WHERE closed_at IS NULL ORDER BY opened_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(zip(
        ("capture_id", "author", "gist", "why_open", "resurface_when", "opened_at"), r
    )) for r in rows]


def close_open(capture_id, note, db_path=DB):
    ensure_open_table(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "UPDATE capture_open SET closed_at=?, closed_note=? "
        "WHERE capture_id=? AND closed_at IS NULL",
        (int(time.time()), note, capture_id)
    )
    conn.commit()
    n = cur.rowcount
    conn.close()
    return n


def is_held(capture_id, db_path=DB):
    ensure_open_table(db_path)
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT 1 FROM capture_open WHERE capture_id=? AND closed_at IS NULL",
        (capture_id,)
    ).fetchone()
    conn.close()
    return row is not None


def is_processed(tweet_id, db_path=DB):
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT id FROM capture_processed WHERE tweet_id = ? LIMIT 1",
        (tweet_id,)
    ).fetchone()
    conn.close()
    return row is not None


def mark_processed(tweet_id, capture_text="", notes="", db_path=DB):
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO capture_processed "
            "(tweet_id, capture_text, processed_at, notes) VALUES (?, ?, ?, ?)",
            (tweet_id, capture_text[:200], int(time.time()), notes)
        )
        conn.commit()
    finally:
        conn.close()


def _extract_capture_id(content):
    """Extract a capture ID, author, and type from a Discord message.

    Returns (capture_id, author, type) or (None, None, None).
    Supports tweets, Substack, arxiv, and generic URLs.
    """
    # Tweet: x.com or twitter.com
    tweet_match = re.search(r'(?:x\.com|twitter\.com)/(\w+)/status/(\d+)', content)
    if tweet_match:
        return tweet_match.group(2), tweet_match.group(1), "tweet"

    # open.substack.com/pub/author/p/slug (check first — more specific)
    sub_match2 = re.search(r'open\.substack\.com/pub/([\w-]+)/p/([\w-]+)', content)
    if sub_match2:
        return f"sub:{sub_match2.group(1)}:{sub_match2.group(2)}", sub_match2.group(1), "substack"

    # author.substack.com/p/slug
    sub_match = re.search(r'([\w-]+)\.substack\.com/p/([\w-]+)', content)
    if sub_match:
        return f"sub:{sub_match.group(1)}:{sub_match.group(2)}", sub_match.group(1), "substack"

    # arxiv
    arxiv_match = re.search(r'arxiv\.org/(?:abs|pdf)/(\d+\.\d+)', content)
    if arxiv_match:
        return f"arxiv:{arxiv_match.group(1)}", "arxiv", "arxiv"

    # Generic URL fallback
    url_match = re.search(r'https?://\S+', content)
    if url_match:
        url = url_match.group(0).rstrip(')')
        parsed = urlparse(url)
        if parsed.netloc and parsed.netloc not in ('discord.com', 'cdn.discordapp.com'):
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            author = parsed.netloc.split('.')[0]
            return f"url:{url_hash}", author, "url"

    return None, None, None


def get_pending(hours=24, db_path=DB):
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    cutoff = int(time.time()) - (hours * 3600)

    captures = conn.execute("""
        SELECT content, created_at FROM activity_feed
        WHERE source = 'discord:capture'
        AND created_at > ?
        ORDER BY created_at DESC
    """, (cutoff,)).fetchall()

    pending = []
    for content, ts in captures:
        capture_id, author, cap_type = _extract_capture_id(content)
        if not capture_id:
            continue

        already = conn.execute(
            "SELECT id FROM capture_processed WHERE tweet_id = ? LIMIT 1",
            (capture_id,)
        ).fetchone()

        if already:
            continue
        if is_held(capture_id, db_path):
            continue
        if True:
            pending.append({
                "tweet_id": capture_id,
                "author": author,
                "content": content[:150],
                "age_hours": round((time.time() - ts) / 3600, 1),
                "type": cap_type,
            })

    conn.close()
    return pending


def fetch_tweet_content(tweet_id):
    """Fetch tweet text via tweet_fetch.py. Returns content string or None."""
    try:
        result = subprocess.run(
            ["python3", os.path.join(os.path.dirname(__file__), "tweet_fetch.py"), tweet_id],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, **_load_env()}
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def post_capture(tweet_id, analysis_text, author="", db_path=DB):
    """Post capture analysis to #operator and mark processed.

    Returns True if posted, False if already processed (skipped).
    This is the ONLY correct way to post capture analyses — it prevents
    reprocessing across context rotations.

    Captures go to #operator only. #threads is for intentional mesh engagement,
    not automatic capture cross-posting.
    """
    if is_processed(tweet_id, db_path):
        return False

    sys.path.insert(0, os.path.dirname(__file__))
    from discord_post import post

    prefix = f"**Capture: @{author}**" if author else "**Capture**"
    content = f"{prefix} (tweet:{tweet_id})\n\n{analysis_text}"
    result = post(content, channel="operator")

    if result.get("status") in (200, 204):
        mark_processed(tweet_id, capture_text=analysis_text[:200],
                       notes=f"auto-posted via post_capture", db_path=db_path)
        # Extract build ideas from the analysis
        try:
            from build_idea_extractor import extract_ideas, save_ideas
            ideas = extract_ideas(analysis_text, source=f"capture:@{author}:tweet:{tweet_id}")
            saved = save_ideas(ideas)
            if saved:
                print(f"  {saved} build idea(s) extracted and saved")
        except Exception:
            pass
        return True
    return False


def _load_env():
    """Load chronicle.env for API keys."""
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    extra = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    extra[k] = v.strip().strip('"').strip("'")
    return extra


def _lfm_score(content, tweet_id="", author=""):
    """Score content via LFM capture scorer. Returns formatted string or None."""
    scorer = os.path.join(os.path.dirname(__file__), "lfm_capture_score.py")
    if not os.path.exists(scorer):
        return None
    try:
        r = subprocess.run(
            ["python3", scorer, "--json"],
            input=content[:800], capture_output=True, text=True, timeout=90,
            env={**os.environ, **_load_env()}
        )
        if r.returncode == 0 and r.stdout.strip():
            data = json.loads(r.stdout.strip())
            d = int(data.get("density", 0))
            n = int(data.get("novelty", 0))
            tag = data.get("tag", "cool")
            note = data.get("note", "")
            return f"[{tag}] density={d} novelty={n} — {note}"
    except Exception:
        pass
    return None


def get_next(n=3, hours=48, db_path=DB):
    """Get next N unprocessed captures, prioritized by recency."""
    pending = get_pending(hours, db_path)
    return pending[:n]


def batch_mark(tweet_ids, db_path=DB):
    """Mark multiple tweet IDs as processed."""
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    try:
        for tid in tweet_ids:
            conn.execute(
                "INSERT OR IGNORE INTO capture_processed "
                "(tweet_id, capture_text, processed_at, notes) VALUES (?, ?, ?, ?)",
                (tid, "", int(time.time()), "batch_mark")
            )
        conn.commit()
    finally:
        conn.close()
    return len(tweet_ids)


def status(db_path=DB):
    ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    total = conn.execute("SELECT COUNT(*) FROM capture_processed").fetchone()[0]
    recent = conn.execute(
        "SELECT COUNT(*) FROM capture_processed WHERE processed_at > ?",
        (int(time.time()) - 86400,)
    ).fetchone()[0]
    oldest = conn.execute("SELECT MIN(processed_at) FROM capture_processed").fetchone()[0]
    conn.close()

    pending = get_pending(24, db_path)

    print(f"CAPTURE TRACKER STATUS")
    print(f"{'=' * 40}")
    print(f"Total processed: {total}")
    print(f"Last 24h: {recent}")
    if oldest:
        print(f"Tracking since: {time.strftime('%Y-%m-%d %H:%M', time.localtime(oldest))}")
    print(f"Pending (24h): {len(pending)}")
    if pending:
        print(f"\nUnprocessed captures:")
        for p in pending:
            cap_type = p.get('type', 'tweet')
            print(f"  @{p['author']} (-{p['age_hours']:.0f}h ago) {cap_type}:{p['tweet_id']}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "pending":
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
        pending = get_pending(hours)
        if not pending:
            print(f"No unprocessed captures in last {hours}h.")
        else:
            print(f"{len(pending)} unprocessed capture(s):")
            for p in pending:
                cap_type = p.get('type', 'tweet')
                print(f"  @{p['author']} (-{p['age_hours']:.0f}h ago) "
                      f"{cap_type}:{p['tweet_id']} | {p['content'][:80]}")

    elif cmd == "hold":
        # capture_tracker.py hold <id> --gist "..." --why "..." [--until "..."] [--author h]
        cid = sys.argv[2]
        def _arg(flag, default=None):
            return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default
        gist = _arg("--gist")
        why = _arg("--why")
        if not gist or not why:
            print("hold requires --gist and --why (why it is still open)")
            return
        hold_open(cid, gist, why, _arg("--until"), _arg("--author"))
        print(f"HELD OPEN: {cid}")
        print(f"  gist: {gist}")
        print(f"  open because: {why}")
        u = _arg("--until")
        if u:
            print(f"  resurface when: {u}")

    elif cmd == "open":
        rows = get_open()
        if not rows:
            print("No captures held open.")
        else:
            print(f"{len(rows)} capture(s) held open:")
            for r in rows:
                age = (time.time() - r["opened_at"]) / 86400
                who = f"@{r['author']} " if r["author"] else ""
                print(f"  {who}{r['capture_id']} ({age:.1f}d)")
                print(f"    {r['gist']}")
                print(f"    OPEN: {r['why_open']}")
                if r["resurface_when"]:
                    print(f"    WHEN: {r['resurface_when']}")

    elif cmd == "close":
        cid = sys.argv[2]
        note = sys.argv[3] if len(sys.argv) > 3 else "closed"
        n = close_open(cid, note)
        print(f"closed {n} open capture(s): {cid}" if n else f"{cid} was not open")

    elif cmd == "next":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        do_score = "--score" in sys.argv
        captures = get_next(n)
        if not captures:
            print("No unprocessed captures.")
        else:
            print(f"Next {len(captures)} unprocessed capture(s):\n")
            for i, p in enumerate(captures, 1):
                cap_type = p.get('type', 'tweet')
                print(f"--- [{i}] @{p['author']} (-{p['age_hours']:.0f}h ago) {cap_type}:{p['tweet_id']} ---")
                print(f"Source: {p['content']}")
                content = None
                if cap_type == "tweet":
                    content = fetch_tweet_content(p['tweet_id'])
                    if content:
                        print(f"\nTweet content:\n{content[:500]}")
                elif cap_type in ("substack", "url"):
                    print(f"\n(Use WebFetch to read content)")
                elif cap_type == "arxiv":
                    arxiv_id = p['tweet_id'].replace('arxiv:', '')
                    print(f"\narxiv paper: https://arxiv.org/abs/{arxiv_id}")
                if do_score and content:
                    temp = _lfm_score(content, p['tweet_id'], p.get('author', ''))
                    if temp:
                        print(f"\n🌡️ {temp}")
                print()

    elif cmd == "mark":
        if len(sys.argv) < 3:
            print("Usage: capture_tracker.py mark <tweet_id> [notes]")
            return
        tweet_id = sys.argv[2]
        notes = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
        mark_processed(tweet_id, notes=notes)
        print(f"Marked tweet {tweet_id} as processed.")

    elif cmd == "batch_mark":
        if len(sys.argv) < 3:
            print("Usage: capture_tracker.py batch_mark <id1> <id2> ...")
            return
        ids = sys.argv[2:]
        count = batch_mark(ids)
        print(f"Marked {count} tweet(s) as processed.")

    elif cmd == "post":
        if len(sys.argv) < 3:
            print("Usage: capture_tracker.py post <tweet_id> [--author handle]")
            print("  Reads analysis from stdin, atomically posts to #operator + marks processed.")
            return
        tweet_id = sys.argv[2]
        author = ""
        if "--author" in sys.argv:
            idx = sys.argv.index("--author")
            if idx + 1 < len(sys.argv):
                author = sys.argv[idx + 1]
        analysis = sys.stdin.read().strip()
        if not analysis:
            print("Error: no analysis on stdin")
            return
        ok = post_capture(tweet_id, analysis, author=author)
        if ok:
            print(f"Posted + marked tweet {tweet_id}")
        else:
            print(f"Skipped tweet {tweet_id} (already processed)")

    elif cmd == "check":
        if len(sys.argv) < 3:
            print("Usage: capture_tracker.py check <tweet_id>")
            return
        tweet_id = sys.argv[2]
        if is_processed(tweet_id):
            print(f"Tweet {tweet_id}: ALREADY PROCESSED")
        else:
            print(f"Tweet {tweet_id}: NOT YET PROCESSED")

    elif cmd == "score":
        if len(sys.argv) < 3:
            print("Usage: capture_tracker.py score <tweet_id>")
            return
        tweet_id = sys.argv[2]
        content = fetch_tweet_content(tweet_id)
        if not content:
            print(f"Could not fetch content for {tweet_id}")
            return
        temp = _lfm_score(content, tweet_id)
        if temp:
            print(temp)
        else:
            print("LFM scoring failed")

    elif cmd == "status":
        status()

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
