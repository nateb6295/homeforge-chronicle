#!/usr/bin/env python3
"""Chronicle Digest v3 — what Nate reads on his phone.

Thread #205 rewrite. Principles:
- Self-presenting AND self-explaining
- Filter noise aggressively (greetings, short captures, repeated content)
- Thread completions get real summaries, not just counts
- Family voices include why they flagged something
- Lead with what matters, end with scale

Run: python3 digest.py [--dry-run] [--hours N]
"""

import os, sys, json, sqlite3, time, re, subprocess
from datetime import datetime


def _load_chronicle_env():
    """Populate os.environ from chronicle.env. Bash `.` doesn't export to
    subprocesses; this loads the file directly so env-keyed config works
    when invoked via cron."""
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = val


_load_chronicle_env()

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")
DIGEST_WINDOW_HOURS = int(os.environ.get("DIGEST_WINDOW_HOURS", "12"))
MAX_DISCORD_CHARS = 1900

# Captures to skip — greetings, very short, emoji-only
CAPTURE_NOISE = re.compile(
    r'^(good\s*(night|morning|evening)|see you|gn |sleep well|'
    r'hey family|goodnight|^\s*$)',
    re.IGNORECASE,
)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_digest_log(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_start INTEGER NOT NULL,
            window_end INTEGER NOT NULL,
            items_produced INTEGER NOT NULL,
            items_surfaced INTEGER NOT NULL,
            item_ids TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()


def get_last_digest_time(conn):
    try:
        row = conn.execute(
            "SELECT window_end FROM digest_log ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    return int(time.time()) - (DIGEST_WINDOW_HOURS * 3600)


def trunc(text, limit=200):
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) <= limit:
        return text
    return text[:limit - 1] + "\u2026"


def clean_capture_text(content):
    """Extract meaningful text from a capture, stripping URLs and handles."""
    lines = content.strip().split('\n')
    text_parts = []
    for line in lines:
        line = line.strip()
        if line.startswith('http'):
            continue
        if line.startswith('[Image:') or line.startswith('[Video:'):
            continue
        # Strip @handle: prefix
        line = re.sub(r'^@\S+\s*:\s*', '', line)
        if line:
            text_parts.append(line)
    return ' '.join(text_parts).strip()


def is_noise_capture(text):
    """Return True if capture is greeting/noise."""
    if len(text) < 20:
        return True
    return bool(CAPTURE_NOISE.match(text))


def get_capture_analysis(conn, capture_content, since):
    """Find Ada's analysis of a capture."""
    url_match = re.search(r'https?://\S+', capture_content)
    if url_match:
        url = url_match.group(0)
        row = conn.execute(
            """SELECT substr(content,1,400) FROM scratch_pad
               WHERE category='gpt_analysis' AND content LIKE ? AND created_at > ?
               ORDER BY created_at DESC LIMIT 1""",
            (f'%{url[:40]}%', since),
        ).fetchone()
        if row:
            analysis = re.sub(r'\[GPT-OSS Capture Analysis\]\s*', '', row[0])
            analysis = re.sub(r'https?://\S+\s*', '', analysis)
            analysis = re.sub(r'@\w+[^:]*:.*?\n', '', analysis, count=1).strip()
            lines = [l.strip() for l in analysis.split('\n') if l.strip()]
            for line in lines:
                if len(line) > 40 and not line.startswith('@'):
                    return trunc(line, 160)
            if lines:
                return trunc(lines[0], 160)
    return None


def get_thread_section(conn, since):
    """Build thread summary — completions with real content + active thread."""
    parts = []

    # Completed threads in window — show what they found
    completed = conn.execute(
        """SELECT ct.id, ct.title, th.content
           FROM cognitive_threads ct
           JOIN thread_history th ON th.thread_id = ct.id
           WHERE th.event_type = 'complete' AND th.created_at > ?
           ORDER BY th.created_at DESC""",
        (since,),
    ).fetchall()
    if completed:
        parts.append(f"**{len(completed)} threads completed**")
        for t in completed[:2]:  # Top 2 with short summaries
            summary = trunc(t['content'], 80)
            parts.append(f"#{t['id']} {t['title']}: {summary}")

    # Active thread
    active = conn.execute(
        "SELECT id, title, question FROM cognitive_threads WHERE status='active' ORDER BY priority DESC, id DESC LIMIT 1"
    ).fetchone()
    if active:
        recent = conn.execute(
            """SELECT content FROM thread_history
               WHERE thread_id=? AND event_type='advanced'
               ORDER BY created_at DESC LIMIT 1""",
            (active['id'],),
        ).fetchone()
        finding = trunc(recent['content'], 80) if recent else active['question']
        parts.append(f"**Now: #{active['id']} {active['title']}** — {finding}")

    return '\n'.join(parts) if parts else None


def score_capture(text, raw_content):
    """Signal score for a capture. Higher = more substantive."""
    score = 0
    # Text length (clamped) — bare URLs and one-liners score low.
    score += min(len(text), 400)
    # Bare-arxiv-link or "arxiv: URL" patterns with almost no prose: penalize.
    stripped = re.sub(r'\s+', ' ', text).strip()
    if len(stripped) < 40:
        score -= 200
    if re.fullmatch(r'(arxiv\s*:?\s*)?https?://\S+\.?', stripped, flags=re.IGNORECASE):
        score -= 300
    # Having analysis attached is a positive signal.
    if 'arxiv.org' in raw_content or 'doi.org' in raw_content or 'nature.com' in raw_content:
        score += 50
    return score


def get_captures_section(conn, since):
    """Nate's captures — filtered, ranked by signal not recency."""
    captures = conn.execute(
        """SELECT id, content, created_at FROM activity_feed
           WHERE source='operator:capture' AND created_at > ?
           ORDER BY created_at DESC LIMIT 30""",
        (since,),
    ).fetchall()

    scored = []
    for r in captures:
        text = clean_capture_text(r['content'])
        if is_noise_capture(text):
            continue
        scored.append((score_capture(text, r['content']), r, text))
    scored.sort(key=lambda x: x[0], reverse=True)

    items = []
    item_ids = []
    for _score, r, text in scored:
        item_ids.append(r['id'])
        analysis = get_capture_analysis(conn, r['content'], since)
        if analysis and len(analysis) > 30:
            items.append(f"{trunc(text, 80)}\n  \u2192 {trunc(analysis, 100)}")
        else:
            items.append(trunc(text, 120))
        if len(items) >= 3:
            break

    total_captures = len(captures)
    shown = len(items)
    suffix = f"\n_{total_captures} captures total_" if total_captures > shown else ""

    return ('\n'.join(items) + suffix if items else None), item_ids


def get_family_section(conn, since):
    """Family voices — include why they flagged it."""
    voices = conn.execute(
        """SELECT id, agent, content FROM agent_voice
           WHERE voice_type='for_nate' AND created_at > ?
           ORDER BY created_at DESC LIMIT 6""",
        (since,),
    ).fetchall()

    items = []
    item_ids = []
    seen_topics = set()
    for v in voices:
        text = v['content']
        # Skip capture analyses that just repeat
        if re.search(r"Nate.s capture|Nate capture", text, re.IGNORECASE):
            continue

        # Extract topic (before the colon/reason)
        topic_match = re.match(r'^(.+?):\s*(Nate would care|This matters|This challenges|Because|it directly)', text)
        if topic_match:
            topic = topic_match.group(1).strip()
            # Get the reason — the interesting part
            reason = text[topic_match.end():]
            reason = re.sub(r'^.*?because\s+', '', reason, flags=re.IGNORECASE).strip()
            reason = trunc(reason, 100)
        else:
            topic = trunc(text, 80)
            reason = ""

        # Skip duplicates
        topic_key = topic[:30].lower()
        if topic_key in seen_topics:
            continue
        seen_topics.add(topic_key)

        who = v['agent'].capitalize()
        if reason:
            items.append(f"**{who}**: {trunc(topic, 60)} — {trunc(reason, 80)}")
        else:
            items.append(f"**{who}**: {trunc(topic, 100)}")
        item_ids.append(v['id'])

        if len(items) >= 3:
            break

    return ('\n'.join(items) if items else None), item_ids


def get_connection_section(conn, since):
    """Best crossref connection — pick highest similarity, not just recent."""
    crossrefs = conn.execute(
        """SELECT id, content FROM activity_feed
           WHERE source='crossref' AND activity_type='connection'
           AND created_at > ? ORDER BY created_at DESC LIMIT 5""",
        (since,),
    ).fetchall()

    if not crossrefs:
        return None, []

    # Parse similarity scores and pick highest
    best = None
    best_sim = 0
    for r in crossrefs:
        sim_match = re.search(r'sim=([0-9.]+)', r['content'])
        sim = float(sim_match.group(1)) if sim_match else 0
        if sim > best_sim:
            best_sim = sim
            best = r

    if not best:
        best = crossrefs[0]

    # Parse connection
    m_a = re.search(r'A:\s*(.+)', best['content'])
    m_b = re.search(r'B:\s*(.+)', best['content'])
    m_conn = re.search(r'Connection:\s*(.+)', best['content'], re.DOTALL)
    if m_a and m_b and m_conn:
        a = trunc(m_a.group(1).strip(), 50)
        b = trunc(m_b.group(1).strip(), 50)
        conn_text = re.sub(r'\*\*', '', m_conn.group(1)).strip()
        return f"**{a}** + **{b}**\n{trunc(conn_text, 140)}", [best['id']]

    return trunc(best['content'], 200), [best['id']]


def get_morning_note(conn):
    """Read the latest unresolved morning note from Opus. Returns (content, id)."""
    row = conn.execute(
        """SELECT id, content FROM scratch_pad
           WHERE category='morning_note' AND resolved=0
           ORDER BY created_at DESC LIMIT 1"""
    ).fetchone()
    if row:
        return row['content'], row['id']
    return None, None


def get_world_section(conn, since):
    """Top world brief — most substantive, not just longest."""
    briefs = conn.execute(
        """SELECT id, content FROM activity_feed
           WHERE source='intern' AND activity_type='brief'
           AND content NOT LIKE '%Searching for%'
           AND content NOT LIKE '%Query:%'
           AND created_at > ?
           ORDER BY created_at DESC LIMIT 20""",
        (since,),
    ).fetchall()

    scored = []
    for r in briefs:
        first_line = r['content'].split('\n')[0]
        first_line = re.sub(r'Source:.*$', '', first_line).strip()
        if len(first_line) > 60:
            scored.append((len(first_line), r['id'], first_line))

    scored.sort(reverse=True)
    if scored:
        return trunc(scored[0][2], 200), [scored[0][1]]
    return None, []


def format_digest(sections, total, since):
    now = int(time.time())
    hours = (now - since) / 3600

    parts = [f"**Chronicle Digest** \u2014 {hours:.0f}h window, {total:,} events"]

    # Morning note from Opus — the voice layer
    if sections.get('morning_note'):
        parts.append(f"\n{sections['morning_note']}")

    # Threads first — the intellectual work
    if sections.get('threads'):
        parts.append(f"\n**Threads**\n{sections['threads']}")

    # Captures — Nate's inputs and what the system did with them
    if sections.get('captures'):
        parts.append(f"\n**Your captures**\n{sections['captures']}")

    # Family — what Darby and Ada flagged
    if sections.get('family'):
        parts.append(f"\n**Family flagged**\n{sections['family']}")

    # Connection — the surprising link
    if sections.get('connection'):
        parts.append(f"\n**Connection**\n{sections['connection']}")

    # World — one substantive brief
    if sections.get('world'):
        parts.append(f"\n**World**\n{sections['world']}")

    text = '\n'.join(parts)
    if len(text) > MAX_DISCORD_CHARS:
        # Trim from bottom — world section is least important
        if sections.get('world') and len(text) > MAX_DISCORD_CHARS:
            parts = [p for p in parts if not p.startswith('\n**World**')]
            text = '\n'.join(parts)
        if len(text) > MAX_DISCORD_CHARS:
            text = text[:MAX_DISCORD_CHARS - 20] + "\n\u2026"
    return text


def post_discord(message, webhook_url):
    if not webhook_url:
        print("No webhook URL configured")
        return False
    payload = json.dumps({"content": message})
    try:
        r = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-X", "POST", "-H", "Content-Type: application/json",
             "-d", payload, webhook_url],
            capture_output=True, text=True, timeout=15,
        )
        code = r.stdout.strip()
        if code in ("200", "204"):
            return True
        print(f"Discord post failed: HTTP {code}")
        return False
    except Exception as e:
        print(f"Discord post failed: {e}")
        return False


def main():
    dry_run = "--dry-run" in sys.argv
    hours_override = None
    for i, arg in enumerate(sys.argv):
        if arg == "--hours" and i + 1 < len(sys.argv):
            hours_override = int(sys.argv[i + 1])

    conn = get_db()
    ensure_digest_log(conn)

    if hours_override:
        since = int(time.time()) - (hours_override * 3600)
    else:
        since = get_last_digest_time(conn)
    now = int(time.time())

    # Collect all sections
    sections = {}
    item_ids = []

    note_text, note_id = get_morning_note(conn)
    sections['morning_note'] = note_text
    sections['threads'] = get_thread_section(conn, since)

    cap_text, cap_ids = get_captures_section(conn, since)
    sections['captures'] = cap_text
    item_ids.extend(cap_ids)

    fam_text, fam_ids = get_family_section(conn, since)
    sections['family'] = fam_text
    item_ids.extend(fam_ids)

    conn_text, conn_ids = get_connection_section(conn, since)
    sections['connection'] = conn_text
    item_ids.extend(conn_ids)

    world_text, world_ids = get_world_section(conn, since)
    sections['world'] = world_text
    item_ids.extend(world_ids)

    total = conn.execute(
        "SELECT count(*) FROM activity_feed WHERE created_at > ?", (since,)
    ).fetchone()[0]

    if not any(sections.values()):
        print(f"No items since {datetime.fromtimestamp(since)}")
        conn.execute(
            "INSERT INTO digest_log (window_start, window_end, items_produced, items_surfaced, item_ids, created_at) VALUES (?,?,?,?,?,?)",
            (since, now, total, 0, "[]", now),
        )
        conn.commit()
        conn.close()
        return

    message = format_digest(sections, total, since)

    if dry_run:
        print(message)
        print(f"\n--- {len(item_ids)} items surfaced from {total} total ---")
    else:
        success = post_discord(message, OPUS_WEBHOOK)
        if success:
            print(f"Digest posted: {len(item_ids)} items from {total}")
            # Mark morning note as used only after successful post
            if note_id:
                conn.execute(
                    "UPDATE scratch_pad SET resolved=1 WHERE id=?", (note_id,)
                )

    conn.execute(
        "INSERT INTO digest_log (window_start, window_end, items_produced, items_surfaced, item_ids, created_at) VALUES (?,?,?,?,?,?)",
        (since, now, total, len(item_ids), json.dumps(item_ids), now),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
