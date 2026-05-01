#!/usr/bin/env python3
"""Morning Brief — personal intelligence digest for Nate.

Build #166: Point the pipeline outward. Same machinery, curated for a human.

This is NOT an LLM synthesis — it's structured data pulled straight from the DB.
No fabrication risk because no generation step. Just facts and highlights.

Usage:
    python3 morning_brief.py              # Generate and post to Discord
    python3 morning_brief.py --preview    # Show without posting
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")


def _db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def get_xrp_status():
    """Current XRP price, 24h change, portfolio value."""
    db = _db()
    # Latest sentinel cycle has price data
    row = db.execute(
        "SELECT content FROM activity_feed "
        "WHERE source='sentinel' AND activity_type='monitor_cycle' "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        return None
    try:
        data = json.loads(row["content"])
        price = data.get("price", 0)
        portfolio = data.get("portfolio_usd", 0)
        xrp_bal = data.get("xrp_balance", 0)
        return {"price": price, "portfolio": portfolio, "xrp_balance": xrp_bal}
    except (json.JSONDecodeError, TypeError):
        return None


def get_nate_captures(hours=24, limit=10):
    """Briefs generated from Nate's captures."""
    db = _db()
    since = int(time.time()) - (hours * 3600)
    rows = db.execute(
        "SELECT substr(content, 1, 200) as content, created_at FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' "
        "AND title LIKE '%Nate capture%' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_for_nate_voices(hours=24, limit=5):
    """Ada and Darby's for_nate messages — what they think Nate should see."""
    db = _db()
    since = int(time.time()) - (hours * 3600)
    # Get substantive for_nate voices with enough context to be useful
    rows = db.execute(
        "SELECT agent, substr(content, 1, 400) as content FROM agent_voice "
        "WHERE voice_type='for_nate' AND created_at > ? "
        "AND length(content) > 100 "
        "AND content NOT LIKE 'Connection %' "
        "AND content NOT LIKE 'Prediction review%' "
        "AND content NOT LIKE 'Nate capture http%' "
        "AND content NOT LIKE 'Nate.%' "
        "ORDER BY created_at DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_predictions_status():
    """Active and recently scored predictions."""
    db = _db()
    active = db.execute(
        "SELECT id, substr(claim, 1, 100) as pred, confidence as probability "
        "FROM prediction_track WHERE status='open' ORDER BY id"
    ).fetchall()
    scored = db.execute(
        "SELECT id, substr(claim, 1, 100) as pred, outcome "
        "FROM prediction_track WHERE status='scored' "
        "ORDER BY scored_at DESC LIMIT 3"
    ).fetchall()
    db.close()
    return [dict(r) for r in active], [dict(r) for r in scored]


def get_thread_status():
    """Active thread title and last advance."""
    db = _db()
    thread = db.execute(
        "SELECT id, title FROM cognitive_threads WHERE status='active' "
        "ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if not thread:
        db.close()
        return None
    last_advance = db.execute(
        "SELECT substr(content, 1, 150) as content, created_at "
        "FROM thread_history WHERE thread_id=? AND event_type='advanced' "
        "ORDER BY created_at DESC LIMIT 1",
        (thread["id"],)
    ).fetchone()
    db.close()
    return {
        "id": thread["id"],
        "title": thread["title"],
        "last_advance": dict(last_advance) if last_advance else None
    }


def get_pipeline_health():
    """Quick health summary."""
    db = _db()
    now = int(time.time())
    briefs_24h = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
        (now - 86400,)
    ).fetchone()[0]
    captures_24h = db.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE source LIKE '%discord%' AND activity_type='message' AND created_at > ?",
        (now - 86400,)
    ).fetchone()[0]
    db.close()
    return {"briefs": briefs_24h, "captures": captures_24h}


def get_sovereignty_signals(hours=24, limit=5):
    """Briefs touching sovereignty, crypto, ICP, decentralization themes."""
    db = _db()
    since = int(time.time()) - (hours * 3600)
    # Look for sovereignty-adjacent keywords in briefs
    keywords = ['sovereignty', 'decentraliz', 'bitcoin', 'XRP', 'crypto',
                'self-custody', 'homeforge', 'local infra', 'ICP', 'DFINITY',
                'surveillance', 'privacy', 'censorship']
    results = []
    seen_content = set()
    for kw in keywords:
        rows = db.execute(
            "SELECT substr(content, 1, 200) as content FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' "
            "AND content LIKE ? AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 2",
            (f"%{kw}%", since)
        ).fetchall()
        for r in rows:
            c = r["content"]
            if c not in seen_content:
                seen_content.add(c)
                results.append(dict(r))
        if len(results) >= limit:
            break
    db.close()
    return results[:limit]


def format_brief():
    """Build the structured morning brief."""
    now = datetime.now()
    lines = [f"**Good morning, Nate. {now.strftime('%A, %B %d')}.**\n"]

    # XRP / Portfolio
    xrp = get_xrp_status()
    if xrp:
        lines.append(f"**XRP** ${xrp['price']:.4f} | "
                      f"Portfolio ${xrp['portfolio']:.2f}")
        lines.append("")

    # Pipeline health
    health = get_pipeline_health()
    lines.append(f"**Pipeline** {health['briefs']} briefs | "
                 f"{health['captures']} captures (24h)")

    # Thread
    thread = get_thread_status()
    if thread:
        lines.append(f"\n**Thread #{thread['id']}** {thread['title']}")
        if thread['last_advance']:
            adv = thread['last_advance']['content'][:120]
            lines.append(f"  Last: {adv}...")

    # Predictions
    active_preds, scored_preds = get_predictions_status()
    if active_preds:
        lines.append(f"\n**Predictions** ({len(active_preds)} active)")
        for p in active_preds[:3]:
            lines.append(f"  #{p['id']} [{p['probability']:.0%}] {p['pred']}")
    if scored_preds:
        for p in scored_preds[:2]:
            outcome = "CORRECT" if p['outcome'] == 'correct' else "WRONG"
            lines.append(f"  #{p['id']} [{outcome}] {p['pred']}")

    # Sovereignty signals
    sov = get_sovereignty_signals()
    if sov:
        lines.append(f"\n**Sovereignty signals** ({len(sov)} found)")
        for s in sov[:3]:
            # First sentence only
            first_sent = s['content'].split('.')[0] + '.'
            if len(first_sent) > 140:
                first_sent = first_sent[:137] + '...'
            lines.append(f"  • {first_sent}")

    # Family highlights (what Darby/Ada flagged for Nate)
    voices = get_for_nate_voices(limit=3)
    if voices:
        lines.append(f"\n**Family flagged for you**")
        for v in voices[:3]:
            agent = v['agent'].capitalize()
            # First meaningful sentence (skip very short fragments)
            content = v['content']
            sentences = [s.strip() for s in content.split('.') if len(s.strip()) > 20]
            if sentences:
                first_sent = sentences[0][:130] + ('...' if len(sentences[0]) > 130 else '.')
            else:
                first_sent = content[:130] + ('...' if len(content) > 130 else '')
            lines.append(f"  [{agent}] {first_sent}")

    # Nate's captures processed
    captures = get_nate_captures(limit=5)
    if captures:
        lines.append(f"\n**Your captures** ({len(captures)} processed)")
        for c in captures[:3]:
            first_sent = c['content'].split('.')[0] + '.'
            if len(first_sent) > 140:
                first_sent = first_sent[:137] + '...'
            lines.append(f"  • {first_sent}")

    return "\n".join(lines)


def post_to_discord(content):
    """Post to Discord."""
    import requests
    if len(content) > 1900:
        content = content[:1897] + "..."
    try:
        r = requests.post(OPUS_WEBHOOK, json={"content": content}, timeout=10)
        return r.status_code == 204
    except Exception as e:
        print(f"Discord error: {e}")
        return False


def main():
    preview = "--preview" in sys.argv

    brief = format_brief()
    print(brief)

    if preview:
        print(f"\n(Preview — {len(brief)} chars)")
        return

    if post_to_discord(brief):
        print("\nPosted to Discord.")
    else:
        print("\nFailed to post.")


if __name__ == "__main__":
    main()
