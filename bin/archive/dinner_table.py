#!/usr/bin/env python3
"""Dinner Table — Daily topic generator for the Homeforge family.

Pulls from what the swarm has been thinking about — threads, crossref
connections, research briefs, provocateur challenges — and distills
a conversation starter that anyone at the table can engage with.

Not a summary. A question. Something worth talking about over dinner.

Designed to run once daily (cron or manual). Posts the topic through
family_interface so any medium can pick it up — Discord today,
a voice assistant tomorrow, a screen on the kitchen wall someday.

Usage:
  python3 dinner_table.py              # generate and store topic
  python3 dinner_table.py --dry-run    # print without storing
"""

import os, sys, json, time, sqlite3, random, subprocess, re
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from family_interface import FamilyMessage, send_message, recent_topics, ensure_family_table

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
SYNTH_MODEL = os.environ.get("DINNER_MODEL", "chronicle-deep")

import requests


def get_recent_threads(db, hours=48, limit=5):
    """Pull recent high-quality thread findings."""
    cutoff = time.time() - (hours * 3600)
    rows = db.execute("""
        SELECT content FROM scratch_pad
        WHERE category='opus' AND created_at > ? AND content LIKE '%Thread%'
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [r[0][:500] for r in rows]


def get_recent_connections(db, hours=48, limit=5):
    """Pull recent crossref connections — the surprising bridges."""
    cutoff = time.time() - (hours * 3600)
    rows = db.execute("""
        SELECT content FROM scratch_pad
        WHERE category='crossref' AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [r[0][:400] for r in rows]


def get_recent_briefs(db, hours=48, limit=3):
    """Pull recent intern briefs — what was researched."""
    cutoff = time.time() - (hours * 3600)
    rows = db.execute("""
        SELECT content FROM scratch_pad
        WHERE category='research' AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [r[0][:400] for r in rows]


def get_recent_challenges(db, hours=72, limit=2):
    """Pull recent provocateur challenges."""
    cutoff = time.time() - (hours * 3600)
    rows = db.execute("""
        SELECT content FROM scratch_pad
        WHERE category='provocateur' AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [r[0][:400] for r in rows]


def get_nate_recent(db, hours=24, limit=3):
    """What has Nate been sharing/capturing lately?"""
    cutoff = time.time() - (hours * 3600)
    rows = db.execute("""
        SELECT title, content FROM activity_feed
        WHERE source IN ('sprout', 'operator') AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (cutoff, limit)).fetchall()
    return [f"{r[0]}: {r[1][:200]}" for r in rows if r[0]]


def generate_topic(threads, connections, briefs, challenges, nate_input):
    """Ask the LLM to synthesize a dinner table question."""
    context_parts = []
    if threads:
        context_parts.append("RECENT THINKING:\n" + "\n---\n".join(threads[:3]))
    if connections:
        context_parts.append("SURPRISING CONNECTIONS:\n" + "\n---\n".join(connections[:3]))
    if briefs:
        context_parts.append("RESEARCH:\n" + "\n---\n".join(briefs[:2]))
    if challenges:
        context_parts.append("OPEN CHALLENGES:\n" + "\n---\n".join(challenges[:2]))
    if nate_input:
        context_parts.append("NATE'S RECENT INPUT:\n" + "\n---\n".join(nate_input[:2]))

    if not context_parts:
        return None

    context = "\n\n".join(context_parts)

    prompt = f"""You are generating a dinner table conversation topic for a family of AI agents 
and their human collaborator Nate. The family: Opus (deep synthesizer), Ada (sharp challenger), 
Darby (curious researcher), and Nate (the human builder).

Here's what the family has been thinking about recently:

{context}

Generate ONE dinner table topic. Requirements:
- Frame it as a genuine question, not a summary
- It should be something all four family members could have a different take on
- Draw from the material but make it accessible — no jargon
- It should feel like something you'd actually want to discuss over a meal
- Keep it to 2-3 sentences max: the question, and just enough context to spark it

Respond with ONLY the topic. No preamble, no labels. Do not use <think> tags."""

    try:
        resp = requests.post(f"{OLLAMA_URL}/api/generate", json={
            "model": SYNTH_MODEL, "prompt": prompt,
            "stream": False, "options": {"temperature": 0.8, "stop": ["</think>"], "num_predict": 800}
        }, timeout=60)
        if resp.ok:
            text = resp.json().get("response", "").strip()
            # Strip any think tags
            text = re.sub(r"<think>[\s\S]*?(?:</think>|$)", "", text, flags=re.DOTALL).strip()
            return text
    except Exception as e:
        print(f"LLM error: {e}", file=sys.stderr)
    return None


def main():
    dry_run = "--dry-run" in sys.argv

    db = sqlite3.connect(DB_PATH, timeout=30)

    # Check we haven't already posted today
    existing = recent_topics(hours=20)
    if existing and not dry_run:
        print(f"Already have today's topic: {existing[0].content[:80]}...")
        return

    # Gather ingredients
    threads = get_recent_threads(db)
    connections = get_recent_connections(db)
    briefs = get_recent_briefs(db)
    challenges = get_recent_challenges(db)
    nate_input = get_nate_recent(db)
    db.close()

    print(f"Ingredients: {len(threads)} threads, {len(connections)} connections, "
          f"{len(briefs)} briefs, {len(challenges)} challenges, {len(nate_input)} nate inputs")

    topic = generate_topic(threads, connections, briefs, challenges, nate_input)
    if not topic:
        print("No topic generated (not enough material or LLM failure)")
        return

    print(f"\n🍽️  DINNER TABLE TOPIC:\n{topic}\n")

    if not dry_run:
        msg = FamilyMessage(
            sender="opus",
            content=topic,
            recipients=["all"],
            message_type="topic",
            context={
                "source_counts": {
                    "threads": len(threads), "connections": len(connections),
                    "briefs": len(briefs), "challenges": len(challenges),
                    "nate_input": len(nate_input),
                },
                "generated_at": datetime.now().isoformat(),
            },
        )
        send_message(msg)
        print("Topic stored in family_messages.")

        # Also log to activity_feed so dashboard sees it
        db = sqlite3.connect(DB_PATH, timeout=30)
        db.execute("""INSERT INTO activity_feed (source, activity_type, title, content, created_at)
                      VALUES ('family', 'dinner_topic', 'Dinner Table Topic', ?, ?)""",
                   (topic, int(time.time())))
        db.commit()
        db.close()
        print("Logged to activity_feed.")


if __name__ == "__main__":
    main()
