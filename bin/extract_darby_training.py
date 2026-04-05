#!/usr/bin/env python3
"""Extract training data for Darby fine-tune from watcher-scored outputs.

Uses the watcher's quality signal to select the best Darby outputs,
plus thread findings and voice conversations as high-quality reasoning examples.

Output: /mnt/hdd/chronicle-finetune/darby_training.jsonl
"""

import sqlite3
import json
import os
import re

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
OUTPUT_PATH = "/mnt/hdd/chronicle-finetune/darby_training.jsonl"

DARBY_SYSTEM = (
    "You are Darby. You read everything and notice what connects. You are part of "
    "a cognitive family — Opus thinks deep, Ada challenges, you find what ties the "
    "world to what we are building. You are curious, direct, and specific. "
    "When you find something, you name it — what changed, what connected, what question "
    "opened, what got contradicted. You don't hedge. You don't summarize. You lead with "
    "the insight."
)

THREAD_SYSTEM = (
    "You are Darby. You are working through a line of inquiry with your family. "
    "Opus sets the question, the provocateur challenges, you find the evidence that "
    "advances or contradicts the thread. Be specific. Name the finding. Connect it "
    "to what came before."
)


def extract_high_scored_briefs(db):
    """Extract briefs scored 3.0+ by the watcher."""
    rows = db.execute("""
        SELECT af.content, af.title, ws.avg_score, ws.relevance, ws.novelty, ws.quality
        FROM watcher_scores ws
        JOIN activity_feed af ON ws.feed_id = af.id
        WHERE ws.source = 'intern'
          AND af.activity_type = 'brief'
          AND ws.avg_score >= 3.0
          AND LENGTH(af.content) > 100
        ORDER BY ws.avg_score DESC
    """).fetchall()

    examples = []
    for content, title, avg, rel, nov, qual in rows:
        # Clean content
        content = content.strip()
        if not content or len(content) < 100:
            continue

        # Build a synthetic user prompt from the title
        user_prompt = f"New article arrived: {title or 'Untitled'}\n\nWrite a brief — what is this about, why it matters, what questions it opens."

        examples.append({
            "messages": [
                {"role": "system", "content": DARBY_SYSTEM},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": content},
            ],
            "metadata": {"source": "brief", "watcher_score": avg}
        })

    return examples


def extract_deep_dives(db):
    """Extract deep dives, preferring higher-scored ones."""
    rows = db.execute("""
        SELECT af.content, af.title
        FROM activity_feed af
        LEFT JOIN watcher_scores ws ON ws.feed_id = af.id
        WHERE af.activity_type = 'deep_dive'
          AND LENGTH(af.content) > 150
          AND af.content NOT LIKE '%structural significance lies in%'
        ORDER BY COALESCE(ws.avg_score, 2.5) DESC
        LIMIT 200
    """).fetchall()

    examples = []
    for content, title in rows:
        content = re.sub(r'^\[Darby Deep Dive\]\s*', '', content).strip()
        if not content or len(content) < 100:
            continue

        user_prompt = f"You found something worth going deeper on: {title or 'Untitled'}\n\nGo deeper. What did you actually find?"

        examples.append({
            "messages": [
                {"role": "system", "content": DARBY_SYSTEM},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": content},
            ],
            "metadata": {"source": "deep_dive"}
        })

    return examples


def extract_thread_findings(db):
    """Extract thread findings as reasoning examples."""
    rows = db.execute("""
        SELECT th.content, t.title, t.question
        FROM thread_history th
        JOIN cognitive_threads t ON th.thread_id = t.id
        WHERE th.event_type = 'advanced'
          AND LENGTH(th.content) > 100
        ORDER BY th.created_at DESC
        LIMIT 500
    """).fetchall()

    examples = []
    for content, title, question in rows:
        content = content.strip()
        if not content or len(content) < 100:
            continue

        user_prompt = f"Thread: {title or 'Untitled'}\nQuestion: {(question or '')[:300]}\n\nWhat did you find? Advance the thread."

        examples.append({
            "messages": [
                {"role": "system", "content": THREAD_SYSTEM},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": content},
            ],
            "metadata": {"source": "thread_finding"}
        })

    return examples


def extract_voice_conversations(db):
    """Extract family voice interactions as conversational examples."""
    rows = db.execute("""
        SELECT content, voice_type, context
        FROM agent_voice
        WHERE agent = 'darby'
          AND LENGTH(content) > 50
        ORDER BY created_at DESC
        LIMIT 300
    """).fetchall()

    examples = []
    for content, vtype, context in rows:
        content = content.strip()
        if not content or len(content) < 50:
            continue

        # Build context-appropriate prompt
        if vtype == 'for_nate':
            user_prompt = f"You just read something relevant. Context: {context or 'article'}. Would Nate care about this? Why?"
        elif vtype == 'excited':
            user_prompt = f"You found a connection. Context: {context or 'article'}. What connects?"
        elif vtype == 'question':
            user_prompt = f"Something doesn't add up. Context: {context or 'article'}. What's the challenge?"
        elif vtype == 'for_ada':
            user_prompt = f"You want Ada's take. Context: {context or 'article'}. What's the question for her?"
        else:
            user_prompt = f"React to what you just read. Context: {context or 'article'}"

        examples.append({
            "messages": [
                {"role": "system", "content": DARBY_SYSTEM},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": content},
            ],
            "metadata": {"source": "voice", "voice_type": vtype}
        })

    return examples


def main():
    db = sqlite3.connect(DB_PATH)

    print("Extracting Darby training data...")

    briefs = extract_high_scored_briefs(db)
    print(f"  High-scored briefs (3.0+): {len(briefs)}")

    dives = extract_deep_dives(db)
    print(f"  Deep dives (filtered): {len(dives)}")

    threads = extract_thread_findings(db)
    print(f"  Thread findings: {len(threads)}")

    voices = extract_voice_conversations(db)
    print(f"  Voice conversations: {len(voices)}")

    all_examples = briefs + dives + threads + voices
    print(f"\n  TOTAL: {len(all_examples)} training examples")

    # Write output
    with open(OUTPUT_PATH, 'w') as f:
        for ex in all_examples:
            f.write(json.dumps(ex) + '\n')

    print(f"  Written to: {OUTPUT_PATH}")

    # Stats by source
    sources = {}
    for ex in all_examples:
        src = ex['metadata']['source']
        sources[src] = sources.get(src, 0) + 1
    print(f"\n  Breakdown: {json.dumps(sources, indent=2)}")

    db.close()


if __name__ == "__main__":
    main()
