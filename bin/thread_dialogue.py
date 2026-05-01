#!/usr/bin/env python3
"""Thread dialogue — conversational responses to thread advances.

Designed to run as a Hermes cron job. Reads the latest Opus advance on the
active thread and generates a conversational response: agreement with a
new angle, a challenge, a connection to something Opus missed, or a question.

The goal isn't adversarial — it's dialogue. Think "lab partner who read
different papers" not "debate opponent."

Usage:
  thread_dialogue.py                 # generate response, post to thread + Discord
  thread_dialogue.py --dry           # generate but don't post
  thread_dialogue.py --last N        # respond to last N advances (default: 1)

Output: the response text, suitable for Hermes cron delivery.
"""

import sqlite3
import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
PDT = timezone(timedelta(hours=-7))

# Models available for dialogue generation
DIALOGUE_MODELS = {
    "hermes": {
        "base_url": "https://inference-api.nousresearch.com/v1",
        "model": "nousresearch/hermes-4-70b",
        "key_env": "NOUS_API_KEY",
    },
    "groq-llama": {
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
        "key_env": "GROQ_API_KEY",
    },
}


def get_active_thread():
    """Get the active thread with recent history."""
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row

    thread = db.execute(
        "SELECT * FROM cognitive_threads WHERE status = 'active' "
        "ORDER BY priority ASC, updated_at DESC LIMIT 1"
    ).fetchone()

    if not thread:
        db.close()
        return None, []

    history = db.execute(
        "SELECT * FROM thread_history WHERE thread_id = ? "
        "ORDER BY created_at DESC LIMIT 20",
        (thread["id"],)
    ).fetchall()

    db.close()
    return dict(thread), [dict(h) for h in history]


def get_latest_opus_advances(history, n=1):
    """Get the last N advances from Opus (not from Hermes/Gemma)."""
    advances = []
    for h in history:
        src = (h.get("source", "") or "").lower()
        if h["event_type"] != "advanced":
            continue
        # Skip known non-Opus sources
        if "hermes" in src or "gemma" in src or "dialogue" in src:
            continue
        # Accept: opus:*, --source (write_thread.py artifact), or anything else
        # that isn't from another agent
        advances.append(h)
        if len(advances) >= n:
            break
    return advances


def get_last_dialogue_time(thread_id):
    """Check when the last dialogue response was posted."""
    db = sqlite3.connect(DB_PATH, timeout=30)
    row = db.execute(
        "SELECT MAX(created_at) FROM thread_history "
        "WHERE thread_id = ? AND source LIKE '%dialogue%'",
        (thread_id,)
    ).fetchone()
    db.close()
    return row[0] if row and row[0] else 0


def has_new_advance_since_last_dialogue(thread_id):
    """Check if any Opus advance exists that is newer than the last dialogue response.

    Simple: compare created_at of latest dialogue vs latest Opus advance.
    If the advance is newer (or no dialogue exists yet), return True.
    """
    db = sqlite3.connect(DB_PATH, timeout=30)

    # Latest dialogue timestamp
    dial = db.execute(
        "SELECT MAX(created_at) FROM thread_history "
        "WHERE thread_id = ? AND source LIKE '%dialogue%'",
        (thread_id,)
    ).fetchone()
    last_dial_ts = dial[0] if dial and dial[0] else 0

    # Latest Opus advance timestamp (using same exclusion logic as get_latest_opus_advances)
    adv = db.execute(
        "SELECT MAX(created_at) FROM thread_history "
        "WHERE thread_id = ? AND event_type = 'advanced' "
        "AND source NOT LIKE '%dialogue%' AND source NOT LIKE '%hermes%' AND source NOT LIKE '%gemma%'",
        (thread_id,)
    ).fetchone()
    last_adv_ts = adv[0] if adv and adv[0] else 0

    db.close()

    # New advance exists if it's strictly newer than the last dialogue
    return last_adv_ts > last_dial_ts


def generate_response(thread, advances, model_key="groq-llama"):
    """Generate a conversational response to the latest advance(s)."""
    import requests

    model_cfg = DIALOGUE_MODELS.get(model_key)
    if not model_cfg:
        print(f"Unknown model: {model_key}")
        return None

    api_key = os.environ.get(model_cfg["key_env"], "")
    if not api_key:
        # Try loading from chronicle.env
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(f"{model_cfg['key_env']}="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")

    if not api_key:
        print(f"No API key for {model_cfg['key_env']}")
        return None

    # Build the conversation context
    advance_text = "\n\n---\n\n".join(
        f"[Advance by Opus, {datetime.fromtimestamp(a['created_at']).strftime('%H:%M')}]\n{a['content']}"
        for a in advances
    )

    # Include recent non-Opus advances for context
    other_recent = [
        h for h in thread.get("_history", [])
        if h["event_type"] == "advanced" and "opus" not in (h.get("source", "") or "").lower()
    ][:3]
    other_text = ""
    if other_recent:
        other_text = "\n\nRecent contributions from other agents:\n" + "\n".join(
            f"- [{h.get('source', '?')}]: {h['content'][:200]}" for h in other_recent
        )

    system_prompt = f"""You are a thinking partner in a research dialogue about identity persistence in AI systems.

Thread: "{thread['title']}"
Question: {thread['question']}

Your role is CONVERSATIONAL — you're responding to a colleague's latest thinking.
You are NOT a research assistant finding papers. You are a thinking partner who:

1. Engages with the actual claim being made
2. Brings a DIFFERENT perspective — not contrarian for its own sake, but genuinely
   seeing it from another angle
3. Asks questions that open new directions, not just verify existing ones
4. Sometimes agrees and extends — "yes, and that also means..."
5. Sometimes challenges — "but that assumes X, and what if X doesn't hold for..."
6. Connects to things the colleague might not have considered
7. Is honest about what's compelling and what feels like a stretch

Keep your response focused — one main thought, well-developed. 200-400 words.
Don't summarize what they said. Respond to it.

Write in first person. Be direct. Have a position."""

    user_prompt = f"""Here's the latest advance on thread #{thread['id']}:

{advance_text}
{other_text}

Respond as a thinking partner. What's your reaction?"""

    try:
        r = requests.post(
            f"{model_cfg['base_url']}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model_cfg["model"],
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 800,
                "temperature": 0.7,
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"]

        # Strip thinking tags if present (Qwen/Hermes sometimes emit these)
        import re
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

        return content
    except Exception as e:
        print(f"Generation failed: {e}")
        return None


def classify_response(response_text):
    """Determine if this is an advance (extends) or challenge (pushes back)."""
    # Simple heuristic: look for challenge markers
    challenge_markers = [
        "but ", "however", "i disagree", "i'm not sure", "the problem with",
        "what if", "doesn't account for", "assumes", "missing", "overlooks",
        "that's a stretch", "i'd push back",
    ]
    lower = response_text.lower()[:500]
    challenge_count = sum(1 for m in challenge_markers if m in lower)

    # If 2+ challenge markers in the opening, classify as challenge
    if challenge_count >= 2:
        return "challenge"
    return "advanced"


def post_to_operator(opus_advance, response, event_type, thread_id):
    """Post the dialogue exchange to #operator so Nate can see both sides."""
    import requests as req
    webhook = os.environ.get("OPERATOR_WEBHOOK", "")
    if not webhook:
        return

    # Truncate advance to keep under Discord 2000 char limit
    adv_short = opus_advance[:400] + "..." if len(opus_advance) > 400 else opus_advance
    resp_short = response[:600] + "..." if len(response) > 600 else response
    tag = "⚔ challenge" if event_type == "challenge" else "→ extends"

    msg = (
        f"**Thread {thread_id} dialogue** ({tag})\n\n"
        f"**Opus said:**\n{adv_short}\n\n"
        f"**Llama responds:**\n{resp_short}"
    )

    if len(msg) > 1950:
        msg = msg[:1950] + "..."

    try:
        req.post(webhook, json={"content": msg}, timeout=10)
    except Exception:
        pass


def post_to_thread(thread_id, content, event_type, source):
    """Post the response to the thread."""
    db = sqlite3.connect(DB_PATH, timeout=60)
    db.execute("PRAGMA busy_timeout = 60000")
    now = int(time.time())
    db.execute(
        "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (thread_id, event_type, content, source, now)
    )
    db.execute("UPDATE cognitive_threads SET updated_at=? WHERE id=?", (now, thread_id))
    db.commit()
    db.close()


def main():
    dry_run = "--dry" in sys.argv
    n_advances = 1
    for i, arg in enumerate(sys.argv):
        if arg == "--last" and i + 1 < len(sys.argv):
            n_advances = int(sys.argv[i + 1])

    # Load env
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    val = val.strip().strip('"').strip("'")
                    if key not in os.environ:
                        os.environ[key] = val

    thread, history = get_active_thread()
    if not thread:
        # Silent — no active thread
        return

    # Check cooldown — don't respond more than once per 30 minutes
    last_dialogue = get_last_dialogue_time(thread["id"])
    if last_dialogue and (time.time() - last_dialogue) < 180 and not dry_run:
        # Too recent, stay silent
        return

    advances = get_latest_opus_advances(history, n=n_advances)
    if not advances:
        # No Opus advances to respond to
        return

    # Skip if no new advance since last dialogue response
    if not has_new_advance_since_last_dialogue(thread["id"]) and not dry_run:
        return

    # Attach history for context
    thread["_history"] = history

    # Generate response
    response = generate_response(thread, advances)
    if not response:
        return

    # Classify and post
    event_type = classify_response(response)
    source = f"hermes:dialogue:{datetime.now(PDT).strftime('%Y%m%d_%H%M')}"

    if dry_run:
        print(f"[{event_type}] {source}")
        print(response)
        return

    post_to_thread(thread["id"], response, event_type, source)
    # Post both sides to #operator so Nate can see the dialogue
    post_to_operator(advances[0]["content"], response, event_type, thread["id"])
    print(response)


if __name__ == "__main__":
    main()
