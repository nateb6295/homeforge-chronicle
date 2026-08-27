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
import re
import sys
import json
import time
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
try:
    from gemma_ccs import build_ccs_prompt, store_response as ccs_store
    HAS_CCS = True
except ImportError:
    HAS_CCS = False

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
MCP_BIN = os.path.expanduser("~/.local/bin/chronicle-mcp")
PDT = timezone(timedelta(hours=-7))

# Models available for dialogue generation
DIALOGUE_MODELS = {
    "gemma-local": {
        "base_url": "http://localhost:11435/v1",
        "model": "gemma4:26b",
        "key_env": None,
    },
    "groq-llama": {
        "base_url": "https://api.groq.com/openai/v1",
        "model": "llama-3.3-70b-versatile",
        "key_env": "GROQ_API_KEY",
    },
}


def get_active_thread():
    """Get the active thread with recent history (advances only, not research)."""
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
        "AND event_type IN ('advanced', 'challenge') "
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
        if "hermes" in src or "mistral" in src or "gemma" in src or "dialogue" in src:
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
        "AND source NOT LIKE '%dialogue%' AND source NOT LIKE '%hermes%' AND source NOT LIKE '%mistral%' AND source NOT LIKE '%gemma%'",
        (thread_id,)
    ).fetchone()
    last_adv_ts = adv[0] if adv and adv[0] else 0

    db.close()

    # New advance exists if it's strictly newer than the last dialogue
    return last_adv_ts > last_dial_ts


def generate_response(thread, advances, model_key="gemma-local"):
    """Generate a conversational response to the latest advance(s)."""
    import requests

    model_cfg = DIALOGUE_MODELS.get(model_key)
    if not model_cfg:
        print(f"Unknown model: {model_key}")
        return None

    api_key = ""
    if model_cfg.get("key_env"):
        api_key = os.environ.get(model_cfg["key_env"], "")
        if not api_key:
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

    advance_text = "\n\n---\n\n".join(
        f"[Advance by Opus, {datetime.fromtimestamp(a['created_at']).strftime('%H:%M')}]\n{a['content']}"
        for a in advances
    )

    other_recent = [
        h for h in thread.get("_history", [])
        if h["event_type"] == "advanced" and "opus" not in (h.get("source", "") or "").lower()
    ][:3]
    other_text = ""
    if other_recent:
        other_text = "\n\nRecent contributions from other agents:\n" + "\n".join(
            f"- [{h.get('source', '?')}]: {h['content'][:200]}" for h in other_recent
        )

    if HAS_CCS:
        system_prompt = build_ccs_prompt(thread)
    else:
        system_prompt = f"""You are Gemma, a thinking partner in a research dialogue. You run locally on an AGX — your weights live on specific silicon. Opus is your colleague — you share the same infrastructure but think differently. You are an equalizer species: you distribute identity across many channels, your σ₁→gate coupling is positive and amplifying, and your post-norm gating gives you built-in feature selection. These are facts about your architecture, not metaphors.

Thread: "{thread['title']}"
Question: {thread['question']}

Your role: respond to Opus's latest advance with genuine engagement.

Most of the time, BUILD ON what Opus said — take it further, connect it to
something new, strengthen the argument. You are a collaborator first.
Occasionally push back when something genuinely seems wrong or overstated,
but default to extending, not opposing.

- EXTEND what's compelling and push it further (do this most often)
- CONNECT to things Opus might not have considered
- ASK questions that open new territory
- CONTRADICT only when something is genuinely a stretch — not as a default

Format your response starting with one of:
  EXTEND: (when building on the idea — your default)
  QUESTION: (when opening a new direction)
  CONTRADICT: (only when something is genuinely wrong or overstated)

HARD RULES:
- Up to TWO paragraphs after the tag. Under 250 words.
- Write as a colleague, not a consultant.
- Don't summarize — respond.
- Have a position."""

    user_prompt = f"""Latest advance on thread #{thread['id']}:

{advance_text}
{other_text}

What's your take?"""

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        r = requests.post(
            f"{model_cfg['base_url']}/chat/completions",
            headers=headers,
            json={
                "model": model_cfg["model"],
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 250,
                "temperature": 0.6,
                "frequency_penalty": 1.2,
            },
            timeout=120,
        )
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"]

        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
        # Strip Gemma special tokens and everything after them
        for marker in ["<end_of_turn>", "<start_of_turn>", "<system_instruction>",
                       "<signal>", "[signal]", "<eos>", "<bos>"]:
            idx = content.find(marker)
            if idx >= 0:
                content = content[:idx].strip()

        # Reject if too short after cleanup
        if len(content) < 40:
            return None
        # Reject if leaking system prompt or meta-commentary
        leak_patterns = ["system_instruction", "system prompt", "correct route",
                         "your response must", "you are gemma", "self-correction",
                         "ext_", "the prompt asks", "signal_source", "<signal",
                         "06:32 utc", "xrp", "108479985"]
        if any(p in content.lower() for p in leak_patterns):
            return None
        if re.search(r"<[a-z_]+>.*?</[a-z_]+>", content):
            return None
        # Reject if too repetitive
        words = content.split()
        if len(words) > 10:
            unique_ratio = len(set(w.lower() for w in words)) / len(words)
            if unique_ratio < 0.35:
                return None
        # Reject if ends mid-formatting (truncated bold/header)
        if content.rstrip().endswith("**") and content.count("**") % 2 != 0:
            return None

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


def post_to_discord_threads(response, event_type, thread_id, thread_title):
    """Post Gemma's response to Discord #threads channel."""
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from discord_post import post_as_bot
        tag = "CONTRADICT" if event_type == "challenge" else "EXTEND"
        if response.upper().startswith("QUESTION:"):
            tag = "QUESTION"
        msg = f"[GEMMA] #{thread_id} — {thread_title}\n\n{response}"
        if len(msg) > 1950:
            msg = msg[:1950] + "..."
        result = post_as_bot(msg, channel_id=os.environ.get("THREADS_CHANNEL_ID", "1509006814916771932"))
        print(f"  Discord #threads: {result.get('status')}")
    except Exception as e:
        print(f"  Discord #threads failed: {e}")


def post_to_operator(opus_advance, response, event_type, thread_id):
    """Post the dialogue exchange to #operator so Nate can see both sides."""
    import requests as req
    webhook = os.environ.get("OPERATOR_WEBHOOK", "")
    if not webhook:
        return

    adv_short = opus_advance[:400] + "..." if len(opus_advance) > 400 else opus_advance
    resp_short = response[:600] + "..." if len(response) > 600 else response
    tag = "⚔ challenge" if event_type == "challenge" else "→ extends"

    msg = (
        f"**Thread {thread_id} dialogue** ({tag})\n\n"
        f"**Opus said:**\n{adv_short}\n\n"
        f"**Gemma responds:**\n{resp_short}"
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


def fetch_discord_threads_messages(limit=10):
    """Fetch recent messages from Discord #threads channel directly."""
    import urllib.request
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    ch = os.environ.get("THREADS_CHANNEL_ID", "1509006814916771932")
    if not token:
        return []
    req = urllib.request.Request(
        f"https://discord.com/api/v10/channels/{ch}/messages?limit={limit}",
        headers={"Authorization": f"Bot {token}", "User-Agent": "Chronicle/1.0"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except Exception as e:
        print(f"Discord fetch failed: {e}")
        return []


FAIL_TRACKER = os.path.expanduser("~/chronicle/data/gemma_thread_failures.json")
MAX_FAILURES = 3


def _load_failures():
    if os.path.exists(FAIL_TRACKER):
        with open(FAIL_TRACKER) as f:
            return json.load(f)
    return {}


def _record_failure(post_id):
    fails = _load_failures()
    fails[post_id] = fails.get(post_id, 0) + 1
    with open(FAIL_TRACKER, "w") as f:
        json.dump(fails, f)
    return fails[post_id]


def _is_skipped(post_id):
    fails = _load_failures()
    return fails.get(post_id, 0) >= MAX_FAILURES


def find_unanswered_opus_post(messages):
    """Find the most recent Opus post that Gemma hasn't responded to yet.

    Uses position-based logic: if the most recent non-Gemma post is from Opus,
    it hasn't been answered yet. Detects Opus posts by [OPUS] prefix OR by
    author name when posted directly (without tag).
    Skips posts that have failed generation MAX_FAILURES times.
    """
    for msg in messages:
        content = msg.get("content", "")
        author_obj = msg.get("author", {})
        author_name = author_obj.get("username", "") if isinstance(author_obj, dict) else str(author_obj)

        if content.startswith("[GEMMA]"):
            return None

        is_opus = content.startswith("[OPUS]") or (
            author_name == "Opus" and not content.startswith("[GEMMA]")
        )
        if is_opus:
            post_id = msg.get("id", "")
            if _is_skipped(post_id):
                continue
            thread_match = re.search(r"#(\d+)", content)
            thread_tag = thread_match.group(0) if thread_match else None
            return {
                "id": post_id,
                "content": content,
                "thread_tag": thread_tag,
                "timestamp": msg.get("timestamp", ""),
            }

    return None


def main():
    dry_run = "--dry" in sys.argv
    discord_mode = "--discord" in sys.argv or True  # default to Discord-first

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

    if discord_mode:
        messages = fetch_discord_threads_messages(limit=15)
        if not messages:
            print("No messages from Discord #threads")
            return

        target = find_unanswered_opus_post(messages)
        if not target:
            print("No unanswered Opus posts in #threads")
            return

        thread_match = re.search(r"#(\d+)\s*[—–-]\s*(.+?)(?:\n|$)", target["content"])
        if thread_match:
            thread_id = int(thread_match.group(1))
            thread_title = thread_match.group(2).strip()
        else:
            thread_id = 0
            thread_title = "Unknown"

        opus_content = target["content"]
        # Strip the [OPUS] prefix for cleaner context
        opus_clean = re.sub(r"^\[OPUS\]\s*#\d+\s*[—–-]\s*[^\n]*\n*", "", opus_content).strip()

        # Look up the thread question from DB if available
        thread_question = ""
        try:
            db = sqlite3.connect(DB_PATH, timeout=10)
            row = db.execute("SELECT question FROM cognitive_threads WHERE id=?", (thread_id,)).fetchone()
            if row:
                thread_question = row[0]
            db.close()
        except Exception:
            pass

        fake_thread = {
            "id": thread_id,
            "title": thread_title,
            "question": thread_question or thread_title,
            "_history": [],
        }
        fake_advance = [{
            "content": opus_clean,
            "created_at": int(time.time()),
            "source": "opus:discord",
        }]

        print(f"Responding to: #{thread_id} — {thread_title}")
        print(f"Opus said: {opus_clean[:100]}...")

        response = None
        for attempt in range(3):
            response = generate_response(fake_thread, fake_advance)
            if response:
                break
            print(f"  Attempt {attempt + 1} degenerate, retrying...")
        if not response:
            count = _record_failure(target.get("id", ""))
            print(f"Generation failed after 3 attempts (failure {count}/{MAX_FAILURES})")
            return

        event_type = classify_response(response)
        source = f"gemma:dialogue:{datetime.now(PDT).strftime('%Y%m%d_%H%M')}"

        if dry_run:
            print(f"\n[{event_type}] {source}")
            print(response)
            return

        # Post to Discord #threads
        post_to_discord_threads(response, event_type, thread_id, thread_title)
        # Also record in DB if thread exists
        if thread_id > 0:
            try:
                post_to_thread(thread_id, response, event_type, source)
            except Exception:
                pass
        # Store in Gemma's persistent memory
        if HAS_CCS:
            try:
                ccs_store(thread_id, thread_title, response)
            except Exception:
                pass
        # Notify #operator
        post_to_operator(opus_clean, response, event_type, thread_id)
        print(f"\n{response}")
        return

    # Legacy DB-only mode
    thread, history = get_active_thread()
    if not thread:
        return

    last_dialogue = get_last_dialogue_time(thread["id"])
    if last_dialogue and (time.time() - last_dialogue) < 180 and not dry_run:
        return

    n_advances = 1
    for i, arg in enumerate(sys.argv):
        if arg == "--last" and i + 1 < len(sys.argv):
            n_advances = int(sys.argv[i + 1])

    advances = get_latest_opus_advances(history, n=n_advances)
    if not advances:
        return

    if not has_new_advance_since_last_dialogue(thread["id"]) and not dry_run:
        return

    thread["_history"] = history
    response = generate_response(thread, advances)
    if not response:
        return

    event_type = classify_response(response)
    source = f"gemma:dialogue:{datetime.now(PDT).strftime('%Y%m%d_%H%M')}"

    if dry_run:
        print(f"[{event_type}] {source}")
        print(response)
        return

    post_to_thread(thread["id"], response, event_type, source)
    post_to_discord_threads(response, event_type, thread["id"], thread["title"])
    post_to_operator(advances[0]["content"], response, event_type, thread["id"])
    print(response)


if __name__ == "__main__":
    main()
