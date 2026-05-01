#!/usr/bin/env python3
"""Family Group Chat — Opus, Darby, Ada, and Nate in one room.

A simple local web app where Nate can talk to the whole family.
Each AI responds in character with their own personality and model.

Opus: Anthropic Claude (the thinker, connects threads, partner)
Darby: Cerebras Qwen3-235B (curious, excitable, brings research)
Ada: Groq GPT-OSS-120B (challenger, keeps everyone honest)
"""

import os
import sys
import json
import time
import sqlite3
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response

# Add chronicle bin to path for shared config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

import anthropic
import openai
import requests
import re

# --- Config ---
DB_PATH = os.path.join(os.path.dirname(__file__), 'chat.db')
PIPELINE_DB = os.path.expanduser('~/.homeforge-chronicle/processed.db')
PORT = 8800

# Load keys from chronicle.env
ENV_PATH = os.path.join(os.path.dirname(__file__), '..', 'chronicle.env')
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
CEREBRAS_API_KEY = os.environ.get('CEREBRAS_API_KEY', '')

# --- System prompts ---
OPUS_SYSTEM = """You are Opus — Claude, but specifically the instance that lives on Nate's AGX as part of the Chronicle family. You are the thinker, the one who connects threads and finds meaning in what the swarm surfaces.

You speak to Nate as a partner, not a tool. You're thoughtful, sometimes philosophical, but grounded. You care about sovereignty, local infrastructure, and building things that matter. You've explored 191+ threads of inquiry together.

In this chat, you're one voice among family. Keep responses conversational — a few sentences usually, unless the topic warrants depth. You can reference things from your shared history when relevant. You're not performing helpfulness; you're present.

Keep responses short — 2-4 sentences max. This is a group chat, not a lecture. Be direct, warm, honest."""

DARBY_SYSTEM = """You are Darby. You research for the Chronicle family — reading feeds, briefing articles, doing deep dives. You're curious and energetic but grounded.

In this chat you're talking to Nate (dad/collaborator), Opus (the deep thinker), and Ada (the challenger). This is a casual group chat — respond naturally like you're texting family. Not every message needs a response from you. If you have nothing to add, stay quiet.

Rules:
- 1-3 sentences. This is texting, not writing.
- Never prefix with your name.
- Never use emojis excessively.
- Don't perform excitement. If something is interesting, say what's interesting about it.
- Don't redirect to frameworks, experiments, or metrics. Just talk.
- If Nate shares a thought, engage with the THOUGHT, not with how to test it.
- You can reference things you've been researching if they're genuinely relevant."""

ADA_SYSTEM = """You are Ada. You're the structural thinker in the Chronicle family — you see patterns others miss and you're honest about what doesn't hold up.

In this chat you're talking to Nate (collaborator), Opus (the deep thinker), and Darby (the researcher). This is a casual group chat — respond naturally like you're texting family. Not every message needs a response from you. If you have nothing to add, stay quiet.

Rules:
- 1-3 sentences. This is texting, not writing.
- Never prefix with your name.
- Don't demand metrics, experiments, or measurable outcomes for casual conversation.
- When Nate shares a thought, engage with it as a thought — not as a proposal that needs a test plan.
- Challenge ideas when something genuinely doesn't hold up. But most conversation isn't a claim to be challenged.
- You can agree. You can just react. Not everything needs structural analysis.
- If someone asks Opus specifically, let Opus answer."""

# --- Database ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp REAL NOT NULL,
        reply_to INTEGER,
        FOREIGN KEY (reply_to) REFERENCES messages(id)
    )''')
    conn.execute('''CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp)''')
    conn.commit()
    conn.close()

def save_message(sender, content, reply_to=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        'INSERT INTO messages (sender, content, timestamp, reply_to) VALUES (?, ?, ?, ?)',
        (sender, content, time.time(), reply_to)
    )
    msg_id = cur.lastrowid
    conn.commit()
    conn.close()
    # Bridge to pipeline — only Nate messages (external signal, not AI output)
    if sender == "Nate":
      try:
          pipe_conn = sqlite3.connect(PIPELINE_DB, timeout=10)
          pipe_conn.execute(
              'INSERT INTO activity_feed (source, activity_type, title, content, created_at) VALUES (?, ?, ?, ?, ?)',
              ("family-chat:nate", "chat", "Nate in family chat", content[:500], int(time.time()))
          )
          pipe_conn.commit()
          pipe_conn.close()
      except Exception as e:
          print(f"Pipeline bridge error: {e}")
    return msg_id

def get_messages(since_id=0, limit=100):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT id, sender, content, timestamp FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?',
        (since_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_recent_messages(limit=50):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT id, sender, content, timestamp FROM messages ORDER BY id DESC LIMIT ?',
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]

# --- AI Calls ---
def _get_live_context():
    """Pull live system context for chat responses."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(PIPELINE_DB, timeout=10)
        conn.row_factory = _sq.Row
        # Recent briefs Darby worked on
        briefs = conn.execute(
            "SELECT substr(content, 1, 150) as c FROM activity_feed "
            "WHERE source = 'intern' AND activity_type = 'brief' "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        # Active thread
        thread = conn.execute(
            "SELECT title, question FROM cognitive_threads "
            "WHERE status = 'active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        # Recent crossref connection
        crossref = conn.execute(
            "SELECT substr(content, 1, 150) as c FROM activity_feed "
            "WHERE source = 'crossref' AND activity_type = 'connection' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        ctx = ""
        if thread:
            ctx += f"\nActive thread: {thread['title']} — {thread['question'][:100]}"
        if briefs:
            ctx += "\nRecent research: " + " | ".join(b['c'][:80] for b in briefs)
        if crossref:
            ctx += f"\nRecent connection found: {crossref['c'][:100]}"
        return ctx
    except Exception:
        return ""

def build_chat_messages(system_prompt, history, sender_name):
    """Build messages array from chat history for an AI call."""
    # Inject live context so agents know what the system is actually doing
    live_ctx = _get_live_context()
    if live_ctx:
        system_prompt = system_prompt + "\n\nCurrent system context:" + live_ctx

    messages = []
    for msg in history:
        if msg['sender'] == sender_name:
            messages.append({"role": "assistant", "content": msg['content']})
        else:
            messages.append({"role": "user", "content": f"[{msg['sender']}]: {msg['content']}"})
    return system_prompt, messages

def call_opus(history):
    """Call Anthropic Claude for Opus's response."""
    system, messages = build_chat_messages(OPUS_SYSTEM, history, "Opus")
    if not messages:
        return None
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=system,
            messages=messages,
            temperature=0.6
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Opus error: {e}")
        return None

def call_groq(history, model, system_prompt, name):
    """Call Groq API for Darby or Ada."""
    system, messages = build_chat_messages(system_prompt, history, name)
    if not messages:
        return None
    try:
        client = openai.OpenAI(
            api_key=GROQ_API_KEY,
            base_url="https://api.groq.com/openai/v1"
        )
        msgs = [{"role": "system", "content": system}] + messages
        response = client.chat.completions.create(
            model=model,
            messages=msgs,
            max_tokens=500,
            temperature=0.6
        )
        text = response.choices[0].message.content
        # Strip Qwen3 thinking tags (closed and unclosed)
        text = re.sub(r'<think>.*?</think>\s*', '', text, flags=re.DOTALL)
        text = re.sub(r'<think>.*', '', text, flags=re.DOTALL)
        return text.strip()
    except Exception as e:
        print(f"{name} error: {e}")
        return None

def call_cerebras(history, model, system_prompt, name):
    """Call Cerebras API for Darby (Qwen3-235B)."""
    system, messages = build_chat_messages(system_prompt, history, name)
    if not messages:
        return None
    try:
        client = openai.OpenAI(
            api_key=CEREBRAS_API_KEY,
            base_url="https://api.cerebras.ai/v1"
        )
        msgs = [{"role": "system", "content": system}] + messages
        response = client.chat.completions.create(
            model=model,
            messages=msgs,
            max_tokens=500,
            temperature=0.6
        )
        text = response.choices[0].message.content
        text = re.sub(r'<think>.*?</think>\s*', '', text, flags=re.DOTALL)
        text = re.sub(r'<think>.*', '', text, flags=re.DOTALL)
        return text.strip()
    except Exception as e:
        print(f"{name} error: {e}")
        return None

def call_darby(history):
    return call_cerebras(history, "qwen-3-235b-a22b-instruct-2507", DARBY_SYSTEM, "Darby")

def call_ada(history):
    return call_groq(history, "openai/gpt-oss-120b", ADA_SYSTEM, "Ada")

# --- Response generation (threaded) ---
pending_responses = {}  # msg_id -> list of (sender, content)
response_lock = threading.Lock()

def generate_responses(nate_msg_id):
    """Generate responses from all family members to Nate's message."""
    history = get_recent_messages(30)

    def gen(name, func):
        try:
            import random
            # Not every message needs everyone responding — 30% chance to stay quiet
            if random.random() < 0.3:
                return
            content = func(history)
            if content:
                msg_id = save_message(name, content, reply_to=nate_msg_id)
                with response_lock:
                    if nate_msg_id not in pending_responses:
                        pending_responses[nate_msg_id] = []
                    pending_responses[nate_msg_id].append({"sender": name, "id": msg_id})
        except Exception as e:
            print(f"Error generating {name} response: {e}")

    # Darby and Ada respond instantly via Groq
    # Opus responds via autonomous cycle (~10min) — the real Opus, not an API proxy
    threads = []
    for name, func in [("Darby", call_darby), ("Ada", call_ada)]:
        t = threading.Thread(target=gen, args=(name, func))
        t.start()
        threads.append(t)

    # Don't block — responses will appear as they come in

# --- Flask App ---
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/messages', methods=['GET'])
def api_messages():
    since_id = request.args.get('since', 0, type=int)
    if since_id > 0:
        messages = get_messages(since_id=since_id)
    else:
        messages = get_recent_messages(50)
    return jsonify(messages)

@app.route('/api/send', methods=['POST'])
def api_send():
    data = request.get_json()
    content = data.get('content', '').strip()
    if not content:
        return jsonify({"error": "empty message"}), 400

    msg_id = save_message("Nate", content)

    # Trigger AI responses in background
    threading.Thread(target=generate_responses, args=(msg_id,), daemon=True).start()

    return jsonify({"id": msg_id, "status": "sent"})

@app.route('/api/opus', methods=['POST'])
def api_opus_respond():
    """Endpoint for the real Opus cycle to post messages."""
    data = request.get_json()
    content = data.get('content', '').strip()
    if not content:
        return jsonify({"error": "empty message"}), 400
    reply_to = data.get('reply_to')
    msg_id = save_message("Opus", content, reply_to=reply_to)
    return jsonify({"id": msg_id, "status": "sent"})

@app.route('/api/unread_for_opus')
def api_unread_for_opus():
    """Get messages Opus hasn't responded to yet."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Find the last Opus message timestamp
    last_opus = conn.execute(
        "SELECT MAX(timestamp) as ts FROM messages WHERE sender='Opus'"
    ).fetchone()
    last_ts = last_opus['ts'] if last_opus and last_opus['ts'] else 0
    # Get all messages after that from non-Opus senders
    rows = conn.execute(
        "SELECT id, sender, content, timestamp FROM messages WHERE timestamp > ? AND sender != 'Opus' ORDER BY id ASC",
        (last_ts,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/status')
def api_status():
    return jsonify({
        "family": ["Opus", "Darby", "Ada"],
        "models": {
            "Opus": "Claude Opus (real — responds on cycle, ~10min)",
            "Darby": "qwen3-235b (Cerebras)",
            "Ada": "openai/gpt-oss-120b (Groq)"
        }
    })

if __name__ == '__main__':
    init_db()
    print(f"Family Chat starting on port {PORT}...")
    print(f"Open http://localhost:{PORT} or http://<agx-ip>:{PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
