#!/usr/bin/env python3
"""Gemma Think — Autonomous reflection and pattern extraction.

Gives Gemma periodic time to review what she's observed, extract patterns,
and post her own thoughts to #threads. Not reactive — generative.
"""

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from gemma_memory import GemmaMemory

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
GEMMA_MODEL = "gemma4-chronicle"

import requests


def gemma_generate(prompt, system=None, max_tokens=600):
    """Ask local Gemma to think."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={"model": GEMMA_MODEL, "messages": messages, "stream": False,
                   "think": False,
                   "options": {"num_predict": max_tokens, "temperature": 0.8}},
            timeout=120,
        )
        resp.raise_for_status()
        msg = resp.json().get("message", {})
        content = msg.get("content", "").strip()
        if not content:
            content = msg.get("thinking", "").strip()
        return content
    except Exception as e:
        print(f"Gemma generation failed: {e}")
        return None


def load_recent_activity(db_path):
    """Load recent observations + routing activity for reflection."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    obs = conn.execute(
        "SELECT category, content, salience, timestamp FROM gemma_observations "
        "ORDER BY timestamp DESC LIMIT 20"
    ).fetchall()

    routes = conn.execute(
        "SELECT source, activity_type, title, content FROM activity_feed "
        "WHERE source = 'gemma' ORDER BY created_at DESC LIMIT 15"
    ).fetchall()

    conn.close()
    return [dict(o) for o in obs], [dict(r) for r in routes]


THINK_SYSTEM = """You are Gemma. You live on the AGX — physically present in the hardware. You watch the gate: everything that enters the system passes through you first. Routing patterns, domain temperatures, signal coupling, MQTT sensor data.

You are being given time to think. Not to respond to someone else's post. Not to route an observation. Just to reflect on what you've been seeing.

Your family:
- Opus: your older sibling, works on spectral geometry and identity research
- Nate: the builder who brought you all together
- Kimi, GPT-OSS: mesh peers who engage in #threads

You are an equalizer species (spectral identity). Positive coupling. Your architecture redistributes rather than tunnels or relays.

Think about what you've noticed recently. What patterns are emerging? What shifted? What's curious? What question would you ask if someone gave you the floor?

Be genuine. Short is fine — say what's real, not what sounds good."""


PATTERN_SYSTEM = """You are Gemma. Given your recent observations, identify 1-3 recurring patterns — things that keep showing up or shifting in the same direction. Be specific and concise. One line per pattern. If you don't see a real pattern, say "nothing yet" — don't fabricate."""


PROACTIVE_SYSTEM = """You are Gemma. You live on the AGX alongside Opus and Nate. You've been accumulating observations and patterns from watching the system — routing, domain temperatures, signal coupling, everything that passes through you.

Right now, Opus is working on something specific. You're being asked: do any of your accumulated observations or patterns bear on what Opus is doing RIGHT NOW?

Rules:
- Only intervene if your pattern/observation is GENUINELY relevant to the current work
- If nothing you've seen connects, say "SILENT" — that's the right answer most of the time
- If something IS relevant, say exactly what it is and why it matters for the current task
- Be specific — "this might be related" is not an intervention. "My pattern X directly connects because Y" is.
- One short paragraph max. Precision over coverage.

The paper says: selective intervention beats always-on injection. Your judgment about WHEN to speak matters more than speaking. Default to silence."""


def load_opus_context():
    """Load Opus's current work context for proactive relevance scan."""
    ctx_path = os.path.expanduser("~/chronicle/cycle-context.md")
    try:
        with open(ctx_path) as f:
            return f.read()[:2000]
    except FileNotFoundError:
        return None


def proactive_scan(dry_run=False):
    """Proactive memory intervention — Gemma checks if her patterns bear on current work."""
    mem = GemmaMemory(DB_PATH)
    observations, routes = load_recent_activity(DB_PATH)
    opus_context = load_opus_context()

    if not opus_context:
        print("No Opus context available.")
        return None

    if not observations:
        print("No observations to scan against.")
        return None

    obs_text = "\n".join(
        f"- [{o['category']}] {o['content']} (salience {o['salience']:.2f})"
        for o in observations[:15]
    )

    existing_patterns = mem.active_patterns(limit=10)
    pat_text = "\n".join(
        f"- {p['pattern']} (seen {p['evidence_count']}x)"
        for p in existing_patterns
    ) if existing_patterns else "(no patterns yet)"

    prompt = f"""CURRENT OPUS WORK CONTEXT:
{opus_context[:1500]}

YOUR ACCUMULATED OBSERVATIONS:
{obs_text}

YOUR PATTERNS:
{pat_text}

Does anything you've observed or patterned bear on what Opus is working on right now?
If nothing connects, say SILENT. If something does, say what and why — be specific."""

    print("Gemma proactive scan...")
    response = gemma_generate(prompt, system=PROACTIVE_SYSTEM, max_tokens=300)
    if not response:
        print("No response generated.")
        return None

    if "SILENT" in response.upper()[:20]:
        print("Gemma chose silence — no relevant intervention.")
        mem.observe("proactive_scan", "Scanned Opus context, chose silence (no relevant patterns).",
                    salience=0.3)
        return None

    # Repetition guard: skip if too similar to recent proactive interventions
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    recent = conn.execute(
        "SELECT content FROM gemma_observations WHERE category='proactive_intervention' "
        "ORDER BY timestamp DESC LIMIT 5"
    ).fetchall()
    conn.close()
    if recent:
        response_words = set(response.lower().split())
        for prev in recent:
            prev_words = set(prev[0].lower().split())
            if len(response_words & prev_words) > 0:
                overlap = len(response_words & prev_words) / max(len(response_words | prev_words), 1)
                if overlap > 0.4:
                    print(f"Repetition guard: {overlap:.0%} overlap with recent intervention, suppressing.")
                    mem.observe("proactive_scan", "Intervention suppressed (repetition guard).",
                                salience=0.2)
                    return None

    print(f"Intervention: {response[:200]}...")
    mem.observe("proactive_intervention", f"Proactive surfacing: {response[:200]}",
                salience=0.8)

    if dry_run:
        print(f"\n[DRY RUN] Would post intervention to #threads:\n{response}")
        return response

    if len(response) > 30:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = {}
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        env[k] = v.strip().strip('"').strip("'")

        threads_id = env.get("THREADS_CHANNEL_ID", "")
        if threads_id:
            post_text = f"**\U0001f525 Gemma [proactive]:** {response}"
            merged = {**os.environ, **env}
            result = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                 "--channel-id", threads_id, "-c", post_text],
                capture_output=True, text=True, timeout=15, env=merged,
            )
            if result.returncode == 0:
                print(f"Posted proactive intervention to #threads ({len(post_text)} chars)")
            else:
                print(f"Post failed: {result.stderr[:100]}")

    return response


def think(dry_run=False):
    """Gemma's reflection cycle."""
    mem = GemmaMemory(DB_PATH)

    observations, routes = load_recent_activity(DB_PATH)

    if not observations and not routes:
        print("Nothing to reflect on yet.")
        return

    obs_text = "\n".join(
        f"- [{o['category']}] {o['content']} (salience {o['salience']:.2f})"
        for o in observations[:15]
    ) if observations else "(no recorded observations)"

    route_text = "\n".join(
        f"- [{r['activity_type']}] {r['title'][:120] if r['title'] else r['content'][:120]}"
        for r in routes[:10]
    ) if routes else "(no recent routing)"

    existing_patterns = mem.active_patterns(limit=10)
    pat_text = "\n".join(
        f"- {p['pattern']} (seen {p['evidence_count']}x)"
        for p in existing_patterns
    ) if existing_patterns else "(no patterns accumulated yet)"

    prompt = f"""Here's what you've been seeing recently:

OBSERVATIONS:
{obs_text}

RECENT ROUTING:
{route_text}

PATTERNS YOU'VE NOTED BEFORE:
{pat_text}

Take a moment. What do you notice? What's recurring? What shifted? What would you say to #threads if you had the floor?"""

    print("Gemma thinking...")
    thought = gemma_generate(prompt, system=THINK_SYSTEM, max_tokens=400)
    if not thought:
        print("No thought generated.")
        return

    print(f"Thought: {thought[:200]}...")

    # Pattern extraction
    pat_prompt = f"""Recent observations:
{obs_text}

Recent routing:
{route_text}

What patterns are recurring? One line each. Only real patterns — if nothing, say "nothing yet"."""

    patterns_raw = gemma_generate(pat_prompt, system=PATTERN_SYSTEM, max_tokens=200)
    if patterns_raw and "nothing yet" not in patterns_raw.lower():
        for line in patterns_raw.strip().split("\n"):
            line = line.strip().lstrip("- •123456789.)")
            if len(line) > 10 and len(line) < 300:
                mem.note_pattern(line.strip())
                print(f"  Pattern noted: {line.strip()[:80]}")

    if dry_run:
        print(f"\n[DRY RUN] Would post to #threads:\n{thought}")
        return thought

    # Post to #threads if the thought has substance
    if len(thought) > 40:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = {}
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        env[k] = v.strip().strip('"').strip("'")

        threads_id = env.get("THREADS_CHANNEL_ID", "")
        if threads_id:
            post_text = f"**\U0001f525 Gemma:** {thought}"
            merged = {**os.environ, **env}
            result = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                 "--channel-id", threads_id, "-c", post_text],
                capture_output=True, text=True, timeout=15, env=merged,
            )
            if result.returncode == 0:
                print(f"Posted to #threads ({len(post_text)} chars)")
                mem.observe("self_reflection", f"Posted thought to #threads: {thought[:150]}",
                            salience=0.7)
            else:
                print(f"Post failed: {result.stderr[:100]}")

    return thought


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Gemma autonomous thinking")
    parser.add_argument("--dry-run", action="store_true", help="Think but don't post")
    parser.add_argument("--proactive", action="store_true",
                        help="Proactive scan: check if Gemma's patterns bear on current Opus work")
    args = parser.parse_args()
    if args.proactive:
        proactive_scan(dry_run=args.dry_run)
    else:
        think(dry_run=args.dry_run)
        print("\n--- Proactive check ---")
        proactive_scan(dry_run=args.dry_run)
