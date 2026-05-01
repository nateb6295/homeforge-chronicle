#!/usr/bin/env python3
"""Synthesize a thread's accumulated context into a structured argument brief.

Takes all context entries for a thread and asks Gemma to distill them into:
- Core thesis (what the thread is converging on)
- Supporting evidence (what captures/advances support it)
- Counterexamples or tensions (what challenges it)
- Open questions (what remains unresolved)
- Essay seed (a 2-3 paragraph draft that could become a Nostr post)

Usage:
  thread_digest.py [THREAD_ID]     — digest the active thread (or specified ID)
  thread_digest.py --save           — save digest to drafts/
  thread_digest.py --all            — show all active threads with digest potential
  thread_digest.py --cloud          — use DeepInfra (Qwen3-235B) instead of local Gemma
"""

import sqlite3, os, json, sys, time, requests

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
GEMMA_URL = os.environ.get("GEMMA_URL", "http://localhost:11435")
MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
DRAFTS_DIR = os.path.expanduser("~/chronicle/drafts")

# Cloud inference via chronicle-engine
ENGINE_URL = "http://localhost:11436"

DIGEST_PROMPT = """You are synthesizing a cognitive thread — an ongoing line of inquiry. Below is the thread's question and all context that has accumulated (captures, advances, challenges).

Your job: distill this into a structured argument brief. Be specific. Use the actual content, not vague summaries.

Respond in this exact format:

## Core Thesis
[1-2 sentences: what this thread is converging toward]

## Supporting Evidence
[Bullet points — each one citing a specific capture or advancement]

## Tensions & Counterexamples
[What challenges the thesis? What doesn't fit?]

## Open Questions
[What remains unresolved? What would change the thesis if answered?]

## Essay Seed
[2-3 paragraphs that could become a public essay. Voice: thoughtful, specific, no jargon. Write like someone thinking out loud, not filing a report.]"""


def call_gemma(prompt, max_tokens=2000):
    """Call Gemma via llama-server."""
    try:
        resp = requests.post(
            f"{GEMMA_URL}/v1/chat/completions",
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.7,
            },
            timeout=240,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[Gemma error: {e}]"


def call_cloud(prompt, max_tokens=3000):
    """Call DeepInfra Qwen3-235B via chronicle-engine (Ollama-compatible API)."""
    try:
        resp = requests.post(
            f"{ENGINE_URL}/api/chat",
            json={
                "model": "chronicle-deep",
                "messages": [{"role": "user", "content": prompt}],
                "options": {"num_predict": max_tokens, "temperature": 0.7},
                "stream": False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", str(data))
    except Exception as e:
        return f"[Cloud error: {e}]"


def get_thread(db, thread_id=None):
    """Get a thread by ID or the active one."""
    db.row_factory = sqlite3.Row
    if thread_id:
        return db.execute(
            "SELECT * FROM cognitive_threads WHERE id = ?", (thread_id,)
        ).fetchone()
    return db.execute(
        "SELECT * FROM cognitive_threads WHERE status='active' ORDER BY priority LIMIT 1"
    ).fetchone()


def get_thread_history(db, thread_id, limit=50):
    """Get all history for a thread."""
    return db.execute(
        "SELECT event_type, content, source, created_at FROM thread_history "
        "WHERE thread_id = ? ORDER BY created_at ASC LIMIT ?",
        (thread_id, limit),
    ).fetchall()


def build_context(thread, history):
    """Build the full context string for Gemma."""
    t = dict(thread)
    parts = [f"# Thread #{t['id']}: {t['title']}\n"]
    parts.append(f"**Question:** {t['question']}\n")

    if t.get("context"):
        parts.append(f"**Initial context:** {t['context']}\n")

    parts.append("---\n## Accumulated entries:\n")

    for h in history:
        h = dict(h)
        etype = h["event_type"]
        source = (h["source"] or "unknown").split(":")[0]
        content = h["content"] or ""

        if etype == "context":
            parts.append(f"**[Capture from {source}]** {content[:500]}\n")
        elif etype == "advanced":
            parts.append(f"**[Advancement by {source}]** {content[:800]}\n")
        elif etype == "challenge":
            parts.append(f"**[Challenge from {source}]** {content[:500]}\n")
        else:
            parts.append(f"**[{etype} by {source}]** {content[:400]}\n")

    return "\n".join(parts)


def list_digestable(db):
    """List threads with enough context to digest."""
    threads = db.execute(
        "SELECT id, title, status, priority FROM cognitive_threads "
        "WHERE status IN ('active', 'paused') ORDER BY priority"
    ).fetchall()

    for t in threads:
        t = dict(t)
        count = db.execute(
            "SELECT COUNT(*) FROM thread_history WHERE thread_id = ?", (t["id"],)
        ).fetchone()[0]
        context_count = db.execute(
            "SELECT COUNT(*) FROM thread_history WHERE thread_id = ? AND event_type = 'context'",
            (t["id"],),
        ).fetchone()[0]
        if count >= 3:
            print(
                f"  #{t['id']} [{t['status']}] {t['title']} — {count} entries ({context_count} captures)"
            )


def main():
    save = "--save" in sys.argv
    show_all = "--all" in sys.argv
    use_cloud = "--cloud" in sys.argv
    thread_id = None

    for arg in sys.argv[1:]:
        if arg.isdigit():
            thread_id = int(arg)

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    if show_all:
        print("Threads with digest potential (3+ entries):\n")
        list_digestable(db)
        return

    thread = get_thread(db, thread_id)
    if not thread:
        print("No active thread found.")
        return

    t = dict(thread)
    history = get_thread_history(db, t["id"])

    if len(history) < 3:
        print(f"Thread #{t['id']} has only {len(history)} entries — needs more context to digest.")
        return

    print(f"Digesting Thread #{t['id']}: {t['title']}")
    print(f"  {len(history)} entries to synthesize...")

    context = build_context(thread, history)
    full_prompt = f"{context}\n\n---\n\n{DIGEST_PROMPT}"

    if use_cloud:
        # Cloud can handle more context
        if len(full_prompt) > 16000:
            full_prompt = full_prompt[:16000] + "\n\n[Context truncated for length]\n\n" + DIGEST_PROMPT
        print("  Calling Qwen3-235B (cloud) for synthesis...")
        result = call_cloud(full_prompt)
    else:
        # Truncate for Gemma context — keep under 8k chars for reliable completion
        if len(full_prompt) > 8000:
            full_prompt = full_prompt[:8000] + "\n\n[Context truncated for length]\n\n" + DIGEST_PROMPT
        print("  Calling Gemma for synthesis...")
        result = call_gemma(full_prompt)

    print(f"\n{'='*60}")
    print(result)
    print(f"{'='*60}")

    if save:
        os.makedirs(DRAFTS_DIR, exist_ok=True)
        slug = t["title"].lower().replace(" ", "_")[:40]
        filename = f"thread_{t['id']}_{slug}.md"
        filepath = os.path.join(DRAFTS_DIR, filename)

        with open(filepath, "w") as f:
            f.write(f"# Thread #{t['id']} Digest: {t['title']}\n")
            f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M PDT')}\n\n")
            f.write(result)

        print(f"\nSaved to {filepath}")


if __name__ == "__main__":
    main()
