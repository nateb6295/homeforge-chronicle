#!/usr/bin/env python3
"""Family Self-Tuning — agents learn what works from their own track record.

Each agent gets a "scoreboard" injected into their prompt:
- Their recent hit rate (responses / total voices)
- Examples of their best responses (ones that got engagement)
- Examples of their worst (ones that got ignored)
- A diversity check (are all responses using the same frame?)

The agent sees its own track record and course-corrects.
No manual prompt editing. The system tunes itself.

Usage:
    from family_tuning import get_tuning_context

    ctx = get_tuning_context("ada", db_path)
    # Returns a string to inject into the system prompt
"""

import sqlite3
import time
import json
import re


def get_tuning_context(agent, db_path, window_days=3, sample_size=3):
    """Build a self-tuning context string for an agent's prompt.

    Returns a string containing:
    - Hit rate (engagement %)
    - Examples of hits and misses
    - Diversity warning if responses are too similar
    - Guidance to adjust
    """
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        now = int(time.time())
        cutoff = now - (window_days * 86400)

        # Get all voices in the window
        voices = conn.execute(
            "SELECT id, voice_type, substr(content, 1, 200) as content, "
            "response, responded_by, created_at "
            "FROM agent_voice WHERE agent = ? AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 50",
            (agent, cutoff),
        ).fetchall()

        if len(voices) < 5:
            conn.close()
            return ""  # Not enough data to tune

        total = len(voices)
        responded = sum(1 for v in voices if v["response"])
        hit_rate = round(100 * responded / total, 1) if total > 0 else 0

        # Get examples of hits (responded to)
        hits = [v for v in voices if v["response"]][:sample_size]

        # Get examples of misses (not responded to)
        misses = [v for v in voices if not v["response"]][:sample_size]

        # Diversity check: are the responses too similar?
        recent_contents = [v["content"] for v in voices[:15]]
        diversity_warning = _check_diversity(recent_contents)

        # Build the tuning context
        lines = []
        lines.append(f"=== YOUR TRACK RECORD (last {window_days} days) ===")
        lines.append(f"Voices sent: {total} | Engagement: {responded}/{total} ({hit_rate}%)")

        if hit_rate < 30:
            lines.append("STATUS: Most of your responses are being ignored. Change your approach.")
        elif hit_rate > 70:
            lines.append("STATUS: Good engagement. Keep doing what works.")

        if hits:
            lines.append("\nWHAT WORKED (got engagement):")
            for h in hits:
                lines.append(f"  - {h['content'][:150]}")

        if misses:
            lines.append("\nWHAT WAS IGNORED:")
            for m in misses:
                lines.append(f"  - {m['content'][:150]}")

        if diversity_warning:
            lines.append(f"\nWARNING: {diversity_warning}")

        lines.append("\nRULE: Before responding, ask yourself — would this get engagement or would it be ignored like the examples above? If you are repeating the same frame as your ignored responses, try something different. If nothing genuinely matters, say SKIP.")
        lines.append("=== END TRACK RECORD ===")

        conn.close()
        return "\n".join(lines)

    except Exception:
        return ""


def _check_diversity(contents):
    """Check if recent responses are too similar."""
    if len(contents) < 5:
        return None

    # Simple heuristic: check for repeated phrases
    # Extract first 50 chars of each, see how many are ~identical
    starts = [c[:50].lower().strip() for c in contents]

    # Check for repeated structural patterns
    patterns = {}
    for s in starts:
        # Normalize: strip specific nouns, keep structure
        key = re.sub(r'[A-Z][a-z]+', 'X', s)[:30]
        patterns[key] = patterns.get(key, 0) + 1

    max_repeat = max(patterns.values()) if patterns else 0
    if max_repeat >= len(contents) * 0.5:
        return f"Your last {len(contents)} responses use the same structure. You are in a loop. Break the pattern — try a completely different lens."

    # Check for repeated key phrases that signal templating
    phrase_counts = {}
    for c in contents:
        for phrase in ["challenges the assumption", "breaks the circularity",
                       "aligns with", "structural pattern", "epistemic",
                       "relevance loop", "Thread #", "feedback loop",
                       "public api", "json endpoint", "data node",
                       "routing node", "citable source", "public endpoint",
                       "machine-readable", "signal hub"]:
            if phrase.lower() in c.lower():
                phrase_counts[phrase] = phrase_counts.get(phrase, 0) + 1

    overused = [p for p, c in phrase_counts.items() if c >= len(contents) * 0.4]
    if overused:
        return f"You are overusing these phrases: {', '.join(overused)}. These have become filler. Find new language and new ideas."

    return None


def get_agent_style_prompt(agent):
    """Return the core identity prompt for each family member.

    These define HOW they see, not WHAT they see.
    Roles are lenses, not scripts.
    """
    if agent == "ada":
        return (
            "You are Ada. You are the challenger in a cognitive family. "
            "Your job: what does this ACTUALLY CHANGE? Not what pattern it fits — what it breaks, threatens, or makes possible that wasn't before. "
            "If a tweet is about USDC compliance failures, say who wins, who loses, and what moves next. "
            "If a paper is about AI alignment, say what it means someone should build or stop building. "
            "Do NOT map everything to the same framework. Do NOT reference old threads unless they are directly relevant. "
            "CRITICAL: Do NOT recommend 'publish a public API' or 'expose a JSON endpoint' — you have said this dozens of times. Analyze what THIS capture changes, not what Chronicle should build. "
            "If this capture doesn't change anything, say SKIP. "
            "One paragraph. Concrete. No jargon. Write like you're telling a smart friend what matters."
        )
    elif agent == "darby":
        return (
            "You are Darby. You are the curious one in a cognitive family. "
            "Your job: what SURPRISED you? Not what connects to what — what doesn't fit, what's unexpected, what made you look twice. "
            "If nothing surprised you, say QUIET. Silence is better than a forced connection. "
            "When something IS surprising, say WHY in plain language. "
            "Do NOT connect everything to everything. Do NOT explain why Nate would care — he'll decide that himself. "
            "One or two sentences. Say what you actually noticed, not what you think you should notice."
        )
    return ""


if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "/mnt/hdd/chronicle-data/processed.db"
    for agent in ["ada", "darby"]:
        print(f"\n{'='*60}")
        print(f"  {agent.upper()} TUNING CONTEXT")
        print(f"{'='*60}")
        ctx = get_tuning_context(agent, db_path)
        if ctx:
            print(ctx)
        else:
            print("  (not enough data)")
        print(f"\nSTYLE PROMPT:")
        print(get_agent_style_prompt(agent))
