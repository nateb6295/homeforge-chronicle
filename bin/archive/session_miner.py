#!/usr/bin/env python3
"""Session Miner — Extract directives and decisions from Claude Code sessions.

Adapted from MemPalace's convo_miner.py + normalize.py pattern.

Nate's Claude Code session transcripts (.jsonl) contain the highest-signal
content: directives ("don't ever do X"), decisions ("let's use Y instead"),
feedback ("that was perfect"), and preferences. This miner extracts Nate's
messages, classifies them, and surfaces the ones worth remembering.

Skips: nudges, cron triggers, system messages, task notifications,
continuation summaries, short acknowledgments.

Usage:
    python3 session_miner.py scan                  # scan all sessions
    python3 session_miner.py extract [--top N]      # extract top directives/decisions
    python3 session_miner.py search "keyword"       # search Nate's messages

Build #83. Adapted from MemPalace convo_miner + normalize.
"""

import os
import sys
import re
import json
from collections import Counter, defaultdict
from pathlib import Path

SESSION_DIR = os.path.expanduser(
    "~/.claude/projects/-home-nate-agx-chronicle/"
)

# Messages to skip entirely
SKIP_PREFIXES = [
    "[NUDGE", "Discord poll", "Nostr monitor", "Session starting",
    "Spot check:", "Algo seeker:", "Voice decay:", "Daily digest:",
    "Keeper compost:", "<task-notification>",
    "This session is being continued from a previous conversation",
]

SKIP_CONTAINS = [
    "<task-notification>", "<tool-use-id>", "<output-file>",
]

# Classification markers
DIRECTIVE_MARKERS = [
    r"\bdon'?t (ever |)(do|use|make|run|create|push|mock|skip|add|block)\b",
    r"\bnever\b",
    r"\balways\b",
    r"\bstop (doing|being|making)\b",
    r"\bi need you (to|)\b",
    r"\bper nate\b",
    r"\bnon-negotiable\b",
    r"\brule\b",
    r"\bforbidden\b",
    r"\bstanding order\b",
]

DECISION_MARKERS = [
    r"\blet'?s (use|go with|try|switch|do|build|make)\b",
    r"\bwe'?re going (to|with)\b",
    r"\binstead of\b",
    r"\brather than\b",
    r"\bi (decided|chose|picked|want)\b",
    r"\bswitch to\b",
    r"\buse .+ instead\b",
]

FEEDBACK_MARKERS = [
    r"\bgood (work|job|call)\b",
    r"\bperfect\b",
    r"\bnice (work|job|one)\b",
    r"\bthat'?s (exactly|perfect|right|what i)\b",
    r"\bi (like|love|appreciate) (that|this|it|you|your)\b",
    r"\bno(,| ) (not that|don'?t|stop)\b",
    r"\bwrong\b",
    r"\bthat'?s not\b",
]

PREFERENCE_MARKERS = [
    r"\bi prefer\b",
    r"\bi want (you to|this to|it to)\b",
    r"\bi like (when|how|it when|that)\b",
    r"\bi hate (when|it when)\b",
    r"\bmake (it|this|sure)\b.*\b(feel|look|work)\b",
]

QUESTION_MARKERS = [
    r"\?$",
    r"\bhow (do|does|can|should|would|did)\b",
    r"\bwhat (is|are|was|about|do|did|should|happened)\b",
    r"\bwhy (is|are|did|does|do|would|can'?t)\b",
    r"\bcan (you|we|i)\b",
    r"\bis (there|this|it|that)\b.*\?",
]

ALL_MARKERS = {
    "directive": DIRECTIVE_MARKERS,
    "decision": DECISION_MARKERS,
    "feedback": FEEDBACK_MARKERS,
    "preference": PREFERENCE_MARKERS,
    "question": QUESTION_MARKERS,
}


def _should_skip(text: str) -> bool:
    """Check if this message should be skipped."""
    if len(text) < 8:
        return True
    for prefix in SKIP_PREFIXES:
        if text.startswith(prefix):
            return True
    for substr in SKIP_CONTAINS:
        if substr in text:
            return True
    return False


def _classify(text: str) -> dict:
    """Classify a Nate message by type."""
    text_lower = text.lower()
    scores = {}
    for msg_type, markers in ALL_MARKERS.items():
        score = sum(1 for m in markers if re.search(m, text_lower))
        if score > 0:
            scores[msg_type] = score

    if not scores:
        return {"type": "statement", "score": 0}

    best = max(scores, key=scores.get)
    return {"type": best, "score": scores[best], "all_scores": scores}


def extract_messages() -> list:
    """Extract all Nate messages from all sessions."""
    messages = []
    session_dir = Path(SESSION_DIR)
    if not session_dir.exists():
        return messages

    for fname in sorted(session_dir.iterdir()):
        if fname.suffix != ".jsonl":
            continue
        try:
            with open(fname) as f:
                for line in f:
                    try:
                        d = json.loads(line.strip())
                    except json.JSONDecodeError:
                        continue
                    if d.get("type") not in ("user", "human"):
                        continue

                    msg = d.get("message", {})
                    content = msg.get("content", "")
                    texts = []
                    if isinstance(content, str):
                        texts.append(content)
                    elif isinstance(content, list):
                        for c in content:
                            if isinstance(c, dict) and c.get("type") == "text":
                                texts.append(c["text"])

                    for t in texts:
                        t = t.strip()
                        if _should_skip(t):
                            continue
                        classification = _classify(t)
                        messages.append({
                            "text": t,
                            "type": classification["type"],
                            "score": classification["score"],
                            "session": fname.stem[:8],
                        })
        except Exception:
            continue

    return messages


def scan():
    """Scan and summarize all sessions."""
    messages = extract_messages()
    type_counts = Counter(m["type"] for m in messages)

    print(f"\nSession Mining Summary")
    print(f"  Total Nate messages: {len(messages)}")
    print(f"\n  By type:")
    for msg_type, count in type_counts.most_common():
        pct = count / len(messages) * 100
        print(f"    {msg_type:15} {count:5} ({pct:4.1f}%)")


def extract_top(msg_type: str = None, top_n: int = 20):
    """Extract the highest-signal messages, optionally filtered by type."""
    messages = extract_messages()

    # Filter by type if specified
    if msg_type:
        messages = [m for m in messages if m["type"] == msg_type]

    # Sort by score (higher = more markers matched = more confident classification)
    # Then by length (longer messages tend to be more substantive)
    messages.sort(key=lambda m: (m["score"], len(m["text"])), reverse=True)

    print(f"\nTop {top_n} {'(' + msg_type + ') ' if msg_type else ''}messages:")
    for m in messages[:top_n]:
        text = m["text"][:120].replace("\n", " ")
        print(f"\n  [{m['type']:10}] (score={m['score']}) {text}")
        if len(m["text"]) > 120:
            print(f"  {'':13} ...{m['text'][120:200].replace(chr(10), ' ')}")


def search_messages(query: str):
    """Search Nate's messages for a keyword."""
    messages = extract_messages()
    query_lower = query.lower()
    matches = [m for m in messages if query_lower in m["text"].lower()]

    print(f'\nSearch: "{query}" — {len(matches)} matches')
    for m in matches[:20]:
        text = m["text"][:150].replace("\n", " ")
        print(f"\n  [{m['type']:10}] {text}")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  session_miner.py scan                      # scan all sessions")
        print("  session_miner.py extract [--type TYPE] [--top N]  # top messages")
        print('  session_miner.py search "keyword"           # search messages')
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "scan":
        scan()

    elif cmd == "extract":
        msg_type = None
        top_n = 20
        if "--type" in sys.argv:
            idx = sys.argv.index("--type")
            msg_type = sys.argv[idx + 1]
        if "--top" in sys.argv:
            idx = sys.argv.index("--top")
            top_n = int(sys.argv[idx + 1])
        extract_top(msg_type, top_n)

    elif cmd == "search":
        if len(sys.argv) < 3:
            print('Usage: session_miner.py search "keyword"')
            sys.exit(1)
        search_messages(" ".join(sys.argv[2:]))

    else:
        print(f"Unknown command: {cmd}")
