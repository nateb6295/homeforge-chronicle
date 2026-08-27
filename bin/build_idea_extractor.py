#!/usr/bin/env python3
"""Extract actionable build ideas from capture analyses.

Scans text for phrases like "could we", "what if we built", "infrastructure
question", "should build", etc. Saves to a persistent log with source context.

Usage:
    # Extract from text (pipe or argument):
    echo "analysis text" | python3 build_idea_extractor.py --source "tweet:12345"
    python3 build_idea_extractor.py "analysis text" --source "tweet:12345"

    # View saved ideas:
    python3 build_idea_extractor.py --list
    python3 build_idea_extractor.py --list --unbuilt    # only unactioned

    # Mark an idea as built:
    python3 build_idea_extractor.py --built 3           # by line number

Called automatically by capture_tracker.py after posting analyses.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

IDEAS_FILE = os.path.expanduser("~/chronicle/data/build_ideas.jsonl")

# Patterns that signal a build idea
IDEA_PATTERNS = [
    r'(?:could (?:we|I)|can we)\s+(?:build|generate|create|run|test|write|add|make|deploy|set up)',
    r'(?:what if (?:we|I))\s+(?:built|made|created|ran|tested|added|generated)',
    r'infrastructure question:?\s*',
    r'(?:should|could|might) (?:build|create|add|write|deploy|run)\b',
    r'(?:worth (?:building|trying|testing|running|creating))\b',
    r'(?:concrete (?:build|infrastructure|tool|script))\b',
    r'(?:build idea|tool idea|experiment idea|probe idea):?\s*',
    r'(?:I want to|we need to|next step:?)\s+(?:build|create|write|add|test|run)',
]

COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in IDEA_PATTERNS]


def extract_ideas(text, source=""):
    """Extract build ideas from text. Returns list of idea dicts."""
    ideas = []
    sentences = re.split(r'(?<=[.!?])\s+|\n', text)

    for i, sentence in enumerate(sentences):
        sentence = sentence.strip()
        if not sentence or len(sentence) < 20:
            continue

        for pattern in COMPILED_PATTERNS:
            match = pattern.search(sentence)
            if match:
                # Get surrounding context (current sentence + next if short)
                context = sentence
                if i + 1 < len(sentences) and len(context) < 100:
                    context += " " + sentences[i + 1].strip()

                # Clean up the idea text
                idea_text = context.strip()
                if len(idea_text) > 300:
                    idea_text = idea_text[:297] + "..."

                ideas.append({
                    "idea": idea_text,
                    "trigger": match.group(0).strip(),
                    "source": source,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "status": "unbuilt",
                })
                break  # one idea per sentence

    return ideas


def save_ideas(ideas):
    """Append ideas to the persistent log."""
    if not ideas:
        return 0

    os.makedirs(os.path.dirname(IDEAS_FILE), exist_ok=True)

    # Dedup against existing ideas
    existing = set()
    if os.path.exists(IDEAS_FILE):
        with open(IDEAS_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    existing.add(entry.get("idea", "")[:80])
                except (json.JSONDecodeError, KeyError):
                    pass

    saved = 0
    with open(IDEAS_FILE, "a") as f:
        for idea in ideas:
            if idea["idea"][:80] not in existing:
                f.write(json.dumps(idea) + "\n")
                existing.add(idea["idea"][:80])
                saved += 1

    return saved


def list_ideas(unbuilt_only=False):
    """Print saved ideas."""
    if not os.path.exists(IDEAS_FILE):
        print("No build ideas saved yet.")
        return

    ideas = []
    with open(IDEAS_FILE) as f:
        for line in f:
            try:
                ideas.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    if unbuilt_only:
        ideas = [i for i in ideas if i.get("status") != "built"]

    if not ideas:
        print("No unbuilt ideas." if unbuilt_only else "No ideas saved.")
        return

    print(f"{'Unbuilt b' if unbuilt_only else 'B'}uild ideas ({len(ideas)}):\n")
    for idx, idea in enumerate(ideas, 1):
        status = idea.get("status", "unbuilt")
        marker = "[ ]" if status == "unbuilt" else "[x]"
        source = idea.get("source", "")
        date = idea.get("extracted_at", "")[:10]
        print(f"  {idx}. {marker} [{date}] {idea['idea'][:120]}")
        if source:
            print(f"     Source: {source}")
        print()


def mark_built(line_num):
    """Mark an idea as built by line number."""
    if not os.path.exists(IDEAS_FILE):
        print("No ideas file.")
        return

    with open(IDEAS_FILE) as f:
        lines = f.readlines()

    if line_num < 1 or line_num > len(lines):
        print(f"Invalid line number {line_num} (1-{len(lines)})")
        return

    try:
        entry = json.loads(lines[line_num - 1])
        entry["status"] = "built"
        entry["built_at"] = datetime.now(timezone.utc).isoformat()
        lines[line_num - 1] = json.dumps(entry) + "\n"

        with open(IDEAS_FILE, "w") as f:
            f.writelines(lines)

        print(f"Marked as built: {entry['idea'][:80]}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extract build ideas from capture analyses")
    parser.add_argument("text", nargs="?", help="Text to extract from")
    parser.add_argument("--source", default="", help="Source identifier (e.g., tweet:12345)")
    parser.add_argument("--list", action="store_true", help="List saved ideas")
    parser.add_argument("--unbuilt", action="store_true", help="Show only unbuilt ideas")
    parser.add_argument("--built", type=int, help="Mark idea N as built")
    args = parser.parse_args()

    if args.list or args.unbuilt:
        list_ideas(unbuilt_only=args.unbuilt)
        return

    if args.built:
        mark_built(args.built)
        return

    # Get text from argument or stdin
    text = args.text
    if not text and not sys.stdin.isatty():
        text = sys.stdin.read()

    if not text:
        print("No text provided. Use --list to view saved ideas.")
        return

    ideas = extract_ideas(text, source=args.source)
    if ideas:
        saved = save_ideas(ideas)
        for idea in ideas:
            print(f"  BUILD IDEA: {idea['idea'][:120]}")
        if saved:
            print(f"\n{saved} new idea(s) saved to {IDEAS_FILE}")
    else:
        print("No build ideas detected.")


if __name__ == "__main__":
    main()
