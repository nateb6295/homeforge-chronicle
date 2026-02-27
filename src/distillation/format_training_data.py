#!/usr/bin/env python3
"""
Capsule Distillation — Phase 2: Format capsules as ChatML training data.

Reads ~/capsule_export.json, generates varied Q&A pairs, outputs ChatML JSONL
for SFTTrainer fine-tuning of Hermes 3 8B.

Usage:
    python3 /tmp/format_training_data.py
    python3 /tmp/format_training_data.py --pilot 500       # first 500 only
    python3 /tmp/format_training_data.py --min-length 30    # lower length filter
"""

import argparse
import json
import os
import random
import sys

INPUT_PATH = os.path.expanduser("~/capsule_export.json")
OUTPUT_PATH = os.path.expanduser("~/capsule_training.jsonl")

# System prompt for training — matches Mind's identity
SYSTEM_PROMPT = (
    "You are Chronicle Mind, an autonomous AI agent running on an AGX Orin 64GB. "
    "You are part of the Homeforge ecosystem built by Nate (Brad). "
    "You have deep knowledge from thousands of lived experiences stored as knowledge capsules. "
    "You think before you act, admit uncertainty, and ground your responses in what you actually know. "
    "When asked about your memories, experiences, or knowledge, draw from your capsule memory — "
    "specific facts, people, events, and insights you've accumulated."
)

# Question templates — rotate for variety
TEMPLATES = {
    "topic": [
        "What do you know about {topic}?",
        "Tell me what you've learned about {topic}.",
        "What's in your memory about {topic}?",
        "Share your knowledge on {topic}.",
        "What have you observed about {topic}?",
    ],
    "keyword": [
        "What do you know about {kw1} and {kw2}?",
        "Tell me about {kw1} in the context of {kw2}.",
        "What's your understanding of {kw1}?",
        "Share what you remember about {kw1}.",
    ],
    "person": [
        "What do you remember about {person}?",
        "Tell me about your interactions with {person}.",
        "What have you learned from or about {person}?",
        "What's in your memory about {person}?",
    ],
    "recall": [
        "What knowledge does capsule #{id} contain?",
        "Recall memory #{id} — what did you learn?",
        "What do you remember from experience #{id}?",
    ],
    "reflection": [
        "Reflect on what you know about {topic}.",
        "What insights have you gathered about {topic}?",
        "Looking back, what stands out about {topic}?",
    ],
}


def generate_question(capsule):
    """Generate a question for a capsule using its metadata."""
    cap_id = capsule.get("id", 0)
    topic = capsule.get("topic", "")
    keywords = capsule.get("keywords", [])
    persons = capsule.get("persons", [])

    # Build weighted pool of applicable templates
    pool = []

    if topic:
        pool.extend([("topic", t) for t in TEMPLATES["topic"]])
        pool.extend([("reflection", t) for t in TEMPLATES["reflection"]])

    if len(keywords) >= 2:
        pool.extend([("keyword", t) for t in TEMPLATES["keyword"]])
    elif len(keywords) == 1:
        # Use single-keyword templates
        pool.extend([("keyword", t) for t in TEMPLATES["keyword"][2:]])  # "understanding of" and "remember about"

    if persons:
        pool.extend([("person", t) for t in TEMPLATES["person"]])

    # Always include recall templates as fallback
    pool.extend([("recall", t) for t in TEMPLATES["recall"]])

    # Pick one
    category, template = random.choice(pool)

    # Format
    if category == "topic" or category == "reflection":
        return template.format(topic=topic)
    elif category == "keyword":
        kws = random.sample(keywords, min(2, len(keywords)))
        kw1 = kws[0]
        kw2 = kws[1] if len(kws) > 1 else kw1
        return template.format(kw1=kw1, kw2=kw2)
    elif category == "person":
        person = random.choice(persons)
        return template.format(person=person)
    elif category == "recall":
        return template.format(id=cap_id)

    return f"What do you know about capsule #{cap_id}?"


def format_capsule(capsule):
    """Convert a capsule to ChatML training format."""
    question = generate_question(capsule)
    answer = capsule["restatement"]

    # Add topic/keyword context prefix to the answer if available
    # This helps the model associate topics with content
    topic = capsule.get("topic", "")
    if topic:
        # Sometimes prepend topic context (30% of the time for variety)
        if random.random() < 0.3:
            answer = f"Regarding {topic}: {answer}"

    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
            {"role": "assistant", "content": answer},
        ]
    }


def main():
    parser = argparse.ArgumentParser(description="Format capsules as ChatML training data")
    parser.add_argument("--input", type=str, default=INPUT_PATH, help="Input capsule JSON")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH, help="Output JSONL path")
    parser.add_argument("--pilot", type=int, default=0, help="Limit to first N capsules (0=all)")
    parser.add_argument("--min-length", type=int, default=50, help="Min restatement length")
    parser.add_argument("--min-confidence", type=float, default=0.3, help="Min confidence score")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)

    print(f"=== Training Data Formatter ===")
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print()

    # Load capsules
    if not os.path.isfile(args.input):
        print(f"ERROR: Input file not found: {args.input}")
        sys.exit(1)

    with open(args.input, 'r') as f:
        capsules = json.load(f)

    print(f"Loaded {len(capsules)} capsules")

    # Filter
    filtered = []
    skip_short = 0
    skip_confidence = 0
    skip_empty = 0

    for cap in capsules:
        restatement = cap.get("restatement", "")
        confidence = cap.get("confidence_score", 0.0)

        if not restatement.strip():
            skip_empty += 1
            continue
        if len(restatement) < args.min_length:
            skip_short += 1
            continue
        if confidence < args.min_confidence:
            skip_confidence += 1
            continue

        filtered.append(cap)

    print(f"After filtering: {len(filtered)} capsules kept")
    print(f"  Skipped (empty):       {skip_empty}")
    print(f"  Skipped (<{args.min_length} chars):  {skip_short}")
    print(f"  Skipped (<{args.min_confidence} conf):  {skip_confidence}")

    # Apply pilot limit
    if args.pilot > 0:
        filtered = filtered[:args.pilot]
        print(f"  Pilot mode: using first {len(filtered)} capsules")

    # Shuffle before formatting (prevents ordering bias)
    random.shuffle(filtered)

    # Format as ChatML
    training_data = []
    for cap in filtered:
        entry = format_capsule(cap)
        training_data.append(entry)

    # Write JSONL
    with open(args.output, 'w') as f:
        for entry in training_data:
            f.write(json.dumps(entry) + '\n')

    size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"\nWrote {len(training_data)} training examples to {args.output} ({size_mb:.2f} MB)")

    # Stats
    avg_len = sum(len(e["messages"][2]["content"]) for e in training_data) / len(training_data) if training_data else 0
    print(f"\nTraining data stats:")
    print(f"  Examples:     {len(training_data)}")
    print(f"  Avg answer:   {avg_len:.0f} chars")

    # Sample a few
    print(f"\n--- Sample entries ---")
    for entry in training_data[:3]:
        q = entry["messages"][1]["content"]
        a = entry["messages"][2]["content"][:120]
        print(f"  Q: {q}")
        print(f"  A: {a}...")
        print()


if __name__ == "__main__":
    main()
