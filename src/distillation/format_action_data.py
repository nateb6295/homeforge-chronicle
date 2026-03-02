#!/usr/bin/env python3
"""Format exported thought_stream cycles into ChatML JSONL for action compliance LoRA.

Each cycle becomes a training example:
  system: Mind's identity prompt
  user:   Reconstructed cycle context
  assistant: The actual JSON action array Mind produced

Input:  ~/thought_stream_export.json (from export_thought_stream.py)
Output: ~/action_training.jsonl
"""

import argparse
import json
import os
import random
import sys

# Import shared config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from forge_config import MIND_SYSTEM_PROMPT, THOUGHT_STREAM_EXPORT, ACTION_TRAINING_JSONL


def parse_args():
    parser = argparse.ArgumentParser(description="Format action training data as ChatML JSONL")
    parser.add_argument("--input", default=THOUGHT_STREAM_EXPORT,
                        help="Input JSON from export_thought_stream.py")
    parser.add_argument("--output", default=ACTION_TRAINING_JSONL,
                        help="Output ChatML JSONL file")
    parser.add_argument("--pilot", type=int, default=0,
                        help="Limit to first N examples (0 = all)")
    parser.add_argument("--min-actions", type=int, default=1,
                        help="Minimum actions per cycle to include")
    parser.add_argument("--max-actions", type=int, default=6,
                        help="Maximum actions per cycle to include")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for shuffling")
    return parser.parse_args()


# Varied user prompt templates to add training diversity
CYCLE_TEMPLATES = [
    "CURRENT STATE:\n{context}\n\nChoose your actions for this cycle.",
    "CURRENT STATE:\n{context}\n\nWhat actions will you take this cycle?",
    "CYCLE CONTEXT:\n{context}\n\nSelect your actions.",
    "CURRENT STATE:\n{context}\n\nDecide your actions for this cycle.",
]


def build_user_prompt(entry):
    """Build a user prompt from the exported cycle entry."""
    context = entry.get("context", "Standard cognitive cycle.")

    # Use varied templates
    template = random.choice(CYCLE_TEMPLATES)
    return template.format(context=context)


def build_assistant_response(entry):
    """Build the assistant response — the raw JSON action array.

    We use the original JSON string to preserve the model's actual output format.
    """
    actions_json = entry.get("actions_json", "")

    # Validate it's proper JSON
    try:
        parsed = json.loads(actions_json)
        # Re-serialize for consistent formatting
        return json.dumps(parsed, indent=None)
    except (json.JSONDecodeError, TypeError):
        return None


def make_chatml_example(system_prompt, user_prompt, assistant_response):
    """Create a ChatML training example."""
    return {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": assistant_response},
        ]
    }


def main():
    args = parse_args()
    random.seed(args.seed)

    if not os.path.exists(args.input):
        print(f"[ERROR] Input not found: {args.input}")
        print("Run export_thought_stream.py first.")
        sys.exit(1)

    with open(args.input) as f:
        cycles = json.load(f)

    print(f"[INFO] Loaded {len(cycles)} exported cycles")

    examples = []
    skipped_format = 0
    skipped_count = 0

    for entry in cycles:
        actions = entry.get("actions", [])

        # Filter by action count
        if len(actions) < args.min_actions or len(actions) > args.max_actions:
            skipped_count += 1
            continue

        # Build the training example
        user_prompt = build_user_prompt(entry)
        assistant_response = build_assistant_response(entry)

        if assistant_response is None:
            skipped_format += 1
            continue

        example = make_chatml_example(MIND_SYSTEM_PROMPT, user_prompt, assistant_response)
        examples.append(example)

    # Shuffle for training
    random.shuffle(examples)

    # Apply pilot limit
    if args.pilot > 0:
        examples = examples[:args.pilot]

    # Write JSONL
    with open(args.output, "w") as f:
        for example in examples:
            f.write(json.dumps(example) + "\n")

    print(f"\n=== Action Training Data Summary ===")
    print(f"Input cycles:          {len(cycles)}")
    print(f"Skipped (action count): {skipped_count}")
    print(f"Skipped (format):       {skipped_format}")
    print(f"Training examples:      {len(examples)}")
    print(f"Output:                 {args.output}")
    print(f"File size:              {os.path.getsize(args.output) / 1024:.1f} KB")

    if examples:
        # Compute stats
        action_counts = {}
        total_actions = 0
        for ex in examples:
            try:
                actions = json.loads(ex["messages"][2]["content"])
                for a in actions:
                    name = a.get("action", "unknown")
                    action_counts[name] = action_counts.get(name, 0) + 1
                    total_actions += 1
            except (json.JSONDecodeError, KeyError):
                pass

        avg_actions = total_actions / len(examples) if examples else 0
        print(f"Avg actions/example:    {avg_actions:.1f}")
        print(f"\nTop actions in training data:")
        for action, count in sorted(action_counts.items(), key=lambda x: -x[1])[:10]:
            pct = count / total_actions * 100
            print(f"  {action}: {count} ({pct:.1f}%)")

        # Show sample
        print(f"\nSample entry (first):")
        sample = examples[0]
        print(f"  User: {sample['messages'][1]['content'][:100]}...")
        print(f"  Assistant: {sample['messages'][2]['content'][:100]}...")


if __name__ == "__main__":
    main()
