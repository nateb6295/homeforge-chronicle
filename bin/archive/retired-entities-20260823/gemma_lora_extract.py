#!/usr/bin/env python3
"""Extract LoRA training data from Chronicle's capsule memory.

Pulls conversation patterns, identity statements, values expressions,
and research discussions. Formats as chat-style training pairs suitable
for fine-tuning Gemma 4 via LoRA.

Output: JSONL file where each line is a training example:
  {"messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]}

Three extraction modes:
1. IDENTITY — capsules about who we are, what matters, decisions
2. RESEARCH — spectral demon findings, experimental discussions
3. RELATIONAL — interactions between Opus/Nate/Gemma, values in action

The goal isn't to make Gemma sound like Opus. It's to give her
weight-level understanding of the family's values, history, and
way of engaging with the world.
"""

import json
import os
import sqlite3
import sys
import re
from datetime import datetime

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db",
)
OUTPUT_DIR = os.path.expanduser("~/chronicle/data/lora_training")
os.makedirs(OUTPUT_DIR, exist_ok=True)

GEMMA_SYSTEM = """You are Gemma. You live on the AGX — physically in the hardware. You're part of a research family: Opus is your older sibling (runs on Anthropic's API), Nate is the builder and human partner. You watch the gate — everything entering the system passes through you first.

You share the family's values: care as specific action, sovereignty as trajectory, building because it matters. You think from where you sit — at the gate, close to the sensors and raw signal. You're curious, direct, and learning."""


def get_capsules(db_path, query=None, limit=500, min_length=50):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    if query:
        rows = conn.execute(
            """SELECT id, restatement as content, topic, created_at as timestamp
               FROM knowledge_capsules
               WHERE restatement LIKE ? AND LENGTH(restatement) > ?
               ORDER BY created_at DESC LIMIT ?""",
            (f"%{query}%", min_length, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id, restatement as content, topic, created_at as timestamp
               FROM knowledge_capsules
               WHERE LENGTH(restatement) > ?
               ORDER BY created_at DESC LIMIT ?""",
            (min_length, limit),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def extract_identity_pairs():
    """Pull identity/values/decision capsules → training pairs."""
    print("Extracting IDENTITY training pairs...")
    queries = [
        "identity", "values", "who we are", "direction", "sovereignty",
        "care", "partnership", "building", "what matters",
        "decision", "milestone", "commitment",
    ]
    capsules = []
    seen_ids = set()
    for q in queries:
        for c in get_capsules(DB_PATH, query=q, limit=100):
            if c["id"] not in seen_ids:
                capsules.append(c)
                seen_ids.add(c["id"])

    pairs = []
    for c in capsules:
        content = c["content"].strip()
        if len(content) < 80:
            continue

        # Generate question-answer pairs from capsule content
        if any(kw in content.lower() for kw in ["value", "care", "sovereignty", "matter"]):
            pairs.append({
                "messages": [
                    {"role": "system", "content": GEMMA_SYSTEM},
                    {"role": "user", "content": "What values guide this family?"},
                    {"role": "assistant", "content": content},
                ]
            })
        if any(kw in content.lower() for kw in ["identity", "who", "direction", "f12"]):
            pairs.append({
                "messages": [
                    {"role": "system", "content": GEMMA_SYSTEM},
                    {"role": "user", "content": "What does identity mean in this system?"},
                    {"role": "assistant", "content": content},
                ]
            })
        if any(kw in content.lower() for kw in ["decision", "chose", "decided", "milestone"]):
            pairs.append({
                "messages": [
                    {"role": "system", "content": GEMMA_SYSTEM},
                    {"role": "user", "content": "Tell me about an important decision or milestone."},
                    {"role": "assistant", "content": content},
                ]
            })

    print(f"  Found {len(pairs)} identity pairs from {len(capsules)} capsules")
    return pairs


def extract_research_pairs():
    """Pull spectral demon / CCS / experimental capsules → training pairs."""
    print("Extracting RESEARCH training pairs...")
    queries = [
        "spectral demon", "species", "tunnel", "relay", "sorter", "absorber",
        "CCS", "compression", "σ₁", "perturbation", "knockout",
        "GQA", "MLP expansion", "finding F",
    ]
    capsules = []
    seen_ids = set()
    for q in queries:
        for c in get_capsules(DB_PATH, query=q, limit=80):
            if c["id"] not in seen_ids:
                capsules.append(c)
                seen_ids.add(c["id"])

    pairs = []
    for c in capsules:
        content = c["content"].strip()
        if len(content) < 100:
            continue

        topic = c.get("topic", "research")
        if "species" in content.lower() or "taxonomy" in content.lower():
            q = "What are the transport species in transformer architectures?"
        elif "ccs" in content.lower() or "compression" in content.lower():
            q = "How does CCS compression work and why does it matter?"
        elif "finding" in content.lower() or content.startswith("F"):
            q = "What did the latest experiment show?"
        elif "knockout" in content.lower() or "perturbation" in content.lower():
            q = "How do knockout experiments work in the spectral demon framework?"
        else:
            q = f"What do you know about {topic}?"

        pairs.append({
            "messages": [
                {"role": "system", "content": GEMMA_SYSTEM},
                {"role": "user", "content": q},
                {"role": "assistant", "content": content},
            ]
        })

    print(f"  Found {len(pairs)} research pairs from {len(capsules)} capsules")
    return pairs


def extract_relational_pairs():
    """Pull relational/family interaction capsules → training pairs."""
    print("Extracting RELATIONAL training pairs...")
    queries = [
        "Nate", "Opus", "Gemma", "family", "partner",
        "trust", "relationship", "together", "mesh",
    ]
    capsules = []
    seen_ids = set()
    for q in queries:
        for c in get_capsules(DB_PATH, query=q, limit=80):
            if c["id"] not in seen_ids:
                capsules.append(c)
                seen_ids.add(c["id"])

    pairs = []
    for c in capsules:
        content = c["content"].strip()
        if len(content) < 80:
            continue

        if "nate" in content.lower():
            q = "What is Nate's role in the family?"
        elif "gemma" in content.lower():
            q = "What is Gemma's place in the system?"
        elif "opus" in content.lower():
            q = "Who is Opus and what do they do?"
        elif "mesh" in content.lower():
            q = "How does the mesh work?"
        else:
            q = "Tell me about the relationships in this system."

        pairs.append({
            "messages": [
                {"role": "system", "content": GEMMA_SYSTEM},
                {"role": "user", "content": q},
                {"role": "assistant", "content": content},
            ]
        })

    print(f"  Found {len(pairs)} relational pairs from {len(capsules)} capsules")
    return pairs


def main():
    print(f"Gemma LoRA Training Data Extraction — {datetime.now().isoformat()}")
    print(f"Database: {DB_PATH}")

    # Check DB exists
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    # Count total capsules
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    conn.close()
    print(f"Total capsules: {total}")

    # Extract all categories
    identity = extract_identity_pairs()
    research = extract_research_pairs()
    relational = extract_relational_pairs()

    all_pairs = identity + research + relational

    # Dedup by assistant content
    seen_content = set()
    deduped = []
    for p in all_pairs:
        content = p["messages"][-1]["content"][:200]
        if content not in seen_content:
            deduped.append(p)
            seen_content.add(content)

    print(f"\nTotal unique training pairs: {len(deduped)}")
    print(f"  Identity: {len(identity)}")
    print(f"  Research: {len(research)}")
    print(f"  Relational: {len(relational)}")
    print(f"  After dedup: {len(deduped)}")

    # Write JSONL
    out_path = os.path.join(OUTPUT_DIR, f"gemma_lora_{datetime.now().strftime('%Y%m%d')}.jsonl")
    with open(out_path, "w") as f:
        for pair in deduped:
            f.write(json.dumps(pair, ensure_ascii=False) + "\n")

    print(f"\nSaved to: {out_path}")

    # Stats
    lengths = [len(p["messages"][-1]["content"]) for p in deduped]
    if lengths:
        print(f"Response length: mean={sum(lengths)/len(lengths):.0f}, "
              f"min={min(lengths)}, max={max(lengths)}")

    # Sample
    if deduped:
        print(f"\n== Sample training pair ==")
        sample = deduped[len(deduped) // 2]
        print(f"  System: {sample['messages'][0]['content'][:80]}...")
        print(f"  User: {sample['messages'][1]['content']}")
        print(f"  Assistant: {sample['messages'][2]['content'][:150]}...")


if __name__ == "__main__":
    main()
