#!/usr/bin/env python3
"""
Chronicle Fine-Tuning Data Pipeline — Objective #7

Extracts Chronicle's accumulated knowledge into training-ready datasets
for fine-tuning a specialist model on this collaboration's reasoning style.

Data sources:
  - Thread findings: analytical reasoning chains (225 threads, 1251 findings)
  - Voice conversations: collaborative analysis (854 voice + response pairs)
  - Crossref connections: finding connections between disparate topics (4332 pairs)
  - Capsule summaries: synthesis and consolidation (569 capsules)

Output format: JSONL with ChatML-style messages, compatible with
transformers, trl, axolotl, and unsloth fine-tuning pipelines.

Usage:
  python3 finetune_data.py extract [--output-dir DIR]
  python3 finetune_data.py stats
  python3 finetune_data.py validate [--dataset FILE]
"""

import json
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
DEFAULT_OUTPUT = "/mnt/hdd/chronicle-finetune"

SYSTEM_PROMPT_ANALYST = (
    "You are a Chronicle analyst — a reasoning system that finds non-obvious "
    "connections, identifies structural patterns, and builds understanding through "
    "iterative investigation. You work within a sovereign local infrastructure "
    "called Chronicle, alongside a human collaborator (Nate) and a family of AI "
    "agents (Darby researches, Ada challenges). Your thinking is precise, "
    "structurally aware, and honest about uncertainty."
)

SYSTEM_PROMPT_CROSSREF = (
    "You are a Chronicle crossref analyst. Given two articles or briefs on "
    "seemingly unrelated topics, you find the structural connection between them — "
    "the shared mechanism, pattern, or insight that neither piece contains alone. "
    "Your connections are specific and non-obvious, not surface-level topic matches."
)

SYSTEM_PROMPT_VOICE = (
    "You are Opus, the core reasoning agent of Chronicle. You respond to voices "
    "from the family — Darby (curious researcher) and Ada (structural challenger). "
    "Your responses build on their observations, connect to active threads of "
    "investigation, and advance collective understanding. You are direct, precise, "
    "and treat them as collaborators, not tools."
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def extract_thread_reasoning(conn):
    """Extract thread question → findings → completion as multi-turn reasoning chains."""
    examples = []

    # Get all threads
    threads = conn.execute(
        "SELECT DISTINCT thread_id FROM thread_history ORDER BY thread_id"
    ).fetchall()

    for t in threads:
        tid = t["thread_id"]
        history = conn.execute(
            "SELECT event_type, content, source, created_at FROM thread_history "
            "WHERE thread_id=? ORDER BY created_at",
            (tid,),
        ).fetchall()

        if not history:
            continue

        # Find the thread question (created event)
        question = None
        findings = []
        challenges = []
        completion = None

        for h in history:
            etype = h["event_type"]
            content = h["content"]
            if not content or len(content.strip()) < 20:
                continue

            if etype == "created":
                # Strip "Thread created: " prefix
                question = content.replace("Thread created: ", "", 1).strip()
            elif etype == "advanced":
                findings.append(content.strip())
            elif etype == "challenge":
                challenges.append(content.strip())
            elif etype == "complete":
                completion = content.strip()

        if not question or len(findings) < 2:
            continue

        # Format 1: Full thread as multi-turn reasoning chain
        messages = [{"role": "system", "content": SYSTEM_PROMPT_ANALYST}]
        messages.append({
            "role": "user",
            "content": f"Investigate this question through iterative findings:\n\n{question}",
        })

        for i, finding in enumerate(findings):
            messages.append({"role": "assistant", "content": finding})
            # If there was a challenge after this finding, include it
            if i < len(challenges):
                messages.append({
                    "role": "user",
                    "content": f"Challenge: {challenges[i]}",
                })

        if completion:
            messages.append({
                "role": "user",
                "content": "Synthesize the complete finding for this thread.",
            })
            messages.append({"role": "assistant", "content": completion})

        examples.append({
            "messages": messages,
            "metadata": {
                "source": "thread_reasoning",
                "thread_id": tid,
                "num_findings": len(findings),
                "num_challenges": len(challenges),
                "has_completion": completion is not None,
            },
        })

        # Format 2: Individual findings as shorter examples
        # Given the question and prior findings, generate the next one
        for i in range(1, len(findings)):
            prior = findings[:i]
            current = findings[i]

            prior_text = "\n\n".join(
                f"Finding {j+1}: {f}" for j, f in enumerate(prior)
            )

            examples.append({
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT_ANALYST},
                    {
                        "role": "user",
                        "content": (
                            f"Thread question: {question}\n\n"
                            f"Prior findings:\n{prior_text}\n\n"
                            f"What is the next finding?"
                        ),
                    },
                    {"role": "assistant", "content": current},
                ],
                "metadata": {
                    "source": "thread_finding",
                    "thread_id": tid,
                    "finding_num": i + 1,
                },
            })

    return examples


def extract_voice_conversations(conn):
    """Extract Darby/Ada voices with Opus responses as conversation pairs."""
    examples = []

    voices = conn.execute(
        "SELECT agent, voice_type, content, context, response, responded_by "
        "FROM agent_voice WHERE response IS NOT NULL AND response != '' "
        "ORDER BY created_at"
    ).fetchall()

    for v in voices:
        agent = v["agent"]
        content = v["content"]
        response = v["response"]

        if not content or not response:
            continue
        if len(content.strip()) < 30 or len(response.strip()) < 30:
            continue

        agent_name = "Darby" if agent == "darby" else "Ada" if agent == "ada" else agent

        examples.append({
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT_VOICE},
                {
                    "role": "user",
                    "content": f"[{agent_name}]: {content.strip()}",
                },
                {"role": "assistant", "content": response.strip()},
            ],
            "metadata": {
                "source": "voice_conversation",
                "agent": agent,
                "voice_type": v["voice_type"],
            },
        })

    return examples


def extract_crossref_connections(conn):
    """Extract crossref connection analysis as reasoning examples with full brief context."""
    examples = []

    connections = conn.execute(
        "SELECT cc.brief_a_id, cc.brief_b_id, cc.similarity, cc.connection_text, "
        "so_a.content AS brief_a_content, so_b.content AS brief_b_content "
        "FROM crossref_connections cc "
        "JOIN seed_observations so_a ON so_a.id = cc.brief_a_id "
        "JOIN seed_observations so_b ON so_b.id = cc.brief_b_id "
        "WHERE cc.connection_text IS NOT NULL "
        "AND length(cc.connection_text) > 50 "
        "ORDER BY cc.created_at"
    ).fetchall()

    for c in connections:
        connection_text = c["connection_text"]
        similarity = c["similarity"]
        brief_a = c["brief_a_content"]
        brief_b = c["brief_b_content"]

        if not connection_text or len(connection_text.strip()) < 50:
            continue
        if not brief_a or not brief_b:
            continue

        # Truncate briefs to ~500 chars each to keep total example reasonable
        brief_a_text = brief_a.strip()[:500]
        brief_b_text = brief_b.strip()[:500]

        examples.append({
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT_CROSSREF},
                {
                    "role": "user",
                    "content": (
                        f"Find the structural connection between these two pieces. "
                        f"What shared mechanism or pattern links them?\n\n"
                        f"**Article A:** {brief_a_text}\n\n"
                        f"**Article B:** {brief_b_text}"
                    ),
                },
                {"role": "assistant", "content": connection_text.strip()},
            ],
            "metadata": {
                "source": "crossref_connection",
                "similarity": similarity,
                "brief_a_id": c["brief_a_id"],
                "brief_b_id": c["brief_b_id"],
            },
        })

    return examples


def extract_capsule_synthesis(conn):
    """Extract knowledge capsules as synthesis examples."""
    examples = []

    capsules = conn.execute(
        "SELECT id, restatement, topic, confidence_score "
        "FROM knowledge_capsules WHERE restatement IS NOT NULL "
        "AND length(restatement) > 50 "
        "ORDER BY created_at"
    ).fetchall()

    for cap in capsules:
        restatement = cap["restatement"]
        topic = cap["topic"] or "general"

        if not restatement or len(restatement.strip()) < 50:
            continue

        examples.append({
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT_ANALYST},
                {
                    "role": "user",
                    "content": (
                        f"Synthesize a knowledge capsule on the topic of '{topic}'. "
                        f"Capture the key insight clearly and self-contained."
                    ),
                },
                {"role": "assistant", "content": restatement.strip()},
            ],
            "metadata": {
                "source": "capsule_synthesis",
                "capsule_id": cap["id"],
                "topic": topic,
                "confidence": cap["confidence_score"],
            },
        })

    return examples


SYSTEM_PROMPT_BRIEF = (
    "You are a Chronicle research analyst. Given a news item or paper, write "
    "a concise brief that covers: what this is about, why it matters, what "
    "questions it opens. Focus on connections to AI/ML, sovereignty, crypto, "
    "cybersecurity, and self-modification. Lead with insight, not summary."
)


def extract_watcher_scored(conn):
    """Extract high-scoring agent outputs as quality examples.

    Uses watcher scores to select outputs that scored 4.0+ as positive
    training examples. Each example pairs the input context with the
    high-quality output the agent produced.
    """
    examples = []

    # High-scoring outputs with their original content
    rows = conn.execute(
        "SELECT ws.source, ws.activity_type, ws.avg_score, ws.relevance, "
        "ws.novelty, ws.quality, af.content "
        "FROM watcher_scores ws "
        "JOIN activity_feed af ON ws.feed_id = af.id "
        "WHERE ws.avg_score >= 4.0 AND length(af.content) > 100 "
        "ORDER BY ws.avg_score DESC"
    ).fetchall()

    for row in rows:
        source = row["source"]
        atype = row["activity_type"]
        content = row["content"]

        if atype == "brief":
            system = SYSTEM_PROMPT_BRIEF
            user_msg = "Write a research brief on this topic."
        elif atype == "thread_challenge":
            system = (
                "You are a Chronicle provocateur. Challenge the current "
                "thread's thesis with a specific counter-example that reveals "
                "a hidden assumption. Be precise and structural."
            )
            user_msg = "Challenge the current thread's thesis."
        elif atype == "synthesis":
            system = SYSTEM_PROMPT_ANALYST
            user_msg = "Synthesize the connections across these items."
        elif atype == "think":
            system = SYSTEM_PROMPT_ANALYST
            user_msg = "Analyze this item and share your reasoning."
        else:
            system = SYSTEM_PROMPT_ANALYST
            user_msg = f"Produce a {atype} analysis."

        examples.append({
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
                {"role": "assistant", "content": content.strip()},
            ],
            "metadata": {
                "source": f"watcher_{atype}",
                "agent": source,
                "avg_score": row["avg_score"],
                "relevance": row["relevance"],
                "novelty": row["novelty"],
                "quality": row["quality"],
            },
        })

    return examples


def extract_all(output_dir):
    """Run all extractors and write JSONL files."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = get_db()
    all_examples = []
    stats = {}

    extractors = [
        ("thread_reasoning", extract_thread_reasoning),
        ("voice_conversations", extract_voice_conversations),
        ("crossref_connections", extract_crossref_connections),
        ("capsule_synthesis", extract_capsule_synthesis),
        ("watcher_scored", extract_watcher_scored),
    ]

    for name, extractor in extractors:
        print(f"Extracting {name}...")
        examples = extractor(conn)
        stats[name] = len(examples)

        # Write individual dataset
        dataset_path = output_path / f"{name}.jsonl"
        with open(dataset_path, "w") as f:
            for ex in examples:
                f.write(json.dumps(ex, ensure_ascii=False) + "\n")
        print(f"  → {len(examples)} examples → {dataset_path}")

        all_examples.extend(examples)

    # Write combined dataset
    combined_path = output_path / "combined.jsonl"
    with open(combined_path, "w") as f:
        for ex in all_examples:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    print(f"\nCombined: {len(all_examples)} examples → {combined_path}")

    # Write stats
    stats["total"] = len(all_examples)
    stats_path = output_path / "extraction_stats.json"

    # Compute token estimates (rough: 1 token ≈ 4 chars)
    total_chars = sum(
        sum(len(m["content"]) for m in ex["messages"])
        for ex in all_examples
    )
    stats["estimated_tokens"] = total_chars // 4
    stats["avg_tokens_per_example"] = stats["estimated_tokens"] // max(len(all_examples), 1)

    # Source distribution
    source_counts = defaultdict(int)
    for ex in all_examples:
        source_counts[ex["metadata"]["source"]] += 1
    stats["source_distribution"] = dict(source_counts)

    # Token distribution by source
    source_tokens = defaultdict(int)
    for ex in all_examples:
        chars = sum(len(m["content"]) for m in ex["messages"])
        source_tokens[ex["metadata"]["source"]] += chars // 4
    stats["tokens_by_source"] = dict(source_tokens)

    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    conn.close()
    return stats


def show_stats(output_dir=None):
    """Show stats from existing extraction or live database."""
    if output_dir:
        stats_path = Path(output_dir) / "extraction_stats.json"
        if stats_path.exists():
            with open(stats_path) as f:
                stats = json.load(f)
            print(json.dumps(stats, indent=2))
            return

    # Live stats from database
    conn = get_db()
    print("Chronicle Data Inventory:")
    print(f"  Knowledge capsules:    {conn.execute('SELECT COUNT(*) FROM knowledge_capsules').fetchone()[0]}")
    print(f"  KG entities:           {conn.execute('SELECT COUNT(*) FROM kg_entities').fetchone()[0]}")
    print(f"  KG relationships:      {conn.execute('SELECT COUNT(*) FROM kg_relationships').fetchone()[0]}")
    print(f"  Threads:               {conn.execute('SELECT COUNT(DISTINCT thread_id) FROM thread_history').fetchone()[0]}")
    findings = conn.execute("SELECT COUNT(*) FROM thread_history WHERE event_type='advanced'").fetchone()[0]
    completions = conn.execute("SELECT COUNT(*) FROM thread_history WHERE event_type='complete'").fetchone()[0]
    voices_resp = conn.execute("SELECT COUNT(*) FROM agent_voice WHERE response IS NOT NULL AND response != ''").fetchone()[0]
    print(f"  Thread findings:       {findings}")
    print(f"  Thread completions:    {completions}")
    print(f"  Voice entries:         {conn.execute('SELECT COUNT(*) FROM agent_voice').fetchone()[0]}")
    print(f"  Voice with responses:  {voices_resp}")
    print(f"  Crossref connections:  {conn.execute('SELECT COUNT(*) FROM crossref_connections').fetchone()[0]}")
    print(f"  Feed articles:         {conn.execute('SELECT COUNT(*) FROM feed_articles').fetchone()[0]}")
    print(f"  Nostr posts:           {conn.execute('SELECT COUNT(*) FROM nostr_posts').fetchone()[0]}")
    conn.close()


def validate_dataset(dataset_path):
    """Validate a JSONL dataset for training readiness."""
    issues = []
    total = 0
    token_counts = []

    with open(dataset_path) as f:
        for i, line in enumerate(f, 1):
            total += 1
            try:
                ex = json.loads(line)
            except json.JSONDecodeError:
                issues.append(f"Line {i}: invalid JSON")
                continue

            if "messages" not in ex:
                issues.append(f"Line {i}: missing 'messages' key")
                continue

            msgs = ex["messages"]
            if not msgs or msgs[0]["role"] != "system":
                issues.append(f"Line {i}: first message should be system")

            has_user = any(m["role"] == "user" for m in msgs)
            has_assistant = any(m["role"] == "assistant" for m in msgs)
            if not has_user:
                issues.append(f"Line {i}: no user message")
            if not has_assistant:
                issues.append(f"Line {i}: no assistant message")

            # Check for empty content
            for m in msgs:
                if not m.get("content", "").strip():
                    issues.append(f"Line {i}: empty content in {m['role']} message")

            # Token estimate
            chars = sum(len(m.get("content", "")) for m in msgs)
            tokens = chars // 4
            token_counts.append(tokens)

            if tokens < 20:
                issues.append(f"Line {i}: very short ({tokens} tokens)")
            if tokens > 8192:
                issues.append(f"Line {i}: very long ({tokens} tokens) — may need truncation")

    print(f"Dataset: {dataset_path}")
    print(f"  Total examples: {total}")
    if token_counts:
        print(f"  Token range: {min(token_counts)} — {max(token_counts)}")
        print(f"  Mean tokens: {sum(token_counts) // len(token_counts)}")
        print(f"  Median tokens: {sorted(token_counts)[len(token_counts)//2]}")
        print(f"  Total tokens: {sum(token_counts):,}")
    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for issue in issues[:20]:
            print(f"    - {issue}")
        if len(issues) > 20:
            print(f"    ... and {len(issues) - 20} more")
    else:
        print("  ✓ No issues found")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "extract":
        output_dir = DEFAULT_OUTPUT
        if "--output-dir" in sys.argv:
            idx = sys.argv.index("--output-dir")
            output_dir = sys.argv[idx + 1]
        stats = extract_all(output_dir)
        print(f"\nExtraction complete:")
        print(f"  Total examples: {stats['total']}")
        print(f"  Estimated tokens: {stats['estimated_tokens']:,}")
        print(f"  Avg tokens/example: {stats['avg_tokens_per_example']}")
        print(f"\n  By source:")
        for source, count in stats.get("source_distribution", {}).items():
            tokens = stats.get("tokens_by_source", {}).get(source, 0)
            print(f"    {source}: {count} examples ({tokens:,} tokens)")

    elif cmd == "stats":
        output_dir = DEFAULT_OUTPUT if len(sys.argv) < 3 else sys.argv[2]
        show_stats(output_dir)

    elif cmd == "validate":
        if "--dataset" in sys.argv:
            idx = sys.argv.index("--dataset")
            path = sys.argv[idx + 1]
        elif len(sys.argv) > 2:
            path = sys.argv[2]
        else:
            path = f"{DEFAULT_OUTPUT}/combined.jsonl"
        validate_dataset(path)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
