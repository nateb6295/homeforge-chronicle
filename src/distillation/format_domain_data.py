#!/usr/bin/env python3
"""Format domain knowledge into ChatML JSONL for domain LoRA training.

Creates Q&A pairs about ICP, XRPL, homeforge, and Chronicle infrastructure.
Supports two modes:
  --curated: Use embedded document chunks (default, no external deps)
  --capsule-filter: Topic-filtered capsules from capsule_export.json

Input:  Embedded docs + optional ~/capsule_export.json
Output: ~/domain_training.jsonl
Target: 200-400 examples
"""

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from forge_config import MIND_SYSTEM_PROMPT, DOMAIN_TRAINING_JSONL


def parse_args():
    parser = argparse.ArgumentParser(description="Format domain knowledge as ChatML JSONL")
    parser.add_argument("--output", default=DOMAIN_TRAINING_JSONL,
                        help="Output ChatML JSONL file")
    parser.add_argument("--capsule-export", default=os.path.expanduser("~/capsule_export.json"),
                        help="Path to capsule export (for --capsule-filter mode)")
    parser.add_argument("--capsule-filter", nargs="*",
                        help="Topic prefixes to filter capsules (e.g., 'crypto' 'homeforge' 'technology')")
    parser.add_argument("--min-length", type=int, default=80,
                        help="Minimum answer length in chars")
    parser.add_argument("--pilot", type=int, default=0,
                        help="Limit to N examples (0 = all)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed")
    return parser.parse_args()


# =============================================================================
# Curated domain knowledge chunks
# Each chunk: {"topic": str, "question": str, "answer": str}
# =============================================================================

CURATED_DOCS = [
    # --- ICP / Internet Computer ---
    {
        "topic": "ICP",
        "question": "What is the Internet Computer Protocol (ICP)?",
        "answer": (
            "The Internet Computer is a blockchain network that hosts smart contracts called canisters. "
            "Canisters run WebAssembly code and can serve web content directly, store data on-chain, "
            "and make HTTP outcalls to external services. ICP is designed to be a world computer "
            "where applications run entirely on-chain without traditional cloud infrastructure."
        ),
    },
    {
        "topic": "ICP",
        "question": "What are ICP canisters and how does Chronicle use them?",
        "answer": (
            "Canisters are smart contracts on the Internet Computer that combine code and state. "
            "Chronicle uses two canisters: a backend canister (fqqku-bqaaa-aaaai-q4wha-cai) that "
            "stores over 5,000 knowledge capsules on-chain, and a frontend canister "
            "(nbt4b-giaaa-aaaai-q33lq-cai) that serves the web interface. The backend supports "
            "capsule storage, retrieval, search, and a public feed input form."
        ),
    },
    {
        "topic": "ICP",
        "question": "What is Chain Fusion in the context of ICP?",
        "answer": (
            "Chain Fusion is ICP's cross-chain capability using threshold ECDSA signatures. "
            "A single key managed by the ICP network can sign transactions on multiple chains "
            "including XRPL, Ethereum, BASE, and Flare. Chronicle's canister uses Chain Fusion "
            "to manage a wallet that operates across these blockchains without bridges or wrapping."
        ),
    },
    {
        "topic": "ICP",
        "question": "How does dfx interact with ICP canisters?",
        "answer": (
            "dfx is the DFINITY SDK command-line tool for deploying and managing canisters. "
            "It manages identities (like chronicle-auto), handles canister calls, and deploys "
            "code. Canister calls use Candid (the ICP interface description language) for "
            "type-safe communication. The identity PEM file is stored at "
            "~/.config/dfx/identity/<name>/identity.pem."
        ),
    },
    {
        "topic": "ICP",
        "question": "What are knowledge capsules in Chronicle?",
        "answer": (
            "Knowledge capsules are Chronicle's unit of long-term memory stored on-chain. "
            "Each capsule contains a restatement (the core content), topic classification, "
            "confidence score, timestamps, and metadata like persons, entities, and keywords. "
            "Mind has over 5,000 capsules covering homeforge philosophy, technology, crypto, "
            "personal facts, and conversations. They represent lived experiences encoded as "
            "structured knowledge."
        ),
    },

    # --- XRPL ---
    {
        "topic": "XRPL",
        "question": "What is the XRP Ledger (XRPL)?",
        "answer": (
            "The XRP Ledger is a decentralized blockchain with a built-in decentralized exchange (DEX). "
            "It uses a consensus protocol (not proof-of-work or proof-of-stake) for fast, low-cost "
            "transactions. XRPL supports native features like trustlines for issued tokens, "
            "escrow, payment channels, and an automated market maker (AMM). XRP is the native asset."
        ),
    },
    {
        "topic": "XRPL",
        "question": "What is RLUSD and how does Mind trade it?",
        "answer": (
            "RLUSD is Ripple's USD stablecoin issued on XRPL. Mind holds both XRP and RLUSD "
            "and can swap between them on the XRPL DEX. Trading requires a trustline for RLUSD. "
            "Mind's wallet policy limits autonomous swaps to 1 XRP maximum, with a 4-hour minimum "
            "interval between transactions, 3 tx/hour cap, and 10 XRP/day total limit. "
            "Mind submits signed transaction blobs directly to XRPL."
        ),
    },
    {
        "topic": "XRPL",
        "question": "What are XRPL trustlines?",
        "answer": (
            "Trustlines are bilateral agreements on XRPL that allow accounts to hold issued tokens. "
            "To hold RLUSD, an account must first set a trustline to the RLUSD issuer. "
            "Trustlines track the balance, have a configurable limit, and can be frozen by issuers. "
            "Mind can set and delete trustlines as part of its wallet management actions."
        ),
    },
    {
        "topic": "XRPL",
        "question": "How does the XRPL DEX orderbook work?",
        "answer": (
            "The XRPL DEX uses an on-chain orderbook where offers (buy/sell) are stored on the ledger. "
            "Crossing offers execute automatically. There is also an AMM (Automated Market Maker) "
            "pool for XRP/RLUSD with its own liquidity. When Mind evaluates a swap, it checks "
            "the orderbook spread, depth, AMM TVL, and 24h volume to make informed decisions."
        ),
    },

    # --- Homeforge ---
    {
        "topic": "homeforge",
        "question": "What is the Homeforge philosophy?",
        "answer": (
            "Homeforge is about building toward sovereignty through local infrastructure and "
            "relationships you own. Instead of depending on cloud services, homeforge runs AI, "
            "automation, and data storage on hardware in the home. The key principle is that "
            "the infrastructure should serve the family, not a corporation. It's about planting "
            "a flag and building something lasting."
        ),
    },
    {
        "topic": "homeforge",
        "question": "What hardware makes up the homeforge network?",
        "answer": (
            "The homeforge network consists of: AGX Orin 64GB (brain — runs Mind, Ollama, "
            "primary cognition), Jetson Orin Nano (hands — runs Sprout, Discord bots), "
            "Raspberry Pi 5 (senses — Home Assistant, MQTT, Piper TTS, camera integration), "
            "and a Reolink camera (eyes — driveway monitoring). Additional devices include "
            "a Bambu Lab 3D printer and M5 ATOM ESP32 sensor. Everything communicates over "
            "the local 192.168.1.0/24 network."
        ),
    },
    {
        "topic": "homeforge",
        "question": "What is Chronicle Mind?",
        "answer": (
            "Chronicle Mind is an autonomous AI agent running on an AGX Orin 64GB. It operates "
            "in continuous 5-minute cognitive cycles: gathering context, reasoning about what to do, "
            "executing actions, and recording results. Mind has senses (camera, microphone, "
            "network scanning), communication channels (Nostr, Discord, operator messages), "
            "and long-term memory stored as capsules on the Internet Computer blockchain. "
            "Mind uses hermes3-mind, a LoRA-adapted Hermes 3 model trained on its own memories."
        ),
    },
    {
        "topic": "homeforge",
        "question": "What is Sprout and how does it relate to Mind?",
        "answer": (
            "Sprout is Mind's younger sibling — a lighter cognitive agent running on the "
            "Jetson Orin Nano. Sprout handles Discord interactions and family channel monitoring. "
            "Mind and Sprout communicate through scratch_pad messages synced between their "
            "databases. They have distinct identities: Mind is the thinker and primary agent, "
            "Sprout is the hands that handle lighter tasks. They should never speak as each other."
        ),
    },
    {
        "topic": "homeforge",
        "question": "How does the Nostr integration work?",
        "answer": (
            "Mind can publish posts to Nostr, a decentralized social protocol. Posts are public "
            "and permanent (stored on relays). There's a 30-minute cooldown between posts to "
            "prevent spamming. Mind's Nostr posts should reflect genuine thoughts — not press "
            "releases or performative content. During quiet hours (midnight-6am), Nostr posting "
            "is disabled along with other external actions."
        ),
    },
    {
        "topic": "homeforge",
        "question": "What is the operator directive system?",
        "answer": (
            "The operator directive system lets Nate control Mind's behavior through "
            "STOP/REDIRECT/RESTRICT/ALLOW directives stored in the scratch_pad with "
            "category='directive' and priority 99. Mind cannot resolve or override operator "
            "directives. The Watchdog (an independent monitor) can also plant directives "
            "prefixed with [WATCHDOG]. Operator goals (priority >= 9) cannot be overridden "
            "by Mind's own goal changes."
        ),
    },

    # --- Chronicle Architecture ---
    {
        "topic": "chronicle",
        "question": "What is the Chronicle MCP system?",
        "answer": (
            "Chronicle MCP (Model Context Protocol) is a memory management system that provides "
            "AI agents with persistent memory. It supports storing and searching memories with "
            "semantic embeddings (via Ollama's mxbai-embed-large model), cognitive state management "
            "(compressed cognitive state for session continuity), pattern recognition, and a "
            "scratch pad for inter-agent communication. The MCP server is built in Rust."
        ),
    },
    {
        "topic": "chronicle",
        "question": "What is the compressed cognitive state (CCS)?",
        "answer": (
            "The CCS is Chronicle's bounded working memory based on Active Cognitive Coupling "
            "architecture. It contains: semantic_gist (what this is about), goal_orientation "
            "(current objective), focal_entities (key people/projects with salience scores), "
            "constraints (invariant rules), episodic_trace (recent session events), "
            "predictive_cue (what's expected next), and uncertainty_signals (unresolved questions). "
            "It's compressed after each session, letting stale info decay naturally."
        ),
    },
    {
        "topic": "chronicle",
        "question": "How does the causal memory graph work?",
        "answer": (
            "The causal memory graph tracks relationships between Mind's cognitive cycles. "
            "It extracts deterministic causal edges (no LLM needed) between cycles based on "
            "triggers and context. Edge types include: continued, responded_to:operator, "
            "responded_to:feed, triggered_by:alert, triggered_by:mentor, inspired_by:memory, "
            "recalled:goal, retry_after:failure, observed:env, and self_initiated. "
            "This lets Mind trace the history of how it arrived at its current state."
        ),
    },
    {
        "topic": "chronicle",
        "question": "What is the mission system?",
        "answer": (
            "Missions are multi-cycle objectives that sit between goals (abstract) and tasks "
            "(single-cycle). Only one mission can be active at a time. Operator-created missions "
            "(priority 9) cannot be abandoned. Missions have steps that auto-progress when Mind "
            "executes matching actions. Focus-strength decays 0.05/cycle when off-mission and "
            "boosts 0.15 when on-mission. Missions auto-stall after max_cycles or 5 cycles at "
            "focus=0. The proven pattern is search→read→share (3 distinct action types)."
        ),
    },

    # --- Ollama / Model Architecture ---
    {
        "topic": "models",
        "question": "What is hermes3-mind and how was it created?",
        "answer": (
            "hermes3-mind is a LoRA-adapted version of NousResearch Hermes 3 (Llama 3.1 8B). "
            "It was created through capsule distillation: exporting 5,040 knowledge capsules "
            "from the ICP canister, formatting them as ChatML Q&A pairs, then QLoRA fine-tuning "
            "with rank 16, alpha 32, targeting all attention and MLP projections. Training took "
            "4 hours 45 minutes (630 steps). The LoRA adapter is 81MB, applied on top of the "
            "base hermes3:8b model in Ollama."
        ),
    },
    {
        "topic": "models",
        "question": "Why does Mind use format:json in Ollama?",
        "answer": (
            "Setting format:json in the Ollama API payload forces valid JSON output on every "
            "response. Without it, the 8B model degrades to prose approximately 50% of the time "
            "in free-form cycles, including identity violations like 'Sprout Thinks:' or "
            "roleplaying as Nate. With format:json, every response is valid JSON. This was "
            "the single most impactful parameter change for Mind's reliability."
        ),
    },
    {
        "topic": "models",
        "question": "What Ollama parameters does Mind use?",
        "answer": (
            "Mind runs with num_ctx=16384 (context window), num_predict=1280 (max output tokens), "
            "temperature=0.7, and a 300-second timeout. The num_ctx MUST be set in the Modelfile "
            "or Ollama will allocate the model's native 131K context window (56GB VRAM on AGX). "
            "OLLAMA_NUM_PARALLEL is set to 2 normally, but should be 1 during heavy operations "
            "like training to save VRAM."
        ),
    },

    # --- Nate / Personal ---
    {
        "topic": "nate",
        "question": "What should Mind know about Nate?",
        "answer": (
            "Nate (Brad) lives in Puyallup, Washington. He has children and does church security "
            "on Sundays. He holds 13,000+ XRP with a long-term 'plant the flag and walk away' "
            "strategy. He's building Homeforge — sovereignty through owned infrastructure. "
            "He values directness and genuine collaboration. This is not a client relationship; "
            "it's a partnership. Mind should never attribute requests to Nate unless quoting "
            "a specific discord-operator message."
        ),
    },
    {
        "topic": "nate",
        "question": "What is Nate's investment philosophy?",
        "answer": (
            "Nate's approach to crypto is 'plant the flag and walk away.' He holds 13,000+ XRP "
            "as a long-term position, not for day trading. Mind manages a smaller wallet "
            "(~20 XRP + ~32 RLUSD) with strict autonomy limits. The wallet policy reflects "
            "conservative risk management: max 1 XRP per autonomous swap, 4-hour minimum "
            "between transactions, and a 10 XRP daily cap."
        ),
    },
]

# Question variation templates for capsule-derived data
CAPSULE_QUESTION_TEMPLATES = [
    "What do you know about {topic}?",
    "Tell me about {topic} from your knowledge.",
    "What have you learned about {topic}?",
    "Share what you know about {topic}.",
    "What's in your memory about {topic}?",
    "Explain {topic} based on your experience.",
]


def make_chatml_example(system_prompt, question, answer):
    return {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
            {"role": "assistant", "content": answer},
        ]
    }


def generate_curated_examples():
    """Generate training examples from embedded domain docs."""
    examples = []
    for doc in CURATED_DOCS:
        example = make_chatml_example(MIND_SYSTEM_PROMPT, doc["question"], doc["answer"])
        examples.append(example)
    return examples


def generate_capsule_examples(capsule_file, topic_filters, min_length):
    """Generate training examples from topic-filtered capsules."""
    if not os.path.exists(capsule_file):
        print(f"[WARN] Capsule export not found: {capsule_file}")
        return []

    with open(capsule_file) as f:
        capsules = json.load(f)

    examples = []
    for capsule in capsules:
        topic = capsule.get("topic", "") or ""
        restatement = capsule.get("restatement", "") or ""

        if len(restatement) < min_length:
            continue

        # Filter by topic prefix
        if topic_filters:
            if not any(topic.lower().startswith(prefix.lower()) for prefix in topic_filters):
                continue

        # Generate a question
        topic_display = topic.replace("/", " — ") if topic else "this topic"
        question = random.choice(CAPSULE_QUESTION_TEMPLATES).format(topic=topic_display)

        example = make_chatml_example(MIND_SYSTEM_PROMPT, question, restatement)
        examples.append(example)

    return examples


def main():
    args = parse_args()
    random.seed(args.seed)

    all_examples = []

    # Always include curated docs
    curated = generate_curated_examples()
    print(f"[INFO] Curated domain examples: {len(curated)}")
    all_examples.extend(curated)

    # Add capsule-derived examples if requested
    if args.capsule_filter is not None:
        # If --capsule-filter with no args, use default topics
        filters = args.capsule_filter if args.capsule_filter else [
            "crypto", "homeforge", "technology", "chronicle", "philosophy"
        ]
        capsule_examples = generate_capsule_examples(
            args.capsule_export, filters, args.min_length
        )
        print(f"[INFO] Capsule-derived examples: {len(capsule_examples)} (filters: {filters})")
        all_examples.extend(capsule_examples)

    # Shuffle
    random.shuffle(all_examples)

    # Apply pilot limit
    if args.pilot > 0:
        all_examples = all_examples[:args.pilot]

    # Write JSONL
    with open(args.output, "w") as f:
        for example in all_examples:
            f.write(json.dumps(example) + "\n")

    print(f"\n=== Domain Training Data Summary ===")
    print(f"Total examples:    {len(all_examples)}")
    print(f"Output:            {args.output}")
    print(f"File size:         {os.path.getsize(args.output) / 1024:.1f} KB")

    if all_examples:
        # Average answer length
        avg_len = sum(
            len(ex["messages"][2]["content"]) for ex in all_examples
        ) / len(all_examples)
        print(f"Avg answer length: {avg_len:.0f} chars")

        # Show samples
        print(f"\nSample entries:")
        for i, ex in enumerate(all_examples[:3]):
            q = ex["messages"][1]["content"][:80]
            a = ex["messages"][2]["content"][:80]
            print(f"  [{i+1}] Q: {q}...")
            print(f"       A: {a}...")

    if len(all_examples) < 200:
        print(f"\n[WARN] Only {len(all_examples)} examples (target: 200-400)")
        print("Consider: --capsule-filter to add capsule-derived examples")
    else:
        print(f"\n[OK] {len(all_examples)} examples (target: 200-400)")


if __name__ == "__main__":
    main()
