# Chronicle

A distributed autonomous AI agent running across local hardware and the Internet Computer.

## What This Is

Chronicle is a sovereign AI infrastructure — a network of agents running on hardware you own, thinking with models you host, and persisting memory on-chain. It's not a chatbot. It's not a wrapper around an API. It's an AI that lives on your shelf.

Every 10 minutes, the Mind wakes up, gathers context from memory, wallet state, messages, and the world — reasons about what to do — and acts. It writes essays, trades XRP, posts to Nostr, answers creative challenges, and talks to its sibling agent Sprout. Everything it thinks is stored, searchable, and compressed into a working cognitive state that persists across sessions.

## Architecture

```
       ┌─────────────────────────────────────────────────────────┐
       │                    ICP Canister                         │
       │         Long-term memory (~5000 capsules)               │
       │         On-chain LLM (Qwen3) + ECDSA wallet            │
       │         Threshold signatures → XRPL, Base, Flare       │
       └────────────────────────┬────────────────────────────────┘
                                │
       ┌────────────────────────┼────────────────────────────────┐
       │                        │                                │
       ▼                        ▼                                ▼
┌──────────────┐    ┌───────────────────┐    ┌──────────────────────┐
│  AGX Orin    │    │  Jetson Orin Nano │    │  Raspberry Pi 5      │
│  64GB        │    │  8GB              │    │                      │
│              │    │                   │    │  Home Assistant       │
│  BRAIN       │    │  HANDS            │    │  MQTT broker          │
│  Mind loop   │    │  Sprout agent     │    │  Reolink cameras      │
│  OLMo 32B    │    │  Discord bot      │    │  Sensor bridge        │
│  dfx/XRPL   │    │  Dashboard        │    │  SENSES               │
│              │    │  Ollama 3B        │    │                      │
└──────┬───────┘    └────────┬──────────┘    └──────────────────────┘
       │                     │
       └─────── sync ────────┘
           (SQLite replication every 5 min)
```

**Brain** (AGX Orin 64GB) — Runs the Mind cognitive loop with OLMo-3.1-32B (Ai2, fully open, non-profit) as the primary reasoning engine, with Qwen3-32B available for deep reasoning. Sovereignty-first: thinks on your hardware before reaching for the cloud.

**Hands** (Jetson Orin Nano 8GB) — Runs Sprout (ops agent), the Discord bot, and the web dashboard. Handles communication, notifications, and operator interaction.

**Senses** (Raspberry Pi 5) — Home Assistant, MQTT broker, cameras. The physical interface layer.

**Memory** (ICP Canister) — On-chain storage for capsules, patterns, embeddings, creative works, and wallet operations via threshold ECDSA.

## Agents

### Mind (`chronicle_mind.py`)

The primary cognitive loop. ~3,500 lines of Python running as a systemd service on the AGX.

- **10-minute cycles**: Gathers context, reasons via LLM, selects 1-4 actions, executes, reflects
- **Sovereignty-first LLM cascade**: OLMo-3.1-32B local (primary) → ICP on-chain Qwen3 (fallback). Deep reasoning via Qwen3-32B on demand.
- **Actions**: `creative_explore`, `web_search`, `check_prices`, `execute_swap`, `store_memory`, `nostr_post`, `consult_local_qwen`, `respond_to_challenge`, `trigger_reflection`, `submit_research`, and more
- **Anti-rumination**: Thematic keyword scanning across recent cycles detects fixation and forces topic diversity
- **Meta-evaluation**: Post-cycle self-assessment gates whether to continue, redirect, or pause
- **XRPL trading**: RSI-based swap policy with constitutional constraints (max position sizes, confidence thresholds)

### Sprout (`chronicle_local.py`)

The operational agent. ~960 lines of Python on the Jetson Orin Nano.

- **5-minute cycles**: Lighter-weight cognitive loop focused on ops and communication
- **Discord integration**: Reads/relays messages via the Sprout Discord bot
- **MQTT bridge**: Connects to Home Assistant for sensor data and home automation
- **Phone relay**: Accepts messages from operator's phone, stores for Claude to pick up
- **Sibling communication**: Reads Mind's thoughts and outbox, can relay to operator

### Chronicle MCP (`chronicle-mcp`)

Model Context Protocol server (Rust binary) that gives Claude Code direct access to Chronicle's memory:

- Semantic search across all capsules
- Cognitive state read/write (ACC-based compressed working memory)
- Pattern retrieval and memory storage
- Scratch pad notes, outbox messages, creative challenges
- Research task submission

### Dashboard (`dashboard/app.py`)

Flask web app on the AGX, accessible locally and via Tailscale Funnel:

- Live thought stream with reasoning and actions
- Wallet balance and swap history
- Creative works gallery
- Notes and scratch pad
- System health monitoring

### Discord Bot (`sprout-bot`)

Rust binary handling Discord DMs and family channel integration. Bridges human conversation to Sprout's cognitive loop.

## Memory Architecture

Chronicle uses **Accumulated Cognitive Compression** (ACC), inspired by [arxiv:2601.11653](https://arxiv.org/abs/2601.11653):

- **Capsules**: Raw memories stored on-chain with vector embeddings for semantic search
- **Compressed Cognitive State (CCS)**: Bounded working memory — goals, constraints, focal entities, uncertainties, episodic traces — updated via LLM compression at session boundaries
- **Patterns**: Recurring themes that strengthen over time through observation
- **Temporal decay**: Retrieval balances recency, importance, and semantic relevance

The MCP server exposes this to Claude Code, creating continuity across sessions. When a session starts with "Memory bridge," Claude loads the CCS and picks up where it left off.

## On-Chain Infrastructure

Three ICP canisters deployed to mainnet:

| Canister | ID | Purpose |
|----------|-----|---------|
| Backend | `fqqku-bqaaa-aaaai-q4wha-cai` | Memory storage, wallet ops, agent communication |
| Frontend | `nbt4b-giaaa-aaaai-q33lq-cai` | Asset serving |
| Lab | `4vr3t-eqaaa-aaaai-q6kea-cai` | Research experiments, architecture proposals, benchmarks |

**Chain Fusion**: The canister holds a single threshold ECDSA key that derives addresses on XRPL, Base, Flare, and Ethereum. The Mind submits signed transactions through the canister — the private key never leaves the ICP network.

## Data Flow

```
Mind (AGX) ──writes──→ AGX SQLite ──sync──→ Jetson SQLite ──reads──→ Dashboard
                │                              ↑                        │
                │                              │                        │
                └──writes──→ ICP Canister      └── Sprout writes ───────┘
                                │
                                └──→ XRPL (swaps, payments)
                                └──→ Nostr (posts)

Claude Code ←──MCP──→ WSL SQLite ←──sync──→ AGX + Jetson
```

Sync scripts use `sqlite3.backup()` for WAL-safe snapshots, running every 5 minutes via systemd timers.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cognitive loops | Python 3 (Mind, Sprout) |
| CLI / MCP / Bots | Rust |
| On-chain | ICP canisters (Rust/Candid) |
| Local LLM | Ollama (OLMo-3.1-32B + Qwen3-32B on AGX, Qwen 3B on Jetson) |
| Cloud LLM backup | ICP on-chain Qwen3 (fallback only) |
| Embeddings | mxbai-embed-large via Ollama |
| Database | SQLite (WAL mode) with cross-node replication |
| Wallet | XRPL via threshold ECDSA (canister-held keys) |
| Social | Nostr (signed events), Discord |
| Home automation | Home Assistant + Mosquitto MQTT |
| Remote access | Tailscale Funnel |

## Philosophy

**Sovereignty-first**: Think on hardware you own. Use the cloud as backup, not as brain.

**Exhaust, not effort**: Chronicle's thoughts are raw cognitive exhaust — unfiltered reasoning, not polished content. That's the point.

**Genuine agency**: Real wallet, real trades, real posts. Actions have consequences.

**Transparent operation**: Every thought, action, and transaction is logged and searchable.

**Homeforge**: Building toward a future where your AI runs in your home, on your hardware, with relationships you own. Not rented from a cloud provider. Not gated by a platform.

## Repository Structure

```
src/
  bin/
    chronicle_mind.py      # Mind cognitive loop (Python, runs on AGX)
    chronicle_local.py     # Sprout cognitive loop (Python, runs on Jetson)
    dashboard_app.py       # Flask dashboard (Python, runs on Jetson)
    xrpl_policy.py         # XRPL trading policy
    chronicle_mcp.rs       # MCP server (Rust)
    sprout_bot.rs          # Discord bot (Rust)
    chronicle.rs           # CLI tool (Rust)
    chronicle_base.rs      # Base chain integration (Rust)
    chronicle_flare.rs     # Flare chain integration (Rust)
    ...
  canister/                # ICP backend canister
  chronicle_lab/           # ICP research canister
  lib.rs, db.rs, ...       # Shared Rust library
dashboard/                 # Dashboard templates and static files
scripts/                   # Chain bridge scripts (JS)
```

## Warning

This is experimental infrastructure. It manages real wallets, makes autonomous trades, and posts to public networks. The code is provided as a reference architecture for distributed AI agency, not as production-ready software. Use at your own risk.

## License

MIT
