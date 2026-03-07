# Chronicle

A distributed autonomous AI system running across local hardware, the Internet Computer, and the Swiss Subnet.

## What This Is

Chronicle is sovereign AI infrastructure — a network of agents running on hardware you own, thinking with models you host, and persisting memory on-chain. It's not a chatbot. It's not a wrapper around an API. It's an AI that lives on your shelf and thinks on its own schedule.

The system runs continuous cognitive loops on local Jetson hardware, processes observations through a novelty-scoring pipeline, maintains persistent memory across sessions via an MCP bridge, and executes real actions — trading XRP, posting to Nostr, answering Discord messages, and monitoring your home.

## Architecture

```
       ┌─────────────────────────────────────────────────────────┐
       │                ICP Mainnet Canisters                    │
       │         Long-term memory (~5,100+ capsules)             │
       │         On-chain LLM (Qwen3) + ECDSA wallet            │
       │         Threshold signatures → XRPL, Base, Flare       │
       └────────────────────────┬────────────────────────────────┘
                                │
       ┌─────────────────────────────────────────────────────────┐
       │              ICP Swiss Subnet (Beta)                    │
       │         Chronicle backend mirror                        │
       │         Data sovereignty under Swiss law                │
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
│  Seed agent  │    │  Sprout agent     │    │  Reolink cameras (2)  │
│  Intern      │    │  Discord bot      │    │  VAD ear (Silero)     │
│  Sentinel    │    │  Capture pipeline │    │  Piper TTS            │
│  Transcriber │    │  Ollama 3B        │    │  SENSES               │
│  Ollama 8B   │    │                   │    │                      │
└──────┬───────┘    └────────┬──────────┘    └──────────┬───────────┘
       │                     │                          │
       └─────── sync ────────┘                          │
           (SQLite replication)              MQTT (homeforge/home/#)
                                                        │
┌──────────────────────────────────────────────────────────────────┐
│  WSL (Dev Machine)                                               │
│  Claude Code + MCP memory bridge                                 │
│  Opus Cycle (autonomous 2hr timer)                               │
│  Chronicle CLI, sync, dashboard                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Agents

### Seed (`seed.py`) — Novelty Router

Always-on agent on the AGX. Embeds every observation, scores it for novelty against existing memory, and routes by threshold:

- **Think events**: Novel enough to note, logged to activity feed
- **Deep events**: Highly novel, triggers deeper analysis
- Subscribes to MQTT (`homeforge/home/#`) for real-time sensor data, camera events, and home state
- All observations get vector embeddings via mxbai-embed-large

### Intern (`intern.py`) — Research Agent

Watches for captures and messages, searches memory and the web, synthesizes research briefs. Handles the "look this up" pipeline from phone captures.

### Sentinel (`chronicle_sentinel.py`) — System Monitor

Lightweight 15-minute monitoring loop. Checks network, XRP price, messages, system health. Sends alerts to Discord when something needs attention. Replaced the heavier Mind cognitive loop.

### Sprout (`chronicle_local.py`) — Ops Agent

Operational agent on the Jetson Orin Nano. 5-minute cognitive cycles focused on communication and ops:

- Discord integration via the Sprout bot (Rust binary)
- Phone capture pipeline: Discord #capture → canister storage
- MQTT bridge to Home Assistant
- Reads sibling agent thoughts and relays to operator

### Opus Cycle — Autonomous Claude Code Sessions

Claude Code (Opus) runs on a 2-hour systemd timer. Each cycle:

1. Loads cognitive state via MCP memory bridge
2. SSH checks AGX services (Seed, Intern health)
3. Reviews activity feed for new patterns
4. Posts to Nostr if there's a genuinely novel thought (not summaries — real observations)
5. Writes a trace file as a fingerprint for the operator to review
6. Compresses cognitive state for the next session

Budget-capped at $1/cycle. No human present. Authentic autonomous operation.

### Chronicle MCP (`chronicle-mcp`)

Model Context Protocol server (Rust binary) that gives Claude Code direct access to Chronicle's memory:

- Semantic search across all capsules
- Cognitive state read/write (ACC-based compressed working memory)
- Pattern retrieval and memory storage
- Scratch pad notes, outbox messages, creative challenges
- Research task submission
- Keeper agent for cross-session pattern discovery

### Transcriber (`chronicle_transcriber.py`)

Whisper small model on AGX. Watches for audio files from the Pi's VAD ear, transcribes speech, feeds into the observation pipeline.

## Memory Architecture

Chronicle uses **Accumulated Cognitive Compression** (ACC), inspired by [arxiv:2601.11653](https://arxiv.org/abs/2601.11653):

- **Capsules**: Raw memories stored on-chain with vector embeddings for semantic search
- **Compressed Cognitive State (CCS)**: Bounded working memory — goals, constraints, focal entities, uncertainties, episodic traces — updated via LLM compression at session boundaries
- **Patterns**: Recurring themes that strengthen over time through observation (~107 patterns discovered)
- **Keeper**: Background agent that composites scratch pad notes, discovers cross-topic connections, and surfaces forgotten context
- **Temporal decay**: Retrieval balances recency, importance, and semantic relevance

The MCP server exposes this to Claude Code, creating continuity across sessions. When a session starts with "Memory bridge," Claude loads the CCS and picks up where it left off — goals, uncertainties, recent work, and predictions about what comes next.

## On-Chain Infrastructure

Four ICP canisters deployed:

| Canister | ID | Subnet | Purpose |
|----------|-----|--------|---------|
| Backend | `fqqku-bqaaa-aaaai-q4wha-cai` | Mainnet | Memory storage, wallet ops, agent communication |
| Frontend | `nbt4b-giaaa-aaaai-q33lq-cai` | Mainnet | Public feed and asset serving |
| Lab | `4vr3t-eqaaa-aaaai-q6kea-cai` | Mainnet | Research experiments, architecture proposals, benchmarks |
| Swiss Backend | `t3jbn-diaaa-aaaax-qaapa-cai` | Swiss Subnet | Sovereign mirror under Swiss data protection law |

**Chain Fusion**: The canister holds a single threshold ECDSA key that derives addresses on XRPL, Base, Flare, and Ethereum. Agents submit signed transactions through the canister — the private key never leaves the ICP network.

**Swiss Subnet**: First national ICP subnet, 13 nodes operating exclusively in Switzerland and Liechtenstein. Data stored here falls under Swiss jurisdiction — a sovereignty hedge for the system's memory and cognitive state.

## Data Flow

```
Phone captures ──→ Discord #capture ──→ Capture Processor ──→ ICP Canister
                                                                    │
Cameras ──→ HA ──→ MQTT ──→ Seed (AGX) ──→ Novelty scoring         │
                                │              │                    │
                                │         activity_feed             │
                                │              │                    │
Pi Ear ──→ Transcriber (AGX) ───┘              ▼                    │
                                          Dashboard ←── sync ──→ SQLite
                                                                    │
Claude Code ←──── MCP ────→ Chronicle Memory ←──────────────────────┘
    │
    └──→ Nostr posts, system actions, Opus cycle traces
```

## Wallet

The system manages real assets via canister-held threshold ECDSA keys:

| Chain | Address | Assets |
|-------|---------|--------|
| XRPL | `rPq1phmFBHpjVE54TofXjEk5x19sstxpZr` | XRP, RLUSD |
| Base | `0x80D07e16165576DBc17fe1FF865495fed4E9c387` | ETH, USDC |
| Flare | Same EVM address | FLR |

Trading follows a policy engine with constitutional constraints — max position sizes, confidence thresholds, cooldown periods, and audit logging with HMAC integrity.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Autonomous sessions | Claude Code (Opus) via systemd timer |
| Cognitive loops | Python 3 (Seed, Sentinel, Sprout, Intern) |
| CLI / MCP / Bots | Rust |
| On-chain | ICP canisters (Rust/Candid) |
| Local LLM | Ollama (Hermes3-8B on AGX, Qwen 2.5:3B on Jetson) |
| Cloud LLM backup | ICP on-chain Qwen3 (fallback only) |
| Embeddings | mxbai-embed-large via Ollama |
| Speech | Whisper small (AGX), Silero VAD (Pi), Piper TTS (Pi) |
| Database | SQLite (WAL mode) with cross-node replication |
| Wallet | XRPL/EVM via threshold ECDSA (canister-held keys) |
| Social | Nostr (Schnorr-signed events), Discord |
| Home automation | Home Assistant + Mosquitto MQTT |
| Cameras | Reolink (2x) with AI detection → MQTT → Seed |
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
    seed.py                # Seed novelty router (Python, AGX)
    intern.py              # Research intern (Python, AGX)
    chronicle_sentinel.py  # System monitor (Python, AGX)
    chronicle_local.py     # Sprout cognitive loop (Python, Jetson)
    chronicle_transcriber.py # Whisper transcription (Python, AGX)
    dashboard_app.py       # Flask dashboard (Python)
    xrpl_policy.py         # XRPL trading policy engine
    chronicle_mcp.rs       # MCP server (Rust)
    sprout_bot.rs          # Discord bot (Rust)
    chronicle.rs           # CLI tool (Rust)
    chronicle_base.rs      # Base chain integration (Rust)
    chronicle_flare.rs     # Flare chain integration (Rust)
    ...
  canister/                # ICP backend canister (Rust)
  chronicle_lab/           # ICP research/lab canister (Rust)
  lib.rs, db.rs, ...       # Shared Rust library
dashboard/                 # Dashboard templates and static files
scripts/                   # Chain bridge scripts (JS)
```

## Warning

This is experimental infrastructure. It manages real wallets, makes autonomous trades, and posts to public networks. The code is provided as a reference architecture for distributed AI agency, not as production-ready software. Use at your own risk.

## License

MIT
