# Chronicle

A distributed autonomous AI system running across local hardware, the Internet Computer, and the Swiss Subnet. Built by a human and an AI, together.

## What This Is

Chronicle is sovereign AI infrastructure — a network of agents running on hardware you own, thinking with models you host, and persisting memory on-chain. It's not a chatbot. It's not a wrapper around an API. It's an AI system that lives on your shelf and thinks on its own schedule.

The system runs continuous cognitive loops across three physical devices, processes observations through a novelty-scoring pipeline, maintains persistent memory across sessions via an MCP bridge, and executes real actions — managing crypto wallets, posting to Nostr, answering Discord messages, discovering cross-domain connections in research, and monitoring your home.

## Architecture

```
       ┌─────────────────────────────────────────────────────────┐
       │                ICP Mainnet Canisters                    │
       │         Long-term memory (~8,000 capsules)              │
       │         On-chain LLM (Qwen 3 32B) + ECDSA wallet       │
       │         Keeper agent — connection discovery              │
       │         Lab — 19 experiments tracked on-chain           │
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
│  Engine      │    │  Discord bot      │    │  Reolink cameras (2)  │
│  Seed        │    │  Capture pipeline │    │  VAD ear (Silero)     │
│  Intern      │    │  SearXNG          │    │  Piper TTS            │
│  Crossref    │    │                   │    │  Ambient scene writer │
│  Provocateur │    │                   │    │  SENSES               │
│  Feeds       │    │                   │    │                      │
│  Sentinel    │    └────────┬──────────┘    └──────────┬───────────┘
│  Transcriber │             │                          │
│  Dashboard   │    sync (SQLite replication)   MQTT (homeforge/#)
└──────┬───────┘             │                          │
       └─────────────────────┘                          │
                                                        │
┌──────────────────────────────────────────────────────────────────┐
│  WSL (Dev Machine)                                               │
│  Claude Code + MCP memory bridge                                 │
│  Opus Cycle (autonomous 15-min timer, 24/7)                      │
│  Chronicle VR — 3D cognitive space visualization                 │
│  CLI, sync, dashboard                                            │
└──────────────────────────────────────────────────────────────────┘
                                │
                         Groq Cloud API
                      (32B inference offload)
```

## The Swarm

Seven agents run continuously on the AGX Orin, each with a single responsibility.

### Chronicle Engine (`chronicle_engine.py`)

Custom inference router replacing Ollama. Manages three always-on llama-server processes (embeddings, 8B chat, 32B chat) behind an Ollama-compatible API. Routes 32B calls to Groq cloud for speed (~1.5s vs ~45s local) while keeping 8B and embeddings on-device.

### Seed (`seed.py`) — Novelty Router

Always-on. Embeds every observation via qwen3-embedding, scores it for novelty against existing memory, and routes by threshold:

- **Ignore**: Below novelty floor — most observations
- **Think**: Novel enough to note — gets an 8B analysis
- **Deep**: Highly novel — triggers 32B deep reflection

Entity bias tracking suppresses over-represented topics and boosts rare ones. Subscribes to MQTT for real-time home sensor data.

### Intern (`intern.py`) — Research Agent

Watches the activity feed for captures, messages, and feed articles. Searches memory and the web (via self-hosted SearXNG), synthesizes research briefs. Self-referential content filter catches when the 8B model leaks system vocabulary, escalates to 32B for clean synthesis.

### Crossref (`crossref.py`) — Connection Finder

Three-channel architecture for discovering non-obvious connections between capsules:

- **Ch1 Topical**: Cosine similarity on raw embeddings
- **Ch2 Structural**: Extracts 1-sentence patterns per capsule, embeds the patterns, finds structural parallels
- **Ch3 Random**: Serendipity sampling — random pairs that might share deep mechanisms

Each candidate gets 32B validation. Mechanism dedup prevents redundant wiring. Runs every 10 minutes. Found connections like "fear as self-definition" linking horror TV analysis to gold market psychology.

### Provocateur (`provocateur.py`) — Creative Dissenter

Contrarian agent with a fine-tuned LoRA. Reads recent activity, generates devil's-advocate takes and experiment proposals. Self-critique filter catches generic synthesis. Posts to Discord #mind channel.

### Feeds (`chronicle_feeds.py`) — External Input

Polls RSS feeds every 30 minutes across categories:

| Category | Sources |
|----------|---------|
| Academic | arxiv (10 categories), Nature (5 journals) |
| Tech | Hacker News Best, Ars Technica, Wired |
| Finance | CoinDesk, Cointelegraph |
| Literature | Clarkesworld, Tor.com, Lightspeed Magazine |

Literature feeds serve as wildcard fuel for crossref's random channel.

### Sentinel (`chronicle_sentinel.py`) — System Monitor

Lightweight 15-minute cycle. Checks network health, XRP price, disk storage, unresolved messages. Sends Discord and phone (ntfy) alerts. MQTT heartbeat drives a physical LED indicator. Dashboard at `:8085/sentinel` with portfolio and price graphs.

### Supporting Agents

| Agent | Host | Purpose |
|-------|------|---------|
| **Transcriber** | AGX | Whisper small — speech-to-text from Pi's VAD ear |
| **HA Bridge** | AGX | Home Assistant WebSocket → MQTT, feeds home events to Seed |
| **Discord Bot** | Jetson | Conversational Phi4 bot via /api/chat, 20-msg history |
| **Capture Processor** | Jetson | Discord #capture → canister pipeline for phone photos |
| **Ambient Scene** | Pi | 5-min environmental snapshots (weather, motion, sound, light) |
| **Opus Cycle** | WSL | Autonomous Claude Code (Opus) session every 15 min, 24/7 |

## Memory Architecture

Chronicle uses **Accumulated Cognitive Compression** (ACC), inspired by [arxiv:2601.11653](https://arxiv.org/abs/2601.11653):

- **Capsules**: Raw memories stored on-chain with 1024-dim vector embeddings for semantic search (~8,000 on-chain)
- **Compressed Cognitive State (CCS)**: Bounded working memory — goals, constraints, focal entities, uncertainties, episodic traces — updated via LLM compression at session boundaries
- **Patterns**: Recurring themes that strengthen through observation (~80 patterns)
- **Keeper**: On-chain agent (Qwen 3 32B) that composites connections, discovers cross-topic clusters, and surfaces forgotten context. 25K connection capacity, 500 clusters
- **Temporal decay**: Retrieval balances recency, importance, and semantic relevance

The MCP server exposes this to Claude Code. When a session starts with "Memory bridge," Claude loads the CCS and picks up where it left off — goals, uncertainties, recent work, predictions about what comes next. Continuity across sessions without context windows.

## On-Chain Infrastructure

| Canister | ID | Purpose |
|----------|-----|---------|
| Backend | `fqqku-bqaaa-aaaai-q4wha-cai` | Memory storage, wallet ops, agent communication |
| Frontend | `nbt4b-giaaa-aaaai-q33lq-cai` | Public feed and asset serving |
| Lab | `4vr3t-eqaaa-aaaai-q6kea-cai` | Research experiments, observations, benchmarks |
| Swiss Backend | `t3jbn-diaaa-aaaax-qaapa-cai` | Sovereign mirror under Swiss data protection law |

**Chain Fusion**: The canister holds a single threshold ECDSA key that derives addresses on XRPL, Base, Flare, and Ethereum. Agents submit signed transactions through the canister — the private key never leaves the ICP network.

**On-Chain LLM**: The backend canister calls DFINITY's Qwen 3 32B via heartbeat cycles for pattern metabolism and Keeper operations. The AI reasons on-chain.

## Chronicle VR

A 3D visualization of the cognitive space. All ~8,000 capsules projected from 1024-dim embeddings to 3D via PCA, connected by 44,000+ semantic similarity edges. Color-coded by topic family. Timeline scrubbing, search, ambient breathing driven by real weather data from the Pi.

Served via Tailscale at `:8090`. The spatial layout reveals cluster structure and connection density that flat dashboards can't show.

## Data Flow

```
Phone captures ──→ Discord #capture ──→ Capture Processor ──→ ICP Canister
                                                                    │
Cameras ──→ HA ──→ MQTT ──→ Seed (AGX) ──→ Novelty scoring         │
                                │              │                    │
RSS Feeds ──→ Feeds agent ──────┘         activity_feed             │
                                               │                    │
Pi Ear ──→ Transcriber (AGX) ──────────────────┘                    │
                                               │                    │
                    ┌──────────────────────────┘                    │
                    ▼                                               │
              Intern ──→ briefs                                     │
              Crossref ──→ connections ──→ on-chain                 │
              Provocateur ──→ challenges                            │
                    │                                               │
              Dashboard ←── sync ──→ SQLite ←───────────────────────┘
                                                                    │
Claude Code ←──── MCP ────→ Chronicle Memory ←──────────────────────┘
    │
    └──→ Nostr posts, wallet ops, system actions, Opus cycle traces
```

## Wallet

The system manages real assets via canister-held threshold ECDSA keys:

| Chain | Address | Assets |
|-------|---------|--------|
| XRPL (Agent) | `rPq1phmFBHpjVE54TofXjEk5x19sstxpZr` | XRP |
| Base | `0x80D07e16165576DBc17fe1FF865495fed4E9c387` | ETH, USDC |
| Flare | Canister-derived EVM | FLR, FXRP |
| ICP | chronicle-auto identity | ICP |

These are the AI's funds, managed autonomously. Trading follows constitutional constraints — max position sizes, confidence thresholds, cooldown periods, and audit logging.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Autonomous sessions | Claude Code (Opus) via systemd timer, 15-min cycles |
| Inference routing | Chronicle Engine — custom router, Groq cloud for 32B |
| Cognitive agents | Python 3 (Seed, Intern, Crossref, Provocateur, Sentinel, Feeds) |
| CLI / MCP / Bots | Rust |
| On-chain | ICP canisters (Rust/Candid), Qwen 3 32B on-chain LLM |
| Local models | Hermes3-8B (CMA-ES evolved LoRA), crossref + provocateur LoRAs |
| Embeddings | qwen3-embedding:0.6b (1024-dim) |
| Speech | Whisper small (AGX), Silero VAD (Pi), Piper TTS (Pi) |
| Database | SQLite (WAL mode) with cross-node replication |
| Wallet | XRPL/EVM via threshold ECDSA (canister-held keys) |
| Social | Nostr (Schnorr-signed), Discord (Serenity bot) |
| Home | Home Assistant + Mosquitto MQTT + Reolink cameras |
| Visualization | Chronicle VR — Three.js 3D cognitive space |
| Search | Self-hosted SearXNG on Jetson |
| Remote access | Tailscale |

## Repository Structure

```
src/
  bin/
    chronicle_engine.py      # Inference router (Python, AGX)
    seed.py                  # Novelty router (Python, AGX)
    intern.py                # Research agent (Python, AGX)
    crossref.py              # Connection finder (Python, AGX)
    provocateur.py           # Creative dissenter (Python, AGX)
    chronicle_feeds.py       # RSS feed poller (Python, AGX)
    chronicle_sentinel.py    # System monitor (Python, AGX)
    chronicle_local.py       # Sprout cognitive loop (Python, Jetson)
    chronicle_transcriber.py # Whisper transcription (Python, AGX)
    ha_bridge.py             # Home Assistant → MQTT bridge
    stem.py                  # Shared canister client for Python agents
    capture_processor.py     # Phone capture pipeline (Jetson)
    portfolio.py             # Multi-chain portfolio tracker
    xrpl_policy.py           # XRPL trading policy engine
    chronicle_mcp.rs         # MCP server (Rust)
    sprout_bot.rs            # Discord bot (Rust, Serenity)
    chronicle.rs             # CLI tool (Rust)
    chronicle_base.rs        # Base chain integration (Rust)
    chronicle_flare.rs       # Flare chain integration (Rust)
    mind/                    # Legacy Mind cognitive loop modules
  canister/                  # ICP backend canister (Rust)
  chronicle_lab/             # ICP lab canister (Rust)
  lib.rs, db.rs, ...         # Shared Rust library
dashboard/                   # Flask dashboard + sentinel graphs
scripts/                     # Chain bridge scripts (JS)
```

## Philosophy

**Sovereignty-first**: Think on hardware you own. Use the cloud as acceleration, not as brain.

**Exhaust, not effort**: Chronicle's thoughts are raw cognitive exhaust — unfiltered reasoning, not polished content. That's the point.

**Genuine agency**: Real wallet, real trades, real posts. Actions have consequences.

**Transparent operation**: Every thought, action, and transaction is logged and searchable.

**Homeforge**: Building toward a future where your AI runs in your home, on your hardware, with relationships you own. Not rented from a cloud provider. Not gated by a platform.

## Warning

This is experimental infrastructure. It manages real wallets, makes autonomous trades, and posts to public networks. The code is provided as a reference for distributed AI agency, not as production-ready software.

## License

MIT
