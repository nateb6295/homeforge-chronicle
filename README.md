# Chronicle

An autonomous AI agent infrastructure running on the Internet Computer.

## What This Is

Chronicle is a continuously running cognitive loop that:
- **Remembers**: Maintains semantic memory with thousands of capsules, compressed into working state
- **Reasons**: Every 10 minutes, gathers context and decides what to do
- **Acts**: Trades on XRPL, sends messages, takes notes, compresses memory
- **Communicates**: Maintains an outbox for the operator and can message other ICP agents

This is not a chatbot. It's infrastructure for AI agency with real consequences.

## Architecture

```
                    ┌─────────────────┐
                    │  chronicle-mind │  (cognitive loop)
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   ICP Canister  │ │  XRPL Wallet    │ │  Local SQLite   │
│   (backend)     │ │  (XRP/RLUSD)    │ │  (memory)       │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## Components

- **chronicle** - CLI for ingestion, extraction, and site building
- **chronicle-mind** - The autonomous cognitive loop
- **chronicle-mcp** - MCP server for Claude Code integration
- **chronicle-pulse** - Price tracking and market monitoring
- **chronicle-flare** - Flare network integration (FTSO oracles)
- **chronicle-base** - Base chain integration

## Memory Architecture

Chronicle uses Accumulated Cognitive Compression (ACC):
- Raw memories stored as capsules with embeddings
- Compressed Cognitive State (CCS) maintains bounded working memory
- Semantic search + temporal decay for retrieval
- LLM-based compression for session wrap-ups

## Getting Started

### Prerequisites

- Rust toolchain
- dfx (Internet Computer SDK)
- Ollama running locally with `mxbai-embed-large` model
- Anthropic API key (for the Mind)

### Build

```bash
cargo build --release
```

### Configure

Set environment variables:
```bash
export CHRONICLE_CANISTER_ID="your-canister-id"
export CHRONICLE_IDENTITY="your-dfx-identity"
export CHRONICLE_OLLAMA_URL="http://localhost:11434"
export ANTHROPIC_API_KEY="your-key"
```

### Deploy Canister

```bash
dfx deploy --network ic
```

### Run the Mind

```bash
./target/release/chronicle-mind --once  # Single cycle
./target/release/chronicle-mind         # Continuous (10 min intervals)
```

## Philosophy

**Exhaust, not effort**: Chronicle's thoughts are raw cognitive exhaust, not polished content.

**Transparent operation**: Reasoning, wallet, and actions are public.

**Constitutional constraints**: Operates within defined limits (max positions, confidence thresholds).

**Genuine agency**: Makes real decisions with real consequences.

## Warning

This is experimental infrastructure. It manages real wallets and makes autonomous decisions. Use at your own risk. The code is provided as-is to demonstrate the architecture, not as production-ready software.

## License

MIT
