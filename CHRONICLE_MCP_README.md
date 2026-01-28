# Chronicle MCP - Persistent Memory for Claude

Chronicle MCP is a Model Context Protocol server that gives Claude persistent memory across sessions. It implements the ACC (Adaptive Context Compression) architecture - bounded cognitive state that compresses and evolves rather than just accumulating.

## What It Does

- **Compressed Cognitive State (CCS)**: A bounded "working memory" containing goals, focal entities, recent events, uncertainties, and relational context
- **Semantic Memory**: Store and retrieve facts, decisions, and observations with semantic search
- **Pattern Detection**: Recurring themes strengthen over time, weak patterns decay
- **Session Continuity**: Start any session with "Memory Bridge" to restore context

## Quick Start

### 1. Build the Binary

```bash
git clone https://github.com/yourusername/homeforge-chronicle.git
cd homeforge-chronicle
cargo build --release --bin chronicle-mcp
```

### 2. Configure Claude Code

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "chronicle-memory": {
      "command": "/path/to/chronicle-mcp",
      "args": []
    }
  }
}
```

### 3. Configure Claude Desktop (Optional)

Add to Claude Desktop's MCP config (location varies by OS):

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
**Linux**: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "chronicle-memory": {
      "command": "/path/to/chronicle-mcp",
      "args": []
    }
  }
}
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CHRONICLE_OLLAMA_URL` | `http://localhost:11434` | Ollama server for embeddings |
| `CHRONICLE_EMBEDDING_MODEL` | `mxbai-embed-large` | Embedding model to use |
| `CHRONICLE_CANISTER_ID` | (none) | Optional ICP canister for chain sync |

## Tools Available

### Core Memory

- **`search_memory`** - Semantic search across stored memories
- **`store_memory`** - Persist a fact, decision, or observation
- **`get_recent_context`** - Get recent memories chronologically
- **`surface_context`** - Proactively find relevant memories for current topic

### Cognitive State (ACC)

- **`get_cognitive_state`** - Load compressed cognitive state (the "Memory Bridge")
- **`update_cognitive_state`** - Manually update state fields
- **`compress_cognitive_state`** - LLM-powered compression of session into CCS

### Patterns

- **`get_patterns`** - Retrieve emerging themes with confidence scores
- **`memory_health`** - Check system status

### Session Management

- **`session_wrapup`** - Batch store memories at end of session

## The Memory Bridge Protocol

Start any session by saying "Memory Bridge" and Claude will:

1. Load the Compressed Cognitive State
2. Check system health
3. Retrieve active patterns
4. Report like "waking up" - what was happening, what's next

This creates continuity across sessions without context accumulation.

## Architecture

```
┌─────────────────────────────────────────┐
│            Claude Session               │
└────────────────┬────────────────────────┘
                 │ MCP Protocol
┌────────────────▼────────────────────────┐
│          Chronicle MCP Server           │
│                                         │
│  ┌─────────────┐  ┌─────────────────┐  │
│  │     CCS     │  │    Patterns     │  │
│  │  (bounded)  │  │ (decay/growth)  │  │
│  └─────────────┘  └─────────────────┘  │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │       Semantic Memory           │   │
│  │    (embeddings + capsules)      │   │
│  └─────────────────────────────────┘   │
└────────────────┬────────────────────────┘
                 │
    ┌────────────┴────────────┐
    │                         │
┌───▼───┐              ┌──────▼──────┐
│SQLite │              │ ICP Canister│
│(local)│              │ (optional)  │
└───────┘              └─────────────┘
```

## Local-First Philosophy

Chronicle works entirely locally by default:
- SQLite database in `~/.homeforge-chronicle/`
- No cloud dependencies required
- Optional: Ollama for semantic embeddings
- Optional: ICP canister for on-chain persistence

## License

MIT
