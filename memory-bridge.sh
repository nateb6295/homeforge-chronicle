#!/bin/bash
# Memory Bridge - Quick connection test and status
# Usage: source this or run directly

CHRONICLE_MCP="/home/bradf/projects/homeforge-chronicle/target/release/chronicle-mcp"

export CHRONICLE_CANISTER_ID="fqqku-bqaaa-aaaai-q4wha-cai"
export CHRONICLE_IDENTITY="chronicle-auto"
export CHRONICLE_OLLAMA_URL="http://192.168.1.11:11434"
export CHRONICLE_EMBEDDING_MODEL="gemma2:2b"

# Quick health check via JSON-RPC
echo '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"bridge-test","version":"1.0"}},"id":1}
{"jsonrpc":"2.0","method":"tools/call","params":{"name":"memory_health","arguments":{}},"id":2}' | \
timeout 30 "$CHRONICLE_MCP" 2>/dev/null | \
grep -o '"status":"[^"]*"' | head -1
