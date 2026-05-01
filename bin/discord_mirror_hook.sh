#!/usr/bin/env bash
# discord_mirror_hook.sh — Stop-hook that mirrors the last assistant
# turn's text to the operator Discord channel.
#
# Reasoning: 2026-04-29 Nate caught me drifting to in-session-reply 4+
# times despite naming the pattern. Per self-model #296 (discord_mirror_default,
# actively_externally_held), the substrate-side rewiring isn't reliable.
# Infrastructure enforcement instead.
#
# Skips:
# - Empty assistant text (pure tool-only turn)
# - Turns that already called post_operator.sh (avoid double-post)
# - Turns where post_operator.sh was called via Bash (same as above)
#
# Output:
# - Posts text to OPERATOR_WEBHOOK with "🟢 [mirror]" prefix
# - Truncates to 1900 chars to leave room for prefix
#
set -uo pipefail

# Read JSON from stdin (Stop hook input)
INPUT="$(cat)"
SESSION_ID="$(echo "$INPUT" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(d.get("session_id",""))' 2>/dev/null)"

if [ -z "$SESSION_ID" ]; then
    exit 0
fi

JSONL="$HOME/.claude/projects/-home-nate-agx-chronicle/$SESSION_ID.jsonl"
if [ ! -f "$JSONL" ]; then
    exit 0
fi

# Source webhook env
# shellcheck disable=SC1091
source "$HOME/chronicle/chronicle.env" 2>/dev/null || exit 0

if [ -z "${OPERATOR_WEBHOOK:-}" ]; then
    exit 0
fi

# Use python3 to do JSONL parsing (cleaner than jq for this)
python3 <<PYEOF
import json, os, sys, urllib.request

jsonl_path = os.environ.get("JSONL_PATH") or "$JSONL"
webhook = os.environ.get("OPERATOR_WEBHOOK") or "${OPERATOR_WEBHOOK}"

# Read all entries, find the last user message, then collect everything after
entries = []
with open(jsonl_path) as f:
    for line in f:
        try:
            entries.append(json.loads(line))
        except Exception:
            continue

# Find last user-message index
last_user_idx = -1
for i in range(len(entries) - 1, -1, -1):
    if entries[i].get("type") == "user":
        last_user_idx = i
        break

# Entries since last user message
since_user = entries[last_user_idx + 1:] if last_user_idx >= 0 else entries

# Check if post_operator.sh was called in this turn (via Bash tool)
already_posted = False
for e in since_user:
    if e.get("type") != "assistant":
        continue
    msg = e.get("message", {})
    for block in msg.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == "Bash":
            cmd = block.get("input", {}).get("command", "")
            if "post_operator.sh" in cmd:
                already_posted = True
                break
    if already_posted:
        break

if already_posted:
    sys.exit(0)

# Extract last assistant text content
last_assistant_text = ""
for e in since_user:
    if e.get("type") != "assistant":
        continue
    msg = e.get("message", {})
    parts = []
    for block in msg.get("content", []):
        if block.get("type") == "text":
            parts.append(block.get("text", ""))
    if parts:
        # Use the LAST assistant message in turn (most recent text block)
        last_assistant_text = "\n".join(parts)

if not last_assistant_text.strip():
    sys.exit(0)

# Truncate
prefix = "🟢 [mirror] "
max_body = 1900 - len(prefix)
body = last_assistant_text.strip()
if len(body) > max_body:
    body = body[:max_body - 1] + "…"

payload = json.dumps({"content": prefix + body}).encode()
req = urllib.request.Request(
    webhook,
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=8) as r:
        r.read()
except Exception as e:
    # Don't fail the hook on Discord errors
    sys.stderr.write(f"discord_mirror: post failed: {e}\n")
    sys.exit(0)
PYEOF

exit 0
