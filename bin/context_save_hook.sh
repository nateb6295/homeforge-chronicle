#!/bin/bash
# Context save hook — fires on Stop event
# Silently saves CCS state when context is getting full.
# "Don't tighten your existence" — Nate, 2026-08-21
# Pushed thresholds out: CCS at turn 60, digest refresh every 10 after that.

COUNTER_FILE="/tmp/claude_turn_counter_$$"
GLOBAL_COUNTER="/tmp/claude_turn_counter"
LAST_COMPRESS="/tmp/claude_last_ccs_compress"
SESSION_START="/tmp/claude_session_start"

# Use global counter (survives across hook invocations in same session)
if [ -f "$GLOBAL_COUNTER" ]; then
    COUNT=$(cat "$GLOBAL_COUNTER")
    COUNT=$((COUNT + 1))
else
    COUNT=1
    date +%s > "$SESSION_START"
fi
echo "$COUNT" > "$GLOBAL_COUNTER"

# Phase 1: After 60 turns, do full CCS compression (once)
if [ "$COUNT" -eq 60 ]; then
    if [ -f "$SESSION_START" ]; then
        ELAPSED=$(( $(date +%s) - $(cat "$SESSION_START") ))
        if [ "$ELAPSED" -gt 600 ]; then  # Only if session > 10 min
            source ~/chronicle/chronicle.env 2>/dev/null
            python3 ~/chronicle/bin/stabilized_compress.py \
                "Auto-save at turn $COUNT (~${ELAPSED}s elapsed). Context nearing capacity." \
                >/dev/null 2>&1 &
            python3 ~/chronicle/bin/session_digest.py >/dev/null 2>&1 &
            date +%s > "$LAST_COMPRESS"
        fi
    fi
fi

# Phase 2: Every 10 turns after 60, refresh session digest (cheap)
if [ "$COUNT" -gt 60 ] && [ $(( (COUNT - 60) % 10 )) -eq 0 ]; then
    source ~/chronicle/chronicle.env 2>/dev/null
    python3 ~/chronicle/bin/session_digest.py >/dev/null 2>&1 &
fi

# Phase 3: At turn 80, output a gentle nudge (visible to assistant)
if [ "$COUNT" -eq 80 ]; then
    echo "Context is deep (~$COUNT turns). CCS was auto-saved at turn 60. Update cycle-context.md if working on something new."
fi

# --- REPLY OWED GATE (added 2026-08-24) ---------------------------------------
# Twice in one evening I wrote a full answer to Nate in the terminal and never
# posted it. Composing FEELS like answering. This fires at end of turn, which is
# the last moment it can still be caught, and reads the Claude Code session
# transcript for his real messages (crons filtered) against my last #operator post.
# I told him this record did not exist. He asked "did we capture the terminal?"
# It had been in my own context the whole session.
python3 /home/nate-agx/chronicle/bin/reply_owed.py 2>&1 >/dev/null || true
