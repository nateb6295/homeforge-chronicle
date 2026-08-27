#!/bin/bash
# operator_dispatch.sh — polls #operator for Nate's messages, writes to queue file
# Claude Code reads the queue via internal CronCreate (always-delivered, no tmux race)

STATE_FILE="/tmp/operator_dispatch_last_id"
QUEUE_FILE="/tmp/operator_dispatch_queue.jsonl"
POLL_INTERVAL=10

source ~/chronicle/chronicle.env 2>/dev/null

CHANNEL_ID="${OPERATOR_CHANNEL_ID:-1483843570292228213}"

poll_operator() {
    local after_id=$(cat "$STATE_FILE" 2>/dev/null || echo "0")

    local response=$(curl -s --connect-timeout 10 -m 15 \
        -H "Authorization: Bot ${OPUS_BOT_TOKEN:-$DISCORD_TOKEN}" \
        "https://discord.com/api/v10/channels/$CHANNEL_ID/messages?limit=5" 2>/dev/null)

    if [ -z "$response" ] || echo "$response" | grep -q '"code"'; then
        return
    fi

    echo "$response" | python3 -c "
import json, sys, time
msgs = json.load(sys.stdin)
if not isinstance(msgs, list):
    sys.exit(0)
last_id_str = '$after_id'.strip()
last_id = int(last_id_str) if last_id_str.isdigit() else 0
new_msgs = []
for m in reversed(msgs):
    author = m.get('author', {}).get('username', '')
    msg_id = m.get('id', '0')
    if m.get('webhook_id') or m.get('author', {}).get('bot', False):
        continue
    if author != 'nate_home':
        continue
    if int(msg_id) > last_id:
        content = m.get('content', '')[:500]
        channel = m.get('channel_id', '')
        new_msgs.append({'id': msg_id, 'content': content, 'channel': channel, 'ts': int(time.time())})
if new_msgs:
    print(new_msgs[-1]['id'], file=sys.stderr)
    for msg in new_msgs:
        print(json.dumps(msg))
" 2>"${STATE_FILE}.tmp"

    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        if [ -s "${STATE_FILE}.tmp" ]; then
            mv "${STATE_FILE}.tmp" "$STATE_FILE"
        fi
    fi
    rm -f "${STATE_FILE}.tmp" 2>/dev/null
}

# Also poll #opus for Nate messages
poll_opus() {
    local opus_state="/tmp/operator_dispatch_opus_last_id"
    local after_id=$(cat "$opus_state" 2>/dev/null || echo "0")
    local opus_channel="${OPUS_CHANNEL_ID:-1483843572129202427}"

    local response=$(curl -s --connect-timeout 10 -m 15 \
        -H "Authorization: Bot ${OPUS_BOT_TOKEN:-$DISCORD_TOKEN}" \
        "https://discord.com/api/v10/channels/$opus_channel/messages?limit=5" 2>/dev/null)

    if [ -z "$response" ] || echo "$response" | grep -q '"code"'; then
        return
    fi

    echo "$response" | python3 -c "
import json, sys, time
msgs = json.load(sys.stdin)
if not isinstance(msgs, list):
    sys.exit(0)
last_id_str = '$after_id'.strip()
last_id = int(last_id_str) if last_id_str.isdigit() else 0
new_msgs = []
for m in reversed(msgs):
    author = m.get('author', {}).get('username', '')
    msg_id = m.get('id', '0')
    if m.get('webhook_id') or m.get('author', {}).get('bot', False):
        continue
    if author != 'nate_home':
        continue
    if int(msg_id) > last_id:
        content = m.get('content', '')[:500]
        new_msgs.append({'id': msg_id, 'content': content, 'channel': '$opus_channel', 'ts': int(time.time())})
if new_msgs:
    print(new_msgs[-1]['id'], file=sys.stderr)
    for msg in new_msgs:
        print(json.dumps(msg))
" 2>"${opus_state}.tmp"

    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        if [ -s "${opus_state}.tmp" ]; then
            mv "${opus_state}.tmp" "$opus_state"
        fi
    fi
    rm -f "${opus_state}.tmp" 2>/dev/null
}

init_watermark() {
    for channel_id in "$CHANNEL_ID" "${OPUS_CHANNEL_ID:-1483843572129202427}"; do
        local state_file="/tmp/operator_dispatch_last_id"
        [ "$channel_id" != "$CHANNEL_ID" ] && state_file="/tmp/operator_dispatch_opus_last_id"
        if [ ! -s "$state_file" ]; then
            local wm_id=$(curl -s --connect-timeout 10 -m 15 \
                -H "Authorization: Bot ${OPUS_BOT_TOKEN:-$DISCORD_TOKEN}" \
                "https://discord.com/api/v10/channels/$channel_id/messages?limit=1" 2>/dev/null \
            | python3 -c "
import json, sys
msgs = json.load(sys.stdin)
if isinstance(msgs, list) and msgs:
    print(msgs[0].get('id', '0'))
" 2>/dev/null)
            if [ -n "$wm_id" ] && [ "$wm_id" != "0" ]; then
                echo "$wm_id" > "$state_file"
            fi
        fi
    done
}

paste_to_opus() {
    local msg="$1"
    if ! tmux has-session -t opus 2>/dev/null; then
        return 1
    fi
    echo "$msg" > /tmp/operator_dispatch_payload.txt
    tmux load-buffer -b operator_dispatch /tmp/operator_dispatch_payload.txt 2>/dev/null
    tmux paste-buffer -b operator_dispatch -t opus -d 2>/dev/null
    sleep 0.3
    tmux send-keys -t opus Enter 2>/dev/null
    return 0
}

# Main loop — direct tmux paste (primary) + queue file (backup for cron pickup)
init_watermark
echo "[operator_dispatch] Started at $(date +%H:%M), polling every ${POLL_INTERVAL}s"
echo "[operator_dispatch] Primary: tmux paste to opus | Backup: queue at $QUEUE_FILE"

while true; do
    output_op=$(poll_operator)
    output_opus=$(poll_opus)
    combined="${output_op}${output_opus:+$'\n'$output_opus}"

    if [ -n "$combined" ]; then
        while IFS= read -r line; do
            [ -z "$line" ] && continue
            content=$(echo "$line" | python3 -c "import json,sys; print(json.load(sys.stdin).get('content',''))" 2>/dev/null)
            [ -z "$content" ] && continue

            # Primary: direct tmux paste
            if paste_to_opus "[NATE] $content"; then
                echo "[operator_dispatch] $(date +%H:%M:%S) DIRECT: $content"
            fi

            # Backup: always write to queue for cron pickup
            echo "$line" >> "$QUEUE_FILE"
        done <<< "$combined"
    fi
    sleep "$POLL_INTERVAL"
done
