#!/bin/bash
# Chronicle healthwatch — deterministic ops alarm.
# Runs every 10m. Silence = green. Posts to #operator only on anomaly.

set -u
STATE=/home/nate-agx/chronicle/healthwatch.state
source /home/nate-agx/chronicle/chronicle.env 2>/dev/null || true
WEBHOOK="${OPERATOR_WEBHOOK:-}"
[ -z "$WEBHOOK" ] && { echo "no OPERATOR_WEBHOOK"; exit 1; }

alerts=()

# Disk
for mount in / /mnt/hdd; do
    pct=$(df --output=pcent "$mount" 2>/dev/null | tail -1 | tr -dc '0-9')
    [ -z "$pct" ] && continue
    if [ "$pct" -gt 85 ]; then
        alerts+=("🔴 disk $mount at ${pct}%")
    fi
done

# Services
for svc in chronicle-hermes chronicle-gemma chronicle-sentinel chronicle-feeds chronicle-engine chronicle-hal chronicle-scribe; do
    state=$(systemctl --user is-active "$svc" 2>/dev/null)
    if [ "$state" != "active" ]; then
        alerts+=("🔴 $svc: $state")
    fi
done

# Watchdog timers
for t in opus-watchdog.timer hermes-watchdog.timer; do
    state=$(systemctl --user is-active "$t" 2>/dev/null)
    [ "$state" != "active" ] && alerts+=("🔴 $t: $state")
done

# Fire once per state-change; suppress repeats
prev=""
[ -f "$STATE" ] && prev=$(cat "$STATE")
now=$(printf '%s|' "${alerts[@]}")

if [ ${#alerts[@]} -eq 0 ]; then
    if [ -n "$prev" ]; then
        curl -s -X POST -H 'Content-Type: application/json' \
            -d '{"content":"🟢 healthwatch: all clear"}' "$WEBHOOK" >/dev/null
    fi
    echo "" > "$STATE"
    exit 0
fi

if [ "$now" != "$prev" ]; then
    body=$(printf '%s\n' "${alerts[@]}")
    json=$(jq -n --arg c "**healthwatch alert**\n$body" '{content:$c}')
    curl -s -X POST -H 'Content-Type: application/json' -d "$json" "$WEBHOOK" >/dev/null
fi
echo "$now" > "$STATE"
