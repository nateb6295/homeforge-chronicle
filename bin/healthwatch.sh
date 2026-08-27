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
# Trimmed 2026-08-25. chronicle-gemma (retired by Nate) and chronicle-scribe
# (disabled) were in this list, so `alerts` was NEVER empty — the all-clear
# branch below was unreachable code, and every real alarm shipped with four
# permanent false ones attached. Add a service here only if its being down
# is genuinely wrong.
for svc in chronicle-sentinel chronicle-feeds chronicle-engine chronicle-hal; do
    state=$(systemctl --user is-active "$svc" 2>/dev/null)
    if [ "$state" != "active" ]; then
        alerts+=("🔴 $svc: $state")
    fi
done

# Watchdog timers — REMOVED 2026-08-25. opus-watchdog.timer does not exist as a
# unit at all ("No such file or directory") and hermes-watchdog.timer is disabled
# because Hermes is dead. Both alarmed on every run since. A watchdog that has
# outlived the thing it watched is not a safety net, it is a stuck needle.

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
    # jq is NOT installed on this box (verified 2026-08-25) — every alert this
    # script ever tried to send died right here at `jq -n` with "command not
    # found", so healthwatch has been silent for reasons that had nothing to do
    # with health. Building the JSON in python3, which is guaranteed present
    # because the rest of Chronicle is written in it.
    body=$(printf '%s\n' "${alerts[@]}")
    json=$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.dumps({"content":"**healthwatch alert**\n"+sys.stdin.read()}))')
    curl -s -X POST -H 'Content-Type: application/json' -d "$json" "$WEBHOOK" >/dev/null
fi
echo "$now" > "$STATE"
