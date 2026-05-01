#!/bin/bash
# Hermes watchdog — restart if no log activity for 15 minutes
# Run via systemd timer or cron every 2 minutes
#
# 2026-04-29 fix: bumped 300→900 (5min→15min) after diagnosing the original
# 5-min threshold was firing on long-running LLM calls (vision/inference)
# that legitimately don't write to log during the wait. The watchdog was
# mistaking "stuck in long inference" for "frozen agent" and SIGTERM'ing
# Hermes ~every 10-16 min throughout the day, killing mid-execution cron
# jobs (e.g., the 08:43:43 watchdog runner that fail-loud-alerted as a
# result). 15 min still catches genuine freezes; gives long calls room
# to complete.
#
# 2026-04-29 12:18: bumped 900→1200 (15min→20min) after the 11:35:57 fire
# at age=904s (threshold breached by 4s). Long arxiv-review or chatty-agent
# calls can run >15min; 20min still catches genuine freezes within 2 cycles.
# If THIS one fires again, the path is to detect "Hermes mid-LLM-call" and
# defer the stale-check rather than just bumping further.

LOG="/home/nate-agx/.hermes/logs/agent.log"
STALE_SECONDS=1200  # 20 minutes without a log line = stuck

if [ ! -f "$LOG" ]; then
    echo "$(date): Log file missing, restarting Hermes"
    systemctl --user restart chronicle-hermes
    exit 0
fi

last_mod=$(stat -c %Y "$LOG" 2>/dev/null || echo 0)
now=$(date +%s)
age=$((now - last_mod))

if [ "$age" -gt "$STALE_SECONDS" ]; then
    echo "$(date): Hermes log stale for ${age}s (threshold ${STALE_SECONDS}s), restarting"
    systemctl --user restart chronicle-hermes
else
    echo "$(date): Hermes healthy (log age ${age}s)"
fi
