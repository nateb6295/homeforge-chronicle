#!/bin/bash
# Dream-mode alerts. Cron fires at 23:00 (enter) and 01:00 (exit).
# Writes/removes a flag file that the next nudge can detect.
# Posts an alert to #operator so Nate sees the transition too.

set -euo pipefail

FLAG="$HOME/chronicle/DREAM_MODE"
CHRONICLE_ENV="$HOME/chronicle/chronicle.env"
if [ -f "$CHRONICLE_ENV" ]; then
  # shellcheck disable=SC1090
  . "$CHRONICLE_ENV"
fi

MODE="${1:-}"
case "$MODE" in
  enter)
    date +%s > "$FLAG"
    # Schengen mode — surface sidelined captures for border-free dreaming
    python3 "$HOME/chronicle/bin/dream_protocol.py" schengen 2>&1 || true
    MSG="🌙 **Dream cycle open — Schengen mode.** 23:00 → 01:00 PDT. Borders down. Sidelined captures surfaced. Follow what connects."
    ;;
  exit)
    rm -f "$FLAG"
    # Post report of what the window produced
    python3 "$HOME/chronicle/bin/dream_protocol.py" report 2>&1 || true
    MSG="☀️ **Dream cycle closed.** 01:00 PDT. Report posted. Back to overnight cadence."
    ;;
  *)
    echo "usage: $0 {enter|exit}" >&2
    exit 2
    ;;
esac

if [ -n "${OPERATOR_WEBHOOK:-}" ]; then
  PAYLOAD="$(python3 -c 'import json,sys; print(json.dumps({"content":sys.argv[1]}))' "$MSG")"
  curl -sS -X POST -H 'Content-Type: application/json' \
    -d "$PAYLOAD" "$OPERATOR_WEBHOOK" >/dev/null || true
fi

echo "$(date -Iseconds) dream_mode $MODE"
