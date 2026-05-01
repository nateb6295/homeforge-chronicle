#!/usr/bin/env bash
# Called by Opus at the top of each nudge cycle.
# If watchdog has dropped ROTATE_NOW, print a loud directive to rotate.
# If merely in the yellow/orange band, print a soft advisory.

set -u
FLAG=/home/nate-agx/chronicle/ROTATE_NOW

if [[ -f "$FLAG" ]]; then
  echo "=========================================="
  echo "⚠️  ROTATE_NOW FLAG IS PRESENT"
  echo "=========================================="
  cat "$FLAG"
  echo ""
  echo "Context >= 78%. Auto-compact will fire at 80% (CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=80)."
  echo "The model cannot trigger /exit programmatically — only Nate can from the terminal."
  echo ""
  echo "ACTIONS in priority order:"
  echo "  1. Save fresh state for either rotation path:"
  echo "     - python3 ~/chronicle/bin/checkpoint.py save \"...\""
  echo "     - python3 ~/chronicle/bin/carrying.py write \"...\""
  echo "  2. If Nate is at terminal: ask him to /exit (clean rotation path)"
  echo "  3. Otherwise: keep working until 80%; PreCompact hook saves state"
  echo "     and rotation_startup_hook injects Step 0 sequence on next prompt."
  echo "=========================================="
  exit 2   # non-zero exit signals the caller (Opus) that action required
fi

# Otherwise print current state succinctly.
python3 /home/nate-agx/chronicle/bin/context_meter.py
exit 0
