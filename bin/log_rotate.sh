#!/bin/bash
# Rotate chronicle logs — keep last 500 lines of any log over 5MB.
# Run from cron or manually. Silent when nothing needs rotation.
MAX_SIZE=$((5 * 1024 * 1024))  # 5MB

for logfile in ~/chronicle/*.log ~/chronicle/data/*.log; do
    [ -L "$logfile" ] && continue  # skip symlinks
    [ ! -f "$logfile" ] && continue
    size=$(stat -c%s "$logfile" 2>/dev/null || echo 0)
    if [ "$size" -gt "$MAX_SIZE" ]; then
        tail -500 "$logfile" > "${logfile}.tmp" && mv "${logfile}.tmp" "$logfile"
        echo "rotated: $(basename "$logfile") (was $(( size / 1024 / 1024 ))MB)"
    fi
done
