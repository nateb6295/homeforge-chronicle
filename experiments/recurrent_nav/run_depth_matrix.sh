#!/bin/bash
# Depth × order matrix: d=3,5 × order=structural,content × 3 repeats = 12 trials
set -eu
cd /home/nate-agx/chronicle
LOG=/tmp/depth_matrix_$(date +%Y%m%d_%H%M).log
echo "[$(date)] starting matrix" > "$LOG"
for rep in 1 2 3; do
  for depth in 3 5; do
    for order in structural content; do
      echo "[$(date)] rep=$rep depth=$depth order=$order" >> "$LOG"
      timeout 300 python3 bin/recurrent_nav_test.py \
        --depth=$depth --ccs_format=prose --ccs_order=$order --ccs_chars=1500 \
        >> "$LOG" 2>&1 || echo "[$(date)] trial FAILED rep=$rep d=$depth o=$order" >> "$LOG"
    done
  done
done
echo "[$(date)] matrix complete" >> "$LOG"
echo "LOG: $LOG"
