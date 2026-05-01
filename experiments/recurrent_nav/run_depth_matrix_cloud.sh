#!/bin/bash
# Depth × order matrix on DeepInfra Gemma-3-27B (cloud).
# d=3,5 × order=structural,content × 3 repeats = 12 trials
set -u
cd /home/nate-agx/chronicle

# Source API keys from chronicle.env (DEEPINFRA_API_KEY, GROQ_API_KEY, ...)
set -a
source /home/nate-agx/chronicle/chronicle.env 2>/dev/null
set +a

LOG=/tmp/depth_matrix_cloud_$(date +%Y%m%d_%H%M).log
echo "[$(date)] starting cloud matrix on deepinfra_gemma" > "$LOG"
for rep in 1 2 3; do
  for depth in 3 5; do
    for order in structural content; do
      echo "[$(date)] rep=$rep depth=$depth order=$order" >> "$LOG"
      timeout 300 python3 bin/recurrent_nav_test.py \
        --backend=deepinfra_gemma --depth=$depth \
        --ccs_format=prose --ccs_order=$order --ccs_chars=1500 \
        >> "$LOG" 2>&1 || echo "[$(date)] trial FAILED rep=$rep d=$depth o=$order" >> "$LOG"
    done
  done
done
echo "[$(date)] cloud matrix complete" >> "$LOG"
