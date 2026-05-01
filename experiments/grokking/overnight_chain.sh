#!/bin/bash
# Overnight experiment chain for grokking paper.
#
# 1. Wait for mul seed 2 to finish
# 2. Run cross-seed mul anatomy
# 3. Launch p=113 addition training
# 4. While p=113 trains: run any remaining analyses (Fourier, progressive ablation)
# 5. After p=113 finishes: run p=113 ablation
# 6. Aggregate everything into morning_digest.md
# 7. Leave a marker file for morning

set -u
GROK="/home/nate-agx/chronicle/experiments/grokking"
OUT="$GROK/overnight"
mkdir -p "$OUT"
LOG="$OUT/chain.log"
DIGEST="$OUT/morning_digest.md"

echo "=== overnight chain started $(date -Iseconds) ===" > "$LOG"

# Step 1: wait for mul seed 2 if not done
echo "[$(date +%H:%M:%S)] waiting for mul seed 2..." >> "$LOG"
while [ ! -f "$GROK/runs/v2_mul_seed2/snapshots/step_050000.pt" ]; do
  sleep 10
done
echo "[$(date +%H:%M:%S)] mul seed 2 done" >> "$LOG"

# Step 2: cross-seed mul anatomy
echo "[$(date +%H:%M:%S)] cross-seed mul anatomy..." >> "$LOG"
python3 "$GROK/cross_seed_mul_anatomy.py" > "$OUT/cross_seed_mul.txt" 2>&1
echo "[$(date +%H:%M:%S)] cross-seed mul done" >> "$LOG"

# Step 3: launch p=113 training in background
echo "[$(date +%H:%M:%S)] launching p=113 training..." >> "$LOG"
cd "$GROK"
nohup python3 grok_p113.py > "$OUT/p113_training.log" 2>&1 &
P113_PID=$!
echo "[$(date +%H:%M:%S)] p=113 PID: $P113_PID" >> "$LOG"

# Step 4: wait for p=113 to finish
wait $P113_PID
echo "[$(date +%H:%M:%S)] p=113 training finished" >> "$LOG"

# Step 5: run p=113 ablation
echo "[$(date +%H:%M:%S)] running p=113 ablation..." >> "$LOG"
python3 "$GROK/p113_ablation.py" > "$OUT/p113_ablation.txt" 2>&1
echo "[$(date +%H:%M:%S)] p=113 ablation done" >> "$LOG"

# Step 6: aggregate digest
echo "[$(date +%H:%M:%S)] aggregating morning digest..." >> "$LOG"
python3 "$GROK/morning_digest.py" > "$DIGEST" 2>&1
echo "[$(date +%H:%M:%S)] digest done at $DIGEST" >> "$LOG"

# Step 7: marker file for morning announcement
touch "$OUT/OVERNIGHT_COMPLETE"
echo "[$(date +%H:%M:%S)] chain complete" >> "$LOG"
