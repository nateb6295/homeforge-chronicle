#!/usr/bin/env bash
# cross_corruption_sweep — sweep rates on extreme substrates.
# Hermes-4-70B (strongest receiver, +0.213 fidelity lift at rate=0.50)
# Qwen-235B (weakest receiver, +0.051 lift at rate=0.50)
# Question: does substrate-amplification curve vary with corruption stress?

set -u
cd "$(dirname "$0")/.."
LOG=~/chronicle/logs/cross_corruption_sweep.log
mkdir -p ~/chronicle/logs
echo "=== sweep start $(date -Is) ===" >>"$LOG"

for RATE in 0.30 0.70 0.90; do
  for PROV in nous-hermes-4-70b deepinfra-qwen-235b; do
    echo "--- run $(date -Is) provider=$PROV rate=$RATE ---" >>"$LOG"
    python3 bin/cross_substrate_probe.py --provider "$PROV" --rate "$RATE" --seeds 5 --iters 3 >>"$LOG" 2>&1
    echo "--- end $(date -Is) provider=$PROV rate=$RATE ---" >>"$LOG"
  done
done
echo "=== sweep complete $(date -Is) ===" >>"$LOG"
