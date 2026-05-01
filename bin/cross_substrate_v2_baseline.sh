#!/usr/bin/env bash
# v2 baseline — operating-as framing across all 5 substrates at rate=0.50.
# Companion to existing v1 (knowing-about) baselines for direct comparison.
# Vasilenko 2026-04 Section 3.8 alignment: knowing-about register loses
# 25-35% attractor coverage at activation level; this measures it at output level.

set -u
cd "$(dirname "$0")/.."
LOG=~/chronicle/logs/cross_substrate_v2.log
mkdir -p ~/chronicle/logs
echo "=== v2 baseline start $(date -Is) ===" >>"$LOG"

for PROV in claude-opus nous-hermes-4-70b deepinfra-deepseek-v3 deepinfra-qwen-235b groq-qwen-32b; do
  echo "--- run $(date -Is) provider=$PROV framing=oa ---" >>"$LOG"
  python3 bin/cross_substrate_probe.py --provider "$PROV" --rate 0.50 --seeds 5 --iters 3 --framing oa >>"$LOG" 2>&1
  echo "--- end $(date -Is) provider=$PROV ---" >>"$LOG"
done
echo "=== v2 baseline complete $(date -Is) ===" >>"$LOG"
