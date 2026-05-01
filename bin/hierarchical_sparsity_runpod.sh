#!/usr/bin/env bash
# RunPod wrapper for hierarchical_sparsity_v0.py — §3.6 v0.5 cross-size test.
#
# Runs the scaffold across multiple model sizes to test the prediction:
# capable models show MORE complex cross-layer attention flow + SIMPLER
# within-layer specialization.
#
# v0 baseline (local Qwen 1.5B): discriminative-gate moves predicted
# direction (HARD 2.084 > EASY 1.538). This RunPod run validates scaling.
#
# Persists results to /workspace/results/hsp_v0_runpod.jsonl
# (use /workspace not /tmp — survives container restart).

set -e

# Persistent output dir (RunPod /workspace is the persistent volume)
OUT_DIR="${OUT_DIR:-/workspace/results}"
mkdir -p "$OUT_DIR"
OUT_FILE="$OUT_DIR/hsp_v0_runpod.jsonl"
LOG_FILE="$OUT_DIR/hsp_v0_runpod.log"

# HF cache to /workspace so models don't fill / on the pod
export HF_HOME="${HF_HOME:-/workspace/hf-cache}"
export TRANSFORMERS_CACHE="${TRANSFORMERS_CACHE:-/workspace/hf-cache/hub}"
mkdir -p "$HF_HOME"

# Path to scaffold — adjust for RunPod cwd if needed
SCRIPT="${HIERSPARSITY_SCRIPT:-./hierarchical_sparsity_v0.py}"
if [ ! -f "$SCRIPT" ]; then
    echo "ERROR: scaffold not found at $SCRIPT" >&2
    echo "Set HIERSPARSITY_SCRIPT or run from chronicle/bin/" >&2
    exit 1
fi

echo "=== Hierarchical Sparsity v0.5 Cross-Size Run ===" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "Output: $OUT_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Models to test — small to large
MODELS=(
    "Qwen/Qwen2.5-1.5B-Instruct"
    "mistralai/Mistral-7B-Instruct-v0.3"
)

# Allow override via env
if [ -n "${MODELS_OVERRIDE:-}" ]; then
    IFS=',' read -ra MODELS <<< "$MODELS_OVERRIDE"
fi

for model in "${MODELS[@]}"; do
    echo "--- Running $model ---" | tee -a "$LOG_FILE"
    if python3 "$SCRIPT" --model "$model" --out "$OUT_FILE" 2>&1 | tee -a "$LOG_FILE"; then
        echo "OK: $model" | tee -a "$LOG_FILE"
    else
        echo "FAILED: $model (continuing to next)" | tee -a "$LOG_FILE"
    fi
    # Free GPU memory between models
    python3 -c "import torch; torch.cuda.empty_cache()" || true
    echo "" | tee -a "$LOG_FILE"
done

echo "Finished: $(date)" | tee -a "$LOG_FILE"
echo ""
echo "Results: $OUT_FILE"
echo "Log: $LOG_FILE"
echo ""
echo "Run hierarchical_sparsity_compare.py on results to see scaling pattern."
