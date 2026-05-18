#!/bin/bash
# Phase 4 RunPod Setup — run this on the pod after SSH'ing in
# Prerequisites: H100 or A100 80GB pod with PyTorch template

set -e

echo "=== Phase 4 SFT Training Setup ==="

# Install dependencies
pip install -q transformers peft trl accelerate datasets bitsandbytes

# Create workspace
mkdir -p /workspace/care_template_dpo_run/adapters

echo "=== Upload your data files ==="
echo "From your local machine, run:"
echo "  scp chronicle/data/care_template_dpo_run/cot_care_traces.jsonl root@<POD_IP>:/workspace/care_template_dpo_run/"
echo "  scp chronicle/data/care_template_dpo_run/cot_care_traces_phase4.jsonl root@<POD_IP>:/workspace/care_template_dpo_run/"
echo "  scp chronicle/bin/phase4_sft_train.py root@<POD_IP>:/workspace/"
echo "  scp chronicle/bin/phase4_post_train_eval.py root@<POD_IP>:/workspace/"
echo ""
echo "Then run training:"
echo "  cd /workspace"
echo "  python3 phase4_sft_train.py --arm A   # think+answer, all 5 domains"
echo "  python3 phase4_sft_train.py --arm B   # answer-only, all 5 domains"
echo "  python3 phase4_sft_train.py --arm C   # think+answer, Phase 3 only (control)"
echo ""
echo "After training, run eval:"
echo "  python3 phase4_post_train_eval.py --arm A"
echo "  python3 phase4_post_train_eval.py --arm B"
echo "  python3 phase4_post_train_eval.py --arm C"
echo ""
echo "Download results:"
echo "  scp root@<POD_IP>:/workspace/care_template_dpo_run/phase4_eval_*.jsonl ."
