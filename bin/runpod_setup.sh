#!/bin/bash
# RunPod setup script for SAE steering pipeline
# Run this FIRST after SSH into the pod

set -e

echo "=== RunPod SAE Setup ==="
echo "Installing dependencies..."

# Core ML stack
pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu126
pip install --no-cache-dir transformers accelerate bitsandbytes safetensors einops tqdm

# Verify GPU
python3 -c "import torch; print(f'GPU: {torch.cuda.get_device_name(0)} ({torch.cuda.get_device_properties(0).total_mem/1e9:.0f}GB)')"

# Login to HF (uses HUGGING_FACE_HUB_TOKEN env var)
pip install --no-cache-dir huggingface_hub
python3 -c "from huggingface_hub import login; import os; login(token=os.environ.get('HUGGING_FACE_HUB_TOKEN',''))"

echo ""
echo "=== Setup Complete ==="
echo "Now run: python3 sae_steering_runpod.py --layer 30"
