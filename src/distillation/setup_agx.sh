#!/bin/bash
# Capsule Distillation — Phase 0: Prepare AGX Orin for training
#
# Run on AGX: ssh nate-agx@192.168.1.70 "bash /tmp/setup_agx.sh"
# Or SCP first: scp /tmp/setup_agx.sh nate-agx@192.168.1.70:/tmp/ && ssh nate-agx@192.168.1.70 "bash /tmp/setup_agx.sh"

set -e

echo "=== Phase 0: AGX Training Stack Setup ==="
echo ""

# Step 1: Check storage
echo "--- Storage before ---"
df -h / | tail -1
echo ""

# Step 2: Delete nemotron-3-nano (unused rollback, ~24GB)
echo "Removing nemotron-3-nano..."
if ollama list 2>/dev/null | grep -q nemotron; then
    ollama rm nemotron-3-nano
    echo "  nemotron-3-nano removed"
else
    echo "  nemotron-3-nano not found (already removed)"
fi
echo ""

echo "--- Storage after cleanup ---"
df -h / | tail -1
echo ""

# Step 3: Install numpy first (avoid conflicts)
echo "Installing numpy 1.26.1 (pin to avoid conflicts)..."
pip3 install numpy==1.26.1 2>&1 | tail -3
echo ""

# Step 4: Install PyTorch from Jetson AI Lab
echo "Installing PyTorch for JetPack 6.2 / CUDA 12.6..."
pip3 install torch --index-url https://pypi.jetson-ai-lab.dev/jp6/cu126 2>&1 | tail -5
echo ""

# Step 5: Install training stack
echo "Installing transformers, peft, bitsandbytes, accelerate, datasets, trl..."
pip3 install transformers peft bitsandbytes accelerate datasets trl 2>&1 | tail -5
echo ""

# Step 6: Clone llama.cpp for LoRA→GGUF conversion
if [ -d "$HOME/llama.cpp" ]; then
    echo "llama.cpp already cloned at ~/llama.cpp"
else
    echo "Cloning llama.cpp (shallow)..."
    git clone --depth 1 https://github.com/ggerganov/llama.cpp ~/llama.cpp
fi
echo ""

# Step 7: Verify
echo "=== Verification ==="
echo ""

echo -n "PyTorch: "
python3 -c "import torch; print(f'{torch.__version__} (CUDA: {torch.cuda.is_available()})')" 2>&1

echo -n "CUDA device: "
python3 -c "import torch; print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'N/A')" 2>&1

echo -n "transformers: "
python3 -c "import transformers; print(transformers.__version__)" 2>&1

echo -n "peft: "
python3 -c "import peft; print(peft.__version__)" 2>&1

echo -n "bitsandbytes: "
python3 -c "import bitsandbytes; print(bitsandbytes.__version__)" 2>&1

echo -n "trl: "
python3 -c "import trl; print(trl.__version__)" 2>&1

echo -n "datasets: "
python3 -c "import datasets; print(datasets.__version__)" 2>&1

echo ""
echo "--- Final storage ---"
df -h / | tail -1
echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next: python3 /tmp/export_capsules.py"
