#!/bin/bash
# Capsule Distillation — Phase 4: Convert LoRA adapter to GGUF and deploy to Ollama
#
# Run on AGX after training completes:
#   bash /tmp/convert_and_deploy.sh

set -e

ADAPTER_DIR="$HOME/mind-lora-adapter"
BASE_MODEL="NousResearch/Hermes-3-Llama-3.1-8B"
GGUF_OUTPUT="$HOME/mind-lora.gguf"
MODELFILE="$HOME/Modelfile.mind"

echo "=== Phase 4: Convert & Deploy ==="
echo ""

# Check prerequisites
if [ ! -d "$ADAPTER_DIR" ]; then
    echo "ERROR: Adapter not found at $ADAPTER_DIR"
    echo "Run train_mind_lora.py first."
    exit 1
fi

if [ ! -f "$HOME/llama.cpp/convert_lora_to_gguf.py" ]; then
    echo "ERROR: llama.cpp not found at ~/llama.cpp"
    echo "Run: git clone --depth 1 https://github.com/ggerganov/llama.cpp ~/llama.cpp"
    exit 1
fi

# Step 1: Install gguf Python package (needed by converter)
echo "Ensuring gguf package is installed..."
pip3 install gguf 2>&1 | tail -2
echo ""

# Step 2: Convert LoRA adapter to GGUF
echo "Converting LoRA adapter to GGUF..."
echo "  Base model: $BASE_MODEL"
echo "  Adapter:    $ADAPTER_DIR"
echo "  Output:     $GGUF_OUTPUT"
echo ""

python3 ~/llama.cpp/convert_lora_to_gguf.py \
    --base "$BASE_MODEL" \
    "$ADAPTER_DIR" \
    --outfile "$GGUF_OUTPUT" \
    --outtype f16

echo ""
ls -lh "$GGUF_OUTPUT"
echo ""

# Step 3: Create Modelfile
echo "Creating Modelfile..."
cat > "$MODELFILE" << 'EOF'
FROM hermes3:8b
ADAPTER ~/mind-lora.gguf
EOF

# Expand ~ in the ADAPTER path for Ollama
sed -i "s|~/|$HOME/|g" "$MODELFILE"

echo "Modelfile at $MODELFILE:"
cat "$MODELFILE"
echo ""

# Step 4: Create Ollama model
echo "Creating Ollama model hermes3-mind..."
ollama create hermes3-mind -f "$MODELFILE"
echo ""

# Step 5: Verify
echo "=== Verification ==="
echo ""
echo "Available models:"
ollama list
echo ""

# Quick test
echo "Quick sanity test..."
echo "Q: What is Homeforge?"
RESPONSE=$(ollama run hermes3-mind "What is Homeforge?" 2>&1 | head -10)
echo "A: $RESPONSE"
echo ""

echo "=== Deploy complete! ==="
echo ""
echo "To compare base vs adapted:"
echo "  ollama run hermes3:8b 'What do you know about Homeforge?'"
echo "  ollama run hermes3-mind 'What do you know about Homeforge?'"
echo ""
echo "To switch Mind to the adapted model:"
echo "  Edit /tmp/chronicle_mind.py: LOCAL_MODEL = 'hermes3-mind'"
echo "  deploy-mind --watch"
echo ""
echo "To rollback:"
echo "  Edit /tmp/chronicle_mind.py: LOCAL_MODEL = 'hermes3:8b'"
echo "  deploy-mind --watch"
