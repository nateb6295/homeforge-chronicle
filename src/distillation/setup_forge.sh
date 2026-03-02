#!/usr/bin/env bash
# setup_forge.sh — Prepare AGX Orin for Evolutionary Model Forge
#
# Run on AGX: bash /tmp/setup_forge.sh
# Or SCP then run: scp setup_forge.sh nate-agx@192.168.1.70:/tmp/ && \
#                  ssh nate-agx@192.168.1.70 "bash /tmp/setup_forge.sh"

set -euo pipefail

echo "=== Evolutionary Model Forge: AGX Setup ==="
echo "Started: $(date)"
echo ""

# ---------------------------------------------------------------------------
# 1. Fix LD_LIBRARY_PATH for cuSPARSELt (needed by bitsandbytes/torch)
# ---------------------------------------------------------------------------
CUSPARSELT_DIR="$HOME/.local/lib/python3.10/site-packages/nvidia/cusparselt/lib"
if [ -d "$CUSPARSELT_DIR" ]; then
    export LD_LIBRARY_PATH="${CUSPARSELT_DIR}:${LD_LIBRARY_PATH:-}"
    echo "[OK] cuSPARSELt library path set: $CUSPARSELT_DIR"

    # Persist in .bashrc if not already there
    if ! grep -q "cusparselt" "$HOME/.bashrc" 2>/dev/null; then
        echo "export LD_LIBRARY_PATH=\"$CUSPARSELT_DIR:\${LD_LIBRARY_PATH:-}\"" >> "$HOME/.bashrc"
        echo "[OK] Added cuSPARSELt to .bashrc"
    fi
else
    echo "[WARN] cuSPARSELt dir not found at $CUSPARSELT_DIR — may need manual fix"
fi

# ---------------------------------------------------------------------------
# 2. Create forge directory structure on HDD
# ---------------------------------------------------------------------------
FORGE_ROOT="/mnt/hdd/forge"

echo ""
echo "--- Creating forge directories at $FORGE_ROOT ---"

if [ ! -d "/mnt/hdd" ]; then
    echo "[ERROR] /mnt/hdd not mounted. Mount HDD first."
    exit 1
fi

mkdir -p "$FORGE_ROOT"/{loras,merged,evolve,candidate,gguf,winner}
mkdir -p "$FORGE_ROOT"/checkpoints-{action,domain}

echo "[OK] Directory structure:"
find "$FORGE_ROOT" -maxdepth 1 -type d | sort

# ---------------------------------------------------------------------------
# 3. Upgrade pip
# ---------------------------------------------------------------------------
echo ""
echo "--- Upgrading pip ---"
pip3 install --upgrade pip --quiet
echo "[OK] pip $(pip3 --version | awk '{print $2}')"

# ---------------------------------------------------------------------------
# 4. Install mergekit from git (avoids safetensors ~=0.5.2 pin from PyPI)
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing mergekit (from git, --no-deps to avoid safetensors conflict) ---"

# Check if already installed
if python3 -c "import mergekit" 2>/dev/null; then
    echo "[OK] mergekit already installed"
else
    pip3 install --no-deps git+https://github.com/arcee-ai/mergekit.git --quiet
    # Install mergekit's deps that we know are compatible
    pip3 install pyyaml tqdm click typing-extensions --quiet
    echo "[OK] mergekit installed from git"
fi

# ---------------------------------------------------------------------------
# 5. Install CMA-ES
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing cma (CMA-ES optimizer) ---"

if python3 -c "import cma" 2>/dev/null; then
    echo "[OK] cma already installed"
else
    pip3 install cma --quiet
    echo "[OK] cma installed"
fi

# ---------------------------------------------------------------------------
# 6. Install/upgrade pydantic
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing pydantic>=2 ---"

PYDANTIC_VER=$(python3 -c "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "0")
MAJOR=$(echo "$PYDANTIC_VER" | cut -d. -f1)
if [ "$MAJOR" -ge 2 ] 2>/dev/null; then
    echo "[OK] pydantic $PYDANTIC_VER already installed"
else
    pip3 install "pydantic>=2" --quiet
    echo "[OK] pydantic installed"
fi

# ---------------------------------------------------------------------------
# 7. Build llama-quantize if missing
# ---------------------------------------------------------------------------
echo ""
echo "--- Checking llama-quantize ---"

LLAMA_DIR="$HOME/llama.cpp"
QUANTIZE_BIN="$LLAMA_DIR/build/bin/llama-quantize"

if [ -x "$QUANTIZE_BIN" ]; then
    echo "[OK] llama-quantize found at $QUANTIZE_BIN"
else
    echo "Building llama.cpp for quantization..."

    if [ ! -d "$LLAMA_DIR" ]; then
        echo "Cloning llama.cpp (shallow)..."
        git clone --depth 1 https://github.com/ggerganov/llama.cpp.git "$LLAMA_DIR"
    fi

    cd "$LLAMA_DIR"
    git pull --depth 1 || true

    mkdir -p build && cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release -DGGML_CUDA=ON
    cmake --build . --target llama-quantize -j$(nproc)

    if [ -x "$QUANTIZE_BIN" ]; then
        echo "[OK] llama-quantize built successfully"
    else
        # Try alternate path (older llama.cpp versions)
        ALT_BIN="$LLAMA_DIR/build/bin/quantize"
        if [ -x "$ALT_BIN" ]; then
            ln -sf "$ALT_BIN" "$QUANTIZE_BIN"
            echo "[OK] llama-quantize linked from alternate path"
        else
            echo "[ERROR] Failed to build llama-quantize"
            echo "Check build output above for errors"
        fi
    fi

    cd "$HOME"
fi

# ---------------------------------------------------------------------------
# 8. Install llama-cpp-python's convert script deps (for safetensors→GGUF)
# ---------------------------------------------------------------------------
echo ""
echo "--- Checking gguf package ---"
if python3 -c "import gguf" 2>/dev/null; then
    echo "[OK] gguf package installed"
else
    pip3 install gguf --quiet
    echo "[OK] gguf package installed"
fi

# ---------------------------------------------------------------------------
# 9. Verify all dependencies
# ---------------------------------------------------------------------------
echo ""
echo "=== Verification ==="

PASS=0
FAIL=0

check() {
    local name=$1
    local cmd=$2
    if eval "$cmd" 2>/dev/null; then
        echo "[OK] $name"
        PASS=$((PASS + 1))
    else
        echo "[FAIL] $name"
        FAIL=$((FAIL + 1))
    fi
}

check "torch (CUDA)"      "python3 -c \"import torch; assert torch.cuda.is_available(), 'no CUDA'\""
check "transformers"       "python3 -c \"import transformers\""
check "peft"               "python3 -c \"import peft\""
check "trl"                "python3 -c \"import trl\""
check "datasets"           "python3 -c \"import datasets\""
check "mergekit"           "python3 -c \"import mergekit\""
check "cma"                "python3 -c \"import cma\""
check "pydantic>=2"        "python3 -c \"import pydantic; assert int(pydantic.__version__.split('.')[0]) >= 2\""
check "safetensors"        "python3 -c \"import safetensors\""
check "gguf"               "python3 -c \"import gguf\""
check "llama-quantize"     "test -x $QUANTIZE_BIN"
check "forge dirs"         "test -d $FORGE_ROOT/evolve && test -d $FORGE_ROOT/merged"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="

if [ "$FAIL" -gt 0 ]; then
    echo "[WARN] Some checks failed — review output above"
    exit 1
else
    echo "[OK] AGX is ready for the Evolutionary Model Forge"
fi

echo ""
echo "Disk space on /mnt/hdd:"
df -h /mnt/hdd | tail -1

echo ""
echo "Finished: $(date)"
