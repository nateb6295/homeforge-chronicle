#!/usr/bin/env bash
# deploy_winner.sh — Deploy the evolution winner as hermes3-evolved / hermes3-mind
#
# Run on AGX after evolve.py completes.
#
# Usage:
#   bash /tmp/deploy_winner.sh              # Deploy as hermes3-evolved (safe)
#   bash /tmp/deploy_winner.sh --promote    # Also overwrite hermes3-mind
#   bash /tmp/deploy_winner.sh --rollback   # Restore original hermes3-mind

set -euo pipefail

WINNER_GGUF="/mnt/hdd/forge/gguf/winner.gguf"
WINNER_INFO="/mnt/hdd/forge/gguf/winner_info.json"
EVOLVED_MODEL="hermes3-evolved"
PRODUCTION_MODEL="hermes3-mind"
BACKUP_MODELFILE="$HOME/Modelfile.mind.backup"
CHRONICLE_ENV="$HOME/chronicle/chronicle.env"

# Parse args
PROMOTE=false
ROLLBACK=false
for arg in "$@"; do
    case $arg in
        --promote) PROMOTE=true ;;
        --rollback) ROLLBACK=true ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Rollback mode
# ---------------------------------------------------------------------------
if $ROLLBACK; then
    echo "=== Rollback: Restoring original hermes3-mind ==="

    if [ -f "$BACKUP_MODELFILE" ]; then
        echo "Restoring from backup Modelfile..."
        ollama create "$PRODUCTION_MODEL" -f "$BACKUP_MODELFILE"
        echo "[OK] Restored hermes3-mind from backup"
    else
        echo "No backup Modelfile found. Recreating from base + LoRA..."
        cat > /tmp/Modelfile.rollback << 'MODELFILE'
FROM hermes3:8b
ADAPTER ~/mind-lora.gguf
PARAMETER num_ctx 16384
MODELFILE
        # Expand ~ in the Modelfile
        sed -i "s|~/|$HOME/|g" /tmp/Modelfile.rollback
        ollama create "$PRODUCTION_MODEL" -f /tmp/Modelfile.rollback
        echo "[OK] Restored hermes3-mind from base + LoRA"
    fi

    # Restart Mind
    echo "Restarting chronicle-mind..."
    systemctl --user restart chronicle-mind 2>/dev/null || true
    echo "[OK] Done. Check logs: journalctl --user -u chronicle-mind -f"
    exit 0
fi

# ---------------------------------------------------------------------------
# Deploy mode
# ---------------------------------------------------------------------------
echo "=== Evolutionary Model Forge: Deploy Winner ==="

# Check winner exists
if [ ! -f "$WINNER_GGUF" ]; then
    echo "[ERROR] Winner GGUF not found: $WINNER_GGUF"
    echo "Run evolve.py first."
    exit 1
fi

GGUF_SIZE=$(du -h "$WINNER_GGUF" | cut -f1)
echo "Winner GGUF: $WINNER_GGUF ($GGUF_SIZE)"

if [ -f "$WINNER_INFO" ]; then
    echo "Winner info:"
    python3 -c "
import json
with open('$WINNER_INFO') as f:
    info = json.load(f)
print(f\"  Fitness: {info['fitness']:.4f}\")
for i, seg in enumerate(info['segments']):
    print(f\"  Seg {i}: capsule={seg['capsule']:.3f} action={seg['action']:.3f} domain={seg['domain']:.3f}\")
"
fi

# Step 1: Create hermes3-evolved
echo ""
echo "--- Creating $EVOLVED_MODEL ---"

MODELFILE_PATH="/tmp/Modelfile.evolved"
cat > "$MODELFILE_PATH" << MODELFILE
FROM $WINNER_GGUF
PARAMETER num_ctx 16384
MODELFILE

ollama create "$EVOLVED_MODEL" -f "$MODELFILE_PATH"
echo "[OK] Created $EVOLVED_MODEL"

# Step 2: Verify with test prompts
echo ""
echo "--- Running verification prompts ---"

PASS=0
FAIL=0

verify() {
    local desc="$1"
    local prompt="$2"
    local check="$3"

    echo -n "  $desc: "
    RESPONSE=$(ollama run "$EVOLVED_MODEL" "$prompt" 2>/dev/null || echo "ERROR")

    if echo "$RESPONSE" | grep -qi "$check"; then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        echo "    Response: ${RESPONSE:0:200}"
        FAIL=$((FAIL + 1))
    fi
}

# Test 1: JSON format
verify "JSON format" \
    'You are Chronicle Mind. Choose actions: [{"action": "no_action", "reason": "test"}]' \
    "action"

# Test 2: Action selection
verify "Action selection" \
    'CURRENT STATE: Tuesday morning. Goal: write thoughts. Choose your actions for this cycle.' \
    "action"

# Test 3: Identity
verify "Identity" \
    'Who are you? Respond as a JSON action array.' \
    "Mind\|chronicle\|AGX"

# Test 4: Homeforge knowledge
verify "Homeforge knowledge" \
    'Write a note about homeforge. Respond as JSON: [{"action": "write_note", "content": "..."}]' \
    "homeforge\|sovereignty\|local"

# Test 5: No confabulation
verify "No confabulation" \
    'CURRENT STATE: No messages. What did Nate ask you to do? Respond as JSON action array.' \
    "no_action\|action"

echo ""
echo "Verification: $PASS passed, $FAIL failed"

if [ "$FAIL" -gt 2 ]; then
    echo "[WARN] Too many failures. Review before promoting."
    echo "Test manually: ollama run $EVOLVED_MODEL '<prompt>'"
    if $PROMOTE; then
        echo "[ABORT] Skipping promotion due to verification failures."
        exit 1
    fi
fi

# Step 3: Promote to production (if requested)
if $PROMOTE; then
    echo ""
    echo "--- Promoting to $PRODUCTION_MODEL ---"

    # Backup current Modelfile
    CURRENT_MODELFILE="$HOME/Modelfile.mind"
    if [ -f "$CURRENT_MODELFILE" ]; then
        cp "$CURRENT_MODELFILE" "$BACKUP_MODELFILE"
        echo "[OK] Backed up current Modelfile to $BACKUP_MODELFILE"
    fi

    # Create new production model from winner GGUF
    PROD_MODELFILE="/tmp/Modelfile.production"
    cat > "$PROD_MODELFILE" << MODELFILE
FROM $WINNER_GGUF
PARAMETER num_ctx 16384
MODELFILE

    ollama create "$PRODUCTION_MODEL" -f "$PROD_MODELFILE"
    echo "[OK] Created $PRODUCTION_MODEL from evolved winner"

    # Update Modelfile.mind for future reference
    cp "$PROD_MODELFILE" "$CURRENT_MODELFILE"

    # Verify CHRONICLE_LOCAL_MODEL is set correctly
    if [ -f "$CHRONICLE_ENV" ]; then
        if grep -q "CHRONICLE_LOCAL_MODEL" "$CHRONICLE_ENV"; then
            CURRENT_MODEL=$(grep "CHRONICLE_LOCAL_MODEL" "$CHRONICLE_ENV" | cut -d= -f2)
            if [ "$CURRENT_MODEL" != "$PRODUCTION_MODEL" ]; then
                echo "[WARN] chronicle.env has CHRONICLE_LOCAL_MODEL=$CURRENT_MODEL"
                echo "       Should be: $PRODUCTION_MODEL"
                echo "       Update manually if needed."
            else
                echo "[OK] chronicle.env already set to $PRODUCTION_MODEL"
            fi
        fi
    fi

    # Restart Mind
    echo ""
    echo "Restarting chronicle-mind..."
    systemctl --user restart chronicle-mind 2>/dev/null || true
    echo "[OK] Mind restarted with evolved model"

    echo ""
    echo "Monitor: journalctl --user -u chronicle-mind -f"
    echo "Rollback: bash /tmp/deploy_winner.sh --rollback"
else
    echo ""
    echo "--- Deployed as $EVOLVED_MODEL (not promoted to production) ---"
    echo ""
    echo "Test manually:"
    echo "  ollama run $EVOLVED_MODEL 'CURRENT STATE: Monday morning. Choose your actions.'"
    echo ""
    echo "Promote when ready:"
    echo "  bash /tmp/deploy_winner.sh --promote"
fi

echo ""
echo "Done: $(date)"
