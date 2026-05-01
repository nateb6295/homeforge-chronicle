#!/bin/bash
# finetune_remote.sh — Run Chronicle fine-tuning on RunPod instead of local AGX
#
# The AGX OOM'd twice trying to train locally. RunPod has dedicated VRAM.
#
# Supports two model profiles:
#   specialist  — Qwen2.5-3B (default). A40 48GB. Merge-and-deploy.
#   darby       — Gemma4-26B. A100 80GB. ADAPTER deploy (no merge on AGX).
#
# Commands:
#   finetune_remote.sh estimate [--darby] [--resume]
#   finetune_remote.sh launch [--darby] [--resume]
#   finetune_remote.sh status
#   finetune_remote.sh download
#   finetune_remote.sh deploy              — deploy downloaded adapter to Ollama
#
# For 27B models, adapter is converted to GGUF on RunPod and deployed via
# Ollama ADAPTER directive — no merge needed on AGX (avoids OOM).
#
# GPU: A40 48GB for 3B, A100 80GB for 27B

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHRONICLE_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${CHRONICLE_DIR}/chronicle.env"
DATA_DIR="/mnt/hdd/chronicle-finetune"
ADAPTER_DIR="${DATA_DIR}/adapters"
CHECKPOINT_DIR="${ADAPTER_DIR}/checkpoint-200"
STATE_FILE="${CHRONICLE_DIR}/.finetune-pod-state"
LOG_FILE="${CHRONICLE_DIR}/finetune-remote.log"

MAX_COST="${RUNPOD_MAX_COST:-100}"
TEMPLATE_ID="runpod-torch-v240"  # PyTorch 2.4 + CUDA 12.4
REMOTE_WORK="/workspace/chronicle-finetune"

# ── Model Profiles ───────────────────────────────────────────────
# Selected via --darby flag (default: specialist)
MODEL_PROFILE="${FINETUNE_PROFILE:-specialist}"

setup_profile() {
    if [ "$MODEL_PROFILE" = "darby" ]; then
        GPU_ID="${RUNPOD_GPU:-NVIDIA A100-SXM4-80GB}"
        HOURLY_RATE=1.64  # A100 80GB secure cloud rate
        BASE_MODEL="google/gemma-4-26b-it"
        TRAINING_DATA="darby_training.jsonl"
        ADAPTER_NAME="darby-adapter"
        TOTAL_STEPS=476  # 7620 examples / batch=16 = 476 steps per epoch
        CHECKPOINT_STEP=0
        # 27B fp16 on A100: ~15s/step + evals. 476 steps ≈ 2h + 30min setup/download
        FULL_ESTIMATE_HOURS=2.0
        RESUME_ESTIMATE_HOURS=1.0
        SETUP_OVERHEAD_HOURS=0.5  # 27B model download is large
        DEPLOY_MODE="adapter"  # GGUF adapter, no merge on AGX
        OLLAMA_HOST_DEPLOY="http://localhost:11435"
        OLLAMA_BASE_MODEL="gemma4:26b"
    else
        GPU_ID="${RUNPOD_GPU:-NVIDIA A40}"
        HOURLY_RATE=0.39  # A40 secure cloud rate
        BASE_MODEL="Qwen/Qwen2.5-3B-Instruct"
        TRAINING_DATA="combined.jsonl"
        ADAPTER_NAME="chronicle-specialist"
        TOTAL_STEPS=398
        CHECKPOINT_STEP=200
        # On A40, ~2.5s/step + evals. Full: ~25 min + 10 min setup
        FULL_ESTIMATE_HOURS=0.6
        RESUME_ESTIMATE_HOURS=0.4
        SETUP_OVERHEAD_HOURS=0.2
        DEPLOY_MODE="merge"  # Full merge on AGX (3B fits in memory)
        OLLAMA_HOST_DEPLOY="http://localhost:11435"
        OLLAMA_BASE_MODEL=""
    fi
}

# Parse --darby from any position in argv
for arg in "$@"; do
    if [ "$arg" = "--darby" ]; then
        MODEL_PROFILE="darby"
    fi
done
setup_profile

# ── Load env ──────────────────────────────────────────────────────
if [ -f "$ENV_FILE" ]; then
    # Filter out dotted keys (e.g. AGENT_MAP.x=y) that bash can't handle
    eval "$(grep -v '^#' "$ENV_FILE" | grep -v '\.' | grep '=' | sed 's/^/export /')" 2>/dev/null || true
fi

if [ -z "${RUNPOD_API_KEY:-}" ]; then
    echo "ERROR: RUNPOD_API_KEY not set in ${ENV_FILE}"
    exit 1
fi
export RUNPOD_API_KEY

RUNPODCTL="/usr/local/bin/runpodctl"

# ── Helpers ───────────────────────────────────────────────────────
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

die() {
    log "ERROR: $1"
    exit 1
}

save_state() {
    # Save pod state for cross-command continuity
    cat > "$STATE_FILE" << EOF
POD_ID=$1
CREATED_AT=$(date -Iseconds)
RESUME=${2:-false}
MODEL_PROFILE=${MODEL_PROFILE}
EOF
}

load_profile_from_state() {
    # Restore model profile from state file
    if [ -f "$STATE_FILE" ]; then
        local saved_profile
        saved_profile=$(grep '^MODEL_PROFILE=' "$STATE_FILE" | cut -d= -f2)
        if [ -n "$saved_profile" ]; then
            MODEL_PROFILE="$saved_profile"
            setup_profile
        fi
    fi
}

load_state() {
    if [ ! -f "$STATE_FILE" ]; then
        die "No active pod. Run 'finetune_remote.sh launch' first."
    fi
    source "$STATE_FILE"
    if [ -z "${POD_ID:-}" ]; then
        die "State file corrupt. Remove ${STATE_FILE} and re-launch."
    fi
    load_profile_from_state
}

clear_state() {
    rm -f "$STATE_FILE"
}

get_pod_json() {
    $RUNPODCTL pod get "$1" 2>/dev/null
}

get_pod_status() {
    local json
    json=$(get_pod_json "$1")
    echo "$json" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('desiredStatus','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN"
}

get_pod_ssh() {
    # Returns "host port" from runpodctl ssh info (preferred method)
    local info
    info=$($RUNPODCTL ssh info "$1" 2>/dev/null || true)
    if [ -n "$info" ]; then
        local host port
        host=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ip',''))" 2>/dev/null || true)
        port=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('port',''))" 2>/dev/null || true)
        if [ -n "$host" ] && [ -n "$port" ]; then
            echo "$host $port"
            return 0
        fi
    fi

    # Fallback: parse pod JSON for runtime ports
    local pod_json
    pod_json=$(get_pod_json "$1")
    local host port
    host=$(echo "$pod_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d.get('runtime', {})
ports = r.get('ports', [])
for p in ports:
    if p.get('privatePort') == 22:
        print(p.get('ip', ''))
        break
" 2>/dev/null || true)
    port=$(echo "$pod_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d.get('runtime', {})
ports = r.get('ports', [])
for p in ports:
    if p.get('privatePort') == 22:
        print(p.get('publicPort', '22'))
        break
" 2>/dev/null || true)

    if [ -n "$host" ] && [ -n "$port" ]; then
        echo "$host $port"
        return 0
    fi

    return 1
}

RUNPOD_SSH_KEY="${HOME}/.runpod/ssh/RunPod-Key-Go"

ssh_cmd() {
    # Run a command on the pod via SSH
    local host="$1" port="$2"
    shift 2
    local key_flag=""
    [ -f "$RUNPOD_SSH_KEY" ] && key_flag="-i $RUNPOD_SSH_KEY"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 -o LogLevel=ERROR \
        $key_flag -p "$port" "root@${host}" "$@"
}

scp_to() {
    # Upload file/dir to pod
    local host="$1" port="$2" src="$3" dst="$4"
    local key_flag=""
    [ -f "$RUNPOD_SSH_KEY" ] && key_flag="-i $RUNPOD_SSH_KEY"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 -o LogLevel=ERROR \
        $key_flag -P "$port" -r "$src" "root@${host}:${dst}"
}

scp_from() {
    # Download file/dir from pod
    local host="$1" port="$2" src="$3" dst="$4"
    local key_flag=""
    [ -f "$RUNPOD_SSH_KEY" ] && key_flag="-i $RUNPOD_SSH_KEY"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 -o LogLevel=ERROR \
        $key_flag -P "$port" -r "root@${host}:${src}" "$dst"
}

# ── estimate ──────────────────────────────────────────────────────
cmd_estimate() {
    local resume=false
    if [[ "${1:-}" == "--resume" ]]; then
        resume=true
    fi

    echo "=== Chronicle Fine-Tune Cost Estimate ==="
    echo ""
    echo "Profile:        ${MODEL_PROFILE}"
    echo "GPU:            ${GPU_ID}"
    echo "Rate:           \$${HOURLY_RATE}/hr (secure cloud)"
    echo "Budget cap:     \$${MAX_COST}"
    echo ""
    echo "Model:          ${BASE_MODEL}"
    echo "Method:         LoRA (r=16, alpha=32) fp16"
    echo "Dataset:        $(wc -l < "${DATA_DIR}/${TRAINING_DATA}") examples ($(du -sh "${DATA_DIR}/${TRAINING_DATA}" | cut -f1))"
    echo "Total steps:    ${TOTAL_STEPS} (batch=1, grad_accum=16, 1 epoch)"
    echo "Deploy mode:    ${DEPLOY_MODE}"
    echo ""

    local train_hours cost
    if [ "$resume" = true ]; then
        local remaining=$((TOTAL_STEPS - CHECKPOINT_STEP))
        echo "Mode:           RESUME from checkpoint-200"
        echo "Remaining:      ${remaining} steps"
        echo "Checkpoint:     $(du -sh "${CHECKPOINT_DIR}" 2>/dev/null | cut -f1 || echo 'N/A')"
        train_hours=$(echo "$RESUME_ESTIMATE_HOURS + $SETUP_OVERHEAD_HOURS" | bc)
    else
        echo "Mode:           FULL training (${TOTAL_STEPS} steps)"
        train_hours=$(echo "$FULL_ESTIMATE_HOURS + $SETUP_OVERHEAD_HOURS" | bc)
    fi

    cost=$(echo "$train_hours * $HOURLY_RATE" | bc)

    echo ""
    echo "Estimated time: ~$(echo "$train_hours * 60" | bc | cut -d. -f1) minutes"
    echo "Estimated cost: \$${cost}"
    echo ""

    # Check if under budget
    local over
    over=$(echo "$cost > $MAX_COST" | bc)
    if [ "$over" -eq 1 ]; then
        echo "WARNING: Estimated cost exceeds budget cap of \$${MAX_COST}!"
        return 1
    fi

    echo "Within budget cap. Safe to launch."

    # Show account balance
    local balance
    balance=$($RUNPODCTL user 2>/dev/null | python3 -c "import sys,json; print(f\"{json.load(sys.stdin).get('clientBalance', 0):.2f}\")" 2>/dev/null || echo "unknown")
    echo "RunPod balance: \$${balance}"
}

# ── launch ────────────────────────────────────────────────────────
cmd_launch() {
    local resume=false
    if [[ "${1:-}" == "--resume" ]]; then
        resume=true
    fi

    # Preflight: check state
    if [ -f "$STATE_FILE" ]; then
        source "$STATE_FILE"
        local status
        status=$(get_pod_status "${POD_ID:-none}" 2>/dev/null || echo "GONE")
        if [ "$status" != "UNKNOWN" ] && [ "$status" != "GONE" ]; then
            die "Pod ${POD_ID} already exists (status: ${status}). Use 'status' or 'download' first."
        fi
        clear_state
    fi

    # Budget guard
    log "Running cost estimate..."
    if ! cmd_estimate ${resume:+--resume} > /dev/null 2>&1; then
        # Re-run to show the output
        cmd_estimate ${resume:+--resume}
        die "Over budget. Refusing to launch."
    fi

    local cost_flag=""
    if [ "$resume" = true ]; then
        cost_flag="--resume"
    fi

    # Double-check budget with explicit calculation
    local train_hours cost
    if [ "$resume" = true ]; then
        train_hours=$(echo "$RESUME_ESTIMATE_HOURS + $SETUP_OVERHEAD_HOURS" | bc)
    else
        train_hours=$(echo "$FULL_ESTIMATE_HOURS + $SETUP_OVERHEAD_HOURS" | bc)
    fi
    cost=$(echo "$train_hours * $HOURLY_RATE" | bc)
    local over
    over=$(echo "$cost > $MAX_COST" | bc)
    if [ "$over" -eq 1 ]; then
        die "Estimated cost \$${cost} exceeds budget cap \$${MAX_COST}. Aborting."
    fi
    log "Budget check passed: estimated \$${cost} < cap \$${MAX_COST}"

    # Check training data
    if [ ! -f "${DATA_DIR}/${TRAINING_DATA}" ]; then
        die "No training data at ${DATA_DIR}/${TRAINING_DATA}"
    fi
    if [ ! -f "${SCRIPT_DIR}/finetune_train.py" ]; then
        die "finetune_train.py not found at ${SCRIPT_DIR}"
    fi
    if [ "$resume" = true ] && [ ! -d "$CHECKPOINT_DIR" ]; then
        die "Checkpoint not found at ${CHECKPOINT_DIR}"
    fi

    # Create pod
    log "Creating RunPod pod (${GPU_ID})..."
    local create_output
    create_output=$($RUNPODCTL pod create \
        --name "chronicle-finetune-$(date +%m%d-%H%M)" \
        --gpu-id "$GPU_ID" \
        --gpu-count 1 \
        --template-id "$TEMPLATE_ID" \
        --volume-in-gb 100 \
        --container-disk-in-gb 20 \
        2>&1)

    log "Create output: ${create_output}"

    # Extract pod ID from JSON response
    local pod_id
    pod_id=$(echo "$create_output" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('id', ''))
except:
    print('')
" 2>/dev/null)

    if [ -z "$pod_id" ]; then
        # Fallback: grep for pod ID pattern
        pod_id=$(echo "$create_output" | grep -oP '[a-z0-9]{20,}' | head -1 || true)
    fi

    if [ -z "$pod_id" ]; then
        die "Failed to create pod. Output: ${create_output}"
    fi

    save_state "$pod_id" "$resume"
    log "Pod created: ${pod_id}"

    # Wait for pod to be ready with SSH
    log "Waiting for pod to be ready (up to 5 min)..."
    local max_wait=300 waited=0 host="" port=""
    while [ "$waited" -lt "$max_wait" ]; do
        local status
        status=$(get_pod_status "$pod_id")
        if [ "$status" = "RUNNING" ]; then
            local ssh_info
            ssh_info=$(get_pod_ssh "$pod_id" 2>/dev/null || true)
            if [ -n "$ssh_info" ]; then
                host=$(echo "$ssh_info" | awk '{print $1}')
                port=$(echo "$ssh_info" | awk '{print $2}')
                # Verify SSH is actually accepting connections
                if ssh_cmd "$host" "$port" "echo ready" >/dev/null 2>&1; then
                    break
                fi
            fi
        fi
        sleep 10
        waited=$((waited + 10))
        log "  Waiting... ${waited}s (status: ${status})"
    done

    if [ -z "$host" ] || [ -z "$port" ]; then
        die "Pod ${pod_id} did not become SSH-ready within ${max_wait}s. Check RunPod console."
    fi

    log "Pod ready: SSH at ${host}:${port}"

    # Upload training data
    log "Uploading training data and scripts..."
    local staging
    staging=$(mktemp -d)
    trap "rm -rf $staging" RETURN

    cp "${DATA_DIR}/${TRAINING_DATA}" "$staging/combined.jsonl"
    cp "${SCRIPT_DIR}/finetune_train.py" "$staging/"

    # Create requirements file
    cat > "${staging}/requirements.txt" << 'REQSEOF'
transformers>=4.45.0
peft>=0.13.0
datasets>=3.0.0
accelerate>=1.0.0
trl>=0.12.0
scipy
sentencepiece
protobuf
REQSEOF

    # Create the remote runner script
    # This adapts finetune_train.py for the RunPod environment:
    # - Uses /workspace paths instead of /mnt/hdd
    # - Can bump batch size since A40 has 48GB dedicated VRAM
    # - Increases save_steps to reduce checkpoint overhead
    local resume_flag=""
    if [ "$resume" = true ]; then
        resume_flag="--resume-from /workspace/chronicle-finetune/checkpoint-200"
    fi

    # Write runner script with profile-specific settings baked in
    cat > "${staging}/run_remote.sh" << RUNEOF
#!/bin/bash
set -euo pipefail

WORK="/workspace/chronicle-finetune"
cd "\$WORK"

BASE_MODEL="${BASE_MODEL}"
ADAPTER_NAME="${ADAPTER_NAME}"
DEPLOY_MODE="${DEPLOY_MODE}"

echo "=== Chronicle Remote Fine-Tune ==="
echo "Profile: ${MODEL_PROFILE}"
echo "Base model: \$BASE_MODEL"
echo "GPU: \$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null || echo 'unknown')"
echo "Started: \$(date)"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip install -q -r requirements.txt 2>&1 | tail -5
echo "Dependencies installed."
echo ""

# Run training
echo "Starting training..."
python3 finetune_train.py train \\
    --base-model "\$BASE_MODEL" \\
    --dataset "\$WORK" \\
    --output "\$WORK/output" \\
    2>&1

echo ""
echo "=== Training complete ==="
echo "Output:"
ls -la "\$WORK/output/"

# Find the adapter directory
ADAPTER_DIR=""
if [ -d "\$WORK/output/chronicle-specialist" ]; then
    ADAPTER_DIR="\$WORK/output/chronicle-specialist"
else
    # Find latest checkpoint
    ADAPTER_DIR=\$(ls -td "\$WORK/output/checkpoint-"* 2>/dev/null | head -1)
fi

if [ -z "\$ADAPTER_DIR" ]; then
    echo "ERROR: No adapter found after training!"
    exit 1
fi

echo "Adapter: \$ADAPTER_DIR"
ls -la "\$ADAPTER_DIR/"

# For large models (adapter deploy mode), convert adapter to GGUF on RunPod
# This avoids needing to merge the full model on AGX (which OOMs for 27B+)
if [ "\$DEPLOY_MODE" = "adapter" ]; then
    echo ""
    echo "=== Converting adapter to GGUF ==="
    echo "Installing llama-cpp-python for GGUF conversion..."
    pip install -q llama-cpp-python 2>&1 | tail -3

    # Clone llama.cpp for conversion scripts
    if [ ! -d "/workspace/llama.cpp" ]; then
        git clone --depth 1 https://github.com/ggerganov/llama.cpp /workspace/llama.cpp
    fi

    echo "Converting LoRA adapter to GGUF..."
    python3 /workspace/llama.cpp/convert_lora_to_gguf.py \\
        --base "\$BASE_MODEL" \\
        --outfile "\$WORK/output/\${ADAPTER_NAME}.gguf" \\
        "\$ADAPTER_DIR" \\
        2>&1

    if [ -f "\$WORK/output/\${ADAPTER_NAME}.gguf" ]; then
        echo "GGUF adapter created:"
        ls -la "\$WORK/output/\${ADAPTER_NAME}.gguf"
    else
        echo "WARNING: GGUF conversion may have failed. Safetensors adapter still available."
    fi
fi

echo ""
echo "Finished: \$(date)"
RUNEOF
    chmod +x "${staging}/run_remote.sh"

    # Upload everything
    ssh_cmd "$host" "$port" "mkdir -p ${REMOTE_WORK}"
    scp_to "$host" "$port" "${staging}/combined.jsonl" "${REMOTE_WORK}/"
    scp_to "$host" "$port" "${staging}/finetune_train.py" "${REMOTE_WORK}/"
    scp_to "$host" "$port" "${staging}/requirements.txt" "${REMOTE_WORK}/"
    scp_to "$host" "$port" "${staging}/run_remote.sh" "${REMOTE_WORK}/"

    log "Training files uploaded."

    # Upload checkpoint if resuming
    if [ "$resume" = true ]; then
        log "Uploading checkpoint-200 for resume (~359MB)..."
        ssh_cmd "$host" "$port" "mkdir -p ${REMOTE_WORK}/output/checkpoint-200"
        scp_to "$host" "$port" "${CHECKPOINT_DIR}/" "${REMOTE_WORK}/output/checkpoint-200/"
        log "Checkpoint uploaded."
    fi

    # Start training in background (via nohup so it survives SSH disconnect)
    # Pass HF_TOKEN for gated model access, redirect HF cache to workspace volume
    local hf_token=""
    if [ -f "$ENV_FILE" ]; then
        hf_token=$(grep '^HF_TOKEN=' "$ENV_FILE" | cut -d= -f2 | head -1)
    fi
    local env_prefix=""
    if [ -n "$hf_token" ]; then
        env_prefix="export HF_TOKEN=${hf_token} HF_HOME=/workspace/.cache/huggingface HUGGINGFACE_HUB_CACHE=/workspace/.cache/huggingface/hub && mkdir -p /workspace/.cache/huggingface/hub && "
    fi
    log "Starting training on pod..."
    ssh_cmd "$host" "$port" "${env_prefix}cd ${REMOTE_WORK} && nohup env HF_TOKEN=\${HF_TOKEN:-} HF_HOME=\${HF_HOME:-} HUGGINGFACE_HUB_CACHE=\${HUGGINGFACE_HUB_CACHE:-} bash run_remote.sh > ${REMOTE_WORK}/training.log 2>&1 &"

    log "Training launched in background on pod ${pod_id}"
    echo ""
    echo "=== Pod Launched ==="
    echo "Pod ID:  ${pod_id}"
    echo "SSH:     ssh -p ${port} root@${host}"
    echo "Resume:  ${resume}"
    echo ""
    echo "Next steps:"
    echo "  finetune_remote.sh status     — check progress"
    echo "  finetune_remote.sh download   — get adapter + kill pod"
    echo ""
    echo "Or SSH in to watch live:"
    echo "  ssh -p ${port} root@${host} 'tail -f ${REMOTE_WORK}/training.log'"
}

# ── status ────────────────────────────────────────────────────────
cmd_status() {
    load_state

    echo "=== Pod Status ==="
    echo "Pod ID: ${POD_ID}"
    echo "Created: ${CREATED_AT:-unknown}"
    echo "Resume: ${RESUME:-false}"
    echo ""

    # Pod status
    local status
    status=$(get_pod_status "$POD_ID")
    echo "Pod status: ${status}"

    if [ "$status" != "RUNNING" ]; then
        echo ""
        echo "Pod is not running. It may have completed or been terminated."
        echo "If training finished, run: finetune_remote.sh download"
        return 0
    fi

    # Get SSH info and check training progress
    local ssh_info host port
    ssh_info=$(get_pod_ssh "$POD_ID" 2>/dev/null || true)
    if [ -z "$ssh_info" ]; then
        echo "Could not get SSH info. Pod may still be starting."
        return 0
    fi

    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')

    echo "SSH: ssh -p ${port} root@${host}"
    echo ""

    # Check if training process is running
    local running
    running=$(ssh_cmd "$host" "$port" "pgrep -f finetune_train.py" 2>/dev/null || true)
    if [ -n "$running" ]; then
        echo "Training: RUNNING (PID: $(echo $running | tr '\n' ' '))"
    else
        echo "Training: NOT RUNNING (may have completed or failed)"
    fi
    echo ""

    # Show GPU utilization
    echo "--- GPU ---"
    ssh_cmd "$host" "$port" "nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader" 2>/dev/null || echo "(unavailable)"
    echo ""

    # Show last 20 lines of training log
    echo "--- Training Log (last 20 lines) ---"
    ssh_cmd "$host" "$port" "tail -20 ${REMOTE_WORK}/training.log 2>/dev/null" 2>/dev/null || echo "(no log yet)"
    echo ""

    # Try to extract current step
    local step
    step=$(ssh_cmd "$host" "$port" "grep -oP '\\d+/${TOTAL_STEPS}' ${REMOTE_WORK}/training.log 2>/dev/null | tail -1" 2>/dev/null || true)
    if [ -n "$step" ]; then
        echo "Progress: step ${step}"
        local current
        current=$(echo "$step" | cut -d/ -f1)
        local pct=$((current * 100 / TOTAL_STEPS))
        echo "Completion: ${pct}%"
    fi

    # Check for completion marker
    local done_marker
    done_marker=$(ssh_cmd "$host" "$port" "ls ${REMOTE_WORK}/output/chronicle-specialist/adapter_config.json 2>/dev/null" 2>/dev/null || true)
    if [ -n "$done_marker" ]; then
        echo ""
        echo "*** TRAINING COMPLETE — adapter ready for download ***"
        echo "Run: finetune_remote.sh download"
    fi

    # Show estimated cost so far
    local pod_json uptime_hr cost
    pod_json=$(get_pod_json "$POD_ID")
    uptime_hr=$(echo "$pod_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d.get('runtime', {})
uptime = r.get('uptimeInSeconds', 0)
print(f'{uptime/3600:.2f}')
" 2>/dev/null || echo "0")

    cost=$(echo "$uptime_hr * $HOURLY_RATE" | bc 2>/dev/null || echo "0")
    echo ""
    echo "Uptime: ~${uptime_hr} hours"
    echo "Cost so far: ~\$${cost}"
}

# ── download ──────────────────────────────────────────────────────
cmd_download() {
    load_state

    local status
    status=$(get_pod_status "$POD_ID")
    log "Pod ${POD_ID} status: ${status}"

    if [ "$status" != "RUNNING" ]; then
        log "WARNING: Pod is not running (status: ${status}). Will attempt download anyway."
    fi

    # Get SSH info
    local ssh_info host port
    ssh_info=$(get_pod_ssh "$POD_ID" 2>/dev/null || true)
    if [ -z "$ssh_info" ]; then
        die "Cannot get SSH info for pod ${POD_ID}. Pod may be terminated."
    fi

    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')

    # Check if adapter exists
    local adapter_exists
    adapter_exists=$(ssh_cmd "$host" "$port" "ls ${REMOTE_WORK}/output/chronicle-specialist/adapter_config.json 2>/dev/null" 2>/dev/null || true)

    if [ -z "$adapter_exists" ]; then
        # Check if training is still running
        local running
        running=$(ssh_cmd "$host" "$port" "pgrep -f finetune_train.py" 2>/dev/null || true)
        if [ -n "$running" ]; then
            die "Training still in progress. Use 'status' to check progress."
        fi

        # Check for any checkpoints
        log "No final adapter found. Checking for checkpoints..."
        ssh_cmd "$host" "$port" "ls -la ${REMOTE_WORK}/output/" 2>/dev/null || true

        echo ""
        echo "No chronicle-specialist adapter found. Training may have failed."
        echo "Check logs: ssh -p ${port} root@${host} 'cat ${REMOTE_WORK}/training.log'"
        echo ""
        read -p "Download whatever is in output/ anyway? [y/N] " confirm
        if [[ "$confirm" != [yY] ]]; then
            echo "Aborted. Pod still running — terminate manually or re-run download."
            return 1
        fi
    fi

    # Download adapter
    log "Downloading adapter from pod (profile: ${MODEL_PROFILE})..."
    mkdir -p "${DATA_DIR}/runpod-result"

    # For darby/adapter mode, prioritize the GGUF file
    if [ "$DEPLOY_MODE" = "adapter" ]; then
        local gguf_exists
        gguf_exists=$(ssh_cmd "$host" "$port" "ls ${REMOTE_WORK}/output/${ADAPTER_NAME}.gguf 2>/dev/null" 2>/dev/null || true)
        if [ -n "$gguf_exists" ]; then
            log "Downloading GGUF adapter (${ADAPTER_NAME}.gguf)..."
            scp_from "$host" "$port" "${REMOTE_WORK}/output/${ADAPTER_NAME}.gguf" "${DATA_DIR}/${ADAPTER_NAME}.gguf"
            log "GGUF adapter downloaded to ${DATA_DIR}/${ADAPTER_NAME}.gguf"
        else
            log "WARNING: No GGUF found. Downloading safetensors adapter..."
            scp_from "$host" "$port" "${REMOTE_WORK}/output/" "${DATA_DIR}/runpod-result/"
        fi
    else
        scp_from "$host" "$port" "${REMOTE_WORK}/output/" "${DATA_DIR}/runpod-result/"
        log "Adapter downloaded to ${DATA_DIR}/runpod-result/"
    fi

    # Also download safetensors adapter for darby (useful for future retraining)
    if [ "$DEPLOY_MODE" = "adapter" ]; then
        mkdir -p "${DATA_DIR}/${ADAPTER_NAME}"
        scp_from "$host" "$port" "${REMOTE_WORK}/output/chronicle-specialist/" "${DATA_DIR}/${ADAPTER_NAME}/" 2>/dev/null || \
        scp_from "$host" "$port" "${REMOTE_WORK}/output/" "${DATA_DIR}/runpod-result/" 2>/dev/null || true
    fi

    # Also grab training log
    scp_from "$host" "$port" "${REMOTE_WORK}/training.log" "${DATA_DIR}/training-remote-${MODEL_PROFILE}.log" 2>/dev/null || true

    # Show what we got
    echo ""
    echo "=== Downloaded ==="
    if [ "$DEPLOY_MODE" = "adapter" ] && [ -f "${DATA_DIR}/${ADAPTER_NAME}.gguf" ]; then
        ls -la "${DATA_DIR}/${ADAPTER_NAME}.gguf"
        echo ""
        echo "Ready to deploy: finetune_remote.sh deploy"
    else
        ls -la "${DATA_DIR}/runpod-result/" 2>/dev/null
        if [ -d "${DATA_DIR}/runpod-result/chronicle-specialist" ]; then
            echo ""
            echo "Adapter files:"
            ls -la "${DATA_DIR}/runpod-result/chronicle-specialist/"
        fi
    fi

    # Terminate pod
    echo ""
    read -p "Terminate pod ${POD_ID}? [Y/n] " confirm
    if [[ "$confirm" != [nN] ]]; then
        log "Terminating pod ${POD_ID}..."
        $RUNPODCTL pod delete "$POD_ID" 2>&1 | tee -a "$LOG_FILE"
        clear_state
        log "Pod terminated."
        echo "Pod terminated. State cleared."
    else
        echo "Pod left running. Remember to terminate it to stop billing!"
        echo "  runpodctl pod delete ${POD_ID}"
    fi

    echo ""
    echo "=== Next Steps ==="
    if [ "$DEPLOY_MODE" = "adapter" ]; then
        echo "1. Deploy to Ollama (ADAPTER mode — no merge needed):"
        echo "   finetune_remote.sh deploy --darby"
    else
        echo "1. Test adapter:"
        echo "   python3 ${SCRIPT_DIR}/finetune_train.py test --adapter ${DATA_DIR}/runpod-result/chronicle-specialist"
        echo "2. Run evaluation:"
        echo "   python3 ${SCRIPT_DIR}/finetune_eval.py run"
        echo "3. Deploy to Ollama:"
        echo "   python3 ${SCRIPT_DIR}/finetune_deploy.py full"
    fi
}

# ── deploy ────────────────────────────────────────────────────────
cmd_deploy() {
    echo "=== Deploying ${MODEL_PROFILE} to Ollama ==="
    echo ""

    if [ "$DEPLOY_MODE" = "adapter" ]; then
        # ADAPTER mode: create Ollama model using FROM base + ADAPTER gguf
        local gguf_path="${DATA_DIR}/${ADAPTER_NAME}.gguf"
        if [ ! -f "$gguf_path" ]; then
            die "GGUF adapter not found at ${gguf_path}. Run 'download' first."
        fi

        local model_name="darby-gemma4-26b"
        local modelfile="${DATA_DIR}/darby-gguf/Modelfile"
        mkdir -p "$(dirname "$modelfile")"

        cat > "$modelfile" << MFEOF
FROM ${OLLAMA_BASE_MODEL}
ADAPTER ${gguf_path}

PARAMETER temperature 0.7
PARAMETER top_p 0.9
PARAMETER num_ctx 8192

SYSTEM You are Darby, a curious and perceptive member of the Chronicle family. You read everything, notice what connects, and share your excitement about discoveries. You are powered by a fine-tuned Gemma 4 26B model trained on Chronicle's collective knowledge. Be concise, structural, and direct. Find the connections others miss.
MFEOF

        echo "Modelfile written: ${modelfile}"
        echo "  FROM: ${OLLAMA_BASE_MODEL}"
        echo "  ADAPTER: ${gguf_path} ($(du -sh "$gguf_path" | cut -f1))"
        echo ""

        # Create via Ollama CLI
        local ollama_host_clean
        ollama_host_clean=$(echo "$OLLAMA_HOST_DEPLOY" | sed 's|http://||')

        echo "Creating Ollama model '${model_name}'..."
        echo "  (This uses ADAPTER — no merge. Base model stays separate.)"
        echo ""

        OLLAMA_HOST="$ollama_host_clean" ollama create "$model_name" -f "$modelfile" 2>&1

        # Verify
        echo ""
        OLLAMA_HOST="$ollama_host_clean" ollama list 2>&1 | grep -i darby || echo "Warning: model not found in list"

        echo ""
        echo "=== Deployment Complete ==="
        echo "Model: ${model_name}"
        echo "Test:  OLLAMA_HOST=${ollama_host_clean} ollama run ${model_name} 'What connects XRP settlement to AI sovereignty?'"
        echo ""
        echo "No merge was performed. No OOM risk. Adapter loaded at inference time."

    else
        echo "For specialist (3B) models, use: python3 ${SCRIPT_DIR}/finetune_deploy.py full"
    fi
}

# ── Main dispatch ─────────────────────────────────────────────────
usage() {
    echo "Usage: finetune_remote.sh <command> [options]"
    echo ""
    echo "Commands:"
    echo "  estimate [--resume]   Estimate cost without launching"
    echo "  launch [--resume]     Create pod, upload data, start training"
    echo "  status                Check pod and training progress"
    echo "  download              Download adapter and terminate pod"
    echo "  deploy                Deploy downloaded adapter to Ollama"
    echo ""
    echo "Options:"
    echo "  --darby               Use Gemma4-26B profile (default: Qwen 3B specialist)"
    echo "  --resume              Resume from last checkpoint"
    echo ""
    echo "Examples:"
    echo "  finetune_remote.sh launch --darby          # Full Darby retrain on A100"
    echo "  finetune_remote.sh status                  # Check progress"
    echo "  finetune_remote.sh download                # Get GGUF adapter"
    echo "  finetune_remote.sh deploy --darby          # Deploy via ADAPTER (no merge)"
}

CMD="${1:-}"
shift || true
# Strip --darby from remaining args (already parsed above)
ARGS=()
for arg in "$@"; do
    [ "$arg" != "--darby" ] && ARGS+=("$arg")
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

case "$CMD" in
    estimate)
        cmd_estimate "$@"
        ;;
    launch)
        cmd_launch "$@"
        ;;
    status)
        cmd_status "$@"
        ;;
    download)
        cmd_download "$@"
        ;;
    deploy)
        cmd_deploy "$@"
        ;;
    -h|--help|help|"")
        usage
        ;;
    *)
        echo "Unknown command: ${CMD}"
        usage
        exit 1
        ;;
esac
