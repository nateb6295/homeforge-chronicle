#!/bin/bash
# cvector_remote.sh — Generate control vectors for Gemma on RunPod
#
# AGX can't load two models simultaneously (64GB shared, Gemma uses 40GB).
# RunPod A100 has 80GB VRAM — loads the model, generates vectors, downloads result.
#
# Flow:
#   1. Create RunPod pod (A100 80GB)
#   2. Upload contrast pairs + build llama.cpp on pod
#   3. Download Gemma GGUF from HuggingFace on pod
#   4. Run llama-cvector-generator
#   5. Download control_vector.gguf back to AGX
#   6. Terminate pod
#
# Commands:
#   cvector_remote.sh estimate              — estimate cost
#   cvector_remote.sh launch [VECTOR_NAME]  — create pod, start generation
#   cvector_remote.sh status                — check pod status
#   cvector_remote.sh download              — download vector and terminate
#   cvector_remote.sh apply [VECTOR_NAME]   — restart llama-server with vector
#
# Budget: ~$2-5 per vector (1-3h on A100 at $1.64/hr)

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHRONICLE_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${CHRONICLE_DIR}/chronicle.env"
CVECTOR_DIR="${CHRONICLE_DIR}/cvectors"
OUTPUT_DIR="/mnt/hdd/models/cvectors"
STATE_FILE="${CHRONICLE_DIR}/.cvector-pod-state"
LOG_FILE="${CHRONICLE_DIR}/cvector-remote.log"

GPU_ID="${RUNPOD_GPU:-NVIDIA A100-SXM4-80GB}"
HOURLY_RATE=1.64
TEMPLATE_ID="runpod-torch-v240"
REMOTE_WORK="/workspace/cvector"
MAX_COST="${RUNPOD_MAX_COST:-10}"

# Model — same GGUF we use locally
MODEL_REPO="bartowski/google_gemma-4-26b-it-GGUF"
MODEL_FILE="gemma-4-26B-A4B-it-Q4_K_M.gguf"

# Default vector
VECTOR_NAME="${2:-critical_analysis}"

RUNPODCTL="/usr/local/bin/runpodctl"
RUNPOD_SSH_KEY="${HOME}/.runpod/ssh/RunPod-Key-Go"

# ── Load env ──────────────────────────────────────────────────────
if [ -f "$ENV_FILE" ]; then
    set -a; source "$ENV_FILE"; set +a
fi

if [ -z "${RUNPOD_API_KEY:-}" ]; then
    echo "ERROR: RUNPOD_API_KEY not set in ${ENV_FILE}"
    exit 1
fi
export RUNPOD_API_KEY

# ── Helpers ───────────────────────────────────────────────────────
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

get_pod_state() {
    if [ -f "$STATE_FILE" ]; then
        cat "$STATE_FILE"
    fi
}

save_pod_state() {
    echo "$1" > "$STATE_FILE"
}

get_pod_json() {
    $RUNPODCTL pod get "$1" 2>/dev/null
}

get_pod_ssh() {
    local pod_json
    pod_json=$(get_pod_json "$1")
    local host port
    host=$(echo "$pod_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ssh = d.get('ssh', {})
if ssh.get('ip'):
    print(ssh['ip'])
else:
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
ssh = d.get('ssh', {})
if ssh.get('port'):
    print(ssh['port'])
else:
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

ssh_cmd() {
    local pod_id="$1"; shift
    local ssh_info host port
    ssh_info=$(get_pod_ssh "$pod_id")
    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=15 -o LogLevel=ERROR \
        $key_opt -p "$port" "root@${host}" "$@"
}

scp_to() {
    local pod_id="$1" local_path="$2" remote_path="$3"
    local ssh_info host port
    ssh_info=$(get_pod_ssh "$pod_id")
    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -P "$port" \
        $key_opt "$local_path" "root@${host}:${remote_path}"
}

scp_from() {
    local pod_id="$1" remote_path="$2" local_path="$3"
    local ssh_info host port
    ssh_info=$(get_pod_ssh "$pod_id")
    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -P "$port" \
        $key_opt "root@${host}:${remote_path}" "$local_path"
}

# ── Commands ──────────────────────────────────────────────────────

cmd_estimate() {
    echo "=== Control Vector Generation Estimate ==="
    echo "GPU:       ${GPU_ID}"
    echo "Rate:      \$${HOURLY_RATE}/hr"
    echo "Model:     ${MODEL_FILE}"
    echo "Vector:    ${VECTOR_NAME}"
    echo ""
    echo "Steps:"
    echo "  1. Pod startup + llama.cpp build:  ~10 min"
    echo "  2. Model download (HuggingFace):   ~5-15 min"
    echo "  3. Vector generation:              ~30-90 min"
    echo "  4. Download result:                ~1 min"
    echo ""
    echo "Estimated time:  1-2 hours"
    echo "Estimated cost:  \$1.64-\$3.28"
    echo "Max budget:      \$${MAX_COST}"
    echo ""

    # Check contrast pairs exist
    local pos_file="${CVECTOR_DIR}/${VECTOR_NAME%%_*}_positive.txt"
    local neg_file="${CVECTOR_DIR}/${VECTOR_NAME%%_*}_negative.txt"
    if [ -f "$pos_file" ] && [ -f "$neg_file" ]; then
        local pos_count neg_count
        pos_count=$(wc -l < "$pos_file")
        neg_count=$(wc -l < "$neg_file")
        echo "Contrast pairs:  ${pos_count} positive, ${neg_count} negative"
    else
        echo "WARNING: Contrast pair files not found:"
        echo "  Expected: ${pos_file}"
        echo "  Expected: ${neg_file}"
    fi

    # Check RunPod balance
    balance=$($RUNPODCTL user 2>/dev/null | python3 -c "import sys,json; print(f\"{json.load(sys.stdin).get('clientBalance', 0):.2f}\")" 2>/dev/null || echo "unknown")
    echo "RunPod balance:  \$${balance}"
}

cmd_launch() {
    log "=== Launching cvector generation pod ==="
    log "Vector: ${VECTOR_NAME}, GPU: ${GPU_ID}"

    # Check for existing pod
    local existing_pod
    existing_pod=$(get_pod_state)
    if [ -n "$existing_pod" ]; then
        echo "ERROR: Pod already running: ${existing_pod}"
        echo "Use 'cvector_remote.sh status' or 'cvector_remote.sh download'"
        exit 1
    fi

    # Check contrast pairs
    local pos_file="${CVECTOR_DIR}/critical_positive.txt"
    local neg_file="${CVECTOR_DIR}/critical_negative.txt"
    if [ ! -f "$pos_file" ] || [ ! -f "$neg_file" ]; then
        echo "ERROR: Contrast pair files not found in ${CVECTOR_DIR}"
        exit 1
    fi

    # Create pod
    log "Creating RunPod pod..."
    local create_output
    create_output=$($RUNPODCTL pod create \
        --name "chronicle-cvector-${VECTOR_NAME}" \
        --gpu-id "$GPU_ID" \
        --gpu-count 1 \
        --volume-in-gb 80 \
        --container-disk-in-gb 20 \
        --template-id "$TEMPLATE_ID" \
        2>&1) || { log "ERROR: Pod creation failed: $create_output"; exit 1; }

    log "Create output: ${create_output}"

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
        pod_id=$(echo "$create_output" | grep -oP '[a-z0-9]{20,}' | head -1 || true)
    fi

    if [ -z "$pod_id" ]; then
        log "ERROR: Could not extract pod ID from: $create_output"
        exit 1
    fi

    save_pod_state "$pod_id"
    log "Pod created: ${pod_id}"
    log "Waiting for pod to be ready..."

    # Wait for SSH
    local retries=0
    while [ $retries -lt 30 ]; do
        if ssh_cmd "$pod_id" "echo ready" 2>/dev/null; then
            break
        fi
        retries=$((retries + 1))
        sleep 10
    done

    if [ $retries -ge 30 ]; then
        log "ERROR: Pod not reachable after 5 min"
        exit 1
    fi

    log "Pod ready. Setting up environment..."

    # Setup script — runs on pod
    ssh_cmd "$pod_id" "bash -s" << 'SETUP'
set -e
mkdir -p /workspace/cvector
cd /workspace/cvector

# Install llama.cpp
if [ ! -f bin/llama-cvector-generator ]; then
    echo "Building llama.cpp..."
    git clone --depth 1 https://github.com/ggml-org/llama.cpp.git 2>&1 | tail -5
    cd llama.cpp
    cmake -B build -DGGML_CUDA=ON -DCMAKE_CUDA_ARCHITECTURES=80 2>&1 | tail -5
    cmake --build build --target llama-cvector-generator -j$(nproc) 2>&1 | tail -10
    cd ..
    mkdir -p bin
    cp llama.cpp/build/bin/llama-cvector-generator bin/
    echo "llama.cpp built."
fi

echo "Setup complete."
SETUP

    log "Uploading contrast pairs..."
    scp_to "$pod_id" "$pos_file" "/workspace/cvector/positive.txt"
    scp_to "$pod_id" "$neg_file" "/workspace/cvector/negative.txt"

    log "Downloading model on pod (this takes a while)..."
    ssh_cmd "$pod_id" "bash -s" << DOWNLOAD
set -e
cd /workspace/cvector
if [ ! -f "${MODEL_FILE}" ]; then
    echo "Downloading ${MODEL_FILE} from HuggingFace..."
    pip install -q huggingface_hub 2>/dev/null
    python3 -c "
from huggingface_hub import hf_hub_download
hf_hub_download('${MODEL_REPO}', '${MODEL_FILE}', local_dir='/workspace/cvector')
print('Download complete.')
"
fi
echo "Model ready."
DOWNLOAD

    log "Starting vector generation..."
    ssh_cmd "$pod_id" "bash -s" << 'GENERATE'
set -e
cd /workspace/cvector
nohup bash -c '
./bin/llama-cvector-generator \
    --model gemma-4-26B-A4B-it-Q4_K_M.gguf \
    --positive-file positive.txt \
    --negative-file negative.txt \
    --output control_vector.gguf \
    --ctx-size 512 \
    --threads 8 \
    --n-gpu-layers 99 \
    2>&1 | tee cvector.log
echo "DONE" >> cvector.log
' > /dev/null 2>&1 &
echo "Generation started in background. Check with: cvector_remote.sh status"
GENERATE

    log "Vector generation running on pod ${pod_id}."
    log "Check progress: cvector_remote.sh status"
    log "When done: cvector_remote.sh download"
}

cmd_status() {
    local pod_id
    pod_id=$(get_pod_state)
    if [ -z "$pod_id" ]; then
        echo "No active cvector pod."
        return
    fi

    echo "Pod: ${pod_id}"

    # Check if generation is done
    local log_tail
    log_tail=$(ssh_cmd "$pod_id" "tail -20 /workspace/cvector/cvector.log 2>/dev/null" 2>/dev/null || echo "[not reachable]")
    echo ""
    echo "=== Generation Log (last 20 lines) ==="
    echo "$log_tail"

    if echo "$log_tail" | grep -q "DONE"; then
        echo ""
        echo "✓ Generation complete! Run: cvector_remote.sh download"
    fi
}

cmd_download() {
    local pod_id
    pod_id=$(get_pod_state)
    if [ -z "$pod_id" ]; then
        echo "No active cvector pod."
        return 1
    fi

    mkdir -p "$OUTPUT_DIR"

    log "Downloading control vector..."
    scp_from "$pod_id" "/workspace/cvector/control_vector.gguf" \
        "${OUTPUT_DIR}/${VECTOR_NAME}.gguf"

    local size
    size=$(du -sh "${OUTPUT_DIR}/${VECTOR_NAME}.gguf" | cut -f1)
    log "Downloaded: ${OUTPUT_DIR}/${VECTOR_NAME}.gguf (${size})"

    # Also grab the log
    scp_from "$pod_id" "/workspace/cvector/cvector.log" \
        "${CVECTOR_DIR}/${VECTOR_NAME}.log" 2>/dev/null || true

    # Terminate pod
    log "Terminating pod ${pod_id}..."
    $RUNPODCTL pod delete "$pod_id" 2>&1 | tee -a "$LOG_FILE"
    rm -f "$STATE_FILE"

    log "Done. Vector saved to ${OUTPUT_DIR}/${VECTOR_NAME}.gguf"
    log "Apply with: cvector_remote.sh apply ${VECTOR_NAME}"
}

cmd_apply() {
    local vector_file="${OUTPUT_DIR}/${VECTOR_NAME}.gguf"
    if [ ! -f "$vector_file" ]; then
        echo "ERROR: Vector file not found: ${vector_file}"
        echo "Generate first with: cvector_remote.sh launch ${VECTOR_NAME}"
        exit 1
    fi

    echo "=== Applying control vector ==="
    echo "Vector: ${vector_file}"
    echo ""
    echo "To apply, update the llama-server systemd service:"
    echo ""
    echo "  ExecStart=/home/nate-agx/llama.cpp/build/bin/llama-server \\"
    echo "    --model /mnt/hdd/models/gemma-4-26B-A4B-it-Q4_K_M.gguf \\"
    echo "    --mmproj /mnt/hdd/models/mmproj-gemma-4-26B-A4B-it-f16.gguf \\"
    echo "    --control-vector ${vector_file} \\"
    echo "    --control-vector-layer-range 25 35 \\"
    echo "    --port 11435 --ctx-size 8192 --n-gpu-layers 99 --threads 8 --flash-attn on"
    echo ""
    echo "Then: systemctl --user restart chronicle-llama"
    echo ""
    echo "WARNING: This affects ALL Gemma inference (captures, synthesis, spot checks)."
    echo "Test with a small alpha first. Add --control-vector-scaled for fine control."
}

# ── Main ──────────────────────────────────────────────────────────
CMD="${1:-estimate}"

case "$CMD" in
    estimate)  cmd_estimate ;;
    launch)    cmd_launch ;;
    status)    cmd_status ;;
    download)  cmd_download ;;
    apply)     cmd_apply ;;
    *)
        echo "Usage: cvector_remote.sh {estimate|launch|status|download|apply} [VECTOR_NAME]"
        echo "Default vector: critical_analysis"
        exit 1
        ;;
esac
