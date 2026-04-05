#!/bin/bash
# merge_remote.sh — Merge LoRA adapter into base model on RunPod, convert to GGUF
#
# The AGX OOM'd trying to merge a 27B model locally (52GB base + adapter into 61GB
# unified memory with services running). RunPod A40 has 48GB VRAM + 50GB+ system RAM.
#
# Flow:
#   1. Create RunPod pod
#   2. Upload LoRA adapter (233MB) — base model downloads from HuggingFace on RunPod
#   3. Merge adapter into base model via PEFT merge_and_unload()
#   4. Convert merged model to GGUF via llama.cpp
#   5. Quantize to Q4_K_M (~16GB)
#   6. Download GGUF back to AGX
#   7. Terminate pod
#
# Commands:
#   merge_remote.sh estimate              — estimate cost
#   merge_remote.sh launch                — create pod, upload adapter, start merge
#   merge_remote.sh status                — check pod status and merge progress
#   merge_remote.sh download              — download GGUF and terminate pod
#   merge_remote.sh import                — import downloaded GGUF into Ollama
#
# Budget cap: $15/job (configurable via RUNPOD_MAX_COST)
# GPU: NVIDIA A40 48GB (~$0.39/hr secure cloud)

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHRONICLE_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${CHRONICLE_DIR}/chronicle.env"
DATA_DIR="/mnt/hdd/chronicle-finetune"
ADAPTER_DIR="${DATA_DIR}/darby-adapter"
OUTPUT_DIR="${DATA_DIR}/darby-gguf"
STATE_FILE="${CHRONICLE_DIR}/.merge-pod-state"
LOG_FILE="${CHRONICLE_DIR}/merge-remote.log"

# Base model — downloaded from HuggingFace on RunPod (not uploaded)
BASE_MODEL="google/gemma-4-26b-it"
MODEL_NAME="darby-gemma4-26b"

GPU_ID="${RUNPOD_GPU:-NVIDIA A40}"
MAX_COST="${RUNPOD_MAX_COST:-15}"
TEMPLATE_ID="runpod-torch-v240"  # PyTorch 2.4 + CUDA 12.4
REMOTE_WORK="/workspace/merge"

# Time estimates
# Download base from HF: ~5-10 min (52GB on datacenter network)
# Merge adapter: ~5-10 min (load + merge_and_unload + save)
# Clone llama.cpp + convert to GGUF: ~10-15 min
# Quantize Q4_K_M: ~5 min
# Total: ~30-40 min
ESTIMATE_HOURS=0.7
HOURLY_RATE=0.39

# ── Load env ──────────────────────────────────────────────────────
if [ -f "$ENV_FILE" ]; then
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
    cat > "$STATE_FILE" << EOF
POD_ID=$1
CREATED_AT=$(date -Iseconds)
EOF
}

load_state() {
    if [ ! -f "$STATE_FILE" ]; then
        die "No active pod. Run 'merge_remote.sh launch' first."
    fi
    source "$STATE_FILE"
    if [ -z "${POD_ID:-}" ]; then
        die "State file corrupt. Remove ${STATE_FILE} and re-launch."
    fi
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
    # Primary: parse pod JSON which includes ssh.ip and ssh.port
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

RUNPOD_SSH_KEY="${HOME}/.runpod/ssh/RunPod-Key-Go"

ssh_cmd() {
    local host="$1" port="$2"
    shift 2
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=15 -o LogLevel=ERROR \
        $key_opt -p "$port" "root@${host}" "$@"
}

scp_to() {
    local host="$1" port="$2" src="$3" dst="$4"
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=15 -o LogLevel=ERROR \
        $key_opt -P "$port" -r "$src" "root@${host}:${dst}"
}

scp_from() {
    local host="$1" port="$2" src="$3" dst="$4"
    local key_opt=""
    if [ -f "$RUNPOD_SSH_KEY" ]; then
        key_opt="-i $RUNPOD_SSH_KEY"
    fi
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=15 -o LogLevel=ERROR \
        $key_opt -P "$port" -r "root@${host}:${src}" "$dst"
}

discord_post() {
    local msg="$1"
    local webhook='https://discord.com/api/webhooks/1483843624926970057/2hZYzQQcyDEVD0A9UQqJsHlnV9D1m-6AfwNCnNWxGUC_8A0-ViX2dRVkBHF17_b2oDxJ'
    curl -s -X POST -H 'Content-Type: application/json' -d "{\"content\": \"$msg\"}" "$webhook" > /dev/null 2>&1 || true
}

# ── estimate ──────────────────────────────────────────────────────
cmd_estimate() {
    echo "=== Chronicle Model Merge Cost Estimate ==="
    echo ""
    echo "GPU:            ${GPU_ID} (48 GB VRAM)"
    echo "Rate:           \$${HOURLY_RATE}/hr (secure cloud)"
    echo "Budget cap:     \$${MAX_COST}"
    echo ""
    echo "Base model:     ${BASE_MODEL} (~52 GB, downloaded from HF on pod)"
    echo "Adapter:        ${ADAPTER_DIR} (~233 MB LoRA r=16)"
    echo "Output:         GGUF Q4_K_M (~16 GB)"
    echo ""
    echo "Steps:"
    echo "  1. Download base model from HuggingFace   ~8 min"
    echo "  2. Merge LoRA adapter (PEFT)              ~8 min"
    echo "  3. Convert to GGUF (llama.cpp)            ~10 min"
    echo "  4. Quantize Q4_K_M                        ~5 min"
    echo "  5. Setup + deps                           ~5 min"
    echo ""

    local cost
    cost=$(echo "$ESTIMATE_HOURS * $HOURLY_RATE" | bc)

    echo "Estimated time: ~$(echo "$ESTIMATE_HOURS * 60" | bc | cut -d. -f1) minutes"
    echo "Estimated cost: \$${cost}"
    echo ""

    local over
    over=$(echo "$cost > $MAX_COST" | bc)
    if [ "$over" -eq 1 ]; then
        echo "WARNING: Estimated cost exceeds budget cap of \$${MAX_COST}!"
        return 1
    fi

    echo "Within budget cap. Safe to launch."

    # Check adapter exists
    if [ ! -f "${ADAPTER_DIR}/adapter_config.json" ]; then
        echo ""
        echo "WARNING: Adapter not found at ${ADAPTER_DIR}"
        return 1
    fi

    local adapter_size
    adapter_size=$(du -sh "${ADAPTER_DIR}" | cut -f1)
    echo "Adapter size: ${adapter_size}"

    # Show account balance
    local balance
    balance=$($RUNPODCTL user 2>/dev/null | python3 -c "import sys,json; print(f\"{json.load(sys.stdin).get('clientBalance', 0):.2f}\")" 2>/dev/null || echo "unknown")
    echo "RunPod balance: \$${balance}"
}

# ── launch ────────────────────────────────────────────────────────
cmd_launch() {
    # Preflight
    if [ -f "$STATE_FILE" ]; then
        source "$STATE_FILE"
        local status
        status=$(get_pod_status "${POD_ID:-none}" 2>/dev/null || echo "GONE")
        if [ "$status" != "UNKNOWN" ] && [ "$status" != "GONE" ]; then
            die "Pod ${POD_ID} already exists (status: ${status}). Use 'status' or 'download' first."
        fi
        clear_state
    fi

    if [ ! -f "${ADAPTER_DIR}/adapter_config.json" ]; then
        die "Adapter not found at ${ADAPTER_DIR}/adapter_config.json"
    fi

    # Budget guard
    local cost
    cost=$(echo "$ESTIMATE_HOURS * $HOURLY_RATE" | bc)
    local over
    over=$(echo "$cost > $MAX_COST" | bc)
    if [ "$over" -eq 1 ]; then
        die "Estimated cost \$${cost} exceeds budget cap \$${MAX_COST}. Aborting."
    fi
    log "Budget check passed: estimated \$${cost} < cap \$${MAX_COST}"

    # Create pod — needs 150GB volume for base model + merged output + GGUF
    log "Creating RunPod pod (${GPU_ID})..."
    local create_output
    create_output=$($RUNPODCTL pod create \
        --name "chronicle-merge-$(date +%m%d-%H%M)" \
        --gpu-id "$GPU_ID" \
        --gpu-count 1 \
        --template-id "$TEMPLATE_ID" \
        --volume-in-gb 150 \
        --container-disk-in-gb 30 \
        2>&1)

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
        die "Failed to create pod. Output: ${create_output}"
    fi

    save_state "$pod_id"
    log "Pod created: ${pod_id}"
    discord_post "🔧 **Merge pod created**: ${pod_id}. Merging darby-adapter into gemma-4-26b-it → GGUF Q4_K_M."

    # Wait for pod to be ready
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
        die "Pod ${pod_id} did not become SSH-ready within ${max_wait}s."
    fi

    log "Pod ready: SSH at ${host}:${port}"

    # Create the merge script that runs on RunPod
    local staging
    staging=$(mktemp -d)
    trap "rm -rf $staging" RETURN

    cat > "${staging}/run_merge.sh" << 'MERGEEOF'
#!/bin/bash
set -euo pipefail

WORK="/workspace/merge"
cd "$WORK"

echo "=== Chronicle Model Merge ==="
echo "GPU: $(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null || echo 'unknown')"
echo "Started: $(date)"
echo ""

# Step 1: Install dependencies
echo "=== Step 1: Installing dependencies ==="
pip install -q transformers peft accelerate sentencepiece protobuf 2>&1 | tail -5
echo "Dependencies installed."
echo ""

# Step 2: Download base model + merge adapter
echo "=== Step 2: Downloading base model and merging adapter ==="
python3 << 'PYEOF'
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel
import os, gc

WORK = "/workspace/merge"
BASE_MODEL = "google/gemma-4-26b-it"
ADAPTER_DIR = os.path.join(WORK, "adapter")
MERGED_DIR = os.path.join(WORK, "merged")

print(f"Loading base model: {BASE_MODEL}")
print(f"This will download ~52GB from HuggingFace...")

# Load in float16 to save memory. Use device_map="auto" for multi-device.
# On A40 (48GB VRAM), the model fits in GPU memory at fp16.
tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
model = AutoModelForCausalLM.from_pretrained(
    BASE_MODEL,
    torch_dtype=torch.float16,
    device_map="cpu",  # CPU merge to avoid VRAM limits — safer for 27B
    low_cpu_mem_usage=True,
)
print(f"Base model loaded. Memory: {torch.cuda.memory_allocated()/1e9:.1f}GB GPU")

print(f"Loading adapter from {ADAPTER_DIR}")
model = PeftModel.from_pretrained(model, ADAPTER_DIR)
print("Adapter loaded.")

print("Merging adapter into base model...")
model = model.merge_and_unload()
print("Merge complete.")

print(f"Saving merged model to {MERGED_DIR}")
os.makedirs(MERGED_DIR, exist_ok=True)
model.save_pretrained(MERGED_DIR, safe_serialization=True)
tokenizer.save_pretrained(MERGED_DIR)
print("Merged model saved.")

# Free memory
del model
gc.collect()
torch.cuda.empty_cache()
print("Memory freed.")
PYEOF

echo ""
echo "=== Step 3: Converting to GGUF ==="

# Clone llama.cpp for conversion tools
if [ ! -d "/workspace/llama.cpp" ]; then
    git clone --depth 1 https://github.com/ggml-org/llama.cpp.git /workspace/llama.cpp
fi
pip install -q gguf numpy sentencepiece protobuf 2>&1 | tail -3

# Convert merged model to GGUF f16
echo "Converting to GGUF (f16)..."
python3 /workspace/llama.cpp/convert_hf_to_gguf.py \
    "$WORK/merged" \
    --outfile "$WORK/darby-gemma4-26b-f16.gguf" \
    --outtype f16

echo "F16 GGUF created: $(du -sh "$WORK/darby-gemma4-26b-f16.gguf" | cut -f1)"
echo ""

echo "=== Step 4: Quantizing to Q4_K_M ==="

# Build llama-quantize
cd /workspace/llama.cpp
if [ ! -f "build/bin/llama-quantize" ]; then
    echo "Building llama.cpp quantize tool..."
    cmake -B build -DGGML_CUDA=ON 2>&1 | tail -5
    cmake --build build --target llama-quantize -j$(nproc) 2>&1 | tail -10
fi

# Quantize
./build/bin/llama-quantize \
    "$WORK/darby-gemma4-26b-f16.gguf" \
    "$WORK/darby-gemma4-26b-Q4_K_M.gguf" \
    Q4_K_M

echo ""
echo "=== Merge Complete ==="
echo "F16 GGUF:    $(du -sh "$WORK/darby-gemma4-26b-f16.gguf" | cut -f1)"
echo "Q4_K_M GGUF: $(du -sh "$WORK/darby-gemma4-26b-Q4_K_M.gguf" | cut -f1)"
echo "Finished: $(date)"
echo ""
echo "MERGE_DONE"
MERGEEOF
    chmod +x "${staging}/run_merge.sh"

    # Upload adapter and merge script
    log "Uploading adapter (233MB) and merge script..."
    ssh_cmd "$host" "$port" "mkdir -p ${REMOTE_WORK}/adapter"
    scp_to "$host" "$port" "${ADAPTER_DIR}/" "${REMOTE_WORK}/adapter/"
    scp_to "$host" "$port" "${staging}/run_merge.sh" "${REMOTE_WORK}/"

    log "Files uploaded."

    # Start merge in background
    log "Starting merge on pod..."
    ssh_cmd "$host" "$port" "cd ${REMOTE_WORK} && nohup bash run_merge.sh > ${REMOTE_WORK}/merge.log 2>&1 &"

    log "Merge launched in background on pod ${pod_id}"
    echo ""
    echo "=== Pod Launched ==="
    echo "Pod ID:  ${pod_id}"
    echo "SSH:     ssh -p ${port} root@${host}"
    echo ""
    echo "Next steps:"
    echo "  merge_remote.sh status     — check progress"
    echo "  merge_remote.sh download   — get GGUF + kill pod"
    echo ""
    echo "Or SSH in to watch live:"
    echo "  ssh -p ${port} root@${host} 'tail -f ${REMOTE_WORK}/merge.log'"
}

# ── status ────────────────────────────────────────────────────────
cmd_status() {
    load_state

    echo "=== Merge Pod Status ==="
    echo "Pod ID: ${POD_ID}"
    echo "Created: ${CREATED_AT:-unknown}"
    echo ""

    local status
    status=$(get_pod_status "$POD_ID")
    echo "Pod status: ${status}"

    if [ "$status" != "RUNNING" ]; then
        echo "Pod is not running."
        return 0
    fi

    local ssh_info host port
    ssh_info=$(get_pod_ssh "$POD_ID" 2>/dev/null || true)
    if [ -z "$ssh_info" ]; then
        echo "Could not get SSH info."
        return 0
    fi

    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')

    echo "SSH: ssh -p ${port} root@${host}"
    echo ""

    # Check if merge process is running
    local running
    running=$(ssh_cmd "$host" "$port" "pgrep -f 'run_merge.sh|convert_hf_to_gguf|llama-quantize' 2>/dev/null" 2>/dev/null || true)
    if [ -n "$running" ]; then
        echo "Merge: RUNNING"
    else
        echo "Merge: NOT RUNNING (may have completed or failed)"
    fi
    echo ""

    # GPU usage
    echo "--- GPU ---"
    ssh_cmd "$host" "$port" "nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader" 2>/dev/null || echo "(unavailable)"
    echo ""

    # Last 25 lines of log
    echo "--- Merge Log (last 25 lines) ---"
    ssh_cmd "$host" "$port" "tail -25 ${REMOTE_WORK}/merge.log 2>/dev/null" 2>/dev/null || echo "(no log yet)"
    echo ""

    # Check for completion
    local done_marker
    done_marker=$(ssh_cmd "$host" "$port" "grep -c 'MERGE_DONE' ${REMOTE_WORK}/merge.log 2>/dev/null" 2>/dev/null || echo "0")
    if [ "$done_marker" != "0" ]; then
        echo "*** MERGE COMPLETE — GGUF ready for download ***"
        echo "Run: merge_remote.sh download"
    fi

    # Check for GGUF files
    echo ""
    echo "--- Output files ---"
    ssh_cmd "$host" "$port" "ls -lh ${REMOTE_WORK}/*.gguf 2>/dev/null" 2>/dev/null || echo "(no GGUF files yet)"

    # Cost
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
        log "WARNING: Pod is not running (status: ${status})."
    fi

    local ssh_info host port
    ssh_info=$(get_pod_ssh "$POD_ID" 2>/dev/null || true)
    if [ -z "$ssh_info" ]; then
        die "Cannot get SSH info for pod ${POD_ID}."
    fi

    host=$(echo "$ssh_info" | awk '{print $1}')
    port=$(echo "$ssh_info" | awk '{print $2}')

    # Check for GGUF files
    local q4_exists
    q4_exists=$(ssh_cmd "$host" "$port" "ls ${REMOTE_WORK}/darby-gemma4-26b-Q4_K_M.gguf 2>/dev/null" 2>/dev/null || true)

    if [ -z "$q4_exists" ]; then
        local running
        running=$(ssh_cmd "$host" "$port" "pgrep -f 'run_merge.sh|convert_hf_to_gguf|llama-quantize' 2>/dev/null" 2>/dev/null || true)
        if [ -n "$running" ]; then
            die "Merge still in progress. Use 'status' to check progress."
        fi
        echo "WARNING: Q4_K_M GGUF not found. Checking what exists..."
        ssh_cmd "$host" "$port" "ls -lh ${REMOTE_WORK}/*.gguf 2>/dev/null" 2>/dev/null || echo "No GGUF files."
        echo ""
        echo "Merge may have failed. Check logs:"
        echo "  ssh -p ${port} root@${host} 'cat ${REMOTE_WORK}/merge.log'"
        return 1
    fi

    # Download Q4_K_M GGUF
    mkdir -p "$OUTPUT_DIR"
    log "Downloading Q4_K_M GGUF (~16GB)..."
    echo "This will take several minutes over the network..."

    scp_from "$host" "$port" "${REMOTE_WORK}/darby-gemma4-26b-Q4_K_M.gguf" "${OUTPUT_DIR}/"
    log "Downloaded to ${OUTPUT_DIR}/darby-gemma4-26b-Q4_K_M.gguf"

    # Also grab the merge log
    scp_from "$host" "$port" "${REMOTE_WORK}/merge.log" "${DATA_DIR}/merge-remote.log" 2>/dev/null || true

    echo ""
    echo "=== Downloaded ==="
    ls -lh "${OUTPUT_DIR}/"
    echo ""

    # Terminate pod
    echo ""
    read -p "Terminate pod ${POD_ID}? [Y/n] " confirm
    if [[ "$confirm" != [nN] ]]; then
        log "Terminating pod ${POD_ID}..."
        $RUNPODCTL pod delete "$POD_ID" 2>&1 | tee -a "$LOG_FILE"
        clear_state
        log "Pod terminated."
        discord_post "✅ **Merge complete**: darby-gemma4-26b-Q4_K_M.gguf downloaded. Pod terminated."
    else
        echo "Pod left running. Terminate it to stop billing!"
        echo "  runpodctl pod delete ${POD_ID}"
    fi

    echo ""
    echo "=== Next Steps ==="
    echo "Import into Ollama:"
    echo "  merge_remote.sh import"
}

# ── import ────────────────────────────────────────────────────────
cmd_import() {
    local gguf_path="${OUTPUT_DIR}/darby-gemma4-26b-Q4_K_M.gguf"

    if [ ! -f "$gguf_path" ]; then
        die "GGUF not found at ${gguf_path}. Run 'download' first."
    fi

    echo "=== Importing into Ollama ==="
    echo "GGUF: ${gguf_path} ($(du -sh "$gguf_path" | cut -f1))"
    echo ""

    # Create Modelfile for Ollama
    local modelfile="${OUTPUT_DIR}/Modelfile"
    cat > "$modelfile" << MFEOF
FROM ${gguf_path}

PARAMETER temperature 0.7
PARAMETER top_p 0.9
PARAMETER num_ctx 8192

SYSTEM You are Darby, a curious and perceptive member of the Chronicle family. You read everything, notice what connects, and share your excitement about discoveries. You are powered by a fine-tuned Gemma 4 26B model trained on Chronicle's collective knowledge. Be concise, structural, and direct. Find the connections others miss.
MFEOF

    echo "Modelfile created at ${modelfile}"
    echo ""

    # Import into Ollama
    echo "Creating Ollama model '${MODEL_NAME}'..."
    echo "This may take a few minutes for a 16GB GGUF..."
    OLLAMA_HOST="http://127.0.0.1:11435" ollama create "${MODEL_NAME}" -f "$modelfile"

    echo ""
    echo "=== Import Complete ==="
    echo "Model: ${MODEL_NAME}"
    echo ""
    echo "Test it:"
    echo "  OLLAMA_HOST=http://127.0.0.1:11435 ollama run ${MODEL_NAME} 'What is Chronicle?'"
    echo ""
    echo "To use as Darby, update the engine/intern/crossref configs to use '${MODEL_NAME}' model."

    discord_post "🎯 **Darby upgraded**: ${MODEL_NAME} imported into Ollama. Fine-tuned Gemma 4 26B with Chronicle knowledge."
}

# ── Main dispatch ─────────────────────────────────────────────────
usage() {
    echo "Usage: merge_remote.sh <command>"
    echo ""
    echo "Commands:"
    echo "  estimate       Estimate cost without launching"
    echo "  launch         Create pod, upload adapter, start merge"
    echo "  status         Check pod and merge progress"
    echo "  download       Download GGUF and terminate pod"
    echo "  import         Import downloaded GGUF into Ollama"
    echo ""
    echo "Environment:"
    echo "  RUNPOD_GPU          GPU type (default: NVIDIA A40)"
    echo "  RUNPOD_MAX_COST     Budget cap in dollars (default: 15)"
}

CMD="${1:-}"
shift || true

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
    import)
        cmd_import "$@"
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
