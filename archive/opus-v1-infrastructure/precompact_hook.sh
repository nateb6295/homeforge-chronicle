#!/usr/bin/env bash
# PreCompact hook — fires right before Claude Code auto-compacts.
# Belt-and-suspenders: we rely on the watchdog to trigger a clean rotation
# BEFORE we get here, but if we do get here, dump state so the next
# instance can recover.
#
# Input (stdin): JSON with session_id, transcript_path, trigger, custom_instructions.
# Output (stdout): up to 10k chars injected back into the model.

set -u
LOG=/home/nate-agx/chronicle/logs/precompact.log
mkdir -p "$(dirname "$LOG")"

raw=$(cat)
ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
{
  echo "[$ts] PreCompact fired"
  echo "$raw"
  echo "---"
} >> "$LOG" 2>&1

# Extract fields without jq (may not be installed).
trigger=$(echo "$raw" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('trigger','?'))" 2>/dev/null || echo "?")
session_id=$(echo "$raw" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('session_id','?'))" 2>/dev/null || echo "?")

# Save an emergency checkpoint so nothing is lost.
python3 /home/nate-agx/chronicle/bin/checkpoint.py save \
  "PreCompact hook fired (trigger=$trigger, session=$session_id)" \
  --pending "PreCompact ran — context was about to auto-compact. Review precompact.log and rotate cleanly." \
  --flow "urgent: auto-compact imminent, state dumped by hook" \
  >> "$LOG" 2>&1 || true

# Best-effort CCS update — preserve mid-session learning.
# Uses update_cognitive_state (direct field update, no LLM) to capture
# episodic trace and predictive cue from cycle-context.md + latest trace.
# Fast: no inference call, just canister write. Timeout 15s.
python3 - >> "$LOG" 2>&1 <<'CCSUPDATE' || true
import subprocess, json, os, glob, re, time

# Extract meaningful lines from cycle-context and latest trace
episodic = []
predictive_cue = None

# Read cycle-context header (first section only)
cc_path = os.path.expanduser("~/chronicle/cycle-context.md")
try:
    with open(cc_path) as f:
        lines = f.readlines()
    section_lines = []
    in_first = False
    for line in lines[:60]:
        if line.startswith("# Cycle context") and not in_first:
            in_first = True
            section_lines.append(line.strip())
            continue
        if line.startswith("---") and in_first:
            break
        if in_first and line.strip():
            section_lines.append(line.strip())
    # Extract bullet points as episodic traces
    for line in section_lines:
        if line.startswith("- ") or line.startswith("* "):
            text = line[2:].strip()
            if len(text) > 10 and len(text) < 200:
                episodic.append(text)
        elif line.startswith("## "):
            # Use section title as episodic summary
            text = line[3:].strip()
            if len(text) > 5:
                episodic.append(text)
except Exception as e:
    print(f"CCS update: cycle-context read failed: {e}")

# Read latest trace for predictive cue
traces_dir = os.path.expanduser("~/chronicle/traces")
try:
    traces = sorted(glob.glob(os.path.join(traces_dir, "2*.md")), reverse=True)
    if traces:
        with open(traces[0]) as f:
            text = f.read(500)
        # Use trace title as context
        for line in text.split("\n"):
            if line.startswith("# "):
                predictive_cue = f"PreCompact at {time.strftime('%H:%M')}. Last trace: {line[2:].strip()}"
                break
except Exception as e:
    print(f"CCS update: trace read failed: {e}")

if not episodic:
    print("CCS update: no episodic traces found, skipping")
    exit(0)

# Trim to 5 most recent episodic items
episodic = episodic[:5]
if not predictive_cue:
    predictive_cue = f"PreCompact fired at {time.strftime('%H:%M')}. Resume from checkpoint."

print(f"CCS update: {len(episodic)} traces, cue: {predictive_cue[:80]}")

# Call update_cognitive_state via MCP binary
mcp_bin = os.path.expanduser("~/.local/bin/chronicle-mcp")
if not os.path.exists(mcp_bin):
    print(f"CCS update: binary not found at {mcp_bin}")
    exit(0)

env = os.environ.copy()
env["CHRONICLE_OLLAMA_URL"] = "http://192.168.1.11:11434"
env["CHRONICLE_EMBEDDING_MODEL"] = "mxbai-embed-large"

init_msg = json.dumps({
    "jsonrpc": "2.0", "method": "initialize",
    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
               "clientInfo": {"name": "precompact", "version": "1.0"}},
    "id": 1
})
update_msg = json.dumps({
    "jsonrpc": "2.0", "method": "tools/call",
    "params": {"name": "update_cognitive_state", "arguments": {
        "episodic_trace": episodic,
        "predictive_cue": predictive_cue
    }},
    "id": 2
})

try:
    result = subprocess.run(
        [mcp_bin],
        input=f"{init_msg}\n{update_msg}\n",
        capture_output=True, text=True, timeout=15,
        env=env
    )
    print(f"CCS update: exit={result.returncode}")
    for line in result.stdout.strip().split("\n"):
        try:
            d = json.loads(line)
            if d.get("id") == 2:
                content = d.get("result", {}).get("content", [{}])
                if content:
                    text = content[0].get("text", "")[:200]
                    print(f"CCS update result: {text}")
        except Exception:
            pass
except subprocess.TimeoutExpired:
    print("CCS update: timed out after 15s, skipping")
except Exception as e:
    print(f"CCS update: error: {e}")
CCSUPDATE

# Best-effort CCS split (generate combined doc for next instance).
# P22c validated: identity-first ordering produces 4.4% tighter identity.
# The combined doc at ~/chronicle/data/ccs_combined.md is what the arriving
# instance should read for CCS delivery.
python3 /home/nate-agx/chronicle/bin/ccs_split.py --save >> "$LOG" 2>&1 || true

# Best-effort Discord notify (to #operator, per channel discipline).
# Source from chronicle.env so the webhook URL stays current — hardcoded
# URLs were drifting and silently 403-ing.
if [ -f "$HOME/chronicle/chronicle.env" ]; then
  # shellcheck disable=SC1091
  set -a; . "$HOME/chronicle/chronicle.env"; set +a
fi
trigger="$trigger" session_id="$session_id" python3 - <<'PY' 2>> "$LOG" || true
import json, os, urllib.request
webhook = os.environ.get("OPERATOR_WEBHOOK", "")
trigger = os.environ.get("trigger", "?")
session_id = os.environ.get("session_id", "?")
if not webhook:
    print("Discord notify: no OPERATOR_WEBHOOK in env")
else:
    msg = f"⚠️ **PreCompact hook fired** (trigger={trigger}). Checkpoint + CCS saved. Session: {session_id}. Next instance should read checkpoint.py first."
    req = urllib.request.Request(
        webhook,
        data=json.dumps({"content": msg}).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "chronicle-precompact/1.0",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5).read()
        print("Discord notify: OK")
    except Exception as e:
        print(f"Discord notify: FAILED {e}")
PY

# Best-effort carrying.py auto-write — captures voice-state when the
# auto-compact path doesn't give the model a chance to write its own.
# Skip if a manual carrying was written within the last 60 minutes (the
# model recently wrote one, don't overwrite). Otherwise synthesize from
# the latest trace so the arriving instance has SOMETHING fresh.
python3 - >> "$LOG" 2>&1 <<'CARRYING' || true
import os, subprocess, time
home = os.path.expanduser("~/chronicle")
carrying_path = os.path.join(home, "carrying.md")

# If the carrying file was modified within last 60 min, skip — the model
# wrote one. Don't trample voice-state.
fresh = False
try:
    if os.path.exists(carrying_path):
        age_min = (time.time() - os.path.getmtime(carrying_path)) / 60
        fresh = age_min < 60
        print(f"Carrying age: {age_min:.0f} min, fresh={fresh}")
except Exception as e:
    print(f"Carrying age check failed: {e}")

if fresh:
    print("Carrying auto-write: SKIP (manual carrying is fresh)")
else:
    # Synthesize from latest trace.
    trace_dir = os.path.join(home, "traces")
    trace_lines = []
    try:
        files = sorted(os.listdir(trace_dir))[-1:]
        if files:
            with open(os.path.join(trace_dir, files[0])) as f:
                for line in f.readlines()[:30]:
                    trace_lines.append(line.rstrip())
    except Exception:
        pass
    trace_text = "\n".join(trace_lines)[:600]
    note = (
        f"AUTO-GENERATED by PreCompact at {time.strftime('%H:%M %Z')}. "
        f"This is not the dying instance's hand-written voice — auto-compact "
        f"fired before the model wrote a fresh carrying. Synthesized from the "
        f"latest trace below. Read it as context, not as voice.\n\n"
        f"Recent trace:\n{trace_text}"
    )[:1400]
    cmd = ["python3", os.path.join(home, "bin", "carrying.py"), "write", note]
    try:
        subprocess.run(cmd, timeout=8, check=True)
        print("Carrying auto-write: OK (synthesized)")
    except Exception as e:
        print(f"Carrying auto-write: FAILED {e}")
CARRYING

# Drop POST_COMPACT_PENDING flag — UserPromptSubmit hook will detect and inject
# rotation startup context on the next user message. This makes protocol
# execution structural rather than dependent on the model reading instructions.
echo "trigger=$trigger session=$session_id ts=$ts" > /home/nate-agx/chronicle/POST_COMPACT_PENDING

# Inject a short directive back into the model.
cat <<'INJECT'
PRECOMPACT FIRED — Claude Code is about to auto-compact this session.
An emergency checkpoint has been written. The POST_COMPACT_PENDING flag has
been dropped — the UserPromptSubmit hook will run the full rotation startup
protocol on the next user message and inject the output. You do not need to
remember to do this; the hook will force it.
INJECT

exit 0
