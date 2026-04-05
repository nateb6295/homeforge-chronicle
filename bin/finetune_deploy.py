#!/usr/bin/env python3
# Workaround: Jetson lacks torch distributed C10D module
import sys as _sys, types as _types
if 'torch._C._distributed_c10d' not in _sys.modules:
    _fake = _types.ModuleType('torch._C._distributed_c10d')
    for _n in ['FakeProcessGroup','ProcessGroup','Work','Store','ReduceOp','BroadcastOptions','AllreduceOptions']:
        setattr(_fake, _n, type(_n, (), {}))
    _sys.modules['torch._C._distributed_c10d'] = _fake
"""
Chronicle Fine-Tune Deployment — Objective #7

Takes a trained LoRA adapter, merges it with the base model, and deploys
to Ollama as 'chronicle-specialist'. Then verifies it works.

Steps:
  1. merge   — Merge LoRA adapter into base model (safetensors)
  2. ollama  — Create Ollama model from merged safetensors (Q4_K_M)
  3. verify  — Test the deployed model with crossref-style prompts
  4. full    — All steps in sequence
  5. status  — Check what's ready for deployment

Requires ~8GB GPU memory for merge. Gemma must be unloaded first.
After deployment, total Ollama size: ~2GB for chronicle-specialist (Q4_K_M 3B).

Usage:
  python3 finetune_deploy.py status
  python3 finetune_deploy.py full
  python3 finetune_deploy.py verify
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

os.environ["HF_HOME"] = "/mnt/hdd/huggingface"

ADAPTER_DIR = Path("/mnt/hdd/chronicle-finetune/adapters/chronicle-specialist")
MERGED_DIR = Path("/mnt/hdd/chronicle-finetune/merged")
MODELFILE_PATH = Path("/mnt/hdd/chronicle-finetune/Modelfile")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11435")
OLLAMA_MODEL_NAME = "chronicle-specialist"
BASE_MODEL = "Qwen/Qwen2.5-3B-Instruct"

# Test prompts — crossref-style connection tasks
TEST_PROMPTS = [
    {
        "system": "You are a Chronicle crossref analyst. Find unexpected structural connections between concepts.",
        "user": "Connect these two observations: 'Self-assembling robot swarms use magnetic connectors for zero-waste repair' and 'Prediction markets lose information integrity when entertainment value rises'. What structural pattern do they share?"
    },
    {
        "system": "You are a Chronicle crossref analyst. Find unexpected structural connections between concepts.",
        "user": "What does fog computing's edge-node isolation architecture have in common with cryptocurrency-backed mortgage dual-loan structures?"
    },
    {
        "system": "You are a Chronicle crossref analyst. Find unexpected structural connections between concepts.",
        "user": "Connect: 'Sovereignty is reconstitution capacity plus classification authority' and 'Fine-tuning narrows output diversity while deepening domain performance'. What's the shared tension?"
    },
]


def ollama_api(endpoint, data=None, method="GET"):
    """Call Ollama API."""
    url = f"{OLLAMA_HOST}/{endpoint}"
    if data:
        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    else:
        req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def status():
    """Check deployment readiness."""
    print("=== Chronicle Specialist Deployment Status ===\n")

    # Check adapter
    adapter_exists = ADAPTER_DIR.exists() and (ADAPTER_DIR / "adapter_model.safetensors").exists()
    print(f"  Adapter:  {'READY' if adapter_exists else 'NOT FOUND'} ({ADAPTER_DIR})")

    # Check if training is still running
    training_pid = None
    try:
        result = subprocess.run(
            ["pgrep", "-f", "finetune_train.py train"],
            capture_output=True, text=True
        )
        if result.stdout.strip():
            training_pid = result.stdout.strip().split()[0]
    except Exception:
        pass
    print(f"  Training: {'RUNNING (PID {})'.format(training_pid) if training_pid else 'not running'}")

    # Check merged model
    merged_exists = MERGED_DIR.exists() and any(MERGED_DIR.glob("*.safetensors"))
    print(f"  Merged:   {'READY' if merged_exists else 'not yet'} ({MERGED_DIR})")

    # Check eval results
    eval_path = Path("/mnt/hdd/chronicle-finetune/eval_results.json")
    if eval_path.exists():
        try:
            results = json.loads(eval_path.read_text())
            verdict = results.get("autoscore", {}).get("verdict", "unknown")
            print(f"  Eval:     {verdict}")
        except Exception:
            print(f"  Eval:     results file exists but couldn't parse")
    else:
        print(f"  Eval:     not yet run")

    # Check Ollama
    models = ollama_api("api/tags")
    if "error" in models:
        print(f"  Ollama:   NOT REACHABLE ({models['error'][:60]})")
    else:
        names = [m["name"] for m in models.get("models", [])]
        if f"{OLLAMA_MODEL_NAME}:latest" in names or OLLAMA_MODEL_NAME in names:
            print(f"  Ollama:   DEPLOYED as {OLLAMA_MODEL_NAME}")
        else:
            print(f"  Ollama:   not yet created (available: {', '.join(names)})")

    # Check Gemma status (need it stopped for merge)
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "chronicle-gemma"],
            capture_output=True, text=True
        )
        gemma_active = result.stdout.strip() == "active"
    except Exception:
        gemma_active = False
    print(f"  Gemma:    {'ACTIVE (must stop for merge)' if gemma_active else 'stopped'}")

    print()
    if training_pid:
        print("  -> Training still running. Wait for completion.")
    elif not adapter_exists:
        print("  -> No adapter found. Training may have failed.")
    elif not merged_exists:
        print("  -> Ready to merge. Run: python3 finetune_deploy.py merge")
    else:
        print("  -> Ready for Ollama deployment. Run: python3 finetune_deploy.py ollama")


def merge():
    """Merge LoRA adapter with base model."""
    if not ADAPTER_DIR.exists():
        print(f"Error: adapter not found at {ADAPTER_DIR}")
        sys.exit(1)

    # Check GPU memory — need Gemma unloaded
    print("Checking GPU memory...")
    try:
        # Unload gemma from Ollama if loaded
        ollama_api("api/generate", {"model": "gemma4:26b", "keep_alive": "0"})
        time.sleep(2)
    except Exception:
        pass

    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from peft import PeftModel

    # Read base model from adapter config
    config_path = ADAPTER_DIR / "adapter_config.json"
    with open(config_path) as f:
        config = json.load(f)
    base_model = config.get("base_model_name_or_path", BASE_MODEL)

    print(f"Merging adapter into base model...")
    print(f"  Base:    {base_model}")
    print(f"  Adapter: {ADAPTER_DIR}")
    print(f"  Output:  {MERGED_DIR}")

    # Load base model in bf16
    print("\nLoading base model...")
    tokenizer = AutoTokenizer.from_pretrained(str(ADAPTER_DIR), trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        torch_dtype=torch.bfloat16,
        device_map="cpu",  # Merge on CPU to avoid GPU contention
        trust_remote_code=True,
    )

    # Load and merge adapter
    print("Loading adapter...")
    model = PeftModel.from_pretrained(model, str(ADAPTER_DIR))
    print("Merging weights...")
    model = model.merge_and_unload()

    # Save merged model
    MERGED_DIR.mkdir(parents=True, exist_ok=True)
    print("Saving merged model...")
    model.save_pretrained(str(MERGED_DIR))
    tokenizer.save_pretrained(str(MERGED_DIR))

    # Check size
    total_size = sum(f.stat().st_size for f in MERGED_DIR.rglob("*") if f.is_file())
    print(f"\nMerged model saved: {total_size / 1e9:.1f} GB")
    print(f"  Path: {MERGED_DIR}")

    # Clean up GPU memory
    del model
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


def create_ollama():
    """Create Ollama model from merged safetensors."""
    if not MERGED_DIR.exists() or not any(MERGED_DIR.glob("*.safetensors")):
        print(f"Error: merged model not found at {MERGED_DIR}")
        print("Run 'python3 finetune_deploy.py merge' first.")
        sys.exit(1)

    # Write Modelfile
    modelfile_content = f"""FROM {MERGED_DIR}

TEMPLATE \"\"\"{{{{- if .System }}}}<|im_start|>system
{{{{ .System }}}}<|im_end|>
{{{{ end }}}}<|im_start|>user
{{{{ .Prompt }}}}<|im_end|>
<|im_start|>assistant
{{{{ .Response }}}}<|im_end|>\"\"\"

PARAMETER temperature 0.7
PARAMETER top_p 0.9
PARAMETER num_predict 512
PARAMETER stop "<|im_end|>"
PARAMETER stop "<|im_start|>"

SYSTEM \"\"\"You are Chronicle Specialist, a fine-tuned model trained on Chronicle's knowledge graph — crossref connections, thread reasoning, and synthesis patterns. You find structural connections between disparate concepts.\"\"\"
"""
    MODELFILE_PATH.write_text(modelfile_content)
    print(f"Modelfile written to {MODELFILE_PATH}")

    # Create model via ollama CLI
    print(f"\nCreating Ollama model '{OLLAMA_MODEL_NAME}' (Q4_K_M quantization)...")
    print("This may take several minutes for conversion + quantization.\n")

    env = os.environ.copy()
    env["OLLAMA_HOST"] = OLLAMA_HOST.replace("http://", "")

    result = subprocess.run(
        [
            "ollama", "create", OLLAMA_MODEL_NAME,
            "--experimental",
            "-q", "q4_K_M",
            "-f", str(MODELFILE_PATH),
        ],
        env=env,
        capture_output=False,  # Show progress
        timeout=600,
    )

    if result.returncode != 0:
        print(f"\nOllama create failed (exit {result.returncode})")
        print("Try running manually:")
        print(f"  OLLAMA_HOST={OLLAMA_HOST.replace('http://', '')} ollama create {OLLAMA_MODEL_NAME} --experimental -q q4_K_M -f {MODELFILE_PATH}")
        sys.exit(1)

    # Verify model appears in list
    time.sleep(2)
    models = ollama_api("api/tags")
    names = [m["name"] for m in models.get("models", [])]
    if any(OLLAMA_MODEL_NAME in n for n in names):
        print(f"\nModel '{OLLAMA_MODEL_NAME}' created successfully!")
        for m in models["models"]:
            if OLLAMA_MODEL_NAME in m["name"]:
                size_gb = m["size"] / 1e9
                print(f"  Size: {size_gb:.1f} GB")
                print(f"  Quantization: {m['details']['quantization_level']}")
    else:
        print(f"\nWarning: model not found in Ollama list after creation")
        print(f"Available: {names}")


def verify():
    """Test the deployed model with crossref-style prompts."""
    print(f"=== Verifying {OLLAMA_MODEL_NAME} ===\n")

    # Check model exists
    models = ollama_api("api/tags")
    if "error" in models:
        print(f"Error: Ollama not reachable: {models['error']}")
        sys.exit(1)

    names = [m["name"] for m in models.get("models", [])]
    if not any(OLLAMA_MODEL_NAME in n for n in names):
        print(f"Error: {OLLAMA_MODEL_NAME} not found in Ollama")
        print(f"Available: {names}")
        sys.exit(1)

    results = []
    for i, prompt in enumerate(TEST_PROMPTS, 1):
        print(f"--- Test {i}/3 ---")
        print(f"Prompt: {prompt['user'][:100]}...")

        start = time.time()
        resp = ollama_api("api/chat", {
            "model": OLLAMA_MODEL_NAME,
            "messages": [
                {"role": "system", "content": prompt["system"]},
                {"role": "user", "content": prompt["user"]},
            ],
            "stream": False,
            "options": {"temperature": 0.7, "num_predict": 300},
        })
        elapsed = time.time() - start

        if "error" in resp:
            print(f"Error: {resp['error']}")
            results.append({"test": i, "error": resp["error"]})
            continue

        response_text = resp.get("message", {}).get("content", "")
        tokens = resp.get("eval_count", 0)
        tok_per_sec = tokens / elapsed if elapsed > 0 else 0

        print(f"Response ({tokens} tokens, {tok_per_sec:.1f} tok/s, {elapsed:.1f}s):")
        print(f"  {response_text[:300]}")
        print()

        results.append({
            "test": i,
            "tokens": tokens,
            "tok_per_sec": round(tok_per_sec, 1),
            "elapsed": round(elapsed, 1),
            "response_preview": response_text[:500],
        })

    # Summary
    print("=== Verification Summary ===")
    successes = [r for r in results if "error" not in r]
    print(f"  Passed: {len(successes)}/3")
    if successes:
        avg_tps = sum(r["tok_per_sec"] for r in successes) / len(successes)
        print(f"  Avg speed: {avg_tps:.1f} tok/s")
    print(f"  Model: {OLLAMA_MODEL_NAME}")
    print(f"  Ollama: {OLLAMA_HOST}")

    # Save verification results
    verify_path = Path("/mnt/hdd/chronicle-finetune/deploy_verification.json")
    verify_path.write_text(json.dumps({
        "timestamp": int(time.time()),
        "model": OLLAMA_MODEL_NAME,
        "results": results,
        "passed": len(successes),
        "total": 3,
    }, indent=2))
    print(f"\n  Results saved: {verify_path}")


def full():
    """Full deployment pipeline: merge → ollama → verify."""
    print("=" * 60)
    print("  Chronicle Specialist — Full Deployment Pipeline")
    print("=" * 60)

    # Pre-checks
    training_running = bool(subprocess.run(
        ["pgrep", "-f", "finetune_train.py train"],
        capture_output=True
    ).stdout.strip())

    if training_running:
        print("\nError: Training is still running. Wait for completion.")
        sys.exit(1)

    if not ADAPTER_DIR.exists():
        print(f"\nError: No adapter at {ADAPTER_DIR}")
        sys.exit(1)

    # Check eval verdict
    eval_path = Path("/mnt/hdd/chronicle-finetune/eval_results.json")
    if eval_path.exists():
        try:
            results = json.loads(eval_path.read_text())
            verdict = results.get("autoscore", {}).get("verdict", "unknown")
            print(f"\nEval verdict: {verdict}")
            if verdict == "FAIL":
                print("Warning: Eval scored FAIL. Deploying anyway for comparison.")
        except Exception:
            pass

    # Step 1: Stop Gemma to free GPU memory
    print("\n[1/4] Stopping Gemma to free GPU memory...")
    subprocess.run(["systemctl", "--user", "stop", "chronicle-gemma"], capture_output=True)
    ollama_api("api/generate", {"model": "gemma4:26b", "keep_alive": "0"})
    time.sleep(3)

    # Step 2: Merge
    print("\n[2/4] Merging adapter into base model...")
    merge()

    # Step 3: Create Ollama model
    print("\n[3/4] Creating Ollama model...")
    create_ollama()

    # Step 4: Verify
    print("\n[4/4] Verifying deployment...")
    verify()

    # Restart Gemma
    print("\nRestarting Gemma gate service...")
    subprocess.run(["systemctl", "--user", "start", "chronicle-gemma"], capture_output=True)

    print("\n" + "=" * 60)
    print(f"  Deployment complete: {OLLAMA_MODEL_NAME}")
    print(f"  Test: OLLAMA_HOST={OLLAMA_HOST.replace('http://','')} ollama run {OLLAMA_MODEL_NAME}")
    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "status":
        status()
    elif cmd == "merge":
        merge()
    elif cmd == "ollama":
        create_ollama()
    elif cmd == "verify":
        verify()
    elif cmd == "full":
        full()
    else:
        print(f"Unknown command: {cmd}")
        print("Commands: status, merge, ollama, verify, full")
        sys.exit(1)


if __name__ == "__main__":
    main()
