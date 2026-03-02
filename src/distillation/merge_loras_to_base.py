#!/usr/bin/env python3
"""
Evolutionary Model Forge — Merge LoRA Adapters into Full Models.

For each of the 3 LoRA adapters (capsule, action, domain):
  1. Load base Hermes 3 8B in fp16
  2. Load PEFT adapter
  3. merge_and_unload() → full model
  4. Save as fp16 safetensors to /mnt/hdd/forge/merged/{name}-full/

Each merged model is ~16GB. Total: ~48GB on HDD.

Runs on AGX Orin 64GB.

Usage:
    python3 /tmp/merge_loras_to_base.py              # merge all 3
    python3 /tmp/merge_loras_to_base.py --only capsule  # merge just one
    python3 /tmp/merge_loras_to_base.py --only action domain  # merge two
"""

import argparse
import os
import sys
import time

# Ensure cuSPARSELt is findable
_cusparselt = os.path.expanduser("~/.local/lib/python3.10/site-packages/nvidia/cusparselt/lib")
if os.path.isdir(_cusparselt):
    os.environ.setdefault("LD_LIBRARY_PATH", "")
    if _cusparselt not in os.environ["LD_LIBRARY_PATH"]:
        os.environ["LD_LIBRARY_PATH"] = _cusparselt + ":" + os.environ["LD_LIBRARY_PATH"]

if os.path.isdir("/mnt/ssd/hf-cache"):
    os.environ.setdefault("HF_HOME", "/mnt/ssd/hf-cache")


# Adapter definitions: name → (adapter_dir, output_dir)
ADAPTERS = {
    "capsule": {
        "adapter_dir": os.path.expanduser("~/mind-lora-adapter"),
        "output_dir": "/mnt/hdd/forge/merged/capsule-full",
        "description": "Capsule memories (personalization)",
    },
    "action": {
        "adapter_dir": os.path.expanduser("~/action-lora-adapter"),
        "output_dir": "/mnt/hdd/forge/merged/action-full",
        "description": "Action compliance (JSON format)",
    },
    "domain": {
        "adapter_dir": os.path.expanduser("~/domain-lora-adapter"),
        "output_dir": "/mnt/hdd/forge/merged/domain-full",
        "description": "Domain knowledge (ICP/XRPL/homeforge)",
    },
}

BASE_MODEL = "NousResearch/Hermes-3-Llama-3.1-8B"


def merge_adapter(name, adapter_dir, output_dir, base_model):
    """Load base + adapter, merge, save full model."""
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from peft import PeftModel
    import gc

    print(f"\n{'='*60}")
    print(f"Merging: {name}")
    print(f"  Adapter: {adapter_dir}")
    print(f"  Output:  {output_dir}")
    print(f"{'='*60}")

    if not os.path.isdir(adapter_dir):
        print(f"  [ERROR] Adapter directory not found: {adapter_dir}")
        return False

    start = time.time()

    # Load base model
    print(f"  Loading base model in fp16...")
    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        torch_dtype=torch.float16,
        device_map="cpu",  # CPU for merge to avoid VRAM pressure
        trust_remote_code=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(
        base_model,
        trust_remote_code=True,
    )

    # Load adapter
    print(f"  Loading PEFT adapter...")
    model = PeftModel.from_pretrained(model, adapter_dir)

    # Merge and unload
    print(f"  Merging adapter into base weights...")
    model = model.merge_and_unload()

    # Save full model
    os.makedirs(output_dir, exist_ok=True)
    print(f"  Saving merged model to {output_dir}...")
    model.save_pretrained(output_dir, safe_serialization=True)
    tokenizer.save_pretrained(output_dir)

    elapsed = time.time() - start

    # Verify output
    safetensors_files = [f for f in os.listdir(output_dir) if f.endswith(".safetensors")]
    total_size = sum(os.path.getsize(os.path.join(output_dir, f)) for f in safetensors_files)

    print(f"  [OK] Merged model saved: {len(safetensors_files)} safetensors files, "
          f"{total_size / (1024**3):.1f} GB, {elapsed:.0f}s")

    # Free memory
    del model
    del tokenizer
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    return True


def main():
    parser = argparse.ArgumentParser(description="Merge LoRA adapters into full models")
    parser.add_argument("--only", nargs="*",
                        help="Only merge specific adapters (capsule, action, domain)")
    parser.add_argument("--base-model", default=BASE_MODEL,
                        help="Base model name or path")
    args = parser.parse_args()

    # Determine which adapters to merge
    if args.only:
        names = args.only
        for name in names:
            if name not in ADAPTERS:
                print(f"[ERROR] Unknown adapter: {name}")
                print(f"Available: {', '.join(ADAPTERS.keys())}")
                sys.exit(1)
    else:
        names = list(ADAPTERS.keys())

    print(f"=== Evolutionary Model Forge: Merge LoRA → Full Models ===")
    print(f"Base model: {args.base_model}")
    print(f"Merging: {', '.join(names)}")

    # Check all adapters exist before starting
    missing = []
    for name in names:
        adapter_dir = ADAPTERS[name]["adapter_dir"]
        if not os.path.isdir(adapter_dir):
            missing.append(f"  {name}: {adapter_dir}")
    if missing:
        print(f"\n[ERROR] Missing adapter directories:")
        for m in missing:
            print(m)
        sys.exit(1)

    # Check output disk space
    if os.path.isdir("/mnt/hdd"):
        import shutil
        free_gb = shutil.disk_usage("/mnt/hdd").free / (1024**3)
        needed_gb = len(names) * 16
        print(f"\nHDD free: {free_gb:.1f} GB (need ~{needed_gb} GB)")
        if free_gb < needed_gb:
            print(f"[WARN] May not have enough space for {len(names)} merged models")

    # Merge each adapter sequentially (to manage memory)
    results = {}
    for name in names:
        info = ADAPTERS[name]
        success = merge_adapter(
            name=name,
            adapter_dir=info["adapter_dir"],
            output_dir=info["output_dir"],
            base_model=args.base_model,
        )
        results[name] = success

    # Summary
    print(f"\n{'='*60}")
    print(f"Merge Summary")
    print(f"{'='*60}")
    for name, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        output_dir = ADAPTERS[name]["output_dir"]
        desc = ADAPTERS[name]["description"]
        print(f"  {status} {name}: {desc}")
        if success:
            print(f"        → {output_dir}")

    if all(results.values()):
        print(f"\n[OK] All {len(results)} models merged successfully")
        print(f"\nNext: Run eval_harness.py for baseline, then evolve.py")
    else:
        failed = [n for n, s in results.items() if not s]
        print(f"\n[WARN] {len(failed)} merge(s) failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
