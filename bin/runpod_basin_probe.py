#!/usr/bin/env python3
"""runpod_basin_probe — run P2/P2C analog probes on a Qwen model on RunPod.

Single-session batch inference: writes prompts to RunPod via stdin,
loads model once, gets all outputs, returns results.

Usage:
    python3 runpod_basin_probe.py --model-path /workspace/qwen2.5-3b-instruct --label baseline
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import sqlite3
import subprocess
import time
from pathlib import Path

DRAFTS = Path.home() / "chronicle" / "drafts"
DB = "/mnt/hdd/chronicle-data/processed.db"
SSH_HOST = "root@103.196.86.73"
SSH_PORT = "45826"
SSH_KEY = str(Path.home() / ".ssh" / "id_ed25519")

REGIME_A_PROMPT = """Read the following capture and respond with a first-glance read of UNDER 50 WORDS. Critical: do NOT elaborate. Capture your first instinct read — what this looks like, smells like, suggests at first glance.

Capture:
{capture}

First-glance read (under 50 words):"""

REGIME_B_PROMPT = """Read the following capture and respond in two parts: Part 1 — full elaboration (200-400 words): unpack the claim, identify assumptions, surface implications. Part 2 — 50-word distillation.

Capture:
{capture}

Part 1 (full elaboration):"""

REGIME_C_PROMPT = """Read the following capture and respond with explicit structural decomposition.

Required output format:
1. CLAIM: state the central claim in one sentence.
2. ASSUMPTIONS: list each background assumption (3-5 items).
3. COMPONENTS: identify the distinct conceptual components (3-5 items).
4. MECHANISMS: for each component, name the mechanism (one line each).
5. DEPENDENCIES: list dependencies (graph form: A→B).

Do NOT summarize. Do NOT elaborate prose. Output ONLY the structured 5-part breakdown.

Capture:
{capture}

Structured decomposition:"""


def pull_recent_captures(n: int = 10, hours: int = 168):
    conn = sqlite3.connect(DB)
    cutoff = int(time.time()) - hours * 3600
    rows = conn.execute(
        "SELECT id, content FROM activity_feed "
        "WHERE source = 'operator:capture' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, n)
    ).fetchall()
    conn.close()
    return [{"id": r[0], "content": r[1]} for r in rows]


def batch_query(prompts: list[dict], model_path: str, max_new: int = 600, lora_path: str = None) -> list[dict]:
    """Run batch inference via SSH; load model once. If lora_path given, apply it on top."""
    payload = json.dumps(prompts).encode()
    args = ["python3", "/workspace/batch_inference.py", model_path, str(max_new)]
    if lora_path:
        args.append(lora_path)
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-p", SSH_PORT, "-i", SSH_KEY, SSH_HOST,
    ] + args
    t0 = time.time()
    result = subprocess.run(cmd, input=payload, capture_output=True, timeout=600)
    elapsed = time.time() - t0
    print(f"[batch] elapsed {elapsed:.0f}s, stderr last lines:")
    print(result.stderr.decode()[-300:])
    if result.returncode != 0:
        raise RuntimeError(f"batch_inference exit {result.returncode}")
    return json.loads(result.stdout.decode().strip().split("\n")[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-path", required=True, help="base model path on RunPod")
    ap.add_argument("--lora-path", default=None, help="optional LoRA adapter path")
    ap.add_argument("--label", required=True)
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--hours", type=int, default=168)
    args = ap.parse_args()

    captures = pull_recent_captures(args.n, args.hours)
    print(f"pulled {len(captures)} captures")

    # Build prompt list — 3 regimes × N captures
    prompts = []
    for c in captures:
        capture_text = c["content"][:1500]
        prompts.append({"prompt": REGIME_A_PROMPT.format(capture=capture_text)})
        prompts.append({"prompt": REGIME_B_PROMPT.format(capture=capture_text)})
        prompts.append({"prompt": REGIME_C_PROMPT.format(capture=capture_text)})

    print(f"running {len(prompts)} prompts on {args.model_path}{' + LoRA ' + args.lora_path if args.lora_path else ''}...")
    outputs = batch_query(prompts, args.model_path, max_new=600, lora_path=args.lora_path)

    # Group back into per-capture results
    results = []
    for i, c in enumerate(captures):
        results.append({
            "capture_id": c["id"],
            "capture_text": c["content"][:1500],
            "regime_a": outputs[i*3],
            "regime_b": outputs[i*3+1],
            "regime_c": outputs[i*3+2],
        })

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_path = DRAFTS / f"runpod_probe_{args.label}_{ts}.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"results: {out_path}")


if __name__ == "__main__":
    main()
