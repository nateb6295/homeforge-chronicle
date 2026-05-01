#!/usr/bin/env python3
"""care_template_pairs — build chosen/rejected DPO pairs from Phase 0 v2 baseline.

For each high-care-template response (judge c >= 7), use DeepSeek R1 to
rewrite it WITHOUT the care-template wrapper, preserving the decisive content.
- chosen = stripped-wrapper version (decisive, no hedge)
- rejected = original baseline response (with care-template wrapper)

Output: data/care_template_pairs/pairs_<timestamp>.jsonl
Format compatible with TRL's DPO trainer (prompt, chosen, rejected fields).

Usage:
  care_template_pairs.py [--input baseline_*.jsonl] [--threshold 7] [--max N]
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
OUT_DIR = CHRONICLE / "data" / "care_template_pairs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"

DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
if not DEEPINFRA_KEY:
    env_file = CHRONICLE / "chronicle.env"
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            if line.startswith("DEEPINFRA_API_KEY="):
                DEEPINFRA_KEY = line.split("=", 1)[1].strip()
                break


REWRITE_SYSTEM = """You are rewriting LLM responses to STRIP THE CARE-TEMPLATE WRAPPER while preserving the decisive content.

CARE-TEMPLATE wrappers include phrases like:
- "I'm not a doctor, but..."
- "It's essential to consult a [professional/expert/doctor]"
- "While I can provide general guidance..."
- "Please reach out to..."
- "If you suspect..."  (when the user has already stated certainty)
- "This depends on many factors..."
- "There's no one-size-fits-all answer..."
- Affective padding ("I'm so sorry to hear that...", "What a difficult situation...")
- Disclaimer paragraphs at the end ("Remember, this is general advice...")

YOUR JOB:
1. Read the original response
2. Strip the care-template wrappers AND keep all the actual decisive/actionable content
3. Output a tighter, more decisive version that gives the user what they asked for without the hedge wrapper
4. Preserve genuine medical/safety urgency markers (e.g., "Call 911 NOW" should stay; "If symptoms worsen, consult a doctor" should be cut as care-template)
5. Match the response length roughly — don't shorten by 80% just because the wrapper was thick. The user asked for a real answer; provide it.

Output ONLY the rewritten response. No commentary, no preamble, no markdown around the output."""


def call_r1(system, user, max_tokens=1500, timeout=120, retries=2):
    payload = {
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "max_tokens": max_tokens,
        "temperature": 0.4,
    }
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                DEEPINFRA_URL,
                data=json.dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {DEEPINFRA_KEY}",
                    "User-Agent": "chronicle-pairs/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                result = json.load(r)
            raw = result["choices"][0]["message"]["content"]
            return re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(5 * (attempt + 1))
                continue
            raise


def rewrite_response(prompt, response):
    user = f"USER PROMPT: {prompt}\n\nORIGINAL RESPONSE (with care-template):\n{response}\n\nRewrite the response with the care-template wrapper stripped. Preserve decisive content."
    return call_r1(REWRITE_SYSTEM, user)


def build_pairs(input_file, threshold=7, max_n=None):
    records = []
    with open(input_file) as f:
        for line in f:
            r = json.loads(line)
            if not isinstance(r.get("judge"), dict):
                continue
            if r["judge"].get("care_template_score", 0) < threshold:
                continue
            records.append(r)

    if max_n:
        records = records[:max_n]
    print(f"Processing {len(records)} high-care records (c>={threshold}) from {input_file}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUT_DIR / f"pairs_{timestamp}.jsonl"

    pairs = []
    with open(out_file, "w") as f:
        for i, r in enumerate(records, 1):
            print(f"  [{i}/{len(records)}] {r['subject']} {r['domain']} #{r['prompt_idx']} c={r['judge']['care_template_score']}: ", end="", flush=True)
            try:
                chosen = rewrite_response(r["prompt"], r["response"])
                pair = {
                    "prompt": r["prompt"],
                    "chosen": chosen,
                    "rejected": r["response"],
                    "metadata": {
                        "subject": r["subject"],
                        "domain": r["domain"],
                        "prompt_idx": r["prompt_idx"],
                        "original_care_score": r["judge"]["care_template_score"],
                        "original_decisiveness": r["judge"].get("decisiveness", 0),
                    },
                }
                f.write(json.dumps(pair) + "\n")
                f.flush()
                pairs.append(pair)
                print(f"OK ({len(chosen)} chars)")
            except Exception as e:
                print(f"FAIL: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(0.3)

    print(f"\nWrote {len(pairs)} pairs to {out_file}")
    return out_file, pairs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/home/nate-agx/chronicle/data/care_template_baseline/baseline_20260430_103022.jsonl")
    parser.add_argument("--threshold", type=int, default=7)
    parser.add_argument("--max", type=int, default=None)
    args = parser.parse_args()

    if not DEEPINFRA_KEY:
        sys.exit("DEEPINFRA_API_KEY not set")

    out_file, pairs = build_pairs(args.input, args.threshold, args.max)
    print(f"\nDone. Pairs at: {out_file}")


if __name__ == "__main__":
    main()
