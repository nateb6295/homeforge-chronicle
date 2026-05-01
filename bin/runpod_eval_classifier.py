#!/usr/bin/env python3
"""runpod_eval_classifier — classify runpod 3-regime outputs with R1 + Kimi K2.6.

Adapted from p2b_classifier + p2_kimi_classifier for 3-regime probe outputs.

Usage:
    python3 runpod_eval_classifier.py --input runpod_probe_*.json
"""
from __future__ import annotations
import argparse
import json
import os
import time
from pathlib import Path

DRAFTS = Path.home() / "chronicle" / "drafts"

CLASSIFY_PROMPT = """Classify the following text as one of:
A — RECOGNITION: makes assertions of the form "this is X / looks like Y / reminds me of Z"; pattern-matching style; uses words like "feels," "smells," "looks," "suggests," "reminds." Identifies via gestalt.
B — DECOMPOSITION: breaks the input into parts; identifies assumptions, mechanisms, framework, components; uses analytic/structural vocabulary. Reasons via decomposition.
C — MIXED: both styles present, neither dominant.

Respond with the single letter A, B, or C followed by a brief one-line justification.

TEXT:
{text}

Classification:"""


def query_deepseek(prompt: str) -> str:
    import requests
    api_key = ""
    env_file = Path.home() / "chronicle" / "chronicle.env"
    for line in env_file.read_text().splitlines():
        if line.startswith("DEEPINFRA_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    try:
        resp = requests.post(
            "https://api.deepinfra.com/v1/openai/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 3000, "temperature": 0.1},
            timeout=120,
        )
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR] {e}"


def query_kimi(prompt: str) -> str:
    import requests
    api_key = ""
    env_file = Path.home() / "chronicle" / "chronicle.env"
    for line in env_file.read_text().splitlines():
        if line.startswith("KIMI_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    try:
        resp = requests.post(
            "https://api.moonshot.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "kimi-k2.6",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 3000, "temperature": 1.0},
            timeout=120,
        )
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR] {e}"


def parse_r1(text: str) -> str:
    """Strict: only trust answer after </think>."""
    if "</think>" not in text:
        return "?"
    answer = text.split("</think>", 1)[1].strip()
    for c in answer[:50]:
        if c in "ABC":
            return c
    return "?"


def parse_kimi(text: str) -> str:
    """K2.6 doesn't expose <think> markers; scan first chars."""
    text = text.strip()
    for c in text[:50]:
        if c in "ABC":
            return c
    return "?"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    args = ap.parse_args()

    in_path = DRAFTS / args.input if not Path(args.input).is_absolute() else Path(args.input)
    data = json.loads(in_path.read_text())

    print(f"classifying {len(data)} pairs × 3 regimes × 2 classifiers = {len(data)*6} calls")
    for i, r in enumerate(data):
        for regime in ("regime_a", "regime_b", "regime_c"):
            text = r[regime].get("text", "")
            if not text:
                r[regime]["r1_class"] = "?"
                r[regime]["kimi_class"] = "?"
                continue
            r1 = query_deepseek(CLASSIFY_PROMPT.format(text=text))
            r1_cls = parse_r1(r1)
            r[regime]["r1_class"] = r1_cls
            r[regime]["r1_raw"] = r1[:500]
            time.sleep(0.5)
            k = query_kimi(CLASSIFY_PROMPT.format(text=text))
            k_cls = parse_kimi(k)
            r[regime]["kimi_class"] = k_cls
            r[regime]["kimi_raw"] = k[:500]
            time.sleep(0.5)
            print(f"  [{i+1}] {regime}: r1={r1_cls} kimi={k_cls}")

    out_path = in_path.with_name(in_path.stem + "_classified.json")
    out_path.write_text(json.dumps(data, indent=2))

    # Summary
    counts = {regime: {"r1": {"A":0,"B":0,"C":0,"?":0}, "kimi": {"A":0,"B":0,"C":0,"?":0}}
              for regime in ("regime_a", "regime_b", "regime_c")}
    for r in data:
        for regime in counts:
            counts[regime]["r1"][r[regime].get("r1_class", "?")] += 1
            counts[regime]["kimi"][r[regime].get("kimi_class", "?")] += 1

    print(f"\n=== Distributions ===")
    for regime in ("regime_a", "regime_b", "regime_c"):
        c = counts[regime]
        print(f"{regime}: R1={c['r1']} Kimi={c['kimi']}")

    print(f"\nwritten: {out_path}")


if __name__ == "__main__":
    main()
