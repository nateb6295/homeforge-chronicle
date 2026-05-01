#!/usr/bin/env python3
"""p2b_classifier — classifies each P2 regime read as recognition or decomposition.

Reads p2_probe_results_*.json, sends each text to DeepSeek R1 with a
classification prompt, writes back classifications.

Usage:
    python3 p2b_classifier.py --input p2_probe_results_20260428_1532.json
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

Respond with ONLY the single letter A, B, or C followed by a brief one-line justification.

TEXT:
{text}

Classification:"""


def query_deepseek(prompt: str) -> dict:
    """Query DeepSeek R1 via DeepInfra."""
    import requests
    api_key = ""
    env_file = Path.home() / "chronicle" / "chronicle.env"
    for line in env_file.read_text().splitlines():
        if line.startswith("DEEPINFRA_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    if not api_key:
        return {"error": "DEEPINFRA_API_KEY not found"}

    t0 = time.time()
    try:
        resp = requests.post(
            "https://api.deepinfra.com/v1/openai/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 3000, "temperature": 0.1},
            timeout=120,
        )
        elapsed_ms = int((time.time() - t0) * 1000)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        body = resp.json()
        return {
            "text": body["choices"][0]["message"]["content"],
            "ms": elapsed_ms,
        }
    except Exception as e:
        return {"error": str(e)}


def parse_classification(text: str) -> str:
    """Extract A/B/C from response — only after </think> closes; reject pre-think text."""
    text = text.strip()
    # DeepSeek R1: only trust answer after </think>
    if "</think>" not in text:
        return "?"  # reasoning didn't complete, classification invalid
    answer = text.split("</think>", 1)[1].strip()
    for c in answer[:50]:
        if c in "ABC":
            return c
    return "?"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    args = ap.parse_args()

    in_path = DRAFTS / args.input if not Path(args.input).is_absolute() else Path(args.input)
    data = json.loads(in_path.read_text())

    print(f"classifying {len(data)} pairs ({len(data)*2} texts)")
    for i, r in enumerate(data):
        for regime in ("regime_a", "regime_b"):
            text = r[regime].get("text", "")
            if not text:
                continue
            result = query_deepseek(CLASSIFY_PROMPT.format(text=text))
            cls = parse_classification(result.get("text", ""))
            r[regime]["classification"] = cls
            r[regime]["classify_full"] = result.get("text", "")
            print(f"  [{i+1}] {regime}: {cls}")
            time.sleep(0.5)

    out_path = in_path.with_name(in_path.stem + "_classified.json")
    out_path.write_text(json.dumps(data, indent=2))
    print(f"written: {out_path}")

    # Summary
    a_counts = {"A": 0, "B": 0, "C": 0, "?": 0}
    b_counts = {"A": 0, "B": 0, "C": 0, "?": 0}
    for r in data:
        a_counts[r["regime_a"].get("classification", "?")] += 1
        b_counts[r["regime_b"].get("classification", "?")] += 1
    print(f"\nREGIME A: {a_counts}")
    print(f"REGIME B: {b_counts}")


if __name__ == "__main__":
    main()
