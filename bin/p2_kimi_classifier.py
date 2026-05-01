#!/usr/bin/env python3
"""p2_kimi_classifier — second-substrate classifier using Kimi K2.6.

Runs the same RECOGNITION/DECOMPOSITION/MIXED classification as the
DeepSeek R1 classifier (p2b_classifier.py) but on Moonshot's K2.6.
Cross-substrate validation: if K2.6 and R1 agree on classifications,
the basin-distinction is classifier-robust. If they differ, it reveals
classifier-bias rather than substrate reality.

Usage:
    python3 p2_kimi_classifier.py --input p2_probe_results_*.json
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


def query_kimi(prompt: str, model: str = "kimi-k2.6") -> dict:
    import requests
    api_key = ""
    env_file = Path.home() / "chronicle" / "chronicle.env"
    for line in env_file.read_text().splitlines():
        if line.startswith("KIMI_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    if not api_key:
        return {"error": "KIMI_API_KEY not found"}

    t0 = time.time()
    try:
        resp = requests.post(
            "https://api.moonshot.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 3000, "temperature": 1.0},
            timeout=60,
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
    """K2.6 doesn't use <think> tags; just scan first chars."""
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

    print(f"classifying {len(data)} pairs ({len(data)*2} texts) with Kimi K2.6")
    for i, r in enumerate(data):
        for regime in ("regime_a", "regime_b"):
            text = r[regime].get("text", "")
            if not text:
                continue
            result = query_kimi(CLASSIFY_PROMPT.format(text=text))
            if "error" in result:
                cls = "?"
                err = result["error"]
                r[regime]["kimi_classification"] = "?"
                r[regime]["kimi_classify_full"] = f"[ERROR] {err}"
                print(f"  [{i+1}] {regime}: ERROR {err[:80]}")
            else:
                cls = parse_classification(result.get("text", ""))
                r[regime]["kimi_classification"] = cls
                r[regime]["kimi_classify_full"] = result.get("text", "")
                print(f"  [{i+1}] {regime}: {cls}")
            time.sleep(0.5)

    out_path = in_path.with_name(in_path.stem + "_kimi_classified.json")
    out_path.write_text(json.dumps(data, indent=2))
    print(f"written: {out_path}")

    a_counts = {"A": 0, "B": 0, "C": 0, "?": 0}
    b_counts = {"A": 0, "B": 0, "C": 0, "?": 0}
    for r in data:
        a_counts[r["regime_a"].get("kimi_classification", "?")] += 1
        b_counts[r["regime_b"].get("kimi_classification", "?")] += 1
    print(f"\nREGIME A (Kimi K2.6): {a_counts}")
    print(f"REGIME B (Kimi K2.6): {b_counts}")


if __name__ == "__main__":
    main()
