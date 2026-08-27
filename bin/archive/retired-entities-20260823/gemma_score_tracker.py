#!/usr/bin/env python3
"""Track Gemma evaluation scores by source/job over time.

Reads gemma_evaluations.jsonl and produces a summary showing which
Hermes jobs consistently produce quality output vs sediment.
"""
import json
import os
import re
import sys
import time
from collections import defaultdict

EVAL_LOG = os.path.expanduser("~/chronicle/data/gemma_evaluations.jsonl")
QUALITY_THRESHOLD = 0.6


def classify_source(entry):
    preview = entry.get("input_preview", "")
    source = entry.get("source", "")

    job_match = re.search(r"job_id:\s*(\w+)", preview)
    if job_match:
        return f"hermes-job-{job_match.group(1)}"

    if "[Hermes]" in preview and "CHALLENGE" in preview:
        return "hermes-challenge"
    if "[Hermes]" in preview:
        return "hermes-post"
    if "Morning note" in preview or "morning note" in preview:
        return "opus-morning"
    if "Cronjob Response" in preview:
        return "hermes-cron"

    return source or "unknown"


def load_evaluations():
    entries = []
    try:
        with open(EVAL_LOG) as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
    except FileNotFoundError:
        pass
    return entries


def report(entries, window_hours=None):
    if window_hours:
        cutoff = time.time() - (window_hours * 3600)
        entries = [e for e in entries if e.get("ts", 0) >= cutoff]

    by_source = defaultdict(list)
    for e in entries:
        src = classify_source(e)
        by_source[src].append(e)

    print(f"Gemma Score Report — {len(entries)} evaluations", end="")
    if window_hours:
        print(f" (last {window_hours}h)")
    else:
        print(" (all time)")
    print("=" * 72)

    rows = []
    for src, evals in by_source.items():
        scores = [e.get("overall", 0) for e in evals]
        mean = sum(scores) / len(scores)
        passes = sum(1 for s in scores if s >= QUALITY_THRESHOLD)
        pass_rate = passes / len(scores) * 100
        recent = scores[-5:]
        rows.append((mean, src, len(scores), pass_rate, recent))

    rows.sort(key=lambda r: r[0])

    print(f"{'Source':<25} {'N':>4} {'Mean':>6} {'Pass%':>6}  Recent")
    print("-" * 72)
    for mean, src, n, pass_rate, recent in rows:
        recent_str = " ".join(f"{s:.1f}" for s in recent)
        flag = " ← sediment" if mean < 0.3 and n >= 2 else ""
        print(f"{src:<25} {n:>4} {mean:>6.2f} {pass_rate:>5.0f}%  [{recent_str}]{flag}")

    total_scores = [e.get("overall", 0) for e in entries]
    if total_scores:
        print("-" * 72)
        total_mean = sum(total_scores) / len(total_scores)
        total_passes = sum(1 for s in total_scores if s >= QUALITY_THRESHOLD)
        print(f"{'TOTAL':<25} {len(total_scores):>4} {total_mean:>6.2f} {total_passes/len(total_scores)*100:>5.0f}%")

    sediment_sources = [src for mean, src, n, _, _ in rows if mean < 0.3 and n >= 2]
    if sediment_sources:
        print(f"\nSediment candidates: {', '.join(sediment_sources)}")


if __name__ == "__main__":
    entries = load_evaluations()
    window = None
    if len(sys.argv) > 1:
        try:
            window = int(sys.argv[1])
        except ValueError:
            pass
    report(entries, window)
