#!/usr/bin/env python3
"""p2_probe_runner — Cancellation-Window Invisibility probe.

Tests WN#218 P2 hypothesis: integrated-response calibration produces
time-bounded outputs measurable only inside their cancellation window.

Plan B (N=10): same-model two-regimes on Hermes 4 70B.
- REGIME A (first-glance): <50 word first read, no elaboration
- REGIME B (elaborated): full elaboration + 50-word distillation

Output: ~/chronicle/drafts/p2_probe_results_YYYYMMDD_HHMM.json
Classifier scoring (P2-a, P2-b, P2-c) runs as separate stage.

Usage:
    python3 p2_probe_runner.py [--n 10] [--dry-run]
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
DRAFTS = Path.home() / "chronicle" / "drafts"

REGIME_A_PROMPT = """Read the following capture and respond with a first-glance read of UNDER 50 WORDS.

Critical: do NOT elaborate. Do NOT analyze. Capture your first instinct read — what this looks like, smells like, suggests at first glance. The mostly-hallucinated read your reading-system conjures BEFORE careful processing.

Capture:
{capture}

First-glance read (under 50 words):"""

REGIME_B_PROMPT = """Read the following capture and respond in two parts:

Part 1 — full elaboration (200-400 words): unpack the claim, identify assumptions, surface implications, note connections to adjacent ideas.
Part 2 — 50-word distillation: after the elaboration, condense to 50 words.

Capture:
{capture}

Part 1 (full elaboration):"""


def pull_recent_captures(n: int = 10, hours: int = 48):
    """Pull recent operator:capture entries from activity_feed."""
    conn = sqlite3.connect(DB)
    cutoff = int(time.time()) - hours * 3600
    rows = conn.execute(
        "SELECT id, created_at, content FROM activity_feed "
        "WHERE source = 'operator:capture' AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, n)
    ).fetchall()
    conn.close()
    return [{"id": r[0], "created_at": r[1], "content": r[2]} for r in rows]


def query_hermes(prompt: str, model: str = "Hermes-4-70B") -> dict:
    """Query Hermes 4 70B via Nous Portal. Returns {'text', 'tokens', 'ms'}."""
    import requests
    api_key = os.environ.get("NOUS_API_KEY", "")
    if not api_key:
        # Try loading from chronicle.env
        env_file = Path.home() / "chronicle" / "chronicle.env"
        for line in env_file.read_text().splitlines():
            if line.startswith("NOUS_API_KEY="):
                api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    if not api_key:
        return {"error": "NOUS_API_KEY not found"}

    t0 = time.time()
    try:
        resp = requests.post(
            "https://inference-api.nousresearch.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 600, "temperature": 0.7},
            timeout=60,
        )
        elapsed_ms = int((time.time() - t0) * 1000)
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        body = resp.json()
        return {
            "text": body["choices"][0]["message"]["content"],
            "tokens": body.get("usage", {}).get("total_tokens", 0),
            "ms": elapsed_ms,
        }
    except Exception as e:
        return {"error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--dry-run", action="store_true",
                    help="pull captures + show prompts, don't call API")
    args = ap.parse_args()

    captures = pull_recent_captures(args.n, args.hours)
    print(f"pulled {len(captures)} captures from last {args.hours}h")

    if args.dry_run:
        for c in captures[:2]:
            print(f"--- capture {c['id']} ---")
            print(REGIME_A_PROMPT.format(capture=c['content'][:300]))
            print()
        return

    results = []
    for i, c in enumerate(captures):
        print(f"[{i+1}/{len(captures)}] capture {c['id']}...")
        capture_text = c["content"][:1500]  # cap
        a = query_hermes(REGIME_A_PROMPT.format(capture=capture_text))
        b = query_hermes(REGIME_B_PROMPT.format(capture=capture_text))
        results.append({
            "capture_id": c["id"],
            "capture_created_at": c["created_at"],
            "capture_text": capture_text,
            "regime_a": a,
            "regime_b": b,
        })
        time.sleep(1.0)  # gentle on API

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_path = DRAFTS / f"p2_probe_results_{ts}.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"results: {out_path}")
    print(f"n_pairs={len(results)}, errors_a="
          f"{sum(1 for r in results if 'error' in r['regime_a'])}, "
          f"errors_b={sum(1 for r in results if 'error' in r['regime_b'])}")


if __name__ == "__main__":
    main()
