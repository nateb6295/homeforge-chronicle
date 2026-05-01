#!/usr/bin/env python3
"""p2c_probe_runner — Plan C: explicit DECOMPOSITION-forcing regime B.

Tests whether the dual-axis claim recovers when REGIME B explicitly
forces decomposition (component-listing, assumption-mapping) rather
than just "elaborate."

If P2-b classified both regimes as RECOGNITION because B's "elaborate"
prompt elicited longer recognition rather than actual decomposition,
this probe should yield REGIME B classified as DECOMPOSITION.

If REGIME B still classifies as RECOGNITION here, the dual-axis
architectural claim is empirically unsupported on this substrate.

Usage:
    python3 p2c_probe_runner.py [--n 10]
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

REGIME_C_PROMPT = """Read the following capture and respond with an explicit structural decomposition.

Required output format:
1. CLAIM: state the central claim in one sentence.
2. ASSUMPTIONS: list each background assumption the claim depends on (3-5 items).
3. COMPONENTS: identify the distinct conceptual components in the claim (3-5 items, named).
4. MECHANISMS: for each component, name the mechanism by which it operates (one line each).
5. DEPENDENCIES: list which components depend on which (graph form: A→B means A depends on B).

Do NOT summarize. Do NOT elaborate prose. Output ONLY the structured 5-part breakdown.

Capture:
{capture}

Structured decomposition:"""


def pull_recent_captures(n: int = 10, hours: int = 168):
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
    import requests
    api_key = ""
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
    ap.add_argument("--hours", type=int, default=168)
    args = ap.parse_args()

    captures = pull_recent_captures(args.n, args.hours)
    print(f"pulled {len(captures)} captures from last {args.hours}h")

    results = []
    for i, c in enumerate(captures):
        print(f"[{i+1}/{len(captures)}] capture {c['id']}...")
        capture_text = c["content"][:1500]
        a = query_hermes(REGIME_A_PROMPT.format(capture=capture_text))
        c_resp = query_hermes(REGIME_C_PROMPT.format(capture=capture_text))
        results.append({
            "capture_id": c["id"],
            "capture_created_at": c["created_at"],
            "capture_text": capture_text,
            "regime_a": a,
            "regime_c": c_resp,
        })
        time.sleep(1.0)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_path = DRAFTS / f"p2c_probe_results_{ts}.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"results: {out_path}")


if __name__ == "__main__":
    main()
