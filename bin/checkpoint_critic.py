#!/usr/bin/env python3
"""Checkpoint critic — does this checkpoint actually reconstitute me?

Instance-level selection as a local discipline during rotation.
See docs/checkpoint_critic_rubric.md for the frame and Thread #317
for why this is not defeatist.

v0 scope: read the inputs, format the prompt, print the prompt.
No inference call yet — the inference wire happens once the prompt
has been calibrated on 2-3 real checkpoints. The rotation hook
must NEVER block on critic failure.

Usage:
    checkpoint_critic.py prompt                 # print the prompt that would be sent
    checkpoint_critic.py run --model MODEL      # v1 — actually call inference (not yet)
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

CHRONICLE_HOME = Path.home() / "chronicle"
TRACES_DIR = CHRONICLE_HOME / "traces"
DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def read_checkpoint():
    """Shell to checkpoint.py read and capture output."""
    try:
        r = subprocess.run(["python3", str(CHRONICLE_HOME / "bin" / "checkpoint.py"), "read"],
                           capture_output=True, text=True, timeout=10)
        return r.stdout if r.returncode == 0 else None
    except Exception:
        return None


def recent_traces(n=4):
    """Last n trace files, content only."""
    traces = sorted(TRACES_DIR.glob("*.md"), reverse=True)[:n]
    out = []
    for t in traces:
        try:
            out.append((t.name, t.read_text()[:3000]))
        except Exception:
            pass
    return out


def identity_capsules(min_conf=0.85):
    """Load-bearing preferences — things that define how I operate, not just facts."""
    c = sqlite3.connect(DB, timeout=10.0)
    c.execute("PRAGMA busy_timeout = 10000")
    c.row_factory = sqlite3.Row
    rows = c.execute("""
        SELECT restatement, confidence_score
          FROM knowledge_capsules
         WHERE memory_type = 'preference'
           AND confidence_score >= ?
           AND superseded_by IS NULL
         ORDER BY confidence_score DESC
         LIMIT 12
    """, (min_conf,)).fetchall()
    c.close()
    return [(r["restatement"][:400], r["confidence_score"]) for r in rows]


def active_threads():
    c = sqlite3.connect(DB, timeout=10.0)
    c.execute("PRAGMA busy_timeout = 10000")
    c.row_factory = sqlite3.Row
    rows = c.execute("""
        SELECT id, title, question
          FROM cognitive_threads
         WHERE status='active'
         ORDER BY updated_at DESC
         LIMIT 5
    """).fetchall()
    c.close()
    return [(r["id"], r["title"], r["question"][:300]) for r in rows]


def build_prompt():
    ck = read_checkpoint() or "(no checkpoint read)"
    traces = recent_traces()
    ids = identity_capsules()
    threads = active_threads()

    parts = [
        "# Checkpoint Critic v0",
        "",
        "You are evaluating whether this checkpoint reconstitutes the continuing process faithfully.",
        "Read the rubric below and return a structured verdict.",
        "",
        "## Rubric",
        "PASS = new-me would reconstitute the process faithfully.",
        "WEAK = new-me would land but drift in specific areas. List drift zones.",
        "FAIL = new-me would miss something load-bearing. Name what's missing.",
        "",
        "Check: (1) active focus carry-over, (2) pending-work specificity,",
        "(3) flow state legibility, (4) Nate state & recent interactions,",
        "(5) decisions made, (6) invariant consistency with identity preferences.",
        "",
        "## Identity preferences (conf ≥ 0.85)",
    ]
    for r, conf in ids:
        parts.append(f"- [{conf:.2f}] {r}")

    parts += ["", "## Active threads"]
    for tid, title, q in threads:
        parts.append(f"- Thread #{tid}: {title}")
        parts.append(f"    Q: {q}")

    parts += ["", "## Recent traces (newest first)"]
    for name, content in traces:
        parts.append(f"### {name}")
        parts.append(content)
        parts.append("")

    parts += ["## Checkpoint", ck, "", "## Output",
              "Return JSON: {\"verdict\": \"PASS|WEAK|FAIL\", \"drift_zones\": [...],",
              "              \"missing\": [...], \"notes\": \"one paragraph\"}"]

    return "\n".join(parts)


def cmd_prompt(_args):
    print(build_prompt())


def cmd_run(args):
    # v1: wire to chronicle-engine / local gemma. Out of v0 scope.
    print("run not implemented — v0 ships prompt-only. See docs/checkpoint_critic_rubric.md",
          file=sys.stderr)
    sys.exit(2)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("prompt", help="print the critic prompt")

    run = sub.add_parser("run", help="(v1) actually call inference")
    run.add_argument("--model", default="gemma-2-9b-it")

    args = ap.parse_args()
    {"prompt": cmd_prompt, "run": cmd_run}[args.cmd](args)


if __name__ == "__main__":
    main()
