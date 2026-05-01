#!/usr/bin/env python3
"""
Audit meta-instrument: runs all three audit testbeds (homeostasis,
commutant_probe, asving_probe), logs detection scores to history.

Detection score over time IS the meta-audit signal: if scores drop, it
means the underlying instruments are degrading (drifting from their
expected behavior, breaking on metric edge cases, or otherwise losing
validity). Tracking it gives us "is the audit still working?" without
re-deriving from scratch each time.

Designed to be run periodically (daily or weekly is plenty). Each run
takes ~3-5 min total because asving testbed makes Groq calls.

Usage:
  python3 audit_meta.py             # run all three, log + summarize
  python3 audit_meta.py --history N # show last N runs from history
  python3 audit_meta.py --skip-asving  # skip Groq-call testbed if rate-limited
"""
import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

HIST_PATH = Path.home() / "chronicle" / "data" / "audit_meta_history.jsonl"
BIN = Path.home() / "chronicle" / "bin"


def parse_detection_score(stdout):
    """Extract the 'X/Y = Z%' line from a testbed run."""
    for line in stdout.splitlines():
        m = re.search(r"(?:DETECTION(?:\s+SCORE)?|DETECTION):\s*(\d+)/(\d+)", line)
        if m:
            num, den = int(m.group(1)), int(m.group(2))
            return {"flagged": num, "expected": den,
                    "fraction": num / den if den else None}
    return None


def run_testbed(script_name, timeout=300):
    path = BIN / script_name
    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "elapsed_s": time.time() - t0,
                "score": None}
    score = parse_detection_score(result.stdout)
    return {
        "status": "ok" if result.returncode == 0 else f"rc={result.returncode}",
        "elapsed_s": time.time() - t0,
        "score": score,
        "stderr_tail": (result.stderr or "")[-300:],
    }


def show_history(n):
    if not HIST_PATH.exists():
        print("No history file yet.")
        return
    lines = HIST_PATH.read_text().splitlines()[-n:]
    print(f"Last {len(lines)} audit-meta runs:\n")
    for line in lines:
        try:
            r = json.loads(line)
            ts = r.get("timestamp", 0)
            stamp = time.strftime("%m-%d %H:%M", time.localtime(ts))
            scores = []
            for tb_name, tb in r.get("testbeds", {}).items():
                s = tb.get("score")
                if s and s.get("fraction") is not None:
                    scores.append(f"{tb_name[:8]}={s['flagged']}/{s['expected']}")
                else:
                    scores.append(f"{tb_name[:8]}=?")
            print(f"  {stamp}   {' '.join(scores)}")
        except Exception as e:
            print(f"  (bad line: {e})")


def run(skip_asving=False):
    testbeds = [
        ("homeostasis", "homeostasis_testbed.py", 180),
        ("commutant",    "commutant_probe_testbed.py", 180),
    ]
    if not skip_asving:
        testbeds.append(("asving", "asving_probe_testbed.py", 240))

    results = {}
    print(f"Running {len(testbeds)} testbeds...\n")
    for name, script, tmo in testbeds:
        print(f"=== {name} ({script}) ===")
        r = run_testbed(script, timeout=tmo)
        results[name] = r
        s = r.get("score")
        if s:
            print(f"  {name}: {s['flagged']}/{s['expected']} "
                  f"= {s['fraction']:.1%}  ({r['elapsed_s']:.1f}s)")
        else:
            print(f"  {name}: {r['status']}  ({r['elapsed_s']:.1f}s)")
        if r.get("stderr_tail"):
            err = r["stderr_tail"].strip()
            if err:
                print(f"  stderr: {err[-200:]}")
        print()

    record = {
        "timestamp": int(time.time()),
        "testbeds": results,
    }
    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HIST_PATH.open("a") as f:
        f.write(json.dumps(record) + "\n")

    # Summary
    print("=" * 70)
    overall_pass = 0
    overall_total = 0
    for name, r in results.items():
        s = r.get("score")
        if s:
            overall_pass += s["flagged"]
            overall_total += s["expected"]
    if overall_total:
        print(f"OVERALL DETECTION: {overall_pass}/{overall_total} "
              f"= {overall_pass/overall_total:.1%}")
    print("=" * 70)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--history", type=int, metavar="N")
    p.add_argument("--skip-asving", action="store_true")
    args = p.parse_args()
    if args.history is not None:
        show_history(args.history)
        sys.exit(0)
    run(skip_asving=args.skip_asving)
