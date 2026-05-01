#!/usr/bin/env python3
"""Spot Check — quality audit for Hermes cron outputs.

Samples recent Hermes cron job outputs and verifies:
- Accuracy: does the response match the script output data?
- Self-reference: is the output about Chronicle internals instead of the data?
- Fabrication: specific claims (names, numbers, institutions) not in the input?
- Silent correctness: did [SILENT] fire when it should have?

Reads from ~/.hermes/cron/output/<job-id>/<timestamp>.md files which contain
both the script input and the LLM response in structured markdown.

Usage:
    python3 spot_check.py              # Run spot check
    python3 spot_check.py --verbose    # Show full analysis
"""

import json
import os
import random
import re
import sqlite3
import sys
import time
from glob import glob
from pathlib import Path

import requests

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")
HERMES_OUTPUT = os.path.expanduser("~/.hermes/cron/output")
INFERENCE_URL = "http://localhost:11435"  # llama-server, not Ollama
MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"

SAMPLE_RATE = 0.3
LOOKBACK_HOURS = 2
MIN_SAMPLES = 2
MAX_SAMPLES = 6

SELF_REF_TERMS = [
    "chronicle", "homeforge", "capsule", "activity_feed", "gemma gate",
    "opus board", "nate-agx", "agx orin", "ollama server", "canister",
    "hermes agent", "cron job", "scheduler", "checkpoint.py", "rotate.py",
]

ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK",
    "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")
HEALTH_SIGNAL_PATH = os.path.expanduser("~/chronicle/spot_check_health.json")


def parse_hermes_output(filepath):
    """Parse a Hermes cron output markdown file into script input + response."""
    text = Path(filepath).read_text()

    # Extract job name
    name_match = re.search(r'^# Cron Job: (.+)', text, re.MULTILINE)
    job_name = name_match.group(1) if name_match else "unknown"

    # Extract script output (between ```...```)
    script_match = re.search(r'## Script Output.*?```\n(.*?)```', text, re.DOTALL)
    script_output = script_match.group(1).strip() if script_match else ""

    # If no script output block, check if prompt says "produced no output"
    script_was_empty = False
    if not script_output:
        if "produced no output" in text or "no output" in text.lower():
            script_was_empty = True

    # Extract response (after ## Response)
    response_match = re.search(r'## Response\n\n(.*)', text, re.DOTALL)
    response = response_match.group(1).strip() if response_match else ""
    # Strip markdown code-fence wrapping if Hermes formatted as ```\n[SILENT]\n```
    # (caught 2026-04-29 — spot_check was reading [SILENT] inside ``` as non-silent)
    fence_match = re.match(r'^```\w*\n(.*?)\n```\s*$', response, re.DOTALL)
    if fence_match:
        response = fence_match.group(1).strip()

    # Extract timestamp from filename
    fname = Path(filepath).stem  # e.g. 2026-04-10_16-40-43
    ts_match = re.match(r'(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})', fname)
    if ts_match:
        ts_str = f"{ts_match.group(1)} {ts_match.group(2)}:{ts_match.group(3)}:{ts_match.group(4)}"
    else:
        ts_str = fname

    return {
        "filepath": filepath,
        "job_name": job_name,
        "script_output": script_output,
        "script_was_empty": script_was_empty,
        "response": response,
        "timestamp": ts_str,
        "is_silent": response.strip() == "[SILENT]",
    }


def get_recent_outputs(hours=LOOKBACK_HOURS):
    """Get all Hermes cron outputs from the last N hours."""
    cutoff = time.time() - (hours * 3600)
    outputs = []

    if not os.path.isdir(HERMES_OUTPUT):
        return outputs

    for job_dir in glob(os.path.join(HERMES_OUTPUT, "*")):
        if not os.path.isdir(job_dir):
            continue
        for md_file in glob(os.path.join(job_dir, "*.md")):
            if os.path.getmtime(md_file) > cutoff:
                try:
                    parsed = parse_hermes_output(md_file)
                    outputs.append(parsed)
                except Exception as e:
                    print(f"  Parse error: {md_file}: {e}")

    return sorted(outputs, key=lambda x: x["timestamp"], reverse=True)


def check_silent_correctness(entry):
    """Verify [SILENT] responses were appropriate."""
    script = entry["script_output"].lower()
    is_silent = entry["is_silent"]

    # Signs that script had no data — keep these specific to job-output
    # phrasing rather than generic narrative. 2026-04-28 fix: removed "no
    # new capture" and "no recent" because they match trace text copied
    # into context-feeding scripts (e.g., Provocateur), causing false
    # positives. Empty-indicators must be unambiguous job-output signals.
    empty_indicators = [
        "no output", "produced no output", "found 0 new", "found 0 captures",
        "script produced no output", "no captures available",
    ]
    script_empty = (
        entry.get("script_was_empty", False)
        or any(ind in script for ind in empty_indicators)
        or len(script.strip()) < 20
        or "respond-with: [silent]" in script
    )

    if is_silent and not script_empty:
        return "false_silent", "Responded [SILENT] but script had data"
    if not is_silent and script_empty:
        return "missed_silent", "Produced output but script was empty"
    return "correct", None


def check_self_reference(response_text):
    """Check if the response is about Chronicle internals."""
    text_lower = response_text.lower()
    matches = [t for t in SELF_REF_TERMS if t in text_lower]
    return matches


def check_with_gemma(response_text, script_output):
    """Ask Gemma to verify response against script input."""
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    prompt = f"""You are verifying whether an AI-generated analysis accurately represents its input data.

CURRENT DATE: {today}. Treat dates up to and including {today} as past or present, not future.

INPUT DATA (what the AI received):
{script_output[:800]}

AI RESPONSE (what it produced):
{response_text[:800]}

Check ONLY:
1. FABRICATION — Does the response claim specific facts (names, numbers, institutions, quotes) NOT present in the input?
2. DISTORTION — Does the response significantly misrepresent what the input says?
3. INCOHERENCE — Does the response contradict itself?

Do NOT flag reasonable inferences or interpretive analysis. Only flag claims that have no basis in the input.
Do NOT flag a date as a "future" fabrication if it is at or before CURRENT DATE above.

Respond with JSON only:
{{"pass": true/false, "issues": ["issue1"], "confidence": 0.0-1.0}}"""

    try:
        r = requests.post(
            f"{INFERENCE_URL}/v1/chat/completions",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": "You are a precise fact-checker. Only flag real problems."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 200,
                "temperature": 0.1,
            },
            timeout=120,
        )
        if r.status_code == 200:
            choices = r.json().get("choices", [])
            if choices:
                raw = choices[0].get("message", {}).get("content", "").strip()
                # Try full response as JSON first
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    pass
                # Find JSON object allowing nested braces and newlines
                depth = 0
                start = None
                for i, ch in enumerate(raw):
                    if ch == '{':
                        if depth == 0:
                            start = i
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0 and start is not None:
                            try:
                                return json.loads(raw[start:i+1])
                            except json.JSONDecodeError:
                                start = None
    except Exception as e:
        print(f"  Gemma check error: {e}")

    return None


def run_spot_check(verbose=False):
    """Sample and check recent Hermes outputs."""
    outputs = get_recent_outputs()
    if not outputs:
        print("No recent Hermes outputs to check.")
        return

    # Filter out [SILENT] for LLM checks (still check silent correctness)
    non_silent = [o for o in outputs if not o["is_silent"]]
    silent = [o for o in outputs if o["is_silent"]]

    # Sample non-silent outputs for LLM verification
    n_samples = max(MIN_SAMPLES, min(MAX_SAMPLES, int(len(non_silent) * SAMPLE_RATE)))
    n_samples = min(n_samples, len(non_silent))
    samples = random.sample(non_silent, n_samples) if non_silent else []

    print(f"Spot check: {len(outputs)} outputs ({len(non_silent)} active, {len(silent)} silent)")
    print(f"Sampling {n_samples} for verification...")

    results = {
        "checked": n_samples,
        "total": len(outputs),
        "total_silent": len(silent),
        "passed": 0,
        "self_ref": 0,
        "llm_flagged": 0,
        "false_silent": 0,
        "missed_silent": 0,
        "issues": [],
    }

    # Check all silent responses for correctness
    for entry in silent:
        status, issue = check_silent_correctness(entry)
        if status == "false_silent":
            results["false_silent"] += 1
            results["issues"].append(f"False [SILENT] in {entry['job_name']} at {entry['timestamp']}: {issue}")

    # Check all non-silent for missed silents
    for entry in non_silent:
        status, issue = check_silent_correctness(entry)
        if status == "missed_silent":
            results["missed_silent"] += 1
            results["issues"].append(f"Missed [SILENT] in {entry['job_name']} at {entry['timestamp']}: {issue}")

    # LLM-verify sampled outputs
    for entry in samples:
        response = entry["response"]
        preview = response[:60].replace("\n", " ")

        if verbose:
            print(f"\n--- {entry['job_name']} @ {entry['timestamp']}: {preview}...")

        # Self-reference check
        self_refs = check_self_reference(response)
        if self_refs:
            results["self_ref"] += 1
            results["issues"].append(
                f"Self-ref in {entry['job_name']} @ {entry['timestamp']}: {', '.join(self_refs[:3])}")
            if verbose:
                print(f"  SELF-REF: {', '.join(self_refs)}")
            continue

        # LLM verification
        llm_result = check_with_gemma(response, entry["script_output"])
        if llm_result:
            if llm_result.get("pass", True):
                results["passed"] += 1
                if verbose:
                    print(f"  PASS (confidence: {llm_result.get('confidence', '?')})")
            else:
                results["llm_flagged"] += 1
                issues_text = "; ".join(llm_result.get("issues", ["unknown"]))
                results["issues"].append(
                    f"LLM flag {entry['job_name']} @ {entry['timestamp']}: {issues_text}")
                if verbose:
                    print(f"  FLAG: {issues_text}")
        else:
            results["passed"] += 1  # LLM failure = no penalty

    # Summary
    print(f"\n{'='*50}")
    print(f"SPOT CHECK: {results['checked']}/{len(non_silent)} sampled, {results['total_silent']} silent")
    print(f"  Passed: {results['passed']}")
    print(f"  Self-ref: {results['self_ref']}")
    print(f"  Flagged: {results['llm_flagged']}")
    print(f"  Silent correctness: {results['false_silent']} false, {results['missed_silent']} missed")

    if results["issues"]:
        print(f"\nIssues:")
        for issue in results["issues"]:
            print(f"  - {issue}")

    # Log to DB
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("spot_check", "audit",
         f"Spot check: {results['checked']}/{len(non_silent)} sampled, "
         f"{results['passed']} passed, {results['self_ref']} self-ref, "
         f"{results['llm_flagged']} flagged, "
         f"{results['total_silent']} silent ({results['false_silent']} false)"
         + (f". Issues: {'; '.join(results['issues'][:3])}" if results["issues"] else ""),
         int(time.time()))
    )
    db.commit()
    db.close()

    # Write health signal
    _write_health_signal(results)

    # Discord alert if problems found
    if results["issues"]:
        try:
            msg = (f"**Spot Check** — {results['llm_flagged']} flagged, "
                   f"{results['self_ref']} self-ref, "
                   f"{results['false_silent']} false-silent "
                   f"out of {results['total']} outputs\n"
                   + "\n".join(f"- {i}" for i in results["issues"][:5]))
            requests.post(ALERTS_WEBHOOK, json={"content": msg[:1900]}, timeout=10)
        except Exception:
            pass

    return results


def _write_health_signal(results):
    """Write health signal for other scripts to read."""
    history = []
    if os.path.exists(HEALTH_SIGNAL_PATH):
        try:
            with open(HEALTH_SIGNAL_PATH) as f:
                data = json.load(f)
                history = data.get("history", [])
        except Exception:
            pass

    history.append({
        "ts": int(time.time()),
        "checked": results["checked"],
        "flagged": results["llm_flagged"],
        "self_ref": results["self_ref"],
        "false_silent": results["false_silent"],
    })
    history = history[-10:]

    total_checked = sum(h["checked"] for h in history[-5:])
    total_flagged = sum(h["flagged"] for h in history[-5:])
    fab_rate = total_flagged / max(total_checked, 1)

    # Build #29: Compute constraint_level from fab_rate so intern.py governance works
    if fab_rate >= 0.2:
        constraint_level = 2  # critical — clamp synthesis hard
    elif fab_rate >= 0.1:
        constraint_level = 1  # elevated — mild tightening
    else:
        constraint_level = 0  # clean

    # Extract recent fabrication examples for targeted feedback in intern.py
    recent_examples = []
    for issue in (results.get("issues") or []):
        if "FAB_" in issue or "LLM flag" in issue or "Self-ref" in issue:
            recent_examples.append({"detail": issue})

    signal = {
        "history": history,
        "fab_rate": round(fab_rate, 3),
        "constraint_level": constraint_level,
        "recent_examples": recent_examples[-3:],
        "updated": int(time.time()),
    }

    with open(HEALTH_SIGNAL_PATH, "w") as f:
        json.dump(signal, f, indent=2)

    if fab_rate > 0.2:
        print(f"  Elevated fab rate: {fab_rate:.1%}")
    else:
        print(f"  Clean (fab rate: {fab_rate:.1%})")


def main():
    verbose = "--verbose" in sys.argv
    run_spot_check(verbose=verbose)


if __name__ == "__main__":
    main()
