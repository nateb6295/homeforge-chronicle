#!/usr/bin/env python3
"""CCS Hygiene — detect stale or noisy operational fields.

Checks each CCS field for staleness signals:
- episodic_trace: duplicate entries, low-information entries
- predictive_cue: references to non-existent scripts/paths
- focal_entities: low-salience entities, stale context
- uncertainty_signals: resolved or outdated uncertainties

Usage:
  ccs_hygiene.py check          # Report issues
  ccs_hygiene.py check --json   # Machine-readable output
  ccs_hygiene.py discord        # Post report to #operator
"""

import json
import os
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

PDT = timezone(timedelta(hours=-7))
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
CHRONICLE_DIR = Path(os.path.expanduser("~/chronicle"))


def get_ccs():
    db = sqlite3.connect(DB_PATH, timeout=10)
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, focal_entities, constraints, "
        "episodic_trace, predictive_cue, uncertainty_signals, updated_at "
        "FROM cognitive_state ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        return None
    return {
        "semantic_gist": row[0] or "",
        "goal_orientation": row[1] or "",
        "focal_entities": row[2] or "",
        "constraints": row[3] or "",
        "episodic_trace": row[4] or "",
        "predictive_cue": row[5] or "",
        "uncertainty_signals": row[6] or "",
        "updated_at": row[7],
    }


def check_episodic(trace_raw):
    """Check episodic_trace for noise."""
    issues = []
    try:
        entries = json.loads(trace_raw) if trace_raw.strip().startswith("[") else [trace_raw]
    except json.JSONDecodeError:
        entries = [line.strip() for line in trace_raw.split("\n") if line.strip()]

    if not entries:
        issues.append(("empty", "episodic_trace is empty"))
        return issues

    # Check for duplicates
    counts = Counter(str(e).strip() for e in entries)
    for entry, count in counts.items():
        if count > 1:
            label = entry[:50] + "..." if len(entry) > 50 else entry
            issues.append(("duplicate", f"'{label}' appears {count}x"))

    # Check for low-information entries
    low_info_patterns = [
        r'^capture:\s*\w+\s+capture$',  # "capture: Nate capture"
        r'^capture:\s*$',                # empty capture
        r'^$',                           # empty string
    ]
    low_info_count = 0
    for entry in entries:
        text = str(entry).strip()
        for pat in low_info_patterns:
            if re.match(pat, text, re.IGNORECASE):
                low_info_count += 1
                break

    if low_info_count > 0:
        pct = low_info_count / len(entries) * 100
        issues.append(("low_info", f"{low_info_count}/{len(entries)} entries ({pct:.0f}%) are low-information"))

    return issues


def check_predictive_cue(cue):
    """Check predictive_cue for references to non-existent paths."""
    issues = []
    if not cue.strip():
        issues.append(("empty", "predictive_cue is empty"))
        return issues

    # Extract file paths
    paths = re.findall(r'(?:~/|/home/)\S+\.(?:py|sh|json|rs|toml|log)', cue)
    paths += re.findall(r'(?:scripts|bin|tools)/\S+\.(?:py|sh|json)', cue)

    for path in paths:
        expanded = os.path.expanduser(path)
        # Also check relative to chronicle dir
        candidates = [expanded, str(CHRONICLE_DIR / path), str(CHRONICLE_DIR / "bin" / os.path.basename(path))]
        found = any(os.path.exists(c) for c in candidates)
        if not found:
            issues.append(("stale_path", f"references non-existent: {path}"))

    # Check for command-line style that looks like auto-generated predictions
    if cue.count("&&") >= 2 or cue.count(";") >= 3:
        issues.append(("overloaded", "predictive_cue looks like chained commands, not a natural prediction"))

    return issues


def check_entities(entities_raw):
    """Check focal_entities for staleness."""
    issues = []
    try:
        entities = json.loads(entities_raw) if entities_raw.strip().startswith("[") else []
    except json.JSONDecodeError:
        issues.append(("parse_error", "focal_entities is not valid JSON"))
        return issues

    if not entities:
        issues.append(("empty", "no focal entities"))
        return issues

    # Check for low-salience entities
    low_sal = [e for e in entities if (e.get("salience") or 0) < 0.4]
    if low_sal:
        names = [e.get("name", "?") for e in low_sal]
        issues.append(("low_salience", f"entities below 0.4 salience: {', '.join(names)}"))

    # Check for entities that reference files/scripts (likely stale)
    for e in entities:
        name = e.get("name", "")
        if re.match(r'.*\.(py|sh|md|json|rs)$', name):
            issues.append(("file_entity", f"entity '{name}' looks like a filename, not a concept"))

    # Check entity count
    if len(entities) > 7:
        issues.append(("overcrowded", f"{len(entities)} entities — consider pruning below 7"))

    return issues


def check_uncertainties(unc_raw):
    """Check uncertainty_signals for staleness."""
    issues = []
    try:
        uncertainties = json.loads(unc_raw) if unc_raw.strip().startswith("[") else []
    except json.JSONDecodeError:
        issues.append(("parse_error", "uncertainty_signals is not valid JSON"))
        return issues

    if not uncertainties:
        # Empty is ok if there genuinely aren't uncertainties
        return issues

    for u in uncertainties:
        desc = u.get("description", "")
        mag = u.get("magnitude", 0)

        # Very low magnitude uncertainties are noise
        if mag < 0.2:
            issues.append(("trivial", f"uncertainty at {mag:.2f}: '{desc[:50]}'"))

    return issues


def check_field_lengths(ccs):
    """Check if total CCS is too long (>= 3500 chars based on sweep optimal ~2190)."""
    issues = []
    total = sum(len(ccs[k]) for k in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "constraints", "episodic_trace", "predictive_cue", "uncertainty_signals"
    ])

    if total > 3500:
        issues.append(("oversized", f"total CCS is {total} chars — optimal is ~2200, verbose was ~3700"))
    elif total < 500:
        issues.append(("sparse", f"total CCS is only {total} chars — may be under-populated"))

    return issues


def run_check(as_json=False):
    ccs = get_ccs()
    if not ccs:
        print("No CCS found.")
        return 1

    all_issues = {}
    all_issues["episodic_trace"] = check_episodic(ccs["episodic_trace"])
    all_issues["predictive_cue"] = check_predictive_cue(ccs["predictive_cue"])
    all_issues["focal_entities"] = check_entities(ccs["focal_entities"])
    all_issues["uncertainty_signals"] = check_uncertainties(ccs["uncertainty_signals"])
    all_issues["total_length"] = check_field_lengths(ccs)

    total_issues = sum(len(v) for v in all_issues.values())

    if as_json:
        print(json.dumps({
            "total_issues": total_issues,
            "updated_at": ccs["updated_at"],
            "issues": {k: [{"type": t, "msg": m} for t, m in v] for k, v in all_issues.items()},
        }, indent=2))
        return 0 if total_issues == 0 else 1

    # Human-readable output
    now = datetime.now(PDT).strftime("%H:%M PDT")
    updated = datetime.fromtimestamp(ccs["updated_at"], PDT).strftime("%H:%M PDT") if ccs["updated_at"] else "?"

    print(f"CCS Hygiene Check — {now} (CCS updated {updated})")
    print()

    if total_issues == 0:
        print("All fields clean.")
        return 0

    for field, issues in all_issues.items():
        if issues:
            print(f"{field}:")
            for itype, msg in issues:
                print(f"  [{itype}] {msg}")
            print()

    print(f"Total: {total_issues} issue(s)")
    return 1


def run_discord():
    """Post hygiene report to Discord."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        run_check()

    text = buf.getvalue()
    if not text.strip():
        return

    chronicle_env = os.path.expanduser("~/chronicle/chronicle.env")
    webhook = ""
    if os.path.exists(chronicle_env):
        for line in open(chronicle_env):
            if line.strip().startswith("OPERATOR_WEBHOOK="):
                webhook = line.strip().split("=", 1)[1].strip("'\"")

    if not webhook:
        print("No OPERATOR_WEBHOOK found", file=sys.stderr)
        return

    import requests
    msg = f"**CCS Hygiene**\n```\n{text[:1700]}\n```"
    try:
        requests.post(webhook, json={"content": msg}, timeout=10)
        print("Posted to Discord.")
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    cmd = sys.argv[1]
    if cmd == "check":
        as_json = "--json" in sys.argv
        sys.exit(run_check(as_json))
    elif cmd == "discord":
        run_discord()
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
