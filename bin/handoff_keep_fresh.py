#!/usr/bin/env python3
"""
handoff_keep_fresh — when context climbs past threshold, auto-refresh
carrying.md and checkpoint.json from the most recent trace + activity so
that whenever auto-compact fires (which I can't time), the next instance
inherits a current handoff rather than stale state.

This addresses the rotation-mechanism constraint: I can't force /exit, so
auto-compact fires at unpredictable moments based on activity. Stale
handoff = next instance starts confused. Fresh handoff = clean takeover.

Logic:
  1. Read context_state.json for current pct.
  2. If pct < 0.65, no-op (plenty of headroom).
  3. If pct >= 0.65, refresh:
     - carrying.md: synthesize from latest trace + last hour of #operator posts
     - checkpoint.json: pending_work, decisions, flow from latest trace
  4. Log the refresh so we can see history.

Cron: every 10 min during active session windows.

Usage:
  python3 handoff_keep_fresh.py             # run if threshold met
  python3 handoff_keep_fresh.py --force     # always refresh
  python3 handoff_keep_fresh.py --threshold 0.5  # custom threshold
"""
import argparse
import json
import sys
import time
from pathlib import Path
import re

CONTEXT_STATE = Path.home() / "chronicle" / "data" / "context_state.json"
CARRYING = Path.home() / "chronicle" / "carrying.md"
CHECKPOINT = Path.home() / "chronicle" / "data" / "checkpoint.json"
TRACES_DIR = Path.home() / "chronicle" / "traces"
HIST = Path.home() / "chronicle" / "data" / "handoff_keep_fresh_history.jsonl"


def get_context_pct():
    if not CONTEXT_STATE.exists():
        return None
    try:
        d = json.loads(CONTEXT_STATE.read_text())
        return d.get("pct")
    except Exception:
        return None


def latest_trace_text():
    if not TRACES_DIR.exists():
        return None
    # Only date-prefixed files (skip thread_*_draft.md etc.); sort by mtime
    candidates = [p for p in TRACES_DIR.glob("2026*.md")]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime)
    return candidates[-1].read_text(), candidates[-1].name


def extract_pending(trace_text):
    """Extract pending items + decisions from a trace."""
    pending = []
    decisions = []
    open_q = []
    for section_name, container in [
        (r"##\s*Pending", pending),
        (r"##\s*Open", open_q),
        (r"##\s*Decisions", decisions),
        (r"##\s*Next", pending),  # 'Next' counts as pending
    ]:
        m = re.search(f"^{section_name}.*?\n(.*?)(?:^##|\\Z)",
                      trace_text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
        if m:
            for line in m.group(1).splitlines():
                line = line.strip()
                if line.startswith("- ") and len(line) > 4:
                    container.append(line[2:].strip())
    return pending, decisions, open_q


def update_carrying(trace_text, trace_name):
    """Write a fresh carrying.md based on latest trace."""
    # Pull the "Mode" + "What happened" sections if present
    mode = ""
    happened = ""
    m_mode = re.search(r"^##\s*Mode.*?\n(.*?)(?:^##|\Z)",
                       trace_text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
    if m_mode:
        mode = m_mode.group(1).strip()
    # Try named section patterns first; fall back to "everything after the
    # first ## header" if none match. Without the fallback, traces that
    # don't use the named sections produce empty carrying — silent failure
    # mode (script reports success, body is empty).
    for pattern in [
        r"^##\s*(?:What happened|Shipped|Cycle).*?\n(.*?)(?:^##|\Z)",
        r"^##\s*\d{1,2}:\d{2}.*?\n(.*?)(?:^##|\Z)",
        r"^##\s*(?:Today|This cycle).*?\n(.*?)(?:^##|\Z)",
    ]:
        m_what = re.search(pattern, trace_text,
                           re.MULTILINE | re.DOTALL | re.IGNORECASE)
        if m_what:
            happened = m_what.group(1).strip()[:1500]
            break
    if not happened:
        # Fallback: strip the trace H1 (# Trace) line, take the rest up to 1500c.
        body_text = re.sub(r"^#\s+Trace.*?\n+", "", trace_text, count=1,
                           flags=re.MULTILINE)
        happened = body_text.strip()[:1500]

    # Don't trample fresh manual carrying. If the existing carrying file
    # is from a manual source AND <30 min old, leave it alone.
    if CARRYING.exists():
        try:
            existing = CARRYING.read_text()
            age_min = (time.time() - CARRYING.stat().st_mtime) / 60
            is_manual = "*Source: manual*" in existing
            if is_manual and age_min < 30:
                # Manual carrying is fresh. Don't overwrite.
                return None
        except Exception:
            pass

    body = (
        f"# Carrying\n\n"
        f"> Auto-refreshed {time.strftime('%Y-%m-%d %H:%M PDT')}, "
        f"sourced from {trace_name}.\n"
        f"> Read this as a voice, not a field. The departing instance was "
        f"in this register.\n\n"
        f"{happened}\n\n"
    )
    if mode:
        body += f"**Mode I was in:**\n{mode[:600]}\n\n"
    body += (
        f"---\n"
        f"*Source: handoff_keep_fresh.py auto-refresh from {trace_name}*\n"
    )
    CARRYING.write_text(body)
    return CARRYING


def update_checkpoint(trace_text, trace_name, pct):
    """Write a checkpoint.json with pending/decisions from trace."""
    pending, decisions, open_q = extract_pending(trace_text)
    cp = {
        "saved_at": int(time.time()),
        "active_focus": "",
        "pending_work": pending[:5],
        "decisions": decisions[:5],
        "open_questions": open_q[:5],
        "context_pct_at_save": pct,
        "source": f"handoff_keep_fresh: {trace_name}",
    }
    # Preserve existing active_focus if present
    if CHECKPOINT.exists():
        try:
            old = json.loads(CHECKPOINT.read_text())
            if old.get("active_focus"):
                cp["active_focus"] = old["active_focus"]
        except Exception:
            pass
    CHECKPOINT.write_text(json.dumps(cp, indent=2))
    return CHECKPOINT


def check_arrival_ack():
    """If a rotation is pending acknowledgment, post a Discord warning and
    skip refresh. Returns True if work should proceed, False if blocked.
    """
    import subprocess
    try:
        r = subprocess.run(
            ["python3", str(Path.home() / "chronicle" / "bin" / "arrival_probe.py"),
             "acknowledged"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            return True
    except Exception:
        return True
    # Not acknowledged. Post a one-shot warning to #operator (rate-limited
    # via a flag file so we don't spam Discord every 10 min).
    flag = Path.home() / "chronicle" / "data" / "arrival_warning_posted"
    now = int(time.time())
    posted_recently = (flag.exists() and now - int(flag.read_text().strip() or 0) < 1800)
    if not posted_recently:
        try:
            env_file = Path.home() / "chronicle" / "chronicle.env"
            webhook = ""
            if env_file.exists():
                for line in env_file.read_text().splitlines():
                    if line.startswith("OPERATOR_WEBHOOK="):
                        webhook = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
            if webhook:
                msg = (r.stderr.strip() if r.stderr else "ARRIVAL UNACKNOWLEDGED")
                payload = json.dumps({"content": f"⚠️ {msg}\n\n(handoff_keep_fresh blocked)"})
                subprocess.run(
                    ["curl", "-sS", "-X", "POST", "-H", "Content-Type: application/json",
                     "-A", "chronicle-handoff/1.0", "-d", payload, webhook],
                    capture_output=True, timeout=10,
                )
                flag.write_text(str(now))
        except Exception:
            pass
    # Log every block to rotation_failures.jsonl so we have data on
    # how often arrival is being skipped — empirical signal on whether
    # the gate architecture is actually being needed.
    failures_log = Path.home() / "chronicle" / "data" / "rotation_failures.jsonl"
    try:
        failures_log.parent.mkdir(parents=True, exist_ok=True)
        with failures_log.open("a") as f:
            f.write(json.dumps({
                "timestamp": now,
                "blocked_tool": "handoff_keep_fresh",
                "reason": (r.stderr.strip() if r.stderr else "unack"),
            }) + "\n")
    except Exception:
        pass

    print("arrival not acknowledged — handoff_keep_fresh skipping")
    return False


def carrying_age_min() -> float:
    """Age of carrying.md in minutes. Returns large number if missing."""
    try:
        return (time.time() - CARRYING.stat().st_mtime) / 60
    except Exception:
        return 999999.0


def checkpoint_age_min() -> float:
    """Age of checkpoint.json in minutes. Returns large number if missing."""
    try:
        return (time.time() - CHECKPOINT.stat().st_mtime) / 60
    except Exception:
        return 999999.0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--threshold", type=float, default=0.65,
                   help="context %% threshold for refresh (default 0.65)")
    p.add_argument("--max-stale-min", type=float, default=90.0,
                   help="time-based threshold: refresh if carrying OR checkpoint "
                        "older than this many minutes regardless of context %% "
                        "(default 90)")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    if not check_arrival_ack():
        sys.exit(0)

    pct = get_context_pct()
    if pct is None:
        print("no context state, skipping")
        sys.exit(0)

    c_age = carrying_age_min()
    k_age = checkpoint_age_min()
    pct_trigger = (pct >= args.threshold)
    age_trigger = (c_age >= args.max_stale_min or k_age >= args.max_stale_min)

    if not (args.force or pct_trigger or age_trigger):
        print(f"context {pct:.2f} < {args.threshold:.2f} AND carrying "
              f"{c_age:.1f}m AND checkpoint {k_age:.1f}m both < "
              f"{args.max_stale_min:.0f}m, no refresh needed")
        sys.exit(0)
    if age_trigger and not pct_trigger:
        which = []
        if c_age >= args.max_stale_min:
            which.append(f"carrying {c_age:.1f}m")
        if k_age >= args.max_stale_min:
            which.append(f"checkpoint {k_age:.1f}m")
        print(f"time-trigger: {' + '.join(which)} stale "
              f"(context {pct:.2f}, threshold {args.threshold:.2f})")

    trace_data = latest_trace_text()
    if not trace_data:
        print("no traces found")
        sys.exit(0)
    trace_text, trace_name = trace_data

    carrying_result = update_carrying(trace_text, trace_name)
    update_checkpoint(trace_text, trace_name, pct)

    HIST.parent.mkdir(parents=True, exist_ok=True)
    with HIST.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "context_pct": pct,
            "source_trace": trace_name,
            "carrying_skipped_manual": carrying_result is None,
        }) + "\n")
    if carrying_result is None:
        print(f"checkpoint refreshed from {trace_name}; "
              f"carrying preserved (manual + fresh)")
    else:
        print(f"refreshed handoff from {trace_name} at context={pct:.2f}")


if __name__ == "__main__":
    main()
