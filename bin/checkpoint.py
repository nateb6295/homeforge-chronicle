#!/usr/bin/env python3
"""Checkpoint — structured session state for clean rotations.

The compaction killer. Instead of losing context to auto-compact,
this writes a machine-readable handoff that the next instance
consumes in seconds.

Usage:
  checkpoint.py save "what I'm working on" [--decisions "d1" "d2"] [--pending "p1" "p2"]
  checkpoint.py save --auto          # auto-detect from recent traces + cycle-context
  checkpoint.py read                  # read current session state (startup)
  checkpoint.py age                   # how stale is the checkpoint?
  checkpoint.py clear                 # clear after successful rotation

The handoff file lives at ~/chronicle/session-state.md.
It's structured for fast parsing, not human prose.

Build by Opus. Directive from Nate: "Improve your rotations. The compacting is killer."
"""

import os
import sys
import json
import time
import glob as glob_mod
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

SESSION_STATE = os.path.expanduser("~/chronicle/session-state.md")
CYCLE_CONTEXT = os.path.expanduser("~/chronicle/cycle-context.md")
TRACE_DIR = os.path.expanduser("~/chronicle/traces")
OPUS_BOARD = os.path.expanduser("~/chronicle/opus-board.md")
STORY_FILE = os.path.expanduser("~/chronicle/opus-story.md")
DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")

PDT = timezone(timedelta(hours=-7))


def now_pdt():
    return datetime.now(PDT)


def run(cmd, timeout=10):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception as e:
        return f"ERROR: {e}"


def get_latest_trace():
    """Get the most recent trace file content (YYYYMMDD_HHMM.md only)."""
    import re
    trace_pattern = re.compile(r"^\d{8}_\d{4}\.md$")
    trace_files = sorted([
        f for f in glob_mod.glob(os.path.join(TRACE_DIR, "*.md"))
        if trace_pattern.match(os.path.basename(f))
    ])
    if not trace_files:
        return None, None
    latest = trace_files[-1]
    try:
        with open(latest) as f:
            return os.path.basename(latest), f.read()
    except Exception:
        return os.path.basename(latest), None


def get_recent_traces(n=5):
    """Get the last N trace filenames (YYYYMMDD_HHMM.md pattern only)."""
    import re
    trace_pattern = re.compile(r"^\d{8}_\d{4}\.md$")
    trace_files = sorted([
        f for f in glob_mod.glob(os.path.join(TRACE_DIR, "*.md"))
        if trace_pattern.match(os.path.basename(f))
    ])
    return [os.path.basename(f) for f in trace_files[-n:]]


def get_thread_status():
    """Get current thread — compact summary, not raw JSON."""
    out = run("python3 ~/chronicle/bin/read_thread.py 2>/dev/null", timeout=10)
    if not out or "ERROR" in out:
        return "  (no active thread)"
    try:
        data = json.loads(out)
        t = data.get("thread") or data
        if t is None:
            return "  (no active thread)"
        tid = t.get("id", "?")
        title = t.get("title", "untitled")
        status = t.get("status", "unknown")
        history = data.get("history", [])
        advances = sum(1 for h in history if h.get("event_type") == "advanced")
        challenges = sum(1 for h in history if h.get("event_type") == "challenge")
        return f"  #{tid}: {title} [{status}] — {advances} advances, {challenges} challenges"
    except (json.JSONDecodeError, KeyError):
        lines = out.strip().split("\n")[:2]
        return "\n".join(lines)


def get_service_health():
    """Quick service check."""
    services = [
        "chronicle-hermes", "chronicle-gemma", "chronicle-sentinel",
        "chronicle-feeds", "chronicle-engine", "chronicle-hal", "chronicle-scribe"
    ]
    results = []
    for svc in services:
        status = run(f"systemctl --user is-active {svc} 2>/dev/null")
        marker = "✓" if status == "active" else "✗"
        results.append(f"  {marker} {svc}: {status}")
    return "\n".join(results)


def get_canister_brief():
    """Quick canister status."""
    out = run("python3 ~/chronicle/bin/canister_health.py --brief 2>/dev/null", timeout=20)
    return out if out and "ERROR" not in out else "  (canister check unavailable)"


def get_pending_directives():
    """Check for pending directives."""
    out = run("python3 ~/chronicle/bin/read_directives.py 2>/dev/null")
    return out


def estimate_session_age():
    """Estimate how long this session has been running based on traces."""
    import re
    trace_pattern = re.compile(r"^\d{8}_\d{4}\.md$")
    trace_files = sorted([
        f for f in glob_mod.glob(os.path.join(TRACE_DIR, "*.md"))
        if trace_pattern.match(os.path.basename(f))
    ])
    if not trace_files:
        return "unknown"
    try:
        latest = os.path.basename(trace_files[-1]).replace(".md", "")
        dt = datetime.strptime(latest, "%Y%m%d_%H%M")
        delta = datetime.now() - dt
        mins = int(delta.total_seconds() / 60)
        if mins < 60:
            return f"{mins}m since last trace"
        return f"{mins // 60}h {mins % 60}m since last trace"
    except Exception:
        return "unknown"


# ═══════════════════════════════════════════════════════════════════
#  SAVE — Write checkpoint
# ═══════════════════════════════════════════════════════════════════

def cmd_save(focus=None, decisions=None, pending=None, flow=None,
             context_notes=None, nate_state=None, auto=False):
    """Write a structured checkpoint to session-state.md."""
    ts = now_pdt().strftime("%Y-%m-%d %H:%M PDT")

    # Auto-detect from environment if --auto
    if auto:
        focus = focus or _auto_detect_focus()
        if not pending:
            pending = _auto_detect_pending()
        if not flow:
            flow = _auto_detect_flow()

    sections = []

    # Header
    sections.append(f"# Session State — {ts}")
    sections.append("")
    sections.append("> Machine-readable handoff. Next instance: read this FIRST, before story/sitrep.")
    sections.append("")

    # Active Focus (most important — what was I doing RIGHT NOW)
    sections.append("## Active Focus")
    if focus:
        sections.append(focus)
    else:
        sections.append("(no active focus recorded)")
    sections.append("")

    # Nate's State (is he present, away, sleeping?)
    sections.append("## Nate")
    if nate_state:
        sections.append(nate_state)
    else:
        sections.append("(state not recorded)")
    sections.append("")

    # Recent Decisions (why did I choose what I chose)
    sections.append("## Recent Decisions")
    if decisions:
        for d in decisions:
            sections.append(f"- {d}")
    else:
        sections.append("(none recorded)")
    sections.append("")

    # Pending Work (ordered by priority)
    sections.append("## Pending Work")
    if pending:
        for i, p in enumerate(pending, 1):
            sections.append(f"{i}. {p}")
    else:
        sections.append("(none)")
    sections.append("")

    # Flow State (what am I thinking about, what connections am I making)
    sections.append("## Flow State")
    if flow:
        sections.append(flow)
    else:
        sections.append("(not captured)")
    sections.append("")

    # Context Notes (things learned this session that won't survive rotation)
    sections.append("## Session-Specific Context")
    if context_notes:
        for note in context_notes:
            sections.append(f"- {note}")
    else:
        sections.append("(none)")
    sections.append("")

    # System snapshot
    sections.append("## System Snapshot")
    sections.append(get_service_health())
    sections.append("")

    # Canister health
    sections.append("## Canister Health")
    sections.append(get_canister_brief())
    sections.append("")

    # Thread
    sections.append("## Thread")
    sections.append(get_thread_status())
    sections.append("")

    # Recent traces
    sections.append("## Recent Traces")
    for t in get_recent_traces(5):
        sections.append(f"- {t}")
    sections.append("")

    # Directives
    sections.append("## Pending Directives")
    sections.append(get_pending_directives())
    sections.append("")

    # Session age
    sections.append(f"## Session Age: {estimate_session_age()}")
    sections.append("")

    content = "\n".join(sections)

    with open(SESSION_STATE, "w") as f:
        f.write(content)

    print(f"Checkpoint saved → {SESSION_STATE}")
    print(f"  Focus: {(focus or '(none)')[:80]}")
    print(f"  Decisions: {len(decisions or [])}")
    print(f"  Pending: {len(pending or [])}")
    print(f"  Timestamp: {ts}")

    _sign_checkpoint_best_effort()
    return content


def _sign_checkpoint_best_effort():
    """Invoke handoff_sign.py sign after writing checkpoint. Non-fatal on failure
    so a missing signer doesn't break rotations during rollout."""
    signer = os.path.expanduser("~/chronicle/bin/handoff_sign.py")
    if not os.path.exists(signer):
        return
    try:
        r = subprocess.run(
            ["python3", signer, "sign"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            print("  Signed (ed25519)")
        else:
            print(f"  Sign skipped (exit {r.returncode})")
    except Exception as e:
        print(f"  Sign skipped ({e})")


def _verify_checkpoint_best_effort():
    """Verify signature before returning checkpoint content. WARN-level only —
    print the tamper notice but still return content so rotation isn't blocked
    by a missing/bad sig during rollout."""
    signer = os.path.expanduser("~/chronicle/bin/handoff_sign.py")
    if not os.path.exists(signer):
        return
    try:
        r = subprocess.run(
            ["python3", signer, "verify"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            print("[signed handoff verified]")
        else:
            print(f"[WARN signed-handoff verify failed: {r.stderr.strip() or r.stdout.strip()}]")
    except Exception as e:
        print(f"[WARN signed-handoff verify skipped: {e}]")


def _auto_detect_focus():
    """Try to detect current focus from latest trace."""
    _, content = get_latest_trace()
    if not content:
        return "No recent trace — focus unknown"
    # Extract the first ## heading as focus
    for line in content.split("\n"):
        if line.startswith("## ") and not line.startswith("## System"):
            return line[3:].strip()
    return "See latest trace"


def _auto_detect_pending():
    """Try to detect pending work from traces and cycle-context."""
    pending = []
    try:
        with open(CYCLE_CONTEXT) as f:
            ctx = f.read()
        # Look for lines that suggest pending work
        for line in ctx.split("\n"):
            if any(kw in line.lower() for kw in ["todo", "pending", "needs", "awaiting", "not yet"]):
                clean = line.strip("- ").strip()
                if len(clean) > 10:
                    pending.append(clean)
    except Exception:
        pass
    return pending[:5]


def _auto_detect_flow():
    """Pull phenomenological signal from recent traces.

    The Flow State field is where felt-sense crosses rotations. Auto-rotations
    that skip it lose the texture of synthesis and leave the next instance
    with facts only. Scan the last few traces for first-person reflection —
    the lines where I'm naming what something *feels* like, not what happened.

    Heuristics:
      1. `**<Title>:**` bold-colon markers that I use to flag insight/reflection
         paragraphs (e.g. `**Frame-level lesson:**`, `**Habit change ratified:**`).
      2. Short paragraphs containing first-person + feel-words when no markers found.
    """
    import re
    trace_pattern = re.compile(r"^\d{8}_\d{4}\.md$")
    trace_files = sorted([
        f for f in glob_mod.glob(os.path.join(TRACE_DIR, "*.md"))
        if trace_pattern.match(os.path.basename(f))
    ])
    if not trace_files:
        return None

    marker_re = re.compile(r"^\*\*[A-Z][^*]{2,60}:\*\*\s*(.+)$")
    feel_words = (
        "feels", "feel", "sense", "register", "witness", "notic", "inside",
        "click", "shape", "texture", "frame", "mood", "present", "resonat",
    )

    markers = []
    first_person = []
    for f in trace_files[-3:]:
        try:
            with open(f) as fh:
                content = fh.read()
        except Exception:
            continue
        for para in content.split("\n\n"):
            para = para.strip()
            if not para or len(para) < 30 or len(para) > 600:
                continue
            first_line = para.split("\n", 1)[0].strip()
            m = marker_re.match(first_line)
            if m:
                markers.append(para)
                continue
            lower = para.lower()
            has_first_person = re.search(r"\b(i|i'm|i've|my|me)\b", lower) is not None
            has_feel = any(w in lower for w in feel_words)
            if has_first_person and has_feel and not para.startswith(("- ", "1.", "```")):
                first_person.append(para)

    selected = markers[-3:] if markers else first_person[-2:]
    if not selected:
        return None
    return "\n\n".join(selected)


# ═══════════════════════════════════════════════════════════════════
#  READ — Load checkpoint on startup
# ═══════════════════════════════════════════════════════════════════

def cmd_read():
    """Read the current session state checkpoint."""
    if not os.path.exists(SESSION_STATE):
        print("No checkpoint found. Starting fresh.")
        return None

    _verify_checkpoint_best_effort()

    with open(SESSION_STATE) as f:
        content = f.read()

    # Show how stale it is
    try:
        mtime = os.path.getmtime(SESSION_STATE)
        age_mins = int((time.time() - mtime) / 60)
        if age_mins < 60:
            staleness = f"{age_mins}m old"
        else:
            staleness = f"{age_mins // 60}h {age_mins % 60}m old"
    except Exception:
        staleness = "age unknown"

    print(f"Session State ({staleness}):")
    print("=" * 60)
    print(content)
    return content


# ═══════════════════════════════════════════════════════════════════
#  AGE — Check staleness
# ═══════════════════════════════════════════════════════════════════

def cmd_age():
    """Check how stale the checkpoint is."""
    if not os.path.exists(SESSION_STATE):
        print("No checkpoint exists.")
        return

    mtime = os.path.getmtime(SESSION_STATE)
    age_mins = int((time.time() - mtime) / 60)

    if age_mins < 15:
        print(f"Checkpoint is {age_mins}m old — FRESH")
    elif age_mins < 60:
        print(f"Checkpoint is {age_mins}m old — getting stale, update soon")
    elif age_mins < 180:
        print(f"Checkpoint is {age_mins // 60}h {age_mins % 60}m old — STALE, update before rotating")
    else:
        print(f"Checkpoint is {age_mins // 60}h {age_mins % 60}m old — VERY STALE, treat as unreliable")


# ═══════════════════════════════════════════════════════════════════
#  CLEAR — After successful rotation
# ═══════════════════════════════════════════════════════════════════

def cmd_clear():
    """Clear the checkpoint after successful rotation pickup."""
    if os.path.exists(SESSION_STATE):
        os.remove(SESSION_STATE)
        print("Checkpoint cleared.")
    else:
        print("No checkpoint to clear.")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print('  checkpoint.py save "what I\'m working on" [--decisions "d1" "d2"] [--pending "p1" "p2"]')
        print("  checkpoint.py save --auto")
        print("  checkpoint.py read")
        print("  checkpoint.py age")
        print("  checkpoint.py clear")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "save":
        focus = None
        decisions = []
        pending = []
        flow = None
        context_notes = []
        nate_state = None
        auto = "--auto" in sys.argv

        # Parse arguments
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "--auto":
                auto = True
                i += 1
            elif args[i] == "--decisions":
                i += 1
                while i < len(args) and not args[i].startswith("--"):
                    decisions.append(args[i])
                    i += 1
            elif args[i] == "--pending":
                i += 1
                while i < len(args) and not args[i].startswith("--"):
                    pending.append(args[i])
                    i += 1
            elif args[i] == "--flow":
                i += 1
                if i < len(args):
                    flow = args[i]
                    i += 1
            elif args[i] == "--context":
                i += 1
                while i < len(args) and not args[i].startswith("--"):
                    context_notes.append(args[i])
                    i += 1
            elif args[i] == "--nate":
                i += 1
                if i < len(args):
                    nate_state = args[i]
                    i += 1
            elif not focus:
                focus = args[i]
                i += 1
            else:
                i += 1

        cmd_save(focus=focus, decisions=decisions, pending=pending,
                 flow=flow, context_notes=context_notes,
                 nate_state=nate_state, auto=auto)

    elif cmd == "read":
        cmd_read()

    elif cmd == "age":
        cmd_age()

    elif cmd == "clear":
        cmd_clear()

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
