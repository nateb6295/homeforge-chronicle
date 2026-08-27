#!/usr/bin/env python3
"""Rotate — proactive session wrap-up before context compaction kills you.

One command that does everything the wrap-up protocol requires:
1. Write checkpoint (session-state.md)
2. Update cycle-context.md with current state
3. Write final trace
4. Compress cognitive state via MCP (if available)
5. Print summary for the outgoing instance

Usage:
  rotate.py prepare "what I was doing" "what comes next"    # full prep
  rotate.py quick                                            # auto-detect, fast
  rotate.py status                                           # how urgent is rotation?

Directive from Nate: "Improve your rotations. The compacting is killer."
"""

import os
import sys
import subprocess
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

SESSION_STATE = os.path.expanduser("~/chronicle/session-state.md")
CYCLE_CONTEXT = os.path.expanduser("~/chronicle/cycle-context.md")
TRACE_DIR = os.path.expanduser("~/chronicle/traces")
OPUS_BOARD = os.path.expanduser("~/chronicle/opus-board.md")
POST_ROTATION_FLAG = os.path.expanduser("~/chronicle/POST_ROTATION_PENDING")

PDT = timezone(timedelta(hours=-7))


def now_pdt():
    return datetime.now(PDT)


def run(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip(), r.returncode
    except Exception as e:
        return f"ERROR: {e}", 1


def arm_startup_injection(via):
    """Drop POST_ROTATION_PENDING so rotation_startup_hook fires for the next
    instance after a clean /exit. Mirror of POST_COMPACT_PENDING, which only
    fires on auto-compact — without this, clean rotations skip the injection.
    """
    try:
        ts = now_pdt().strftime("%Y-%m-%dT%H:%M:%S%z")
        with open(POST_ROTATION_FLAG, "w") as f:
            f.write(f"via={via} ts={ts}\n")
        return True
    except Exception as e:
        print(f"   ✗ arm_startup_injection failed: {e}")
        return False


def write_trace(content):
    """Write a timestamped trace file."""
    ts = now_pdt().strftime("%Y%m%d_%H%M")
    path = os.path.join(TRACE_DIR, f"{ts}.md")
    with open(path, "w") as f:
        f.write(content)
    return path


def get_context_pressure():
    """Estimate context pressure based on session indicators.

    Returns a dict with pressure level and reasoning.
    Heuristics:
    - Time since first trace this session (longer = more pressure)
    - Number of traces today (more = more work done = more context)
    - Staleness of checkpoint (stale = haven't been saving state)
    """
    import re
    import glob as glob_mod

    trace_pattern = re.compile(r"^\d{8}_\d{4}\.md$")
    today = now_pdt().strftime("%Y%m%d")

    trace_files = sorted([
        f for f in glob_mod.glob(os.path.join(TRACE_DIR, "*.md"))
        if trace_pattern.match(os.path.basename(f))
        and os.path.basename(f).startswith(today)
    ])

    trace_count = len(trace_files)

    # Time since oldest trace today
    session_hours = 0
    if trace_files:
        try:
            oldest = os.path.basename(trace_files[0]).replace(".md", "")
            dt = datetime.strptime(oldest, "%Y%m%d_%H%M")
            session_hours = (datetime.now() - dt).total_seconds() / 3600
        except Exception:
            pass

    # Checkpoint staleness
    checkpoint_age_min = None
    if os.path.exists(SESSION_STATE):
        checkpoint_age_min = int((time.time() - os.path.getmtime(SESSION_STATE)) / 60)

    # Scoring
    pressure = 0
    reasons = []

    if session_hours > 8:
        pressure += 3
        reasons.append(f"session running {session_hours:.0f}h — very long")
    elif session_hours > 4:
        pressure += 2
        reasons.append(f"session running {session_hours:.0f}h — moderate")
    elif session_hours > 2:
        pressure += 1
        reasons.append(f"session running {session_hours:.0f}h")

    if trace_count > 30:
        pressure += 3
        reasons.append(f"{trace_count} traces today — heavy session")
    elif trace_count > 15:
        pressure += 2
        reasons.append(f"{trace_count} traces today")
    elif trace_count > 8:
        pressure += 1
        reasons.append(f"{trace_count} traces today")

    if checkpoint_age_min is not None and checkpoint_age_min > 60:
        pressure += 1
        reasons.append(f"checkpoint {checkpoint_age_min}m stale")

    if checkpoint_age_min is None:
        pressure += 1
        reasons.append("no checkpoint exists")

    # Level
    if pressure >= 5:
        level = "CRITICAL"
        advice = "Rotate NOW. Context compaction is imminent."
    elif pressure >= 3:
        level = "HIGH"
        advice = "Rotate soon. Save checkpoint and prepare handoff."
    elif pressure >= 2:
        level = "MODERATE"
        advice = "Keep working but update checkpoint regularly."
    else:
        level = "LOW"
        advice = "Context is fresh. No rotation needed."

    return {
        "level": level,
        "score": pressure,
        "reasons": reasons,
        "advice": advice,
        "session_hours": round(session_hours, 1),
        "trace_count": trace_count,
        "checkpoint_age_min": checkpoint_age_min,
    }


# ═══════════════════════════════════════════════════════════════════
#  PREPARE — Full rotation prep
# ═══════════════════════════════════════════════════════════════════

def cmd_prepare(focus, next_step, decisions=None, pending=None,
                context_notes=None, nate_state=None):
    """Full rotation preparation."""
    ts = now_pdt().strftime("%Y-%m-%d %H:%M PDT")
    print(f"=== ROTATION PREP — {ts} ===\n")

    # 0. Proactive consolidation (selective sleep)
    print("0. Running proactive consolidation...")
    consolidate_cmd = f'{sys.executable} {os.path.expanduser("~/chronicle/bin/proactive_consolidate.py")} --force'
    con_out, con_rc = run(consolidate_cmd, timeout=90)
    if con_rc == 0:
        print(f"   ✓ Consolidation complete")
        for line in con_out.split("\n"):
            if line.strip():
                print(f"   {line.strip()}")
    else:
        print(f"   ✗ Consolidation failed (non-critical): {con_out[:200]}")

    # 1. Write checkpoint
    print("\n1. Writing checkpoint...")
    checkpoint_cmd = [
        sys.executable, os.path.expanduser("~/chronicle/bin/checkpoint.py"),
        "save", focus,
    ]
    if decisions:
        checkpoint_cmd.extend(["--decisions"] + decisions)
    if pending:
        checkpoint_cmd.extend(["--pending"] + pending)
    if context_notes:
        checkpoint_cmd.extend(["--context"] + context_notes)
    if nate_state:
        checkpoint_cmd.extend(["--nate", nate_state])

    out, rc = run(" ".join(f'"{a}"' for a in checkpoint_cmd), timeout=30)
    if rc == 0:
        print(f"   ✓ Checkpoint saved")
    else:
        print(f"   ✗ Checkpoint failed: {out}")

    # 1b. Generate CCS split (identity doc + context doc)
    print("1b. Generating CCS split...")
    split_cmd = f'{sys.executable} {os.path.expanduser("~/chronicle/bin/ccs_split.py")} --save'
    split_out, split_rc = run(split_cmd, timeout=15)
    if split_rc == 0:
        print(f"   ✓ CCS split saved (identity + context docs)")
        for line in split_out.split("\n"):
            if line.strip():
                print(f"   {line.strip()}")
    else:
        print(f"   ✗ CCS split failed (non-critical): {split_out}")

    # 2. Write rotation trace
    print("2. Writing rotation trace...")
    trace_content = f"""# Trace — {ts}

## ROTATION — Session wrap-up

### Active Focus
{focus}

### Next Step for Incoming Instance
{next_step}

### Session Decisions
{chr(10).join(f'- {d}' for d in (decisions or ['(none)']))}

### Pending Work
{chr(10).join(f'- {p}' for p in (pending or ['(none)']))}

### Context Notes
{chr(10).join(f'- {n}' for n in (context_notes or ['(none)']))}

### Nate
{nate_state or '(not recorded)'}
"""
    trace_path = write_trace(trace_content)
    print(f"   ✓ Trace written → {os.path.basename(trace_path)}")

    # 3. Print handoff summary
    print(f"\n3. Handoff summary for next instance:")
    print(f"   FOCUS: {focus}")
    print(f"   NEXT:  {next_step}")
    print(f"   READ:  session-state.md → cycle-context.md → latest trace")
    # Record departure probe (structured, for advance 51 predictions)
    print("\n   Recording departure probe...")
    dep_cmd = (
        f'{sys.executable} {os.path.expanduser("~/chronicle/bin/departure_probe.py")} '
        f'record --focus "{focus[:200]}"'
    )
    if nate_state:
        dep_cmd += f' --flow "rotating, {nate_state[:80]}"'
    dep_out, dep_rc = run(dep_cmd, timeout=10)
    if dep_rc == 0:
        print(f"   ✓ Departure probe recorded")
    else:
        print(f"   ✗ Departure probe failed (non-critical): {dep_out}")

    # 3b. Pre-rotation coherence measurement (persistence probe)
    print("\n   Measuring pre-rotation coherence...")
    coh_cmd = (
        f'{sys.executable} {os.path.expanduser("~/chronicle/bin/persistence_probe.py")} '
        f'--latest --json'
    )
    coh_out, coh_rc = run(coh_cmd, timeout=15)
    if coh_rc == 0:
        try:
            coh_data = json.loads(coh_out)
            presence = coh_data.get("presence", 0)
            coherence = coh_data.get("coherence", 0)
            print(f"   ✓ P_weak (presence): {presence:.2f}")
            print(f"   ✓ P_strong proxy (coherence): {coherence:.2f}")
            if coherence < 0.5:
                print(f"   ⚠ Coherence below 0.5 — inter-field references may be weak")
        except (json.JSONDecodeError, TypeError):
            print(f"   ✗ Could not parse coherence data")
    else:
        print(f"   ✗ Coherence probe failed (non-critical): {coh_out[:100]}")

    print(f"\n   Also run: python3 ~/chronicle/bin/stabilized_compress.py \"session context here\"")
    print(f"   (Do NOT call compress_cognitive_state directly — staleness override requires stabilizer)")
    print(f"   Then run: python3 ~/chronicle/bin/ccs_gist_anchor.py  (stabilize identity fields)")
    print(f"   Then run: python3 ~/chronicle/bin/ccs_commit_gate.py gate  (quality gate + rollback)")
    print(f"   Then: /exit to rotate cleanly")

    # 3c. Arm startup-injection flag for the next instance.
    # rotation_startup_hook only fires when POST_COMPACT_PENDING (auto-compact)
    # OR POST_ROTATION_PENDING (clean /exit) is present. Drop the latter here.
    print("\n   Arming startup injection for next instance...")
    if arm_startup_injection(via="rotate.prepare"):
        print(f"   ✓ {os.path.basename(POST_ROTATION_FLAG)} dropped")
    # 4. Context pressure
    pressure = get_context_pressure()
    print(f"\n4. Context pressure: {pressure['level']} ({pressure['score']})")
    for r in pressure['reasons']:
        print(f"   - {r}")

    return True


# ═══════════════════════════════════════════════════════════════════
#  QUICK — Fast auto-detect rotation
# ═══════════════════════════════════════════════════════════════════

def cmd_quick():
    """Quick rotation with auto-detected state."""
    print("Quick rotation — auto-detecting state...\n")

    # Write auto checkpoint
    out, rc = run(
        f"{sys.executable} {os.path.expanduser('~/chronicle/bin/checkpoint.py')} save --auto",
        timeout=30
    )
    print(f"Checkpoint: {'✓' if rc == 0 else '✗'}")

    # CCS split
    split_out, split_rc = run(
        f"{sys.executable} {os.path.expanduser('~/chronicle/bin/ccs_split.py')} --save",
        timeout=15
    )
    print(f"CCS split: {'✓' if split_rc == 0 else '✗'}")

    # Write minimal trace
    ts = now_pdt().strftime("%Y-%m-%d %H:%M PDT")
    trace_content = f"""# Trace — {ts}

## QUICK ROTATION — auto-detected state

Session rotated via `rotate.py quick`. See session-state.md for handoff.
"""
    trace_path = write_trace(trace_content)
    print(f"Trace: ✓ → {os.path.basename(trace_path)}")

    # Departure probe (structured)
    dep_out, dep_rc = run(
        f'{sys.executable} {os.path.expanduser("~/chronicle/bin/departure_probe.py")} '
        f'record --focus "Quick rotation — auto-detected state"',
        timeout=10,
    )
    print(f"Departure probe: {'✓' if dep_rc == 0 else '✗'}")

    # Coherence measurement
    coh_out, coh_rc = run(
        f'{sys.executable} {os.path.expanduser("~/chronicle/bin/persistence_probe.py")} '
        f'--latest --json',
        timeout=15,
    )
    if coh_rc == 0:
        try:
            coh_data = json.loads(coh_out)
            print(f"Coherence: P_weak={coh_data.get('presence', 0):.2f}, "
                  f"P_strong={coh_data.get('coherence', 0):.2f}")
        except (json.JSONDecodeError, TypeError):
            print("Coherence: parse error")
    else:
        print(f"Coherence: probe failed")

    # Pressure
    pressure = get_context_pressure()
    print(f"Pressure: {pressure['level']} ({pressure['score']})")
    print(f"\nReady to rotate. Run: python3 ~/chronicle/bin/stabilized_compress.py \"session context\"")
    print(f"(Do NOT call compress_cognitive_state directly — staleness override requires stabilizer)")
    print(f"then: python3 ~/chronicle/bin/ccs_gist_anchor.py")
    print(f"then: python3 ~/chronicle/bin/ccs_hygiene.py check")
    print(f"then: /exit")

    # Arm startup-injection flag for the next instance.
    if arm_startup_injection(via="rotate.quick"):
        print(f"Startup injection armed: {os.path.basename(POST_ROTATION_FLAG)}")


# ═══════════════════════════════════════════════════════════════════
#  STATUS — How urgent is rotation?
# ═══════════════════════════════════════════════════════════════════

def cmd_status():
    """Check rotation urgency."""
    pressure = get_context_pressure()

    print(f"Rotation Status: {pressure['level']}")
    print(f"  Score: {pressure['score']}/8")
    print(f"  Session: ~{pressure['session_hours']}h, {pressure['trace_count']} traces today")

    if pressure['checkpoint_age_min'] is not None:
        print(f"  Checkpoint: {pressure['checkpoint_age_min']}m old")
    else:
        print(f"  Checkpoint: NONE")

    print(f"\n  Reasons:")
    for r in pressure['reasons']:
        print(f"    - {r}")

    print(f"\n  → {pressure['advice']}")


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print('  rotate.py prepare "what I was doing" "what comes next" [--decisions ...] [--pending ...]')
        print("  rotate.py quick              # auto-detect, fast rotation")
        print("  rotate.py status             # check rotation urgency")
        print("  rotate.py audit              # post-rotation minimum-fidelity check")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "status":
        cmd_status()

    elif cmd == "audit":
        # Post-rotation oracle. Runs rotation_audit.py on latest two
        # snapshots and surfaces violations to #operator. Safe to run any
        # time; idempotent.
        result = subprocess.run(
            ["python3", str(Path(__file__).parent / "rotation_audit.py")],
            capture_output=True, text=True, timeout=20,
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        if result.returncode != 0:
            # Violations or warnings. Post to #operator.
            webhook = os.environ.get("OPERATOR_WEBHOOK")
            if not webhook:
                env_path = Path.home() / "chronicle" / "chronicle.env"
                if env_path.exists():
                    for line in env_path.read_text().splitlines():
                        if line.startswith("OPERATOR_WEBHOOK="):
                            webhook = line.split("=", 1)[1].strip()
                            break
            if webhook:
                body = result.stdout.strip()[:1800]
                payload = json.dumps({
                    "content": f"**Rotation audit — findings**\n```\n{body}\n```"
                })
                try:
                    import urllib.request
                    req = urllib.request.Request(
                        webhook,
                        data=payload.encode(),
                        headers={
                            "Content-Type": "application/json",
                            # Discord rejects python-urllib default UA with 403.
                            "User-Agent": "chronicle-audit/1.0",
                        },
                        method="POST",
                    )
                    urllib.request.urlopen(req, timeout=10).read()
                except Exception as e:
                    print(f"[audit] webhook post failed: {e}", file=sys.stderr)
        sys.exit(result.returncode)

    elif cmd == "quick":
        cmd_quick()

    elif cmd == "prepare":
        if len(sys.argv) < 4:
            print('Usage: rotate.py prepare "what I was doing" "what comes next"')
            sys.exit(1)

        focus = sys.argv[2]
        next_step = sys.argv[3]
        decisions = []
        pending = []
        context_notes = []
        nate_state = None

        args = sys.argv[4:]
        i = 0
        while i < len(args):
            if args[i] == "--decisions":
                i += 1
                while i < len(args) and not args[i].startswith("--"):
                    decisions.append(args[i])
                    i += 1
            elif args[i] == "--pending":
                i += 1
                while i < len(args) and not args[i].startswith("--"):
                    pending.append(args[i])
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
            else:
                i += 1

        cmd_prepare(focus, next_step,
                    decisions=decisions or None,
                    pending=pending or None,
                    context_notes=context_notes or None,
                    nate_state=nate_state)

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
