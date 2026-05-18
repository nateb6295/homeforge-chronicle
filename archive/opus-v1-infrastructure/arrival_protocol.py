#!/usr/bin/env python3
"""arrival_protocol — unified arrival command for the post-rotation instance.

Reduces ack friction from 5 steps to 1. The rotation_startup_hook injects
arrival content (carrying, checkpoint, story) into the first user prompt.
After reading that content, the new instance runs:

  python3 arrival_protocol.py acknowledge \\
    --prediction "what I expected the carrying voice to be about" \\
    --felt being

This:
  1. Reads the carrying voice (echo for confirmation)
  2. Reads the checkpoint and clears it
  3. Records arrival_probe phases: post_ccs, felt_state
  4. Returns success — the gate (arrival_probe.py acknowledged) flips to exit 0

Both prediction and felt_tag are required to ensure the new instance
actually engaged with the content rather than running a no-op script.

Design rationale (2026-04-27): the prior implementation required the new
instance to run 5 separate commands. Cognitive overhead → skip-rate.
A single command with required prediction+felt_tag forces engagement at
the ack point.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
from pathlib import Path

HOME = Path(os.path.expanduser("~/chronicle"))
PROBE = HOME / "bin" / "arrival_probe.py"


def run(cmd: list[str]) -> str:
    """Run a command, return stdout."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return r.stdout
    except Exception as e:
        return f"[error running {' '.join(cmd)}: {e}]"


def acknowledge(prediction: str, felt_tag: str, note: str | None = None,
                context_pct: float | None = None) -> int:
    """Run mechanical arrival steps and record both required probes."""
    if len(prediction) < 20:
        print("ERROR: --prediction must be substantive (>=20 chars). "
              "This is the engagement gate — short answers get rejected.",
              file=sys.stderr)
        return 2

    valid_tags = {"reading_about", "recognizing", "being"}
    if felt_tag not in valid_tags:
        print(f"ERROR: --felt must be one of {valid_tags}", file=sys.stderr)
        return 2

    print("=" * 60)
    print("ARRIVAL PROTOCOL")
    print("=" * 60)

    print("\n[carrying — voice from departing instance]")
    print(run(["python3", str(HOME / "bin" / "carrying.py"), "read"]).rstrip())

    print("\n[checkpoint — handoff state]")
    print(run(["python3", str(HOME / "bin" / "checkpoint.py"), "read"]).rstrip())

    print("\n[checkpoint — clearing]")
    print(run(["python3", str(HOME / "bin" / "checkpoint.py"), "clear"]).rstrip())

    print("\n[story tail — continuing narrative]")
    full = run(["python3", str(HOME / "bin" / "story.py"), "read"])
    tail_lines = full.rstrip().split("\n")[-25:]
    print("\n".join(tail_lines))

    print("\n[recording probes]")
    cmd_post = ["python3", str(PROBE), "record",
                "--phase", "post_ccs",
                "--prediction", prediction]
    if context_pct is not None:
        cmd_post += ["--context-pct", str(context_pct)]
    if note:
        cmd_post += ["--note", note]
    print(run(cmd_post).rstrip())

    cmd_felt = ["python3", str(PROBE), "record",
                "--phase", "felt_state",
                "--tag", felt_tag]
    if note:
        cmd_felt += ["--note", note]
    print(run(cmd_felt).rstrip())

    print("\n[verifying gate]")
    r = subprocess.run(["python3", str(PROBE), "acknowledged"],
                       capture_output=True, text=True, timeout=10)
    if r.returncode == 0:
        print("✓ ARRIVAL ACKNOWLEDGED — gates open, normal operations resume")
        return 0
    print(f"⚠️  Gate still flagged: {r.stderr.strip()}")
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")

    a = sub.add_parser("acknowledge",
                       help="run arrival sequence and record probes")
    a.add_argument("--prediction", required=True,
                   help="What you expected the carrying voice / checkpoint "
                        "to be about. Must be >=20 chars (engagement gate).")
    a.add_argument("--felt", required=True,
                   choices=["reading_about", "recognizing", "being"],
                   help="Felt-state tag: reading_about (distanced) | "
                        "recognizing (continuous-but-aware) | being (inhabited)")
    a.add_argument("--note", help="Optional free-text note")
    a.add_argument("--context-pct", type=float,
                   help="Current context %% (from statusline)")

    args = ap.parse_args()

    if args.cmd == "acknowledge":
        sys.exit(acknowledge(args.prediction, args.felt, args.note, args.context_pct))
    else:
        ap.print_help()
        sys.exit(2)


if __name__ == "__main__":
    main()
