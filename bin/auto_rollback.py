#!/usr/bin/env python3
"""
Auto-rollback: when homeostasis composite flips red, post to #operator with
specific recommended actions instead of silently logging and waiting.

Called by the homeostasis cron if composite status goes yellow or red.
Not called on green → green (quiet).

Sends a single Discord post:
  - Status + which components red/yellow
  - Likely upstream cause (based on which component failed)
  - Concrete recommended action
  - Link to the diagnostic runbook

Part of the resilience stack so that Opus doesn't need Nate to interpret
the score and decide what to do about it.

Usage:
  python3 auto_rollback.py         # decide + post based on current state
  python3 auto_rollback.py --dry   # show what would be posted, don't post
"""
import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from homeostasis import compute  # noqa

HIST = Path.home() / "chronicle" / "data" / "homeostasis_history.jsonl"


def get_prev_status():
    if not HIST.exists():
        return None
    lines = HIST.read_text().splitlines()
    if len(lines) < 2:
        return None
    # last line is current; second-to-last is previous
    try:
        prev = json.loads(lines[-2])
        return prev.get("composite_status")
    except Exception:
        return None


def recommendation_for(red_yellow_components):
    """Given components in red/yellow, recommend an action."""
    recs = []
    m = {
        "uncertainty_flow":
            "Compressor isn't getting fresh open-questions in session "
            "summaries. Run stabilized_compress with an explicit OPEN "
            "QUESTIONS section.",
        "gist_freeze":
            "Gist hasn't evolved across last 5 snapshots. Either compression "
            "is stuck or genuinely nothing has shifted; review recent traces.",
        "entity_retention":
            "Focal_entities churning too fast or too slow. Check entity_guard "
            "tier quotas; look at which entities dropped vs persisted.",
        "constraint_stability":
            "Constraints churning. Either new directives flipped them (OK) "
            "or compression is drifting them (not OK). Review directives vs "
            "compression log.",
        "field_volatility":
            "Fields are stagnant or thrashing. Compare most-recent snapshot "
            "to 3-back; if identical, compression isn't firing; if totally "
            "different, something is corrupting state.",
        "predictive_calibration":
            "LLM-judge scoring prior cue→trace alignment low. Either cues "
            "are vague OR the trace isn't matching predicted direction. "
            "Pull most recent predictive_cue and compare to recent traces.",
    }
    for name, status in red_yellow_components:
        if name in m:
            recs.append(f"- {name} ({status}): {m[name]}")
    if not recs:
        recs.append("- composite degraded without single-component signal; full audit recommended")
    return "\n".join(recs)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry", action="store_true")
    args = p.parse_args()

    # Read the current state (without re-running, use last history entry)
    if not HIST.exists():
        print("no homeostasis history yet; run homeostasis.py first")
        sys.exit(0)
    lines = HIST.read_text().splitlines()
    if not lines:
        print("empty history")
        sys.exit(0)
    cur = json.loads(lines[-1])
    status = cur.get("composite_status", "unknown")
    composite = cur.get("composite_fitness")

    if status == "green":
        print("status=green, no rollback needed")
        sys.exit(0)

    # Collect red/yellow components
    red_yellow = []
    for name, c in cur.get("components", {}).items():
        if c.get("status") in ("red", "yellow"):
            red_yellow.append((name, c["status"]))

    prev = get_prev_status()
    # Flip detection: only post if flipped (not continuously red)
    if prev == status:
        print(f"status={status}, same as previous; skipping post")
        sys.exit(0)

    body = (
        f"**🔴 Homeostasis flipped {prev or '?'} → {status.upper()}**\n\n"
        f"Composite fitness: {composite:.3f}\n\n"
        f"Affected components:\n"
        f"{recommendation_for(red_yellow)}\n\n"
        f"Auto-rollback actions taken: pausing any new ship-work flagged at "
        f"CONSIDER+DECIDE tier until composite returns to green. Still "
        f"executing routine DECIDE-tier work and direct responses to Nate."
    )

    if args.dry:
        print("=== DRY RUN ===")
        print(body)
        sys.exit(0)

    # Load env + post
    env_path = Path.home() / "chronicle" / "chronicle.env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            v = v.strip().strip('"').strip("'")
            if k.strip() and k.strip() not in os.environ:
                os.environ[k.strip()] = v
    webhook = os.environ.get("OPERATOR_WEBHOOK")
    if not webhook:
        print("ERROR: OPERATOR_WEBHOOK not set")
        sys.exit(1)
    req = urllib.request.Request(
        webhook,
        data=json.dumps({"content": body[:2000]}).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "chronicle-auto-rollback/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"posted (HTTP {resp.status})")
    except Exception as e:
        print(f"post failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
