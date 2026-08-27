#!/usr/bin/env python3
"""Discovery — the walker's signal.

When the wheel spins and I walk the results, most are noise. When something
genuinely connects, this is how it finds Nate. Not a report. Not a digest.
A tap on the shoulder.

Usage:
  discovery.py "entity_a" "entity_b" "one sentence connection" ["falsification"]
  discovery.py --from-wheel  # walk latest wheel results interactively

The signal is the selection. The wheel proposes thousands. I walk dozens.
This fires for one.
"""
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from discord_post import post

JOURNAL = Path.home() / "chronicle" / "data" / "discovery_journal.jsonl"
WHEEL_RESULTS = Path.home() / "chronicle" / "data" / "bruno_wheel.json"


def format_discovery(a: str, b: str, connection: str, falsification: str = "") -> str:
    lines = [
        "**⚡ DISCOVERY**",
        "",
        f"**[{a}]** ↔ **[{b}]**",
        "",
        connection,
    ]
    if falsification:
        lines.append("")
        lines.append(f"*Testable: {falsification}*")
    return "\n".join(lines)


def log_discovery(a: str, b: str, connection: str, falsification: str = "") -> None:
    entry = {
        "timestamp": int(time.time()),
        "pair": [a, b],
        "connection": connection,
        "falsification": falsification or None,
        "status": "PROPOSED",
    }
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    with open(JOURNAL, "a") as f:
        f.write(json.dumps(entry) + "\n")


def send_discovery(a: str, b: str, connection: str, falsification: str = "") -> dict:
    msg = format_discovery(a, b, connection, falsification)
    log_discovery(a, b, connection, falsification)
    return post(msg, channel="operator")


def walk_wheel() -> None:
    """Walk the latest wheel results. Print candidates for manual selection."""
    if not WHEEL_RESULTS.is_file():
        print("No wheel results to walk.")
        return

    data = json.loads(WHEEL_RESULTS.read_text())
    candidates = [r for r in data.get("results", []) if r.get("status") == "CANDIDATE"]

    if not candidates:
        print("No candidates in latest wheel spin.")
        return

    print(f"Walking {len(candidates)} candidates from wheel spin "
          f"({time.strftime('%H:%M', time.localtime(data.get('timestamp', 0)))})")
    print("=" * 50)

    for i, c in enumerate(candidates):
        a, b = c["pair"]
        testable = "TESTABLE" if c.get("testable") else "untestable"
        print(f"\n  [{i+1}] {a} ↔ {b}")
        print(f"      {c['proposal'][:200]}")
        print(f"      fit={c.get('mean_sim', 0):.3f}, {testable}")
        if c.get("falsification"):
            print(f"      test: {c['falsification'][:150]}")


def main():
    if len(sys.argv) >= 4:
        a = sys.argv[1]
        b = sys.argv[2]
        connection = sys.argv[3]
        falsification = sys.argv[4] if len(sys.argv) > 4 else ""
        result = send_discovery(a, b, connection, falsification)
        print(json.dumps(result))
    elif "--from-wheel" in sys.argv:
        walk_wheel()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
