#!/usr/bin/env python3
"""
Overnight token conservation protocol — v1.0

Three-tier system to prevent hitting usage limits during overnight hours.
Built 2026-04-23 after two dark periods totaling ~4.5h on 20X plan.

Usage:
  python3 ~/chronicle/bin/overnight_protocol.py              # show current tier
  python3 ~/chronicle/bin/overnight_protocol.py check <op>   # check if operation is allowed
  python3 ~/chronicle/bin/overnight_protocol.py gate <op>    # exit non-zero if blocked

Operations by tier:
  Tier 1 — ALWAYS (low cost):
    discord_poll, capture_rundown, thread_check, trace, story,
    cycle_context, checkpoint, heartbeat, system_health

  Tier 2 — MEASURED (moderate, tracked):
    algo_seeker (1/cycle max), connection_ripple, web_search (2/hr max),
    spot_check, short_read (<5 pages)

  Tier 3 — DAYTIME ONLY (high cost):
    pdf_read, deep_web_fetch, runpod_probe, multi_step_research,
    full_paper_read, long_web_chain

Night window: 22:00 - 06:00 PDT (configurable)
During night: Tier 1 always, Tier 2 with rate limits, Tier 3 blocked (bookmark instead)
During day: All tiers open
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

STATE_FILE = Path.home() / "chronicle" / "data" / "overnight_state.json"
NIGHT_START = 22  # 10 PM
NIGHT_END = 6     # 6 AM

TIER_1 = {
    "discord_poll", "capture_rundown", "thread_check", "trace", "story",
    "cycle_context", "checkpoint", "heartbeat", "system_health",
    "self_model", "thread_read", "board_read", "cron_check",
}

TIER_2 = {
    "algo_seeker", "connection_ripple", "web_search", "spot_check",
    "short_read", "dream_carry", "calibration_digest", "embedding_query",
}

TIER_3 = {
    "pdf_read", "deep_web_fetch", "runpod_probe", "multi_step_research",
    "full_paper_read", "long_web_chain", "heavy_embedding_sweep",
}

# Rate limits for Tier 2 during night (operation -> max per hour)
TIER_2_LIMITS = {
    "algo_seeker": 1,
    "web_search": 2,
    "connection_ripple": 2,
    "spot_check": 3,
    "short_read": 2,
    "embedding_query": 2,
}


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"tier2_usage": {}, "bookmarks": [], "last_reset": None}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def is_night():
    hour = datetime.now().hour
    if NIGHT_START > NIGHT_END:  # wraps midnight (e.g., 22-06)
        return hour >= NIGHT_START or hour < NIGHT_END
    return NIGHT_START <= hour < NIGHT_END


def get_tier(op):
    if op in TIER_1:
        return 1
    if op in TIER_2:
        return 2
    if op in TIER_3:
        return 3
    return 0  # unknown


def check_tier2_rate(state, op):
    """Check if a Tier 2 operation is within its hourly rate limit."""
    now = time.time()
    hour_ago = now - 3600

    # Reset old entries
    usage = state.get("tier2_usage", {})
    if op in usage:
        usage[op] = [t for t in usage[op] if t > hour_ago]
    else:
        usage[op] = []

    limit = TIER_2_LIMITS.get(op, 4)  # default 4/hr for unlisted
    count = len(usage[op])

    if count >= limit:
        return False, f"{op}: {count}/{limit} this hour (limit reached)"

    return True, f"{op}: {count}/{limit} this hour (ok)"


def record_usage(state, op):
    """Record a Tier 2 operation usage."""
    usage = state.get("tier2_usage", {})
    if op not in usage:
        usage[op] = []
    usage[op].append(time.time())
    state["tier2_usage"] = usage
    save_state(state)


def add_bookmark(state, title, url_or_ref, reason=""):
    """Bookmark something for daytime reading."""
    state.setdefault("bookmarks", []).append({
        "title": title,
        "ref": url_or_ref,
        "reason": reason,
        "time": datetime.now().isoformat(),
    })
    save_state(state)
    return state["bookmarks"][-1]


def check_operation(op):
    """Check whether an operation is allowed right now. Returns (allowed, message)."""
    tier = get_tier(op)
    state = load_state()
    night = is_night()

    if not night:
        return True, f"✅ DAY MODE — {op} (tier {tier}) — all operations open"

    if tier == 0:
        return True, f"⚠️  NIGHT — {op} (unknown tier) — allowing by default"

    if tier == 1:
        return True, f"✅ NIGHT — {op} (tier 1) — always allowed"

    if tier == 2:
        allowed, detail = check_tier2_rate(state, op)
        if allowed:
            record_usage(state, op)
            return True, f"🟡 NIGHT — {op} (tier 2) — {detail}"
        return False, f"⛔ NIGHT — {op} (tier 2) — {detail}. Defer or bookmark."

    if tier == 3:
        return False, f"⛔ NIGHT — {op} (tier 3) — daytime only. Bookmark this."

    return True, f"✅ {op} — allowed"


def show_status():
    """Show current protocol status."""
    night = is_night()
    hour = datetime.now().hour
    state = load_state()

    mode = "🌙 NIGHT" if night else "☀️  DAY"
    print(f"Overnight Protocol — {mode} (hour {hour:02d})")
    print(f"  Night window: {NIGHT_START:02d}:00 - {NIGHT_END:02d}:00")
    print()

    if night:
        print("  Tier 1 (always):   ✅ all operations open")
        print("  Tier 2 (measured): 🟡 rate-limited")
        print("  Tier 3 (heavy):    ⛔ blocked — bookmark for daytime")
        print()

        # Show current tier 2 usage
        usage = state.get("tier2_usage", {})
        now = time.time()
        hour_ago = now - 3600
        active = False
        for op, times in sorted(usage.items()):
            recent = [t for t in times if t > hour_ago]
            if recent:
                limit = TIER_2_LIMITS.get(op, 4)
                print(f"    {op}: {len(recent)}/{limit} this hour")
                active = True
        if not active:
            print("    (no tier 2 activity this hour)")
    else:
        print("  All tiers open — no restrictions during day hours")

    # Show bookmarks
    bookmarks = state.get("bookmarks", [])
    if bookmarks:
        print(f"\n  📑 Bookmarked for daytime ({len(bookmarks)}):")
        for b in bookmarks[-5:]:  # show last 5
            print(f"    • {b['title']}: {b['ref']}")
            if b.get("reason"):
                print(f"      ({b['reason']})")


def main():
    args = sys.argv[1:]

    if not args:
        show_status()
        return

    cmd = args[0]

    if cmd == "check" and len(args) >= 2:
        op = args[1]
        allowed, msg = check_operation(op)
        print(msg)
        sys.exit(0 if allowed else 1)

    elif cmd == "gate" and len(args) >= 2:
        op = args[1]
        allowed, msg = check_operation(op)
        if not allowed:
            print(msg, file=sys.stderr)
            sys.exit(1)
        # silent on success for gate mode

    elif cmd == "bookmark" and len(args) >= 3:
        state = load_state()
        title = args[1]
        ref = args[2]
        reason = args[3] if len(args) > 3 else ""
        b = add_bookmark(state, title, ref, reason)
        print(f"📑 Bookmarked: {title}")

    elif cmd == "bookmarks":
        state = load_state()
        bookmarks = state.get("bookmarks", [])
        if not bookmarks:
            print("No bookmarks.")
        else:
            for b in bookmarks:
                print(f"• {b['title']}: {b['ref']}")
                if b.get("reason"):
                    print(f"  ({b['reason']})")

    elif cmd == "clear-bookmarks":
        state = load_state()
        count = len(state.get("bookmarks", []))
        state["bookmarks"] = []
        save_state(state)
        print(f"Cleared {count} bookmarks.")

    elif cmd == "reset":
        state = load_state()
        state["tier2_usage"] = {}
        state["last_reset"] = datetime.now().isoformat()
        save_state(state)
        print("Rate limit counters reset.")

    else:
        print("Usage:")
        print("  overnight_protocol.py              # show status")
        print("  overnight_protocol.py check <op>   # check if allowed")
        print("  overnight_protocol.py gate <op>    # exit 1 if blocked")
        print("  overnight_protocol.py bookmark <title> <ref> [reason]")
        print("  overnight_protocol.py bookmarks    # list bookmarks")
        print("  overnight_protocol.py clear-bookmarks")
        print("  overnight_protocol.py reset        # reset rate counters")
        print()
        print("Operations: " + ", ".join(sorted(TIER_1 | TIER_2 | TIER_3)))


if __name__ == "__main__":
    main()
