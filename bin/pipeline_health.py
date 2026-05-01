#!/usr/bin/env python3
"""Pipeline health monitor — flow rates, not process liveness.

Lesson from 2026-04-14 night: `systemctl is-active` returned green while
Gemma's classify path had been silently dead for 3 days. Process aliveness
does not equal pipeline aliveness. This script measures actual work
output across the key streams and flags flatlines.

Streams monitored:
  - gemma:classify  — seed_routing_log rows with route != 'stochastic_reset'
  - gemma:advance   — thread_history rows source LIKE 'gemma:%'
  - hermes:research — thread_history rows source LIKE 'hermes:%'
  - opus:trace      — traces/*.md files modified in window
  - threads:advance — thread_history rows source='opus' event='advanced'
  - captures        — activity_feed rows source LIKE '%operator:capture%'

For each stream: compute cadence over configurable window (default 6h),
compare against baseline (prior 24h, rate-matched), flag if current rate
is zero OR < 20% of baseline.

Usage:
    pipeline_health.py check                # human-readable report
    pipeline_health.py check --window-hr 3  # custom window
    pipeline_health.py json                 # machine-readable
    pipeline_health.py alert                # exit 1 if any stream flatlined
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
TRACES_DIR = Path(os.environ.get("CHRONICLE_TRACES",
                                 "/home/nate-agx/chronicle/traces"))
BRIDGE_STATE = Path("/home/nate-agx/.hermes/scripts/.bridge_last_id")

# Flatline threshold: current-window rate must be at least this fraction of baseline
FLATLINE_RATIO = 0.20


def conn():
    c = sqlite3.connect(DB, timeout=30.0)
    c.execute("PRAGMA busy_timeout = 30000")
    c.row_factory = sqlite3.Row
    return c


def count_since(c, sql, params):
    """Run a COUNT(*) SQL returning one int."""
    row = c.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def stream_rates(c, window_hr):
    """Return list of {stream, current, baseline, rate_ratio, flatlined}."""
    now = int(time.time())
    w = window_hr * 3600
    cur_since = now - w
    base_since = now - (24 * 3600 + w)   # prior 24h excluding current window
    base_until = now - w

    streams = []

    # gemma:classify — normal routes. Upstream: seed_observations arriving.
    cur = count_since(c,
        "SELECT COUNT(*) FROM seed_routing_log "
        "WHERE timestamp > ? AND route != 'stochastic_reset'",
        (cur_since,))
    base = count_since(c,
        "SELECT COUNT(*) FROM seed_routing_log "
        "WHERE timestamp > ? AND timestamp <= ? AND route != 'stochastic_reset'",
        (base_since, base_until))
    ups = count_since(c,
        "SELECT COUNT(*) FROM seed_observations WHERE timestamp > ?",
        (cur_since,))
    streams.append(_row("gemma:classify", cur, cur / window_hr,
                        base, base / 24.0, upstream=ups, upstream_label="seed_obs"))

    # gemma:thread — bridge writes thread_history from captures.
    # Upstream: unprocessed captures (id > bridge_last_id).
    cur = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND source LIKE 'gemma:%'",
        (cur_since,))
    base = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND created_at <= ? AND source LIKE 'gemma:%'",
        (base_since, base_until))
    ups = _bridge_backlog(c)
    streams.append(_row("gemma:thread", cur, cur / window_hr,
                        base, base / 24.0, upstream=ups, upstream_label="bridge_backlog"))

    # hermes:research — deep-routed items are the upstream trigger.
    cur = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND source LIKE 'hermes%'",
        (cur_since,))
    base = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND created_at <= ? AND source LIKE 'hermes%'",
        (base_since, base_until))
    ups = count_since(c,
        "SELECT COUNT(*) FROM seed_routing_log WHERE timestamp > ? AND route='deep'",
        (cur_since,))
    streams.append(_row("hermes:research", cur, cur / window_hr,
                        base, base / 24.0, upstream=ups, upstream_label="deep_routes"))

    # opus:trace — file-based. No upstream; this is self-cadence.
    cur = _count_traces_since(cur_since)
    base = _count_traces_since(base_since, base_until)
    streams.append(_row("opus:trace", cur, cur / window_hr,
                        base, base / 24.0))

    # threads:advance — opus self-cadence.
    cur = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND source='opus' AND event_type='advanced'",
        (cur_since,))
    base = count_since(c,
        "SELECT COUNT(*) FROM thread_history "
        "WHERE created_at > ? AND created_at <= ? AND source='opus' AND event_type='advanced'",
        (base_since, base_until))
    streams.append(_row("threads:advance", cur, cur / window_hr,
                        base, base / 24.0))

    # captures — external (Nate). Use hour-of-day shape instead of upstream check.
    cur = count_since(c,
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE created_at > ? AND source LIKE '%operator:capture%'",
        (cur_since,))
    base = count_since(c,
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE created_at > ? AND created_at <= ? AND source LIKE '%operator:capture%'",
        (base_since, base_until))
    # Overnight (23:00-07:00 local) is expected-quiet for operator captures.
    hr_local = int(time.strftime("%H", time.localtime(now)))
    overnight = (hr_local >= 23 or hr_local < 7)
    streams.append(_row("captures", cur, cur / window_hr,
                        base, base / 24.0,
                        expected_idle=overnight,
                        upstream_label="nate_awake" if not overnight else "overnight"))

    return streams


def _bridge_backlog(c):
    """How many operator-capture activity_feed rows sit above the bridge's last processed id."""
    last_id = 0
    try:
        last_id = int(BRIDGE_STATE.read_text().strip() or 0)
    except (OSError, ValueError):
        return None  # unknown — don't treat as idle
    row = c.execute(
        "SELECT COUNT(*) FROM activity_feed "
        "WHERE id > ? AND source LIKE '%operator:capture%'",
        (last_id,)).fetchone()
    return int(row[0]) if row else 0


def _count_traces_since(since, until=None):
    until = until if until is not None else int(time.time())
    n = 0
    if not TRACES_DIR.exists():
        return 0
    for p in TRACES_DIR.glob("*.md"):
        try:
            m = p.stat().st_mtime
            if since < m <= until:
                n += 1
        except OSError:
            continue
    return n


def _row(name, current, current_rate, baseline, baseline_rate,
         upstream=None, upstream_label=None, expected_idle=False):
    """Compute stream health row.

    A stream is flatlined only when baseline suggests work should be happening
    AND the current rate has collapsed AND there's no idle-excuse (upstream
    empty, or known-quiet hour-of-day).
    """
    flatlined = False
    rate_ratio = None
    if baseline_rate > 0.5:
        rate_ratio = (current_rate / baseline_rate) if baseline_rate else 0
        if rate_ratio < FLATLINE_RATIO:
            flatlined = True

    # Idle-excuse: upstream has no work, or caller flagged expected_idle.
    idle_expected = bool(expected_idle) or (upstream == 0)
    if flatlined and idle_expected:
        flatlined = False

    return {
        "stream": name,
        "current_count": current,
        "current_rate_per_hr": round(current_rate, 2),
        "baseline_count_24h": baseline,
        "baseline_rate_per_hr": round(baseline_rate, 2),
        "rate_ratio": round(rate_ratio, 3) if rate_ratio is not None else None,
        "upstream": upstream,
        "upstream_label": upstream_label,
        "idle_expected": idle_expected,
        "flatlined": flatlined,
    }


def cmd_check(args):
    c = conn()
    rows = stream_rates(c, args.window_hr)
    print(f"Pipeline health — window={args.window_hr}h — baseline=prior 24h\n")
    print(f"{'stream':<18} {'cur':>6} {'cur/hr':>8} {'base':>6} {'base/hr':>8} {'ratio':>7} {'upstream':>14}  status")
    print("-" * 90)
    for r in rows:
        ratio = f"{r['rate_ratio']:.2f}" if r['rate_ratio'] is not None else "  -  "
        ups = r.get('upstream')
        ups_label = r.get('upstream_label') or ""
        ups_cell = f"{ups_label}={ups}" if ups is not None else ups_label or "-"
        if r["flatlined"]:
            status = "🔴 FLATLINE"
        elif r["idle_expected"] and r["current_rate_per_hr"] == 0:
            status = "⚪ idle (ok)"
        elif r['rate_ratio'] is not None and r['rate_ratio'] < 0.5:
            status = "🟡 low"
        else:
            status = "🟢 ok"
        print(f"{r['stream']:<18} {r['current_count']:>6} {r['current_rate_per_hr']:>8.2f} "
              f"{r['baseline_count_24h']:>6} {r['baseline_rate_per_hr']:>8.2f} {ratio:>7} "
              f"{ups_cell:>14}  {status}")

    any_flat = any(r["flatlined"] for r in rows)
    print()
    if any_flat:
        print("RED FLAG: one or more streams have flatlined vs baseline.")
    else:
        print("All monitored streams above flatline threshold.")


def cmd_json(_args):
    c = conn()
    rows = stream_rates(c, _args.window_hr)
    print(json.dumps({
        "computed_at": int(time.time()),
        "window_hr": _args.window_hr,
        "streams": rows,
        "any_flatlined": any(r["flatlined"] for r in rows),
    }, indent=2))


def cmd_alert(args):
    c = conn()
    rows = stream_rates(c, args.window_hr)
    flats = [r for r in rows if r["flatlined"]]
    if flats:
        for r in flats:
            print(f"FLATLINED: {r['stream']} "
                  f"(current {r['current_rate_per_hr']:.2f}/hr vs baseline "
                  f"{r['baseline_rate_per_hr']:.2f}/hr)", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    check = sub.add_parser("check", help="human-readable health report")
    check.add_argument("--window-hr", type=float, default=6.0)

    jp = sub.add_parser("json", help="machine-readable output")
    jp.add_argument("--window-hr", type=float, default=6.0)

    alert = sub.add_parser("alert", help="exit 1 if any stream flatlined")
    alert.add_argument("--window-hr", type=float, default=6.0)

    args = ap.parse_args()
    {"check": cmd_check, "json": cmd_json, "alert": cmd_alert}[args.cmd](args)


if __name__ == "__main__":
    main()
