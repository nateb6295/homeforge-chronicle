#!/usr/bin/env python3
"""Canister health dashboard — rotation tool for directive 5a.

Shows balances, burn rates from audit trail, and depletion projections.

Usage:
    canister_health.py              # Full health report
    canister_health.py --brief      # One-line per canister
    canister_health.py --refresh    # Query live balances first, then report
"""

import sqlite3
import subprocess
import sys
import os
import re
import time
from datetime import datetime, timedelta

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
DFX = os.path.expanduser("~/.local/share/dfx/bin/dfx")
NETWORK = "ic"

CANISTERS = {
    "keeper":   "mjoko-laaaa-aaaai-q7tlq-cai",
    "backend":  "fqqku-bqaaa-aaaai-q4wha-cai",
    "lab":      "4vr3t-eqaaa-aaaai-q6kea-cai",
    "frontend": "nbt4b-giaaa-aaaai-q33lq-cai",
    "swiss":    "t3jbn-diaaa-aaaax-qaapa-cai",
}

# External dependency canisters — referenced for completeness, but balance is
# not under our control so we don't track burn. llm is the on-chain LLM that
# keeper_ask invokes (we pay via attached cycles per call, not by topping up
# this canister). kinic is on the Swiss-adjacent fiduciary subnet, used for
# cross-canister memory adjunct.
DEP_CANISTERS = {
    "llm":   "w36hm-eqaaa-aaaal-qr76a-cai",
    "kinic": "yygli-5qaaa-aaaak-apg7q-cai",
}

# Priority order for reporting (keeper > backend > lab > frontend > swiss)
PRIORITY = ["keeper", "backend", "lab", "frontend", "swiss"]

WARN_THRESHOLD = 2_000_000_000_000   # 2T
ALERT_THRESHOLD = 3_000_000_000_000  # 3T


def fmt_cycles(cycles):
    """Format cycles as human-readable string."""
    if cycles >= 1_000_000_000_000:
        return f"{cycles / 1_000_000_000_000:.2f}T"
    elif cycles >= 1_000_000_000:
        return f"{cycles / 1_000_000_000:.1f}B"
    elif cycles >= 1_000_000:
        return f"{cycles / 1_000_000:.0f}M"
    return str(cycles)


def get_audit_history(db, name, limit=20):
    """Recent audit entries, last 24h only so a pre-fix drain doesn't
    swamp the current burn rate. Falls back to any points if 24h is empty."""
    cutoff = int(time.time()) - 86400
    rows = db.execute(
        "SELECT balance, idle_burn_per_day, logged_at FROM canister_cycle_log "
        "WHERE canister_name = ? AND logged_at >= ? "
        "ORDER BY logged_at DESC LIMIT ?",
        (name, cutoff, limit)
    ).fetchall()
    if not rows:
        rows = db.execute(
            "SELECT balance, idle_burn_per_day, logged_at FROM canister_cycle_log "
            "WHERE canister_name = ? ORDER BY logged_at DESC LIMIT ?",
            (name, limit)
        ).fetchall()
    return rows


def calculate_burn_rate(history):
    """Calculate actual burn rate from audit history.

    Returns (cycles_per_day, confidence) where confidence is:
      'measured' if we have 2+ data points spanning 1+ hours
      'estimated' if using idle_burn_per_day from dfx
      'unknown' if no data
    """
    if len(history) < 2:
        if history and history[0][1] > 0:
            return history[0][1], "idle_estimate"
        return 0, "unknown"

    # Use oldest and newest to get the longest baseline
    newest = history[0]   # (balance, idle_burn, logged_at)
    oldest = history[-1]

    time_span = newest[2] - oldest[2]  # seconds
    if time_span < 3600:  # less than 1 hour — not enough data
        if newest[1] > 0:
            return newest[1], "idle_estimate"
        return 0, "insufficient_data"

    balance_delta = oldest[0] - newest[0]  # positive means burning
    if balance_delta <= 0:
        # Balance went UP (top-up happened) — find segments without top-ups
        # For now, fall back to idle estimate
        if newest[1] > 0:
            return newest[1], "idle_estimate"
        return 0, "topped_up"

    days = time_span / 86400
    rate = balance_delta / days
    return int(rate), "measured"


def project_depletion(balance, burn_rate):
    """Project when canister will hit thresholds."""
    if burn_rate <= 0:
        return None, None

    days_to_alert = (balance - ALERT_THRESHOLD) / burn_rate
    days_to_warn = (balance - WARN_THRESHOLD) / burn_rate

    alert_date = datetime.now() + timedelta(days=days_to_alert) if days_to_alert > 0 else None
    warn_date = datetime.now() + timedelta(days=days_to_warn) if days_to_warn > 0 else None

    return alert_date, warn_date


def query_live_balance(name):
    """Query live balance from IC network."""
    cid = CANISTERS[name]
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    try:
        result = subprocess.run(
            [DFX, "canister", "--network", NETWORK, "status", cid],
            capture_output=True, text=True, timeout=15, env=env
        )
        balance = None
        idle_burn = 0
        for line in result.stdout.split("\n"):
            if line.startswith("Balance:"):
                m = re.search(r"([\d_]+)\s+Cycles", line)
                if m:
                    balance = int(m.group(1).replace("_", ""))
            if "Idle cycles burned" in line:
                m = re.search(r"([\d_]+)\s+Cycles", line)
                if m:
                    idle_burn = int(m.group(1).replace("_", ""))
        return balance, idle_burn
    except Exception as e:
        return None, 0


def refresh_balances(db):
    """Query all canisters live and log to audit trail."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS canister_cycle_log ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "canister_name TEXT NOT NULL, "
        "balance INTEGER NOT NULL, "
        "idle_burn_per_day INTEGER DEFAULT 0, "
        "logged_at INTEGER NOT NULL)"
    )
    results = {}
    for name in PRIORITY:
        balance, idle_burn = query_live_balance(name)
        if balance is not None:
            db.execute(
                "INSERT INTO canister_cycle_log (canister_name, balance, idle_burn_per_day, logged_at) "
                "VALUES (?, ?, ?, ?)",
                (name, balance, idle_burn, int(time.time()))
            )
            results[name] = (balance, idle_burn)
            print(f"  Queried {name}: {fmt_cycles(balance)}")
    db.commit()
    return results


def report(brief=False):
    """Generate health report from audit trail."""
    db = sqlite3.connect(DB_PATH, timeout=5)

    total_entries = db.execute("SELECT COUNT(*) FROM canister_cycle_log").fetchone()[0]

    if not brief:
        print("═══════════════════════════════════════════════════════")
        print("  CANISTER HEALTH REPORT")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M PDT')}")
        print(f"  Audit trail: {total_entries} entries")
        print("═══════════════════════════════════════════════════════")

    alerts = []

    for name in PRIORITY:
        history = get_audit_history(db, name)

        if not history:
            if brief:
                print(f"  {name}: NO DATA")
            else:
                print(f"\n  {name.upper()} ({CANISTERS[name]})")
                print(f"    No audit data available. Run with --refresh.")
            continue

        balance = history[0][0]
        last_check = datetime.fromtimestamp(history[0][2])
        burn_rate, confidence = calculate_burn_rate(history)
        alert_date, warn_date = project_depletion(balance, burn_rate)

        # Status indicator
        if balance < WARN_THRESHOLD:
            status = "!! CRITICAL"
            alerts.append(f"{name} CRITICAL: {fmt_cycles(balance)}")
        elif balance < ALERT_THRESHOLD:
            status = "!  LOW"
            alerts.append(f"{name} LOW: {fmt_cycles(balance)}")
        else:
            status = "OK"

        if brief:
            burn_str = f"burn {fmt_cycles(burn_rate)}/day" if burn_rate > 0 else "burn unknown"
            proj_str = ""
            if alert_date and alert_date > datetime.now():
                days = (alert_date - datetime.now()).days
                proj_str = f" → 3T in {days}d"
            print(f"  {status:12s} {name:10s} {fmt_cycles(balance):>8s}  {burn_str}{proj_str}")
        else:
            print(f"\n  {name.upper()} ({CANISTERS[name]})")
            print(f"    Balance:    {fmt_cycles(balance)}  [{status}]")
            print(f"    Last check: {last_check.strftime('%Y-%m-%d %H:%M')}")
            print(f"    Burn rate:  {fmt_cycles(burn_rate)}/day ({confidence})")
            print(f"    Data pts:   {len(history)}")

            if alert_date and alert_date > datetime.now():
                days = (alert_date - datetime.now()).days
                print(f"    Hits 3T:    ~{alert_date.strftime('%Y-%m-%d')} ({days} days)")
            elif burn_rate > 0:
                print(f"    Hits 3T:    ALREADY BELOW or insufficient data")

            if warn_date and warn_date > datetime.now():
                days = (warn_date - datetime.now()).days
                print(f"    Hits 2T:    ~{warn_date.strftime('%Y-%m-%d')} ({days} days)")

            # Show recent trend if we have enough data
            if len(history) >= 3:
                print(f"    Trend:      ", end="")
                for i, (bal, _, ts) in enumerate(history[:5]):
                    t = datetime.fromtimestamp(ts).strftime("%H:%M")
                    print(f"{fmt_cycles(bal)}@{t}", end="  ")
                print()

    if not brief:
        print("\n───────────────────────────────────────────────────────")
        if alerts:
            for a in alerts:
                print(f"  ⚠ {a}")
        else:
            print("  All canisters healthy.")
        print("───────────────────────────────────────────────────────")

    db.close()
    return alerts


if __name__ == "__main__":
    brief = "--brief" in sys.argv
    do_refresh = "--refresh" in sys.argv

    if do_refresh:
        print("Refreshing live balances...")
        db = sqlite3.connect(DB_PATH, timeout=5)
        refresh_balances(db)
        db.close()
        print()

    report(brief=brief)
