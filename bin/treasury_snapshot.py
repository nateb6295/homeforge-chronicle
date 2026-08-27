#!/usr/bin/env python3
"""Treasury snapshot — track multi-chain portfolio over time.

Runs portfolio.get_full_portfolio() and writes a JSON snapshot.
Compares to previous snapshot; if positions changed (deposits, withdrawals,
non-trivial rebalancing), prints a delta summary.

Usage:
    python3 treasury_snapshot.py            # take snapshot, compare to last
    python3 treasury_snapshot.py --history  # show last 10 snapshots
    python3 treasury_snapshot.py --silent   # only output if changes detected
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from portfolio import get_full_portfolio  # noqa

SNAPSHOT_DIR = Path.home() / "chronicle" / "data" / "treasury_snapshots"
DELTA_THRESHOLD_USD = 1.00  # only flag changes > $1
DELTA_THRESHOLD_PCT = 0.5   # or > 0.5% relative change


def take_snapshot():
    p = get_full_portfolio()
    return {
        "timestamp": int(time.time()),
        # Kimi, Aug 24: a banner beside a smaller number is still advisory —
        # the number stays readable as "the total". Change the SHAPE instead:
        # when a chain is unreachable there IS no total, and any consumer that
        # does arithmetic on it fails loudly rather than understating quietly.
        "total_usd": None if p.get("errors") else p["totals"]["usd"],
        "reachable_usd": p["totals"]["usd"],
        "breakdown": p["totals"]["breakdown"],
        "prices": p.get("prices", {}),
        # A chain that FAILS to fetch returns zero balances, so the total
        # silently drops and looks like a withdrawal. Carry the errors into
        # the snapshot or that is indistinguishable from money leaving.
        "errors": p.get("errors", []),
    }


def latest_snapshot():
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snaps = sorted(SNAPSHOT_DIR.glob("*.json"))
    if not snaps:
        return None
    with snaps[-1].open() as f:
        return json.load(f)


def save_snapshot(snap):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOT_DIR / f"{snap['timestamp']}.json"
    with path.open("w") as f:
        json.dump(snap, f, indent=2)
    return path


def diff_snapshots(prev, curr):
    """Return list of position-change strings, only above threshold."""
    if not prev:
        return ["[no prior snapshot]"]
    # An unreachable chain reads as a zero balance. Diffing that against a
    # complete snapshot manufactures a withdrawal that never happened.
    if prev.get("total_usd") is None or curr.get("total_usd") is None:
        return ["[DELTA SUPPRESSED — a snapshot is incomplete; "
                "comparing it would report a fetch failure as a position change]"]
    changes = []
    # Total USD change
    prev_usd = prev.get("total_usd", 0)
    curr_usd = curr.get("total_usd", 0)
    delta_usd = curr_usd - prev_usd
    if abs(delta_usd) > DELTA_THRESHOLD_USD:
        changes.append(f"TOTAL: ${prev_usd:.2f} → ${curr_usd:.2f} (Δ ${delta_usd:+.2f})")
    # Per-asset change (using breakdown dict)
    prev_b = prev.get("breakdown", {})
    curr_b = curr.get("breakdown", {})
    all_keys = set(prev_b) | set(curr_b)
    for asset in sorted(all_keys):
        pp = prev_b.get(asset, {}).get("usd", 0)
        cp = curr_b.get(asset, {}).get("usd", 0)
        delta = cp - pp
        if abs(delta) < DELTA_THRESHOLD_USD:
            continue
        pct = (delta / pp * 100) if pp > 0 else 100
        if abs(pct) < DELTA_THRESHOLD_PCT and abs(delta) < 5:
            continue
        prev_amt = prev_b.get(asset, {}).get("amount", 0)
        curr_amt = curr_b.get(asset, {}).get("amount", 0)
        changes.append(f"  {asset}: {prev_amt:.4f}→{curr_amt:.4f} (${pp:.2f}→${cp:.2f}, Δ ${delta:+.2f}, {pct:+.1f}%)")
    return changes


def main():
    silent = "--silent" in sys.argv
    show_history = "--history" in sys.argv

    if show_history:
        snaps = sorted(SNAPSHOT_DIR.glob("*.json"))[-10:]
        for s in snaps:
            with s.open() as f:
                d = json.load(f)
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(d["timestamp"]))
            t = d.get("total_usd")
            if t is None:
                print(f"{ts}   PARTIAL  (floor ${d.get('reachable_usd', 0):.2f})")
            else:
                print(f"{ts}  ${t:>8.2f}")
        return

    prev = latest_snapshot()
    curr = take_snapshot()
    changes = diff_snapshots(prev, curr) if prev else []

    save_snapshot(curr)

    if silent and not changes:
        return  # nothing to report

    print(f"=== Treasury snapshot {time.strftime('%Y-%m-%d %H:%M PDT')} ===")
    errs = curr.get("errors") or []
    if errs:
        # Lead with this. A silent zero is indistinguishable from a withdrawal.
        banner = (f"!! {len(errs)} CHAIN(S) FAILED TO FETCH — the total below "
                  f"is a FLOOR, not a balance:")
        print(banner)
        print(banner, file=sys.stderr)
        for e in errs:
            print(f"   - {e}")
            print(f"   - {e}", file=sys.stderr)
    if errs:
        print("Total: PARTIAL — NOT COMPUTED. Unreachable chains read as zero, "
              "so any sum here would understate the balance.")
        print(f"  reachable chains only: ${curr.get('reachable_usd', 0):.2f} "
              f"(a FLOOR, not a balance)")
    else:
        print(f"Total: ${curr.get('total_usd', 0):.2f}")
    if changes:
        print()
        print("Changes since last snapshot:")
        for c in changes:
            print(c)
    else:
        print("No significant position changes since last snapshot.")


if __name__ == "__main__":
    main()
