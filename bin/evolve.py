#!/usr/bin/env python3
"""Agent Evolution System — Observation → Adaptation loops.

Agents observe their own performance and propose adaptations.
Adaptations are validated, applied via modify_agent.py, and tracked.

Usage:
  evolve.py propose <agent> "thesis" "observation" "change"
  evolve.py auto                — run all registered adaptation loops
  evolve.py status              — show pending/applied/rejected evolutions
  evolve.py history [--last N]  — show evolution history

Registered loops:
  1. fab_rate → intern fidelity prefix adjustment
  2. skip_rate → feed threshold tuning
  3. crossref_volume → crossref cycle timing
  4. gemma_routing → feed source weighting

Safety:
  - All changes go through modify_agent.py (backup, syntax check, rollback)
  - Max 1 auto-evolution per loop per 6 hours (no oscillation)
  - Every evolution has a thesis (why), observation (what triggered it),
    and expected outcome (how to verify)
  - Verification check runs 2 hours after deployment
"""

import sys, os, time, json, sqlite3, subprocess
from datetime import datetime

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
BIN_DIR = "/home/nate-agx/chronicle/bin"
COOLDOWN_HOURS = 6  # min hours between auto-evolutions per loop


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_table(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS agent_evolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loop_name TEXT NOT NULL,
            agent TEXT NOT NULL,
            thesis TEXT NOT NULL,
            observation TEXT NOT NULL,
            change_description TEXT NOT NULL,
            change_diff TEXT,
            expected_outcome TEXT,
            status TEXT DEFAULT 'proposed',
            verification_result TEXT,
            initiated_by TEXT DEFAULT 'auto',
            proposed_at INTEGER NOT NULL,
            applied_at INTEGER,
            verified_at INTEGER
        )
    """)
    db.commit()


def log(msg):
    print(f"[evolve] {msg}")


def cooldown_ok(db, loop_name):
    """Check if enough time has passed since last evolution for this loop."""
    row = db.execute(
        "SELECT MAX(proposed_at) FROM agent_evolutions WHERE loop_name=? AND status IN ('applied','proposed')",
        (loop_name,)
    ).fetchone()
    if row and row[0]:
        elapsed_h = (time.time() - row[0]) / 3600
        if elapsed_h < COOLDOWN_HOURS:
            return False, f"{elapsed_h:.1f}h since last (need {COOLDOWN_HOURS}h)"
    return True, "ok"


# ─── Loop 1: Fab Rate → Intern Fidelity ───

def loop_fab_rate(db):
    """If fab rate is consistently high, tighten the intern's fidelity prefix.
    If fab rate is consistently low, we could relax — but we don't auto-relax.
    Only tightens. Relaxation requires Opus review."""

    # Get recent fab rate from spot check audits
    rows = db.execute(
        "SELECT content FROM activity_feed WHERE source='spot_check' AND activity_type='audit' "
        "AND created_at > strftime('%s','now','-6 hours') ORDER BY created_at DESC LIMIT 6"
    ).fetchall()

    if len(rows) < 3:
        return None  # not enough data

    # Parse fab rates
    rates = []
    for r in rows:
        content = r['content'] if isinstance(r, sqlite3.Row) else r[0]
        # Format: "Spot check: 5/35 sampled, 4 passed, 0 self-ref, 1 flagged, 5/5 with provenance"
        import re
        # Look for fab rate in the content
        m = re.search(r'fab rate:\s*([\d.]+)%', content, re.IGNORECASE)
        if m:
            rates.append(float(m.group(1)))

    if len(rates) < 3:
        return None

    avg_rate = sum(rates) / len(rates)
    recent_rate = rates[0]

    # Threshold: if average fab rate > 20% over last 3+ checks, propose tightening
    if avg_rate > 20:
        return {
            'loop_name': 'fab_rate_fidelity',
            'agent': 'intern',
            'thesis': f'Fab rate averaging {avg_rate:.1f}% over {len(rates)} checks — crossref leaks dominating. '
                      f'Tightening fidelity prefix should reduce model hallucination of source attribution.',
            'observation': f'Recent fab rates: {", ".join(f"{r:.1f}%" for r in rates)}. Average: {avg_rate:.1f}%.',
            'change_description': 'Add explicit source-boundary instruction to intern fidelity prefix',
            'expected_outcome': 'Fab rate should drop below 15% within 2 hours of deployment',
        }

    return None


# ─── Loop 2: Skip Rate → Feed Threshold ───

def loop_skip_rate(db):
    """If intern is skipping too many items, the seed threshold may be too strict."""

    rows = db.execute(
        "SELECT content FROM activity_feed WHERE source='intern' AND activity_type='skip' "
        "AND created_at > strftime('%s','now','-3 hours')"
    ).fetchall()
    total_rows = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='intern' AND activity_type='pickup' "
        "AND created_at > strftime('%s','now','-3 hours')"
    ).fetchone()

    skips = len(rows)
    pickups = total_rows[0] if total_rows else 0
    total = skips + pickups

    if total < 10:
        return None

    skip_rate = skips / total * 100

    if skip_rate > 50:
        return {
            'loop_name': 'skip_rate_threshold',
            'agent': 'intern',
            'thesis': f'Skip rate at {skip_rate:.0f}% ({skips}/{total}) — too many items rejected at seed stage. '
                      f'Threshold may be too strict for current feed mix.',
            'observation': f'{skips} skips vs {pickups} pickups in last 3h.',
            'change_description': 'Relax seed_too_thin threshold or adjust total_source minimum',
            'expected_outcome': 'Skip rate should drop below 30% within next feed cycle',
        }

    return None


# ─── Loop 3: Crossref Volume ───

def loop_crossref_volume(db):
    """If crossref connections are low, investigate and propose fix."""

    row = db.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='crossref' "
        "AND created_at > strftime('%s','now','-6 hours')"
    ).fetchone()

    connections_6h = row[0] if row else 0

    # Also get the 7d average
    row_7d = db.execute(
        "SELECT COUNT(*) / 7.0 FROM activity_feed WHERE source='crossref' "
        "AND created_at > strftime('%s','now','-7 days')"
    ).fetchone()

    avg_daily = row_7d[0] if row_7d else 0
    expected_6h = avg_daily / 4  # rough 6h expectation

    if expected_6h > 5 and connections_6h < expected_6h * 0.3:
        return {
            'loop_name': 'crossref_volume',
            'agent': 'crossref',
            'thesis': f'Crossref producing {connections_6h} connections in 6h vs expected ~{expected_6h:.0f}. '
                      f'May indicate keeper-pull issue, embedding service down, or threshold too strict.',
            'observation': f'{connections_6h} connections in 6h. 7d daily average: {avg_daily:.0f}.',
            'change_description': 'Diagnose: check keeper-pull sync, embedding service, similarity threshold',
            'expected_outcome': 'Crossref volume should return to >50% of daily average within 6h',
        }

    return None


# ─── Loop 4: Gemma Route Distribution ───

def loop_gemma_routing(db):
    """If Gemma's ignore rate shifts significantly, propose investigation."""

    row = db.execute(
        "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > strftime('%s','now','-6 hours') GROUP BY route"
    ).fetchall()

    if not row:
        return None

    routing = {r[0]: r[1] for r in row}
    total = sum(routing.values())
    if total < 50:
        return None

    ignore_rate = routing.get('ignore', 0) / total * 100
    deep_rate = routing.get('deep', 0) / total * 100

    # Get 7d baseline
    row_7d = db.execute(
        "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > strftime('%s','now','-7 days') GROUP BY route"
    ).fetchall()

    if row_7d:
        baseline = {r[0]: r[1] for r in row_7d}
        baseline_total = sum(baseline.values())
        baseline_ignore = baseline.get('ignore', 0) / baseline_total * 100 if baseline_total else 85
    else:
        baseline_ignore = 85

    # Flag if ignore rate dropped more than 10 points (feed quality shift)
    if ignore_rate < baseline_ignore - 10:
        return {
            'loop_name': 'gemma_routing_shift',
            'agent': 'gemma',
            'thesis': f'Gemma ignore rate dropped to {ignore_rate:.1f}% vs {baseline_ignore:.1f}% baseline. '
                      f'Feed quality may have improved or gate sensitivity shifted.',
            'observation': f'6h routing: {json.dumps(routing)}. Deep rate: {deep_rate:.1f}%.',
            'change_description': 'No action needed if quality improved. Flag for review if gate is degrading.',
            'expected_outcome': 'Monitor — if ignore rate stays low with good briefs, this is healthy adaptation',
        }

    return None


# ─── Registered loops ───

LOOPS = {
    'fab_rate_fidelity': loop_fab_rate,
    'skip_rate_threshold': loop_skip_rate,
    # crossref_volume removed — crossref service stopped post-pivot (2026-04-11)
    'gemma_routing_shift': loop_gemma_routing,
}


def cmd_auto(db):
    """Run all registered adaptation loops."""
    ensure_table(db)
    log("Running evolution loops...")
    proposals = []

    for name, fn in LOOPS.items():
        ok, reason = cooldown_ok(db, name)
        if not ok:
            log(f"  [{name}] cooldown: {reason}")
            continue

        try:
            result = fn(db)
            if result:
                log(f"  [{name}] PROPOSAL: {result['thesis'][:100]}")
                now = int(time.time())
                db.execute(
                    "INSERT INTO agent_evolutions "
                    "(loop_name, agent, thesis, observation, change_description, "
                    "expected_outcome, status, proposed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'proposed', ?)",
                    (result['loop_name'], result['agent'], result['thesis'],
                     result['observation'], result['change_description'],
                     result.get('expected_outcome', ''), now)
                )
                db.commit()
                proposals.append(result)
            else:
                log(f"  [{name}] no action needed")
        except Exception as e:
            log(f"  [{name}] error: {e}")

    if proposals:
        log(f"\n{len(proposals)} evolution(s) proposed. Review with: evolve.py status")
        # Post to Discord
        try:
            msg = f"**Evolution System — {len(proposals)} proposal(s):**\n"
            for p in proposals:
                msg += f"\n• **{p['agent']}** ({p['loop_name']}): {p['thesis'][:150]}"
            webhook = os.environ.get("OPUS_WEBHOOK", "")
            subprocess.run(
                ["curl", "-s", "-X", "POST", webhook,
                 "-H", "Content-Type: application/json",
                 "-d", json.dumps({"content": msg[:1900]})],
                timeout=15, capture_output=True
            )
        except Exception:
            pass
    else:
        log("\nAll loops stable. No evolutions proposed.")

    return proposals


def cmd_propose(db, args):
    """Manually propose an evolution."""
    if len(args) < 3:
        print("Usage: evolve.py propose <agent> \"thesis\" \"observation\" \"change\"")
        sys.exit(1)

    agent = args[0]
    thesis = args[1]
    observation = args[2]
    change = args[3] if len(args) > 3 else thesis
    now = int(time.time())

    ensure_table(db)
    cur = db.execute(
        "INSERT INTO agent_evolutions "
        "(loop_name, agent, thesis, observation, change_description, "
        "status, initiated_by, proposed_at) "
        "VALUES (?, ?, ?, ?, ?, 'proposed', 'manual', ?)",
        ('manual', agent, thesis, observation, change, now)
    )
    db.commit()
    log(f"Evolution #{cur.lastrowid} proposed for {agent}")


def cmd_status(db):
    """Show evolution status."""
    ensure_table(db)
    rows = db.execute(
        "SELECT id, loop_name, agent, thesis, status, "
        "datetime(proposed_at,'unixepoch','localtime'), verification_result "
        "FROM agent_evolutions ORDER BY proposed_at DESC LIMIT 20"
    ).fetchall()

    if not rows:
        print("No evolutions recorded.")
        return

    print("=== Agent Evolutions ===")
    for r in rows:
        icon = {'proposed': '📋', 'applied': '🔧', 'verified': '✅',
                'rejected': '❌', 'rolled_back': '↩️'}.get(r[3], '?')
        print(f"  #{r[0]} {icon} [{r[4]}] {r[2]} — {r[1]}")
        print(f"    {r[3][:100]}")
        print(f"    Proposed: {r[5]}")
        if r[6]:
            print(f"    Verification: {r[6][:100]}")
        print()


def cmd_history(db, args):
    """Show evolution history."""
    limit = 10
    if args and args[0] == '--last':
        limit = int(args[1]) if len(args) > 1 else 10

    ensure_table(db)
    rows = db.execute(
        "SELECT id, loop_name, agent, thesis, observation, change_description, "
        "status, verification_result, datetime(proposed_at,'unixepoch','localtime') "
        "FROM agent_evolutions ORDER BY proposed_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    if not rows:
        print("No evolution history.")
        return

    print(f"=== Evolution History (last {limit}) ===")
    for r in rows:
        print(f"\n  #{r[0]} [{r[6]}] {r[2]} / {r[1]}")
        print(f"    Thesis: {r[3][:120]}")
        print(f"    Observation: {r[4][:120]}")
        print(f"    Change: {r[5][:120]}")
        if r[7]:
            print(f"    Verification: {r[7][:120]}")
        print(f"    When: {r[8]}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    db = db_connect()

    if action == "auto":
        cmd_auto(db)
    elif action == "propose":
        cmd_propose(db, sys.argv[2:])
    elif action == "status":
        cmd_status(db)
    elif action == "history":
        cmd_history(db, sys.argv[2:])
    else:
        print(f"Unknown action: {action}")
        print(__doc__)
        sys.exit(1)

    db.close()


if __name__ == "__main__":
    main()
