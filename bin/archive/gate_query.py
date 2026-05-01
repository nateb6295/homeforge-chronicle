#!/usr/bin/env python3
"""Gate query — lets family agents pull Gemma's actual routing data.

Build #64: Family agents can query gate stats instead of theorizing blind.
Build #66: Added dissent stats — Ada's second-opinion sampling on ignored observations.

Queries:
  distribution [hours]  — route distribution (ignore/think/deep) over last N hours
  domains [hours]       — which domains are being routed where
  stochastic [hours]    — stochastic reset results
  temperatures          — current domain temperatures
  correlations          — recent domain arrival correlations
  dissent [hours]       — dissent gate promotions vs confirmations
"""

import os
import sqlite3
import time
import json

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def distribution(hours=6):
    """Route distribution over last N hours."""
    conn = _db()
    cutoff = int(time.time()) - (hours * 3600)
    rows = conn.execute(
        "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > ? GROUP BY route ORDER BY cnt DESC",
        (cutoff,)
    ).fetchall()
    conn.close()
    total = sum(r["cnt"] for r in rows)
    lines = [f"Gate routing distribution (last {hours}h, {total} total):"]
    for r in rows:
        pct = (r["cnt"] / total * 100) if total else 0
        lines.append(f"  {r['route']}: {r['cnt']} ({pct:.1f}%)")
    return "\n".join(lines)


def domains(hours=6):
    """Which source domains are being routed where."""
    conn = _db()
    cutoff = int(time.time()) - (hours * 3600)
    rows = conn.execute(
        "SELECT o.source, r.route, COUNT(*) as cnt "
        "FROM seed_routing_log r "
        "JOIN seed_observations o ON r.observation_id = o.id "
        "WHERE r.timestamp > ? AND r.route != 'ignore' "
        "GROUP BY o.source, r.route ORDER BY cnt DESC LIMIT 20",
        (cutoff,)
    ).fetchall()
    conn.close()
    lines = [f"Non-ignore routes by source (last {hours}h):"]
    for r in rows:
        lines.append(f"  {r['source']} → {r['route']}: {r['cnt']}")
    return "\n".join(lines)


def stochastic(hours=12):
    """Stochastic reset results."""
    conn = _db()
    cutoff = int(time.time()) - (hours * 3600)
    rows = conn.execute(
        "SELECT a.source, a.activity_type, substr(a.content, 1, 120) as snippet "
        "FROM activity_feed a "
        "WHERE a.source = 'gemma' AND a.activity_type = 'stochastic_reset' "
        "AND a.created_at > ? ORDER BY a.created_at DESC LIMIT 10",
        (cutoff,)
    ).fetchall()
    conn.close()
    lines = [f"Stochastic resets (last {hours}h): {len(rows)} found"]
    for r in rows:
        lines.append(f"  [{r['activity_type']}] {r['snippet']}")
    return "\n".join(lines)


def temperatures():
    """Current domain temperatures."""
    conn = _db()
    rows = conn.execute(
        "SELECT domain, temperature, direction, last_shock_at "
        "FROM domain_temperature WHERE last_shock_at > 0 "
        "ORDER BY temperature DESC"
    ).fetchall()
    conn.close()
    lines = ["Domain temperatures (current):"]
    for r in rows:
        lines.append(f"  {r['domain']}: {r['temperature']:.2f} ({r['direction']})")
    return "\n".join(lines) if len(lines) > 1 else "No active domain temperatures."


def correlations():
    """Recent domain arrival correlations from activity feed."""
    conn = _db()
    rows = conn.execute(
        "SELECT source, activity_type, substr(content, 1, 150) as snippet "
        "FROM activity_feed "
        "WHERE source = 'gemma' AND content LIKE '%coupling%' "
        "ORDER BY created_at DESC LIMIT 5"
    ).fetchall()
    conn.close()
    lines = ["Recent domain correlations:"]
    for r in rows:
        lines.append(f"  {r['snippet']}")
    return "\n".join(lines) if len(lines) > 1 else "No recent correlation alerts."


def dissent(hours=6):
    """Dissent gate stats — Build #66 proof-of-attention consensus."""
    conn = _db()
    cutoff = int(time.time()) - (hours * 3600)
    # Dissent promotions show model_used ending in '+dissent'
    promotions = conn.execute(
        "SELECT COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > ? AND model_used LIKE '%+dissent'",
        (cutoff,)
    ).fetchone()["cnt"]
    # Total classified ignores (model_used is NOT NULL and route is ignore)
    classified_ignores = conn.execute(
        "SELECT COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > ? AND route = 'ignore' AND model_used IS NOT NULL",
        (cutoff,)
    ).fetchone()["cnt"]
    # Dedup ignores (model_used IS NULL)
    dedup_ignores = conn.execute(
        "SELECT COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > ? AND route = 'ignore' AND model_used IS NULL",
        (cutoff,)
    ).fetchone()["cnt"]
    # Total observations
    total = conn.execute(
        "SELECT COUNT(*) as cnt FROM seed_routing_log WHERE timestamp > ?",
        (cutoff,)
    ).fetchone()["cnt"]
    conn.close()

    lines = [f"Dissent gate stats (last {hours}h, {total} total observations):"]
    lines.append(f"  Dedup ignores (pre-classifier): {dedup_ignores}")
    lines.append(f"  Classified ignores (eligible for dissent): {classified_ignores}")
    lines.append(f"  Dissent promotions (Ada overruled Gemma): {promotions}")
    if classified_ignores > 0:
        rate = promotions / classified_ignores * 100
        lines.append(f"  Dissent rate: {rate:.1f}%")
    else:
        lines.append(f"  Dissent rate: N/A (no classified ignores yet)")
    lines.append(f"  Sample rate: 10% of classified ignores")
    return "\n".join(lines)


def query(query_str):
    """Parse a query string and return results."""
    parts = query_str.strip().lower().split()
    if not parts:
        return "Available queries: distribution, domains, stochastic, temperatures, correlations, dissent"

    cmd = parts[0]
    hours = 6
    if len(parts) > 1:
        try:
            hours = int(parts[1])
        except ValueError:
            pass

    if cmd in ("distribution", "dist", "routes"):
        return distribution(hours)
    elif cmd in ("domains", "sources"):
        return domains(hours)
    elif cmd in ("stochastic", "resets"):
        return stochastic(hours)
    elif cmd in ("temperatures", "temps", "temp"):
        return temperatures()
    elif cmd in ("correlations", "corr", "coupling"):
        return correlations()
    elif cmd in ("dissent", "consensus"):
        return dissent(hours)
    else:
        return f"Unknown query '{cmd}'. Available: distribution, domains, stochastic, temperatures, correlations, dissent"


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: gate_query.py <query> [hours]")
        print("Queries: distribution, domains, stochastic, temperatures, correlations, dissent")
    else:
        print(query(" ".join(sys.argv[1:])))
