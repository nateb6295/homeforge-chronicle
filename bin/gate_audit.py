#!/usr/bin/env python3
"""Gate Audit — Complementary instrument for the Gemma gate.

Thread #247 found that the gate discards ~59 think-quality observations daily.
The stochastic reset catches some, but it re-asks Gemma (same instrument).
This audit uses novelty score (a different signal) to surface high-quality
items from the discard pile.

Runs periodically (cron or standalone). Queries recent ignored observations
with novelty scores above the think-route average, and feeds them into the
activity stream as 'gate_audit' items for crossref and provocateur pickup.

Not a replacement for the gate. A second pair of eyes on what the gate throws away.

Usage:
    python3 gate_audit.py              # Run audit, surface high-novelty ignored items
    python3 gate_audit.py --stats      # Show gate statistics without surfacing
    python3 gate_audit.py --dry-run    # Show what would be surfaced without doing it
"""

import os, sys, json, time, sqlite3
from datetime import datetime

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)

# Threshold: ignore items with novelty above this are candidates for audit
# Based on Thread #247 data: think avg = 0.409, ignore avg = 0.168
NOVELTY_THRESHOLD = 0.40  # match think-route average

# How far back to look (seconds)
LOOKBACK = 6 * 3600  # 6 hours

# Max items to surface per run (avoid flooding)
MAX_SURFACE = 5

# Rate limit: don't re-audit items we've already surfaced
STATE_FILE = os.path.expanduser("~/.homeforge-chronicle/gate_audit_state.json")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"surfaced_ids": [], "last_run": 0, "total_surfaced": 0}


def save_state(state):
    # Keep only last 200 surfaced IDs
    state["surfaced_ids"] = state["surfaced_ids"][-200:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_gate_stats(conn, hours=24):
    """Gate statistics for the given time window."""
    cutoff = int(time.time()) - hours * 3600
    routes = conn.execute(
        "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
        "WHERE timestamp > ? GROUP BY route ORDER BY cnt DESC",
        (cutoff,)
    ).fetchall()

    total = sum(r["cnt"] for r in routes)
    stats = {r["route"]: r["cnt"] for r in routes}

    # Novelty distribution per route
    novelty = {}
    for route in stats:
        row = conn.execute(
            "SELECT AVG(o.novelty_score) as avg_n, "
            "MAX(o.novelty_score) as max_n, "
            "MIN(o.novelty_score) as min_n "
            "FROM seed_routing_log r "
            "JOIN seed_observations o ON o.id = r.observation_id "
            "WHERE r.route = ? AND r.timestamp > ?",
            (route, cutoff)
        ).fetchone()
        if row:
            novelty[route] = {
                "avg": round(row["avg_n"] or 0, 3),
                "max": round(row["max_n"] or 0, 3),
                "min": round(row["min_n"] or 0, 3),
            }

    # Count high-novelty ignored items
    high_novelty_ignored = conn.execute(
        "SELECT COUNT(*) FROM seed_routing_log r "
        "JOIN seed_observations o ON o.id = r.observation_id "
        "WHERE r.route = 'ignore' AND r.timestamp > ? "
        "AND o.novelty_score >= ?",
        (cutoff, NOVELTY_THRESHOLD)
    ).fetchone()[0]

    return {
        "total": total,
        "routes": stats,
        "novelty": novelty,
        "high_novelty_ignored": high_novelty_ignored,
        "hours": hours,
    }


def get_objective_keywords(conn):
    """Extract keywords from active objectives and recent threads."""
    keywords = set()

    # Active objectives
    try:
        rows = conn.execute(
            "SELECT title, description FROM strategic_objectives WHERE status='active'"
        ).fetchall()
        for r in rows:
            text = f"{r['title']} {r['description'] or ''}".lower()
            for word in text.split():
                if len(word) >= 4 and word.isalpha():
                    keywords.add(word)
    except Exception:
        pass

    # Recent thread topics (last 7 days)
    try:
        cutoff = int(time.time()) - 7 * 86400
        rows = conn.execute(
            "SELECT question FROM cognitive_threads "
            "WHERE created_at > ? ORDER BY created_at DESC LIMIT 10",
            (cutoff,)
        ).fetchall()
        for r in rows:
            text = r["question"].lower()
            for word in text.split():
                if len(word) >= 4 and word.isalpha():
                    keywords.add(word)
    except Exception:
        pass

    # Core Chronicle/Homeforge terms always relevant
    keywords.update([
        "sovereignty", "prediction", "calibration", "infrastructure",
        "perception", "xrpl", "wallet", "canister", "frigate",
        "mortgage", "crypto", "ceasefire", "iran", "houthi",
        "surveillance", "privacy", "decentralized", "federated",
        "embedding", "fine-tune", "training", "evaluation",
    ])

    return keywords


def find_audit_candidates(conn, state):
    """Find high-novelty ignored items that match system interests.

    Two-stage filter:
    1. Novelty >= threshold (think-average) — filters out low-novelty noise
    2. Keyword relevance — matches against active objectives, recent threads,
       and core system terms. Prevents surfacing irrelevant high-novelty items.
    """
    cutoff = int(time.time()) - LOOKBACK
    surfaced_ids = set(state.get("surfaced_ids", []))

    rows = conn.execute(
        "SELECT r.observation_id, o.content, o.novelty_score, o.source, r.timestamp "
        "FROM seed_routing_log r "
        "JOIN seed_observations o ON o.id = r.observation_id "
        "WHERE r.route = 'ignore' AND r.timestamp > ? "
        "AND o.novelty_score >= ? "
        "AND o.source NOT LIKE 'mqtt:%' "
        "AND o.source NOT LIKE '%ear/scene%' "
        "AND o.source NOT LIKE '%Search:%' "
        "AND (o.content LIKE 'Research brief:%' "
        "     OR o.content LIKE 'Provocateur%' "
        "     OR o.content LIKE 'Nate capture%' "
        "     OR o.content LIKE '%crossref%') "
        "AND length(o.content) > 100 "
        "ORDER BY o.novelty_score DESC "
        "LIMIT 50",
        (cutoff, NOVELTY_THRESHOLD)
    ).fetchall()

    # Second pass: keyword relevance scoring
    keywords = get_objective_keywords(conn)
    scored = []
    for r in rows:
        if r["observation_id"] in surfaced_ids:
            continue
        content_lower = r["content"].lower()
        matches = sum(1 for kw in keywords if kw in content_lower)
        if matches >= 2:  # At least 2 keyword matches
            scored.append((matches, r["novelty_score"], dict(r)))

    # Sort by keyword matches (primary), then novelty (secondary)
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)

    candidates = [item[2] for item in scored[:MAX_SURFACE]]
    return candidates


def surface_candidates(conn, candidates, state):
    """Insert audit candidates into activity_feed for downstream pickup."""
    now = int(time.time())
    surfaced = 0

    for c in candidates:
        content = c["content"]
        obs_id = c["observation_id"]
        novelty = c["novelty_score"]

        # Truncate content for activity feed
        if len(content) > 2000:
            content = content[:1997] + "..."

        title = f"[gate_audit] {c['source']}"
        metadata = json.dumps({
            "route_reason": "gate_audit",
            "original_obs_id": obs_id,
            "novelty_score": novelty,
            "gate_route": "ignore",
            "audit_reason": f"novelty {novelty:.3f} >= {NOVELTY_THRESHOLD} threshold",
        })

        conn.execute(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("gate_audit", "gate_audit", title, content, metadata, now)
        )

        state["surfaced_ids"].append(obs_id)
        surfaced += 1
        log(f"  Surfaced obs {obs_id} (novelty {novelty:.3f}): {content[:80]}...")

    conn.commit()
    state["total_surfaced"] = state.get("total_surfaced", 0) + surfaced
    state["last_run"] = now
    save_state(state)

    return surfaced


def print_stats(conn):
    """Print gate statistics."""
    stats = get_gate_stats(conn, hours=24)
    total = stats["total"]

    print(f"\n=== Gate Statistics ({stats['hours']}h) ===\n")
    for route, cnt in sorted(stats["routes"].items(), key=lambda x: -x[1]):
        pct = cnt / total * 100
        n = stats["novelty"].get(route, {})
        print(f"  {route:20s}: {cnt:5d} ({pct:5.1f}%)  "
              f"novelty avg={n.get('avg', 0):.3f} max={n.get('max', 0):.3f}")

    print(f"\n  Total: {total}")
    print(f"  High-novelty ignored (>{NOVELTY_THRESHOLD}): {stats['high_novelty_ignored']}")
    if stats["routes"].get("ignore", 0) > 0:
        miss_rate = stats["high_novelty_ignored"] / stats["routes"]["ignore"] * 100
        print(f"  Estimated miss rate: {miss_rate:.1f}% of ignored items")

    state = load_state()
    print(f"\n  Gate audit total surfaced: {state.get('total_surfaced', 0)}")
    if state.get("last_run"):
        ago = (time.time() - state["last_run"]) / 3600
        print(f"  Last audit: {ago:.1f}h ago")


def main():
    dry_run = "--dry-run" in sys.argv
    stats_only = "--stats" in sys.argv

    conn = get_db()

    if stats_only:
        print_stats(conn)
        conn.close()
        return

    log("═══ Gate Audit ═══")

    # Always show stats
    stats = get_gate_stats(conn, hours=6)
    total = stats["total"]
    ignore_count = stats["routes"].get("ignore", 0)
    high_novelty = stats["high_novelty_ignored"]
    log(f"Gate (6h): {total} total, {ignore_count} ignored, "
        f"{high_novelty} high-novelty ignored (>{NOVELTY_THRESHOLD})")

    # Find candidates
    state = load_state()
    candidates = find_audit_candidates(conn, state)

    if not candidates:
        log("No high-novelty ignored items to surface.")
        conn.close()
        return

    log(f"Found {len(candidates)} audit candidates:")
    for c in candidates:
        content_preview = c["content"].replace("\n", " ")[:100]
        log(f"  obs {c['observation_id']} (novelty {c['novelty_score']:.3f}): {content_preview}")

    if dry_run:
        log("(dry run — not surfacing)")
        conn.close()
        return

    # Surface them
    surfaced = surface_candidates(conn, candidates, state)
    log(f"Surfaced {surfaced} items to activity feed.")
    conn.close()


if __name__ == "__main__":
    main()
