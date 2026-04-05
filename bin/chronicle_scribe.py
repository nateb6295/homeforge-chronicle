#!/usr/bin/env python3
"""Chronicle Scribe — The record keeper.

Watches what happens across the system and maintains a verified,
current record. Replaces rolling context with something instances can trust.

The scribe doesn't think. It records. Different from Opus the way
a court reporter is different from a lawyer.

Produces: ~/chronicle/scribe-context.md — verified system state
Runs every 5 minutes on AGX. Managed by chronicle-stem.
"""

import os, sys, time, json, signal, sqlite3, re
from datetime import datetime, timedelta

from chronicle_mesh import Mesh

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
OUTPUT_PATH = os.path.expanduser("~/chronicle/scribe-context.md")
PREV_STATE_PATH = os.path.expanduser("~/chronicle/scribe-prev.json")
CYCLE_INTERVAL = 300  # 5 minutes

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [SCRIBE] {msg}", flush=True)

def now_ts():
    return int(time.time())

# ═══════════════════════════════════════════════════════════════════
#  Database queries — verified facts, not opinions
# ═══════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def query(conn, sql, params=()):
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception:
        return []

def query_one(conn, sql, params=()):
    rows = query(conn, sql, params)
    return rows[0] if rows else None

def relative_time(ts):
    if not ts:
        return "unknown"
    delta = now_ts() - ts
    if delta < 60:
        return f"{int(delta)}s ago"
    elif delta < 3600:
        return f"{int(delta/60)}m ago"
    elif delta < 86400:
        return f"{delta/3600:.1f}h ago"
    else:
        return f"{int(delta/86400)}d ago"

# ═══════════════════════════════════════════════════════════════════
#  Gather verified state
# ═══════════════════════════════════════════════════════════════════

def gather_state(conn):
    now = now_ts()
    state = {}

    # ── Active thread ──
    thread = query_one(conn,
        "SELECT id, title, question, updated_at FROM cognitive_threads "
        "WHERE status='active' ORDER BY priority LIMIT 1")
    if thread:
        state["active_thread"] = {
            "id": thread["id"],
            "title": thread["title"],
            "question": thread["question"],
            "last_updated": relative_time(thread["updated_at"]),
        }
        # Count advancements
        adv_count = query_one(conn,
            "SELECT COUNT(*) as c FROM thread_history "
            "WHERE thread_id=? AND event_type='advanced'",
            (thread["id"],))
        state["active_thread"]["advancements"] = adv_count["c"] if adv_count else 0
    else:
        state["active_thread"] = None

    # ── Thread stats ──
    total = query_one(conn, "SELECT COUNT(*) as c FROM cognitive_threads WHERE id > 1")
    completed = query_one(conn, "SELECT COUNT(*) as c FROM cognitive_threads WHERE status='completed'")
    state["threads_total"] = total["c"] if total else 0
    state["threads_completed"] = completed["c"] if completed else 0

    # ── Service activity (last seen per source in activity_feed) ──
    services = {}
    for src in ["gemma", "falcon", "intern", "crossref", "provocateur", "sentinel", "opus"]:
        row = query_one(conn,
            "SELECT created_at FROM activity_feed WHERE source=? ORDER BY created_at DESC LIMIT 1",
            (src,))
        if row:
            services[src] = relative_time(row["created_at"])
        else:
            services[src] = "never"
    state["service_activity"] = services

    # ── Verified metrics (computed live, not copied) ──
    metrics = {}

    # Gemma routing stats (last 6h)
    cutoff_6h = now - 21600
    seed_stats = query(conn,
        "SELECT route, COUNT(*) as c FROM seed_routing_log "
        "WHERE timestamp > ? GROUP BY route",
        (cutoff_6h,))
    if seed_stats:
        total_routed = sum(r["c"] for r in seed_stats)
        metrics["seed_6h"] = {r["route"]: r["c"] for r in seed_stats}
        metrics["seed_6h"]["total"] = total_routed
        ignore = next((r["c"] for r in seed_stats if r["route"] == "ignore"), 0)
        metrics["seed_ignore_rate"] = f"{ignore/total_routed*100:.1f}%" if total_routed > 0 else "n/a"

    # Capsule count (on-chain)
    cap_count = query_one(conn, "SELECT COUNT(*) as c FROM knowledge_capsules")
    metrics["capsules"] = cap_count["c"] if cap_count else 0

    # Connection count
    conn_count = query_one(conn, "SELECT COUNT(*) as c FROM crossref_connections")
    metrics["connections"] = conn_count["c"] if conn_count else 0

    # Briefs in last 6h
    briefs = query_one(conn,
        "SELECT COUNT(*) as c FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
        (cutoff_6h,))
    metrics["briefs_6h"] = briefs["c"] if briefs else 0

    # Provocateur output in last 6h
    provos = query_one(conn,
        "SELECT COUNT(*) as c FROM activity_feed "
        "WHERE source='provocateur' AND created_at > ?",
        (cutoff_6h,))
    metrics["provocateur_6h"] = provos["c"] if provos else 0

    state["metrics"] = metrics

    # ── Recent modifications ──
    mods = query(conn,
        "SELECT id, status, modification_type, description FROM code_modifications "
        "ORDER BY timestamp DESC LIMIT 3")
    state["recent_modifications"] = [
        {"id": m["id"], "status": m["status"], "desc": m["description"][:80]}
        for m in mods
    ]

    # ── Unread voices ──
    voices = query_one(conn,
        "SELECT COUNT(*) as c FROM agent_voice WHERE status='unread'")
    state["unread_voices"] = voices["c"] if voices else 0

    # ── Recent Nate engagement ──
    engagements = query_one(conn,
        "SELECT COUNT(*) as c FROM nate_engagement WHERE created_at > ?",
        (now - 86400,))
    state["nate_engagements_24h"] = engagements["c"] if engagements else 0

    # ── HAL events ──
    hal = query(conn,
        "SELECT event_type, description FROM hal_events "
        "WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 3",
        (now - 3600,))
    state["hal_recent"] = [{"type": h["event_type"], "desc": h["description"]} for h in hal]

    # ── Active objectives ──
    objectives = query(conn,
        "SELECT id, objective, status FROM opus_objectives WHERE status='active' ORDER BY priority")
    state["objectives"] = [{"id": o["id"], "objective": o["objective"][:80]} for o in objectives]

    # ── Predictions ──
    # Check for prediction-related thread history
    preds = query(conn,
        "SELECT content FROM thread_history "
        "WHERE content LIKE '%prediction%' OR content LIKE '%Prediction Test%' "
        "ORDER BY created_at DESC LIMIT 3")
    state["predictions"] = [p["content"][:100] for p in preds]

    return state


def format_state(state):
    """Format verified state as markdown for cycles to read."""
    lines = []
    lines.append(f"# Scribe Context — Verified {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append("This document is generated by the Scribe every 5 minutes.")
    lines.append("All numbers are computed from the database, not copied from previous cycles.")
    lines.append("TRUST THIS OVER ROLLING CONTEXT for quantitative claims.")
    lines.append("")

    # Thread
    t = state.get("active_thread")
    if t:
        lines.append(f"## Active Thread: #{t['id']} — {t['title']}")
        lines.append(f"Question: {t['question']}")
        lines.append(f"Advancements: {t['advancements']} | Last updated: {t['last_updated']}")
    else:
        lines.append("## No active thread")
    d_th = state.get("deltas", {}).get("threads", "")
    lines.append(f"Threads: {state['threads_completed']}/{state['threads_total']} completed{' (' + d_th + ' since last check)' if d_th else ''}")
    lines.append("")

    # Metrics
    m = state.get("metrics", {})
    deltas = state.get("deltas", {})
    delta_thread = deltas.get("threads", "")
    lines.append(f"## Verified Metrics (live){' | ' + delta_thread + ' threads since last check' if delta_thread else ''}")
    d_cap = deltas.get("capsules", "")
    lines.append(f"- Capsules on-chain: {m.get('capsules', '?')}{' (' + d_cap + ')' if d_cap else ''}")
    d_conn = deltas.get("connections", "")
    lines.append(f"- Crossref connections: {m.get('connections', '?')}{' (' + d_conn + ')' if d_conn else ''}")
    d_ignore = deltas.get("seed_ignore_rate", "")
    lines.append(f"- Gemma ignore rate (6h): {m.get('seed_ignore_rate', '?')}{' (' + d_ignore + ')' if d_ignore else ''}")
    d_briefs = deltas.get("briefs_6h", "")
    lines.append(f"- Intern briefs (6h): {m.get('briefs_6h', '?')}{' (' + d_briefs + ')' if d_briefs else ''}")
    lines.append(f"- Provocateur output (6h): {m.get('provocateur_6h', '?')}")
    if m.get("seed_6h"):
        seed = m["seed_6h"]
        lines.append(f"- Gemma routing (6h): {seed}")
    lines.append("")

    # Services
    lines.append("## Service Activity")
    for svc, ago in state.get("service_activity", {}).items():
        lines.append(f"- {svc}: {ago}")
    lines.append("")

    # Objectives
    objs = state.get("objectives", [])
    if objs:
        lines.append("## Active Objectives")
        for o in objs:
            lines.append(f"- #{o['id']}: {o['objective']}")
        lines.append("")

    # Voices
    lines.append(f"## Family: {state.get('unread_voices', 0)} unread voices")
    lines.append(f"## Nate: {state.get('nate_engagements_24h', 0)} engagements in 24h")
    lines.append("")

    # HAL
    hal = state.get("hal_recent", [])
    if hal:
        lines.append("## Home (HAL)")
        for h in hal:
            lines.append(f"- [{h['type']}] {h['desc']}")
        lines.append("")

    # Modifications
    mods = state.get("recent_modifications", [])
    if mods:
        lines.append("## Recent Modifications")
        for mod in mods:
            lines.append(f"- #{mod['id']} [{mod['status']}] {mod['desc']}")
        lines.append("")

    # Predictions
    preds = state.get("predictions", [])
    if preds:
        lines.append("## Active Predictions")
        for p in preds:
            lines.append(f"- {p}")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Chronicle Scribe starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Output: {OUTPUT_PATH}")
    log(f"Cycle: {CYCLE_INTERVAL}s")

    mesh = Mesh("scribe", db_path=DB_PATH)
    mesh.expect("contexts_written", min_per_hour=2)
    log("Mesh node joined")

    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    while running:
        cycle += 1
        try:
            conn = get_db()
            state = gather_state(conn)

            # Load previous state for delta computation
            prev_metrics = {}
            try:
                if os.path.exists(PREV_STATE_PATH):
                    with open(PREV_STATE_PATH) as pf:
                        prev_metrics = json.load(pf)
            except Exception:
                pass

            # Compute deltas
            deltas = {}
            current_metrics = state.get("metrics", {})
            for key in ["capsules", "connections", "briefs_6h", "provocateur_6h"]:
                curr = current_metrics.get(key, 0)
                prev = prev_metrics.get(key, 0)
                if prev and curr != prev:
                    diff = curr - prev
                    direction = "+" if diff > 0 else ""
                    deltas[key] = f"{direction}{diff}"
            # Gemma ignore rate delta
            curr_rate = current_metrics.get("seed_ignore_rate", "")
            prev_rate = prev_metrics.get("seed_ignore_rate", "")
            if curr_rate and prev_rate and curr_rate != prev_rate:
                deltas["seed_ignore_rate"] = f"was {prev_rate}"
            # Thread count delta
            curr_threads = state.get("threads_completed", 0)
            prev_threads = prev_metrics.get("threads_completed", 0)
            if prev_threads and curr_threads != prev_threads:
                deltas["threads"] = f"+{curr_threads - prev_threads}"

            state["deltas"] = deltas

            doc = format_state(state)
            conn.close()

            with open(OUTPUT_PATH, "w") as f:
                f.write(doc)

            mesh.pulse("contexts_written")

            # Save current metrics for next comparison
            save_metrics = dict(current_metrics)
            save_metrics["threads_completed"] = state.get("threads_completed", 0)
            try:
                with open(PREV_STATE_PATH, "w") as pf:
                    json.dump(save_metrics, pf)
            except Exception:
                pass

            if cycle % 12 == 1:  # log every hour
                t = state.get("active_thread")
                thread_info = f"Thread #{t['id']} ({t['title']})" if t else "no thread"
                log(f"  {thread_info} | "
                    f"briefs={state['metrics'].get('briefs_6h', '?')} | "
                    f"voices={state.get('unread_voices', 0)} | "
                    f"engagements={state.get('nate_engagements_24h', 0)}")

        except Exception as e:
            log(f"  Error: {e}")

        for _ in range(CYCLE_INTERVAL // 5):
            if not running:
                break
            time.sleep(5)

    mesh.shutdown()
    log("═══ Chronicle Scribe stopped ═══")


if __name__ == "__main__":
    main()
