#!/usr/bin/env python3
"""
Chronicle Dashboard - Unified visibility into Chronicle Mind & Sprout
One source of truth for everything the agents do.
"""

import sqlite3
import json
import os
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db")
)
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "30"))


# ═══════════════════════════════════════════════════════════════════
#  Database helpers
# ═══════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query(sql, params=(), limit=50):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def query_one(sql, params=()):
    rows = query(sql, params, limit=1)
    return rows[0] if rows else None


def ts_to_str(ts):
    """Convert unix timestamp to human-readable string."""
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def ts_to_relative(ts):
    """Convert unix timestamp to relative time like '5m ago'."""
    if not ts:
        return ""
    try:
        delta = time.time() - int(ts)
        if delta < 60:
            return f"{int(delta)}s ago"
        elif delta < 3600:
            return f"{int(delta/60)}m ago"
        elif delta < 86400:
            return f"{delta/3600:.1f}h ago"
        else:
            return f"{delta/86400:.1f}d ago"
    except Exception:
        return ""


# Make helpers available to templates
app.jinja_env.globals.update(
    ts_to_str=ts_to_str,
    ts_to_relative=ts_to_relative,
    now=lambda: int(time.time()),
    REFRESH_INTERVAL=REFRESH_INTERVAL,
)


# ═══════════════════════════════════════════════════════════════════
#  Routes
# ═══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Overview dashboard -- the one-glance view."""

    # Sprout state
    sprout = query_one("SELECT * FROM sprout_state WHERE id = 1")

    # Cognitive state (Chronicle Mind)
    ccs = query_one("SELECT * FROM cognitive_state WHERE id = 1")

    # Latest thoughts (both agents mixed)
    thoughts = query(
        "SELECT * FROM thought_stream ORDER BY created_at DESC LIMIT 5"
    )

    # Latest activity
    activity = query(
        "SELECT * FROM activity_feed ORDER BY created_at DESC LIMIT 10"
    )

    # Wallet snapshot
    latest_xrp = query_one(
        "SELECT * FROM price_history WHERE symbol='XRP' ORDER BY timestamp DESC LIMIT 1"
    )
    swaps = query(
        "SELECT * FROM swap_history ORDER BY timestamp DESC LIMIT 3"
    )

    # Predictions summary
    pred_stats = query_one(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN settled=1 AND won=1 THEN 1 ELSE 0 END) as wins, "
        "SUM(CASE WHEN settled=1 AND won=0 THEN 1 ELSE 0 END) as losses, "
        "SUM(CASE WHEN settled=0 THEN 1 ELSE 0 END) as pending "
        "FROM ftso_predictions"
    )

    # Service health (from mind_timestamps)
    timestamps = {
        r["key"]: r["timestamp"]
        for r in query("SELECT key, timestamp FROM mind_timestamps")
    }

    # Creative works count
    creative_count = query_one("SELECT COUNT(*) as c FROM creative_works")

    # Notes summary
    notes_active = query_one(
        "SELECT COUNT(*) as c FROM scratch_pad WHERE resolved=0"
    )
    notes_high = query(
        "SELECT * FROM scratch_pad WHERE resolved=0 AND priority >= 7 "
        "ORDER BY priority DESC LIMIT 5"
    )

    # Outbox unread
    outbox_unread = query_one(
        "SELECT COUNT(*) as c FROM outbox WHERE acknowledged=0"
    )

    # Cognitive metrics (Phase 3)
    reflections = query(
        "SELECT content, created_at FROM scratch_pad "
        "WHERE category='reflection' AND resolved=0 "
        "ORDER BY created_at DESC LIMIT 3"
    )
    goal = query_one(
        "SELECT content, created_at FROM scratch_pad "
        "WHERE category='goal' AND resolved=0 "
        "ORDER BY priority DESC LIMIT 1"
    )

    # Action success rate from recent cycles
    recent_results = query(
        "SELECT action_results FROM thought_stream "
        "WHERE action_results != '' ORDER BY id DESC LIMIT 10"
    )
    total_actions = 0
    ok_actions = 0
    for rr in recent_results:
        parts = (rr.get("action_results") or "").split(";")
        for p in parts:
            p = p.strip()
            if "=true" in p:
                ok_actions += 1
                total_actions += 1
            elif "=false" in p or "=error" in p:
                total_actions += 1
    cognitive = {
        "reflections": reflections,
        "goal": goal,
        "success_rate": round((ok_actions / total_actions * 100) if total_actions > 0 else 0),
        "total_actions": total_actions,
    }

    return render_template("index.html",
        sprout=sprout, ccs=ccs, thoughts=thoughts, activity=activity,
        latest_xrp=latest_xrp, swaps=swaps, pred_stats=pred_stats,
        timestamps=timestamps, creative_count=creative_count,
        notes_active=notes_active, notes_high=notes_high,
        outbox_unread=outbox_unread,
        cognitive=cognitive,
    )


@app.route("/stream")
def stream():
    """Unified activity stream -- everything chronological."""
    page = int(request.args.get("page", 1))
    source = request.args.get("source", "all")
    per_page = 50
    offset = (page - 1) * per_page

    where = ""
    params = ()
    if source != "all":
        where = "WHERE source = ?"
        params = (source,)

    items = query(
        f"SELECT * FROM activity_feed {where} ORDER BY created_at DESC "
        f"LIMIT ? OFFSET ?",
        params + (per_page, offset)
    )

    total = query_one(
        f"SELECT COUNT(*) as c FROM activity_feed {where}", params
    )

    sources = query(
        "SELECT DISTINCT source FROM activity_feed ORDER BY source"
    )

    return render_template("stream.html",
        items=items, page=page, per_page=per_page,
        total=total["c"] if total else 0,
        source=source, sources=[s["source"] for s in sources],
    )


@app.route("/thoughts")
def thoughts():
    """Full thought stream with reasoning."""
    page = int(request.args.get("page", 1))
    per_page = 20
    offset = (page - 1) * per_page

    items = query(
        "SELECT * FROM thought_stream ORDER BY created_at DESC "
        "LIMIT ? OFFSET ?",
        (per_page, offset)
    )
    total = query_one("SELECT COUNT(*) as c FROM thought_stream")

    return render_template("thoughts.html",
        items=items, page=page, per_page=per_page,
        total=total["c"] if total else 0,
    )


@app.route("/creative")
def creative():
    """Creative works and challenges."""
    works = query("SELECT * FROM creative_works ORDER BY created_at DESC")
    challenges = query(
        "SELECT * FROM creative_challenges ORDER BY posed_at DESC"
    )
    return render_template("creative.html",
        works=works, challenges=challenges,
    )


@app.route("/wallet")
def wallet():
    """Wallet, swaps, predictions, price history."""
    swaps = query("SELECT * FROM swap_history ORDER BY timestamp DESC LIMIT 20")
    predictions = query(
        "SELECT * FROM ftso_predictions ORDER BY created_at DESC LIMIT 30"
    )
    prices = query(
        "SELECT * FROM price_history WHERE symbol='XRP' "
        "ORDER BY timestamp DESC LIMIT 100"
    )
    alerts = query("SELECT * FROM alerts ORDER BY created_at DESC")

    pred_stats = query_one(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN settled=1 AND won=1 THEN 1 ELSE 0 END) as wins, "
        "SUM(CASE WHEN settled=1 AND won=0 THEN 1 ELSE 0 END) as losses "
        "FROM ftso_predictions"
    )

    return render_template("wallet.html",
        swaps=swaps, predictions=predictions, prices=prices,
        alerts=alerts, pred_stats=pred_stats,
    )


@app.route("/notes")
def notes():
    """Scratch pad, outbox, messages."""
    tab = request.args.get("tab", "active")

    if tab == "all":
        pad = query(
            "SELECT * FROM scratch_pad ORDER BY priority DESC, created_at DESC LIMIT 100"
        )
    elif tab == "resolved":
        pad = query(
            "SELECT * FROM scratch_pad WHERE resolved=1 "
            "ORDER BY updated_at DESC LIMIT 50"
        )
    else:
        pad = query(
            "SELECT * FROM scratch_pad WHERE resolved=0 "
            "ORDER BY priority DESC, created_at DESC LIMIT 100"
        )

    outbox = query("SELECT * FROM outbox ORDER BY created_at DESC LIMIT 30")

    categories = query(
        "SELECT category, COUNT(*) as c FROM scratch_pad WHERE resolved=0 "
        "GROUP BY category ORDER BY c DESC"
    )

    return render_template("notes.html",
        pad=pad, outbox=outbox, categories=categories, tab=tab,
    )


@app.route("/projects")
def projects():
    """Project tracker."""
    projs = query("SELECT * FROM projects ORDER BY priority DESC, updated_at DESC")
    updates = query(
        "SELECT pu.*, p.name as project_name FROM project_updates pu "
        "JOIN projects p ON pu.project_id = p.id "
        "ORDER BY pu.created_at DESC LIMIT 20"
    )
    return render_template("projects.html", projects=projs, updates=updates)


@app.route("/api/health")
def api_health():
    """JSON health endpoint for external monitoring."""
    sprout = query_one("SELECT * FROM sprout_state WHERE id = 1")
    latest_thought = query_one(
        "SELECT created_at FROM thought_stream ORDER BY created_at DESC LIMIT 1"
    )
    latest_activity = query_one(
        "SELECT created_at FROM activity_feed ORDER BY created_at DESC LIMIT 1"
    )
    return jsonify({
        "status": "ok",
        "sprout_focus": sprout.get("current_focus", "") if sprout else "",
        "sprout_energy": sprout.get("energy_level", 0) if sprout else 0,
        "last_thought": ts_to_relative(latest_thought["created_at"]) if latest_thought else "never",
        "last_activity": ts_to_relative(latest_activity["created_at"]) if latest_activity else "never",
    })


@app.route("/api/cognitive")
def api_cognitive():
    """Phase 2+3 cognitive metrics: action diversity, reflections, meta-eval, performance."""
    # Recent cycles with action results
    recent = query(
        "SELECT cycle_id, actions_taken, action_results, context_summary, created_at "
        "FROM thought_stream ORDER BY id DESC LIMIT 20"
    )

    # Action frequency analysis (last 24h)
    action_freq = {}
    success_count = 0
    fail_count = 0
    for row in recent:
        try:
            names = json.loads(row.get("actions_taken", "[]"))
            for name in names:
                action_freq[name] = action_freq.get(name, 0) + 1
        except Exception:
            pass
        results = row.get("action_results", "")
        if results:
            for part in results.split(";"):
                part = part.strip()
                if "=true" in part:
                    success_count += 1
                elif "=false" in part or "=error" in part:
                    fail_count += 1

    total = success_count + fail_count
    success_rate = (success_count / total * 100) if total > 0 else 0

    # Action diversity: unique types in last 10 vs last 20 cycles
    recent_10 = set()
    recent_20 = set()
    for i, row in enumerate(recent):
        try:
            names = json.loads(row.get("actions_taken", "[]"))
            for name in names:
                if i < 10:
                    recent_10.add(name)
                recent_20.add(name)
        except Exception:
            pass

    # Reflections
    reflections = query(
        "SELECT id, content, created_at FROM scratch_pad "
        "WHERE category='reflection' AND resolved=0 "
        "ORDER BY created_at DESC LIMIT 5"
    )

    # Current goal
    goal = query_one(
        "SELECT content, created_at FROM scratch_pad "
        "WHERE category='goal' AND resolved=0 "
        "ORDER BY priority DESC LIMIT 1"
    )

    # Action set fingerprints (detect repetition)
    action_sets = []
    for row in recent[:5]:
        try:
            names = sorted(json.loads(row.get("actions_taken", "[]")))
            action_sets.append(names)
        except Exception:
            pass
    unique_sets = len(set(tuple(s) for s in action_sets))

    # Recent ideas
    ideas = query(
        "SELECT id, content, created_at FROM scratch_pad "
        "WHERE category='idea' AND resolved=0 "
        "ORDER BY created_at DESC LIMIT 5"
    )

    return jsonify({
        "cycles_analyzed": len(recent),
        "success_rate": round(success_rate, 1),
        "total_actions": total,
        "action_frequency": dict(sorted(action_freq.items(), key=lambda x: -x[1])),
        "diversity_last_10": len(recent_10),
        "diversity_last_20": len(recent_20),
        "unique_action_sets_last_5": unique_sets,
        "repetition_detected": unique_sets <= 2 and len(action_sets) >= 3,
        "goal": goal.get("content", "") if goal else None,
        "goal_set": ts_to_relative(goal.get("created_at")) if goal else None,
        "reflections": [
            {"content": r["content"][:200], "when": ts_to_relative(r["created_at"])}
            for r in reflections
        ],
        "ideas": [
            {"content": i["content"][:200], "when": ts_to_relative(i["created_at"])}
            for i in ideas
        ],
        "recent_cycles": [
            {
                "id": r["cycle_id"],
                "actions": r["actions_taken"],
                "results": (r.get("action_results") or "")[:200],
                "when": ts_to_relative(r["created_at"]),
            }
            for r in recent[:5]
        ],
    })


if __name__ == "__main__":
    port = int(os.environ.get("DASHBOARD_PORT", "8085"))
    app.run(host="0.0.0.0", port=port, debug=False)
