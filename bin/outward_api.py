#!/usr/bin/env python3
"""Chronicle Outward API — public surface for Chronicle's best output.

A lightweight read-only API serving today's briefs, connections, challenges,
and system health as structured JSON. Thread #282's conclusion made concrete.

Port 11437. No auth required (read-only, local network).
"""

import json
import sqlite3
import time
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
PORT = 11437
HEALTH_PATH = os.path.expanduser("~/chronicle/spot_check_health.json")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def briefs_handler(params):
    """Top briefs from the last N hours, ranked by argument_depth + entity_count."""
    hours = int(params.get("hours", ["24"])[0])
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    since = int(time.time()) - (hours * 3600)

    db = get_db()
    rows = db.execute("""
        SELECT id, title, content, metadata, created_at
        FROM activity_feed
        WHERE activity_type = 'brief' AND created_at > ?
        ORDER BY created_at DESC
    """, (since,)).fetchall()
    db.close()

    # Score and rank
    scored = []
    for r in rows:
        meta = {}
        try:
            meta = json.loads(r["metadata"] or "{}")
        except Exception:
            pass
        entity_count = meta.get("entity_count", 0)
        argument_depth = meta.get("argument_depth", 0)
        input_chars = meta.get("gen_ctx", {}).get("input_chars", 0)
        score = (argument_depth * 2) + entity_count + (1 if input_chars > 500 else 0)
        scored.append({
            "id": r["id"],
            "title": r["title"],
            "content": r["content"],
            "source": meta.get("source_path", "unknown"),
            "score": score,
            "argument_depth": argument_depth,
            "entity_count": entity_count,
            "created_at": r["created_at"],
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:limit]


def connections_handler(params):
    """Recent crossref connections."""
    hours = int(params.get("hours", ["24"])[0])
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    since = int(time.time()) - (hours * 3600)

    db = get_db()
    rows = db.execute("""
        SELECT id, content, metadata, created_at
        FROM activity_feed
        WHERE activity_type = 'connection' AND created_at > ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (since, limit)).fetchall()
    db.close()

    results = []
    for r in rows:
        meta = {}
        try:
            meta = json.loads(r["metadata"] or "{}")
        except Exception:
            pass
        results.append({
            "id": r["id"],
            "content": r["content"],
            "similarity": meta.get("similarity", 0),
            "interest": meta.get("interest", 0),
            "channel": meta.get("channel", "unknown"),
            "created_at": r["created_at"],
        })
    return results


def challenges_handler(params):
    """Recent provocateur contrarian challenges."""
    hours = int(params.get("hours", ["24"])[0])
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    since = int(time.time()) - (hours * 3600)

    db = get_db()
    rows = db.execute("""
        SELECT id, content, created_at
        FROM activity_feed
        WHERE activity_type = 'contrarian' AND created_at > ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (since, limit)).fetchall()
    db.close()

    return [{"id": r["id"], "content": r["content"], "created_at": r["created_at"]} for r in rows]


def syntheses_handler(params):
    """Recent provocateur syntheses — the best cross-domain connections."""
    hours = int(params.get("hours", ["24"])[0])
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    since = int(time.time()) - (hours * 3600)

    db = get_db()
    rows = db.execute("""
        SELECT id, content, created_at
        FROM activity_feed
        WHERE activity_type = 'synthesis' AND created_at > ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (since, limit)).fetchall()
    db.close()

    return [{"id": r["id"], "content": r["content"], "created_at": r["created_at"]} for r in rows]


def health_handler(params):
    """System health summary."""
    db = get_db()

    # Activity counts last 24h
    since_24h = int(time.time()) - 86400
    counts = db.execute("""
        SELECT activity_type, COUNT(*) as cnt
        FROM activity_feed
        WHERE created_at > ?
        GROUP BY activity_type
        ORDER BY cnt DESC
    """, (since_24h,)).fetchall()

    # Latest brief timestamp
    latest = db.execute("""
        SELECT created_at FROM activity_feed
        WHERE activity_type = 'brief'
        ORDER BY created_at DESC LIMIT 1
    """).fetchone()
    db.close()

    # Fab rate from health signal
    fab_info = {"fab_rate": None, "constraint_level": None}
    try:
        if os.path.exists(HEALTH_PATH):
            with open(HEALTH_PATH) as f:
                health = json.load(f)
            fab_info["fab_rate"] = health.get("fab_rate")
            fab_info["constraint_level"] = health.get("constraint_level")
    except Exception:
        pass

    latest_ts = latest["created_at"] if latest else None
    age_minutes = round((time.time() - latest_ts) / 60, 1) if latest_ts else None

    return {
        "activity_24h": {r["activity_type"]: r["cnt"] for r in counts},
        "latest_brief_age_minutes": age_minutes,
        "fabrication": fab_info,
        "timestamp": int(time.time()),
    }


def stats_handler(params):
    """Overall Chronicle stats."""
    db = get_db()
    total = db.execute("SELECT COUNT(*) as cnt FROM activity_feed").fetchone()["cnt"]
    oldest = db.execute("SELECT MIN(created_at) as ts FROM activity_feed WHERE created_at > 1700000000").fetchone()["ts"]
    sources = db.execute("""
        SELECT source, COUNT(*) as cnt FROM activity_feed
        GROUP BY source ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    db.close()

    return {
        "total_activities": total,
        "earliest_timestamp": oldest,
        "days_running": round((time.time() - oldest) / 86400, 1) if oldest else 0,
        "top_sources": {r["source"]: r["cnt"] for r in sources},
    }


def rationales_handler(params):
    """Prediction rationale ledger — WHY we predicted, not just what.

    Darby's proposal: a public, timestamped, signed ledger of reasoning traces.
    Each entry includes a SHA-256 hash of the reasoning for verifiability.
    """
    import hashlib
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    settled_only = params.get("settled", ["false"])[0].lower() == "true"

    db = get_db()
    query = """
        SELECT id, symbol, direction, entry_price, confidence, reasoning,
               timeframe_hours, created_at, settles_at, settled, won
        FROM ftso_predictions
        WHERE reasoning IS NOT NULL AND reasoning != ''
    """
    if settled_only:
        query += " AND settled = 1"
    query += " ORDER BY created_at DESC LIMIT ?"
    rows = db.execute(query, (limit,)).fetchall()
    db.close()

    results = []
    for r in rows:
        reasoning = r["reasoning"] or ""
        reasoning_hash = hashlib.sha256(reasoning.encode("utf-8")).hexdigest()[:16]
        entry = {
            "id": r["id"],
            "symbol": r["symbol"],
            "direction": r["direction"],
            "confidence": round(r["confidence"], 4),
            "reasoning": reasoning,
            "reasoning_hash": reasoning_hash,
            "timeframe_hours": r["timeframe_hours"],
            "created_at": r["created_at"],
            "settles_at": r["settles_at"],
        }
        if r["settled"]:
            entry["settled"] = True
            entry["won"] = bool(r["won"])
            entry["calibration"] = "correct" if r["won"] else "incorrect"
        else:
            entry["settled"] = False
        results.append(entry)
    return results


def thread_handler(params):
    """Live thread status — active thread with recent advancements and challenges."""
    db = get_db()

    # Get active thread
    thread = db.execute(
        "SELECT id, title, question, context, status, created_at, updated_at "
        "FROM cognitive_threads WHERE status='active' ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if not thread:
        db.close()
        return {"active": False, "message": "No active thread"}

    # Get history (last 10 events)
    history = db.execute(
        "SELECT event_type, substr(content, 1, 500) as content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at DESC LIMIT 10",
        (thread["id"],)
    ).fetchall()
    db.close()

    advances = sum(1 for h in history if h["event_type"] == "advanced")
    challenges = sum(1 for h in history if h["event_type"] == "challenge")

    # Count total advances and challenges
    db2 = get_db()
    totals = db2.execute(
        "SELECT event_type, count(*) as cnt FROM thread_history "
        "WHERE thread_id=? GROUP BY event_type", (thread["id"],)
    ).fetchall()
    db2.close()
    total_map = {r["event_type"]: r["cnt"] for r in totals}

    return {
        "active": True,
        "id": thread["id"],
        "title": thread["title"],
        "question": thread["question"],
        "context": thread["context"][:500] if thread["context"] else None,
        "total_advancements": total_map.get("advanced", 0),
        "total_challenges": total_map.get("challenge", 0),
        "created_at": thread["created_at"],
        "updated_at": thread["updated_at"],
        "recent_events": [
            {
                "type": h["event_type"],
                "content": h["content"],
                "source": h["source"],
                "timestamp": h["created_at"],
            }
            for h in history
        ],
    }


def captures_handler(params):
    """Nate's recent captures + Darby's deep dive analysis."""
    hours = int(params.get("hours", ["48"])[0])
    limit = min(int(params.get("limit", ["20"])[0]), 50)
    since = int(time.time()) - (hours * 3600)

    db = get_db()

    # Get captures
    captures = db.execute("""
        SELECT id, content, created_at
        FROM activity_feed
        WHERE source='operator:capture' AND created_at > ?
        ORDER BY created_at DESC LIMIT ?
    """, (since, limit)).fetchall()

    # Get corresponding briefs and deep dives
    results = []
    for cap in captures:
        cap_id = cap["id"]
        cap_text = cap["content"]

        # Find the brief generated from this capture
        brief = db.execute("""
            SELECT title, content, metadata FROM activity_feed
            WHERE source='intern' AND activity_type='brief'
            AND json_extract(metadata, '$.input_id') = ?
            LIMIT 1
        """, (cap_id,)).fetchone()

        # Find any deep dive on this brief
        deep_dive = None
        if brief:
            brief_meta = {}
            try:
                brief_meta = json.loads(brief["metadata"] or "{}")
            except Exception:
                pass
            dd = db.execute("""
                SELECT content FROM activity_feed
                WHERE source='intern' AND activity_type='deep_dive'
                AND json_extract(metadata, '$.original_brief') = ?
                LIMIT 1
            """, (cap_id,)).fetchone()
            if dd:
                deep_dive = dd["content"]

        results.append({
            "capture_id": cap_id,
            "raw_capture": cap_text[:500],
            "captured_at": cap["created_at"],
            "brief": {
                "title": brief["title"] if brief else None,
                "content": brief["content"][:500] if brief else None,
            } if brief else None,
            "deep_dive": deep_dive[:800] if deep_dive else None,
        })

    db.close()
    return results


ROUTES = {
    "/api/briefs": briefs_handler,
    "/api/connections": connections_handler,
    "/api/challenges": challenges_handler,
    "/api/syntheses": syntheses_handler,
    "/api/rationales": rationales_handler,
    "/api/thread": thread_handler,
    "/api/captures": captures_handler,
    "/api/health": health_handler,
    "/api/stats": stats_handler,
}

LANDING_HTML = """<!DOCTYPE html>
<html><head><title>Chronicle Outward API</title>
<style>
body { font-family: monospace; background: #0a0a0a; color: #c0c0c0; padding: 2em; max-width: 700px; }
h1 { color: #e0e0e0; }
a { color: #7cacf8; }
code { background: #1a1a1a; padding: 2px 6px; border-radius: 3px; }
.endpoint { margin: 0.8em 0; }
.desc { color: #888; margin-left: 1em; }
</style></head><body>
<h1>Chronicle Outward API</h1>
<p>Read-only surface for Chronicle's output. All endpoints return JSON.</p>

<div class="endpoint"><a href="/api/briefs">/api/briefs</a> <code>?hours=24&limit=20</code>
<div class="desc">Top briefs ranked by argument depth and entity count</div></div>

<div class="endpoint"><a href="/api/connections">/api/connections</a> <code>?hours=24&limit=20</code>
<div class="desc">Recent crossref connections between capsules</div></div>

<div class="endpoint"><a href="/api/challenges">/api/challenges</a> <code>?hours=24&limit=20</code>
<div class="desc">Provocateur's contrarian challenges</div></div>

<div class="endpoint"><a href="/api/syntheses">/api/syntheses</a> <code>?hours=24&limit=20</code>
<div class="desc">Cross-domain synthesis connections</div></div>

<div class="endpoint"><a href="/api/rationales">/api/rationales</a> <code>?limit=20&settled=false</code>
<div class="desc">Prediction rationale ledger — WHY we predicted, with reasoning hashes</div></div>

<div class="endpoint"><a href="/api/thread">/api/thread</a>
<div class="desc">Live thread status — active thread with recent advancements and challenges</div></div>

<div class="endpoint"><a href="/api/captures">/api/captures</a> <code>?hours=48&limit=20</code>
<div class="desc">Nate's recent captures with intern briefs and Darby deep dives</div></div>

<div class="endpoint"><a href="/api/health">/api/health</a>
<div class="desc">System health, activity counts, fabrication rate</div></div>

<div class="endpoint"><a href="/api/stats">/api/stats</a>
<div class="desc">Overall Chronicle statistics</div></div>

<p style="margin-top: 2em; color: #555;">Chronicle — sovereign knowledge infrastructure</p>
</body></html>"""


class OutwardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

        if path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(LANDING_HTML.encode())
            return

        handler = ROUTES.get(path)
        if not handler:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "not found", "endpoints": list(ROUTES.keys())}).encode())
            return

        try:
            result = handler(params)
            body = json.dumps(result, indent=2)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body.encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def log_message(self, format, *args):
        pass  # suppress request logging


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), OutwardHandler)
    print(f"Chronicle Outward API listening on port {PORT}")
    server.serve_forever()
