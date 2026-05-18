#!/usr/bin/env python3
"""Thread Viewer — live conversation view of cognitive threads.

One page showing all active threads with their full dialogue history.
Color-coded by source, auto-refreshes every 30s.

Usage:
  thread_viewer.py              # serve on port 8086
  thread_viewer.py --port 8088  # custom port
"""
import argparse
import json
import sqlite3
import time
from flask import Flask, Response

DB = "/mnt/hdd/chronicle-data/processed.db"
app = Flask(__name__)

SOURCE_COLORS = {
    "opus": "#7c5cbf",
    "opus-session": "#7c5cbf",
    "opus-capture": "#9b7ed8",
    "hermes:partner": "#3b82f6",
    "hermes:researcher": "#3b82f6",
    "hermes:dialogue": "#60a5fa",
    "gemma:partner": "#eab308",
    "gemma": "#eab308",
    "nate": "#10b981",
}

MODE_LABELS = {
    "advance": "advance",
    "advanced": "advance",
    "challenge": "challenge",
    "extend": "extend",
    "connect": "connect",
    "ask": "ask",
    "observation": "observation",
    "research": "research",
    "context": "context",
    "support": "support",
}


def get_color(source):
    for key, color in SOURCE_COLORS.items():
        if key in (source or "").lower():
            return color
    return "#6b7280"


def get_threads():
    db = sqlite3.connect(DB, timeout=5)
    db.row_factory = sqlite3.Row
    threads = db.execute("""
        SELECT t.id, t.title, t.question, t.status, t.priority,
               COUNT(th.id) as entry_count,
               MAX(th.created_at) as last_activity
        FROM cognitive_threads t
        LEFT JOIN thread_history th ON t.id = th.thread_id
        WHERE t.status = 'active'
        GROUP BY t.id
        ORDER BY last_activity DESC
    """).fetchall()
    result = []
    for t in threads:
        entries = db.execute("""
            SELECT id, event_type, content, source, created_at
            FROM thread_history
            WHERE thread_id = ?
            ORDER BY created_at DESC
            LIMIT 50
        """, (t["id"],)).fetchall()
        result.append({
            "id": t["id"],
            "title": t["title"],
            "question": t["question"],
            "priority": t["priority"],
            "entry_count": t["entry_count"],
            "last_activity": t["last_activity"],
            "entries": list(reversed(entries)),
        })
    db.close()
    return result


def format_time(ts):
    try:
        return time.strftime("%b %d %H:%M", time.localtime(int(ts)))
    except (ValueError, TypeError, OSError):
        return str(ts)[:16] if ts else "?"


def format_source(source):
    s = (source or "unknown").split(":")[0]
    return s.capitalize()


@app.route("/")
def index():
    threads = get_threads()
    now = time.strftime("%H:%M:%S %Z")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>Thread Viewer</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'SF Mono', 'Fira Code', monospace; background: #0f0f0f; color: #e0e0e0; padding: 20px; }}
  h1 {{ color: #7c5cbf; margin-bottom: 4px; font-size: 1.3em; }}
  .meta {{ color: #666; font-size: 0.8em; margin-bottom: 24px; }}
  .thread {{ margin-bottom: 32px; border: 1px solid #222; border-radius: 8px; overflow: hidden; }}
  .thread-header {{ background: #1a1a1a; padding: 12px 16px; cursor: pointer; }}
  .thread-header:hover {{ background: #222; }}
  .thread-title {{ font-size: 1.1em; font-weight: bold; }}
  .thread-question {{ color: #888; font-size: 0.85em; margin-top: 4px; }}
  .thread-stats {{ color: #555; font-size: 0.75em; margin-top: 4px; }}
  .entries {{ padding: 0; }}
  .entry {{ padding: 10px 16px; border-bottom: 1px solid #1a1a1a; }}
  .entry:last-child {{ border-bottom: none; }}
  .entry-header {{ display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }}
  .source {{ font-weight: bold; font-size: 0.85em; }}
  .mode {{ font-size: 0.7em; padding: 2px 6px; border-radius: 3px; background: #222; color: #aaa; text-transform: uppercase; }}
  .mode-challenge {{ background: #7f1d1d; color: #fca5a5; }}
  .mode-extend {{ background: #14532d; color: #86efac; }}
  .mode-connect {{ background: #1e3a5f; color: #93c5fd; }}
  .mode-ask {{ background: #713f12; color: #fde68a; }}
  .mode-advance {{ background: #2d1b69; color: #c4b5fd; }}
  .mode-observation {{ background: #1a1a2e; color: #94a3b8; }}
  .timestamp {{ color: #555; font-size: 0.7em; margin-left: auto; }}
  .content {{ font-size: 0.88em; line-height: 1.5; color: #ccc; white-space: pre-wrap; word-break: break-word; }}
  .collapsed .entries {{ display: none; }}
  .toggle {{ color: #555; float: right; }}
</style>
</head>
<body>
<h1>Chronicle Threads</h1>
<div class="meta">{len(threads)} active threads | refreshes every 30s | {now}</div>
"""

    for t in threads:
        last = format_time(t["last_activity"]) if t["last_activity"] else "never"
        collapsed = "" if t == threads[0] else "collapsed"
        html += f"""
<div class="thread {collapsed}" id="thread-{t['id']}">
  <div class="thread-header" onclick="this.parentElement.classList.toggle('collapsed')">
    <div class="thread-title">
      <span class="toggle">▸</span>
      #{t['id']} — {t['title']}
    </div>
    <div class="thread-question">{t['question'] or ''}</div>
    <div class="thread-stats">{t['entry_count']} entries | priority {t['priority']} | last: {last}</div>
  </div>
  <div class="entries">
"""
        for e in t["entries"]:
            source = e["source"] or "unknown"
            color = get_color(source)
            etype = e["event_type"] or "entry"
            mode_class = f"mode-{MODE_LABELS.get(etype, '')}" if etype in MODE_LABELS else ""
            ts = format_time(e["created_at"])
            content = (e["content"] or "").replace("<", "&lt;").replace(">", "&gt;")

            html += f"""
    <div class="entry">
      <div class="entry-header">
        <span class="source" style="color: {color}">{format_source(source)}</span>
        <span class="mode {mode_class}">{etype}</span>
        <span class="timestamp">{ts}</span>
      </div>
      <div class="content">{content}</div>
    </div>
"""
        html += "  </div>\n</div>\n"

    html += """
<script>
document.querySelectorAll('.thread').forEach(t => {
  const toggle = t.querySelector('.toggle');
  if (toggle) {
    const update = () => { toggle.textContent = t.classList.contains('collapsed') ? '▸' : '▾'; };
    update();
    t.querySelector('.thread-header').addEventListener('click', () => setTimeout(update, 10));
  }
});
</script>
</body></html>"""

    return Response(html, content_type="text/html")


@app.route("/api/threads")
def api_threads():
    threads = get_threads()
    data = []
    for t in threads:
        data.append({
            "id": t["id"],
            "title": t["title"],
            "question": t["question"],
            "entry_count": t["entry_count"],
            "entries": [
                {"id": e["id"], "type": e["event_type"], "source": e["source"],
                 "content": e["content"], "ts": e["created_at"]}
                for e in t["entries"]
            ]
        })
    return json.dumps(data, indent=2), 200, {"Content-Type": "application/json"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8086)
    args = parser.parse_args()
    print(f"Thread Viewer: http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False)
