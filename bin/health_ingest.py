#!/usr/bin/env python3
"""Health data ingest — receives Apple Watch biometrics via REST API.

Health Auto Export (iOS) POSTs JSON to this endpoint. Data is stored
in processed.db and optionally forwarded to MQTT for chronicle-hal.

Usage:
  python3 health_ingest.py                # start server on port 8678
  python3 health_ingest.py --port 8678    # explicit port
  python3 health_ingest.py --test         # show recent data
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
DEFAULT_PORT = 8678

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric TEXT NOT NULL,
            value REAL,
            unit TEXT,
            timestamp REAL NOT NULL,
            source TEXT DEFAULT 'apple_watch',
            raw_json TEXT,
            created_at REAL DEFAULT (strftime('%%s', 'now'))
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_health_metric_ts
        ON health_data(metric, timestamp DESC)
    """)
    db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_health_dedup
        ON health_data(metric, timestamp, value)
    """)
    db.commit()
    db.close()


SLEEP_STAGES = {"InBed": 0, "Asleep": 1, "Awake": 2, "Core": 3, "Deep": 4, "REM": 5}
RAW_LOG = os.path.join(os.path.dirname(__file__), "..", "data", "health_raw_payloads.jsonl")


def _parse_ts(ts_val):
    from datetime import datetime, timezone, timedelta
    import re
    if isinstance(ts_val, str):
        s = ts_val.strip()
        s = s.replace("Z", "+00:00")
        # Health Auto Export sends " -0700" (no colon) — normalize to "-07:00"
        m = re.search(r'([+-])(\d{2})(\d{2})$', s)
        if m:
            s = s[:m.start()] + m.group(1) + m.group(2) + ':' + m.group(3)
        try:
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            pass
        # Last resort: strip tz and assume local
        try:
            clean = re.sub(r'\s*[+-]\d{2}:?\d{2}$', '', s)
            return datetime.fromisoformat(clean).timestamp()
        except Exception:
            return time.time()
    return float(ts_val) if ts_val else time.time()


def _handle_sleep_summary(db, name, point):
    """Handle Health Auto Export summary format: totalSleep, core, deep, rem, awake as hours."""
    ts = _parse_ts(point.get("sleepStart", point.get("date", "")))
    raw = json.dumps(point)
    stored = 0

    for stage_key in ["totalSleep", "core", "deep", "rem", "awake", "inBed", "asleep"]:
        val = point.get(stage_key)
        if val is not None and isinstance(val, (int, float)) and val > 0:
            db.execute(
                "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
                (f"sleep_{stage_key.lower()}", round(val * 60, 1), "min", ts, raw))
            stored += 1

    # Also store total hours directly
    total = point.get("totalSleep")
    if total is not None and isinstance(total, (int, float)):
        db.execute(
            "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
            ("sleep_hours", round(total, 2), "hr", ts, raw))
        stored += 1

    return stored


def _handle_sleep_interval(db, name, point):
    """Handle individual sleep stage intervals with startDate/endDate."""
    start_str = point.get("startDate", point.get("start"))
    end_str = point.get("endDate", point.get("end"))
    stage = point.get("value", point.get("qty", ""))

    if not start_str or not end_str:
        return 0

    start_ts = _parse_ts(start_str)
    end_ts = _parse_ts(end_str)
    if start_ts == end_ts:
        return 0

    duration_min = (end_ts - start_ts) / 60.0
    stage_num = SLEEP_STAGES.get(stage, -1) if isinstance(stage, str) else stage

    db.execute(
        "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
        (f"sleep_{stage.lower() if isinstance(stage, str) else 'unknown'}", duration_min, "min",
         start_ts, json.dumps(point))
    )
    db.execute(
        "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
        ("sleep_duration", duration_min, "min", start_ts, json.dumps(point))
    )
    return 2


SLEEP_LOG = os.path.join(os.path.dirname(__file__), "..", "data", "health_sleep_debug.jsonl")


def _try_store_point(db, name, point, item_units=""):
    """Store a single data point. Returns count of rows stored."""
    is_sleep = "sleep" in name.lower()
    if is_sleep:
        try:
            with open(SLEEP_LOG, "a") as f:
                f.write(json.dumps({"name": name, "keys": list(point.keys()) if isinstance(point, dict) else "not_dict", "sample": {k: str(v)[:100] for k, v in (point.items() if isinstance(point, dict) else [])}}) + "\n")
        except Exception:
            pass
        if "totalSleep" in point:
            return _handle_sleep_summary(db, name, point)
        if "startDate" in point or "start" in point or "inBed" in str(point.get("value", "")):
            return _handle_sleep_interval(db, name, point)

    value = point.get("qty", point.get("value", point.get("avg")))
    unit = point.get("units", item_units)
    ts = _parse_ts(point.get("date", point.get("timestamp", time.time())))

    if value is None:
        return 0

    try:
        float_val = float(value)
    except (ValueError, TypeError):
        db.execute(
            "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
            (name, 0.0, str(value), ts, json.dumps(point))
        )
        return 1

    db.execute(
        "INSERT OR IGNORE INTO health_data (metric, value, unit, timestamp, raw_json) VALUES (?, ?, ?, ?, ?)",
        (name, float_val, unit, ts, json.dumps(point))
    )
    return 1


def store_metrics(payload):
    db = sqlite3.connect(DB_PATH)
    stored = 0

    try:
        os.makedirs(os.path.dirname(RAW_LOG), exist_ok=True)
        metric_names = []
        if "data" in payload and "metrics" in payload["data"]:
            metric_names = [m.get("name", m.get("type", "?")) for m in payload["data"]["metrics"] if isinstance(m, dict)]
        elif "metrics" in payload:
            metric_names = [m.get("name", m.get("type", "?")) for m in payload["metrics"] if isinstance(m, dict)]
        with open(RAW_LOG, "a") as f:
            f.write(json.dumps({"ts": time.time(), "metric_count": len(metric_names), "metrics": metric_names}) + "\n")
        if any("sleep" in m.lower() for m in metric_names):
            sleep_dump = os.path.join(os.path.dirname(RAW_LOG), "health_sleep_full_payload.jsonl")
            with open(sleep_dump, "a") as f:
                f.write(json.dumps({"ts": time.time(), "payload": payload}, default=str) + "\n")
    except Exception:
        pass

    if "data" in payload and "metrics" in payload["data"]:
        metrics = payload["data"]["metrics"]
    elif "metrics" in payload:
        metrics = payload["metrics"]
    elif isinstance(payload, list):
        metrics = payload
    else:
        metrics = [payload]

    for item in metrics:
        if isinstance(item, dict):
            name = item.get("name", item.get("type", "unknown"))

            if "sleep" in name.lower():
                try:
                    with open(SLEEP_LOG, "a") as f:
                        sample_data = item.get("data", [])[:3] if isinstance(item.get("data"), list) else "no_data_key"
                        f.write(json.dumps({"ts": time.time(), "name": name, "item_keys": list(item.keys()), "units": item.get("units", ""), "data_count": len(item.get("data", [])) if isinstance(item.get("data"), list) else 0, "sample_points": sample_data}, default=str) + "\n")
                except Exception:
                    pass

            if "data" in item and isinstance(item["data"], list):
                for point in item["data"]:
                    stored += _try_store_point(db, name, point, item.get("units", ""))
            else:
                stored += _try_store_point(db, name, item, "")

    db.commit()
    db.close()

    try:
        forward_to_mqtt(payload)
    except Exception:
        pass

    try:
        check_departures()
    except Exception:
        pass

    return stored


def check_departures():
    import subprocess
    subprocess.Popen(
        ["python3", os.path.join(os.path.dirname(__file__), "health_alert_bio.py")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def forward_to_mqtt(payload):
    try:
        import paho.mqtt.publish as publish
        publish.single(
            "chronicle/health",
            json.dumps(payload),
            hostname=MQTT_BROKER,
            port=MQTT_PORT,
        )
    except Exception:
        pass


class HealthHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": "invalid JSON"}')
            return

        stored = store_metrics(payload)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"stored": stored}).encode())

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok", "service": "health_ingest"}')
            return

        if self.path == "/recent":
            db = sqlite3.connect(DB_PATH)
            rows = db.execute(
                "SELECT metric, value, unit, timestamp FROM health_data ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            db.close()
            data = [{"metric": r[0], "value": r[1], "unit": r[2], "timestamp": r[3]} for r in rows]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data, indent=2).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        metric_count = args[0] if args else ""
        if "POST" in str(metric_count):
            print(f"[health_ingest] {time.strftime('%H:%M:%S')} {format % args}", flush=True)


def show_recent():
    db = sqlite3.connect(DB_PATH)
    try:
        rows = db.execute(
            "SELECT metric, value, unit, datetime(timestamp, 'unixepoch', 'localtime') FROM health_data ORDER BY timestamp DESC LIMIT 20"
        ).fetchall()
    except sqlite3.OperationalError:
        print("No health_data table yet. Start the server first.")
        return
    db.close()

    if not rows:
        print("No health data yet.")
        return

    print(f"{'Metric':30s} {'Value':>10s} {'Unit':10s} {'Time':20s}")
    print("-" * 75)
    for r in rows:
        print(f"{r[0]:30s} {r[1]:10.1f} {r[2]:10s} {r[3]:20s}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()

    if args.test:
        show_recent()
        return

    init_db()
    server = HTTPServer(("0.0.0.0", args.port), HealthHandler)
    print(f"Health ingest listening on port {args.port}", flush=True)
    print(f"  POST http://192.168.1.70:{args.port}/  — receive health data", flush=True)
    print(f"  GET  http://192.168.1.70:{args.port}/recent — view recent data", flush=True)
    print(f"  GET  http://192.168.1.70:{args.port}/health — health check", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
