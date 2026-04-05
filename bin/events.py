"""Chronicle Event Bus — Lightweight pub/sub via SQLite.

Usage:
    from events import EventBus

    bus = EventBus(db_path)
    bus.publish("seed", "capsule:routed", {"capsule_id": 123, "significance": 0.8})

    # Poll for new events
    for event in bus.poll("intern", after_id=last_seen_id, event_types=["capsule:routed"]):
        process(event)
"""

import json
import time
import sqlite3


class EventBus:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_table()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        conn = self._conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)")
        conn.commit()
        conn.close()

    def publish(self, source: str, event_type: str, payload: dict = None):
        """Publish an event. Returns the event ID."""
        conn = self._conn()
        payload_json = json.dumps(payload) if payload else None
        cur = conn.execute(
            "INSERT INTO events (source, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
            (source, event_type, payload_json, time.time())
        )
        event_id = cur.lastrowid
        conn.commit()
        conn.close()
        return event_id

    def poll(self, subscriber: str = None, after_id: int = 0,
             event_types: list = None, sources: list = None,
             limit: int = 100) -> list:
        """Poll for events after a given ID. Returns list of dicts."""
        conn = self._conn()
        query = "SELECT id, source, event_type, payload, created_at FROM events WHERE id > ?"
        params = [after_id]

        if event_types:
            placeholders = ",".join("?" * len(event_types))
            query += f" AND event_type IN ({placeholders})"
            params.extend(event_types)

        if sources:
            placeholders = ",".join("?" * len(sources))
            query += f" AND source IN ({placeholders})"
            params.extend(sources)

        query += " ORDER BY id ASC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        conn.close()

        return [
            {
                "id": row["id"],
                "source": row["source"],
                "event_type": row["event_type"],
                "payload": json.loads(row["payload"]) if row["payload"] else None,
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def latest_id(self) -> int:
        """Get the latest event ID (for initializing watermarks)."""
        conn = self._conn()
        row = conn.execute("SELECT MAX(id) as max_id FROM events").fetchone()
        conn.close()
        return row["max_id"] or 0

    def prune(self, max_age_hours: int = 48):
        """Remove events older than max_age_hours."""
        conn = self._conn()
        cutoff = time.time() - (max_age_hours * 3600)
        conn.execute("DELETE FROM events WHERE created_at < ?", (cutoff,))
        conn.commit()
        conn.close()
