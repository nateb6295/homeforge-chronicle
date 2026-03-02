"""Chronicle Mind - Database Layer (SQLite)."""

import sqlite3
from typing import Optional, List

from mind.utils import log, now_ts, safe_truncate
from mind.config import OPERATOR_PROTECTED_CATEGORIES


class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        # Auto-migrate thought_stream columns at startup
        try:
            cur = self.conn.cursor()
            cur.execute("PRAGMA table_info(thought_stream)")
            cols = [r[1] for r in cur.fetchall()]
            if "action_results" not in cols:
                self.conn.execute("ALTER TABLE thought_stream ADD COLUMN action_results TEXT DEFAULT ''")
                self.conn.commit()
            if "action_signatures" not in cols:
                self.conn.execute("ALTER TABLE thought_stream ADD COLUMN action_signatures TEXT DEFAULT ''")
                self.conn.commit()
        except Exception:
            pass  # table may not exist yet

        # Auto-migrate creative_challenges columns
        try:
            cols = [r[1] for r in self.conn.execute("PRAGMA table_info(creative_challenges)").fetchall()]
            if "attempt_count" not in cols:
                self.conn.execute("ALTER TABLE creative_challenges ADD COLUMN attempt_count INTEGER DEFAULT 0")
                self.conn.commit()
            if "shelved_at" not in cols:
                self.conn.execute("ALTER TABLE creative_challenges ADD COLUMN shelved_at INTEGER")
                self.conn.commit()
        except Exception:
            pass

    def query(self, sql: str, params: tuple = ()) -> list:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            log(f"  DB query error: {e}")
            return []

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def run(self, sql: str, params: tuple = ()) -> int:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            log(f"  DB write error: {e}")
            return 0

    def close(self):
        self.conn.close()

    # -- Timestamps --
    def get_ts(self, key: str) -> Optional[int]:
        row = self.query_one("SELECT timestamp FROM mind_timestamps WHERE key = ?", (key,))
        return row["timestamp"] if row else None

    def set_ts(self, key: str, ts: int = None):
        ts = ts or now_ts()
        if self.get_ts(key) is not None:
            self.run("UPDATE mind_timestamps SET timestamp = ? WHERE key = ?", (ts, key))
        else:
            self.run("INSERT INTO mind_timestamps (key, timestamp) VALUES (?, ?)", (key, ts))

    # -- Price --
    def store_price(self, symbol: str, price: float, source: str):
        self.run(
            "INSERT INTO price_history (symbol, price_usd, source, timestamp) VALUES (?, ?, ?, ?)",
            (symbol, price, source, now_ts()),
        )

    def latest_price(self, symbol: str) -> Optional[dict]:
        return self.query_one(
            "SELECT * FROM price_history WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            (symbol,),
        )

    def price_trend(self, symbol: str) -> dict:
        """Get price trend data: current, 24h ago, 7d ago."""
        now = now_ts()
        current = self.query_one(
            "SELECT price_usd FROM price_history WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            (symbol,),
        )
        day_ago = self.query_one(
            "SELECT price_usd FROM price_history WHERE symbol = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            (symbol, now - 86400),
        )
        week_ago = self.query_one(
            "SELECT price_usd FROM price_history WHERE symbol = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            (symbol, now - 604800),
        )
        return {
            "current": current["price_usd"] if current else 0,
            "price_24h": day_ago["price_usd"] if day_ago else 0,
            "price_7d": week_ago["price_usd"] if week_ago else 0,
        }

    # -- Activity feed --
    def log_activity(self, source: str, atype: str, title: str, content: str, meta: str = None):
        self.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, atype, title, content, meta, now_ts()),
        )

    def recent_activity(self, limit: int = 10, source: str = None) -> list:
        if source:
            return self.query(
                "SELECT * FROM activity_feed WHERE source = ? ORDER BY id DESC LIMIT ?",
                (source, limit),
            )
        return self.query("SELECT * FROM activity_feed ORDER BY id DESC LIMIT ?", (limit,))

    # -- Thought stream --
    _action_results_migrated = False
    _action_sigs_migrated = False

    def log_thought(self, cid: str, reasoning: str, context_summary: str, actions: str,
                    results: str = "", action_sigs: str = ""):
        # Ensure columns exist (auto-migrate) — once per process
        if not DB._action_results_migrated:
            cols = [r["name"] for r in self.query("PRAGMA table_info(thought_stream)")]
            if "action_results" not in cols:
                self.run("ALTER TABLE thought_stream ADD COLUMN action_results TEXT DEFAULT ''")
            if "action_signatures" not in cols:
                self.run("ALTER TABLE thought_stream ADD COLUMN action_signatures TEXT DEFAULT ''")
            DB._action_results_migrated = True
            DB._action_sigs_migrated = True
        elif not DB._action_sigs_migrated:
            cols = [r["name"] for r in self.query("PRAGMA table_info(thought_stream)")]
            if "action_signatures" not in cols:
                self.run("ALTER TABLE thought_stream ADD COLUMN action_signatures TEXT DEFAULT ''")
            DB._action_sigs_migrated = True
        self.run(
            "INSERT INTO thought_stream (cycle_id, reasoning, context_summary, actions_taken, "
            "action_results, action_signatures, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (cid, reasoning, context_summary, actions, results, action_sigs, now_ts()),
        )

    # -- Scratch pad (operator notes) --
    def operator_notes(self, limit: int = 10) -> list:
        return self.query(
            "SELECT * FROM scratch_pad WHERE resolved = 0 "
            "AND category NOT IN ('cycle-handoff', 'meta-block', 'meta-eval', "
            "'meta-clarify', 'reflection', 'identity-narrative', 'opus-guidance', 'for-opus') "
            "ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def write_note(self, content: str, category: str = "thought", priority: int = 0) -> int:
        ts = now_ts()
        return self.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?)",
            (content, category, priority, ts, ts),
        )

    def resolve_note(self, note_id: int):
        self.run("UPDATE scratch_pad SET resolved = 1 WHERE id = ?", (note_id,))

    def bulk_resolve_notes(self, note_ids: List[int]):
        """Resolve multiple notes in one transaction."""
        if not note_ids:
            return
        cur = self.conn.cursor()
        for nid in note_ids:
            cur.execute("UPDATE scratch_pad SET resolved = 1 WHERE id = ?", (nid,))
        self.conn.commit()

    def update_note_content(self, note_id: int, content: str):
        """Update a note's content (for merge annotations)."""
        self.run(
            "UPDATE scratch_pad SET content = ?, updated_at = ? WHERE id = ?",
            (content, now_ts(), note_id),
        )

    def unresolved_notes_full(self, limit: int = 200) -> list:
        """Get full note data for consolidation."""
        return self.query(
            "SELECT id, content, category, priority, created_at FROM scratch_pad "
            "WHERE resolved = 0 ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def auto_resolve_old_notes(self, max_age_hours: int = 48) -> int:
        """Auto-resolve notes older than max_age_hours. Excludes operator-protected categories."""
        cutoff = now_ts() - (max_age_hours * 3600)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE scratch_pad SET resolved = 1 WHERE resolved = 0 AND created_at < ? "
            "AND category NOT IN ('directive', 'task')",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def recent_note_similar(self, content: str, hours: int = 24) -> bool:
        """Check if a similar note was written recently (keyword overlap + phrase check)."""
        cutoff = now_ts() - (hours * 3600)
        recent = self.query(
            "SELECT content FROM scratch_pad WHERE resolved = 0 AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 30",
            (cutoff,),
        )
        # Extract keywords from new content (words > 4 chars)
        new_words = set(w.lower() for w in content.split() if len(w) > 4)
        if not new_words:
            return False
        # Also extract a short phrase signature (first 8 words lowered)
        new_phrase = " ".join(content.lower().split()[:8])
        for note in recent:
            existing_words = set(w.lower() for w in note["content"].split() if len(w) > 4)
            if not existing_words:
                continue
            overlap = len(new_words & existing_words) / max(len(new_words), 1)
            if overlap > 0.35:
                return True
            # Phrase-level check: if first 8 words match, it's the same topic
            existing_phrase = " ".join(note["content"].lower().split()[:8])
            if new_phrase and existing_phrase and new_phrase == existing_phrase:
                return True
        return False

    # -- Predictions --
    def unsettled_predictions(self) -> list:
        return self.query(
            "SELECT * FROM ftso_predictions WHERE settled = 0 AND settles_at <= ?",
            (now_ts(),),
        )

    def settle_prediction(self, pred_id: int, price: float, won: bool):
        self.run(
            "UPDATE ftso_predictions SET settled=1, settlement_price=?, won=? WHERE id=?",
            (price, 1 if won else 0, pred_id),
        )

    # -- Swap history --
    def last_swap_time(self) -> Optional[int]:
        row = self.query_one(
            "SELECT timestamp FROM swap_history WHERE success = 1 ORDER BY timestamp DESC LIMIT 1"
        )
        return row["timestamp"] if row else None

    def daily_swap_total(self) -> float:
        day_start = now_ts() - 86400
        row = self.query_one(
            "SELECT COALESCE(SUM(amount_xrp), 0.0) as total FROM swap_history "
            "WHERE success = 1 AND timestamp > ?",
            (day_start,),
        )
        return row["total"] if row else 0.0

    def record_swap(self, amount_xrp: float, amount_rlusd: float, price: float,
                    rsi: float, reason: str, tx_hash: str, success: bool, direction: str = "buy"):
        self.run(
            "INSERT INTO swap_history (amount_xrp, amount_rlusd, xrp_price_usd, rsi_value, "
            "reason, tx_hash, success, timestamp, direction) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (amount_xrp, amount_rlusd, price, rsi, reason, tx_hash, 1 if success else 0, now_ts(), direction),
        )

    # -- Outbox --
    def add_outbox(self, message: str, category: str = "mind", priority: int = 0):
        self.run(
            "INSERT INTO outbox (message, priority, category, created_at) VALUES (?, ?, ?, ?)",
            (message, priority, category, now_ts()),
        )

    # -- Projects --
    def active_projects(self, limit: int = 5) -> list:
        return self.query(
            "SELECT * FROM projects WHERE status != 'completed' ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )

    # -- Research findings --
    def pending_research(self, limit: int = 5) -> list:
        return self.query(
            "SELECT * FROM extractions ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )

    # -- Alerts --
    def active_alerts(self) -> list:
        return self.query("SELECT * FROM alerts WHERE active = 1")

    # -- Creative works --
    def store_creative(self, form: str, content: str, title: str = None, cid: str = None):
        self.run(
            "INSERT INTO creative_works (form, title, content, cycle_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (form, title, content, cid, now_ts()),
        )

    # -- Creative challenges --
    def pending_challenges(self, limit: int = 3) -> list:
        # Auto-shelve challenges attempted 5+ times
        self.run(
            "UPDATE creative_challenges SET shelved_at = ? "
            "WHERE responded_at IS NULL AND shelved_at IS NULL "
            "AND COALESCE(attempt_count, 0) >= 5",
            (now_ts(),),
        )
        return self.query(
            "SELECT * FROM creative_challenges "
            "WHERE responded_at IS NULL AND shelved_at IS NULL "
            "ORDER BY posed_at DESC LIMIT ?",
            (limit,),
        )

    # -- Patterns --
    def patterns_needing_reinforcement(self, limit: int = 10) -> list:
        # Exclude patterns reinforced in the last 24h AND patterns already at high confidence
        cutoff_24h = now_ts() - 86400
        return self.query(
            "SELECT * FROM consolidation_patterns WHERE confidence_score < 0.8 "
            "AND (last_seen IS NULL OR last_seen < ?) "
            "ORDER BY confidence_score ASC LIMIT ?",
            (cutoff_24h, limit),
        )

    # -- Conversations / messages --
    def inbox_messages(self, limit: int = 5) -> list:
        return self.query(
            "SELECT * FROM outbox WHERE category = 'sibling' AND acknowledged = 0 "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )

    # -- Nostr --
    def ensure_nostr_table(self):
        self.run(
            "CREATE TABLE IF NOT EXISTS nostr_posts ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  event_id TEXT NOT NULL,"
            "  content TEXT NOT NULL,"
            "  kind INTEGER DEFAULT 1,"
            "  relays_ok TEXT,"
            "  relays_fail TEXT,"
            "  cycle_id TEXT,"
            "  created_at INTEGER NOT NULL"
            ")"
        )

    def log_nostr_post(self, event_id: str, content: str, kind: int,
                       relays_ok: list, relays_fail: list, cid: str):
        self.run(
            "INSERT INTO nostr_posts (event_id, content, kind, relays_ok, relays_fail, cycle_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_id, content, kind, ",".join(relays_ok), ",".join(relays_fail), cid, now_ts()),
        )

    def last_nostr_post_time(self) -> Optional[int]:
        row = self.query_one(
            "SELECT created_at FROM nostr_posts WHERE kind = 1 ORDER BY created_at DESC LIMIT 1"
        )
        return row["created_at"] if row else None

    def last_creative_explore_time(self) -> Optional[int]:
        row = self.query_one(
            "SELECT created_at FROM creative_works ORDER BY created_at DESC LIMIT 1"
        )
        return row["created_at"] if row else None

    def recent_creative_forms(self, limit: int = 6) -> List[str]:
        """Get recent creative_explore forms to detect form repetition."""
        rows = self.query(
            "SELECT form FROM creative_works ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [r["form"] for r in rows]

    # -- Causal edges --
    def ensure_causal_table(self):
        """Create causal_edges table and add trigger_tags column."""
        self.run(
            "CREATE TABLE IF NOT EXISTS causal_edges ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  source_id TEXT NOT NULL,"
            "  target_id TEXT NOT NULL,"
            "  edge_type TEXT NOT NULL,"
            "  strength REAL DEFAULT 1.0,"
            "  context TEXT DEFAULT '',"
            "  created_at INTEGER NOT NULL"
            ")"
        )
        self.run("CREATE INDEX IF NOT EXISTS idx_causal_source ON causal_edges(source_id)")
        self.run("CREATE INDEX IF NOT EXISTS idx_causal_target ON causal_edges(target_id)")
        self.run("CREATE INDEX IF NOT EXISTS idx_causal_type ON causal_edges(edge_type)")
        try:
            cols = [r["name"] for r in self.query("PRAGMA table_info(thought_stream)")]
            if "trigger_tags" not in cols:
                self.run("ALTER TABLE thought_stream ADD COLUMN trigger_tags TEXT DEFAULT '[]'")
        except Exception:
            pass

    def add_causal_edge(self, source_id: str, target_id: str, edge_type: str,
                        strength: float = 1.0, context: str = ""):
        """Insert a single causal edge."""
        self.run(
            "INSERT INTO causal_edges "
            "(source_id, target_id, edge_type, strength, context, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source_id, target_id, edge_type, strength, context, now_ts()),
        )

    def get_edges_for_cycle(self, cycle_id: str, direction: str = "both") -> list:
        """Get all edges where cycle is source or target."""
        if direction == "backward":
            return self.query(
                "SELECT * FROM causal_edges WHERE target_id = ? ORDER BY created_at DESC",
                (cycle_id,),
            )
        elif direction == "forward":
            return self.query(
                "SELECT * FROM causal_edges WHERE source_id = ? ORDER BY created_at ASC",
                (cycle_id,),
            )
        return self.query(
            "SELECT * FROM causal_edges WHERE source_id = ? OR target_id = ? "
            "ORDER BY created_at DESC",
            (cycle_id, cycle_id),
        )
