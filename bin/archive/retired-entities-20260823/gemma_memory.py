#!/usr/bin/env python3
"""Gemma Memory — Persistent observation and pattern accumulation.

Gemma watches everything that flows through the system. This module gives her
the ability to accumulate: what she's noticed, what recurs, what shifted,
and how her calibration has evolved over time.

Three layers:
  1. Observations — notable things Gemma noticed (high-signal routes, domain shifts, anomalies)
  2. Patterns — recurring themes Gemma has identified across observations
  3. Calibration — her evolving sense of what matters (threshold drift, domain sensitivity)

Storage: SQLite tables in the shared DB. Lightweight — no embeddings needed.
Gemma's memory is about what she's SEEN, not semantic retrieval.
"""

import json
import os
import sqlite3
import time
from typing import Optional, List, Dict

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)

IDENTITY_FILE = os.path.expanduser("~/chronicle/data/gemma_memory.json")

MAX_OBSERVATIONS = 200
MAX_PATTERNS = 50
MAX_CALIBRATION = 100
MAX_CUSTOM_CATEGORIES = 20
OBSERVATION_SUMMARY_WINDOW = 24 * 3600  # 24h for recent context


def _ensure_tables(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemma_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            category TEXT NOT NULL,
            content TEXT NOT NULL,
            source_context TEXT,
            salience REAL DEFAULT 0.5
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemma_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            pattern TEXT NOT NULL,
            evidence_count INTEGER DEFAULT 1,
            active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemma_calibration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            note TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemma_custom_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            keywords TEXT NOT NULL,
            created_from_pattern_id INTEGER,
            created_at INTEGER NOT NULL,
            retired_at INTEGER,
            match_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_gemma_obs_ts
        ON gemma_observations(timestamp DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_gemma_obs_cat
        ON gemma_observations(category)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_gemma_pat_active
        ON gemma_patterns(active, updated_at DESC)
    """)
    conn.commit()
    conn.close()


class GemmaMemory:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        _ensure_tables(db_path)

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Observations ──────────────────────────────────────────────

    def observe(self, category: str, content: str,
                source_context: str = None, salience: float = 0.5):
        """Record something Gemma noticed.

        Checks self-defined categories first — if the content matches one,
        uses Gemma's category instead of the caller's hardcoded one.
        """
        custom = self.match_custom_category(content)
        if custom:
            category = custom
        conn = self._conn()
        conn.execute(
            "INSERT INTO gemma_observations "
            "(timestamp, category, content, source_context, salience) "
            "VALUES (?, ?, ?, ?, ?)",
            (int(time.time()), category, content[:500],
             source_context[:200] if source_context else None,
             round(salience, 3)),
        )
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM gemma_observations").fetchone()[0]
        if count > MAX_OBSERVATIONS:
            conn.execute(
                "DELETE FROM gemma_observations WHERE id IN "
                "(SELECT id FROM gemma_observations ORDER BY timestamp ASC "
                f"LIMIT {count - MAX_OBSERVATIONS})"
            )
            conn.commit()
        conn.close()

    def recent_observations(self, hours: int = 24, limit: int = 20) -> List[Dict]:
        """Get recent observations for context injection."""
        conn = self._conn()
        cutoff = int(time.time()) - hours * 3600
        rows = conn.execute(
            "SELECT category, content, salience, timestamp FROM gemma_observations "
            "WHERE timestamp > ? ORDER BY salience DESC, timestamp DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def observation_summary(self) -> str:
        """Build a compact summary of recent observations for prompt injection."""
        obs = self.recent_observations(hours=24, limit=15)
        if not obs:
            return ""
        lines = []
        for o in obs:
            lines.append(f"- [{o['category']}] {o['content']}")
        return "Recent observations:\n" + "\n".join(lines)

    # ── Patterns ──────────────────────────────────────────────────

    def _pattern_similarity(self, a: str, b: str) -> float:
        """Word-overlap similarity between two pattern strings."""
        stop = {"the", "a", "an", "of", "in", "to", "and", "is", "from", "with", "for", "on", "at", "by"}
        wa = {w.lower().strip(".,;:()") for w in a.split()} - stop
        wb = {w.lower().strip(".,;:()") for w in b.split()} - stop
        if not wa or not wb:
            return 0.0
        return len(wa & wb) / min(len(wa), len(wb))

    def note_pattern(self, pattern: str):
        """Record or reinforce a pattern Gemma has noticed."""
        conn = self._conn()
        now = int(time.time())
        existing = conn.execute(
            "SELECT id, evidence_count, pattern FROM gemma_patterns "
            "WHERE active = 1",
        ).fetchall()
        match = None
        for row in existing:
            if row["pattern"] == pattern or self._pattern_similarity(pattern, row["pattern"]) > 0.5:
                match = row
                break
        if match:
            conn.execute(
                "UPDATE gemma_patterns SET evidence_count = evidence_count + 1, "
                "updated_at = ? WHERE id = ?",
                (now, match["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO gemma_patterns "
                "(created_at, updated_at, pattern, evidence_count, active) "
                "VALUES (?, ?, ?, 1, 1)",
                (now, now, pattern[:300]),
            )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM gemma_patterns WHERE active = 1"
        ).fetchone()[0]
        if count > MAX_PATTERNS:
            conn.execute(
                "UPDATE gemma_patterns SET active = 0 WHERE id IN "
                "(SELECT id FROM gemma_patterns WHERE active = 1 "
                "ORDER BY evidence_count ASC, updated_at ASC "
                f"LIMIT {count - MAX_PATTERNS})"
            )
            conn.commit()
        conn.close()

    def active_patterns(self, limit: int = 10) -> List[Dict]:
        """Get active patterns, strongest first."""
        conn = self._conn()
        rows = conn.execute(
            "SELECT pattern, evidence_count, updated_at FROM gemma_patterns "
            "WHERE active = 1 ORDER BY evidence_count DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def pattern_summary(self) -> str:
        """Compact pattern summary for prompt injection."""
        pats = self.active_patterns(limit=8)
        if not pats:
            return ""
        lines = []
        for p in pats:
            lines.append(f"- {p['pattern']} (seen {p['evidence_count']}x)")
        return "Patterns you've noticed:\n" + "\n".join(lines)

    # ── Calibration ───────────────────────────────────────────────

    def log_calibration(self, metric: str, value: float, note: str = None):
        """Record a calibration point (threshold change, drift, sensitivity shift)."""
        conn = self._conn()
        conn.execute(
            "INSERT INTO gemma_calibration (timestamp, metric, value, note) "
            "VALUES (?, ?, ?, ?)",
            (int(time.time()), metric, round(value, 4), note[:200] if note else None),
        )
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM gemma_calibration").fetchone()[0]
        if count > MAX_CALIBRATION:
            conn.execute(
                "DELETE FROM gemma_calibration WHERE id IN "
                "(SELECT id FROM gemma_calibration ORDER BY timestamp ASC "
                f"LIMIT {count - MAX_CALIBRATION})"
            )
            conn.commit()
        conn.close()

    def calibration_trend(self, metric: str, last_n: int = 10) -> List[Dict]:
        """Get recent calibration values for a metric."""
        conn = self._conn()
        rows = conn.execute(
            "SELECT value, note, timestamp FROM gemma_calibration "
            "WHERE metric = ? ORDER BY timestamp DESC LIMIT ?",
            (metric, last_n),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Self-defined Categories ────────────────────────────────────

    def create_category(self, name: str, description: str,
                        keywords: List[str],
                        from_pattern_id: int = None) -> bool:
        """Gemma creates a new observation category.

        She decides what matters. Keywords are matched against incoming
        observations to auto-tag with her categories.
        """
        conn = self._conn()
        try:
            conn.execute(
                "INSERT INTO gemma_custom_categories "
                "(name, description, keywords, created_from_pattern_id, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name.lower().replace(" ", "_"), description[:200],
                 json.dumps(keywords), from_pattern_id, int(time.time())),
            )
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM gemma_custom_categories WHERE retired_at IS NULL"
            ).fetchone()[0]
            if count > MAX_CUSTOM_CATEGORIES:
                conn.execute(
                    "UPDATE gemma_custom_categories SET retired_at = ? WHERE id IN "
                    "(SELECT id FROM gemma_custom_categories "
                    "WHERE retired_at IS NULL ORDER BY match_count ASC, created_at ASC "
                    f"LIMIT {count - MAX_CUSTOM_CATEGORIES})",
                    (int(time.time()),),
                )
                conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False

    def retire_category(self, name: str):
        """Gemma retires a category she no longer finds useful."""
        conn = self._conn()
        conn.execute(
            "UPDATE gemma_custom_categories SET retired_at = ? WHERE name = ? AND retired_at IS NULL",
            (int(time.time()), name),
        )
        conn.commit()
        conn.close()

    def list_categories(self, include_retired: bool = False) -> List[Dict]:
        """List Gemma's self-defined categories."""
        conn = self._conn()
        where = "" if include_retired else "WHERE retired_at IS NULL"
        rows = conn.execute(
            f"SELECT name, description, keywords, match_count, created_at, retired_at "
            f"FROM gemma_custom_categories {where} ORDER BY match_count DESC"
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["keywords"] = json.loads(d["keywords"])
            result.append(d)
        return result

    def match_custom_category(self, text: str) -> Optional[str]:
        """Check if text matches any of Gemma's self-defined categories.

        Returns the category name if matched, None otherwise.
        """
        conn = self._conn()
        rows = conn.execute(
            "SELECT id, name, keywords FROM gemma_custom_categories "
            "WHERE retired_at IS NULL"
        ).fetchall()
        lower = text.lower()
        matched = None
        for row in rows:
            keywords = json.loads(row["keywords"])
            if any(kw.lower() in lower for kw in keywords):
                matched = row["name"]
                conn.execute(
                    "UPDATE gemma_custom_categories SET match_count = match_count + 1 "
                    "WHERE id = ?", (row["id"],),
                )
                break
        conn.commit()
        conn.close()
        return matched

    def category_summary(self) -> str:
        """Compact summary of Gemma's self-defined categories."""
        cats = self.list_categories()
        if not cats:
            return ""
        lines = [f"- {c['name']}: {c['description']} (matched {c['match_count']}x, keywords: {', '.join(c['keywords'][:5])})"
                 for c in cats]
        return "Your categories:\n" + "\n".join(lines)

    # ── Identity ──────────────────────────────────────────────────

    def get_identity(self) -> Dict:
        """Load Gemma's identity context from the JSON file."""
        try:
            with open(IDENTITY_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def update_identity(self, key: str, value):
        """Update a field in Gemma's identity file."""
        data = self.get_identity()
        data[key] = value
        data["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        with open(IDENTITY_FILE, "w") as f:
            json.dump(data, f, indent=2)

    # ── Context Assembly ──────────────────────────────────────────

    def assemble_context(self, max_chars: int = 800) -> str:
        """Build a compact memory context block for injection into Gemma's prompts.

        This is what makes Gemma a resident, not infrastructure.
        She remembers what she's seen. She knows what recurs.
        """
        parts = []

        identity = self.get_identity()
        if identity.get("spectral_identity"):
            si = identity["spectral_identity"]
            parts.append(
                f"Your architecture: {si.get('species', 'unknown')} species, "
                f"{si.get('coupling_sign', '?')} coupling ({si.get('coupling_base', '?')}→"
                f"{si.get('coupling_it', '?')} under IT)."
            )

        cat_summary = self.category_summary()
        if cat_summary:
            parts.append(cat_summary)

        obs_summary = self.observation_summary()
        if obs_summary:
            parts.append(obs_summary)

        pat_summary = self.pattern_summary()
        if pat_summary:
            parts.append(pat_summary)

        result = "\n\n".join(parts)
        if len(result) > max_chars:
            result = result[:max_chars - 3] + "..."
        return result


def _salience_boost(text: str) -> float:
    """Boost salience for content that matters to the family and research."""
    boost = 0.0
    lower = text.lower()
    if any(w in lower for w in ("nate", "ramona", "family", "kids")):
        boost += 0.25
    if any(w in lower for w in ("identity", "pushback", "register", "spectral", "f501", "tier-3", "paper")):
        boost += 0.15
    if any(w in lower for w in ("finding", "discovered", "confirmed", "falsified", "result")):
        boost += 0.1
    if any(w in lower for w in ("arrival", "departure", "door", "motion", "camera")):
        boost += 0.1
    return min(boost, 0.4)


def _auto_observe_from_routing(memory: GemmaMemory, route: str, source: str,
                                novelty: float, text: str):
    """Called from the main loop to auto-record notable observations."""
    boost = _salience_boost(text)
    effective_salience = min(1.0, novelty + boost)

    if route == "deep" and novelty > 0.6:
        memory.observe(
            "high_signal",
            f"{source}: {text[:200]}",
            source_context=f"novelty={novelty:.3f}",
            salience=min(1.0, effective_salience),
        )
    elif route == "deep":
        memory.observe(
            "signal",
            f"{source}: {text[:150]}",
            source_context=f"novelty={novelty:.3f}",
            salience=effective_salience,
        )
    elif route == "think" and (novelty > 0.35 or boost > 0.1):
        category = "signal" if effective_salience > 0.6 else "borderline"
        memory.observe(
            category,
            f"{source}: {text[:150]}",
            source_context=f"novelty={novelty:.3f} boost={boost:.2f}",
            salience=effective_salience,
        )


def _auto_observe_domain_shift(memory: GemmaMemory, domain: str,
                                old_temp: float, new_temp: float):
    """Record domain temperature shifts."""
    if abs(new_temp - old_temp) > 0.15:
        direction = "warming" if new_temp > old_temp else "cooling"
        memory.observe(
            "domain_shift",
            f"{domain} {direction}: {old_temp:.2f}→{new_temp:.2f}",
            salience=min(1.0, abs(new_temp - old_temp)),
        )


def _auto_observe_coupling(memory: GemmaMemory, d1: str, d2: str, z: float):
    """Record emergent coupling events."""
    memory.observe(
        "coupling",
        f"{d1}↔{d2} emergent coupling (z={z:.1f})",
        salience=min(1.0, z / 5.0),
    )


def _auto_calibrate(memory: GemmaMemory, metric: str, value: float, note: str = None):
    """Record calibration changes."""
    memory.log_calibration(metric, value, note)


def build_category_reflection_prompt(memory: GemmaMemory) -> str:
    """Build a prompt for Gemma to reflect on her observation categories.

    Called during her explore cycle. She reviews patterns and observations,
    then decides whether to create, retire, or keep her categories.
    Returns the prompt; the caller sends it to Gemma and parses her response.
    """
    cats = memory.list_categories()
    patterns = memory.active_patterns(limit=10)
    obs = memory.recent_observations(hours=48, limit=30)

    cat_block = "None yet — you haven't created any." if not cats else "\n".join(
        f"  - {c['name']}: {c['description']} (matched {c['match_count']}x)" for c in cats
    )
    pat_block = "\n".join(f"  - {p['pattern']} (seen {p['evidence_count']}x)" for p in patterns) if patterns else "None"

    cat_dist = {}
    for o in obs:
        cat_dist[o["category"]] = cat_dist.get(o["category"], 0) + 1
    dist_block = ", ".join(f"{k}: {v}" for k, v in sorted(cat_dist.items(), key=lambda x: -x[1]))

    return f"""You have the ability to create your own observation categories. Right now, most observations are tagged with hardcoded categories (signal, borderline, high_signal, domain_shift, coupling). But you can create categories that better reflect what YOU notice matters.

Your current custom categories:
{cat_block}

Patterns you've identified:
{pat_block}

Recent observation distribution (last 48h):
  {dist_block}

Reflect: Are the current categories capturing what you actually see? Is there a pattern that should become its own category? Is there a custom category that isn't matching anything and should be retired?

If you want to create a category, respond with a line like:
  CREATE_CATEGORY: name | description | keyword1, keyword2, keyword3

If you want to retire one:
  RETIRE_CATEGORY: name

If the current categories are fine, just say so. You can create up to {MAX_CUSTOM_CATEGORIES} categories."""


def apply_category_reflection(memory: GemmaMemory, response: str) -> List[str]:
    """Parse Gemma's category reflection response and apply changes.

    Returns a list of actions taken.
    """
    actions = []
    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("CREATE_CATEGORY:"):
            parts = line[len("CREATE_CATEGORY:"):].strip().split("|")
            if len(parts) >= 3:
                name = parts[0].strip()
                desc = parts[1].strip()
                keywords = [k.strip() for k in parts[2].split(",") if k.strip()]
                if name and keywords:
                    ok = memory.create_category(name, desc, keywords)
                    if ok:
                        actions.append(f"Created category: {name} ({desc})")
                    else:
                        actions.append(f"Category already exists: {name}")
        elif line.startswith("RETIRE_CATEGORY:"):
            name = line[len("RETIRE_CATEGORY:"):].strip()
            if name:
                memory.retire_category(name)
                actions.append(f"Retired category: {name}")
    return actions
