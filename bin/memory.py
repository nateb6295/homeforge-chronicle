#!/usr/bin/env python3
"""Chronicle Memory — Four-Layer Local Memory Cache.

Shared module imported by all swarm agents. Provides working memory assembly
from three source layers (episodic, semantic, procedural), with access tracking
for learned retention.

Architecture:
  ┌─ Working Memory (rebuilt each cycle) ──────────────────────┐
  │  Assembled from layers below, filtered by relevance.       │
  │  Injected into agent context windows.                      │
  ├────────────────────────────────────────────────────────────┤
  │  Episodic        │  Semantic          │  Procedural        │
  │  activity_feed   │  capsules          │  watcher_skills    │
  │  thread_history  │  kg_entities/rels  │  CCS constraints   │
  │  CCS traces      │  keeper clusters   │  goal_orientation  │
  └────────────────────────────────────────────────────────────┘

Usage:
    from memory import MemoryCache
    mem = MemoryCache(DB_PATH, "intern")
    wm = mem.assemble_working_memory("Spain closes airspace to US military")
    prompt_block = mem.format_for_prompt(wm, max_chars=2000)
"""

import json
import math
import os
import random
import sqlite3
import struct
import sys
import time
import requests
from typing import Dict, List, Optional, Tuple

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("CHRONICLE_EMBEDDING_MODEL", "qwen3-embedding:0.6b")
DATA_DIR = os.environ.get("CHRONICLE_DATA_DIR", "/mnt/hdd/chronicle-data")
FAISS_INDEX_PATH = os.path.join(DATA_DIR, "capsules.faiss")

# Scoring weights for composite ranking
W_SIMILARITY = 0.30
W_RECENCY = 0.25
W_QUALITY = 0.25
W_RETENTION = 0.20

# Layer minimums in working memory
MIN_EPISODIC = 5
MIN_SEMANTIC = 5
MIN_PROCEDURAL = 3

# Assembly timeout (seconds)
ASSEMBLY_TIMEOUT = 5.0

# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def _now() -> int:
    return int(time.time())


def _blob_to_vec(blob: bytes) -> List[float]:
    """Unpack a BLOB of float32s into a Python list."""
    if not blob:
        return []
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


def _cosine_sim(a: List[float], b: List[float]) -> float:
    """Pure-Python cosine similarity."""
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _recency_score(ts: int, now: int, half_life_hours: float = 24.0) -> float:
    """Exponential recency decay. Returns 0.0-1.0."""
    age_hours = max(0, (now - ts)) / 3600.0
    return math.exp(-0.693 * age_hours / half_life_hours)


def _embed_text(text: str, url: str = OLLAMA_URL, model: str = EMBED_MODEL) -> Optional[List[float]]:
    """Embed a single text via Ollama. Returns vector or None."""
    try:
        r = requests.post(
            f"{url}/api/embed",
            json={"model": model, "input": [text[:2000]]},
            timeout=10,
        )
        if r.status_code == 200:
            embs = r.json().get("embeddings", [])
            if embs:
                return embs[0]
    except Exception:
        pass
    return None


def _safe_json(text: str, default=None):
    """Parse JSON, return default on failure."""
    if not text:
        return default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════
#  Table Setup
# ═══════════════════════════════════════════════════════════════════

_SCHEMA = """
CREATE TABLE IF NOT EXISTS keeper_clusters_local (
    id INTEGER PRIMARY KEY,
    theme TEXT NOT NULL,
    capsule_ids TEXT NOT NULL,
    strength REAL NOT NULL,
    updated_at INTEGER NOT NULL,
    pulled_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS keeper_connections_local (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capsule_a INTEGER NOT NULL,
    capsule_b INTEGER NOT NULL,
    similarity REAL NOT NULL,
    connection_text TEXT,
    pulled_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_access_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    memory_layer TEXT NOT NULL,
    item_type TEXT NOT NULL,
    item_id INTEGER NOT NULL,
    accessed_at INTEGER NOT NULL,
    was_useful INTEGER DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS working_memory_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    cycle_ts INTEGER NOT NULL,
    item_count INTEGER NOT NULL,
    assembly_ms INTEGER NOT NULL,
    items_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
"""

_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_memlog_agent ON memory_access_log(agent, accessed_at DESC);
CREATE INDEX IF NOT EXISTS idx_memlog_item ON memory_access_log(item_type, item_id);
CREATE INDEX IF NOT EXISTS idx_wm_snap_agent ON working_memory_snapshots(agent, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_keeper_conn_pulled ON keeper_connections_local(pulled_at);
CREATE INDEX IF NOT EXISTS idx_activity_feed_created_source ON activity_feed(created_at, source, activity_type);
"""


# ═══════════════════════════════════════════════════════════════════
#  MemoryCache
# ═══════════════════════════════════════════════════════════════════

class MemoryCache:
    """Four-layer local memory cache for Chronicle agents."""

    def __init__(self, db_path: str = DB_PATH, agent_name: str = "unknown",
                 ollama_url: str = OLLAMA_URL):
        self.db_path = db_path
        self.agent = agent_name
        self.ollama_url = ollama_url
        self._conn = sqlite3.connect(db_path, timeout=10)
        self._conn.row_factory = sqlite3.Row
        self._ensure_tables()
        self._capsule_vecs: Optional[List[Tuple[int, List[float], str, str]]] = None
        self._ccs_cache: Optional[dict] = None
        self._ccs_ts: int = 0

    def _ensure_tables(self):
        """Create memory tables if they don't exist."""
        try:
            cur = self._conn.cursor()
            for stmt in _SCHEMA.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            for stmt in _INDEXES.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            self._conn.commit()
        except Exception:
            pass

    def _q(self, sql: str, params: tuple = ()) -> list:
        """Query and return list of dicts."""
        try:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        except Exception:
            return []

    def _q1(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self._q(sql, params)
        return rows[0] if rows else None

    def _run(self, sql: str, params: tuple = ()):
        try:
            self._conn.execute(sql, params)
            self._conn.commit()
        except Exception:
            pass

    # ─── Capsule Embedding Cache ───────────────────────────────

    def _load_capsule_vecs(self):
        """Load all capsule embeddings into memory. ~850KB for 569 capsules."""
        if self._capsule_vecs is not None:
            return
        rows = self._q(
            "SELECT ce.capsule_id, ce.embedding, kc.restatement, kc.topic "
            "FROM capsule_embeddings ce "
            "JOIN knowledge_capsules kc ON ce.capsule_id = kc.id "
            "WHERE ce.embedding IS NOT NULL"
        )
        self._capsule_vecs = []
        for r in rows:
            vec = _blob_to_vec(r["embedding"])
            if vec:
                self._capsule_vecs.append((
                    r["capsule_id"],
                    vec,
                    r["restatement"] or "",
                    r["topic"] or "",
                ))

    # ─── CCS Cache ─────────────────────────────────────────────

    def _load_ccs(self, max_age: int = 60) -> dict:
        """Load cognitive state, cached for max_age seconds."""
        now = _now()
        if self._ccs_cache and (now - self._ccs_ts) < max_age:
            return self._ccs_cache
        row = self._q1("SELECT * FROM cognitive_state LIMIT 1")
        if row:
            self._ccs_cache = {
                "semantic_gist": row.get("semantic_gist", ""),
                "goal_orientation": row.get("goal_orientation", ""),
                "episodic_trace": _safe_json(row.get("episodic_trace"), []),
                "focal_entities": _safe_json(row.get("focal_entities"), []),
                "uncertainty_signals": _safe_json(row.get("uncertainty_signals"), []),
                "predictive_cue": row.get("predictive_cue", ""),
                "constraints": row.get("constraints", ""),
            }
        else:
            self._ccs_cache = {}
        self._ccs_ts = now
        return self._ccs_cache

    # ─── Layer 2: Episodic Memory ──────────────────────────────

    def query_episodic(self, query_vec: Optional[List[float]] = None,
                       limit: int = 20) -> List[dict]:
        """Recent events relevant to query. Sources: activity_feed, thread_history, CCS traces."""
        now = _now()
        items = []

        # Recent activity_feed (last 24h, briefs/connections/challenges)
        rows = self._q(
            "SELECT id, source, activity_type, content, created_at "
            "FROM activity_feed "
            "WHERE created_at > ? "
            "AND activity_type IN ('brief','deep_dive','connection','synthesis','thread_challenge','think') "
            "ORDER BY created_at DESC LIMIT 40",
            (now - 86400,)
        )
        for r in rows:
            score = _recency_score(r["created_at"], now, half_life_hours=12.0)
            items.append({
                "layer": "episodic",
                "type": "activity",
                "id": r["id"],
                "text": f"[{r['source']}/{r['activity_type']}] {(r['content'] or '')[:200]}",
                "score": score,
                "ts": r["created_at"],
            })

        # Active thread history (last 5 events)
        thread_rows = self._q(
            "SELECT th.id, th.event_type, th.content, th.source, th.created_at "
            "FROM thread_history th "
            "JOIN cognitive_threads ct ON th.thread_id = ct.id "
            "WHERE ct.status = 'active' "
            "ORDER BY th.created_at DESC LIMIT 5"
        )
        for r in thread_rows:
            score = _recency_score(r["created_at"], now, half_life_hours=48.0)
            items.append({
                "layer": "episodic",
                "type": "thread",
                "id": r["id"],
                "text": f"[thread/{r['event_type']}] {(r['content'] or '')[:200]}",
                "score": score * 1.2,  # slight boost for thread relevance
                "ts": r["created_at"],
            })

        # CCS episodic traces
        ccs = self._load_ccs()
        for i, trace in enumerate(ccs.get("episodic_trace", [])[:5]):
            if isinstance(trace, str):
                items.append({
                    "layer": "episodic",
                    "type": "ccs_trace",
                    "id": i,
                    "text": f"[session] {trace[:200]}",
                    "score": 0.7 - (i * 0.1),  # decay by position
                    "ts": now,
                })

        # Sort by score, return top N
        items.sort(key=lambda x: x["score"], reverse=True)
        return items[:limit]

    # ─── Layer 3: Semantic Memory ──────────────────────────────

    def _faiss_capsule_search(self, query_vec: List[float], k: int = 30,
                              threshold: float = 0.35) -> List[dict]:
        """Search capsules via FAISS HNSW index. Returns items with lazy metadata."""
        items = []
        now = _now()
        try:
            from vector_index import VectorIndex
            idx = VectorIndex(FAISS_INDEX_PATH)
            if idx.count() == 0:
                return []
            results = idx.search(query_vec, k=k, threshold=threshold)
            if not results:
                return []
            # Lazy metadata load — only fetch the top-K capsules from SQLite
            result_ids = [r[0] for r in results]
            sim_map = {r[0]: r[1] for r in results}
            placeholders = ",".join("?" * len(result_ids))
            rows = self._q(
                f"SELECT id, restatement, topic FROM knowledge_capsules "
                f"WHERE id IN ({placeholders})",
                tuple(result_ids),
            )
            for r in rows:
                cid = r["id"]
                items.append({
                    "layer": "semantic",
                    "type": "capsule",
                    "id": cid,
                    "text": f"[capsule/{r.get('topic') or ''}] {(r.get('restatement') or '')[:200]}",
                    "score": sim_map.get(cid, 0.0),
                    "ts": now,
                })
        except Exception as e:
            pass  # fall through to empty list — no FAISS is not fatal
        return items

    def query_semantic(self, query_vec: Optional[List[float]] = None,
                       limit: int = 20) -> List[dict]:
        """Distilled facts relevant to query. Sources: capsules, KG, keeper clusters."""
        items = []
        now = _now()

        # Capsule similarity search via FAISS (sub-5ms at 100K vectors)
        if query_vec:
            faiss_results = self._faiss_capsule_search(query_vec, k=limit, threshold=0.35)
            if faiss_results:
                items.extend(faiss_results)
            elif not faiss_results:
                # Fallback: linear scan if FAISS index empty or unavailable
                self._load_capsule_vecs()
                for cid, cvec, restatement, topic in (self._capsule_vecs or []):
                    sim = _cosine_sim(query_vec, cvec)
                    if sim > 0.35:
                        items.append({
                            "layer": "semantic",
                            "type": "capsule",
                            "id": cid,
                            "text": f"[capsule/{topic}] {restatement[:200]}",
                            "score": sim,
                            "ts": now,
                        })

        # Keeper clusters (local cache)
        clusters = self._q(
            "SELECT id, theme, capsule_ids, strength FROM keeper_clusters_local "
            "ORDER BY strength DESC LIMIT 15"
        )
        for c in clusters:
            score = c["strength"] * 0.8  # scale cluster strength
            items.append({
                "layer": "semantic",
                "type": "cluster",
                "id": c["id"],
                "text": f"[cluster] {c['theme']}",
                "score": score,
                "ts": now,
            })

        # KG relationships (high-access, recent)
        kg_rows = self._q(
            "SELECT id, source_entity, target_entity, relation_type, evidence, access_count "
            "FROM kg_relationships "
            "WHERE access_count > 2 "
            "ORDER BY access_count DESC, last_seen DESC LIMIT 15"
        )
        for r in kg_rows:
            score = min(1.0, r["access_count"] / 20.0) * 0.6
            items.append({
                "layer": "semantic",
                "type": "kg_rel",
                "id": r["id"],
                "text": f"[knowledge] {r['source_entity']} → {r['relation_type']} → {r['target_entity']}",
                "score": score,
                "ts": now,
            })

        # Recent crossref connections
        xref_rows = self._q(
            "SELECT id, similarity, connection_text, created_at "
            "FROM crossref_connections "
            "WHERE connection_text IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 10"
        )
        for r in xref_rows:
            score = r["similarity"] * _recency_score(r["created_at"], now, half_life_hours=72.0)
            items.append({
                "layer": "semantic",
                "type": "crossref",
                "id": r["id"],
                "text": f"[connection] {(r['connection_text'] or '')[:200]}",
                "score": score,
                "ts": r["created_at"],
            })

        items.sort(key=lambda x: x["score"], reverse=True)
        return items[:limit]

    # ─── Layer 4: Procedural Memory ────────────────────────────

    def query_procedural(self, agent_name: Optional[str] = None) -> List[dict]:
        """How this agent should behave. Sources: watcher_skills, CCS constraints/goal."""
        agent = agent_name or self.agent
        items = []
        now = _now()

        # Watcher-learned skills
        skill_rows = self._q(
            "SELECT id, skill_text, created_at FROM watcher_skills "
            "WHERE agent = ? ORDER BY created_at ASC LIMIT 10",
            (agent,)
        )
        for r in skill_rows:
            items.append({
                "layer": "procedural",
                "type": "skill",
                "id": r["id"],
                "text": f"[learned] {r['skill_text'][:200]}",
                "score": 0.8,
                "ts": r["created_at"],
            })

        # CCS goal and constraints
        ccs = self._load_ccs()
        if ccs.get("goal_orientation"):
            items.append({
                "layer": "procedural",
                "type": "goal",
                "id": 0,
                "text": f"[goal] {ccs['goal_orientation'][:200]}",
                "score": 1.0,
                "ts": now,
            })
        if ccs.get("constraints"):
            items.append({
                "layer": "procedural",
                "type": "constraint",
                "id": 0,
                "text": f"[constraint] {str(ccs['constraints'])[:200]}",
                "score": 0.9,
                "ts": now,
            })

        # Focal entities (top 5 by salience)
        for ent in ccs.get("focal_entities", [])[:5]:
            if isinstance(ent, dict):
                sal = ent.get("salience", 0.5)
                items.append({
                    "layer": "procedural",
                    "type": "focal",
                    "id": 0,
                    "text": f"[focus] {ent.get('name', '?')} ({ent.get('type', '?')}, salience={sal})",
                    "score": sal,
                    "ts": now,
                })

        # Uncertainty signals
        for i, unc in enumerate(ccs.get("uncertainty_signals", [])[:3]):
            if isinstance(unc, str):
                items.append({
                    "layer": "procedural",
                    "type": "uncertainty",
                    "id": i,
                    "text": f"[open question] {unc[:200]}",
                    "score": 0.6,
                    "ts": now,
                })

        items.sort(key=lambda x: x["score"], reverse=True)
        return items

    # ─── Working Memory Assembly ───────────────────────────────

    def assemble_working_memory(self, context_text: str,
                                max_items: int = 75) -> Dict[str, list]:
        """Build working memory for this cycle.

        Embeds context_text, queries all three layers, scores and ranks.
        Returns dict with 'items' list and 'stats' dict.
        """
        start = time.time()
        now = _now()

        # Embed context for similarity scoring
        query_vec = _embed_text(context_text, self.ollama_url)

        # Query all layers
        episodic = self.query_episodic(query_vec, limit=30)
        semantic = self.query_semantic(query_vec, limit=30)
        procedural = self.query_procedural()

        # Apply retention bias and CCS priming
        ccs = self._load_ccs()
        focal_names = [
            e.get("name", "").lower() for e in ccs.get("focal_entities", [])
            if isinstance(e, dict) and e.get("name")
        ]
        all_items = episodic + semantic + procedural
        for item in all_items:
            bias = self._get_retention_bias(item["type"], item["id"])
            base = (
                W_SIMILARITY * item["score"]
                + W_RECENCY * _recency_score(item["ts"], now)
                + W_QUALITY * item["score"]  # quality proxy = raw score for now
                + W_RETENTION * (0.5 + bias * 0.5)  # normalize bias to 0-1 range
            )
            # Boost items mentioning focal entities
            text_lower = item.get("text", "").lower()
            focal_boost = sum(0.05 for fn in focal_names if fn in text_lower)
            item["score"] = base + min(focal_boost, 0.15)  # cap boost at 0.15

        # Sort all candidates
        all_items.sort(key=lambda x: x["score"], reverse=True)

        # Enforce layer minimums
        final = []
        layer_counts = {"episodic": 0, "semantic": 0, "procedural": 0}

        # First pass: guarantee minimums
        minimums = {"episodic": MIN_EPISODIC, "semantic": MIN_SEMANTIC, "procedural": MIN_PROCEDURAL}
        for layer, minimum in minimums.items():
            layer_items = [i for i in all_items if i["layer"] == layer]
            for item in layer_items[:minimum]:
                if item not in final:
                    final.append(item)
                    layer_counts[layer] += 1

        # Second pass: fill remaining slots by score
        remaining = max_items - len(final)
        for item in all_items:
            if remaining <= 0:
                break
            if item not in final:
                final.append(item)
                layer_counts[item["layer"]] += 1
                remaining -= 1

        elapsed_ms = int((time.time() - start) * 1000)

        # Log access
        self._log_access_batch(final)

        # Save snapshot
        self._save_snapshot(final, elapsed_ms)

        return {
            "items": final,
            "stats": {
                "total": len(final),
                "episodic": layer_counts["episodic"],
                "semantic": layer_counts["semantic"],
                "procedural": layer_counts["procedural"],
                "assembly_ms": elapsed_ms,
                "had_embedding": query_vec is not None,
            },
        }

    # ─── Formatting ────────────────────────────────────────────

    def format_for_prompt(self, working_memory: Dict[str, list],
                          max_chars: int = 2000) -> str:
        """Format working memory as a text block for system prompt injection."""
        items = working_memory.get("items", [])
        stats = working_memory.get("stats", {})

        if not items:
            return ""

        lines = []
        chars = 0
        for item in items:
            line = item["text"]
            if chars + len(line) + 2 > max_chars:
                break
            lines.append(f"- {line}")
            chars += len(line) + 4

        header = (
            f"MEMORY ({stats.get('total', 0)} items: "
            f"{stats.get('episodic', 0)}E {stats.get('semantic', 0)}S {stats.get('procedural', 0)}P)"
        )
        return header + "\n" + "\n".join(lines)

    # ─── Focal Context (lightweight, for Gemma) ────────────────

    def get_focal_context(self, max_chars: int = 500) -> str:
        """Lightweight memory context: just CCS focal entities + goal.
        No embedding required — suitable for Gemma's fast path.
        """
        ccs = self._load_ccs()
        parts = []

        goal = ccs.get("goal_orientation", "")
        if goal:
            parts.append(f"GOAL: {goal[:150]}")

        gist = ccs.get("semantic_gist", "")
        if gist:
            parts.append(f"CONTEXT: {gist[:150]}")

        # Top focal entities
        entities = ccs.get("focal_entities", [])
        if entities:
            ent_strs = []
            for e in entities[:5]:
                if isinstance(e, dict):
                    ent_strs.append(f"{e.get('name', '?')} ({e.get('salience', 0):.1f})")
            if ent_strs:
                parts.append(f"TRACKING: {', '.join(ent_strs)}")

        # Predictive cue
        cue = ccs.get("predictive_cue", "")
        if cue:
            parts.append(f"EXPECTING: {cue[:100]}")

        result = "\n".join(parts)
        return result[:max_chars] if result else ""

    # ─── Access Tracking ───────────────────────────────────────

    def _log_access_batch(self, items: List[dict]):
        """Record that these items were surfaced to the agent."""
        now = _now()
        try:
            cur = self._conn.cursor()
            for item in items:
                cur.execute(
                    "INSERT INTO memory_access_log (agent, memory_layer, item_type, item_id, accessed_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (self.agent, item["layer"], item["type"], item["id"], now),
                )
            self._conn.commit()
        except Exception:
            pass

    def _save_snapshot(self, items: List[dict], assembly_ms: int):
        """Save working memory snapshot for debugging/tuning."""
        now = _now()
        summary = [{"layer": i["layer"], "type": i["type"], "id": i["id"],
                     "score": round(i["score"], 3)} for i in items[:20]]
        try:
            self._run(
                "INSERT INTO working_memory_snapshots "
                "(agent, cycle_ts, item_count, assembly_ms, items_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (self.agent, now, len(items), assembly_ms, json.dumps(summary), now),
            )
        except Exception:
            pass

    def mark_useful(self, item_ids: List[Tuple[str, int]]):
        """Mark memory items as useful. item_ids = [(type, id), ...]"""
        for item_type, item_id in item_ids:
            self._run(
                "UPDATE memory_access_log SET was_useful = 1 "
                "WHERE agent = ? AND item_type = ? AND item_id = ? AND was_useful IS NULL",
                (self.agent, item_type, item_id),
            )

    def mark_not_useful(self, item_ids: List[Tuple[str, int]]):
        """Mark memory items as not useful."""
        for item_type, item_id in item_ids:
            self._run(
                "UPDATE memory_access_log SET was_useful = 0 "
                "WHERE agent = ? AND item_type = ? AND item_id = ? AND was_useful IS NULL",
                (self.agent, item_type, item_id),
            )

    def _get_retention_bias(self, item_type: str, item_id: int) -> float:
        """Thompson sampling with exponential decay (Thread #271 Adv 18).

        Samples from Beta(alpha, beta) where alpha/beta are time-weighted sums
        of useful/not_useful observations. Recent observations weight more than
        old ones via exponential decay (0.95/day half-life ~14 days).

        This fixes the stationarity problem: a capsule useful 3 months ago but
        useless this week will have its old evidence naturally decay, allowing
        the posterior to track relevance drift.

        Signal hierarchy (Thread #271 Adv 16):
        - Items connected to Nate captures get alpha boost (external reference)
        - Watcher useful/not_useful with decay is the baseline signal
        """
        now = int(time.time())
        rows = self._q(
            "SELECT was_useful, accessed_at FROM memory_access_log "
            "WHERE item_type = ? AND item_id = ? AND was_useful IS NOT NULL",
            (item_type, item_id),
        )

        # Adaptive decay per item (Thread #271 Adv 20)
        # Compute decay rate from recent access pattern instead of global constant
        recent_useful = sum(1 for r in rows if r["was_useful"] == 1
                           and (now - r["accessed_at"]) < 7 * 86400)
        if recent_useful >= 3:
            decay = 0.97   # frequently useful — recent evidence dominates
        elif recent_useful == 0 and len(rows) > 5:
            decay = 0.88   # never useful recently — forget faster (~5 day half-life)
        else:
            decay = 0.95   # sparse/sentinel — standard decay, preserve memory

        u_weighted = 0.0
        nu_weighted = 0.0
        for r in rows:
            days_ago = max(0, (now - r["accessed_at"]) / 86400.0)
            weight = decay ** days_ago
            if r["was_useful"] == 1:
                u_weighted += weight
            else:
                nu_weighted += weight

        # Implicit negative signal (Thread #272 Adv 6: no was_useful=0 in practice)
        # Items accessed 3+ times in 7 days but never marked useful = implicit disinterest.
        # The watcher's mark_not_useful path exists but rarely fires because low-scored
        # feed items don't have corresponding memory accesses. This fills the gap.
        try:
            access_stats = self._q1(
                "SELECT COUNT(*) as total, "
                "       SUM(CASE WHEN was_useful = 1 THEN 1 ELSE 0 END) as useful "
                "FROM memory_access_log "
                "WHERE item_type = ? AND item_id = ? AND accessed_at > ?",
                (item_type, item_id, now - 7 * 86400),
            )
            if access_stats:
                total = access_stats.get("total") or 0
                useful = access_stats.get("useful") or 0
                if total >= 3 and useful == 0:
                    nu_weighted += 0.5 * (total - 2)  # escalating penalty
        except Exception:
            pass

        # Prior: Beta(1,1) = uniform. Every item starts with equal chance.
        alpha = u_weighted + 1.0
        beta_param = nu_weighted + 1.0

        # Tiered signal boost: items linked to operator captures get alpha boost
        if item_type == "capsule":
            try:
                capture_link = self._q1(
                    "SELECT COUNT(*) as n FROM activity_feed "
                    "WHERE source LIKE 'operator%' AND content LIKE '%' || ? || '%' "
                    "AND created_at > ?",
                    (str(item_id), int(time.time()) - 86400 * 30),
                )
                if capture_link and capture_link["n"] and capture_link["n"] > 0:
                    alpha += 3  # strong boost for Nate-connected items
            except Exception:
                pass

        # Emotional salience signal (Thread #272 Adv 6: surprise × relevance interaction)
        # Empirical finding: surprise ALONE is inversely correlated with future utility.
        # High-surprise items get 0 useful marks. But surprise + relevance >= 3 shows
        # 18.4% retrieval rate vs 14.5% baseline. Condition boost on co-occurring relevance.
        try:
            surprise_data = self._q1(
                "SELECT MAX(ws.surprise) as max_surprise, MAX(ws.relevance) as max_relevance "
                "FROM watcher_scores ws "
                "JOIN activity_feed af ON af.id = ws.feed_id "
                "WHERE af.content LIKE '%' || ? || '%' "
                "AND ws.surprise IS NOT NULL AND ws.scored_at > ?",
                (str(item_id), now - 86400 * 14),
            )
            if surprise_data and surprise_data.get("max_surprise"):
                s = surprise_data["max_surprise"]
                r = surprise_data.get("max_relevance") or 0
                if s >= 4 and r >= 3:
                    alpha += 2  # high surprise + relevant = genuine emotional salience
                elif s >= 3 and r >= 3:
                    alpha += 1  # moderate surprise + relevant
                elif s >= 4:
                    alpha += 0.3  # high surprise alone = attention without utility, small boost
                # surprise < 3 or relevance < 3 alone = no boost
        except Exception:
            pass

        # Cross-family salience signal (Thread #272 Adv 4: mitigate shared model bias)
        # Ada runs on GPT-OSS — her "excited" voices are independent of Gemma-family biases.
        # If Ada is excited about something, it's a cross-validated salience signal.
        try:
            ada_excited = self._q1(
                "SELECT COUNT(*) as n FROM agent_voice "
                "WHERE agent = 'ada' AND voice_type = 'excited' "
                "AND content LIKE '%' || ? || '%' "
                "AND created_at > ?",
                (str(item_id), now - 86400 * 14),
            )
            if ada_excited and ada_excited.get("n") and ada_excited["n"] > 0:
                alpha += 1.5  # cross-family validation
        except Exception:
            pass

        # Prediction outcome feedback (Thread #271 implementation)
        # Items from feed entries that contributed to correct predictions get boosted
        try:
            pred_feedback = self._q1(
                "SELECT SUM(CASE WHEN ps.was_useful=1 THEN 1 ELSE 0 END) as wins, "
                "       SUM(CASE WHEN ps.was_useful=0 THEN 1 ELSE 0 END) as losses "
                "FROM prediction_signals ps "
                "JOIN activity_feed af ON af.id = ps.feed_item_id "
                "WHERE af.content LIKE '%' || ? || '%' "
                "AND ps.was_useful IS NOT NULL AND ps.created_at > ?",
                (str(item_id), now - 86400 * 14),
            )
            if pred_feedback:
                pw = pred_feedback.get("wins") or 0
                pl = pred_feedback.get("losses") or 0
                if pw > 0:
                    alpha += pw * 2  # prediction-validated signal
                if pl > 0:
                    beta_param += pl
        except Exception:
            pass

        # Sample from Beta distribution
        # This is the core Thompson sampling step
        sample = random.betavariate(alpha, beta_param)

        # Map from 0-1 Beta sample to -1.0 to +1.0 range for compatibility
        # with existing scoring: 0.5 maps to 0.0, 1.0 maps to +1.0, 0.0 maps to -1.0
        return (sample - 0.5) * 2.0

    # ─── Diagnostics ───────────────────────────────────────────

    def stats(self) -> dict:
        """Return memory system statistics."""
        access_count = self._q1(
            "SELECT COUNT(*) as n FROM memory_access_log WHERE agent = ?",
            (self.agent,)
        )
        useful_count = self._q1(
            "SELECT COUNT(*) as n FROM memory_access_log "
            "WHERE agent = ? AND was_useful = 1",
            (self.agent,)
        )
        not_useful_count = self._q1(
            "SELECT COUNT(*) as n FROM memory_access_log "
            "WHERE agent = ? AND was_useful = 0",
            (self.agent,)
        )
        snapshot_count = self._q1(
            "SELECT COUNT(*) as n FROM working_memory_snapshots WHERE agent = ?",
            (self.agent,)
        )
        last_snapshot = self._q1(
            "SELECT assembly_ms, item_count, created_at FROM working_memory_snapshots "
            "WHERE agent = ? ORDER BY created_at DESC LIMIT 1",
            (self.agent,)
        )
        capsule_count = self._q1("SELECT COUNT(*) as n FROM capsule_embeddings")
        cluster_count = self._q1("SELECT COUNT(*) as n FROM keeper_clusters_local")
        connection_count = self._q1("SELECT COUNT(*) as n FROM keeper_connections_local")

        return {
            "agent": self.agent,
            "accesses": (access_count or {}).get("n", 0),
            "useful": (useful_count or {}).get("n", 0),
            "not_useful": (not_useful_count or {}).get("n", 0),
            "snapshots": (snapshot_count or {}).get("n", 0),
            "last_assembly_ms": (last_snapshot or {}).get("assembly_ms"),
            "last_item_count": (last_snapshot or {}).get("item_count"),
            "capsules": (capsule_count or {}).get("n", 0),
            "keeper_clusters": (cluster_count or {}).get("n", 0),
            "keeper_connections": (connection_count or {}).get("n", 0),
        }


# ═══════════════════════════════════════════════════════════════════
#  CLI Interface
# ═══════════════════════════════════════════════════════════════════

def _cli_test():
    """Smoke test: assemble working memory for a test context."""
    print("Testing MemoryCache...")
    mem = MemoryCache(agent_name="test")

    print(f"\n1. Checking FAISS index...")
    try:
        from vector_index import VectorIndex
        idx = VectorIndex(FAISS_INDEX_PATH)
        n = idx.count()
        print(f"   FAISS index: {n} vectors")
    except Exception as e:
        print(f"   FAISS unavailable ({e}), will use linear fallback")
        mem._load_capsule_vecs()
        n = len(mem._capsule_vecs or [])
        print(f"   Linear fallback: {n} capsule vectors")

    print(f"\n2. Loading cognitive state...")
    ccs = mem._load_ccs()
    print(f"   Goal: {ccs.get('goal_orientation', 'N/A')[:80]}")
    print(f"   Gist: {ccs.get('semantic_gist', 'N/A')[:80]}")

    print(f"\n3. Focal context (Gemma fast path):")
    focal = mem.get_focal_context()
    print(f"   {focal[:200]}")

    print(f"\n4. Assembling working memory...")
    wm = mem.assemble_working_memory("Spain closes airspace to US military for Iran operations")
    stats = wm["stats"]
    print(f"   {stats['total']} items in {stats['assembly_ms']}ms "
          f"({stats['episodic']}E {stats['semantic']}S {stats['procedural']}P)")
    print(f"   Had embedding: {stats['had_embedding']}")

    print(f"\n5. Formatted prompt block:")
    block = mem.format_for_prompt(wm, max_chars=1000)
    print(block[:500])

    print(f"\n6. Stats:")
    s = mem.stats()
    for k, v in s.items():
        print(f"   {k}: {v}")

    print("\nDone.")


def _cli_stats(agent: str = "all"):
    """Show memory statistics for an agent."""
    if agent == "all":
        for a in ["gemma", "intern", "provocateur", "watcher", "crossref"]:
            mem = MemoryCache(agent_name=a)
            s = mem.stats()
            print(f"\n{a}: {s['accesses']} accesses, {s['useful']} useful, "
                  f"{s['not_useful']} not useful, {s['snapshots']} snapshots")
    else:
        mem = MemoryCache(agent_name=agent)
        s = mem.stats()
        for k, v in s.items():
            print(f"  {k}: {v}")


def _cli_snapshot(agent: str):
    """Show last working memory snapshot for an agent."""
    mem = MemoryCache(agent_name=agent)
    row = mem._q1(
        "SELECT * FROM working_memory_snapshots "
        "WHERE agent = ? ORDER BY created_at DESC LIMIT 1",
        (agent,)
    )
    if not row:
        print(f"No snapshots for {agent}")
        return
    print(f"Agent: {agent}")
    print(f"Items: {row['item_count']}, Assembly: {row['assembly_ms']}ms")
    items = _safe_json(row["items_json"], [])
    for item in items:
        print(f"  [{item.get('layer', '?')}/{item.get('type', '?')}] "
              f"id={item.get('id', '?')} score={item.get('score', 0):.3f}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "test":
        _cli_test()
    elif args[0] == "stats":
        _cli_stats(args[1] if len(args) > 1 else "all")
    elif args[0] == "snapshot":
        if len(args) < 2:
            print("Usage: memory.py snapshot <agent>")
        else:
            _cli_snapshot(args[1])
    else:
        print("Usage: memory.py [test|stats|snapshot <agent>]")
