#!/usr/bin/env python3
"""Seed Agent — Novelty Router for Homeforge.

A persistent process that watches all input streams in the homeforge
environment, embeds observations, scores novelty via cosine similarity
against a rolling window, and routes to the appropriate depth of thinking.

Intelligence is compression over time. Novelty = compression failure = attend.

Runs on AGX (192.168.1.70). Never exits.
"""

import os, sys, time, math, json, re, signal, sqlite3, struct, subprocess
from datetime import datetime
from typing import Optional, List, Tuple
from collections import deque

import requests

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
JETSON_OLLAMA = os.environ.get("SEED_JETSON_OLLAMA", "http://192.168.1.11:11434")
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
EMBED_MODEL = "chronicle-embed"
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
THINK_MODEL = "hermes3-mind"      # AGX 8B — quick analysis
DEEP_MODEL = "chronicle-deep"    # AGX 32B — deep reasoning
LOOP_INTERVAL = int(os.environ.get("SEED_INTERVAL", "8"))  # seconds between cycles
WINDOW_SIZE = int(os.environ.get("SEED_WINDOW", "200"))     # rolling embedding window
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "1478214472786251837")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")

# Novelty thresholds — calibrated from Experiment #17 analysis
# Original: ignore<0.3, glance<0.6, think<0.8 → produced 0 thinks in 10k obs
# Lowered based on actual distribution: 73.7% scores <0.1, max observed 0.617
THRESH_IGNORE = 0.2
THRESH_GLANCE = 0.35
THRESH_THINK = 0.5

# Entity bias — retrograde feedback from KG to routing
BIAS_REBUILD_INTERVAL = 100   # rebuild every N seed cycles (~13 min)
BIAS_LOOKBACK_DAYS = 7        # how far back to scan routing history
BIAS_MIN_OBS = 3              # minimum observations to assign bias
BIAS_MAX_ENTITIES = 500       # cap entities by mention_count
BIAS_RANGE = 0.3              # max absolute bias adjustment

# Sources that get priority routing — always at least "think" if above ignore
PRIORITY_SOURCES = {"canister", "capture", "discord", "greeting"}

# Sources to skip entirely — NOT embedded or scored.
# system:health (44.6%), thought_stream (23.4%), sentinel monitor (3.1%)
# were drowning signal with near-identical vectors, compressing novelty.
SKIP_SOURCES = set(os.environ.get("SEED_SKIP_SOURCES",
    "system:health,thought_stream,activity:sentinel:monitor_cycle"
).split(","))

# Topics to skip when polling canister (prevents echo loops)
SKIP_CANISTER_TOPICS = {"intern/research", "intern"}

# ═══════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════

def now_ts() -> int:
    return int(time.time())

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    print(f"[{now_iso()}] {msg}", flush=True)

def safe_truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + "..."


def reinforce_capsule_async(capsule_id: int):
    """Fire-and-forget metabolism reinforcement (non-blocking for tight loop)."""
    try:
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        subprocess.Popen(
            [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID,
             "reinforce_capsule", f"({capsule_id} : nat64)",
             "--identity", "chronicle-auto"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        log(f"  Reinforce capsule {capsule_id} (async)")
    except Exception as e:
        log(f"  Reinforce error: {e}")

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self):
        """Create seed tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS seed_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                source TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding BLOB,
                novelty_score REAL DEFAULT 0.0
            );
            CREATE TABLE IF NOT EXISTS seed_routing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                observation_id INTEGER REFERENCES seed_observations(id),
                route TEXT NOT NULL,
                model_used TEXT,
                output TEXT,
                feedback_score REAL
            );
            CREATE TABLE IF NOT EXISTS seed_thresholds (
                category TEXT PRIMARY KEY,
                threshold_low REAL NOT NULL,
                threshold_high REAL NOT NULL,
                last_updated INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_seed_obs_ts ON seed_observations(timestamp);
            CREATE INDEX IF NOT EXISTS idx_seed_obs_source ON seed_observations(source);
            CREATE INDEX IF NOT EXISTS idx_seed_route_ts ON seed_routing_log(timestamp);
            CREATE TABLE IF NOT EXISTS seed_entity_bias (
                entity_id INTEGER PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                entity_type TEXT,
                avg_route_value REAL NOT NULL,
                observation_count INTEGER NOT NULL,
                bias_factor REAL NOT NULL,
                last_rebuilt INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_seed_entity_bias_name
                ON seed_entity_bias(canonical_name);
        """)
        self.conn.commit()

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


# ═══════════════════════════════════════════════════════════════════
#  Embedding & Similarity
# ═══════════════════════════════════════════════════════════════════

def embed_text(text: str) -> Optional[List[float]]:
    """Embed a single text via Ollama on AGX."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": [safe_truncate(text, 500)]},
            timeout=15,
        )
        if r.status_code == 200:
            embs = r.json().get("embeddings", [])
            if embs:
                return embs[0]
    except Exception as e:
        log(f"  Embed error: {e}")
    return None


def cosine_sim(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def vec_to_blob(vec: List[float]) -> bytes:
    """Pack float vector to bytes for SQLite BLOB storage."""
    return struct.pack(f"{len(vec)}f", *vec)


def blob_to_vec(blob: bytes) -> List[float]:
    """Unpack bytes back to float vector."""
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


# ═══════════════════════════════════════════════════════════════════
#  Observation Streams
# ═══════════════════════════════════════════════════════════════════

class ObservationStream:
    """Unified collector that polls all available sources."""

    def __init__(self, db: DB):
        self.db = db
        self._watermarks = {}  # source → last seen id/timestamp
        self._mqtt_client = None
        self._mqtt_queue = deque(maxlen=100)
        self._init_mqtt()

    def _init_mqtt(self):
        """Try to connect to MQTT broker. Non-fatal if unavailable."""
        try:
            import paho.mqtt.client as mqtt
            # paho-mqtt 2.x uses CallbackAPIVersion
            try:
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="seed-router")
            except (AttributeError, TypeError):
                client = mqtt.Client(client_id="seed-router", protocol=mqtt.MQTTv311)
            client.on_message = self._on_mqtt
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.subscribe([
                ("homeforge/home/#", 0),
                ("homeforge/agents/#", 0),
                ("homeforge/prices/#", 0),
                ("frigate/+/+/state", 0),
                ("frigate/events", 0),
                ("frigate/reviews", 0),
                ("homeforge/ear/#", 0),
            ])
            client.loop_start()
            self._mqtt_client = client
            log("MQTT connected")
        except Exception as e:
            log(f"MQTT unavailable (non-fatal): {e}")

    def _on_mqtt(self, _client, _userdata, msg):
        try:
            # Skip binary payloads (snapshots, thumbnails)
            if msg.topic.endswith("/snapshot") or msg.topic.endswith("/thumbnail"):
                return
            payload = msg.payload.decode("utf-8", errors="replace")
            # Parse Frigate events into readable observations
            if msg.topic == "frigate/events" or msg.topic == "frigate/reviews":
                try:
                    event = json.loads(payload)
                    before = event.get("before", {})
                    after = event.get("after", event)
                    label = after.get("label", before.get("label", "unknown"))
                    camera = after.get("camera", before.get("camera", "unknown"))
                    score = after.get("top_score", after.get("score", 0))
                    etype = event.get("type", "update")
                    if etype == "end" or (isinstance(score, (int, float)) and score < 0.5):
                        return  # Skip low-confidence and end events
                    payload = f"Frigate: {label} detected on {camera} (confidence: {score:.0%}, type: {etype})"
                except (json.JSONDecodeError, TypeError):
                    pass
            self._mqtt_queue.append({
                "source": f"mqtt:{msg.topic}",
                "content": f"[{msg.topic}] {payload}",
                "timestamp": now_ts(),
            })
        except Exception:
            pass

    def collect(self) -> List[dict]:
        """Collect new observations from all streams. Returns list of dicts."""
        obs = []
        obs.extend(self._poll_sentinel())
        obs.extend(self._poll_activity())
        obs.extend(self._poll_thoughts())
        obs.extend(self._poll_canister())
        obs.extend(self._drain_mqtt())
        obs.extend(self._poll_system())
        return obs

    def _poll_sentinel(self) -> List[dict]:
        """New alerts since last check."""
        wm = self._watermarks.get("alerts", 0)
        rows = self.db.query(
            "SELECT id, name, alert_type, message, created_at FROM alerts "
            "WHERE id > ? ORDER BY id ASC LIMIT 20", (wm,)
        )
        if rows:
            self._watermarks["alerts"] = rows[-1]["id"]
        return [
            {
                "source": f"sentinel:alert:{r.get('alert_type', 'unknown')}",
                "content": f"{r.get('name', '')} — {r.get('message', '')}".strip(" —"),
                "timestamp": r.get("created_at", now_ts()),
            }
            for r in rows
        ]

    def _poll_activity(self) -> List[dict]:
        """New activity feed entries (excluding our own output to avoid self-referential loops)."""
        wm = self._watermarks.get("activity", 0)
        # First advance watermark past ALL entries (including our own)
        max_row = self.db.query_one(
            "SELECT MAX(id) as m FROM activity_feed WHERE id > ?", (wm,)
        )
        if max_row and max_row["m"]:
            self._watermarks["activity"] = max_row["m"]
        # Then return only non-seed entries for processing
        rows = self.db.query(
            "SELECT id, source, activity_type, title, content, created_at FROM activity_feed "
            "WHERE id > ? AND source NOT IN ('seed', 'intern', 'crossref', 'provocateur') ORDER BY id ASC LIMIT 20", (wm,)
        )
        return [
            {
                "source": f"activity:{r['source']}:{r['activity_type']}",
                "content": f"{r.get('title', '')} — {r.get('content', '')}".strip(" —"),
                "timestamp": r.get("created_at", now_ts()),
            }
            for r in rows
        ]

    def _poll_thoughts(self) -> List[dict]:
        """New thought stream entries (Mind/Sprout cycles)."""
        wm = self._watermarks.get("thoughts", 0)
        rows = self.db.query(
            "SELECT id, cycle_id, reasoning, actions_taken, created_at FROM thought_stream "
            "WHERE id > ? ORDER BY id ASC LIMIT 10", (wm,)
        )
        if rows:
            self._watermarks["thoughts"] = rows[-1]["id"]
        return [
            {
                "source": "thought_stream",
                "content": safe_truncate(r.get("reasoning", ""), 300),
                "timestamp": r.get("created_at", now_ts()),
            }
            for r in rows
        ]

    def _poll_canister(self) -> List[dict]:
        """Poll ICP canister for new capsules (every 10th call ~= ~80s)."""
        count = self._watermarks.get("_canister_count", 0) + 1
        self._watermarks["_canister_count"] = count
        if count % 10 != 0:
            return []
        try:
            token = ""
            try:
                with open(TOKEN_PATH) as f:
                    token = f.read().strip()
            except Exception:
                pass
            wm = self._watermarks.get("canister_id", 0)
            r = requests.get(
                f"{CANISTER_URL}/api/recent",
                params={"limit": 10, "token": token},
                timeout=15,
            )
            if r.status_code != 200:
                return []
            capsules = r.json().get("capsules", [])
            obs = []
            for c in capsules:
                cid = c.get("id", 0)
                if cid <= wm:
                    continue
                topic = c.get("topic") or ""
                # Skip intern capsules to prevent echo loops
                if any(skip in topic for skip in SKIP_CANISTER_TOPICS):
                    continue
                text = c.get("restatement", "")
                persons = ", ".join(c.get("persons", []))
                content = f"[capsule:{cid}] {text}"
                if topic:
                    content += f" (topic: {topic})"
                if persons:
                    content += f" (from: {persons})"
                # Canister may return ISO timestamps or unix ints
                raw_ts = c.get("timestamp", now_ts())
                try:
                    ts = int(raw_ts)
                except (ValueError, TypeError):
                    try:
                        from datetime import datetime as _dt, timezone as _tz
                        ts = int(_dt.fromisoformat(str(raw_ts).replace("Z", "+00:00")).timestamp())
                    except Exception:
                        ts = now_ts()
                obs.append({
                    "source": "canister:capsule",
                    "content": content,
                    "timestamp": ts,
                })
            if capsules:
                max_id = max(c.get("id", 0) for c in capsules)
                if max_id > wm:
                    self._watermarks["canister_id"] = max_id
            return obs
        except Exception as e:
            log(f"  Canister poll error: {e}")
            return []

    def _drain_mqtt(self) -> List[dict]:
        """Drain queued MQTT messages."""
        items = []
        while self._mqtt_queue:
            items.append(self._mqtt_queue.popleft())
        return items

    def _poll_system(self) -> List[dict]:
        """Periodic system state (every 5th call only)."""
        count = self._watermarks.get("_sys_count", 0) + 1
        self._watermarks["_sys_count"] = count
        if count % 5 != 0:
            return []
        try:
            load = os.getloadavg()
            stat = os.statvfs("/")
            free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
            content = f"system: load={load[0]:.1f}/{load[1]:.1f}/{load[2]:.1f} disk_free={free_gb:.1f}GB"
            return [{"source": "system:health", "content": content, "timestamp": now_ts()}]
        except Exception:
            return []

    def shutdown(self):
        if self._mqtt_client:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()


# ═══════════════════════════════════════════════════════════════════
#  Novelty Router
# ═══════════════════════════════════════════════════════════════════

class NoveltyRouter:
    """Embed observations, compare against rolling window, score novelty."""

    def __init__(self, db: DB, window_size: int = WINDOW_SIZE):
        self.db = db
        self.window: deque = deque(maxlen=window_size)  # list of embedding vectors
        self._entity_bias_cache: dict = {}  # lowercase name -> bias_factor
        self._load_recent_window()
        self._refresh_bias_cache()

    def _load_recent_window(self):
        """Warm up the window from recent DB observations."""
        rows = self.db.query(
            "SELECT embedding FROM seed_observations "
            "WHERE embedding IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT ?",
            (WINDOW_SIZE,),
        )
        for r in reversed(rows):  # oldest first
            if r["embedding"]:
                self.window.append(blob_to_vec(r["embedding"]))
        log(f"Window loaded: {len(self.window)} embeddings")

    def score(self, text: str) -> Tuple[float, Optional[List[float]]]:
        """Embed text and return (novelty_score, embedding). Higher = more novel."""
        vec = embed_text(text)
        if vec is None:
            return 0.0, None

        if not self.window:
            # First observation is maximally novel
            return 1.0, vec

        # Compare against window — novelty = 1 - max_similarity
        max_sim = max(cosine_sim(vec, w) for w in self.window)
        novelty = 1.0 - max_sim
        return novelty, vec

    def add_to_window(self, vec: List[float]):
        self.window.append(vec)

    def rebuild_entity_bias(self):
        """Rebuild entity bias scores from KG routing history. Runs periodically."""
        cutoff = now_ts() - (BIAS_LOOKBACK_DAYS * 86400)

        # 1. Load all recent routed observations with their route decisions
        rows = self.db.query(
            "SELECT o.id, LOWER(o.content) as content, r.route "
            "FROM seed_observations o "
            "JOIN seed_routing_log r ON r.observation_id = o.id "
            "WHERE o.timestamp > ?",
            (cutoff,),
        )
        if not rows:
            return

        # 2. Load top KG entities by mention count
        entities = self.db.query(
            "SELECT id, canonical_name, entity_type, aliases FROM kg_entities "
            "WHERE mention_count >= 2 "
            "ORDER BY mention_count DESC LIMIT ?",
            (BIAS_MAX_ENTITIES,),
        )
        if not entities:
            return

        ROUTE_VAL = {"ignore": 0.0, "glance": 0.25, "think": 0.75, "deep": 1.0}

        # 3. For each entity, find matching observations and compute avg route value
        bias_rows = []
        for ent in entities:
            names = [ent["canonical_name"].lower()]
            if ent["aliases"]:
                try:
                    aliases = json.loads(ent["aliases"])
                    names.extend(a.lower() for a in aliases if len(a) >= 4)
                except Exception:
                    pass
            # Skip very short names to avoid false matches
            names = [n for n in names if len(n) >= 4]
            if not names:
                continue

            matches = []
            for obs in rows:
                if any(n in obs["content"] for n in names):
                    matches.append(ROUTE_VAL.get(obs["route"], 0.5))

            if len(matches) >= BIAS_MIN_OBS:
                avg_val = sum(matches) / len(matches)
                bias = max(-BIAS_RANGE, min(BIAS_RANGE, (avg_val - 0.5) * 0.6))
                bias_rows.append((
                    ent["id"], ent["canonical_name"], ent["entity_type"],
                    round(avg_val, 4), len(matches), round(bias, 4), now_ts(),
                ))

        # 4. Replace table contents
        self.db.run("DELETE FROM seed_entity_bias")
        for row in bias_rows:
            self.db.run(
                "INSERT INTO seed_entity_bias VALUES (?, ?, ?, ?, ?, ?, ?)", row,
            )

        neg = sum(1 for r in bias_rows if r[5] < -0.05)
        pos = sum(1 for r in bias_rows if r[5] > 0.05)
        log(f"  Entity bias rebuilt: {len(bias_rows)} entities (suppress={neg}, boost={pos})")

    def _refresh_bias_cache(self):
        """Load entity bias into memory for fast hot-path lookup."""
        try:
            rows = self.db.query("SELECT canonical_name, bias_factor FROM seed_entity_bias")
            self._entity_bias_cache = {r["canonical_name"].lower(): r["bias_factor"] for r in rows}
            if self._entity_bias_cache:
                log(f"  Bias cache: {len(self._entity_bias_cache)} entities")
        except Exception:
            self._entity_bias_cache = {}

    def get_entity_bias(self, text: str) -> float:
        """Fast entity bias lookup. Returns bias adjustment for observation text."""
        if not self._entity_bias_cache:
            return 0.0
        text_lower = text.lower()
        biases = [
            bias for name, bias in self._entity_bias_cache.items()
            if name in text_lower
        ]
        if not biases:
            return 0.0
        # Negative bias dominates — sensor noise is a stronger signal than entity boost
        if min(biases) < -0.05:
            return min(biases)
        return max(biases)

    def route(self, novelty: float, source: str, text: str = "") -> str:
        """Determine route based on novelty score, source category, and entity bias."""
        # Apply entity bias from KG retrograde feedback
        entity_adj = self.get_entity_bias(text) if text else 0.0
        adjusted = max(0.0, min(1.0, novelty + entity_adj))

        # Check per-category thresholds first
        category = source.split(":")[0]
        custom = self.db.query_one(
            "SELECT threshold_low, threshold_high FROM seed_thresholds WHERE category = ?",
            (category,),
        )
        low = custom["threshold_low"] if custom else THRESH_IGNORE
        high = custom["threshold_high"] if custom else THRESH_THINK

        # Priority sources: if above ignore, bump to at least "think"
        is_priority = any(p in source for p in PRIORITY_SOURCES)

        if adjusted < low:
            return "glance" if is_priority else "ignore"
        elif adjusted < THRESH_GLANCE:
            return "think" if is_priority else "glance"
        elif adjusted < high:
            return "think"
        else:
            # Cap canister:capsule at think — intern skips deep canister
            # re-observations, so deep output is wasted 32B budget
            if source == "canister:capsule":
                return "think"
            return "deep"


# ═══════════════════════════════════════════════════════════════════
#  Action Dispatch
# ═══════════════════════════════════════════════════════════════════

class ActionDispatch:
    """Fire the appropriate model or action based on route."""

    @staticmethod
    def execute(route: str, observation: dict, db: DB) -> Optional[str]:
        if route == "ignore":
            return None
        if route == "glance":
            return None  # Phase 1: just store, no model call
        if route == "think":
            return ActionDispatch._think(observation)
        if route == "deep":
            return ActionDispatch._deep_think(observation, db)
        return None

    @staticmethod
    def _think(obs: dict) -> Optional[str]:
        """Quick analysis via hermes3-mind 8B on AGX."""
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": THINK_MODEL,
                    "messages": [
                        {"role": "system", "content":
                            "State what pattern is changing and why it matters. "
                            "Be specific — name the mechanism, not the category. "
                            "1-2 sentences. Never start with 'This observation' or similar filler."},
                        {"role": "user", "content":
                            f"Source: {obs['source']}\n"
                            f"Observation: {obs['content']}"},
                    ],
                    "stream": False,
                    "options": {"num_predict": 128, "temperature": 0.3},
                },
                timeout=120,
            )
            if r.status_code == 200:
                return r.json().get("message", {}).get("content", "")
        except Exception as e:
            log(f"  Think error: {e}")
        return None

    @staticmethod
    def _deep_think(obs: dict, db: DB) -> Optional[str]:
        """Deep reflection via hermes3 on AGX. May produce alerts."""
        try:
            # Gather recent context
            recent = db.query(
                "SELECT content, novelty_score FROM seed_observations "
                "WHERE novelty_score > 0.5 ORDER BY timestamp DESC LIMIT 5"
            )
            context = "\n".join(
                f"- [{r['novelty_score']:.2f}] {safe_truncate(r['content'], 150)}"
                for r in recent
            )

            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": DEEP_MODEL,
                    "messages": [
                        {"role": "system", "content":
                            "You are the deep-thinking layer of a homeforge novelty detection system. "
                            "You only get called when something genuinely surprising happens. "
                            "Analyze the observation in context. What's surprising here? "
                            "What does this imply? Should the operator be alerted? "
                            "Be concise — 2-4 sentences max."},
                        {"role": "user", "content":
                            f"NOVEL OBSERVATION (source: {obs['source']}):\n"
                            f"{obs['content']}\n\n"
                            f"RECENT HIGH-NOVELTY CONTEXT:\n{context}"},
                    ],
                    "stream": False,
                    "options": {"num_predict": 1024, "temperature": 0.5, "num_ctx": 4096},
                },
                timeout=300,
            )
            if r.status_code == 200:
                output = r.json().get("message", {}).get("content", "")
                # Alert operator via MQTT if available
                ActionDispatch._publish_alert(obs, output)
                return output
        except Exception as e:
            log(f"  Deep think error: {e}")
        return None

    @staticmethod
    def _publish_alert(obs: dict, insight: str):
        """Publish high-novelty insight to MQTT for visibility."""
        try:
            import paho.mqtt.publish as publish
            publish.single(
                "homeforge/seed/insight",
                json.dumps({
                    "source": obs["source"],
                    "observation": safe_truncate(obs["content"], 200),
                    "insight": insight,
                    "timestamp": now_ts(),
                }),
                hostname=MQTT_BROKER,
                port=MQTT_PORT,
            )
        except Exception:
            pass  # MQTT is best-effort


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Seed Agent starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (embed/deep) | {JETSON_OLLAMA} (think)")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Window: {WINDOW_SIZE} | Interval: {LOOP_INTERVAL}s")
    log(f"Thresholds: ignore<{THRESH_IGNORE} glance<{THRESH_GLANCE} think<{THRESH_THINK} deep>=")
    log(f"Skip canister topics: {SKIP_CANISTER_TOPICS}")

    db = DB(DB_PATH)
    stream = ObservationStream(db)
    router = NoveltyRouter(db)
    dispatch = ActionDispatch()

    # Graceful shutdown
    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    stats = {"ignore": 0, "glance": 0, "think": 0, "deep": 0, "errors": 0}

    while running:
        cycle += 1
        try:
            observations = stream.collect()

            for obs in observations:
                text = obs["content"]
                if not text or len(text.strip()) < 5:
                    continue

                # Skip content-free queue notifications (sprout captures, etc.)
                # These are identical metadata strings that waste embedding calls
                # and pollute the novelty window with duplicate vectors.
                _lower = text.lower()
                if any(skip in _lower for skip in (
                    "phone capture queued",
                    "capture queued type:",
                )):
                    continue

                # Skip sources that flood the window with low-value embeddings
                if obs["source"] in SKIP_SOURCES:
                    continue

                novelty, vec = router.score(text)

                if vec is None:
                    stats["errors"] += 1
                    continue

                # Store observation
                obs_id = db.run(
                    "INSERT INTO seed_observations (timestamp, source, content, embedding, novelty_score) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (obs["timestamp"], obs["source"], safe_truncate(text, 2000), vec_to_blob(vec), novelty),
                )

                # Route (with entity bias from KG retrograde feedback)
                route = router.route(novelty, obs["source"], text)
                output = dispatch.execute(route, obs, db)

                # Log routing decision
                routing_log_id = db.run(
                    "INSERT INTO seed_routing_log (timestamp, observation_id, route, model_used, output) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        now_ts(), obs_id, route,
                        THINK_MODEL if route == "think" else DEEP_MODEL if route == "deep" else None,
                        safe_truncate(output, 2000) if output else None,
                    ),
                )

                # Reinforce metabolism for canister capsules that triggered think/deep
                if route in ("think", "deep") and obs.get("source") == "canister:capsule":
                    m = re.search(r'\[capsule:(\d+)\]', obs.get("content", ""))
                    if m:
                        reinforce_capsule_async(int(m.group(1)))

                # Log to activity_feed for dashboard visibility
                if route in ("think", "deep"):
                    title = f"Seed [{route}] {obs['source']} (novelty={novelty:.2f})"
                    content = output or safe_truncate(obs["content"], 300)
                    # Store original observation in metadata so downstream
                    # consumers (Intern) can use the raw content for search
                    entity_bias = router.get_entity_bias(text)
                    metadata = json.dumps({
                        "original_source": obs["source"],
                        "original_content": safe_truncate(obs["content"], 1000),
                        "novelty": round(novelty, 3),
                        "entity_bias": round(entity_bias, 3) if abs(entity_bias) > 0.01 else 0,
                        "routing_log_id": routing_log_id,
                    })
                    db.run(
                        "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        ("seed", route, safe_truncate(title, 200), safe_truncate(content, 2000), metadata, now_ts()),
                    )

                # Update window
                router.add_to_window(vec)
                stats[route] += 1

                if route != "ignore":
                    bias_str = ""
                    entity_adj = router.get_entity_bias(text)
                    if abs(entity_adj) > 0.01:
                        bias_str = f" (bias={entity_adj:+.3f})"
                    log(f"  [{obs['source']}] novelty={novelty:.3f}{bias_str} → {route}"
                        + (f" → {safe_truncate(output, 80)}" if output else ""))

            # Periodic stats (every 50 cycles ~= every ~7 min)
            if cycle % 50 == 0:
                total = sum(stats.values())
                log(f"Stats @ cycle {cycle}: {stats} (total={total}, window={len(router.window)})")

            # Periodic entity bias rebuild from KG
            if cycle % BIAS_REBUILD_INTERVAL == 0 and cycle > 0:
                try:
                    router.rebuild_entity_bias()
                    router._refresh_bias_cache()
                except Exception as e:
                    log(f"  Entity bias rebuild error: {e}")

        except Exception as e:
            log(f"Cycle error: {e}")
            stats["errors"] += 1

        time.sleep(LOOP_INTERVAL)

    # Cleanup
    stream.shutdown()
    db.close()
    log("═══ Seed Agent stopped ═══")


if __name__ == "__main__":
    main()
