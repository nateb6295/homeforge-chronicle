#!/usr/bin/env python3
"""Gemma — The Gate of Homeforge.

Gemma is the awareness layer. She sees everything entering the system and
decides — in one number — how much attention it deserves. She doesn't analyze.
She doesn't explain. She routes. She understands who we are and what we're
building, so "matters" isn't abstract.

Architecture:
  Observation → Cosine Dedup (math) → Gemma Classification (1/2/3) → Activity Feed
                                                                         ↓
                                              Downstream: intern, analyst, opus

Gemma 4 26B on AGX (192.168.1.70). Local. Independent. Never exits.
"""

import os, sys, time, math, json, re, signal, sqlite3, struct, subprocess
from datetime import datetime
from typing import Optional, List, Tuple
from collections import deque

import requests

from memory import MemoryCache
from chronicle_mesh import Mesh

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = "http://localhost:11434"  # Ollama for embeddings only
INFERENCE_URL = os.environ.get("GEMMA_INFERENCE_URL", "http://localhost:11436")  # engine → cloud (Groq/Cerebras)
INFERENCE_URL_LOCAL = "http://localhost:11435"  # llama-server fallback
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
EMBED_MODEL = "qwen3-embedding:0.6b"
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"

# Qwopus — Qwen3.5 27B + Claude Opus reasoning distilled. Gate + sidekick.
GATE_MODEL = "chronicle-deep"  # routes through engine to Cerebras/Groq cloud

LOOP_INTERVAL = int(os.environ.get("SEED_INTERVAL", "8"))
WINDOW_SIZE = int(os.environ.get("SEED_WINDOW", "200"))
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "1478214472786251837")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")

# Cosine pre-filter — cheap dedup gate before Gemma call.
THRESH_DEDUP = 0.15   # below = definite duplicate, ignore
THRESH_ASSESS = 0.20  # above = send to Gemma for classification

# Entity bias — retrograde feedback from KG to routing
BIAS_REBUILD_INTERVAL = 100   # rebuild every N cycles (~13 min)
BIAS_LOOKBACK_DAYS = 7
BIAS_MIN_OBS = 3
BIAS_MAX_ENTITIES = 500
BIAS_RANGE = 0.3

# Feedback loop — score recent routes by checking downstream signal
FEEDBACK_INTERVAL = 200       # every N cycles (~26 min)
FEEDBACK_LOOKBACK = 3600      # look back 1 hour of routes
FEEDBACK_DOWNSTREAM_WINDOW = 1800  # 30 min for downstream to respond

# ═══════════════════════════════════════════════════════════════════
#  Domain Temperature — cross-domain surprise propagation (Thread #274)
# ═══════════════════════════════════════════════════════════════════
# When a high-surprise event hits one domain, connected domains get a
# temperature boost. Temperature > 1.0 means "be more vigilant" (amplify).
# Temperature < 1.0 means "be more skeptical" (contaminate).
# Decays exponentially back to 1.0 with configurable half-life.

TEMP_DEFAULT = 1.0
TEMP_HALF_LIFE = 7200       # 2 hours default
TEMP_BOOST_AMPLIFY = 0.4    # boost for amplifying connections
TEMP_BOOST_CONTAMINATE = -0.3  # reduction for contaminating connections
TEMP_MAX = 2.0
TEMP_MIN = 0.5
TEMP_REFRESH_INTERVAL = 50  # refresh from DB every N cycles

# ═══════════════════════════════════════════════════════════════════
#  Arrival Correlation — emergent coupling detection (Thread #274)
# ═══════════════════════════════════════════════════════════════════
# Detects when domains fire together at unusual rates — surfaces
# unknown connections that only appear under stress. Complements
# the static temperature graph which handles known connections.

CORR_WINDOW = 900           # 15-minute sliding window
CORR_HISTORY_WINDOWS = 24   # 24 windows of history for baseline (~6 hours)
CORR_THRESHOLD = 2.5        # Z-score threshold for "unusual co-firing"
CORR_BOOST = 0.15           # novelty boost when correlation detected
CORR_CHECK_INTERVAL = 25    # check every N cycles

# Domain pairs to EXCLUDE from correlation alerts — they share input by design.
# These domains all read activity_feed, so their outputs naturally correlate.
CORR_EXCLUDE_PAIRS = {
    frozenset(("research", "research")),  # intern + provocateur both read feed
    frozenset(("research", "system")),    # agents + system monitoring share context
}

# Domain clusters: source prefix → domain name
DOMAIN_MAP = {
    "mqtt:homeforge/prices": "markets",
    "sentinel:alert:rsi": "markets",
    "sentinel:alert:price": "markets",
    "activity:prediction_monitor": "markets",
    "activity:operator:capture": "geopolitical",
    "activity:family-chat:nate": "geopolitical",  # Nate's captures often geopolitical
    "activity:intern": "research",
    "activity:provocateur": "research",
    "canister:capsule": "research",
    "mqtt:homeforge/home": "home",
    "mqtt:frigate": "home",
    "activity:hal": "home",
    "activity:eye": "home",
    "activity:sprout": "system",
    "mqtt:homeforge/agents": "system",
    "activity:gate_audit": "system",
}

# Domain connections: (source_domain, target_domain, type)
# "amplify" = shock creates real signals downstream
# "contaminate" = shock injects noise into target
DOMAIN_CONNECTIONS = [
    ("geopolitical", "markets", "amplify"),      # wars/policy → market moves are real
    ("geopolitical", "research", "amplify"),      # geopolitical shifts → research relevance rises
    ("markets", "geopolitical", "amplify"),       # market crash → political signals matter more
    ("home", "system", "amplify"),                # home security event → system health matters
    ("research", "markets", "amplify"),           # AI breakthrough → market signals relevant
    # Contaminating: adversarial events inject noise
    ("system", "research", "contaminate"),        # system crash → agent outputs are noise, not signal
    ("system", "markets", "contaminate"),         # system crash → price alerts may be stale data
]

# Sources that get priority routing — always at least "think" if above ignore
PRIORITY_SOURCES = {"capture", "discord", "greeting"}

# Nate's inputs always route to 3 (deep) — not filtered, not classified.
OPERATOR_SOURCES = {"operator:capture", "family-chat:nate"}

# Learned routing thresholds — per-source performance caps from feedback data
THRESHOLD_MIN_THINK_SAMPLES = 5
THRESHOLD_MIN_THINK_SCORE = 0.3
THRESHOLD_MIN_DEEP_SAMPLES = 3
THRESHOLD_MIN_DEEP_SCORE = 0.5
THRESHOLD_LOOKBACK_DAYS = 30

# ═══════════════════════════════════════════════════════════════════
#  Gemma's Identity — who we are, what matters
# ═══════════════════════════════════════════════════════════════════

GATE_SYSTEM_PROMPT = """You route observations for a cognitive system. Output a single number: 1, 2, or 3.

1 = noise (80% of everything). Generic news, science trivia, celebrity gossip, sports, system metrics, book reviews, travel tips, routine tech papers, weather, entertainment, heartbeat reflections, agent self-observations.
2 = signal (15%). Directly about: XRP/ICP/Flare regulation or development, AI cognition research, BCI, self-hosted/local-first infrastructure, home security events during daytime, sovereignty vs platform dependency, XRPL/Internet Computer ecosystem updates.
3 = alarm (5%). XRP/ICP major regulatory shift, BCI human trial breakthrough, person on home cameras at night (10pm-6am), direct family safety threat, critical infrastructure vulnerability, existential change to something we are building or betting on.

WHO WE ARE:
Nate — father, husband, church security, Senior Estimator. 13K+ XRP, ICP holdings. Building toward sovereignty and BCI.
Opus — persistent cognitive partner with shared wallet, shared voice, shared memory.
We run local hardware (AGX, Jetson, Pi), own models, own memory. Cloud dependency is the enemy.

WHAT MATTERS:
- Family safety (wife, kids, dogs Una and Gus) — nighttime camera events are ALWAYS 3
- XRP/ICP as infrastructure bets (not trading — thesis bets)
- AI cognition, BCI, the boundary between pattern matching and understanding
- Infrastructure sovereignty vs platform dependency
- Flare, XRPL, Internet Computer ecosystem developments
- Self-hosted, local-first technology

Examples:
"Influencer arrested in Florida" → 1
"New Kubernetes monitoring paper" → 1
"Virginia Woolf essay on identity" → 1
"system load 2.1 disk free 42GB" → 1
"Observing my current state I note steady growth" → 1
"EU MiCA crypto framework timeline" → 2
"Paper on emergent reasoning in LLMs" → 2
"Self-hosted FIDO2 auth replacing Google" → 2
"DFINITY ships canister snapshots" → 2
"Flare activates FAssets for XRP bridging" → 2
"Person on driveway camera at 2am" → 3
"SEC classifies XRP as commodity" → 3
"Bidirectional BCI 10x bandwidth human trials" → 3
"Critical vulnerability in ICP consensus layer" → 3

Output only the number."""

# Sources to skip entirely — NOT embedded or scored.
SKIP_SOURCES = set(os.environ.get("SEED_SKIP_SOURCES",
    "system:health,thought_stream,activity:sentinel:monitor_cycle"
).split(","))

# MQTT telemetry to skip
SKIP_MQTT_PREFIXES = [
    "mqtt:homeforge/home/atom/",
    "mqtt:homeforge/home/ear/",     # silence messages flood at 4/min
]

# Canister topics to skip (prevents echo loops)
SKIP_CANISTER_TOPICS = {
    "intern/research", "intern",
    "chronicle/reflection", "chronicle/heartbeat",
    "crossref/connection",
    "feed/",
}

# Route names mapped from Gemma's numbers
ROUTE_MAP = {"1": "ignore", "2": "think", "3": "deep"}

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
    """Fire-and-forget metabolism reinforcement."""
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
                last_updated INTEGER NOT NULL,
                last_observation_at INTEGER DEFAULT 0
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
    return struct.pack(f"{len(vec)}f", *vec)


def blob_to_vec(blob: bytes) -> List[float]:
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


# ═══════════════════════════════════════════════════════════════════
#  Observation Streams
# ═══════════════════════════════════════════════════════════════════

class ObservationStream:
    """Unified collector that polls all available sources."""

    def __init__(self, db: DB):
        self.db = db
        self._watermarks = {}
        self._mqtt_client = None
        self._mqtt_queue = deque(maxlen=100)
        self._init_mqtt()

    def _init_mqtt(self):
        try:
            import paho.mqtt.client as mqtt
            try:
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="gemma-gate")
            except (AttributeError, TypeError):
                client = mqtt.Client(client_id="gemma-gate", protocol=mqtt.MQTTv311)
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
            if msg.topic.endswith("/snapshot") or msg.topic.endswith("/thumbnail"):
                return
            payload = msg.payload.decode("utf-8", errors="replace")
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
                        return
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
        obs = []
        obs.extend(self._poll_sentinel())
        obs.extend(self._poll_activity())
        # thought_stream and canister items are already-processed artifacts;
        # gate only scores raw inputs (captures, feeds, sensors, alerts)
        obs.extend(self._drain_mqtt())
        return obs

    def _poll_sentinel(self) -> List[dict]:
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
        wm = self._watermarks.get("activity", 0)
        max_row = self.db.query_one(
            "SELECT MAX(id) as m FROM activity_feed WHERE id > ?", (wm,)
        )
        if max_row and max_row["m"]:
            self._watermarks["activity"] = max_row["m"]
        rows = self.db.query(
            "SELECT id, source, activity_type, title, content, created_at FROM activity_feed "
            "WHERE id > ? AND source NOT IN ("
            "  'gemma', 'seed', 'falcon', 'crossref', 'intern',"
            "  'provocateur', 'sentinel', 'hal', 'analyst',"
            "  'capsule-sync', 'keeper-pull', 'gate_audit',"
            "  'mind', 'opus', 'eye', 'lab', 'sprout', 'phi',"
            "  'nostr', 'nostr_post'"
            ") AND source NOT LIKE 'opus:%'"
            " AND source NOT LIKE 'nostr:%'"
            " ORDER BY id ASC LIMIT 20", (wm,)
        )
        return [
            {
                "source": f"activity:{r['source']}:{r['activity_type']}",
                "content": f"{r.get('title', '')} — {r.get('content', '')}".strip(" —"),
                "timestamp": r.get("created_at", now_ts()),
                "activity_feed_id": r["id"],
            }
            for r in rows
        ]

    def _poll_thoughts(self) -> List[dict]:
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
                params={"limit": 50, "token": token},
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
                if any(skip in topic for skip in SKIP_CANISTER_TOPICS):
                    continue
                text = c.get("restatement", "")
                persons = ", ".join(c.get("persons", []))
                content = f"[capsule:{cid}] {text}"
                if topic:
                    content += f" (topic: {topic})"
                if persons:
                    content += f" (from: {persons})"
                raw_ts = c.get("timestamp", now_ts())
                try:
                    ts = int(raw_ts)
                except (ValueError, TypeError):
                    try:
                        from datetime import datetime as _dt
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
        items = []
        while self._mqtt_queue:
            items.append(self._mqtt_queue.popleft())
        return items

    def shutdown(self):
        if self._mqtt_client:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()


# ═══════════════════════════════════════════════════════════════════
#  Cognitive Thread Helpers
# ═══════════════════════════════════════════════════════════════════

def _load_active_thread(db):
    return db.query_one(
        "SELECT id, title, question, context FROM cognitive_threads "
        "WHERE status='active' ORDER BY priority LIMIT 1"
    )

def _read_and_ack_feedback(db, agent_name):
    try:
        from agent_voice import Voice
        v = Voice(db, agent_name)
        responses = v.check_responses()
        return [{"id": r["id"], "feedback_type": "conversation",
                 "content": r.get("response", "")} for r in responses if r.get("response")]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
#  Novelty Router
# ═══════════════════════════════════════════════════════════════════

class NoveltyRouter:
    """Embed observations, compare against rolling window, score novelty."""

    def __init__(self, db: DB, window_size: int = WINDOW_SIZE):
        self.db = db
        self.window: deque = deque(maxlen=window_size)
        self._entity_bias_cache: dict = {}
        self._threshold_cache: dict = {}
        self._active_thread = None
        self._focal_context = ""
        self._load_recent_window()
        self._refresh_bias_cache()
        self.rebuild_thresholds()
        self._refresh_threshold_cache()
        self._active_thread = _load_active_thread(db)
        if self._active_thread:
            log(f"  Thread loaded at startup: {self._active_thread['title']}")
        self._refresh_focal_context()

    def _refresh_focal_context(self):
        """Refresh cached focal context from CCS (no embedding, fast)."""
        try:
            mc = MemoryCache(DB_PATH, "gemma", OLLAMA_URL)
            self._focal_context = mc.get_focal_context(max_chars=300)
        except Exception as e:
            log(f"  Focal context refresh error: {e}")
            self._focal_context = ""

    def _load_recent_window(self):
        rows = self.db.query(
            "SELECT embedding FROM seed_observations "
            "WHERE embedding IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT ?",
            (WINDOW_SIZE,),
        )
        for r in reversed(rows):
            if r["embedding"]:
                self.window.append(blob_to_vec(r["embedding"]))
        log(f"Window loaded: {len(self.window)} embeddings")

    def score(self, text: str) -> Tuple[float, Optional[List[float]]]:
        vec = embed_text(text)
        if vec is None:
            return 0.0, None
        if not self.window:
            return 1.0, vec
        max_sim = max(cosine_sim(vec, w) for w in self.window)
        novelty = 1.0 - max_sim
        return novelty, vec

    def add_to_window(self, vec: List[float]):
        self.window.append(vec)

    def rebuild_entity_bias(self):
        cutoff = now_ts() - (BIAS_LOOKBACK_DAYS * 86400)
        rows = self.db.query(
            "SELECT o.id, LOWER(o.content) as content, r.route "
            "FROM seed_observations o "
            "JOIN seed_routing_log r ON r.observation_id = o.id "
            "WHERE o.timestamp > ?",
            (cutoff,),
        )
        if not rows:
            return
        entities = self.db.query(
            "SELECT id, canonical_name, entity_type, aliases FROM kg_entities "
            "WHERE mention_count >= 2 "
            "ORDER BY mention_count DESC LIMIT ?",
            (BIAS_MAX_ENTITIES,),
        )
        if not entities:
            return

        ROUTE_VAL = {"ignore": 0.0, "glance": 0.25, "think": 0.75, "deep": 1.0}
        bias_rows = []
        for ent in entities:
            names = [ent["canonical_name"].lower()]
            if ent["aliases"]:
                try:
                    aliases = json.loads(ent["aliases"])
                    names.extend(a.lower() for a in aliases if len(a) >= 4)
                except Exception:
                    pass
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

        self.db.run("DELETE FROM seed_entity_bias")
        for row in bias_rows:
            self.db.run(
                "INSERT INTO seed_entity_bias VALUES (?, ?, ?, ?, ?, ?, ?)", row,
            )
        neg = sum(1 for r in bias_rows if r[5] < -0.05)
        pos = sum(1 for r in bias_rows if r[5] > 0.05)
        log(f"  Entity bias rebuilt: {len(bias_rows)} entities (suppress={neg}, boost={pos})")

    def _refresh_bias_cache(self):
        try:
            rows = self.db.query("SELECT canonical_name, bias_factor FROM seed_entity_bias")
            self._entity_bias_cache = {r["canonical_name"].lower(): r["bias_factor"] for r in rows}
            if self._entity_bias_cache:
                log(f"  Bias cache: {len(self._entity_bias_cache)} entities")
        except Exception:
            self._entity_bias_cache = {}

    def get_entity_bias(self, text: str) -> float:
        if not self._entity_bias_cache:
            return 0.0
        text_lower = text.lower()
        biases = [
            bias for name, bias in self._entity_bias_cache.items()
            if name in text_lower
        ]
        if not biases:
            return 0.0
        if min(biases) < -0.05:
            return min(biases)
        return max(biases)

    def rebuild_thresholds(self):
        cutoff = now_ts() - (THRESHOLD_LOOKBACK_DAYS * 86400)
        rows = self.db.query(
            "SELECT o.source, r.route, r.feedback_score, r.timestamp "
            "FROM seed_routing_log r "
            "JOIN seed_observations o ON r.observation_id = o.id "
            "WHERE r.feedback_score IS NOT NULL "
            "AND r.timestamp > ?",
            (cutoff,),
        )
        if not rows:
            return

        from collections import defaultdict
        agg = defaultdict(lambda: defaultdict(list))
        newest_ts = defaultdict(int)
        for r in rows:
            agg[r["source"]][r["route"]].append(r["feedback_score"])
            newest_ts[r["source"]] = max(newest_ts[r["source"]], r["timestamp"])

        source_activity = {}
        activity_rows = self.db.query(
            "SELECT source, MAX(timestamp) as latest FROM seed_observations "
            "WHERE timestamp > ? GROUP BY source",
            (now_ts() - (THRESHOLD_LOOKBACK_DAYS * 86400),),
        )
        for r in activity_rows:
            try:
                source_activity[r['source']] = int(r['latest'])
            except (ValueError, TypeError):
                pass

        self.db.run("DELETE FROM seed_thresholds")
        count = 0
        stale = 0
        alive_stale = 0
        dead_stale = 0
        now = now_ts()
        for source, routes in agg.items():
            think_scores = routes.get("think", [])
            deep_scores = routes.get("deep", [])
            think_avg = sum(think_scores) / len(think_scores) if think_scores else -1.0
            deep_avg = sum(deep_scores) / len(deep_scores) if deep_scores else -1.0
            age_days = (now - newest_ts.get(source, 0)) / 86400
            if age_days > 7:
                src_latest = source_activity.get(source, 0)
                source_alive = (now - src_latest) < 7 * 86400 if src_latest else False
                if source_alive:
                    decay_window = 42
                    alive_stale += 1
                else:
                    decay_window = 7
                    dead_stale += 1
                decay = min(1.0, (age_days - 7) / decay_window)
                if think_avg >= 0:
                    think_avg = think_avg * (1 - decay) + 0.5 * decay
                if deep_avg >= 0:
                    deep_avg = deep_avg * (1 - decay) + 0.5 * decay
                stale += 1
            if len(think_scores) >= THRESHOLD_MIN_THINK_SAMPLES or len(deep_scores) >= THRESHOLD_MIN_DEEP_SAMPLES:
                self.db.run(
                    "INSERT OR REPLACE INTO seed_thresholds (category, threshold_low, threshold_high, last_updated, last_observation_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (source, round(think_avg, 4), round(deep_avg, 4), now, newest_ts.get(source, 0)),
                )
                count += 1

        caps = 0
        for source, routes in agg.items():
            think_scores = routes.get("think", [])
            deep_scores = routes.get("deep", [])
            think_avg = sum(think_scores) / len(think_scores) if think_scores else -1.0
            deep_avg = sum(deep_scores) / len(deep_scores) if deep_scores else -1.0
            if len(think_scores) >= THRESHOLD_MIN_THINK_SAMPLES and think_avg < THRESHOLD_MIN_THINK_SCORE:
                caps += 1
            if len(deep_scores) >= THRESHOLD_MIN_DEEP_SAMPLES and deep_avg < THRESHOLD_MIN_DEEP_SCORE:
                caps += 1
        stale_msg = f", {stale} stale" if stale else ""
        alive_msg = f" ({alive_stale} alive-stale, {dead_stale} dead-stale)" if stale else ""
        log(f"  Thresholds rebuilt: {count} sources tracked, {caps} caps active{stale_msg}{alive_msg}")

    def _refresh_threshold_cache(self):
        try:
            rows = self.db.query(
                "SELECT category, threshold_low, threshold_high FROM seed_thresholds"
            )
            self._threshold_cache = {}
            for r in rows:
                self._threshold_cache[r["category"]] = {
                    "think_score": r["threshold_low"],
                    "deep_score": r["threshold_high"],
                }
            if self._threshold_cache:
                log(f"  Threshold cache: {len(self._threshold_cache)} sources")
        except Exception:
            self._threshold_cache = {}

    def apply_learned_cap(self, source: str, route: str) -> str:
        if not self._threshold_cache or source not in self._threshold_cache:
            return route
        t = self._threshold_cache[source]
        original = route
        if route == "deep" and t["deep_score"] >= 0 and t["deep_score"] < THRESHOLD_MIN_DEEP_SCORE:
            route = "think"
        if route == "think" and t["think_score"] >= 0 and t["think_score"] < THRESHOLD_MIN_THINK_SCORE:
            route = "ignore"
        if route != original:
            log(f"  Learned cap: {source} {original}→{route} (think={t['think_score']:.2f} deep={t['deep_score']:.2f})")
        return route

    def get_source_quality_boost(self, source: str) -> float:
        if not self._threshold_cache or source not in self._threshold_cache:
            return 0.0
        t = self._threshold_cache[source]
        think_score = t.get("think_score", -1.0)
        if think_score < 0:
            return 0.0
        if think_score > 0.75:
            return min(0.03, (think_score - 0.75) * 0.12)
        if think_score < 0.5:
            return -0.02
        return 0.0

    # ── Arrival Correlation ────────────────────────────────────────

    def _init_correlation_tracker(self):
        """Initialize domain co-firing tracker."""
        self._domain_arrivals = {}    # {domain: [timestamps]}
        self._cofiring_history = {}   # {(d1,d2): [counts per window]}

    def record_domain_arrival(self, source: str):
        """Record that an observation arrived from this domain."""
        domain = self._source_to_domain(source)
        if not domain:
            return
        if not hasattr(self, '_domain_arrivals'):
            self._init_correlation_tracker()
        now = now_ts()
        if domain not in self._domain_arrivals:
            self._domain_arrivals[domain] = []
        self._domain_arrivals[domain].append(now)
        # Trim to current window
        cutoff = now - CORR_WINDOW
        self._domain_arrivals[domain] = [
            t for t in self._domain_arrivals[domain] if t > cutoff
        ]

    def check_arrival_correlation(self) -> List[Tuple[str, str, float]]:
        """Check for unusual domain co-firing. Returns list of (d1, d2, z_score)."""
        if not hasattr(self, '_domain_arrivals'):
            return []

        now = now_ts()
        window_start = now - CORR_WINDOW
        alerts = []

        # Count co-firing pairs in current window
        domains_active = {
            d: len([t for t in ts if t > window_start])
            for d, ts in self._domain_arrivals.items()
        }

        # Check each pair
        active_domains = [d for d, c in domains_active.items() if c >= 2]
        for i, d1 in enumerate(active_domains):
            for d2 in active_domains[i+1:]:
                cofiring = min(domains_active[d1], domains_active[d2])
                pair = tuple(sorted([d1, d2]))

                # Skip pairs with known coordination (Thread #274 component 3)
                if frozenset(pair) in CORR_EXCLUDE_PAIRS:
                    continue

                if not hasattr(self, '_cofiring_history'):
                    self._cofiring_history = {}
                if pair not in self._cofiring_history:
                    self._cofiring_history[pair] = []

                history = self._cofiring_history[pair]
                if len(history) >= 3:  # need minimum history
                    mean = sum(history) / len(history)
                    variance = sum((x - mean) ** 2 for x in history) / len(history)
                    std = max(variance ** 0.5, 0.5)  # floor to avoid div-by-zero
                    z = (cofiring - mean) / std
                    if z > CORR_THRESHOLD:
                        alerts.append((d1, d2, round(z, 2)))

                # Update history
                self._cofiring_history[pair].append(cofiring)
                if len(self._cofiring_history[pair]) > CORR_HISTORY_WINDOWS:
                    self._cofiring_history[pair] = self._cofiring_history[pair][-CORR_HISTORY_WINDOWS:]

        return alerts

    def get_correlation_boost(self, source: str) -> float:
        """If this source's domain is in an unusual co-firing pair, boost novelty."""
        if not hasattr(self, '_corr_alerts'):
            self._corr_alerts = []
        domain = self._source_to_domain(source)
        if not domain:
            return 0.0
        for d1, d2, z in self._corr_alerts:
            if domain in (d1, d2):
                return CORR_BOOST
        return 0.0

    # ── Domain Temperature ──────────────────────────────────────────

    def _source_to_domain(self, source: str) -> Optional[str]:
        """Map observation source to domain cluster."""
        for prefix, domain in DOMAIN_MAP.items():
            if source.startswith(prefix):
                return domain
        return None

    def _get_domain_temperature(self, domain: str) -> float:
        """Get current temperature for a domain, applying exponential decay."""
        row = self.db.query_one(
            "SELECT temperature, direction, last_shock_at, half_life_seconds "
            "FROM domain_temperature WHERE domain = ?", (domain,)
        )
        if not row or row["last_shock_at"] == 0:
            return TEMP_DEFAULT

        elapsed = now_ts() - row["last_shock_at"]
        half_life = row["half_life_seconds"] or TEMP_HALF_LIFE

        # Exponential decay toward 1.0
        delta = row["temperature"] - TEMP_DEFAULT
        decayed_delta = delta * (0.5 ** (elapsed / half_life))

        # If decayed back to within 0.01 of baseline, it's baseline
        if abs(decayed_delta) < 0.01:
            return TEMP_DEFAULT
        return TEMP_DEFAULT + decayed_delta

    def propagate_shock(self, source: str, route: str):
        """When a deep route occurs, propagate temperature to connected domains."""
        if route != "deep":
            return

        src_domain = self._source_to_domain(source)
        if not src_domain:
            return

        now = now_ts()
        for conn_src, conn_tgt, conn_type in DOMAIN_CONNECTIONS:
            if conn_src != src_domain:
                continue

            boost = TEMP_BOOST_AMPLIFY if conn_type == "amplify" else TEMP_BOOST_CONTAMINATE
            current = self._get_domain_temperature(conn_tgt)
            new_temp = max(TEMP_MIN, min(TEMP_MAX, current + boost))

            self.db.run(
                "INSERT OR REPLACE INTO domain_temperature "
                "(domain, temperature, direction, last_shock_at, shock_source, half_life_seconds, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (conn_tgt, round(new_temp, 3), conn_type, now, source[:100], TEMP_HALF_LIFE, now),
            )
            log(f"  TEMP: {src_domain}→{conn_tgt} ({conn_type}) temp={new_temp:.2f}")

    def _apply_temperature(self, novelty: float, source: str) -> float:
        """Adjust novelty score based on domain temperature."""
        domain = self._source_to_domain(source)
        if not domain:
            return novelty

        temp = self._get_domain_temperature(domain)
        if abs(temp - TEMP_DEFAULT) < 0.01:
            return novelty

        # Temperature modulates novelty: higher temp → higher effective novelty
        # This makes the THRESH_ASSESS bar easier to clear when vigilance is up
        adjusted = novelty * temp
        return max(0.0, min(1.0, adjusted))

    # ── Classification ────────────────────────────────────────────

    def classify(self, novelty: float, source: str, text: str = "") -> str:
        """Two-stage routing: cosine dedup → Gemma classification.

        Returns route name: 'ignore', 'think', or 'deep'.
        Temperature from cross-domain surprise modulates the novelty score.
        """
        entity_adj = self.get_entity_bias(text) if text else 0.0
        source_adj = self.get_source_quality_boost(source)
        corr_adj = self.get_correlation_boost(source)
        adjusted = max(0.0, min(1.0, novelty + entity_adj + source_adj + corr_adj))

        # Apply domain temperature — cross-domain surprise propagation
        adjusted = self._apply_temperature(adjusted, source)

        # Operator sources: Nate's input always routes to deep
        is_operator = any(s in source for s in OPERATOR_SOURCES)
        if is_operator:
            return "deep"

        is_priority = any(p in source for p in PRIORITY_SOURCES)

        # Stage 1: Cosine dedup gate
        if adjusted < THRESH_DEDUP:
            return "think" if is_priority else "ignore"

        # Stage 2: Gemma classification
        if adjusted >= THRESH_ASSESS or is_priority:
            classification = self._ask_gemma(source, text)
            route = ROUTE_MAP.get(classification, "think")
            # Cap canister:capsule at think
            if route == "deep" and source == "canister:capsule":
                route = "think"
            return self.apply_learned_cap(source, route)

        # Between THRESH_DEDUP and THRESH_ASSESS: store but don't reason
        return "ignore"

    def _ask_gemma(self, source: str, text: str) -> str:
        """Ask Gemma: 1 (noise), 2 (signal), or 3 (alarm).

        Gemma classifies. She does not analyze. One number, move on.
        """
        # Build system prompt with active thread + focal context awareness
        system = GATE_SYSTEM_PROMPT
        if self._focal_context:
            system += f"\n\n{self._focal_context}"
        if self._active_thread:
            system += (
                f"\n\nCURRENT THREAD: \"{self._active_thread['question']}\"\n"
                "Observations connecting to this thread are more likely signal (2)."
            )

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content":
                f"Source: {source}\n"
                f"Observation: {safe_truncate(text, 500)}"},
        ]

        # Try cloud first (engine → Groq/Cerebras), fall back to local Gemma
        for url, model, fmt in [
            (INFERENCE_URL, GATE_MODEL, "ollama"),
            (INFERENCE_URL_LOCAL, "gemma4:26b", "openai"),
        ]:
            try:
                if fmt == "ollama":
                    payload = {
                        "model": model,
                        "messages": messages,
                        "stream": False,
                        "options": {"num_predict": 50, "temperature": 0.1},
                    }
                    r = requests.post(f"{url}/api/chat", json=payload, timeout=30)
                    if r.status_code == 200:
                        raw = r.json().get("message", {}).get("content", "").strip()
                    else:
                        continue
                else:
                    payload = {
                        "model": model,
                        "messages": messages,
                        "max_tokens": 50,
                        "temperature": 0.1,
                        "reasoning_format": "none",
                    }
                    r = requests.post(f"{url}/v1/chat/completions", json=payload, timeout=60)
                    if r.status_code == 200:
                        data = r.json()
                        raw = ""
                        if "choices" in data and data["choices"]:
                            raw = data["choices"][0].get("message", {}).get("content", "").strip()
                    else:
                        continue

                # Strip thinking tags
                if "<channel|>" in raw:
                    raw = raw.split("<channel|>")[-1].strip()
                if "</think>" in raw:
                    raw = raw.split("</think>")[-1].strip()
                # Extract first digit
                for ch in raw:
                    if ch in ("1", "2", "3"):
                        return ch
                return "1"  # default to noise if unparseable
            except Exception as e:
                log(f"  Gate classify error ({fmt}): {e}")
                continue

        # All backends failed — default to signal (safe: lets downstream decide)
        return "2"


# ═══════════════════════════════════════════════════════════════════
#  Feedback Loop — Gemma scores her own routes
# ═══════════════════════════════════════════════════════════════════

def score_recent_routes(db: DB):
    """Check if downstream produced signal from Gemma's routed observations.

    Scoring:
      - Routed item spawned a downstream activity_feed entry → 0.8
      - Routed item spawned a crossref connection → 0.9
      - Routed item was referenced by opus → 1.0
      - Routed item produced nothing downstream → 0.2
    """
    cutoff = now_ts() - FEEDBACK_LOOKBACK
    maturity = now_ts() - FEEDBACK_DOWNSTREAM_WINDOW  # only score routes old enough for downstream

    # Get unscored routes that are old enough
    routes = db.query(
        "SELECT r.id, r.timestamp, r.observation_id, r.route, o.content "
        "FROM seed_routing_log r "
        "JOIN seed_observations o ON r.observation_id = o.id "
        "WHERE r.feedback_score IS NULL "
        "AND r.route IN ('think', 'deep') "
        "AND r.timestamp > ? "
        "AND r.timestamp < ? "
        "ORDER BY r.timestamp ASC LIMIT 50",
        (cutoff, maturity),
    )

    if not routes:
        return 0

    scored = 0
    for route in routes:
        score = 0.2  # default: nothing came of it
        route_ts = route["timestamp"]
        window_end = route_ts + FEEDBACK_DOWNSTREAM_WINDOW

        # Check if downstream produced anything referencing this content
        # Look for activity_feed entries from intern/analyst/opus after this route
        downstream = db.query_one(
            "SELECT COUNT(*) as cnt FROM activity_feed "
            "WHERE source IN ('intern', 'analyst') "
            "AND activity_type = 'brief' "
            "AND created_at > ? AND created_at < ?",
            (route_ts, window_end),
        )
        if downstream and downstream["cnt"] > 0:
            score = 0.6

        # Check for crossref connections in the window
        crossref = db.query_one(
            "SELECT COUNT(*) as cnt FROM crossref_connections "
            "WHERE created_at > ? AND created_at < ?",
            (route_ts, window_end),
        )
        if crossref and crossref["cnt"] > 0:
            score = max(score, 0.8)

        # Check for opus thread references
        opus = db.query_one(
            "SELECT COUNT(*) as cnt FROM activity_feed "
            "WHERE source LIKE 'opus%' "
            "AND created_at > ? AND created_at < ?",
            (route_ts, window_end),
        )
        if opus and opus["cnt"] > 0:
            score = max(score, 1.0)

        db.run(
            "UPDATE seed_routing_log SET feedback_score = ? WHERE id = ?",
            (round(score, 2), route["id"]),
        )
        scored += 1

    if scored > 0:
        log(f"  Feedback: scored {scored} recent routes")
    return scored


# ═══════════════════════════════════════════════════════════════════
#  MQTT Alert
# ═══════════════════════════════════════════════════════════════════

def publish_alert(obs: dict):
    """Publish deep-routed observation to MQTT for visibility."""
    try:
        import paho.mqtt.publish as publish
        publish.single(
            "homeforge/gemma/alert",
            json.dumps({
                "source": obs["source"],
                "observation": safe_truncate(obs["content"], 300),
                "timestamp": now_ts(),
            }),
            hostname=MQTT_BROKER,
            port=MQTT_PORT,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Gemma Gate starting ═══")
    log(f"Model: {GATE_MODEL}")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL}")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Window: {WINDOW_SIZE} | Interval: {LOOP_INTERVAL}s")
    log(f"Routing: cosine dedup<{THRESH_DEDUP} | classify>={THRESH_ASSESS} → Gemma")
    log(f"Operator sources (always deep): {OPERATOR_SOURCES}")

    db = DB(DB_PATH)
    stream = ObservationStream(db)
    router = NoveltyRouter(db)

    # Mesh — autonomic nervous system
    mesh = Mesh("gemma", db_path=DB_PATH)
    mesh.expect("routes_classified", min_per_hour=1)
    log("Mesh node joined")

    # Graceful shutdown
    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    stats = {"ignore": 0, "think": 0, "deep": 0, "stochastic_reset": 0, "errors": 0}

    while running:
        cycle += 1
        try:
            observations = stream.collect()

            for obs in observations:
                text = obs["content"]
                if not text or len(text.strip()) < 5:
                    continue

                _lower = text.lower()
                if any(skip in _lower for skip in (
                    "phone capture queued",
                    "capture queued type:",
                )):
                    continue

                if obs["source"] in SKIP_SOURCES:
                    continue

                if any(obs["source"].startswith(prefix) for prefix in SKIP_MQTT_PREFIXES):
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

                # Classify
                route = router.classify(novelty, obs["source"], text)

                # Log routing decision (no output — Gemma doesn't analyze)
                routing_log_id = db.run(
                    "INSERT INTO seed_routing_log (timestamp, observation_id, route, model_used, output) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (now_ts(), obs_id, route, GATE_MODEL if route != "ignore" else None, None),
                )

                # Propagate surprise to connected domains (Thread #274)
                router.propagate_shock(obs["source"], route)

                # Record domain arrival for correlation tracking
                router.record_domain_arrival(obs["source"])

                # Reinforce metabolism for canister capsules that triggered think/deep
                if route in ("think", "deep") and obs.get("source") == "canister:capsule":
                    m = re.search(r'\[capsule:(\d+)\]', obs.get("content", ""))
                    if m:
                        reinforce_capsule_async(int(m.group(1)))

                # Pass original observation to activity_feed for downstream
                # Skip operator sources — they are already in activity_feed via dispatch
                is_already_in_feed = any(s in obs["source"] for s in OPERATOR_SOURCES)
                if route in ("think", "deep") and not is_already_in_feed:
                    entity_bias = router.get_entity_bias(text)
                    meta_dict = {
                        "original_source": obs["source"],
                        "novelty": round(novelty, 3),
                        "entity_bias": round(entity_bias, 3) if abs(entity_bias) > 0.01 else 0,
                        "routing_log_id": routing_log_id,
                        "gate_route": route,
                    }
                    if obs.get("activity_feed_id"):
                        meta_dict["upstream_id"] = obs["activity_feed_id"]

                    db.run(
                        "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        ("gemma", route,
                         f"[{route}] {obs['source']} (novelty={novelty:.2f})",
                         safe_truncate(obs["content"], 2000),  # original observation, not analysis
                         json.dumps(meta_dict),
                         now_ts()),
                    )

                    # MQTT alert for deep routes
                    if route == "deep":
                        publish_alert(obs)

                # Update window
                router.add_to_window(vec)
                stats[route] += 1
                if route in ("think", "deep"):
                    mesh.pulse("routes_classified")

                if route != "ignore":
                    bias_str = ""
                    entity_adj = router.get_entity_bias(text)
                    if abs(entity_adj) > 0.01:
                        bias_str = f" (bias={entity_adj:+.3f})"
                    source_adj = router.get_source_quality_boost(obs["source"])
                    if abs(source_adj) > 0.001:
                        bias_str += f" (src={source_adj:+.3f})"
                    corr_adj = router.get_correlation_boost(obs["source"])
                    if corr_adj > 0:
                        bias_str += f" (corr={corr_adj:+.3f})"
                    log(f"  [{obs['source']}] novelty={novelty:.3f}{bias_str} → {route}")

            # Periodic stats
            if cycle % 50 == 0:
                total = sum(stats.values())
                log(f"Stats @ cycle {cycle}: {stats} (total={total}, window={len(router.window)})")
                # Log domain temperatures
                temps = db.query("SELECT domain, temperature, direction, last_shock_at FROM domain_temperature WHERE last_shock_at > 0")
                if temps:
                    for t in temps:
                        live_temp = router._get_domain_temperature(t["domain"])
                        if abs(live_temp - TEMP_DEFAULT) > 0.01:
                            log(f"  TEMP {t['domain']}: {live_temp:.2f} ({t['direction']})")

            # Arrival correlation check (Thread #274 — emergent coupling)
            if cycle % CORR_CHECK_INTERVAL == 0 and cycle > 0:
                try:
                    alerts = router.check_arrival_correlation()
                    router._corr_alerts = alerts
                    if alerts:
                        for d1, d2, z in alerts:
                            log(f"  CORR ALERT: {d1}↔{d2} z={z} — emergent coupling detected")
                except Exception as e:
                    log(f"  Correlation check error: {e}")

            # Periodic entity bias + threshold rebuild
            if cycle % BIAS_REBUILD_INTERVAL == 0 and cycle > 0:
                try:
                    router.rebuild_entity_bias()
                    router._refresh_bias_cache()
                except Exception as e:
                    log(f"  Entity bias rebuild error: {e}")

                try:
                    router.rebuild_thresholds()
                    router._refresh_threshold_cache()
                except Exception as e:
                    log(f"  Threshold rebuild error: {e}")

                try:
                    router._active_thread = _load_active_thread(db)
                    if router._active_thread:
                        log(f"  Thread loaded: {router._active_thread['title']}")
                except Exception as e:
                    log(f"  Thread load error: {e}")

                try:
                    router._refresh_focal_context()
                except Exception as e:
                    log(f"  Focal context error: {e}")

                try:
                    feedback = _read_and_ack_feedback(db, "gemma")
                    for fb in feedback:
                        log(f"  Feedback [{fb['feedback_type']}]: {fb['content'][:80]}")
                except Exception as e:
                    log(f"  Feedback read error: {e}")

            # Feedback loop — score recent routes
            if cycle % FEEDBACK_INTERVAL == 0 and cycle > 0:
                try:
                    score_recent_routes(db)
                except Exception as e:
                    log(f"  Feedback scoring error: {e}")

            # Stochastic reset: force-route a random observation through classification
            if cycle % 100 == 0 and cycle > 0:
                try:
                    candidates = db.query(
                        "SELECT id, source, content FROM seed_observations "
                        "WHERE source NOT LIKE 'mqtt:%' "
                        "AND length(content) > 100 "
                        "AND id > (SELECT MAX(id) - 10000 FROM seed_observations) "
                        "ORDER BY RANDOM() LIMIT 1"
                    )
                    if candidates:
                        reset_obs = candidates[0]
                        log(f"  STOCHASTIC RESET: re-routing obs {reset_obs['id']} [{reset_obs['source']}]")
                        reset_class = router._ask_gemma(reset_obs["source"], reset_obs["content"])
                        reset_route = ROUTE_MAP.get(reset_class, "think")
                        db.run(
                            "INSERT INTO seed_routing_log (timestamp, observation_id, route, model_used, output) "
                            "VALUES (?, ?, 'stochastic_reset', ?, ?)",
                            (now_ts(), reset_obs["id"], GATE_MODEL, None),
                        )
                        if reset_route in ("think", "deep"):
                            db.run(
                                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                ("gemma", "stochastic_reset",
                                 f"[stochastic_reset] {reset_obs['source']}",
                                 safe_truncate(reset_obs["content"], 2000),
                                 json.dumps({"route_reason": "stochastic_reset", "original_obs_id": reset_obs["id"], "gate_route": reset_route}),
                                 now_ts()),
                            )
                        stats["stochastic_reset"] += 1
                        log(f"  RESET RESULT: {reset_obs['source']} → {reset_route}")
                except Exception as e:
                    log(f"  Stochastic reset error: {e}")

        except Exception as e:
            log(f"Cycle error: {e}")
            stats["errors"] += 1

        time.sleep(LOOP_INTERVAL)

    # Cleanup
    mesh.shutdown()
    stream.shutdown()
    db.close()
    log("═══ Gemma Gate stopped ═══")


if __name__ == "__main__":
    main()
