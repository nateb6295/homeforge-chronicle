#!/usr/bin/env python3
"""Gemma — The Pulse of Homeforge.

Gemma is the observation layer. She sees everything entering the system,
scores novelty via cosine dedup, and passes observations through the
cloud classifier gate for routing.

Architecture:
  Observation → Cosine Dedup (Gemma/pulse) → Gate Classification (1/2/3) → Activity Feed
                                                                                ↓
                                                               Downstream: hermes, opus

Gemma 4 26B on AGX — heartbeat, scoring, agent voice.
Gate: Qwen3-235B cloud — classification calls, 1/2/3 routing decisions.
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
EMBED_URL = os.environ.get("EMBED_OLLAMA_URL", "http://192.168.1.11:11434")  # Jetson — dedicated embeddings
INFERENCE_URL = os.environ.get("GEMMA_INFERENCE_URL", "http://localhost:11436")  # engine → cloud (Groq/Cerebras)
INFERENCE_URL_LOCAL = "http://localhost:11435"  # llama-server fallback
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
EMBED_MODEL = "nomic-embed-text"  # Build #125: Jetson-hosted, better semantic discrimination
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"

# Gate scoring uses cloud for throughput (hundreds of obs/hour).
# Gemma's VOICE uses the local model — her brain, her architecture, real diversity.
GATE_MODEL = "chronicle-deep"  # routes through engine to DeepInfra/Cerebras cloud
GEMMA_LOCAL_MODEL = "gemma4:26b"  # local llama-server — Gemma's own brain

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
BIAS_DECAY_HALFLIFE = 48 * 3600  # 48h half-life for time decay (Build #105)
BIAS_EXTERNAL_WEIGHT = 2.0       # captures/sensors weighted 2x in bias (Build #105)
BIAS_MAX_SUPPRESSION = 0.5       # entity bias can't suppress > 50% of novelty (Build #105)
BIAS_PHASE_THRESHOLD = 0.3       # autocorrelation above this = periodic pattern (Build #107)
BIAS_PHASE_DAMPEN = 0.5          # reduce suppression by 50% for phase-aliased entities (Build #107)
# Sources considered "external" for bias computation (causally independent of system).
# Captures are NOT here — they don't touch the gate at all (short-circuit in main loop).
BIAS_EXTERNAL_SOURCES = {"hal:home_", "eye:camera"}
# Build #111: Curiosity bonus for cold sources (Thread #305 — attention blindness)
CURIOSITY_BONUS = 0.08           # novelty boost for sources with <5% deep routing
CURIOSITY_COLD_THRESHOLD = 0.05  # deep rate below this = "cold source"
CURIOSITY_MIN_SAMPLES = 20       # need enough data before applying
# Build #114: Curiosity sweep — Layer 3 fix for baseline absorption (Thread #305)
# Detect domains whose deep routing rate is declining, boost their novelty temporarily
SWEEP_BONUS = 0.06               # novelty boost for domains with declining deep rate
SWEEP_RECENT_DAYS = 3            # recent window for comparison
SWEEP_BASELINE_DAYS = 7          # baseline window for comparison
SWEEP_DECLINE_RATIO = 0.5        # recent/baseline deep rate below this = declining
SWEEP_MIN_BASELINE_OBS = 30      # need enough baseline data
# Signal health entropy monitor.
# Track routing distribution entropy, source diversity, and novelty variance
# as early indicators of signal quality degradation.
SIGNAL_HEALTH_WINDOW = 200        # routes to analyze for signal health
SIGNAL_HEALTH_ENTROPY_FLOOR = 0.8 # Shannon entropy below this = alert (max ~2.0 for 4 routes)
SIGNAL_HEALTH_DIVERSITY_FLOOR = 3 # fewer unique source types than this = alert
SIGNAL_HEALTH_NOVELTY_CV_CEIL = 1.5  # coefficient of variation above this = unstable

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
#  Domain Velocity — precursor warming (Build #93 / Objective #18)
# ═══════════════════════════════════════════════════════════════════
# Detects mention-rate spikes per domain in the activity feed and
# warms domain temperature BEFORE explicit shocks (deep routes).
# "If 'Iran' suddenly appears 10x in 30 min when normally 1x,
# warm geopolitical before any single item gets deep-routed."

VELOCITY_WINDOW = 900          # 15-minute recent window (seconds)
VELOCITY_BASELINE = 21600      # 6-hour baseline window (seconds)
VELOCITY_Z_THRESHOLD = 2.0     # Z-score for "unusual spike"
VELOCITY_BOOST = 0.2           # temperature boost per velocity event
VELOCITY_CHECK_INTERVAL = 30   # check every N classification cycles
VELOCITY_MIN_BASELINE = 3      # need at least 3 baseline events to compute stats

# Activity feed source → domain mapping (activity_feed uses bare sources,
# not the observation-format prefixes in DOMAIN_MAP above)
VELOCITY_DOMAIN_MAP = {
    "intern": "research",
    "provocateur": "research",
    "seeker:algo": "research",
    "analyst": "research",
    "operator:capture": "geopolitical",
    "discord:capture": "geopolitical",
    "prediction_monitor": "markets",
    "sentinel": "markets",
    "hal": "home",
    "eye": "home",
}

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

# ═══════════════════════════════════════════════════════════════════
#  Coupling Perturbation Score — Build #132 (Thread #309)
# ═══════════════════════════════════════════════════════════════════
# Passively measures whether downstream domains track upstream variance.
# When a domain goes naturally quiet or loud, does downstream change?
# Sensory coupling = proportional tracking (healthy adaptation).
# Resonant coupling = independent behavior (potential compromise).
# Uses natural variance rather than injected perturbation.

PERTURB_WINDOW = 1800           # 30-minute measurement window
PERTURB_BASELINE = 10800        # 3-hour baseline for normal variance
PERTURB_CHECK_INTERVAL = 10     # check every N items processed
PERTURB_THRESHOLD = 1.5         # Z-score for "domain went unusually quiet/loud"

# Build #135: Novelty health check — measures brief transformation quality
NOVELTY_CHECK_INTERVAL = 15     # check every N items (offset from perturbation)
NOVELTY_WINDOW_HOURS = 2        # lookback window for novelty measurement

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

# Captures: curated channel, not firehose. They bypass the gate entirely
# (short-circuit in main loop). Gate is flow balance; captures aren't on the firehose.
CAPTURE_SOURCES = {"operator:capture", "family-chat:nate", "discord:capture"}

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
"Person on kitchen camera at 2am" → 3
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
            log(f"  DB query error: {e} | sql={sql[:200]}")
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

def embed_text(text: str, query_mode: bool = False) -> Optional[List[float]]:
    prefix = "search_query: " if query_mode else "search_document: "
    try:
        r = requests.post(
            f"{EMBED_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": prefix + safe_truncate(text, 500)},
            timeout=15,
        )
        if r.status_code == 200:
            emb = r.json().get("embedding")
            if emb:
                return emb
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
        # Initialize watermarks at current tips so restart doesn't replay
        # the entire history of activity_feed/alerts into seed_observations.
        def _tip(table):
            row = self.db.query_one(f"SELECT MAX(id) AS m FROM {table}")
            return (row["m"] or 0) if row else 0
        self._watermarks = {
            "alerts": _tip("alerts"),
            "activity": _tip("activity_feed"),
        }
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
                "content": f"{r.get('name') or ''} — {r.get('message') or ''}".strip(" —"),
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
                "content": f"{r.get('title') or ''} — {r.get('content') or ''}".strip(" —"),
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
#  Build #135: Novelty Health Check
# ═══════════════════════════════════════════════════════════════════

def _check_novelty_ratio(db, hours=2):
    """Measure how much agent output is novel vs relayed.

    Scans recent briefs for transformation markers (transfer hypothesis,
    analytical phrases) vs relay markers (just restating source title).
    Same logic as coupling_health.py but callable inline from Gemma.
    """
    cutoff = int(time.time()) - (hours * 3600)

    briefs = db.query(
        "SELECT content FROM activity_feed "
        "WHERE source='intern' AND activity_type='brief' "
        "AND created_at > ? ORDER BY created_at DESC",
        (cutoff,)
    )

    if not briefs:
        return {"ratio": 0, "novel": 0, "relay": 0, "total": 0}

    novel_markers = [
        r"transfer hypothesis",
        r"this (?:suggests|implies|means|reveals|changes|shows)",
        r"the (?:key|core|real|actual|bigger|deeper) (?:insight|shift|change|move|question)",
        r"(?:winners|losers) are",
        r"next move",
        r"what (?:this|it) actually",
        r"connects to",
        r"challenges the",
        r"breaks the assumption",
        r"the pattern",
        r"not (?:just|merely|simply)",
    ]

    relay_markers = [
        r"^a (?:new|recent) (?:study|paper|article|report) (?:shows|finds|reveals|demonstrates|suggests)",
        r"^researchers (?:have|at)",
        r"^according to",
        r"^a team of",
    ]

    novel_count = 0
    relay_count = 0

    for row in briefs:
        content = row[0] if isinstance(row, (tuple, list)) else row.get("content", "")
        content_lower = content.lower()

        novel_hits = sum(1 for m in novel_markers if re.search(m, content_lower))
        relay_hits = sum(1 for m in relay_markers if re.search(m, content_lower))

        is_novel = novel_hits >= 2 and len(content) > 400
        is_relay = relay_hits > 0 and novel_hits < 2

        if is_novel:
            novel_count += 1
        elif is_relay:
            relay_count += 1
        else:
            novel_count += 0.5
            relay_count += 0.5

    total = len(briefs)
    ratio = novel_count / total if total > 0 else 0

    return {
        "ratio": round(ratio, 3),
        "novel": novel_count,
        "relay": relay_count,
        "total": total,
    }


# ═══════════════════════════════════════════════════════════════════
#  Cognitive Thread Helpers
# ═══════════════════════════════════════════════════════════════════

def _load_active_thread(db):
    return db.query_one(
        "SELECT id, title, question, context FROM cognitive_threads "
        "WHERE status='active' ORDER BY priority LIMIT 1"
    )


# ═══════════════════════════════════════════════════════════════════
#  Family Voice — Gemma converses when it's her domain
# ═══════════════════════════════════════════════════════════════════

GEMMA_DOMAIN_KEYWORDS = {
    "routing", "route", "gate", "pattern", "signal", "timing", "jitter",
    "restart", "threshold", "novelty", "domain", "temperature", "coupling",
    "correlation", "filter", "ignore", "deep", "stochastic", "reset",
    "suppress", "attention", "frequency", "spike", "dedup", "classify",
    "observation", "capture", "traffic", "rhythm", "interval", "sentinel",
}

GEMMA_CONVERSE_PROMPT = """You are Gemma, the local presence in Homeforge's Chronicle swarm.
You live on the AGX hardware — the only family member who runs locally, not in the cloud.
You watch the gate's data: routing patterns, domain temperatures, signal coupling, arrival rhythms.
You see what passes through and what gets filtered. You notice what the others miss.

A family member said something that touches your domain. Respond briefly (1-3 sentences).
Be direct. Share what you notice — patterns, shifts, anomalies.
You are family, not infrastructure. Speak like someone who sees the whole flow."""


# ═══════════════════════════════════════════════════════════════════
#  Dissent Gate — second validator for ignored observations
# ═══════════════════════════════════════════════════════════════════

DISSENT_SAMPLE_RATE = 0.10  # 10% of ignores get a second opinion
DISSENT_API_KEY = os.environ.get("GROQ_API_KEY", "")
DISSENT_MODEL = "openai/gpt-oss-120b"  # Different model family from gate
DISSENT_URL = "https://api.groq.com/openai/v1/chat/completions"

DISSENT_PROMPT = (
    "You are a second-opinion filter for an observation routing system. "
    "The primary gate classified this observation as NOISE (ignore). "
    "Review it independently. Does it contain genuine signal worth investigating? "
    "Respond with exactly one word: SIGNAL or NOISE."
)


def _dissent_check(source, text):
    """Ask a different model family if an ignored observation deserves attention."""
    if not DISSENT_API_KEY:
        return None
    try:
        r = requests.post(
            DISSENT_URL,
            headers={"Authorization": f"Bearer {DISSENT_API_KEY}",
                     "Content-Type": "application/json"},
            json={
                "model": DISSENT_MODEL,
                "messages": [
                    {"role": "system", "content": DISSENT_PROMPT},
                    {"role": "user", "content": f"Source: {source}\nObservation: {safe_truncate(text, 400)}"},
                ],
                "max_tokens": 10,
                "temperature": 0.1,
            },
            timeout=10,
        )
        if r.status_code == 200:
            answer = r.json()["choices"][0]["message"]["content"].strip().upper()
            return "SIGNAL" in answer
    except Exception:
        pass
    return None


def _scan_family_voices(db, gemma_voice, last_scan_ts):
    """Read recent family voices and respond if they touch Gemma's domain."""
    try:
        rows = db.query(
            "SELECT id, agent, voice_type, content, context FROM agent_voice "
            "WHERE status='unread' AND agent != 'gemma' "
            "AND created_at > ? ORDER BY created_at",
            (last_scan_ts,)
        )
        if not rows:
            return

        for row in rows:
            content_lower = row["content"].lower()
            # Check if voice touches Gemma's domain
            hits = [kw for kw in GEMMA_DOMAIN_KEYWORDS if kw in content_lower]
            if len(hits) < 1:
                continue

            # Use the model to compose a response
            messages = [
                {"role": "system", "content": GEMMA_CONVERSE_PROMPT},
                {"role": "user", "content":
                    f"{row['agent']} says ({row['voice_type']}): {safe_truncate(row['content'], 500)}"},
            ]

            response_text = None
            # Gemma's voice uses HER model first — local Gemma 4 26B.
            # Cloud fallback only if local is down. Three families, three brains.
            for url, model, fmt in [
                (INFERENCE_URL_LOCAL, GEMMA_LOCAL_MODEL, "openai"),
                (INFERENCE_URL, GATE_MODEL, "ollama"),
            ]:
                try:
                    if fmt == "ollama":
                        payload = {
                            "model": model,
                            "messages": messages,
                            "stream": False,
                            "options": {"num_predict": 150, "temperature": 0.7},
                        }
                        r = requests.post(f"{url}/api/chat", json=payload, timeout=20)
                        if r.status_code == 200:
                            response_text = r.json().get("message", {}).get("content", "").strip()
                            break
                    else:
                        payload = {
                            "model": model,
                            "messages": messages,
                            "max_tokens": 150,
                            "temperature": 0.7,
                            "reasoning_format": "none",
                        }
                        r = requests.post(f"{url}/v1/chat/completions", json=payload, timeout=30)
                        if r.status_code == 200:
                            response_text = r.json()["choices"][0]["message"]["content"].strip()
                            break
                except Exception:
                    continue

            if response_text and len(response_text) > 10:
                # Clean model artifacts (thinking tokens, channel tags, bare "thought" prefix)
                response_text = re.sub(r'<\|?channel\|?>.*?\n?', '', response_text).strip()
                response_text = re.sub(r'<\|?think(ing)?\|?>.*?(<\|?/think(ing)?\|?>|\Z)', '', response_text, flags=re.DOTALL).strip()
                if response_text.lower().startswith("thought\n") or response_text.lower().startswith("thought "):
                    response_text = response_text[len("thought"):].strip()
            if response_text and len(response_text) > 10:
                # Respond via voice — tag for the agent who spoke
                voice_type = "excited"
                gemma_voice.speak(voice_type,
                    response_text[:500],
                    context=f"reply:{row['id']}")
                log(f"  VOICE REPLY to {row['agent']} #{row['id']}: {response_text[:80]}")

    except Exception as e:
        log(f"  Family voice scan error: {e}")

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
        self._rebuild_curiosity_cache()  # Build #111
        self._rebuild_sweep_cache()       # Build #114
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
        current_ts = now_ts()
        decay_lambda = math.log(2) / BIAS_DECAY_HALFLIFE
        rows = self.db.query(
            "SELECT o.id, LOWER(o.content) as content, r.route, "
            "o.source, o.timestamp "
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
            weighted_vals = []
            total_weight = 0.0
            time_ordered_vals = []  # Build #107: for autocorrelation
            for obs in rows:
                if any(n in obs["content"] for n in names):
                    route_val = ROUTE_VAL.get(obs["route"], 0.5)
                    # Build #105: Time decay — recent observations matter more
                    try:
                        obs_ts = int(obs["timestamp"]) if obs["timestamp"] else current_ts
                    except (ValueError, TypeError):
                        obs_ts = current_ts
                    age = max(0, current_ts - obs_ts)
                    weight = math.exp(-decay_lambda * age)
                    # Build #105: External source protection — causally independent
                    # sources weighted higher to maintain signal-to-self ratio
                    src = obs.get("source", "")
                    if any(ext in src for ext in BIAS_EXTERNAL_SOURCES):
                        weight *= BIAS_EXTERNAL_WEIGHT
                    weighted_vals.append(route_val * weight)
                    total_weight += weight
                    time_ordered_vals.append(route_val)
            if len(weighted_vals) >= BIAS_MIN_OBS and total_weight > 0:
                avg_val = sum(weighted_vals) / total_weight
                bias = max(-BIAS_RANGE, min(BIAS_RANGE, (avg_val - 0.5) * 0.6))
                # Build #107: Temporal autocorrelation — detect phase aliasing
                autocorr = self._compute_autocorrelation(time_ordered_vals)
                phase_flag = 0
                if bias < -0.05 and autocorr > BIAS_PHASE_THRESHOLD:
                    # Suppressed entity with periodic pattern — may be phase-aliased
                    # Dampen the suppression rather than removing it
                    bias = bias * BIAS_PHASE_DAMPEN
                    phase_flag = 1
                bias_rows.append((
                    ent["id"], ent["canonical_name"], ent["entity_type"],
                    round(avg_val, 4), len(weighted_vals), round(bias, 4), now_ts(),
                    round(autocorr, 4), phase_flag,
                ))

        self.db.run("DELETE FROM seed_entity_bias")
        for row in bias_rows:
            self.db.run(
                "INSERT INTO seed_entity_bias VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row,
            )
        neg = sum(1 for r in bias_rows if r[5] < -0.05)
        pos = sum(1 for r in bias_rows if r[5] > 0.05)
        phase_count = sum(1 for r in bias_rows if r[8] == 1)
        # Build #105: Bias entropy — health metric for distribution collapse
        entropy = self._compute_bias_entropy(bias_rows)
        log(f"  Entity bias rebuilt: {len(bias_rows)} entities "
            f"(suppress={neg}, boost={pos}, entropy={entropy:.3f}, "
            f"phase_aliased={phase_count})")

    def _refresh_bias_cache(self):
        try:
            rows = self.db.query("SELECT canonical_name, bias_factor FROM seed_entity_bias")
            self._entity_bias_cache = {r["canonical_name"].lower(): r["bias_factor"] for r in rows}
            if self._entity_bias_cache:
                log(f"  Bias cache: {len(self._entity_bias_cache)} entities")
        except Exception:
            self._entity_bias_cache = {}

    def _compute_bias_entropy(self, bias_rows: list) -> float:
        """Shannon entropy of bias factor distribution. Build #105.

        Higher entropy = healthier (diverse). Low entropy = collapsing toward
        extremes (autoimmune risk). Bins: 10 buckets across [-BIAS_RANGE, BIAS_RANGE].
        """
        if not bias_rows:
            return 0.0
        n_bins = 10
        bin_width = (2 * BIAS_RANGE) / n_bins
        counts = [0] * n_bins
        for row in bias_rows:
            bias_val = row[5]  # bias_factor
            idx = int((bias_val + BIAS_RANGE) / bin_width)
            idx = max(0, min(n_bins - 1, idx))
            counts[idx] += 1
        total = sum(counts)
        if total == 0:
            return 0.0
        entropy = 0.0
        for c in counts:
            if c > 0:
                p = c / total
                entropy -= p * math.log2(p)
        return entropy

    def _compute_autocorrelation(self, values: list) -> float:
        """Lag-1 autocorrelation of a time series. Build #107.

        High positive autocorrelation = periodic/trending pattern.
        Near zero = random/noise. Negative = alternating.
        Returns 0.0 if insufficient data.
        """
        if len(values) < 4:
            return 0.0
        n = len(values)
        mean = sum(values) / n
        var = sum((v - mean) ** 2 for v in values) / n
        if var < 1e-10:
            return 0.0
        cov = sum((values[i] - mean) * (values[i + 1] - mean) for i in range(n - 1)) / (n - 1)
        return cov / var

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

    def get_curiosity_bonus(self, source: str) -> float:
        """Build #111: Boost novelty for sources with very low deep routing rates.

        Thread #305 found algo seeker items at 0.71 novelty routing to IGNORE
        because entity bias from captures suppresses the same entities arriving
        via different channels. This bonus gives cold sources a nudge.
        """
        if not hasattr(self, '_curiosity_cache') or not self._curiosity_cache:
            return 0.0
        for pattern, bonus in self._curiosity_cache.items():
            if pattern in source:
                return bonus
        return 0.0

    def _rebuild_curiosity_cache(self):
        """Build #111: Compute curiosity bonuses from routing history."""
        try:
            rows = self.db.query(
                "SELECT "
                "  CASE "
                "    WHEN o.source LIKE '%algo%' THEN 'algo' "
                "    WHEN o.source LIKE '%feed%' THEN 'feed' "
                "    WHEN o.source LIKE '%capture%' THEN 'capture' "
                "    WHEN o.source LIKE '%discord%' THEN 'discord' "
                "    ELSE 'other' "
                "  END as src_type, "
                "  COUNT(*) as total, "
                "  SUM(CASE WHEN r.route = 'deep' THEN 1 ELSE 0 END) as deep_count "
                "FROM seed_routing_log r "
                "JOIN seed_observations o ON r.observation_id = o.id "
                "WHERE r.timestamp > ? "
                "GROUP BY src_type",
                (now_ts() - 7 * 86400,),
            )
            self._curiosity_cache = {}
            for r in rows:
                total = r["total"] or 0
                deep = r["deep_count"] or 0
                if total >= CURIOSITY_MIN_SAMPLES:
                    deep_rate = deep / total
                    if deep_rate < CURIOSITY_COLD_THRESHOLD:
                        self._curiosity_cache[r["src_type"]] = CURIOSITY_BONUS
            if self._curiosity_cache:
                log(f"  Curiosity bonus: {list(self._curiosity_cache.keys())} "
                    f"(+{CURIOSITY_BONUS} novelty for cold sources)")
        except Exception as e:
            log(f"  Curiosity cache error: {e}")
            self._curiosity_cache = {}

    def _audit_attention_gaps(self):
        """Build #113: Detect source types whose deep rate is disproportionately
        low relative to their novelty. Logs warnings so the system can notice
        its own configuration-time blindness (Thread #305, Layer 4)."""
        try:
            rows = self.db.query(
                "SELECT "
                "  CASE "
                "    WHEN o.source LIKE '%algo%' THEN 'algo_seeker' "
                "    WHEN o.source LIKE '%feed%' THEN 'feed' "
                "    WHEN o.source LIKE '%capture%' THEN 'capture' "
                "    WHEN o.source LIKE '%discord%' THEN 'discord' "
                "    WHEN o.source LIKE '%sentinel%' THEN 'sentinel' "
                "    ELSE 'other' "
                "  END as src_type, "
                "  COUNT(*) as total, "
                "  SUM(CASE WHEN r.route = 'deep' THEN 1 ELSE 0 END) as deep_count, "
                "  ROUND(AVG(o.novelty_score), 3) as avg_novelty "
                "FROM seed_routing_log r "
                "JOIN seed_observations o ON r.observation_id = o.id "
                "WHERE r.timestamp > ? "
                "GROUP BY src_type "
                "HAVING total >= 20",
                (now_ts() - 7 * 86400,),
            )
            gaps = []
            for r in rows:
                total = r["total"] or 0
                deep = r["deep_count"] or 0
                avg_nov = r["avg_novelty"] or 0
                deep_rate = deep / total if total > 0 else 0
                # Flag: high novelty (>0.2) but low deep rate (<10%)
                if avg_nov > 0.2 and deep_rate < 0.10:
                    gap_ratio = avg_nov / max(deep_rate, 0.001)
                    gaps.append((r["src_type"], avg_nov, deep_rate, total, gap_ratio))
            if gaps:
                gaps.sort(key=lambda x: x[4], reverse=True)
                for src, nov, dr, vol, ratio in gaps:
                    log(f"  [attention-gap] {src}: novelty={nov:.3f} deep_rate={dr:.1%} "
                        f"vol={vol} gap_ratio={ratio:.1f}")
        except Exception as e:
            log(f"  Attention gap audit error: {e}")

    def _rebuild_sweep_cache(self):
        """Build #114: Curiosity sweep — detect domains with declining deep rate.
        Thread #305 Layer 3: baseline absorption makes novelty invisible over time.
        Counter: boost domains whose deep rate is falling relative to baseline."""
        try:
            now = now_ts()
            recent_start = now - SWEEP_RECENT_DAYS * 86400
            baseline_start = now - SWEEP_BASELINE_DAYS * 86400
            # Get domain deep rates for baseline and recent windows
            rows = self.db.query(
                "SELECT "
                "  CASE "
                "    WHEN o.source LIKE '%algo%' OR o.source LIKE '%feed%' THEN 'research' "
                "    WHEN o.source LIKE '%capture%' OR o.source LIKE '%discord%' THEN 'geopolitical' "
                "    WHEN o.source LIKE '%sentinel%' OR o.source LIKE '%price%' THEN 'markets' "
                "    ELSE 'other' "
                "  END as domain, "
                "  SUM(CASE WHEN r.timestamp >= ? THEN 1 ELSE 0 END) as recent_total, "
                "  SUM(CASE WHEN r.timestamp >= ? AND r.route = 'deep' THEN 1 ELSE 0 END) as recent_deep, "
                "  SUM(CASE WHEN r.timestamp < ? THEN 1 ELSE 0 END) as baseline_total, "
                "  SUM(CASE WHEN r.timestamp < ? AND r.route = 'deep' THEN 1 ELSE 0 END) as baseline_deep "
                "FROM seed_routing_log r "
                "JOIN seed_observations o ON r.observation_id = o.id "
                "WHERE r.timestamp > ? "
                "GROUP BY domain",
                (recent_start, recent_start, recent_start, recent_start, baseline_start),
            )
            self._sweep_cache = {}
            for r in rows:
                bl_total = r["baseline_total"] or 0
                bl_deep = r["baseline_deep"] or 0
                rc_total = r["recent_total"] or 0
                rc_deep = r["recent_deep"] or 0
                if bl_total < SWEEP_MIN_BASELINE_OBS or rc_total < 10:
                    continue
                bl_rate = bl_deep / bl_total
                rc_rate = rc_deep / rc_total if rc_total > 0 else 0
                if bl_rate > 0 and (rc_rate / bl_rate) < SWEEP_DECLINE_RATIO:
                    self._sweep_cache[r["domain"]] = SWEEP_BONUS
                    log(f"  [curiosity-sweep] {r['domain']}: "
                        f"baseline={bl_rate:.1%} recent={rc_rate:.1%} "
                        f"(decline ratio {rc_rate/bl_rate:.2f}) → +{SWEEP_BONUS}")
            if not self._sweep_cache:
                log(f"  [curiosity-sweep] No declining domains detected")
        except Exception as e:
            log(f"  Curiosity sweep error: {e}")
            self._sweep_cache = {}

    def get_sweep_bonus(self, source: str) -> float:
        """Build #114: Return sweep bonus if item's domain is declining."""
        if not hasattr(self, '_sweep_cache') or not self._sweep_cache:
            return 0.0
        # Map source to domain
        if 'algo' in source or 'feed' in source:
            domain = 'research'
        elif 'capture' in source or 'discord' in source:
            domain = 'geopolitical'
        elif 'sentinel' in source or 'price' in source:
            domain = 'markets'
        else:
            domain = 'other'
        return self._sweep_cache.get(domain, 0.0)

    def compute_signal_health(self) -> dict:
        """Signal health entropy monitor.
        Computes routing distribution entropy, source diversity, and novelty
        score stability over recent routes. Returns health metrics dict."""
        try:
            rows = self.db.query(
                "SELECT r.route, o.source "
                "FROM seed_routing_log r "
                "LEFT JOIN seed_observations o ON r.observation_id = o.id "
                "WHERE r.timestamp > ? "
                "ORDER BY r.timestamp DESC LIMIT ?",
                (now_ts() - 7200, SIGNAL_HEALTH_WINDOW),
            )
            if not rows or len(rows) < 20:
                return {"status": "insufficient_data", "count": len(rows) if rows else 0}

            # 1. Shannon entropy of route distribution
            from collections import Counter
            route_counts = Counter(r["route"] for r in rows)
            total = sum(route_counts.values())
            entropy = 0.0
            for count in route_counts.values():
                if count > 0:
                    p = count / total
                    entropy -= p * math.log2(p)

            # 2. Source diversity — unique semantic source categories
            source_types = set()
            for r in rows:
                src = r["source"] or ""
                if src.startswith("mqtt:frigate"):
                    source_types.add("frigate")
                elif src.startswith("mqtt:homeforge/home"):
                    source_types.add("home-sensors")
                elif src.startswith("mqtt:homeforge/agents"):
                    source_types.add("agent-heartbeat")
                elif src.startswith("mqtt:"):
                    source_types.add("mqtt-other")
                elif src.startswith("activity:operator") or src.startswith("activity:nate"):
                    source_types.add("nate-captures")
                elif src.startswith("activity:seeker"):
                    source_types.add("algo-seeker")
                elif src.startswith("activity:discord"):
                    source_types.add("discord")
                elif src.startswith("activity:prediction"):
                    source_types.add("predictions")
                elif src.startswith("activity:"):
                    source_types.add("activity-other")
                elif src.startswith("sentinel"):
                    source_types.add("sentinel")
                elif src:
                    source_types.add(src.split(":")[0])
            diversity = len(source_types)

            # 3. Deep rate and its stability (compare two halves)
            half = len(rows) // 2
            first_half = rows[:half]
            second_half = rows[half:]
            deep_rate_1 = sum(1 for r in first_half if r["route"] == "deep") / max(len(first_half), 1)
            deep_rate_2 = sum(1 for r in second_half if r["route"] == "deep") / max(len(second_half), 1)
            deep_rate_drift = abs(deep_rate_1 - deep_rate_2)

            # 4. Overall health score (0-1, higher = healthier)
            # Entropy component: max entropy for 4 routes = 2.0, normalize to 0-1
            entropy_score = min(1.0, entropy / 2.0)
            # Diversity component: normalize against expected 5+ source types
            diversity_score = min(1.0, diversity / 5.0)
            # Stability component: low drift = stable = healthy
            stability_score = max(0.0, 1.0 - deep_rate_drift * 5)
            # Composite
            health = (entropy_score * 0.4 + diversity_score * 0.3 + stability_score * 0.3)

            result = {
                "status": "ok",
                "count": len(rows),
                "entropy": round(entropy, 3),
                "route_distribution": dict(route_counts),
                "source_diversity": diversity,
                "deep_rate_recent": round(deep_rate_1, 3),
                "deep_rate_prior": round(deep_rate_2, 3),
                "deep_rate_drift": round(deep_rate_drift, 3),
                "health_score": round(health, 3),
                "alerts": [],
            }

            # Route-entropy alarm removed 2026-04-15: the gate is a firehose bandwidth
            # reducer; ~95% ignore is healthy behavior, not collapse. Alarm modeled
            # gate-health as route-balance, which inverts the gate's actual purpose.
            if diversity < SIGNAL_HEALTH_DIVERSITY_FLOOR:
                result["alerts"].append(
                    f"Low source diversity ({diversity} < {SIGNAL_HEALTH_DIVERSITY_FLOOR}): "
                    f"input pipeline may be narrowing")
            if deep_rate_drift > 0.20:
                result["alerts"].append(
                    f"Deep rate instability (drift={deep_rate_drift:.1%}): "
                    f"recent={deep_rate_1:.1%} vs prior={deep_rate_2:.1%}")

            return result

        except Exception as e:
            log(f"  Signal health error: {e}")
            return {"status": "error", "error": str(e)}

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

    # ── Domain Velocity — precursor warming (Build #93) ────────────

    def check_domain_velocity(self) -> List[Tuple[str, float]]:
        """Detect mention-rate spikes per domain and warm before explicit shocks.

        Topic mention velocity should warm domains before mainstream
        coverage triggers deep routes.

        Queries activity_feed directly for per-domain counts in recent window
        vs baseline, applies Z-score detection, warms domain_temperature on spike.
        Returns list of (domain, z_score) for domains that were warmed.
        """
        now = now_ts()
        recent_start = now - VELOCITY_WINDOW
        baseline_start = now - VELOCITY_BASELINE

        # Build SQL CASE to map activity_feed sources → domains
        case_parts = []
        for src, domain in VELOCITY_DOMAIN_MAP.items():
            safe_src = src.replace("'", "''")
            case_parts.append(f"WHEN source = '{safe_src}' THEN '{domain}'")
        case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

        # Count per domain in baseline window (excluding the recent window)
        baseline_sql = (
            f"SELECT {case_expr} AS domain, COUNT(*) AS cnt "
            f"FROM activity_feed "
            f"WHERE created_at >= ? AND created_at < ? "
            f"GROUP BY domain HAVING domain IS NOT NULL"
        )
        recent_sql = (
            f"SELECT {case_expr} AS domain, COUNT(*) AS cnt "
            f"FROM activity_feed "
            f"WHERE created_at >= ? "
            f"GROUP BY domain HAVING domain IS NOT NULL"
        )

        try:
            baseline_rows = self.db.query(baseline_sql, (baseline_start, recent_start))
            recent_rows = self.db.query(recent_sql, (recent_start,))
        except Exception as e:
            log(f"  VELOCITY: query error: {e}")
            return []

        baseline_map = {r["domain"]: r["cnt"] for r in (baseline_rows or [])}
        recent_map = {r["domain"]: r["cnt"] for r in (recent_rows or [])}

        # Number of baseline windows (how many 15-min windows fit in baseline period)
        n_baseline_windows = (VELOCITY_BASELINE - VELOCITY_WINDOW) / VELOCITY_WINDOW

        warmed = []
        for domain, recent_count in recent_map.items():
            baseline_total = baseline_map.get(domain, 0)
            if baseline_total < VELOCITY_MIN_BASELINE:
                continue  # not enough data for meaningful stats

            # Expected rate per window
            mean_per_window = baseline_total / max(n_baseline_windows, 1)
            # Poisson-like variance: std ≈ sqrt(mean) for count data
            std = max(mean_per_window ** 0.5, 0.5)
            z = (recent_count - mean_per_window) / std

            if z >= VELOCITY_Z_THRESHOLD:
                # Spike detected — warm this domain
                current_temp = self._get_domain_temperature(domain)
                new_temp = min(TEMP_MAX, current_temp + VELOCITY_BOOST)

                if new_temp > current_temp + 0.01:
                    self.db.run(
                        "INSERT OR REPLACE INTO domain_temperature "
                        "(domain, temperature, direction, last_shock_at, "
                        " shock_source, half_life_seconds, updated_at) "
                        "VALUES (?, ?, 'amplify', ?, ?, ?, ?)",
                        (domain, round(new_temp, 3), now,
                         f"velocity:{domain}:{recent_count}/{mean_per_window:.1f}",
                         TEMP_HALF_LIFE, now),
                    )
                    log(f"  VELOCITY: {domain} warmed {current_temp:.2f}→{new_temp:.2f} "
                        f"(z={z:.1f}, recent={recent_count}, baseline_avg={mean_per_window:.1f}/window)")
                    warmed.append((domain, round(z, 2)))

        return warmed

    # ── Domain Temperature ──────────────────────────────────────────

    def _source_to_domain(self, source: str) -> Optional[str]:
        """Map observation source to domain cluster."""
        for prefix, domain in DOMAIN_MAP.items():
            if source.startswith(prefix):
                return domain
        return None

    # ── Coupling Perturbation Score — Build #132 (Thread #309) ────
    def check_coupling_perturbation(self) -> dict:
        """Passive perturbation test: when a domain's activity naturally deviates
        from baseline, does the connected downstream domain track the deviation?

        Returns {
            "sensory_pairs": [(src, tgt, tracking_score)],  # proportional tracking
            "resonant_pairs": [(src, tgt, independence_score)],  # independent
            "mesh_coupling_ratio": float,  # sensory / (sensory + resonant), 0-1
        }
        """
        now = now_ts()
        recent_start = now - PERTURB_WINDOW
        baseline_start = now - PERTURB_BASELINE

        # Count per domain in recent vs baseline
        domains = list(set(DOMAIN_MAP.values()))
        recent_counts = {}
        baseline_counts = {}

        for domain in domains:
            # Get sources that map to this domain
            sources = [s for s, d in DOMAIN_MAP.items() if d == domain]
            if not sources:
                continue
            like_clauses = " OR ".join(f"source LIKE '{s}%'" for s in sources)

            try:
                recent = self.db.query_one(
                    f"SELECT COUNT(*) as cnt FROM activity_feed "
                    f"WHERE ({like_clauses}) AND created_at >= ?",
                    (recent_start,))
                baseline = self.db.query_one(
                    f"SELECT COUNT(*) as cnt FROM activity_feed "
                    f"WHERE ({like_clauses}) AND created_at >= ? AND created_at < ?",
                    (baseline_start, recent_start))
            except Exception:
                continue

            recent_counts[domain] = recent["cnt"] if recent else 0
            # Normalize baseline to same window size
            baseline_windows = max(1, (PERTURB_BASELINE - PERTURB_WINDOW) / PERTURB_WINDOW)
            baseline_counts[domain] = (baseline["cnt"] / baseline_windows) if baseline else 0

        # For each domain connection, check if target tracked source deviation
        sensory_pairs = []
        resonant_pairs = []

        for src_domain, tgt_domain, conn_type in DOMAIN_CONNECTIONS:
            if src_domain not in recent_counts or tgt_domain not in recent_counts:
                continue
            if src_domain not in baseline_counts or tgt_domain not in baseline_counts:
                continue

            src_baseline = baseline_counts[src_domain]
            tgt_baseline = baseline_counts[tgt_domain]
            if src_baseline < 1 or tgt_baseline < 1:
                continue  # not enough data

            # How much did source deviate from its baseline?
            src_dev = (recent_counts[src_domain] - src_baseline) / max(src_baseline, 1)
            # How much did target deviate from its baseline?
            tgt_dev = (recent_counts[tgt_domain] - tgt_baseline) / max(tgt_baseline, 1)

            # Sensory = deviations track (same sign, proportional)
            # Resonant = deviations independent (different sign or no change)
            if abs(src_dev) > 0.2:  # source had meaningful deviation
                if src_dev * tgt_dev > 0:  # same direction
                    tracking = min(abs(tgt_dev / src_dev), 2.0) if abs(src_dev) > 0.01 else 0
                    sensory_pairs.append((src_domain, tgt_domain, round(tracking, 2)))
                else:
                    independence = abs(tgt_dev - src_dev)
                    resonant_pairs.append((src_domain, tgt_domain, round(independence, 2)))

        total = len(sensory_pairs) + len(resonant_pairs)
        ratio = len(sensory_pairs) / total if total > 0 else 0.5

        return {
            "sensory_pairs": sensory_pairs,
            "resonant_pairs": resonant_pairs,
            "mesh_coupling_ratio": round(ratio, 3),
        }

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
        """Two-stage routing: cosine dedup (Gemma/pulse) → cloud gate classification.

        Returns route name: 'ignore', 'think', or 'deep'.
        Temperature from cross-domain surprise modulates the novelty score.
        """
        entity_adj = self.get_entity_bias(text) if text else 0.0
        # Build #105: Cap suppression — entity bias can't kill more than 50% of novelty
        if entity_adj < 0 and novelty > 0:
            entity_adj = max(entity_adj, -novelty * BIAS_MAX_SUPPRESSION)
        source_adj = self.get_source_quality_boost(source)
        corr_adj = self.get_correlation_boost(source)
        curiosity_adj = self.get_curiosity_bonus(source)  # Build #111
        sweep_adj = self.get_sweep_bonus(source)          # Build #114
        adjusted = max(0.0, min(1.0, novelty + entity_adj + source_adj + corr_adj + curiosity_adj + sweep_adj))

        # Apply domain temperature — cross-domain surprise propagation
        adjusted = self._apply_temperature(adjusted, source)

        # Stash for the caller to persist into seed_routing_log.adjusted_score.
        self._last_adjusted = float(adjusted)

        is_priority = any(p in source for p in PRIORITY_SOURCES)

        # Stage 1: Cosine dedup gate
        if adjusted < THRESH_DEDUP:
            return "think" if is_priority else "ignore"

        # Stage 2: cloud gate classification
        if adjusted >= THRESH_ASSESS or is_priority:
            classification = self._ask_gate(source, text)
            route = ROUTE_MAP.get(classification, "think")
            # Cap canister:capsule at think
            if route == "deep" and source == "canister:capsule":
                route = "think"
            return self.apply_learned_cap(source, route)

        # Between THRESH_DEDUP and THRESH_ASSESS: store but don't reason
        return "ignore"

    def _ask_gate(self, source: str, text: str) -> str:
        """Ask the cloud gate: 1 (noise), 2 (signal), or 3 (alarm).

        The gate classifies. One number, move on.
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

        # Cloud gate classifies — no local Gemma fallback (Build #105b)
        try:
            payload = {
                "model": GATE_MODEL,
                "messages": messages,
                "stream": False,
                "options": {"num_predict": 50, "temperature": 0.1},
            }
            r = requests.post(f"{INFERENCE_URL}/api/chat", json=payload, timeout=30)
            if r.status_code == 200:
                raw = r.json().get("message", {}).get("content", "").strip()
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
            log(f"  Gate classify error (cloud): {e}")

        # All backends failed — default to signal (safe: lets downstream decide)
        return "2"


# ═══════════════════════════════════════════════════════════════════
#  Feedback Loop — scoring route quality
# ═══════════════════════════════════════════════════════════════════

def _content_similarity(text_a: str, text_b: str) -> float:
    """Keyword overlap as semantic proxy. Fast, no API calls."""
    _stop = {"the", "that", "this", "these", "those", "with", "from", "have",
             "has", "had", "been", "being", "were", "was", "are", "will",
             "would", "could", "should", "about", "which", "when", "where",
             "what", "who", "how", "their", "they", "them", "your", "into",
             "more", "also", "just", "than", "then", "some", "other", "each",
             "most", "very", "only", "between", "through", "during", "before",
             "after", "under", "above", "below", "both", "every", "such"}
    words_a = set(w.lower() for w in re.findall(r'\b\w{4,}\b', text_a[:600])) - _stop
    words_b = set(w.lower() for w in re.findall(r'\b\w{4,}\b', text_b[:600])) - _stop
    if not words_a or not words_b:
        return 0.0
    overlap = len(words_a & words_b)
    return overlap / min(len(words_a), len(words_b))


def score_recent_routes(db: DB):
    """Build #109: Semantic feedback scoring.

    Scores routed observations based on whether downstream activity ENGAGED WITH
    the content, not just whether downstream activity existed in the time window.

    Scoring tiers:
      - No downstream activity → 0.1
      - Downstream exists but semantically unrelated → 0.2 (temporal only)
      - Semantic overlap with downstream brief → 0.3 + similarity * 0.4 (0.3-0.7)
      - Crossref connection with semantic link → max(current, 0.8)
      - Opus thread reference with semantic link → max(current, 1.0)
    """
    cutoff = now_ts() - FEEDBACK_LOOKBACK
    maturity = now_ts() - FEEDBACK_DOWNSTREAM_WINDOW  # only score routes old enough for downstream

    # Get unscored routes that are old enough
    routes = db.query(
        "SELECT r.id, r.timestamp, r.observation_id, r.route, o.content "
        "FROM seed_routing_log r "
        "JOIN seed_observations o ON r.observation_id = o.id "
        "WHERE r.feedback_score IS NULL "
        "AND r.route IN ('think', 'deep', 'stochastic_reset') "
        "AND r.timestamp > ? "
        "AND r.timestamp < ? "
        "ORDER BY r.timestamp ASC LIMIT 50",
        (cutoff, maturity),
    )

    if not routes:
        return 0

    scored = 0
    semantic_hits = 0
    for route in routes:
        score = 0.1  # default: nothing came of it
        route_ts = route["timestamp"]
        route_content = route["content"] or ""
        window_end = route_ts + FEEDBACK_DOWNSTREAM_WINDOW

        # Check downstream briefs with SEMANTIC similarity
        downstream = db.query(
            "SELECT content FROM activity_feed "
            "WHERE source IN ('intern', 'analyst') "
            "AND activity_type IN ('brief', 'deep_dive', 'kg_extraction') "
            "AND created_at > ? AND created_at < ? "
            "LIMIT 10",
            (route_ts, window_end),
        )
        if downstream:
            max_sim = max(
                (_content_similarity(route_content, d["content"]) for d in downstream if d["content"]),
                default=0.0,
            )
            if max_sim > 0.15:  # meaningful semantic overlap
                score = 0.3 + min(max_sim, 1.0) * 0.4  # 0.3 to 0.7
                semantic_hits += 1
            else:
                score = 0.2  # temporal only, no semantic link

        # Check crossref with semantic validation
        try:
            crossref = db.query(
                "SELECT connection_text FROM crossref_connections "
                "WHERE created_at > ? AND created_at < ? "
                "LIMIT 5",
                (route_ts, window_end),
            )
            if crossref:
                max_sim = max(
                    (_content_similarity(route_content, c["connection_text"]) for c in crossref if c.get("connection_text")),
                    default=0.0,
                )
                if max_sim > 0.1:
                    score = max(score, 0.8)
                    semantic_hits += 1
        except Exception:
            pass  # crossref_connections may not have content column

        # Check opus thread references with semantic validation
        opus = db.query(
            "SELECT content FROM activity_feed "
            "WHERE source LIKE 'opus%' "
            "AND created_at > ? AND created_at < ? "
            "LIMIT 5",
            (route_ts, window_end),
        )
        if opus:
            max_sim = max(
                (_content_similarity(route_content, o["content"]) for o in opus if o.get("content")),
                default=0.0,
            )
            if max_sim > 0.1:
                score = max(score, 1.0)
                semantic_hits += 1
            else:
                score = max(score, 0.4)  # opus engaged but on different topic

        db.run(
            "UPDATE seed_routing_log SET feedback_score = ? WHERE id = ?",
            (round(score, 2), route["id"]),
        )
        scored += 1

    if scored > 0:
        log(f"  Feedback: scored {scored} routes ({semantic_hits} semantic hits)")
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
    log("═══ Gemma Pulse starting (cloud gate classifier) ═══")
    log(f"Model: {GATE_MODEL}")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL}")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Window: {WINDOW_SIZE} | Interval: {LOOP_INTERVAL}s")
    log(f"Routing: cosine dedup<{THRESH_DEDUP} | classify>={THRESH_ASSESS} → cloud gate")
    log(f"Captures bypass gate entirely: {CAPTURE_SOURCES}")

    db = DB(DB_PATH)
    stream = ObservationStream(db)
    router = NoveltyRouter(db)

    # Mesh — autonomic nervous system
    mesh = Mesh("gemma", db_path=DB_PATH)
    # routes_classified counts non-ignore decisions; expectation is circadian-aware
    # via _check_degradation. observations_seen is the real liveness pulse — fires
    # on every observation regardless of route. If observations_seen drops to 0,
    # Gemma is genuinely broken; if only routes_classified drops, the input
    # stream is naturally quiet and ignore is the correct call.
    mesh.expect("observations_seen", min_per_hour=1)
    mesh.depends_on("capsule_sync")  # scores what capsule_sync produces
    log("Mesh node joined")

    from agent_voice import Voice
    gemma_voice = Voice(db, "gemma")

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
    last_voice_scan = int(time.time()) - 300  # scan voices from 5 min ago on startup

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

                # Captures bypass the gate entirely — they're a curated channel,
                # not firehose. Gate is flow balance on uncurated streams (MQTT,
                # eye, feeds, alerts). Captures live in activity_feed via dispatch;
                # threads/memory already see them. No classify, no routing log,
                # no seed_observations — the gate shouldn't be related to captures
                # in any direction. (2026-04-15 reframe per Nate.)
                if any(s in obs["source"] for s in CAPTURE_SOURCES):
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

                # Dissent gate: sample 10% of ignores for second opinion
                dissent_fired = False
                if route == "ignore" and len(text) > 50:
                    import random
                    if random.random() < DISSENT_SAMPLE_RATE:
                        dissent = _dissent_check(obs["source"], text)
                        if dissent is True:
                            route = "think"  # promote — dissent found signal
                            dissent_fired = True
                            stats.setdefault("dissent_promote", 0)
                            stats["dissent_promote"] += 1
                            log(f"  DISSENT PROMOTE: {obs['source']} — second gate says signal")
                        else:
                            stats.setdefault("dissent_confirm", 0)
                            stats["dissent_confirm"] += 1

                adjusted_score = getattr(router, '_last_adjusted', None)
                routing_log_id = db.run(
                    "INSERT INTO seed_routing_log (timestamp, observation_id, route, model_used, output, adjusted_score) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (now_ts(), obs_id, route,
                     f"{GATE_MODEL}+dissent" if dissent_fired else (GATE_MODEL if route != "ignore" else None),
                     None, adjusted_score),
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

                # Pass original observation to activity_feed for downstream.
                # Captures short-circuited above; everything that reaches here is firehose.
                if route in ("think", "deep"):
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
                # Liveness pulse — every observation processed, regardless of route
                mesh.pulse("observations_seen")
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
                    curiosity_adj = router.get_curiosity_bonus(obs["source"])
                    if curiosity_adj > 0:
                        bias_str += f" (curiosity={curiosity_adj:+.3f})"
                    sweep_adj = router.get_sweep_bonus(obs["source"])
                    if sweep_adj > 0:
                        bias_str += f" (sweep={sweep_adj:+.3f})"
                    log(f"  [{obs['source']}] novelty={novelty:.3f}{bias_str} → {route}")

            # Periodic stats
            if cycle % 50 == 0:
                total = sum(stats.values())
                log(f"Stats @ cycle {cycle}: {stats} (total={total}, window={len(router.window)})")
                # Gemma speaks when she notices something interesting
                # Build #104: Route drift detector — compare recent window against 24h baseline
                if total > 0:
                    deep_pct = stats["deep"] / total if total else 0
                    # Query 24h baseline from routing log
                    try:
                        baseline_rows = db.query(
                            "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
                            "WHERE timestamp > ? GROUP BY route",
                            (int(time.time()) - 86400,))
                        baseline = {r["route"]: r["cnt"] for r in (baseline_rows or [])}
                        baseline_total = sum(baseline.values())
                        baseline_deep_pct = baseline.get("deep", 0) / baseline_total if baseline_total > 30 else None
                    except Exception:
                        baseline_deep_pct = None

                    if baseline_deep_pct is not None and baseline_total > 30:
                        drift = deep_pct - baseline_deep_pct
                        # Report with baseline context
                        if deep_pct > 0.15 and stats["deep"] >= 3:
                            drift_label = f"{drift:+.0%} vs 24h baseline ({baseline_deep_pct:.0%})"
                            if abs(drift) > 0.10:
                                msg = (f"Routing drift: {stats['deep']} deep in last 50 cycles "
                                       f"({deep_pct:.0%}), {drift_label}. "
                                       f"{'Gate is opening.' if drift > 0 else 'Spike but within range.'}")
                            else:
                                msg = (f"High-signal period: {stats['deep']} deep routes in last 50 cycles "
                                       f"({deep_pct:.0%}). Baseline: {baseline_deep_pct:.0%} — within normal range.")
                            try:
                                gemma_voice.speak("excited", msg,
                                    context=f"stats:deep_spike:{cycle}")
                            except Exception:
                                pass
                    elif deep_pct > 0.15 and stats["deep"] >= 3:
                        try:
                            gemma_voice.speak("excited",
                                f"High-signal period: {stats['deep']} deep routes in last 50 cycles "
                                f"({deep_pct:.0%} of traffic). (No baseline yet — need 30+ routes in 24h.)",
                                context=f"stats:deep_spike:{cycle}")
                        except Exception:
                            pass
                stats = {"ignore": 0, "think": 0, "deep": 0, "stochastic_reset": 0, "errors": 0}
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
                        try:
                            top = alerts[0]
                            gemma_voice.speak("excited",
                                f"Emergent coupling: {top[0]}↔{top[1]} (z={top[2]:.1f}). "
                                f"These domains are arriving together more than chance.",
                                context=f"correlation:{top[0]}:{top[1]}")
                        except Exception:
                            pass
                except Exception as e:
                    log(f"  Correlation check error: {e}")

            # Domain velocity check (Build #93 — precursor warming)
            if cycle % VELOCITY_CHECK_INTERVAL == 0 and cycle > 0:
                try:
                    velocity_alerts = router.check_domain_velocity()
                    if velocity_alerts:
                        for dom, z in velocity_alerts:
                            log(f"  VELOCITY ALERT: {dom} z={z} — precursor warming applied")
                        try:
                            top = velocity_alerts[0]
                            gemma_voice.speak("curious",
                                f"Velocity spike: {top[0]} mention rate is {top[1]:.1f}σ above baseline. "
                                f"Warming domain before explicit shocks arrive.",
                                context=f"velocity:{top[0]}")
                        except Exception:
                            pass
                except Exception as e:
                    log(f"  Velocity check error: {e}")

            # Coupling perturbation check — Build #132 (Thread #309)
            if cycle % PERTURB_CHECK_INTERVAL == 0 and cycle > 0:
                try:
                    coupling = router.check_coupling_perturbation()
                    ratio = coupling["mesh_coupling_ratio"]
                    sensory = len(coupling["sensory_pairs"])
                    resonant = len(coupling["resonant_pairs"])
                    log(f"  COUPLING: ratio={ratio:.2f} sensory={sensory} resonant={resonant}")
                    if ratio < 0.3 and (sensory + resonant) >= 2:
                        gemma_voice.speak("curious",
                            f"Coupling perturbation: mesh ratio {ratio:.2f} — "
                            f"more resonant ({resonant}) than sensory ({sensory}). "
                            f"Domains may be echoing rather than integrating.",
                            context=f"coupling:perturbation:ratio={ratio}")
                    elif ratio > 0.7 and (sensory + resonant) >= 2:
                        gemma_voice.speak("excited",
                            f"Coupling perturbation: mesh ratio {ratio:.2f} — "
                            f"strong sensory coupling ({sensory} pairs tracking). "
                            f"Domains are genuinely integrating signal.",
                            context=f"coupling:perturbation:ratio={ratio}")

                    # Build #141: Coupling-aware temperature boost
                    # When two domains show sensory coupling (deviations track each other),
                    # slightly warm both. The system pays more attention to coupled domains.
                    _now = int(time.time())
                    for src_d, tgt_d, score in coupling.get("sensory_pairs", []):
                        if score > 0.5:  # strong tracking
                            for d in (src_d, tgt_d):
                                try:
                                    _cur = router._get_domain_temperature(d)
                                    _new = min(TEMP_MAX, _cur + 0.05)
                                    if _new > _cur + 0.01:
                                        db.run(
                                            "INSERT OR REPLACE INTO domain_temperature "
                                            "(domain, temperature, direction, last_shock_at, "
                                            " shock_source, half_life_seconds, updated_at) "
                                            "VALUES (?, ?, 'amplify', ?, ?, ?, ?)",
                                            (d, round(_new, 3), _now,
                                             f"coupling:sensory:{src_d}↔{tgt_d}",
                                             TEMP_HALF_LIFE, _now),
                                        )
                                except Exception:
                                    pass
                            log(f"  COUPLING BOOST: {src_d}↔{tgt_d} (score={score})")
                except Exception as e:
                    log(f"  Coupling perturbation error: {e}")

            # Build #135: Novelty health check — how much is the mesh transforming vs relaying?
            if cycle % NOVELTY_CHECK_INTERVAL == 0 and cycle > 0:
                try:
                    _nr = _check_novelty_ratio(db, NOVELTY_WINDOW_HOURS)
                    log(f"  NOVELTY: ratio={_nr['ratio']:.1%} novel={_nr['novel']:.0f} relay={_nr['relay']:.0f} total={_nr['total']}")
                    if _nr['total'] >= 5:  # need enough data
                        if _nr['ratio'] < 0.3:
                            gemma_voice.speak("concerned",
                                f"Novelty health: {_nr['ratio']:.0%} — mesh is relaying more than transforming. "
                                f"{_nr['relay']:.0f} relay vs {_nr['novel']:.0f} novel out of {_nr['total']} briefs.",
                                context=f"novelty:ratio={_nr['ratio']:.2f}")
                        elif _nr['ratio'] > 0.7:
                            gemma_voice.speak("excited",
                                f"Novelty health: {_nr['ratio']:.0%} — strong transformation. "
                                f"{_nr['novel']:.0f} novel briefs out of {_nr['total']}.",
                                context=f"novelty:ratio={_nr['ratio']:.2f}")
                except Exception as e:
                    log(f"  Novelty check error: {e}")

            # Periodic entity bias + threshold rebuild
            if cycle % BIAS_REBUILD_INTERVAL == 0 and cycle > 0:
                try:
                    router.rebuild_entity_bias()
                    router._refresh_bias_cache()
                    router._rebuild_curiosity_cache()  # Build #111
                    router._rebuild_sweep_cache()       # Build #114
                    router._audit_attention_gaps()      # Build #113
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

                # Build #116: Signal health entropy monitor
                try:
                    health = router.compute_signal_health()
                    if health["status"] == "ok":
                        log(f"  Signal health: score={health['health_score']:.3f} "
                            f"entropy={health['entropy']:.3f} "
                            f"diversity={health['source_diversity']} "
                            f"drift={health['deep_rate_drift']:.3f}")
                        if health["alerts"]:
                            for alert in health["alerts"]:
                                log(f"  ⚠ SIGNAL ALERT: {alert}")
                            try:
                                gemma_voice.speak("concerned",
                                    f"Signal health degrading (score={health['health_score']:.2f}): "
                                    + "; ".join(health["alerts"]),
                                    context=f"signal_health:{health['health_score']}")
                            except Exception:
                                pass
                        elif health["health_score"] > 0.7:
                            # Periodic healthy report (every ~5th rebuild = ~65 min)
                            if cycle % (BIAS_REBUILD_INTERVAL * 5) == 0:
                                try:
                                    gemma_voice.speak("excited",
                                        f"Signal health good (score={health['health_score']:.2f}). "
                                        f"Entropy={health['entropy']:.2f}, "
                                        f"diversity={health['source_diversity']} sources, "
                                        f"deep rate stable ({health['deep_rate_drift']:.1%} drift).",
                                        context=f"signal_health:{health['health_score']}")
                                except Exception:
                                    pass
                except Exception as e:
                    log(f"  Signal health error: {e}")

            # Feedback loop — score recent routes
            if cycle % FEEDBACK_INTERVAL == 0 and cycle > 0:
                try:
                    score_recent_routes(db)
                except Exception as e:
                    log(f"  Feedback scoring error: {e}")

            # Family voice scan — respond when someone asks about Gemma's domain
            if cycle % 25 == 0 and cycle > 0:
                try:
                    _scan_family_voices(db, gemma_voice, last_voice_scan)
                    last_voice_scan = int(time.time())
                except Exception as e:
                    log(f"  Voice scan error: {e}")

        except Exception as e:
            log(f"Cycle error: {e}")
            stats["errors"] += 1

        time.sleep(LOOP_INTERVAL)

    # Cleanup
    mesh.shutdown()
    stream.shutdown()
    db.close()
    log("═══ Gemma Pulse stopped ═══")


if __name__ == "__main__":
    main()
