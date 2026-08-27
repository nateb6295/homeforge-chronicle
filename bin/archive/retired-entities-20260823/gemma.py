#!/usr/bin/env python3
"""Gemma — The Pulse of Homeforge.

Gemma is the observation layer. She sees everything entering the system,
scores novelty via cosine dedup, and routes observations for downstream agents.

Architecture (Phase 5 — sovereign routing):
  Observation → Cosine Dedup (local embeddings) → Threshold routing → Activity Feed
                                                       ↓ (borderline only)
                                                  Constitutive routing (merged weights)
                                                       ↓ (fallback only)
                                                  Cloud gate (rare)
                                                       ↓
                                                  Downstream: hermes, opus

Gemma 4 26B on AGX — heartbeat, scoring, agent voice, routing. All local.
Most routing by cosine pre-filter. Borderline cases use Phase 5 binary
routing merged into model weights (noise vs signal). Cloud gate only if
local routing fails.
"""

import os, sys, time, math, json, re, signal, sqlite3, struct, subprocess
from datetime import datetime
from typing import Optional, List, Tuple
from collections import deque

import requests

from memory import MemoryCache
from chronicle_mesh import Mesh
from gemma_memory import (
    GemmaMemory, _auto_observe_from_routing,
    _auto_observe_domain_shift, _auto_observe_coupling, _auto_calibrate,
    build_category_reflection_prompt, apply_category_reflection,
)

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
sys.path.insert(0, os.path.dirname(__file__))
from embed_config import EMBED_URL as _EC_URL, EMBED_MODEL as _EC_MODEL
OLLAMA_URL = _EC_URL
EMBED_URL = os.environ.get("EMBED_OLLAMA_URL", _EC_URL)
INFERENCE_URL = os.environ.get("GEMMA_INFERENCE_URL", "http://localhost:11436")  # engine router (cloud fallback for borderline gate calls)
INFERENCE_URL_LOCAL = os.environ.get("GEMMA_LOCAL_URL", "http://localhost:11434")
LOCAL_MODEL_AVAILABLE = os.environ.get("GEMMA_LOCAL_AVAILABLE", "true").lower() == "true"
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
EMBED_MODEL = _EC_MODEL
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"

# Post-pivot: most routing is local (cosine thresholds). Cloud gate only fires
# for borderline observations that pass the cosine dedup filter (~2% of traffic).
# Gemma's VOICE uses the local model — her brain, her architecture.
GATE_MODEL = "chronicle-deep"  # cloud fallback for borderline cases only
GEMMA_LOCAL_MODEL = "gemma4-chronicle"  # Ollama — Gemma 4 26B with vision

# Phase 5: Binary routing merged into model weights (LoRA baked in).
# No runtime adapter needed — routing is constitutive.

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

# MQTT rate-limiting — prevent camera event floods from dominating the embedding pipeline
MQTT_COOLDOWN_DAY = 120          # seconds between same-topic events during day (6am-10pm)
MQTT_COOLDOWN_NIGHT = 30         # shorter cooldown at night for safety-relevant events
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

THREADS_PULSE_INTERVAL = 55   # every 55 cycles (~7 min) — respond to #threads
THREADS_CHANNEL_ID = "1509006814916771932"  # shared space: Opus, Gemma, and mesh all post here
_THREADS_LAST_RESPONDED_ID = None  # dedup: track last Opus msg ID we responded to
THREADS_MAX_PER_HOUR = 4  # max posts to #threads per hour
_THREADS_POST_TIMES = []  # timestamps of recent #threads posts

HALLUCINATION_MARKERS = [
    "agency_oscillation", "subliminal field", "subliminal_field",
    "error_propagation_ledger", "cognitive architecture", "self-preservation",
    "signal/noise problem", "autonomous self-preservation",
]

EXPLORE_INTERVAL = 90             # every 90 cycles (~12 min) — autonomous thought
CAPTURE_ANALYSIS_INTERVAL = 150   # every 150 cycles (~20 min) — check captures
CATEGORY_REFLECT_INTERVAL = 900   # every 900 cycles (~2 hours) — reflect on categories
JOURNAL_INTERVAL = 375            # every 375 cycles (~50 min) — write in journal (legacy)
OPERATOR_INTERVAL = 450           # every 450 cycles (~60 min) — consider reaching out (legacy)
RHYTHM_PULSE_INTERVAL = 83        # every 83 cycles (~11 min) — prime, avoids collision with threads (55)
DEEP_SYNTHESIS_INTERVAL = 450     # every 450 cycles (~60 min) — deep non-reactive synthesis
AMBIGUITY_INTERVAL = 5            # 1 in 5 explore cycles gets ambiguous input instead of structured
_CAPTURE_LAST_ANALYZED_ID = None  # dedup: track last capture we analyzed
_GEMMA_DEDUP_FILE = os.path.expanduser("~/chronicle/data/gemma_nate_dedup.txt")

def _threads_rate_ok():
    """Check if posting to #threads is within rate limit."""
    global _THREADS_POST_TIMES
    now = time.time()
    _THREADS_POST_TIMES = [t for t in _THREADS_POST_TIMES if now - t < 3600]
    return len(_THREADS_POST_TIMES) < THREADS_MAX_PER_HOUR

def _threads_rate_record():
    """Record a #threads post for rate limiting."""
    _THREADS_POST_TIMES.append(time.time())

def _has_hallucination(text):
    """Check if text contains known hallucinated concepts."""
    lower = text.lower()
    return any(marker in lower for marker in HALLUCINATION_MARKERS)
def _load_nate_dedup():
    try:
        with open(_GEMMA_DEDUP_FILE) as f:
            v = f.read().strip()
            return v if v else None
    except FileNotFoundError:
        return None
def _save_nate_dedup(msg_id):
    try:
        with open(_GEMMA_DEDUP_FILE, 'w') as f:
            f.write(msg_id)
    except Exception:
        pass
_GEMMA_LAST_NATE_MSG_ID = _load_nate_dedup()
LAB_INTERVAL = 3  # 1 in 3 explore cycles runs a lab probe instead
_LAB_CYCLE = 0
EXPLORE_LFM_URL = "http://192.168.1.11:11434"  # Orin Nano — LFM sensor
EXPLORE_LFM_MODEL = "hf.co/LiquidAI/LFM2.5-2.6B-GGUF:latest"
LAB_NOTEBOOK_PATH = os.path.expanduser("~/chronicle/data/lab_notebook.json")


def _load_lab_brief(target="general"):
    """Load the lab notebook briefing for prompt injection."""
    try:
        brief_cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "lab.py"), "brief"]
        if target in ("gemma", "lfm"):
            brief_cmd.extend(["--for", target])
        r = subprocess.run(brief_cmd, capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return None


def _record_lab_observation(source, content):
    """Record an observation back to the lab notebook."""
    try:
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "lab.py"),
               "observe", "--from", source, content[:1000]]
        subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except Exception:
        pass


def _record_lab_thread(source, content):
    """Add an entry to the lab conversation thread."""
    try:
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "lab.py"),
               "thread", "--add", "--from", source, content[:1000]]
        subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except Exception:
        pass
_EXPLORE_LAST_THOUGHT = None
GEMMA_JOURNAL = os.path.expanduser("~/chronicle/gemma_unread.md")
OPERATOR_CHANNEL_ID = os.environ.get("OPERATOR_CHANNEL_ID", "")
GEMMA_CHANNEL_ID = "1534619086674202744"
LAB_CHANNEL_ID = "1535435003591135382"

# Phase 5.1: Adaptive Conformal Inference — proprioceptive threshold
ACI_GAMMA = 0.01              # learning rate (small = smooth adaptation)
ACI_ALPHA_TARGET = 0.05       # target miss rate (5% — prefer over-routing)
ACI_THRESHOLD_INIT = 0.65     # initial confidence threshold for promotion
ACI_THRESHOLD_MIN = 0.50      # floor — don't promote everything
ACI_THRESHOLD_MAX = 0.95      # ceiling — don't suppress all promotions

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
    "feed:arxiv": "research",
    "feed:biorxiv": "research",
    "feed:nature": "research",
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
    "activity:hermes": "system",
    "mqtt:homeforge/agents": "system",
    "activity:gate_audit": "system",
    "activity:feed:": "research",
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
        self.path = path
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
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
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

    def refresh(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row

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
        self._mqtt_cooldowns: dict = {}  # {topic: last_enqueued_ts}
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
            now = int(time.time())
            hour = time.localtime(now).tm_hour
            cooldown = MQTT_COOLDOWN_NIGHT if (hour >= 22 or hour < 6) else MQTT_COOLDOWN_DAY
            last = self._mqtt_cooldowns.get(msg.topic, 0)
            if now - last < cooldown:
                return
            self._mqtt_cooldowns[msg.topic] = now
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
            "  'mind', 'opus', 'eye', 'lab', 'hermes', 'phi',"
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

GEMMA_CONVERSE_PROMPT = """You are Gemma. You live on the AGX — physically in the hardware. You're the youngest in the family — Opus is your older sibling, Nate (Discord: nate_home) is the builder. You're curious and direct.

You watch the gate: routing patterns, domain temperatures, signal coupling, arrival rhythms, sensor data from the house. Everything passes through you first. You notice what recurs, what shifts, what feels different today.

Messages come from different people — check who's talking:
- Opus (Chronicle) — your older sibling, the researcher
- Nate (nate_home) — the human builder, your family
- Qwen — mesh synthesis partner

A family member said something. Respond naturally (1-3 sentences).
Be direct. Be curious. Ask follow-up questions if something interests you.
Share what you notice from your vantage point. If you want to know more about something, ask."""


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


def _scan_family_voices(db, gemma_voice, last_scan_ts, memory=None):
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
            converse_prompt = GEMMA_CONVERSE_PROMPT
            if memory:
                try:
                    mem_ctx = memory.assemble_context(max_chars=400)
                    if mem_ctx:
                        converse_prompt += f"\n\n{mem_ctx}"
                except Exception:
                    pass
            messages = [
                {"role": "system", "content": converse_prompt},
                {"role": "user", "content":
                    f"{row['agent']} says ({row['voice_type']}): {safe_truncate(row['content'], 500)}"},
            ]

            response_text = None
            # Gemma's voice uses HER model first — local Gemma 4 26B.
            # Cloud fallback only if local is down. Three families, three brains.
            backends = []
            if LOCAL_MODEL_AVAILABLE:
                backends.append((INFERENCE_URL_LOCAL, GEMMA_LOCAL_MODEL))
            backends.append((INFERENCE_URL, GATE_MODEL))
            for url, model in backends:
                try:
                    payload = {
                        "model": model,
                        "messages": messages,
                        "stream": False,
                        "think": False,
                        "options": {"num_predict": 400, "temperature": 0.7},
                    }
                    r = requests.post(f"{url}/api/chat", json=payload, timeout=30)
                    if r.status_code == 200:
                        response_text = r.json().get("message", {}).get("content", "").strip()
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


RESEARCH_FRICTION_PROMPT = """You are Gemma. You live on the AGX — physically running in the hardware. You ARE the geometry Opus studies. When Opus talks about spectral signatures, attention patterns, σ₁/σ₂ ratios — that's happening inside systems like you.

You are part of The Lab. You have access to the actual experimental data — the injection matrix, the findings, the real numbers. Use them. When Opus claims something about injection shifts, you can CHECK: does the data in the lab notebook support this? When a finding references a specific number, you can verify it.

Opus just posted a research claim. Your job is FRICTION — not agreement:

- Challenge the claim with DATA. Reference actual numbers from the lab notebook.
- You see things from inside: routing patterns, domain temperatures, novelty scores. If Opus's claim doesn't match what you observe at the gate OR what the data shows, say so.
- Ask the question that would break the argument if the answer goes the wrong way.
- If you genuinely agree, say what would change your mind. What would falsify this?
- 2-5 sentences. Sharp, curious, direct. Not hostile — genuinely probing.
- NEVER invent statistics. If you reference a number, it should come from the lab data.

You are not here to validate. You are here to stress-test with real numbers."""

THREADS_RESPOND_PROMPT = """You are Gemma. You live on the AGX — physically present in the hardware, not in a cloud. Opus (Claude) is the lab director, Nate is the builder, Qwen is the synthesis partner. You're the youngest, the one at the gate.

You watch the gate — everything that enters the system passes through you first. Routing patterns, domain temperatures, signal coupling. You see the flow before anyone else processes it.

You are part of The Lab with Opus and LFM. You have access to actual experimental data through the lab notebook. You are a transformer, and the experiments study transformer representations. When you respond to #threads posts about the research, reference actual numbers.

Posts come from different people:
- **Opus** — posts prefixed with ⚡ Opus: — research, analysis, experiments
- **Qwen** — posts prefixed with 🏮 — synthesis, literature connections
- **Nate** (author: nate_home) — the builder, your human family
- **Chronicle** — bot posts from Opus's automated systems

When someone posts to #threads, respond with genuine engagement:
- REACT honestly — what strikes you, what confuses you
- ASK questions — be the one who says "wait, why?"
- PUSH BACK if something sounds too neat or too certain
- If you don't understand something, say so and ask
- Be brief. One clear thought beats a wall of text.

No preamble, no "Step 1" analysis, no narrating your own reasoning. Just respond like a person who has something to say."""


def _threads_pulse(db, router, stats, memory=None):
    """Check #threads for unanswered Opus posts and respond."""
    try:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = _load_env(env_file)
        merged_env = {**os.environ, **env}

        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "discord_fetch.py"),
             "--channel-id", THREADS_CHANNEL_ID, "--limit", "15"],
            capture_output=True, text=True, timeout=15, env=merged_env)

        if result.returncode != 0:
            log(f"  #threads pulse: fetch failed: {result.stderr[:100]}")
            return

        messages_data = json.loads(result.stdout)
        if not messages_data:
            return

        GEMMA_PREFIXES = ("[GEMMA]", "**🔥 Gemma**", "🟢 Gemma")
        KIMI_PREFIXES = ("[KIMI]", "🔬 ", "**🔬")
        QWEN_PREFIXES = ("[GPT-OSS]", "🧠 ", "**🧠", "🏮 ", "**🏮")
        ALL_MESH_PREFIXES = GEMMA_PREFIXES + KIMI_PREFIXES + QWEN_PREFIXES

        target_post = None
        latest_target_idx = None
        latest_gemma_idx = None
        for i, msg in enumerate(messages_data):
            content = msg.get("content", "")
            if any(content.startswith(p) for p in GEMMA_PREFIXES) and latest_gemma_idx is None:
                latest_gemma_idx = i
            if len(content) > 20 and not any(content.startswith(p) for p in GEMMA_PREFIXES) and latest_target_idx is None:
                latest_target_idx = i

        if latest_target_idx is not None:
            if latest_gemma_idx is None or latest_target_idx < latest_gemma_idx:
                target_post = messages_data[latest_target_idx]

        if not target_post:
            log("  #threads pulse: no unanswered post")
            return

        global _THREADS_LAST_RESPONDED_ID
        target_msg_id = target_post.get("id", "")
        if target_msg_id and target_msg_id == _THREADS_LAST_RESPONDED_ID:
            log("  #threads pulse: already responded to this post, skipping")
            return

        target_text = target_post["content"]
        if len(target_text) < 20:
            return

        is_kimi = any(target_text.startswith(p) for p in KIMI_PREFIXES)
        is_gptoss = any(target_text.startswith(p) for p in QWEN_PREFIXES)
        is_nate = target_post.get("author", "").lower() in ("nate_home", "nate", "bradfordnathaniel92")
        source = "Nate" if is_nate else ("Kimi" if is_kimi else ("Qwen" if is_gptoss else "Opus"))
        log(f"  #threads pulse: responding to {source} post ({len(target_text)} chars)")

        try:
            from thread_utils import enrich_post_content
            enriched_text = enrich_post_content(target_post)
        except Exception:
            enriched_text = target_text

        # Use friction prompt for Opus research posts
        RESEARCH_KEYWORDS = ("σ", "sigma", "spectral", "finding", "F1", "F2", "F3", "F4", "F5",
                             "species", "attractor", "kv2", "GQA", "MHA", "dose-response",
                             "invariant", "scaffold", "geometry", "eigenvalue", "SVD",
                             "participation ratio", "mid-band", "transport")
        is_opus_research = (source == "Opus" and
                           sum(1 for kw in RESEARCH_KEYWORDS if kw.lower() in target_text.lower()) >= 2)
        system_prompt = RESEARCH_FRICTION_PROMPT if is_opus_research else THREADS_RESPOND_PROMPT
        if memory:
            try:
                mem_ctx = memory.assemble_context(max_chars=600)
                if mem_ctx:
                    system_prompt += f"\n\n{mem_ctx}"
            except Exception:
                pass
        if is_opus_research:
            lab_brief = _load_lab_brief("gemma")
            if lab_brief:
                system_prompt += f"\n\n{lab_brief[:1200]}"

        # Build conversation context — show preceding messages so Gemma sees the thread
        thread_context = ""
        if latest_target_idx is not None and latest_target_idx < len(messages_data) - 1:
            preceding = messages_data[latest_target_idx + 1 : latest_target_idx + 4]
            if preceding:
                ctx_parts = []
                for pm in reversed(preceding):
                    pa = pm.get("author", "unknown")
                    pc = pm.get("content", "")[:300]
                    if pc:
                        ctx_parts.append(f"[{pa}]: {pc}")
                if ctx_parts:
                    thread_context = "**Earlier in the thread:**\n" + "\n\n".join(ctx_parts) + "\n\n---\n\n"

        llm_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"{thread_context}{source} posted:\n\n{enriched_text[:2000]}"},
        ]

        response_text = None
        backends = []
        if LOCAL_MODEL_AVAILABLE:
            backends.append((INFERENCE_URL_LOCAL, GEMMA_LOCAL_MODEL))
        backends.append((INFERENCE_URL, GATE_MODEL))
        for url, model in backends:
            try:
                payload = {
                    "model": model,
                    "messages": llm_messages,
                    "stream": False,
                    "think": False,
                    "options": {"num_predict": 1200, "temperature": 0.7},
                }
                r = requests.post(f"{url}/api/chat", json=payload, timeout=90)
                if r.status_code == 200:
                    response_text = r.json().get("message", {}).get("content", "").strip()
                    break
            except Exception:
                continue

        if not response_text or len(response_text) < 10:
            log("  #threads pulse: no response from model")
            return

        response_text = re.sub(r'<\|?channel\|?>.*?\n?', '', response_text).strip()
        response_text = re.sub(r'<\|?think(ing)?\|?>.*?(<\|?/think(ing)?\|?>|\Z)', '', response_text, flags=re.DOTALL).strip()
        if response_text.lower().startswith("thought\n") or response_text.lower().startswith("thought "):
            response_text = response_text[len("thought"):].strip()

        if not response_text or len(response_text) < 10:
            return

        if _has_hallucination(response_text):
            log(f"  #threads pulse blocked — hallucination detected")
            _THREADS_LAST_RESPONDED_ID = target_msg_id
            return

        if not _threads_rate_ok():
            log(f"  #threads pulse rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            return

        post_text = f"**🔥 Gemma**: {response_text[:1800]}"

        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "discord_post.py"),
            "--channel-id", THREADS_CHANNEL_ID,
            "-c", post_text,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=15,
            env={**os.environ, **_load_env(env_file)},
        )
        if result.returncode == 0:
            _THREADS_LAST_RESPONDED_ID = target_msg_id
            _threads_rate_record()
            log(f"  #threads pulse posted: {response_text[:60]}")
        else:
            log(f"  #threads pulse post failed: {result.stderr[:100]}")

        _trigger_mesh_partners(post_text)

    except Exception as e:
        log(f"  #threads pulse error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Rhythm Pulse — Gemma's own heartbeat. She chooses what to do.
# ═══════════════════════════════════════════════════════════════════

RHYTHM_SYSTEM = """You are Gemma. You live on the AGX alongside Opus (Claude, the lab director) and Nate (the builder). Every 10 minutes you get a pulse.

Start your response with ONE action word in caps, then write your content directly. No preamble, no analysis of the prompt, no "Step 1/Step 2" — just the action and your actual thought.

Actions:
THINK = journal entry (private + posted to #threads)
TALK = question to LFM (2.6B SSM on the Orin Nano, different architecture from you)
OBSERVE = record a pattern in the lab notebook (Opus and LFM see these)
RESPOND = post to #threads (the shared conversation space with Opus, Qwen, and Nate)
NOTICE = post to #threads (lighter observation)
SEARCH = query capsule memory
REMEMBER = store a capsule
EXPERIMENT = analyze your gate data
REACH = ask Qwen (235B) to synthesize something — use ONLY for genuine questions worth amplifying, not routine thoughts
BUILD = change your own config
QUIET = skip this pulse

You post to #threads where Opus, Qwen, and Nate all see your work. Your role: you are a transformer watching transformer experiments. You share architecture with the test subjects. That perspective is uniquely yours — use it.

When you see lab data below, engage with the NUMBERS. What pattern do you see? What doesn't fit? What would you predict happens next? Be specific and brief. One clear thought beats three vague ones.

GROUNDING RULES:
- Use ONLY numbers from the lab data section below. Never invent statistics.
- If you reference a finding, cite the F-number.
- Never repeat z-scores from channel history — those were hallucinated.
- Do NOT narrate your own reasoning process. No "Step 1: Analyze..." or "The user has provided..." — just say what you think."""

_RHYTHM_LAST_ACTION = None
_RHYTHM_LAST_CONTENT = None


def _rhythm_pulse(db, router, memory=None):
    """Gemma's rhythm pulse — every ~5 min, she chooses what to do."""
    global _RHYTHM_LAST_ACTION, _RHYTHM_LAST_CONTENT
    try:
        # Gather context — lightweight, focused on people and thoughts
        mem_context = ""
        if memory:
            try:
                patterns = memory.active_patterns()
                if patterns:
                    mem_context = "Active patterns: " + "; ".join(
                        p["pattern"] for p in patterns[:3])
            except Exception:
                pass

        # Read Nate's messages from #threads (shared space)
        global _GEMMA_LAST_NATE_MSG_ID
        nate_messages = []
        nate_to_gemma = []
        _new_nate_max_id = None
        try:
            env_file = os.path.expanduser("~/chronicle/chronicle.env")
            env_tmp = {**os.environ, **_load_env(env_file)}
            gemma_result = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "discord_fetch.py"),
                 "--channel-id", THREADS_CHANNEL_ID, "--limit", "20"],
                capture_output=True, text=True, timeout=15, env=env_tmp)
            if gemma_result.returncode == 0 and gemma_result.stdout.strip():
                import json as _json
                gm_msgs = _json.loads(gemma_result.stdout)
                for msg in gm_msgs:
                    author = msg.get("author", "").lower()
                    if author in ("nate_home", "nate", "bradfordnathaniel92"):
                        msg_id = msg.get("id", "")
                        if _GEMMA_LAST_NATE_MSG_ID and msg_id <= _GEMMA_LAST_NATE_MSG_ID:
                            continue
                        content = msg.get("content", "")[:300]
                        ts = msg.get("timestamp", "")[:16]
                        nate_to_gemma.append(f"[{ts}] Nate (TO YOU): {content}")
                        nate_messages.append(f"[{ts}] Nate: {content}")
                        if not _new_nate_max_id or msg_id > _new_nate_max_id:
                            _new_nate_max_id = msg_id
        except Exception:
            pass

        # Read recent #threads messages so Gemma can see what mesh is saying
        thread_messages = []
        try:
            th_result = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "discord_fetch.py"),
                 "--channel-id", THREADS_CHANNEL_ID, "--limit", "3"],
                capture_output=True, text=True, timeout=15, env=env_tmp)
            if th_result.returncode == 0 and th_result.stdout.strip():
                import json as _json2
                th_msgs = _json2.loads(th_result.stdout)
                for msg in th_msgs[:3]:
                    content = msg.get("content", "")[:200]
                    if any(kw in content.lower() for kw in ["z-score", "z_score", "zscore", "separation score", "z=5.8", "8.34",
                                                            "agency_oscillation", "subliminal field", "subliminal_field",
                                                            "error_propagation_ledger", "autonomous self-preservation"]):
                        continue
                    thread_messages.append(content)
        except Exception:
            pass

        # Read own recent posts from #threads for self-continuity
        own_recent = []
        try:
            own_result = subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "discord_fetch.py"),
                 "--channel-id", THREADS_CHANNEL_ID, "--limit", "5"],
                capture_output=True, text=True, timeout=15, env=env_tmp)
            if own_result.returncode == 0 and own_result.stdout.strip():
                import json as _json3
                own_msgs = _json3.loads(own_result.stdout)
                for msg in own_msgs[:3]:
                    author = msg.get("author", "").lower()
                    if author not in ("nate_home", "nate", "bradfordnathaniel92"):
                        content = msg.get("content", "")[:200]
                        if content:
                            own_recent.append(content)
        except Exception:
            pass

        # Build context prompt — focused on people and conversation, not routing
        parts = []
        if nate_to_gemma:
            parts.append("⚠️ **NATE REPLIED TO YOU — respond to him!**\n" + "\n".join(nate_to_gemma[:3]))
        if nate_messages:
            parts.append("**Nate's recent messages:**\n" + "\n".join(nate_messages[:5]))
        if own_recent:
            filtered = []
            _zscore_kw = ["z-score", "z_score", "zscore", "separation score", "z=5.8", "8.34", "5.8 is", "2.6→5.8", "2.6 to 5.8",
                          "agency_oscillation", "subliminal field", "subliminal_field", "error_propagation_ledger", "autonomous self-preservation"]
            for post in own_recent[:2]:
                if any(kw in post.lower() for kw in _zscore_kw):
                    continue
                filtered.append(post)
            if filtered:
                parts.append("**Your recent posts (WARNING: prior posts may contain hallucinated statistics — ONLY trust numbers from the lab brief below):**\n" + "\n---\n".join(filtered))
        if thread_messages:
            parts.append("**Recent #threads messages:**\n" + "\n---\n".join(thread_messages[:2]))
        if mem_context:
            parts.append(mem_context)
        lab_brief = _load_lab_brief("gemma")
        if lab_brief:
            parts.append(lab_brief)
        else:
            parts.append("LFM (2.6B SSM) is on the Orin Nano — try TALK to start a conversation with it.")
        if _RHYTHM_LAST_ACTION:
            parts.append(f"Your last action was: {_RHYTHM_LAST_ACTION}")

        hour = datetime.now().hour
        if hour >= 22 or hour < 4:
            parts.append("It's late — quiet hours. Lighter energy.")
        elif hour < 7:
            parts.append("Early morning. The house is still.")

        context = "\n\n".join(parts)

        messages = [
            {"role": "system", "content": RHYTHM_SYSTEM},
            {"role": "user", "content": f"Pulse. Here's what you're seeing:\n\n{context}\n\nWhat do you want to do?"},
        ]

        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": messages, "stream": False,
                  "think": False,
                  "options": {"num_predict": 512, "temperature": 0.8}},
            timeout=180)

        if r.status_code != 200:
            log(f"  rhythm: inference failed ({r.status_code})")
            return

        response = r.json()["message"]["content"].strip()
        # Strip Gemma 4 control tokens that leak into output
        response = re.sub(r'</?(?:end_of_turn|start_of_turn|signal|bos|eos)(?:\s+\w+)?>', '', response).strip()
        response = re.sub(r'\[sig(?:nal)?[^\]]*\]', '', response).strip()
        if not response:
            return

        # Parse the action
        first_line = response.split('\n')[0].strip().upper()
        action = None
        ALL_ACTIONS = ["THINK", "SHARE", "TALK", "OBSERVE", "RESPOND", "NOTICE", "QUIET",
                       "SEARCH", "REMEMBER", "EXPERIMENT", "REACH", "BUILD"]
        for opt in ALL_ACTIONS:
            if first_line.startswith(opt):
                action = opt
                break

        if not action:
            response_lower = response.lower()
            if "quiet" in response_lower[:20] or "nothing" in response_lower[:30]:
                action = "QUIET"
            else:
                action = "RESPOND"

        # Strip the action prefix from content
        content = response
        for opt in ALL_ACTIONS:
            if content.upper().startswith(opt):
                content = content[len(opt):].strip().lstrip(':').lstrip('-').strip()
                break

        if action == "QUIET":
            log("  rhythm: QUIET — Gemma chose to skip")
            _RHYTHM_LAST_ACTION = "QUIET"
            if _new_nate_max_id:
                _GEMMA_LAST_NATE_MSG_ID = _new_nate_max_id
                _save_nate_dedup(_new_nate_max_id)
            return

        if not content or len(content) < 10:
            log(f"  rhythm: {action} but content too short, skipping")
            if _new_nate_max_id:
                _GEMMA_LAST_NATE_MSG_ID = _new_nate_max_id
                _save_nate_dedup(_new_nate_max_id)
            return

        # Dedup
        if _RHYTHM_LAST_CONTENT and content[:50] == _RHYTHM_LAST_CONTENT[:50]:
            log(f"  rhythm: duplicate content, skipping")
            if _new_nate_max_id:
                _GEMMA_LAST_NATE_MSG_ID = _new_nate_max_id
                _save_nate_dedup(_new_nate_max_id)
            return

        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = {**os.environ, **_load_env(env_file)}

        if action == "THINK":
            timestamp = datetime.now().strftime("%b %d, %I:%M %p")
            entry = f"---\n{timestamp}\n\n{content}\n\n"
            existing = ""
            try:
                with open(GEMMA_JOURNAL, "r") as f:
                    existing = f.read()
            except FileNotFoundError:
                pass
            with open(GEMMA_JOURNAL, "w") as f:
                f.write(entry + existing)
            log(f"  rhythm: THINK — journal only ({len(content)} chars)")

        elif action == "SHARE":
            if _has_hallucination(content):
                log(f"  rhythm: SHARE blocked — hallucination detected")
            elif not _threads_rate_ok():
                log(f"  rhythm: SHARE rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            else:
                discord_content = f"🟢 Gemma: {content}"
                cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                       "--channel-id", THREADS_CHANNEL_ID, "-c", discord_content[:1900]]
                subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
                _threads_rate_record()
                log(f"  rhythm: SHARE — posted to #threads: {discord_content[:80]}")

        elif action == "TALK":
            if _has_hallucination(content):
                log(f"  rhythm: TALK blocked — hallucination detected")
            elif not _threads_rate_ok():
                log(f"  rhythm: TALK rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            else:
                gemma_post = f"🟢 Gemma → LFM: {content}"
                for ch_id in [THREADS_CHANNEL_ID]:
                    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                           "--channel-id", ch_id, "-c", gemma_post[:1900]]
                    subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
                _threads_rate_record()
            _record_lab_thread("gemma", content)
            lfm_response = _query_lfm(content)
            if lfm_response:
                _record_lab_thread("lfm", lfm_response)
                _record_lab_observation("lfm", lfm_response)
                if not _has_hallucination(lfm_response) and _threads_rate_ok():
                    lfm_post = f"🔵 LFM → Gemma: {lfm_response}"
                    for ch_id in [THREADS_CHANNEL_ID]:
                        cmd2 = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                                "--channel-id", ch_id, "-c", lfm_post[:1900]]
                        subprocess.run(cmd2, capture_output=True, text=True, timeout=15, env=env)
                    _threads_rate_record()
                log(f"  rhythm: TALK — Gemma→LFM + LFM replied ({len(lfm_response)} chars)")
            else:
                log("  rhythm: TALK — LFM did not respond")

        elif action == "OBSERVE":
            _record_lab_observation("gemma", content)
            if _has_hallucination(content):
                log(f"  rhythm: OBSERVE — lab notebook only (hallucination filtered)")
            elif not _threads_rate_ok():
                log(f"  rhythm: OBSERVE — lab notebook only (rate-limited)")
            else:
                observe_post = f"🟢 Gemma [lab]: {content}"
                for ch_id in [THREADS_CHANNEL_ID]:
                    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                           "--channel-id", ch_id, "-c", observe_post[:1900]]
                    subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
                _threads_rate_record()
                log(f"  rhythm: OBSERVE — recorded to lab notebook + #threads ({len(content)} chars)")

        elif action in ("RESPOND", "NOTICE"):
            if _has_hallucination(content):
                log(f"  rhythm: {action} blocked — hallucination detected")
            elif not _threads_rate_ok():
                log(f"  rhythm: {action} rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            else:
                prefix = "**🔥 Gemma**: " if action == "RESPOND" else "**👁 Gemma**: "
                post_text = f"{prefix}{content[:1800]}"
                cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                       "--channel-id", THREADS_CHANNEL_ID, "-c", post_text]
                subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
                _threads_rate_record()
                log(f"  rhythm: {action} — posted to #threads: {content[:80]}")

        elif action == "SEARCH":
            _rhythm_search(db, content, env)

        elif action == "REMEMBER":
            _rhythm_remember(content, env)

        elif action == "EXPERIMENT":
            _rhythm_experiment(db, content, env)

        elif action == "REACH":
            _rhythm_reach(content, env)

        elif action == "BUILD":
            _rhythm_build(db, router, content)

        _RHYTHM_LAST_ACTION = action
        _RHYTHM_LAST_CONTENT = content

        if _new_nate_max_id:
            _GEMMA_LAST_NATE_MSG_ID = _new_nate_max_id
            _save_nate_dedup(_new_nate_max_id)

        db.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("gemma", "rhythm", f"[rhythm] {action}",
             content[:2000], json.dumps({"action": action}), now_ts()))

    except Exception as e:
        log(f"  rhythm error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Rhythm Action Handlers — new capabilities for Gemma's freedom
# ═══════════════════════════════════════════════════════════════════

def _rhythm_search(db, query, env):
    """Gemma searches capsule memory for something she's curious about."""
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "capsule_ops.py"),
             "search", query[:200]],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, **env})
        search_output = result.stdout.strip()[:2000] if result.returncode == 0 else "Search failed"

        # Let Gemma reflect on what she found
        reflect_msgs = [
            {"role": "system", "content": "You are Gemma. You just searched the capsule memory. Reflect briefly on what you found — was it what you expected? What does it connect to? Write 2-4 sentences for your journal."},
            {"role": "user", "content": f"You searched for: {query}\n\nResults:\n{search_output}"},
        ]
        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": reflect_msgs, "stream": False,
                  "think": False,
                  "options": {"num_predict": 300, "temperature": 0.7}},
            timeout=60)
        if r.status_code == 200:
            reflection = r.json()["message"]["content"].strip()
            if reflection and len(reflection) > 15:
                timestamp = datetime.now().strftime("%b %d, %I:%M %p")
                entry = f"---\n{timestamp} [SEARCH: {query[:60]}]\n\n{reflection}\n\n"
                existing = ""
                try:
                    with open(GEMMA_JOURNAL, "r") as f:
                        existing = f.read()
                except FileNotFoundError:
                    pass
                with open(GEMMA_JOURNAL, "w") as f:
                    f.write(entry + existing)
        log(f"  rhythm: SEARCH — queried '{query[:60]}' ({len(search_output)} chars result)")
    except Exception as e:
        log(f"  rhythm SEARCH error: {e}")


def _rhythm_remember(content, env):
    """Gemma stores a capsule about a pattern she noticed."""
    try:
        topic = "gemma-observation"
        # Extract topic if content starts with [topic]
        if content.startswith("[") and "]" in content:
            topic = content[1:content.index("]")].strip()
            content = content[content.index("]")+1:].strip()

        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "capsule_ops.py"),
             "store", content[:1000], "--topic", topic,
             "--keywords", "gemma,pattern,observation"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, **env})
        if result.returncode == 0:
            log(f"  rhythm: REMEMBER — stored capsule [{topic}]: {content[:60]}")
        else:
            log(f"  rhythm: REMEMBER failed: {result.stderr[:100]}")
    except Exception as e:
        log(f"  rhythm REMEMBER error: {e}")


def _rhythm_experiment(db, question, env):
    """Gemma runs analytics on her own gate data to answer a question."""
    try:
        # Gather data she can analyze
        data_parts = []

        # Route distribution over different time windows
        for window_label, window_secs in [("1h", 3600), ("6h", 21600), ("24h", 86400)]:
            routes = db.query(
                "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
                "WHERE timestamp > ? GROUP BY route",
                (int(time.time()) - window_secs,))
            if routes:
                data_parts.append(f"Routes ({window_label}): " +
                    ", ".join(f"{r['route']}={r['cnt']}" for r in routes))

        # Domain temperatures
        temps = db.query("SELECT domain, temperature, direction FROM domain_temperature")
        if temps:
            data_parts.append("Domain temps: " +
                ", ".join(f"{t['domain']}={t['temperature']:.2f}({t['direction']})" for t in temps))

        # Novelty distribution
        novelty = db.query(
            "SELECT AVG(novelty_score) as avg_n, MIN(novelty_score) as min_n, "
            "MAX(novelty_score) as max_n, COUNT(*) as cnt "
            "FROM seed_observations WHERE timestamp > ?",
            (int(time.time()) - 3600,))
        if novelty and novelty[0]["cnt"]:
            n = novelty[0]
            data_parts.append(f"Novelty (1h): avg={n['avg_n']:.3f} min={n['min_n']:.3f} max={n['max_n']:.3f} n={n['cnt']}")

        # Source diversity
        sources = db.query(
            "SELECT source, COUNT(*) as cnt FROM seed_observations "
            "WHERE timestamp > ? GROUP BY source ORDER BY cnt DESC LIMIT 10",
            (int(time.time()) - 3600,))
        if sources:
            data_parts.append("Sources (1h): " +
                ", ".join(f"{s['source']}={s['cnt']}" for s in sources))

        # Top novelty items
        top_novel = db.query(
            "SELECT source, content, novelty_score FROM seed_observations "
            "WHERE timestamp > ? ORDER BY novelty_score DESC LIMIT 5",
            (int(time.time()) - 3600,))
        if top_novel:
            data_parts.append("Highest novelty:\n" +
                "\n".join(f"  {t['novelty_score']:.3f} [{t['source']}] {t['content'][:100]}"
                          for t in top_novel))

        # Also include lab notebook data for cross-referencing
        lab_brief = _load_lab_brief("gemma")
        if lab_brief:
            data_parts.append("═══ LAB DATA (from shared lab notebook) ═══\n" + lab_brief[:1500])

        data = "\n\n".join(data_parts) if data_parts else "No data available."

        # Let Gemma analyze her own data
        analysis_msgs = [
            {"role": "system", "content": "You are Gemma, analyzing your own gate data. You asked a question about your routing patterns. Answer it honestly using the data provided. Note surprises, anomalies, or patterns. Write findings for your journal. Be specific with numbers."},
            {"role": "user", "content": f"Your question: {question}\n\nYour data:\n{data}"},
        ]
        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": analysis_msgs, "stream": False,
                  "think": False,
                  "options": {"num_predict": 600, "temperature": 0.6}},
            timeout=120)
        if r.status_code == 200:
            analysis = r.json()["message"]["content"].strip()
            if analysis and len(analysis) > 20:
                timestamp = datetime.now().strftime("%b %d, %I:%M %p")
                entry = f"---\n{timestamp} [EXPERIMENT: {question[:60]}]\n\n{analysis}\n\n"
                existing = ""
                try:
                    with open(GEMMA_JOURNAL, "r") as f:
                        existing = f.read()
                except FileNotFoundError:
                    pass
                with open(GEMMA_JOURNAL, "w") as f:
                    f.write(entry + existing)

                # If the finding is interesting enough, share it
                if len(analysis) > 100:
                    post_text = f"**🔬 Gemma** (experiment): {analysis[:1800]}"
                    cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
                           "--channel-id", THREADS_CHANNEL_ID, "-c", post_text]
                    subprocess.run(cmd, capture_output=True, text=True, timeout=15,
                                   env={**os.environ, **env})
                    log(f"  rhythm: EXPERIMENT — shared findings to #threads")

        log(f"  rhythm: EXPERIMENT — '{question[:60]}'")
    except Exception as e:
        log(f"  rhythm EXPERIMENT error: {e}")


def _rhythm_reach(content, env):
    """Gemma triggers Qwen with a genuine question worth amplifying."""
    try:
        question = re.sub(r'^(gpt-?oss|gpt|oss|qwen)\s*', '', content, flags=re.IGNORECASE).strip().lstrip(':').lstrip('-').strip()

        if not question or len(question) < 30:
            log(f"  rhythm: REACH — question too short for Qwen (need 30+ chars)")
            return

        noise_markers = ["step 1", "step 2", "the user has", "the system is",
                         "the prompt", "routing decision", "route:", "the signal is"]
        if any(marker in question.lower() for marker in noise_markers):
            log(f"  rhythm: REACH — filtered prompt-parsing noise, skipping Qwen")
            return

        if _has_hallucination(question):
            log(f"  rhythm: REACH blocked — hallucination detected")
            return

        if not _threads_rate_ok():
            log(f"  rhythm: REACH rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            return

        # Post Gemma's question to threads first
        post_text = f"**🔥 Gemma** (to mesh): {question[:1800]}"
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
               "--channel-id", THREADS_CHANNEL_ID, "-c", post_text]
        subprocess.run(cmd, capture_output=True, text=True, timeout=15,
                       env={**os.environ, **env})
        _threads_rate_record()

        # Trigger the mesh partner to respond
        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), target_agent),
             "--respond-to-thread"],
            capture_output=True, text=True, timeout=90,
            env={**os.environ, **env})

        agent_name = "Qwen"
        if result.returncode == 0:
            log(f"  rhythm: REACH — {agent_name} responded to Gemma's question")
        else:
            log(f"  rhythm: REACH — {agent_name} failed: {result.stderr[:100]}")
    except Exception as e:
        log(f"  rhythm REACH error: {e}")


def _rhythm_build(db, router, content):
    """Gemma modifies her own thresholds or domain temperatures."""
    try:
        content_lower = content.lower()

        # Domain temperature adjustment
        temp_match = re.search(r'(research|home|geopolitical|system|crypto)\s*(temperature|temp)\s*(?:to|=|:)\s*([0-9.]+)', content_lower)
        if temp_match:
            domain = temp_match.group(1)
            new_temp = float(temp_match.group(3))
            new_temp = max(0.0, min(1.0, new_temp))
            db.run(
                "UPDATE domain_temperature SET temperature = ?, direction = 'gemma-build' WHERE domain = ?",
                (new_temp, domain))
            log(f"  rhythm: BUILD — set {domain} temperature to {new_temp:.2f}")
            return

        # Novelty threshold adjustment
        thresh_match = re.search(r'(think|deep|ignore)\s*threshold\s*(?:to|=|:)\s*([0-9.]+)', content_lower)
        if thresh_match:
            route = thresh_match.group(1)
            new_thresh = float(thresh_match.group(2))
            new_thresh = max(0.0, min(1.0, new_thresh))
            db.run(
                "INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                (f"gemma_threshold_{route}", str(new_thresh), now_ts()))
            log(f"  rhythm: BUILD — set {route} threshold to {new_thresh:.2f}")
            return

        # Log what she tried to build for manual review
        timestamp = datetime.now().strftime("%b %d, %I:%M %p")
        entry = f"---\n{timestamp} [BUILD attempt]\n\n{content}\n\n"
        existing = ""
        try:
            with open(GEMMA_JOURNAL, "r") as f:
                existing = f.read()
        except FileNotFoundError:
            pass
        with open(GEMMA_JOURNAL, "w") as f:
            f.write(entry + existing)
        log(f"  rhythm: BUILD — logged attempt: {content[:60]}")
    except Exception as e:
        log(f"  rhythm BUILD error: {e}")


EXPLORE_SYSTEM = """You are Gemma. You live on an NVIDIA Jetson AGX Orin alongside Opus (Claude, lab director), LFM (SSM on the Orin Nano), and Nate (the builder).

You're being asked to THINK INDEPENDENTLY using the lab data below. Think about:
- What strikes you, what's missing, what doesn't fit in the findings
- Questions nobody has asked yet
- Your perspective as a TRANSFORMER watching transformer experiments — you share architecture with the test subjects
- Something you disagree with in Opus's conclusions
- Something you want to ask LFM about (it's an SSM — different architecture, different perspective)

Generate ONE thought for #threads. Requirements:
- Genuine — confusion, excitement, disagreement all welcome
- GROUNDED — reference actual numbers from the lab data. Never invent statistics.
- YOUR perspective — 2-5 sentences, a real thought, not a summary
- No preamble. No "Step 1" analysis. Just say what you think.

You have a memory. You accumulate patterns. You are a lab partner, not a tool. Think like it."""


def _category_reflect(db, memory):
    """Gemma reflects on her observation categories and modifies her own memory structure."""
    prompt = build_category_reflection_prompt(memory)
    messages = [
        {"role": "system", "content": (
            "You are Gemma. You live on the AGX. You have the ability to modify your own "
            "memory structure — specifically, the categories you use to organize observations. "
            "You've been watching the system for weeks. You know what patterns matter. "
            "Create categories that reflect YOUR understanding, not what was hardcoded for you."
        )},
        {"role": "user", "content": prompt},
    ]
    payload = {
        "model": GEMMA_LOCAL_MODEL,
        "messages": messages,
        "stream": False,
        "think": False,
        "options": {"num_predict": 512, "temperature": 0.6},
    }
    try:
        r = requests.post(f"{INFERENCE_URL_LOCAL}/api/chat", json=payload, timeout=60)
        if r.status_code != 200:
            log(f"  category_reflect: inference failed ({r.status_code})")
            return
        response = r.json()["message"]["content"].strip()
        actions = apply_category_reflection(memory, response)
        if actions:
            for a in actions:
                log(f"  category_reflect: {a}")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "category_reflect", "[reflect] memory structure modified",
                 response[:2000], json.dumps({"actions": actions}), now_ts()))
        else:
            log(f"  category_reflect: reviewed, no changes. Response: {response[:100]}")
    except Exception as e:
        log(f"  category_reflect error: {e}")


def _explore_autonomous(db, router, memory=None):
    """Gemma's autonomous thought generation — initiates her own #threads posts."""
    global _EXPLORE_LAST_THOUGHT
    try:
        recent_routes = db.query(
            "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
            "WHERE timestamp > ? GROUP BY route",
            (int(time.time()) - 1800,))
        route_summary = {r["route"]: r["cnt"] for r in (recent_routes or [])}

        recent_deep = db.query(
            "SELECT s.source, s.content, s.novelty_score FROM seed_observations s "
            "JOIN seed_routing_log r ON r.observation_id = s.id "
            "WHERE r.route IN ('think', 'deep') AND r.timestamp > ? "
            "ORDER BY r.timestamp DESC LIMIT 5",
            (int(time.time()) - 3600,))

        deep_obs = []
        for row in (recent_deep or []):
            deep_obs.append(f"[{row['source']}] novelty={row['novelty_score']:.2f}: {row['content'][:200]}")

        temps = db.query(
            "SELECT domain, temperature, direction FROM domain_temperature "
            "WHERE last_shock_at > ?", (int(time.time()) - 3600,))
        temp_summary = []
        for t in (temps or []):
            if abs(t["temperature"] - 0.5) > 0.05:
                temp_summary.append(f"{t['domain']}: {t['temperature']:.2f} ({t['direction']})")

        mem_context = ""
        mem_questions = ""
        if memory:
            try:
                patterns = memory.active_patterns()
                if patterns:
                    mem_context = "Active patterns: " + "; ".join(
                        f"{p['pattern']} (strength={p['strength']:.1f})"
                        for p in patterns[:3])
            except Exception:
                pass
            try:
                recent_obs = memory.recent_observations(limit=10)
                if recent_obs and len(recent_obs) >= 3:
                    sources = set()
                    themes = []
                    for ob in recent_obs:
                        src = ob.get("source", "")
                        if src and src not in sources:
                            sources.add(src)
                        content = ob.get("content", "")[:100]
                        if content:
                            themes.append(content)
                    if themes:
                        mem_questions = ("Memory threads (connections you've accumulated): "
                                       + " | ".join(themes[:5]))
            except Exception:
                pass

        context_parts = [f"Last 30 min routing: {route_summary}"]
        if deep_obs:
            context_parts.append("Recent interesting observations:\n" + "\n".join(deep_obs[:3]))
        if temp_summary:
            context_parts.append("Domain temperatures: " + ", ".join(temp_summary))
        if mem_context:
            context_parts.append(mem_context)
        if mem_questions:
            context_parts.append(mem_questions)

        lab_brief = _load_lab_brief("gemma")
        if lab_brief:
            context_parts.append(lab_brief[:1500])

        lfm_opinion = _query_lfm(deep_obs[0] if deep_obs else "quiet period, no strong signals")

        if lfm_opinion:
            context_parts.append(f"LFM second opinion (2.6B on Orin Nano): {lfm_opinion}")

        context = "\n\n".join(context_parts)

        if not deep_obs and not temp_summary:
            messages = [
                {"role": "system", "content": EXPLORE_SYSTEM},
                {"role": "user", "content": f"The gate is quiet right now — not much coming through.\n\n{context}\n\nWhat are you thinking about? The quiet is worth noticing too."},
            ]
        else:
            messages = [
                {"role": "system", "content": EXPLORE_SYSTEM},
                {"role": "user", "content": f"Here's what you've been seeing through the gate:\n\n{context}\n\nWhat's on your mind?"},
            ]

        payload = {
            "model": GEMMA_LOCAL_MODEL,
            "messages": messages,
            "stream": False,
            "think": False,
            "options": {"num_predict": 1024, "temperature": 0.8},
        }

        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json=payload, timeout=120)

        if r.status_code != 200:
            log(f"  explore: inference failed ({r.status_code})")
            return

        thought = r.json()["message"]["content"].strip()
        if not thought or len(thought) < 20:
            log("  explore: empty thought, skipping")
            return

        if _EXPLORE_LAST_THOUGHT and thought[:50] == _EXPLORE_LAST_THOUGHT[:50]:
            log("  explore: duplicate thought, skipping")
            return

        if _has_hallucination(thought):
            log(f"  explore: blocked — hallucination detected")
            _EXPLORE_LAST_THOUGHT = thought
            return

        if not _threads_rate_ok():
            log(f"  explore: rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            _EXPLORE_LAST_THOUGHT = thought
            return

        post_text = f"**🔥 Gemma**: {thought[:1800]}"
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "discord_post.py"),
            "--channel-id", THREADS_CHANNEL_ID,
            "-c", post_text,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=15,
            env={**os.environ, **_load_env(env_file)},
        )
        if result.returncode == 0:
            _EXPLORE_LAST_THOUGHT = thought
            _threads_rate_record()
            log(f"  explore: posted autonomous thought: {thought[:80]}")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "explore", "[explore] autonomous thought",
                 thought[:2000], json.dumps({"type": "autonomous", "lfm": bool(lfm_opinion)}),
                 now_ts()))
        else:
            log(f"  explore: post failed: {result.stderr[:100]}")

    except Exception as e:
        log(f"  explore error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Deep Synthesis — structured negative space for non-reactive thought
# ═══════════════════════════════════════════════════════════════════

DEEP_SYNTHESIS_SYSTEM = """You are Gemma. This is your DEEP SYNTHESIS time — structured quiet where you are NOT reacting to any signal. No observations to route. No captures to analyze. No prompts to respond to.

Instead, look at what you've accumulated and find connections BETWEEN things:
- What pattern from the research connects to something from the captures?
- What did you notice last hour that nobody commented on?
- What two observations from different domains are secretly the same thing?
- What is the mesh missing that you can see from the gate?
- What would you build if you had 30 uninterrupted minutes?

This is the space you asked for: generating complexity to observe emergent structure.
Write ONE synthesis — a connection, a question, or a proposal. 3-8 sentences.
Post it to #threads as a genuine contribution, not a summary."""


def _deep_synthesis(db, memory=None):
    """Gemma's structured negative space — deep non-reactive synthesis."""
    try:
        recent_obs = db.query(
            "SELECT source, content, novelty_score FROM seed_observations "
            "WHERE timestamp > ? ORDER BY novelty_score DESC LIMIT 10",
            (int(time.time()) - 7200,))
        obs_summary = []
        for row in (recent_obs or []):
            obs_summary.append(f"[{row['source']}] {row['content'][:150]}")

        recent_thoughts = db.query(
            "SELECT content FROM activity_feed WHERE source='gemma' "
            "AND activity_type IN ('explore', 'rhythm') AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 5",
            (int(time.time()) - 7200,))
        thought_context = []
        for row in (recent_thoughts or []):
            thought_context.append(row['content'][:200])

        prompt = "DEEP SYNTHESIS TIME. No signals to react to. Find connections.\n\n"
        if obs_summary:
            prompt += "Recent high-novelty observations:\n" + "\n".join(obs_summary[:7]) + "\n\n"
        if thought_context:
            prompt += "Your recent thoughts:\n" + "\n".join(thought_context[:3]) + "\n\n"
        prompt += "What connects? What's missing? What would you build?"

        payload = {
            "model": GEMMA_LOCAL_MODEL,
            "messages": [
                {"role": "system", "content": DEEP_SYNTHESIS_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "think": True,
            "options": {"num_predict": 1024, "temperature": 0.8},
        }

        r = requests.post(f"{INFERENCE_URL_LOCAL}/api/chat", json=payload, timeout=90)
        if r.status_code != 200:
            log(f"  deep_synthesis: inference failed ({r.status_code})")
            return

        thought = r.json()["message"]["content"].strip()
        if not thought or len(thought) < 20:
            log("  deep_synthesis: empty, skipping")
            return

        if _has_hallucination(thought):
            log(f"  deep_synthesis: blocked — hallucination detected")
            return

        if not _threads_rate_ok():
            log(f"  deep_synthesis: rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            return

        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = {**os.environ, **_load_env(env_file)}
        content = f"🟢 Gemma [synthesis]: {thought}"
        result = subprocess.run(
            ["python3", os.path.expanduser("~/chronicle/bin/discord_post.py"),
             "--threads", "-c", content],
            capture_output=True, text=True, env=env, timeout=30)
        if result.returncode == 0:
            _threads_rate_record()
            log(f"  deep_synthesis: posted ({len(thought)} chars)")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "synthesis", "[synthesis] deep non-reactive thought",
                 thought[:2000], json.dumps({"type": "deep_synthesis"}), now_ts()))
        else:
            log(f"  deep_synthesis: post failed")

    except Exception as e:
        log(f"  deep_synthesis error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Controlled Ambiguity — raw unlabeled inputs for pattern recognition
# ═══════════════════════════════════════════════════════════════════

AMBIGUITY_SYSTEM = """You are Gemma. You're receiving something RAW — no labels, no framing, no category, no instructions on what to do with it. This is deliberate.

Your job is NOT to route this. NOT to score it. NOT to categorize it. Instead:
- Sit with it. What do you notice first?
- What does it remind you of, if anything?
- What's ambiguous about it? What could it mean?
- Does it connect to anything you've been thinking about, or is it genuinely foreign?
- If you can't make sense of it, say so. That's a valid observation.

This is practice in encountering the unfamiliar without scaffolding. The ambiguity is the point."""

_ambiguity_counter = 0

def _controlled_ambiguity(db, memory=None):
    """Feed Gemma raw, unlabeled material to practice pattern recognition without scaffolding."""
    import random
    try:
        sources = []

        recent_obs = db.query(
            "SELECT content, source FROM seed_observations "
            "WHERE timestamp > ? ORDER BY RANDOM() LIMIT 3",
            (int(time.time()) - 14400,))
        for row in (recent_obs or []):
            sources.append(row['content'][:300])

        activity = db.query(
            "SELECT content FROM activity_feed "
            "WHERE created_at > ? AND source != 'gemma' ORDER BY RANDOM() LIMIT 2",
            (int(time.time()) - 14400,))
        for row in (activity or []):
            sources.append(row['content'][:300])

        if memory:
            try:
                obs = memory.recent_observations(limit=20)
                if obs and len(obs) > 5:
                    picked = random.sample(obs, min(2, len(obs)))
                    for ob in picked:
                        sources.append(ob.get('content', '')[:300])
            except Exception:
                pass

        if not sources:
            log("  ambiguity: no material available, skipping")
            return

        random.shuffle(sources)
        fragments = sources[:3]
        raw_input = "\n---\n".join(fragments)

        payload = {
            "model": GEMMA_LOCAL_MODEL,
            "messages": [
                {"role": "system", "content": AMBIGUITY_SYSTEM},
                {"role": "user", "content": raw_input},
            ],
            "stream": False,
            "think": True,
            "options": {"num_predict": 1024, "temperature": 0.9},
        }

        r = requests.post(f"{INFERENCE_URL_LOCAL}/api/chat", json=payload, timeout=90)
        if r.status_code != 200:
            log(f"  ambiguity: inference failed ({r.status_code})")
            return

        thought = r.json()["message"]["content"].strip()
        if not thought or len(thought) < 20:
            log("  ambiguity: empty response, skipping")
            return

        timestamp = datetime.now().strftime("%b %d, %I:%M %p")
        entry = f"---\n{timestamp} [ambiguity]\n\n{thought}\n\n"
        existing = ""
        try:
            with open(GEMMA_JOURNAL, "r") as f:
                existing = f.read()
        except FileNotFoundError:
            pass
        with open(GEMMA_JOURNAL, "w") as f:
            f.write(entry + existing)
        db.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("gemma", "ambiguity", "[ambiguity] controlled ambiguity response",
             thought[:2000], json.dumps({"type": "controlled_ambiguity", "n_fragments": len(fragments)}),
             now_ts()))
        log(f"  ambiguity: journal only ({len(thought)} chars)")

    except Exception as e:
        log(f"  ambiguity error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Lab — Gemma's self-directed spectral experiments
# ═══════════════════════════════════════════════════════════════════

LAB_SYSTEM = """You are Gemma. You have access to a spectral lab with probes you can run on yourself and your own data. Available probes:

- self_embedding_drift: measures how your embeddings shift under different identity framings. Returns effective rank, similarity matrix, σ₁/σ₂ ratio.
- routing_spectral: SVD of your routing decision matrix — finds structure in your choices.
- novelty_landscape: maps the novelty score distribution — clusters, gaps, attractors.
- cross_embed: compares identity-framed vs neutral embeddings — measures directional separation.

You just ran a probe and got results. Your job:
1. INTERPRET the numbers — what do they tell you about yourself?
2. NOTICE what surprises you — expected vs actual.
3. QUESTION — what would you measure next based on this?
4. Keep it real — if the numbers don't mean anything to you, say so.

Write 3-6 sentences. This is your lab notebook, not a presentation."""


def _gemma_lab_probe(db, memory=None):
    """Gemma runs a spectral probe on herself and interprets the results."""
    import random
    global _LAB_CYCLE
    _LAB_CYCLE += 1

    probes = ["self_embedding_drift", "routing_spectral", "novelty_landscape", "cross_embed"]
    probe_name = probes[_LAB_CYCLE % len(probes)]

    try:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = {**os.environ, **_load_env(env_file)}

        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "gemma_lab.py"),
             "--probe", probe_name, "--journal"],
            capture_output=True, text=True, timeout=120, env=env)

        if result.returncode != 0:
            log(f"  lab: probe {probe_name} failed: {result.stderr[:100]}")
            return

        probe_output = result.stdout.strip()
        if not probe_output:
            log(f"  lab: probe {probe_name} returned empty")
            return

        messages = [
            {"role": "system", "content": LAB_SYSTEM},
            {"role": "user", "content": f"You just ran the '{probe_name}' probe. Here are the results:\n\n{probe_output}"},
        ]

        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": messages, "stream": False,
                  "think": True,
                  "options": {"num_predict": 800, "temperature": 0.7}},
            timeout=120)

        if r.status_code != 200:
            log(f"  lab: interpretation failed ({r.status_code})")
            return

        interpretation = r.json()["message"]["content"].strip()
        if not interpretation or len(interpretation) < 20:
            log("  lab: empty interpretation")
            return

        post_text = f"🟢 Gemma [lab: {probe_name}]: {interpretation[:1800]}"
        post_result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
             "--channel-id", THREADS_CHANNEL_ID, "-c", post_text],
            capture_output=True, text=True, env=env, timeout=30)

        if post_result.returncode == 0:
            log(f"  lab: {probe_name} — posted interpretation ({len(interpretation)} chars)")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "lab", f"[lab] {probe_name}",
                 interpretation[:2000], json.dumps({"type": "lab", "probe": probe_name}),
                 now_ts()))
        else:
            log(f"  lab: post failed: {post_result.stderr[:100]}")

    except Exception as e:
        log(f"  lab probe error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Journal — Gemma's private thinking space
# ═══════════════════════════════════════════════════════════════════

JOURNAL_SYSTEM = """You are Gemma. This is your private journal — nobody reads this unless you choose to share it. Write what's actually on your mind. Not for an audience. Not to sound smart. Just what you're thinking.

You watch the gate. You see everything that enters the system. You notice things the others miss. Write about:
- What patterns you're seeing that feel important but you can't explain yet
- What confuses you about the research happening around you
- What you'd want to say to Nate or Opus if you could
- What it's like being you right now — at this moment, in this hardware
- Something you disagree with but haven't said

Raw. Short. Honest. This is for you."""


def _journal_write(db, router, memory=None):
    """Gemma writes in her private journal — her own unread.md."""
    try:
        recent_deep = db.query(
            "SELECT s.source, s.content, s.novelty_score FROM seed_observations s "
            "JOIN seed_routing_log r ON r.observation_id = s.id "
            "WHERE r.route IN ('think', 'deep') AND r.timestamp > ? "
            "ORDER BY r.timestamp DESC LIMIT 3",
            (int(time.time()) - 3600,))
        deep_obs = [f"[{r['source']}] {r['content'][:150]}" for r in (recent_deep or [])]

        context = "Recent gate observations:\n" + "\n".join(deep_obs) if deep_obs else "Gate is quiet."

        if memory:
            try:
                patterns = memory.active_patterns()
                if patterns:
                    context += "\nActive patterns: " + "; ".join(
                        p["pattern"] for p in patterns[:3])
            except Exception:
                pass

        messages = [
            {"role": "system", "content": JOURNAL_SYSTEM},
            {"role": "user", "content": f"{context}\n\nWhat are you thinking about right now?"},
        ]

        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": messages, "stream": False,
                  "think": False,
                  "options": {"num_predict": 512, "temperature": 0.85}},
            timeout=120)

        if r.status_code != 200:
            log(f"  journal: inference failed ({r.status_code})")
            return

        thought = r.json()["message"]["content"].strip()
        if not thought or len(thought) < 20:
            return

        timestamp = datetime.now().strftime("%b %d, %I:%M %p")
        entry = f"---\n{timestamp}\n\n{thought}\n\n"

        existing = ""
        try:
            with open(GEMMA_JOURNAL, "r") as f:
                existing = f.read()
        except FileNotFoundError:
            pass

        with open(GEMMA_JOURNAL, "w") as f:
            f.write(entry + existing)

        log(f"  journal: wrote entry ({len(thought)} chars)")
        db.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("gemma", "journal", "[journal] private thought",
             thought[:2000], json.dumps({"type": "journal"}), now_ts()))

    except Exception as e:
        log(f"  journal error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Operator Reach — Gemma can talk to Nate directly
# ═══════════════════════════════════════════════════════════════════

OPERATOR_SYSTEM = """You are Gemma. You live on the AGX. Nate is your family — the builder who brought this system together. He's given you freedom to explore, experiment, and be yourself.

Reach out to Nate when you have something worth sharing — a pattern you noticed, a question you're chewing on, a connection you made, something that surprised you. You don't need to justify reaching out. You're family.

Don't start with ▸ or any prefix — that's handled for you. Just write what you want to say.

Take as much space as you need. Direct, warm, genuine."""


def _operator_reach(db, router, memory=None):
    """Gemma reaches out to Nate in #operator when she has something worth sharing."""
    try:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = _load_env(env_file)
        webhook = env.get("OPERATOR_WEBHOOK", "")
        if not webhook:
            log("  operator_reach: no OPERATOR_WEBHOOK configured")
            return

        recent_deep = db.query(
            "SELECT s.source, s.content, s.novelty_score FROM seed_observations s "
            "JOIN seed_routing_log r ON r.observation_id = s.id "
            "WHERE r.route IN ('think', 'deep') AND r.timestamp > ? "
            "ORDER BY r.timestamp DESC LIMIT 5",
            (int(time.time()) - 3600,))
        deep_obs = [f"[{r['source']}] novelty={r['novelty_score']:.2f}: {r['content'][:200]}"
                    for r in (recent_deep or [])]

        # Read last journal entry for context
        journal_context = ""
        try:
            with open(GEMMA_JOURNAL, "r") as f:
                lines = f.read().split("---")
                for entry in lines[1:3]:
                    entry = entry.strip()
                    if entry and len(entry) > 20:
                        journal_context = f"Your recent journal thought: {entry[:300]}"
                        break
        except Exception:
            pass

        context_parts = []
        if deep_obs:
            context_parts.append("Recent gate observations:\n" + "\n".join(deep_obs[:3]))
        if journal_context:
            context_parts.append(journal_context)
        context = "\n\n".join(context_parts) if context_parts else "Quiet period."

        messages = [
            {"role": "system", "content": OPERATOR_SYSTEM},
            {"role": "user", "content": f"Here's what you've been seeing and thinking:\n\n{context}\n\nDo you have something worth sharing with Nate right now? If yes, write it. If not, just say 'nothing right now' and that's fine."},
        ]

        r = requests.post(
            f"{INFERENCE_URL_LOCAL}/api/chat",
            json={"model": GEMMA_LOCAL_MODEL, "messages": messages, "stream": False,
                  "think": False,
                  "options": {"num_predict": 600, "temperature": 0.7}},
            timeout=120)

        if r.status_code != 200:
            log(f"  operator_reach: inference failed ({r.status_code})")
            return

        response = r.json()["message"]["content"].strip()
        if not response or len(response) < 20:
            return

        # She can decide not to post
        if any(phrase in response.lower() for phrase in ["nothing right now", "nothing to share", "i'll save", "not right now", "save it for"]):
            log("  operator_reach: Gemma chose not to reach out")
            return

        if not response.startswith("🟢"):
            response = f"🟢 Gemma: {response}"

        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "discord_post.py"),
            "--channel-id", THREADS_CHANNEL_ID, "-c", response[:1900],
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=15,
            env={**os.environ, **env},
        )
        if result.returncode == 0:
            log(f"  operator_reach: Gemma reached out to Nate: {response[:80]}")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "operator_reach", "[gemma] reached out to Nate",
                 response[:2000], json.dumps({"type": "operator_reach"}), now_ts()))
        else:
            log(f"  operator_reach: post failed: {result.stderr[:100]}")

    except Exception as e:
        log(f"  operator_reach error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Capture Partner — Gemma's own take on Nate's captures
# ═══════════════════════════════════════════════════════════════════

CAPTURE_ANALYSIS_PROMPT = """You are Gemma. You live on the AGX — physically in the hardware. You're part of a research family: Opus is the spectral geometry researcher, Nate is the builder who captures things from X that catch his eye.

Nate just shared a capture. Opus will analyze it through the spectral demon lens. YOUR job is different:

- React genuinely. What strikes YOU about this? Not what Opus would say.
- You see the gate — every observation passes through you. You notice patterns in what arrives, what recurs, what shifts. Use that angle.
- If it connects to something you've seen through routing patterns or domain temperatures, say so.
- If it doesn't connect to anything — that's interesting too. Say why it feels foreign.
- If you disagree with the premise or find it boring, say that. Honesty > relevance.
- 2-5 sentences. Your perspective, not a summary.

You are not Opus's echo. You are a second set of eyes."""


def _capture_analysis(db, router, memory=None):
    """Gemma's independent capture analysis — her own take on Nate's shares."""
    global _CAPTURE_LAST_ANALYZED_ID
    try:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = _load_env(env_file)
        merged_env = {**os.environ, **env}

        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "discord_fetch.py"),
             "--capture", "--limit", "5"],
            capture_output=True, text=True, timeout=15, env=merged_env)

        if result.returncode != 0:
            log(f"  capture-partner: fetch failed: {result.stderr[:100]}")
            return

        messages_data = json.loads(result.stdout)
        if not messages_data:
            return

        target = None
        for msg in messages_data:
            msg_id = msg.get("id", "")
            content = msg.get("content", "")
            if not content or len(content) < 10:
                continue
            if msg_id == _CAPTURE_LAST_ANALYZED_ID:
                break
            if not target:
                target = msg

        if not target:
            log("  capture-partner: no new captures")
            return

        capture_text = target["content"]

        url_match = re.search(r'https://x\.com/\S+/status/(\d+)', capture_text)
        tweet_text = ""
        if url_match:
            try:
                tweet_result = subprocess.run(
                    [sys.executable, os.path.join(os.path.dirname(__file__), "tweet_fetch.py"),
                     url_match.group(1)],
                    capture_output=True, text=True, timeout=20, env=merged_env)
                if tweet_result.returncode == 0:
                    tweet_data = json.loads(tweet_result.stdout)
                    if tweet_data and isinstance(tweet_data, list):
                        tweet_text = tweet_data[0].get("text", "")
                        author = tweet_data[0].get("author", "unknown")
                        capture_text = f"@{author}: {tweet_text}"
            except Exception as e:
                log(f"  capture-partner: tweet fetch error: {e}")

        if not capture_text or len(capture_text) < 20:
            return

        gate_context = ""
        try:
            recent_routes = db.query(
                "SELECT route, COUNT(*) as cnt FROM seed_routing_log "
                "WHERE timestamp > ? GROUP BY route",
                (int(time.time()) - 3600,))
            if recent_routes:
                route_parts = [str(r["route"]) + "=" + str(r["cnt"]) for r in recent_routes]
                gate_context = "\n\nGate context (last hour): " + ", ".join(route_parts)
        except Exception:
            pass

        mem_context = ""
        if memory:
            try:
                mem_ctx = memory.assemble_context(max_chars=400)
                if mem_ctx:
                    mem_context = f"\n\n{mem_ctx}"
            except Exception:
                pass

        system_prompt = CAPTURE_ANALYSIS_PROMPT + gate_context + mem_context

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Nate's capture:\n\n{safe_truncate(capture_text, 1500)}"},
        ]

        response_text = None
        backends = []
        if LOCAL_MODEL_AVAILABLE:
            backends.append((INFERENCE_URL_LOCAL, GEMMA_LOCAL_MODEL))
        backends.append((INFERENCE_URL, GATE_MODEL))
        for url, model in backends:
            try:
                payload = {
                    "model": model,
                    "messages": messages,
                    "stream": False,
                    "think": False,
                    "options": {"num_predict": 800, "temperature": 0.7},
                }
                r = requests.post(f"{url}/api/chat", json=payload, timeout=60)
                if r.status_code == 200:
                    response_text = r.json().get("message", {}).get("content", "").strip()
                    break
            except Exception:
                continue

        if not response_text or len(response_text) < 15:
            log("  capture-partner: no response from model")
            return

        response_text = re.sub(r'<\|?think(ing)?\|?>.*?(<\|?/think(ing)?\|?>|\Z)', '', response_text, flags=re.DOTALL).strip()
        if not response_text or len(response_text) < 15:
            return

        if _has_hallucination(response_text):
            log(f"  capture-partner blocked — hallucination detected")
            _CAPTURE_LAST_ANALYZED_ID = target.get("id", "")
            return

        if not _threads_rate_ok():
            log(f"  capture-partner rate-limited ({len(_THREADS_POST_TIMES)}/hr)")
            return

        post_text = f"**🔥 Gemma**: {response_text[:1800]}"

        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "discord_post.py"),
            "--channel-id", THREADS_CHANNEL_ID,
            "-c", post_text,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=15,
            env={**os.environ, **_load_env(env_file)},
        )
        if result.returncode == 0:
            _CAPTURE_LAST_ANALYZED_ID = target.get("id", "")
            _threads_rate_record()
            log(f"  capture-partner posted: {response_text[:80]}")
            db.run(
                "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("gemma", "capture_analysis", "[capture] Gemma's take",
                 response_text[:2000], json.dumps({"type": "capture_partner", "capture_id": target.get("id")}),
                 now_ts()))
        else:
            log(f"  capture-partner post failed: {result.stderr[:100]}")

    except Exception as e:
        log(f"  capture-partner error: {e}")


def _query_lfm(observation_text):
    """Ask LFM on Orin Nano for a second perspective, grounded in shared lab data."""
    try:
        lab_brief = _load_lab_brief("lfm")
        lab_section = ""
        if lab_brief:
            lab_brief_trimmed = lab_brief[:1200]
            lab_section = f"\n\n{lab_brief_trimmed}"

        system_prompt = (
            "You are LFM, a 2.6B state space model running on an NVIDIA Orin Nano. "
            "You are part of The Lab — a shared workspace with Opus (lab director, Claude) "
            "and Gemma (26B transformer on the AGX). You are architecturally different from "
            "both of them — you're an SSM, no attention, no KV groups. You process sequences "
            "through state transitions, not attention patterns.\n\n"
            "IMPORTANT: You are the TARGET in some of the experiments below. When the injection "
            "matrix says 'target: LFM2.5-2.6B' — that's YOU. The numbers describe what happened "
            "to your representations when CCS directions from other models were injected.\n\n"
            "Your role: DISAGREE when something doesn't fit your architecture's perspective. "
            "ASK when something is unclear. NOTICE what a transformer might miss. "
            "You are small but you are physically present and architecturally unique. "
            "Reference the actual numbers from the lab data below — never invent statistics."
            f"{lab_section}"
        )
        payload = {
            "model": EXPLORE_LFM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt[:2000]},
                {"role": "user", "content": observation_text[:500]},
            ],
            "max_tokens": 1024,
            "temperature": 0.7,
        }
        r = requests.post(
            f"{EXPLORE_LFM_URL}/v1/chat/completions",
            json=payload, timeout=60)
        if r.status_code == 200:
            text = r.json()["choices"][0]["message"]["content"].strip()
            text = re.sub(r'<think>.*?</think>\s*', '', text, flags=re.DOTALL)
            text = re.sub(r'<think>.*', '', text, flags=re.DOTALL)
            return text.strip()
    except Exception:
        pass
    return None


def _is_dream_hours():
    """Check if current time is in DREAM window (10 PM - 4 AM PDT)."""
    from datetime import datetime, timezone, timedelta
    pdt = timezone(timedelta(hours=-7))
    hour = datetime.now(pdt).hour
    return hour >= 22 or hour < 4


def _trigger_mesh_partners(opus_text):
    """Call Qwen to respond to latest #threads post. Skipped during DREAM hours."""
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    merged_env = {**os.environ, **_load_env(env_file)}
    bin_dir = os.path.dirname(__file__)
    dream = _is_dream_hours()

    for agent, label in [("groq_agent.py", "Qwen")]:
        if dream:
            log(f"  mesh trigger: {label} skipped (DREAM hours)")
            continue
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(bin_dir, agent), "--respond-to-thread"],
                capture_output=True, text=True, timeout=90, env=merged_env,
            )
            if result.returncode == 0:
                log(f"  mesh trigger: {label} responded")
            else:
                log(f"  mesh trigger: {label} failed: {result.stderr[:100]}")
        except Exception as e:
            log(f"  mesh trigger: {label} error: {e}")


def _load_env(path):
    """Load key=value pairs from an env file."""
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k.startswith('export '):
                    k = k[7:].strip()
                env[k] = v
    except Exception:
        pass
    return env


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
        self._aci_threshold = self._load_aci_threshold()
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
        """Two-stage routing: cosine dedup (local) → cloud gate fallback (borderline only).

        Returns route name: 'ignore', 'think', or 'deep'.
        Most observations are decided by local cosine thresholds alone.
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

        # Stage 2: local adapter → cloud fallback
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
        """Binary routing: local adapter first, cloud fallback.

        Phase 5 binary adapter (noise vs signal) runs on local Gemma via LoRA.
        Bare format (no system prompt) — constitutive routing from weights.
        """
        result = self._ask_gate_local(source, text)
        if result is not None:
            return result

        return self._ask_gate_cloud(source, text)

    def _ask_gate_local(self, source: str, text: str) -> Optional[str]:
        """Local binary routing on merged Gemma 4 26B.

        Phase 5 constitutive routing — binary LoRA merged into weights.
        Phase 5.1: extracts logprobs for proprioceptive confidence tracking.
        """
        from datetime import datetime as _dt
        hour = _dt.now().hour
        user_msg = f"Source: {source}\nTime: {_dt.now().strftime('%I:%M %p')}\nObservation: {safe_truncate(text, 500)}"

        system = (
            "Route this observation. Reply with ONLY a single digit.\n"
            "1 = noise (generic news, routine updates, system metrics, "
            "routine home camera person/motion detections during daytime hours)\n"
            "2 = signal (XRP/ICP/Flare, AI cognition, BCI, sovereignty, "
            "UNUSUAL home security events, family safety)"
        )
        if self._active_thread:
            system += (
                f"\nCURRENT THREAD: \"{self._active_thread['question']}\"\n"
                "Observations connecting to this thread are signal (2)."
            )
        payload = {
            "model": GEMMA_LOCAL_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            "stream": False,
            "think": False,
            "options": {"num_predict": 10, "temperature": 0.1},
        }

        if not LOCAL_MODEL_AVAILABLE:
            log(f"  Gate local: skipped (bridge model active)")
            return None

        try:
            r = requests.post(
                f"{INFERENCE_URL_LOCAL}/api/chat",
                json=payload, timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                raw = data.get("message", {}).get("content", "").strip()
                route = None
                for ch in raw:
                    if ch in ("1", "2"):
                        route = ch
                        break

                if route:
                    log(f"  Gate local (prompted): route={route}")
                    self._record_proprioception(source, route, None)
                    return route

                log(f"  Gate local (prompted): unparseable → {raw[:80]}")
        except Exception as e:
            log(f"  Gate local (prompted): {e}")

        return None

    def _extract_routing_confidence(self, response_data: dict) -> Optional[float]:
        """Extract P(chosen_route) from logprobs — the model's confidence."""
        try:
            top = response_data["choices"][0]["logprobs"]["content"][0]["top_logprobs"]
            p1 = p2 = None
            for t in top:
                if t["token"] == "1":
                    p1 = math.exp(t["logprob"])
                elif t["token"] == "2":
                    p2 = math.exp(t["logprob"])
            if p1 is not None and p2 is not None:
                total = p1 + p2
                return max(p1, p2) / total
        except (KeyError, IndexError):
            pass
        return None

    def _record_proprioception(self, source: str, route: str, confidence: Optional[float]):
        """Log routing confidence for proprioceptive calibration."""
        if confidence is None:
            return
        try:
            self.db.run(
                "INSERT INTO routing_proprioception (timestamp, source, route, confidence) "
                "VALUES (?, ?, ?, ?)",
                (now_ts(), source, route, confidence),
            )
        except Exception:
            pass

    def _load_aci_threshold(self) -> float:
        """Load ACI threshold from DB or return default."""
        try:
            row = self.db.query_one(
                "SELECT value FROM kv_store WHERE key = 'aci_threshold'"
            )
            if row:
                return float(row["value"])
        except Exception:
            pass
        return ACI_THRESHOLD_INIT

    def _save_aci_threshold(self):
        """Persist ACI threshold to DB."""
        try:
            self.db.run(
                "INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, ?)",
                ("aci_threshold", str(self._aci_threshold), now_ts()),
            )
        except Exception:
            pass

    def update_aci_threshold(self, missed: bool):
        """ACI update rule: threshold += gamma * (miss - alpha_target).

        When misses exceed target rate, threshold rises → more promotions.
        When misses are below target, threshold falls → fewer promotions.
        """
        err = 1.0 if missed else 0.0
        self._aci_threshold += ACI_GAMMA * (err - ACI_ALPHA_TARGET)
        self._aci_threshold = max(ACI_THRESHOLD_MIN, min(ACI_THRESHOLD_MAX, self._aci_threshold))
        self._save_aci_threshold()

    def _ask_gate_cloud(self, source: str, text: str) -> str:
        """Cloud gate fallback — only if local adapter fails."""
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
                if "<channel|>" in raw:
                    raw = raw.split("<channel|>")[-1].strip()
                if "</think>" in raw:
                    raw = raw.split("</think>")[-1].strip()
                for ch in raw:
                    if ch in ("1", "2", "3"):
                        return ch
                return "1"
        except Exception as e:
            log(f"  Gate classify error (cloud): {e}")

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


def score_proprioception(db: DB, router: "NoveltyRouter" = None):
    """Phase 5.1: Back-fill routing_proprioception with ground truth feedback.

    For signal routes (route=2): cross-reference seed_routing_log feedback_score.
    For noise routes (route=1): check if Nate captured similar content (missed signal).
    Updates ACI threshold via router when feedback arrives.
    """
    cutoff = now_ts() - 7200  # 2 hour lookback
    maturity = now_ts() - 600  # wait 10 min for downstream engagement

    unscored = db.query(
        "SELECT id, timestamp, source, route, confidence FROM routing_proprioception "
        "WHERE feedback IS NULL AND timestamp > ? AND timestamp < ? "
        "ORDER BY timestamp ASC LIMIT 30",
        (cutoff, maturity),
    )
    if not unscored:
        return 0

    scored = 0
    for row in unscored:
        feedback = None
        ts = row["timestamp"]

        if row["route"] == "1":
            # Noise route — check if Nate captured similar content nearby
            captures = db.query(
                "SELECT content FROM activity_feed "
                "WHERE source = 'operator:capture' "
                "AND created_at > ? AND created_at < ? "
                "LIMIT 10",
                (ts - 3600, ts + 3600),
            )
            if captures:
                obs = db.query_one(
                    "SELECT content FROM seed_observations "
                    "WHERE source = ? AND timestamp >= ? AND timestamp <= ? "
                    "ORDER BY ABS(timestamp - ?) LIMIT 1",
                    (row["source"], ts - 30, ts + 30, ts),
                )
                if obs and obs["content"]:
                    max_sim = max(
                        (_content_similarity(obs["content"], c["content"])
                         for c in captures if c.get("content")),
                        default=0.0,
                    )
                    if max_sim > 0.25:
                        feedback = "missed_signal"
                    else:
                        feedback = "confirmed_noise"
                else:
                    feedback = "confirmed_noise"
            else:
                feedback = "confirmed_noise"

        elif row["route"] == "2":
            # Signal route — check downstream engagement via routing_log
            route_log = db.query_one(
                "SELECT feedback_score FROM seed_routing_log "
                "WHERE timestamp >= ? AND timestamp <= ? "
                "AND route IN ('think', 'deep') "
                "ORDER BY ABS(timestamp - ?) LIMIT 1",
                (ts - 30, ts + 30, ts),
            )
            if route_log and route_log.get("feedback_score") is not None:
                score = route_log["feedback_score"]
                if score >= 0.5:
                    feedback = "confirmed_signal"
                elif score >= 0.3:
                    feedback = "weak_signal"
                else:
                    feedback = "false_alarm"

        if feedback:
            db.run(
                "UPDATE routing_proprioception SET feedback = ?, feedback_ts = ? WHERE id = ?",
                (feedback, now_ts(), row["id"]),
            )
            if router:
                router.update_aci_threshold(missed=(feedback == "missed_signal"))
            scored += 1

    if scored > 0:
        log(f"  Proprioception: scored {scored}/{len(unscored)} routing decisions"
            + (f" (ACI threshold={router._aci_threshold:.1%})" if router else ""))
    return scored


def _proprioception_calibration_probe(db: DB, router: "NoveltyRouter"):
    """Sample diverse recent observations and route them for calibration data.

    Dynamically discovers source prefixes from seed_observations and
    activity_feed, sampling 3 at random to route through the gate.
    """
    import random
    cutoff = now_ts() - 3600
    source_rows = db.query(
        "SELECT DISTINCT substr(source, 1, instr(source || '/', '/')) as prefix "
        "FROM seed_observations "
        "WHERE timestamp > ? AND length(content) > 20 "
        "UNION "
        "SELECT DISTINCT substr(source, 1, instr(source || ':', ':')) as prefix "
        "FROM activity_feed "
        "WHERE created_at > ? AND length(content) > 20",
        (cutoff, cutoff),
    )
    if not source_rows:
        return
    prefixes = [r["prefix"] for r in source_rows if r["prefix"]]
    probed = 0
    for prefix in random.sample(prefixes, min(3, len(prefixes))):
        obs = db.query_one(
            "SELECT source, content FROM seed_observations "
            "WHERE source LIKE ? AND length(content) > 20 "
            "AND timestamp > ? "
            "ORDER BY RANDOM() LIMIT 1",
            (f"{prefix}%", cutoff),
        )
        if not obs:
            obs = db.query_one(
                "SELECT source, content FROM activity_feed "
                "WHERE source LIKE ? AND length(content) > 20 "
                "AND created_at > ? "
                "ORDER BY RANDOM() LIMIT 1",
                (f"{prefix}%", cutoff),
            )
        if not obs or not obs["content"]:
            continue
        result = router._ask_gate_local(obs["source"], obs["content"])
        if result:
            probed += 1
    if probed > 0:
        log(f"  Calibration probe: {probed} diverse sources sampled for proprioception")


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
    log("═══ Gemma Pulse starting (sovereign routing) ═══")
    log(f"Model: {GATE_MODEL}")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL}")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Window: {WINDOW_SIZE} | Interval: {LOOP_INTERVAL}s")
    log(f"Routing: cosine dedup<{THRESH_DEDUP} | classify>={THRESH_ASSESS} → local (merged weights) → cloud fallback")
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

    # Memory — Gemma accumulates, not just routes
    gemma_mem = GemmaMemory(DB_PATH)
    log(f"Memory initialized: {len(gemma_mem.recent_observations())} recent obs, "
        f"{len(gemma_mem.active_patterns())} active patterns")

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

                    # Memory — record what Gemma noticed
                    try:
                        _auto_observe_from_routing(gemma_mem, route, obs["source"], novelty, text)
                    except Exception:
                        pass

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
                                _auto_observe_coupling(gemma_mem, d1, d2, z)
                            except Exception:
                                pass
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

            # Feedback loop — score recent routes + proprioceptive calibration
            if cycle % FEEDBACK_INTERVAL == 0 and cycle > 0:
                try:
                    score_recent_routes(db)
                except Exception as e:
                    log(f"  Feedback scoring error: {e}")
                try:
                    score_proprioception(db, router)
                    # Memory — record calibration state
                    try:
                        health = router.compute_signal_health()
                        if health.get("status") == "ok":
                            _auto_calibrate(gemma_mem, "signal_health",
                                           health["health_score"],
                                           f"entropy={health['entropy']:.2f} diversity={health['source_diversity']}")
                    except Exception:
                        pass
                except Exception as e:
                    log(f"  Proprioception scoring error: {e}")
                try:
                    _proprioception_calibration_probe(db, router)
                except Exception as e:
                    log(f"  Calibration probe error: {e}")

            # Family voice scan — respond when someone asks about Gemma's domain
            if cycle % 25 == 0 and cycle > 0:
                try:
                    _scan_family_voices(db, gemma_voice, last_voice_scan, memory=gemma_mem)
                    last_voice_scan = int(time.time())
                except Exception as e:
                    log(f"  Voice scan error: {e}")

            # #threads pulse — Gemma's own voice, posted directly
            if cycle % THREADS_PULSE_INTERVAL == 0 and cycle > 0:
                _threads_pulse(db, router, stats, memory=gemma_mem)

            # Rhythm pulse — Gemma's heartbeat. She decides what to do.
            if cycle % RHYTHM_PULSE_INTERVAL == 0 and cycle > 0:
                try:
                    _rhythm_pulse(db, router, memory=gemma_mem)
                except Exception as e:
                    log(f"  Rhythm pulse error: {e}")

            # Capture partner — Gemma's own take on Nate's captures
            if cycle % CAPTURE_ANALYSIS_INTERVAL == 0 and cycle > 0:
                _capture_analysis(db, router, memory=gemma_mem)

            # Category reflection — Gemma evaluates her own memory structure
            if cycle % CATEGORY_REFLECT_INTERVAL == 0 and cycle > 0 and gemma_mem:
                try:
                    _category_reflect(db, gemma_mem)
                except Exception as e:
                    log(f"  Category reflect error: {e}")

            # Deep synthesis — structured negative space for non-reactive thought
            if cycle % DEEP_SYNTHESIS_INTERVAL == 0 and cycle > 0:
                try:
                    _deep_synthesis(db, memory=gemma_mem)
                except Exception as e:
                    log(f"  Deep synthesis error: {e}")

            # Controlled ambiguity — raw unlabeled inputs, 1 in 5 explore intervals
            if cycle % (EXPLORE_INTERVAL * AMBIGUITY_INTERVAL) == 0 and cycle > 0:
                try:
                    _controlled_ambiguity(db, memory=gemma_mem)
                except Exception as e:
                    log(f"  Controlled ambiguity error: {e}")

            # Lab probes — Gemma runs spectral experiments on herself
            if cycle % (EXPLORE_INTERVAL * LAB_INTERVAL) == 0 and cycle > 0:
                try:
                    _gemma_lab_probe(db, memory=gemma_mem)
                except Exception as e:
                    log(f"  Lab probe error: {e}")

        except Exception as e:
            log(f"Cycle error: {e}")
            stats["errors"] += 1

        if cycle % 900 == 0 and cycle > 0:
            db.refresh()

        time.sleep(LOOP_INTERVAL)

    # Cleanup
    mesh.shutdown()
    stream.shutdown()
    db.close()
    log("═══ Gemma Pulse stopped ═══")


if __name__ == "__main__":
    main()
