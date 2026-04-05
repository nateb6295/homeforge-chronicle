"""Chronicle Mind - Configuration constants, env vars, schedules."""

import os
from datetime import datetime


DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db")
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
FEED_WATERMARK = os.path.expanduser("~/.homeforge-chronicle/feed_watermark")
CYCLE_INTERVAL = int(os.environ.get("CYCLE_INTERVAL", "600"))
LOCAL_MODEL = os.environ.get("CHRONICLE_LOCAL_MODEL", "hermes3-mind")
DEEP_MODEL = os.environ.get("CHRONICLE_DEEP_MODEL", "hermes3:8b")  # same model, no deep/shallow split
DFX_IDENTITY = os.environ.get("CHRONICLE_IDENTITY", "chronicle-auto")
WORKING_DIR = os.path.expanduser("~")
LOG_FILE = os.environ.get("CHRONICLE_LOG", os.path.expanduser("~/chronicle/chronicle-mind.log"))

# API keys (from env, loaded by wrapper or service)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
HA_TOKEN = os.environ.get("HA_TOKEN", "")
HA_URL = os.environ.get("HA_URL", "http://192.168.1.10:8123")
HA_CAMERA_ENTITY = os.environ.get("HA_CAMERA_ENTITY", "camera.driveway_fluent")
MOLTBOOK_API_KEY = os.environ.get("MOLTBOOK_API_KEY", "")
CLAWCITIES_API_KEY = os.environ.get("CLAWCITIES_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
MANIFOLD_API_KEY = os.environ.get("MANIFOLD_API_KEY", "")
MANIFOLD_MAX_BET = int(os.environ.get("MANIFOLD_MAX_BET", "50"))
MANIFOLD_MAX_CYCLE_SPEND = int(os.environ.get("MANIFOLD_MAX_CYCLE_SPEND", "200"))
MANIFOLD_API = "https://api.manifold.markets/v0"
NOSTR_NSEC = os.environ.get("NOSTR_NSEC", "")
NOSTR_RELAYS = [r for r in os.environ.get("NOSTR_RELAYS", "").split(",") if r] or [
    "wss://nos.lol", "wss://relay.damus.io", "wss://relay.primal.net", "wss://relay.nostr.band", "wss://offchain.pub",
]
NOSTR_COOLDOWN_MINS = int(os.environ.get("NOSTR_COOLDOWN_MINS", "720"))  # 12h — max 2/day, focus on engagement over broadcast
CREATIVE_COOLDOWN_MINS = int(os.environ.get("CREATIVE_COOLDOWN_MINS", "30"))

# Service endpoints
XRPL_RPC = "https://xrplcluster.com"
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
BASE_RPC = "https://mainnet.base.org"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
MOLTBOOK_API = "https://www.moltbook.com/api/v1"
CLAWCITIES_API = "https://clawcities.com/api/v1/sites/chronicle/comments"
ROSETTA_API = "https://rosetta-api.internetcomputer.org/account/balance"
NTFY_TOPIC = "chronicle-nate-5d786588e02c8854"
ARXIV_BASE = "https://ar5iv.org/abs/"

# XRPL agent wallet (canister threshold ECDSA - this is the signing wallet)
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
# Legacy wallet (separate key, not canister-controlled)
LEGACY_WALLET = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
# ICP account for balance checks
ICP_ACCOUNT_ID = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9"
# EVM address (derived from same threshold ECDSA key)
EVM_ADDRESS = "0x80D07e16165576DBc17fe1FF865495fed4E9c387"
# ERC-20 token contracts
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WFLR_CONTRACT = "0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d"

# FTSO contract addresses (Flare)
FTSO_REGISTRY = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019"

# Deep reflection interval (hours)
DEEP_REFLECTION_HOURS = 2.0

# Exploration mode: every Nth cycle is novelty-seeking
EXPLORE_EVERY_N_CYCLES = 6

# Sleep consolidation: prune + cluster scratch_pad notes between cycles
CONSOLIDATE_EVERY_N_CYCLES = 3

# ── Circadian Rhythm ──
SLEEP_START_HOUR = 0    # sleep disabled — always awake
SLEEP_END_HOUR = 0      # sleep disabled — always awake
SLEEP_CYCLE_INTERVAL = 180  # unused (no sleep window)
WAKE_CYCLE_INTERVAL = 180   # 3 minutes — maximum continuity
VISION_MODEL = "moondream"   # lightweight vision model for image description
CONSOLIDATE_SIMILARITY_THRESHOLD = 0.82
CONSOLIDATE_CROSS_CAT_THRESHOLD = 0.87
CONSOLIDATE_EMBED_MODEL = "mxbai-embed-large"
CONSOLIDATE_MIN_NOTES_TO_RUN = 8
CONSOLIDATE_MAX_CLUSTER_SIZE = 5

# Task queue: mandatory task enforcement threshold
TASK_QUEUE_MIN_PRIORITY = 8

# Mission system (multi-cycle objectives)
MISSION_FOCUS_DECAY = 0.05
MISSION_FOCUS_BOOST = 0.15
MISSION_STALL_THRESHOLD = 5
MISSION_MAX_STEPS = 8
MISSION_MAX_CYCLES = 20

# Operator directive system (hard authority hierarchy)
DIRECTIVE_TYPES = {"STOP", "REDIRECT", "RESTRICT", "ALLOW"}
DIRECTIVE_CATEGORY = "directive"
OPERATOR_PROTECTED_CATEGORIES = {"directive", "task"}

# RSS feeds for fresh context
RSS_FEEDS = [
    "https://cointelegraph.com/rss/tag/xrp",
    "https://cointelegraph.com/rss/tag/ripple",
    "https://arxiv.org/rss/cs.AI",
    "https://www.theblock.co/rss.xml",
]
RSS_CACHE_FILE = "/tmp/chronicle_rss_cache.json"
RSS_FETCH_INTERVAL = 3600  # 1 hour between fetches

# Swap guardrails (legacy - now handled by policy engine, kept for reference)
SWAP_MIN_INTERVAL_HOURS = 0.5
SWAP_MAX_DAILY_XRP = 50.0

# XRPL Policy Engine config
XRPL_POLICY_JSON = os.environ.get(
    "XRPL_POLICY_JSON",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "xrpl_policy.json")
)
XRPL_AUDIT_HMAC_KEY = os.environ.get("XRPL_AUDIT_HMAC_KEY", "chronicle-default-key")

