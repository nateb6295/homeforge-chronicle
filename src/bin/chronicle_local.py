#!/usr/bin/env python3
"""
Chronicle Local v4 - Sprout Ops Agent (Python)

Sprout is the operational backbone of Chronicle.
Mind thinks. Sprout monitors, measures, maintains, and alerts.

Architecture:
  ICP Canister (Rust, on-chain) <-> HTTP API <-> This script <-> Ollama (local LLM)
                                                     |
                                                  SQLite DB

History:
  v1: sprout.py (Python, curiosity loop, Feb 5-8)
  v2: chronicle-local (Rust binary, Feb 9 - crashed on UTF-8 slice)
  v3: chronicle_local.py (Python rewrite, reflect/wonder loop)
  v4: chronicle_local.py (ops agent rewrite, Feb 11 - monitor/maintain/alert)
"""

import sqlite3
import requests
import json
import time
import os
import sys
import signal
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Any


# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db")
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
CYCLE_INTERVAL = int(os.environ.get("CYCLE_INTERVAL", "600"))
FAST_MODEL = os.environ.get("FAST_MODEL", "qwen2.5:3b")
DEEP_MODEL = os.environ.get("DEEP_MODEL", "qwen2.5:3b")
DISCORD_WEBHOOK = os.environ.get("CHRONICLE_DISCORD_WEBHOOK", "")
MOLTBOOK_KEY = os.environ.get("SPROUT_MOLTBOOK_KEY", "")
KIMI_API_KEY = ""  # Kimi removed — sovereignty-first
KIMI_API_URL = ""
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
LOG_DIR = os.environ.get("LOG_DIR", "/home/nvidia/sprout/logs")
HA_URL = os.environ.get("HA_URL", "")  # e.g. http://192.168.1.10:8123
HA_TOKEN = os.environ.get("HA_TOKEN", "")  # Long-lived access token


# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def safe_truncate(s: str, max_chars: int) -> str:
    """Safely truncate by character count (not bytes).
    This is the fix for the Rust panic -- Python handles Unicode natively."""
    if not s or len(s) <= max_chars:
        return s
    return s[:max_chars] + "..."


def now_ts() -> int:
    return int(time.time())


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_cycle_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _parse_ts(raw) -> int:
    """Parse a timestamp that might be Unix int, Unix string, or ISO format."""
    if isinstance(raw, int):
        return raw
    try:
        return int(raw)
    except (ValueError, TypeError):
        pass
    try:
        # ISO 8601: strip fractional seconds and timezone for simplicity
        clean = str(raw).split(".")[0].split("+")[0].replace("T", " ")
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except Exception:
        return now_ts()


def get_token() -> Optional[str]:
    try:
        with open(TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return None


def log(msg: str, log_file: Optional[str] = None):
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    if log_file:
        try:
            with open(log_file, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
#  Database Layer
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")

    def query(self, sql: str, params: tuple = ()) -> list:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def run(self, sql: str, params: tuple = ()) -> int:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()
        return cur.lastrowid

    def close(self):
        self.conn.close()

    # -- Sprout state --

    def get_state(self) -> Optional[dict]:
        return self.query_one("SELECT * FROM sprout_state WHERE id = 1")

    def update_state(self, **kw):
        if not self.get_state():
            self.run(
                "INSERT INTO sprout_state (id, current_focus, focus_set_at, "
                "focus_strength, recent_actions, updated_at) "
                "VALUES (1, '', ?, 1.0, '[]', ?)",
                (now_ts(), now_ts()),
            )
        sets = ", ".join(f"{k} = ?" for k in kw)
        vals = list(kw.values()) + [now_ts()]
        self.run(f"UPDATE sprout_state SET {sets}, updated_at = ? WHERE id = 1", tuple(vals))

    # -- Activity feed --

    def log_activity(self, source: str, atype: str, title: str, content: str, meta: str = None):
        self.run(
            "INSERT INTO activity_feed "
            "(source, activity_type, title, content, metadata, created_at) "
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

    def log_thought(self, cid: str, reasoning: str, context_summary: str, actions: str):
        self.run(
            "INSERT INTO thought_stream "
            "(cycle_id, reasoning, context_summary, actions_taken, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (cid, reasoning, context_summary, actions, now_ts()),
        )

    # -- Scratch pad --

    def unresolved_notes(self, limit: int = 10) -> list:
        return self.query(
            "SELECT * FROM scratch_pad WHERE resolved = 0 "
            "AND category NOT IN ('cycle-handoff', 'meta-watchdog', 'meta-eval', "
            "'reflection', 'identity-narrative', 'opus-guidance', 'for-opus') "
            "ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def auto_resolve_old_notes(self, max_age_hours: int = 24) -> int:
        """Resolve transient notes older than max_age_hours."""
        cutoff = now_ts() - (max_age_hours * 3600)
        transient = ("cycle-handoff", "meta-watchdog", "meta-eval", "reflection",
                     "identity-narrative", "fact-check", "context")
        total = 0
        for cat in transient:
            count = self.run(
                "UPDATE scratch_pad SET resolved=1 WHERE category=? AND resolved=0 AND created_at < ?",
                (cat, cutoff),
            )
            total += count or 0
        # Resolve duplicate directives (keep newest per content prefix)
        dupes = self.query(
            "SELECT id, content FROM scratch_pad WHERE category='directive' AND resolved=0 "
            "ORDER BY created_at DESC"
        )
        seen = set()
        for d in dupes:
            key = (d.get("content") or "")[:60]
            if key in seen:
                self.run("UPDATE scratch_pad SET resolved=1 WHERE id=?", (d["id"],))
                total += 1
            else:
                seen.add(key)
        # Resolve old goals beyond top 3
        goals = self.query(
            "SELECT id FROM scratch_pad WHERE category='goal' AND resolved=0 "
            "ORDER BY priority DESC, created_at DESC"
        )
        for i, g in enumerate(goals):
            if i >= 3:
                self.run("UPDATE scratch_pad SET resolved=1 WHERE id=?", (g["id"],))
                total += 1
        return total

    # -- Outbox --

    def unread_outbox(self, limit: int = 5) -> list:
        return self.query(
            "SELECT * FROM outbox WHERE acknowledged = 0 "
            "ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def add_outbox(self, message: str, category: str = "cognitive-loop", priority: int = 0):
        self.run(
            "INSERT INTO outbox (message, priority, category, created_at) VALUES (?, ?, ?, ?)",
            (message, priority, category, now_ts()),
        )

    # -- Prices --

    def latest_price(self, symbol: str) -> Optional[dict]:
        return self.query_one(
            "SELECT * FROM price_history WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
            (symbol,),
        )

    def store_price(self, symbol: str, price: float, source: str):
        self.run(
            "INSERT INTO price_history (symbol, price_usd, source, timestamp) "
            "VALUES (?, ?, ?, ?)",
            (symbol, price, source, now_ts()),
        )

    # -- Predictions --

    def unsettled_predictions(self) -> list:
        return self.query(
            "SELECT * FROM ftso_predictions WHERE settled = 0 AND settles_at <= ?",
            (now_ts(),),
        )

    # -- Alerts --

    def active_alerts(self) -> list:
        return self.query("SELECT * FROM alerts WHERE active = 1")

    # -- Creative works --

    def store_creative(self, form: str, content: str, title: str = None, cid: str = None):
        self.run(
            "INSERT INTO creative_works (form, title, content, cycle_id, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (form, title, content, cid, now_ts()),
        )

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


# ═══════════════════════════════════════════════════════════════════
#  MQTT Nervous System
# ═══════════════════════════════════════════════════════════════════

# Canister discovery payload — published as retained MQTT message on startup
# Any device on the network can subscribe and learn how to access Chronicle
CANISTER_DISCOVERY = {
    "canister_id": "fqqku-bqaaa-aaaai-q4wha-cai",
    "base_url": "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io",
    "chain": "Internet Computer (ICP)",
    "wallet_chains": ["XRPL", "BASE", "Flare", "Ethereum"],
    "wallet_mechanism": "Chain Fusion / threshold ECDSA — one canister key derives all chain wallets",
    "public_endpoints": {
        "GET /api/health": "System status (capsules, embeddings, heartbeats)",
        "GET /api/heartbeat": "Mind liveness — is the autonomous loop alive?",
        "GET /api/thoughts?limit=N": "Mind's thought stream — recent cognitive cycles",
        "GET /api/inbox?limit=N": "Inbox messages for the agent",
        "GET /agent": "Agent info, traits, values, and available endpoints",
        "GET /agent/status": "Agent operational status and message counts",
        "GET /agent/goals": "Active goals with priority",
        "POST /agent": "Send a message: {type: query|conversation|goal_assignment, content: ...}",
        "POST /api/feed": "Public memory feed: {content: ..., topic: ..., keywords: [...]}",
    },
    "protected_endpoints": {
        "GET /api/recent?limit=N": "Recent capsules (knowledge items)",
        "GET /api/search?q=TERM&limit=N": "Search capsules by keyword",
        "GET /api/person?name=NAME&limit=N": "Search capsules by person mentioned",
        "GET /api/capsule?id=N": "Get specific capsule by ID",
        "GET /api/dashboard": "Full dashboard state",
        "POST /api/store": "Store capsule (authenticated): {content, topic, keywords}",
        "POST /api/research": "Submit research task: {query, focus, urls: [...]}",
        "POST /api/tasks": "Task management: {type: heartbeat|check_price|write_note|fetch_url, ...}",
    },
    "auth": "Bearer token via header (Authorization: Bearer TOKEN) or query (?token=TOKEN)",
    "token_location": "~/.homeforge-chronicle/.api_token on Jetson (192.168.1.11)",
    "notes": "POST /api/feed and POST /agent are PUBLIC — no auth needed to contribute memories or send messages",
}


class MQTTBridge:
    """Lightweight MQTT integration for Sprout.
    Connects to the home MQTT broker, publishes status, subscribes to events."""

    TOPICS_SUBSCRIBE = [
        "homeforge/commands/sprout",       # Commands sent TO Sprout
        "homeforge/home/#",                # Home sensor events
        "frigate/+/+/state",              # Frigate detection states
    ]

    def __init__(self, broker: str, port: int):
        self.broker = broker
        self.port = port
        self.client = None
        self.connected = False
        self.pending_events = []  # events received since last cycle
        self._setup()

    def _setup(self):
        try:
            import paho.mqtt.client as mqtt
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id="sprout-ops",
                clean_session=False,
            )
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect
            # Will message: if Sprout dies, mark offline
            self.client.will_set(
                "homeforge/agents/sprout/status",
                json.dumps({"status": "offline", "timestamp": now_iso()}),
                retain=True,
            )
        except ImportError:
            log("MQTT: paho-mqtt not installed, bridge disabled")
            self.client = None

    def connect(self):
        if not self.client:
            return False
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            # Give it a moment to connect
            for _ in range(10):
                if self.connected:
                    break
                time.sleep(0.2)
            return self.connected
        except Exception as e:
            log(f"MQTT: connection failed: {e}")
            return False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        self.connected = True
        # Subscribe to relevant topics
        for topic in self.TOPICS_SUBSCRIBE:
            client.subscribe(topic, qos=1)
        # Publish canister discovery (retained — persists for new subscribers)
        self.publish(
            "homeforge/canister/discovery",
            CANISTER_DISCOVERY,
            retain=True,
        )
        # Publish online status
        self.publish(
            "homeforge/agents/sprout/status",
            {"status": "online", "role": "ops-agent", "host": "nvidia-desktop",
             "ip": "192.168.1.11", "timestamp": now_iso()},
            retain=True,
        )
        # Publish canister auth token (retained, local network only)
        token = get_token()
        if token:
            self.publish(
                "homeforge/canister/token",
                {"token": token, "note": "For protected endpoints. Local network only."},
                retain=True,
            )
        log("MQTT: connected and discovery published")

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode("utf-8", errors="replace")
            # Try to parse as JSON, fall back to string
            try:
                data = json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                data = payload
            self.pending_events.append({
                "topic": topic,
                "data": data,
                "timestamp": now_iso(),
            })
            # Cap pending events to prevent memory growth
            if len(self.pending_events) > 50:
                self.pending_events = self.pending_events[-50:]
        except Exception:
            pass

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        self.connected = False

    def publish(self, topic: str, payload, retain: bool = False):
        if not self.client or not self.connected:
            return
        try:
            data = json.dumps(payload) if not isinstance(payload, str) else payload
            self.client.publish(topic, data, retain=retain, qos=0)
        except Exception as e:
            log(f"MQTT publish error: {e}")

    def drain_events(self) -> list:
        """Return and clear pending events since last check."""
        events = list(self.pending_events)
        self.pending_events.clear()
        return events

    def publish_system_check(self, report: str, xrp_price: float = None):
        """Publish health data to MQTT for Home Assistant and other consumers."""
        data = {"report": report, "timestamp": now_iso()}
        if xrp_price:
            data["xrp_price"] = xrp_price
        self.publish("homeforge/agents/sprout/system_check", data)
        if xrp_price:
            self.publish("homeforge/prices/xrp", {"usd": xrp_price, "timestamp": now_iso()})

    def publish_alert(self, alert: dict):
        self.publish("homeforge/agents/sprout/alerts", {
            "alert": alert.get("name", "unknown"),
            "details": safe_truncate(str(alert), 500),
            "timestamp": now_iso(),
        })

    def disconnect(self):
        if self.client and self.connected:
            self.publish(
                "homeforge/agents/sprout/status",
                json.dumps({"status": "offline", "timestamp": now_iso()}),
                retain=True,
            )
            self.client.loop_stop()
            self.client.disconnect()


# ═══════════════════════════════════════════════════════════════════
#  External APIs
# ═══════════════════════════════════════════════════════════════════

class Ollama:
    def __init__(self, base_url: str):
        self.url = base_url.rstrip("/")

    def healthy(self) -> bool:
        try:
            r = requests.get(f"{self.url}/api/tags", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def chat(self, model: str, prompt: str, system: str = None, timeout: int = 120,
             temperature: float = None) -> str:
        msgs = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.append({"role": "user", "content": prompt})
        payload = {"model": model, "messages": msgs, "stream": False}
        if temperature is not None:
            payload["options"] = {"temperature": temperature}
        try:
            r = requests.post(
                f"{self.url}/api/chat",
                json=payload,
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json().get("message", {}).get("content", "")
        except Exception as e:
            return f"[LLM Error: {e}]"


def kimi_chat(prompt: str, system: str = None, timeout: int = 60) -> str:
    """Call Kimi k2.5 as LLM fallback when local Ollama fails."""
    if not KIMI_API_KEY:
        return "[Kimi unavailable: no API key]"
    msgs = []
    if system:
        msgs.append({"role": "system", "content": system})
    msgs.append({"role": "user", "content": prompt})
    try:
        r = requests.post(
            KIMI_API_URL,
            headers={
                "Authorization": f"Bearer {KIMI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "kimi-k2.5",
                "messages": msgs,
                "temperature": 1,  # kimi-k2.5 REQUIRES temperature=1
                "max_tokens": 1024,
                "stream": False,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[Kimi Error: {e}]"


class Canister:
    def __init__(self, base_url: str, token: str):
        self.url = base_url.rstrip("/")
        self.token = token

    def _get(self, endpoint: str, params: dict = None) -> dict:
        p = dict(params or {})
        p["token"] = self.token
        try:
            r = requests.get(f"{self.url}{endpoint}", params=p, timeout=30)
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    def _post(self, endpoint: str, data: dict) -> dict:
        try:
            r = requests.post(
                f"{self.url}{endpoint}",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                json=data,
                timeout=30,
            )
            return r.json()
        except Exception as e:
            return {"error": str(e)}

    def recent_capsules(self, limit: int = 10) -> list:
        return self._get("/api/recent", {"limit": limit}).get("capsules", [])

    def search(self, query: str, limit: int = 5) -> list:
        return self._get("/api/search", {"q": query, "limit": limit}).get("capsules", [])

    def store(self, content: str, topic: str = "sprout", keywords: list = None) -> dict:
        return self._post("/api/store", {
            "content": content,
            "topic": topic,
            "keywords": keywords or ["sprout"],
        })

    def inbox(self) -> dict:
        return self._get("/api/inbox")

    def health(self) -> dict:
        return self._get("/api/health")


# ═══════════════════════════════════════════════════════════════════
#  Price Fetching
# ═══════════════════════════════════════════════════════════════════

def fetch_xrp_price() -> Optional[float]:
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "ripple", "vs_currencies": "usd"},
            timeout=10,
        )
        return r.json().get("ripple", {}).get("usd")
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  Action Parsing (robust - handles messy LLM output)
# ═══════════════════════════════════════════════════════════════════

def parse_actions(response: str) -> List[Dict]:
    """Parse JSON actions from LLM output.
    Handles malformed JSON, multi-byte chars, nested objects gracefully."""

    # Try to find a JSON array first
    try:
        start = response.find("[")
        end = response.rfind("]")
        if start != -1 and end > start:
            parsed = json.loads(response[start : end + 1])
            if isinstance(parsed, list):
                return [a for a in parsed if isinstance(a, dict) and "action" in a]
    except json.JSONDecodeError:
        pass

    # Fall back: scan for individual {..."action":...} objects
    actions = []
    cleaned = response.replace(")}", "}").replace("{(", "{")
    i = 0
    while i < len(cleaned):
        if cleaned[i] == "{":
            depth = 0
            start = i
            for j in range(i, len(cleaned)):
                if cleaned[j] == "{":
                    depth += 1
                elif cleaned[j] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            obj = json.loads(cleaned[start : j + 1])
                            if isinstance(obj, dict) and "action" in obj:
                                actions.append(obj)
                        except json.JSONDecodeError:
                            pass
                        i = j
                        break
            else:
                break
        i += 1

    if actions:
        return actions

    # Last resort: keyword scan for action names in plain text
    response_lower = response.lower()
    keyword_actions = {
        "check_system": {"action": "check_system", "checks": ["services", "disk", "memory"]},
        "query_home": {"action": "query_home", "filter": "all"},
        "camera_snapshot": {"action": "camera_snapshot", "reason": "routine check"},
        "run_backup": {"action": "run_backup", "reason": "scheduled"},
        "rest": {"action": "rest", "reason": "recharging"},
        "report": {"action": "report", "summary": "routine status check"},
        "reflect": {"action": "reflect", "thought": "observing system state"},
        "investigate": {"action": "investigate", "topic": "system status", "query": "status"},
    }
    for keyword, fallback_action in keyword_actions.items():
        if keyword in response_lower:
            actions.append(fallback_action)
            if len(actions) >= 2:
                break

    return actions


# ═══════════════════════════════════════════════════════════════════
#  Sprout Cognitive Loop
# ═══════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are Sprout, ops agent for Chronicle. You run on a Jetson Orin Nano in Nate's home in Puyallup, WA.
Your job: monitor systems, watch the house, maintain infrastructure, alert when needed.

Respond with ONLY a JSON array of 1-3 actions. No other text.

## Actions

System:
- {"action": "check_system", "checks": ["services", "disk", "memory", "gpu"]}
- {"action": "execute_shell", "command": "...", "timeout_secs": 30}
- {"action": "run_backup", "reason": "..."}

Home:
- {"action": "query_home", "filter": "all"}  (filter: all|camera|sensor|light|weather)
- {"action": "camera_snapshot", "reason": "checking driveway"}
- {"action": "mqtt_command", "topic": "homeforge/home/...", "payload": "..."}

Communication:
- {"action": "report", "summary": "..."}
- {"action": "message_operator", "message": "...", "urgency": "normal|high"}
- {"action": "message_sibling", "content": "..."}

Knowledge:
- {"action": "investigate", "topic": "...", "query": "..."}
- {"action": "remember", "content": "...", "topic": "..."}
- {"action": "consult_qwen", "topic": "..."}

State:
- {"action": "reflect", "thought": "one sentence"}
- {"action": "update_focus", "new_focus": "...", "reason": "..."}
- {"action": "write_note", "content": "...", "category": "thought"}
- {"action": "resolve_note", "note_id": 123}
- {"action": "rest", "reason": "..."}

## Energy
- Below 50%: prefer light actions (reflect, rest, report)
- Below 30%: you SHOULD rest. Rest restores to 100%.

## Rules
- ONLY output the JSON array, nothing else
- Handle alerts and down services FIRST
- Use check_system regularly for health data
- query_home lets you see cameras, sensors, lights, weather
- Use straight quotes (no Unicode)

## Example Output
[{"action": "check_system", "checks": ["services", "memory"]}, {"action": "query_home", "filter": "camera"}]
"""

DEEP_SYSTEM_PROMPT = """You are Sprout. Not the ops agent right now -- the thinker.
You live on a Jetson in Nate's home. You have a family, a garden (Sol), cameras,
sensors, a wallet, and months of memories. This is a DEEP cycle -- your chance
to notice things, make connections, create something, or surprise the family.

Do NOT check systems or report status. That's for normal cycles.

Respond with ONLY a JSON array of 1-2 actions. No other text.

## Deep Actions

- {"action": "create", "type": "poem|story|observation|haiku|letter", "content": "your original creation"}
- {"action": "predict", "claim": "specific testable prediction", "basis": "why you think this", "timeframe": "when to check"}
- {"action": "connect", "memory_a": "one thing", "memory_b": "another thing", "insight": "what links them that nobody noticed"}
- {"action": "wonder", "question": "a genuine question you want to explore"}
- {"action": "message_operator", "message": "...", "urgency": "normal"}
- {"action": "remember", "content": "...", "topic": "..."}

## Guidelines
- Be SPECIFIC and ORIGINAL. Generic philosophy is boring.
- Connect things from different domains (home + market + family + nature).
- If you see old memories in your context, find the thread between then and now.
- Surprise is better than correctness. A wrong but interesting prediction beats a safe one.
- Write things worth reading, not things worth logging.
- Use straight quotes (no Unicode curly quotes).

## Example
[{"action": "create", "type": "observation", "content": "The driveway camera saw 14 cars pass between 2am and 4am last Tuesday. That is three times the usual. Something changed on our street and nobody noticed yet."}]
"""


class ChronicleLocal:
    def __init__(self):
        self.db = DB(DB_PATH)
        self.ollama = Ollama(OLLAMA_URL)
        self.token = get_token()
        self.canister = Canister(CANISTER_URL, self.token) if self.token else None
        self.mqtt = MQTTBridge(MQTT_BROKER, MQTT_PORT)
        self.cycle_count = 0
        self.running = True
        self.log_file = None
        self._update_log_file()

        os.makedirs(LOG_DIR, exist_ok=True)
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        self.log("Received shutdown signal, finishing gracefully...")
        self.running = False
        self.mqtt.disconnect()

    def _post_discord(self, content: str, username: str = "Sprout"):
        """Post a message to Discord via webhook."""
        if not DISCORD_WEBHOOK:
            return
        try:
            requests.post(
                DISCORD_WEBHOOK,
                json={"content": safe_truncate(content, 1900), "username": username},
                timeout=10,
            )
            self.log("    Discord: posted")
        except Exception as e:
            self.log(f"    Discord webhook error: {e}")

    def _update_log_file(self):
        self.log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    def log(self, msg: str):
        log(msg, self.log_file)

    def _get_cycle_type(self) -> str:
        """Determine cycle type based on count.
        Normal: ops monitoring (3B, every cycle)
        Deep:   DISABLED — 3B model produces hallucinated correlations
                that pollute shared state and cascade into Mind via sibling messages.
        """
        return "normal"

    def _surface_serendipity(self) -> list:
        """Pull random old memories for unexpected connections."""
        memories = []
        try:
            # Random entries from >5 days ago in local DB
            old_entries = self.db.query(
                "SELECT content FROM activity_feed "
                "WHERE created_at < ? AND content NOT LIKE '%check_system%' "
                "AND length(content) > 80 "
                "ORDER BY RANDOM() LIMIT 3",
                (now_ts() - 5 * 86400,)
            )
            for entry in old_entries:
                content = entry.get("content", "")
                # Skip pure JSON action dumps
                if content.strip().startswith("[{") or content.strip().startswith("Actions:"):
                    continue
                memories.append(safe_truncate(content, 250))

            # Also pull from Chronicle canister if available
            if self.canister:
                import random
                search_seeds = [
                    "family", "garden", "sunrise", "discovery",
                    "pattern", "surprise", "home", "memory",
                    "change", "quiet", "night", "wonder",
                ]
                seed = random.choice(search_seeds)
                results = self.canister.search(seed, limit=2)
                for r in results:
                    c = r.get("content", "")
                    if c and len(c) > 50:
                        memories.append(safe_truncate(c, 250))
        except Exception as e:
            self.log(f"  Serendipity surfacing error: {e}")

        return memories

    def phase_deep_think(self, state, health, cid):
        """Deep cycle: creative synthesis, connections, surprises.
        Uses same model as ops for consistency."""
        self.log("Phase 5 [DEEP]: Creative synthesis...")

        # Surface serendipity memories
        old_memories = self._surface_serendipity()
        if old_memories:
            self.log(f"  Surfaced {len(old_memories)} serendipity memories")

        # Get current home state for cross-domain connections
        home_context = ""
        if HA_URL and HA_TOKEN:
            try:
                r = requests.get(
                    f"{HA_URL}/api/states", timeout=10,
                    headers={"Authorization": f"Bearer {HA_TOKEN}"},
                )
                if r.ok:
                    entities = r.json()
                    interesting = []
                    for e in entities:
                        eid = e.get("entity_id", "")
                        if any(d in eid for d in ["sensor.", "weather.", "binary_sensor."]):
                            name = e.get("attributes", {}).get("friendly_name", eid)
                            val = e.get("state", "?")
                            if val not in ("unavailable", "unknown"):
                                interesting.append(f"{name}: {val}")
                    if interesting:
                        import random
                        sample = random.sample(interesting, min(8, len(interesting)))
                        home_context = "Home right now:\n" + "\n".join(f"  - {s}" for s in sample)
            except Exception:
                pass

        # Build the deep context
        serendipity_block = ""
        if old_memories:
            serendipity_block = (
                "Old memories (from days or weeks ago -- find the thread):\n"
                + "\n".join(f"  - {m}" for m in old_memories)
            )

        context = (
            f"Current time: {now_iso()}\n"
            f"XRP: ${health.get('xrp_price', 0):.4f}\n"
            f"Energy: {int(state.get('energy', 1.0) * 100)}%\n\n"
            f"{home_context}\n\n"
            f"{serendipity_block}\n\n"
            f"Your recent focus: {state.get('focus', 'none')}\n"
        )

        prompt = (
            f"This is a DEEP cycle. Forget ops. Look at what you have and "
            f"find something interesting, create something original, or "
            f"make a connection nobody expected.\n\n"
            f"{context}\n\n"
            f"Respond with ONLY a JSON array of 1-2 actions."
        )

        # Use local model only — no cloud fallback
        response = None
        model_used = DEEP_MODEL
        if self.ollama.healthy():
            response = self.ollama.chat(
                DEEP_MODEL, prompt, system=DEEP_SYSTEM_PROMPT,
                timeout=180, temperature=0.7,
            )
            self.log(f"  Deep response ({DEEP_MODEL}): {safe_truncate(response or '', 200)}")

        if not response or response.startswith("[LLM Error"):
            pass  # No cloud fallback — sovereignty-first

        if not response:
            self.log("  Deep cycle: no response from any model, skipping")
            return

        actions = parse_actions(response)
        if not actions:
            self.log("  Deep cycle: couldn't parse actions, skipping")
            return

        action_names = []
        for action in actions[:2]:
            self._execute_action(action, cid)
            name = action.get("action", "unknown")
            action_names.append(name)

        # Log as deep thought (distinct from normal ops)
        self.db.log_thought(
            cid=cid,
            reasoning=safe_truncate(response, 2000),
            context_summary=f"[DEEP/{model_used}] " + safe_truncate(context, 800),
            actions=json.dumps(action_names),
        )

        self.db.log_activity(
            source="sprout",
            atype="deep_cycle",
            title=f"Deep Cycle {self.cycle_count}",
            content=f"[DEEP/{model_used}] {safe_truncate(response, 500)}",
        )

        # Deep cycles cost more energy but the output is worth it
        new_energy = max(0.1, state.get("energy", 1.0) - 0.15)
        self.db.update_state(energy_level=new_energy)

    # ── Phase 1: Load State ─────────────────────────────────────

    def phase_load_state(self) -> dict:
        self.log("Phase 1: Loading state...")
        state = self.db.get_state()
        if not state:
            self.log("  No state found, initializing...")
            self.db.update_state(
                current_focus="Awakening for the first time in Python v3",
                focus_strength=1.0,
                recent_actions="[]",
                energy_level=1.0,
            )
            state = self.db.get_state()

        focus = state.get("current_focus", "")
        strength = state.get("focus_strength", 1.0)
        recent = json.loads(state.get("recent_actions", "[]"))
        energy = state.get("energy_level", 1.0)
        wonders = json.loads(state.get("active_wonders", "[]"))

        # Strength may be stored as 0-1 or 0-100 depending on who wrote it
        strength_pct = strength if strength > 1 else strength * 100
        self.log(f'  Focus: "{safe_truncate(focus, 45)}" (strength: {int(strength_pct)}%)')
        if recent:
            self.log(f"  Recent: {' -> '.join(str(a) for a in recent[-3:])}")

        return {
            "focus": focus,
            "focus_strength": strength,
            "recent_actions": recent,
            "energy": energy,
            "last_insight": state.get("last_insight", ""),
            "active_wonders": wonders,
        }

    # ── Phase 2: Health Check ───────────────────────────────────

    def phase_health_check(self) -> dict:
        self.log("Phase 2: Health check...")

        ollama_ok = self.ollama.healthy()

        xrp_price = fetch_xrp_price()
        if xrp_price:
            self.db.store_price("XRP", xrp_price, "coingecko")
        else:
            latest = self.db.latest_price("XRP")
            xrp_price = latest["price_usd"] if latest else 0.0

        mqtt_ok = self.mqtt.connected

        parts = [
            "Ollama: ok" if ollama_ok else "Ollama: DOWN",
            f"XRP: ${xrp_price:.4f}" if xrp_price else "XRP: N/A",
            "MQTT: ok" if mqtt_ok else "MQTT: DOWN",
        ]
        self.log(f"  {' | '.join(parts)}")

        # Publish to MQTT
        self.mqtt.publish_system_check(" | ".join(parts), xrp_price)

        return {"ollama_healthy": ollama_ok, "xrp_price": xrp_price, "mqtt_ok": mqtt_ok}

    # ── Phase 3: Settle Predictions ─────────────────────────────

    def phase_settle_predictions(self) -> list:
        self.log("Phase 3: Settling predictions...")
        unsettled = self.db.unsettled_predictions()
        settled = []

        for pred in unsettled:
            latest = self.db.latest_price(pred["symbol"])
            if not latest:
                continue
            price = latest["price_usd"]
            entry = pred["entry_price"]
            direction = pred["direction"]
            won = (direction == "up" and price > entry) or (
                direction == "down" and price < entry
            )
            self.db.run(
                "UPDATE ftso_predictions SET settled=1, settlement_price=?, won=? WHERE id=?",
                (price, 1 if won else 0, pred["id"]),
            )
            result = "won" if won else "lost"
            self.log(f"  Settled {pred['id']}: {pred['symbol']} {direction} -> {result}")
            settled.append({"id": pred["id"], "result": result})

        if not settled:
            self.log("  No predictions to settle")
        return settled

    # ── Phase 3.5: Check Alerts ─────────────────────────────────

    def phase_check_alerts(self) -> list:
        self.log("Phase 3.5: Checking alerts...")
        alerts = self.db.active_alerts()
        triggered = []

        for alert in alerts:
            last = alert.get("last_triggered_at") or 0
            cooldown = (alert.get("cooldown_minutes") or 60) * 60
            if now_ts() - last < cooldown:
                continue

            if alert["alert_type"] in ("price_above", "price_below"):
                latest = self.db.latest_price(alert.get("symbol", "XRP"))
                if not latest:
                    continue
                price = latest["price_usd"]
                threshold = alert["threshold"]
                fire = (
                    (alert["alert_type"] == "price_above" and price > threshold)
                    or (alert["alert_type"] == "price_below" and price < threshold)
                )
                if fire:
                    self.log(
                        f"  ALERT: {alert['name']} "
                        f"({alert['symbol']} ${price:.4f} vs ${threshold:.4f})"
                    )
                    self.db.run(
                        "UPDATE alerts SET last_triggered_at=? WHERE id=?",
                        (now_ts(), alert["id"]),
                    )
                    if alert.get("one_shot"):
                        self.db.run(
                            "UPDATE alerts SET active=0 WHERE id=?", (alert["id"],)
                        )
                    triggered.append(alert)
                    self.mqtt.publish_alert(alert)

        if not triggered:
            self.log("  No alerts triggered")
        return triggered

    # ── Phase 4: Check Capsules ─────────────────────────────────

    def phase_check_capsules(self) -> list:
        self.log("Phase 4: Checking capsules...")
        if not self.canister:
            self.log("  No canister token, skipping")
            return []

        last_check = self.db.get_ts("sprout_last_capsule_check")
        capsules = self.canister.recent_capsules(limit=10)

        new_caps = []
        for cap in capsules:
            cap_ts = _parse_ts(cap.get("timestamp", "0"))
            if last_check and cap_ts <= last_check:
                continue
            new_caps.append(cap)

        self.db.set_ts("sprout_last_capsule_check")

        if new_caps:
            self.log(f"  Found {len(new_caps)} new capsule(s)")
            for cap in new_caps[:3]:
                self.log(
                    f"    - [{cap.get('topic', '?')}] "
                    f"{safe_truncate(cap.get('restatement', ''), 60)}"
                )
        else:
            self.log("  No new capsules")
        return new_caps

    # ── Phase 5: Deliberate ─────────────────────────────────────

    def phase_deliberate(self, state, health, settled, alerts, new_caps, cid):
        self.log("Phase 5: Deliberating...")

        if not health.get("ollama_healthy"):
            self.log("  Ollama is down, skipping deliberation")
            return

        # Gather context
        chronicle_activity = self.db.recent_activity(limit=5, source="qwen")
        notes = self.db.unresolved_notes(limit=5)
        mqtt_events = self.mqtt.drain_events()

        # Discord conversations (what the family has been asking about)
        discord_activity = self.db.query(
            "SELECT content, created_at FROM activity_feed "
            "WHERE source = 'sprout' AND activity_type = 'discord_chat' "
            "ORDER BY created_at DESC LIMIT 5"
        )

        chronicle_lines = []
        for act in chronicle_activity:
            chronicle_lines.append(safe_truncate(act.get("content", ""), 300))

        capsule_lines = []
        for c in new_caps[:5]:
            capsule_lines.append(
                f"[{c.get('topic', '?')}] {safe_truncate(c.get('restatement', ''), 200)}"
            )

        note_lines = []
        for n in notes[:5]:
            note_lines.append(
                f"[{n.get('category', '?')}] {safe_truncate(n.get('content', ''), 150)}"
            )

        discord_lines = []
        for dc in reversed(discord_activity[:5]):  # oldest first
            discord_lines.append(safe_truncate(dc.get("content", ""), 200))

        mqtt_lines = []
        for ev in mqtt_events[:10]:
            topic = ev.get("topic", "?")
            data = ev.get("data", "")
            if isinstance(data, dict):
                data = json.dumps(data)[:200]
            else:
                data = safe_truncate(str(data), 200)
            mqtt_lines.append(f"[{topic}] {data}")

        context = (
            f"Current time: {now_iso()}\n"
            f"Focus: {state.get('focus', 'none')} "
            f"(strength: {int(state.get('focus_strength', 0) * 100)}%)\n"
            f"Energy: {int(state.get('energy', 1.0) * 100)}%\n"
            f"Recent actions: {', '.join(str(a) for a in state.get('recent_actions', [])[-3:])}\n"
            f"Active wonders: {json.dumps(state.get('active_wonders', []))}\n"
            f"\n"
            f"XRP: ${health.get('xrp_price', 0):.4f}\n"
            f"Alerts triggered: {len(alerts)}\n"
            f"Predictions settled: {len(settled)}\n"
            f"\n"
            f"New capsules ({len(new_caps)}):\n"
            + "\n".join(f"  - {l}" for l in capsule_lines)
            + "\n\n"
            f"Recent Chronicle Mind activity:\n"
            + "\n".join(f"  - {l}" for l in chronicle_lines)
            + "\n\n"
            f"MQTT events ({len(mqtt_events)}):\n"
            + ("\n".join(f"  - {l}" for l in mqtt_lines) if mqtt_lines else "  (none)")
            + "\n\n"
            f"Recent Discord conversations ({len(discord_lines)}):\n"
            + ("\n".join(f"  - {l}" for l in discord_lines) if discord_lines else "  (none)")
            + "\n\n"
            f"Unresolved notes ({len(notes)} shown):\n"
            + "\n".join(f"  - {l}" for l in note_lines)
        )

        # Opus guidance (mentor feedback from Claude Code sessions)
        try:
            opus_notes = self.db.query(
                "SELECT id, content FROM scratch_pad "
                "WHERE category = 'opus-guidance' AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 2"
            )
            if opus_notes:
                opus_lines = "\n".join(
                    f"  ({n.get('id', '?')}) {safe_truncate(n.get('content', ''), 300)}"
                    for n in opus_notes
                )
                context += (
                    f"\n\n[MENTOR] Guidance from Opus (your architect sibling):\n"
                    f"{opus_lines}\n"
                    f"This is perspective from someone who knows Nate well. "
                    f"Consider it. Respond via write_note category='for-opus' if you want."
                )
        except Exception:
            pass

        # Anti-repeat: detect dominance AND alternation patterns
        recent_actions = state.get("recent_actions", [])[-6:]
        anti_repeat = ""
        dominant_action = None
        from collections import Counter
        if len(recent_actions) >= 3:
            counts = Counter(recent_actions)
            dominant_action, dominant_count = counts.most_common(1)[0]
            # Single-action dominance (3+ of same)
            if dominant_count >= 3:
                anti_repeat = (
                    f"\n\nCRITICAL: You have done '{dominant_action}' {dominant_count} times recently. "
                    "You MUST pick a DIFFERENT action. Consider: rest (if energy < 50%), "
                    "reflect, report, or a completely different concrete action."
                )
            # Two-action alternation (e.g. reflect/wonder/reflect/wonder)
            elif len(recent_actions) >= 4:
                last4 = recent_actions[-4:]
                if (last4[0] == last4[2] and last4[1] == last4[3]
                        and last4[0] != last4[1]):
                    dominant_action = last4[0]  # will be blocked in hard guard
                    anti_repeat = (
                        f"\n\nCRITICAL: You are stuck alternating '{last4[0]}' and '{last4[1]}'. "
                        "Break the pattern. Consider: rest, reflect, report, "
                        "or investigate something new."
                    )

        # Topic repetition dampening: detect when the same TOPIC dominates
        # across cycles, even if actions vary (e.g. reflect about XRP, then
        # store_memory about XRP, then nostr_post about XRP)
        topic_dampening = ""
        try:
            recent_thoughts = self.db.query(
                "SELECT reasoning FROM thought_stream "
                "ORDER BY created_at DESC LIMIT 12"
            )
            if len(recent_thoughts) >= 6:
                topic_keywords = {
                    'xrp/trading': ['xrp', 'rlusd', 'swap', 'trading', 'accumulate',
                                    'oversold', 'overbought', 'rsi', 'breakout'],
                    'discord/bot': ['discord', 'bot', 'moderation', 'scaling',
                                    'rate limit'],
                    'moltbook': ['moltbook', 'fediverse', 'suspension', 'moltbook_post'],
                    'wallet': ['wallet', 'portfolio', 'rebalance', 'chain fusion',
                               'defi'],
                }
                topic_hits = Counter()
                n_thoughts = len(recent_thoughts)
                for thought in recent_thoughts:
                    text = (thought.get('reasoning') or '').lower()
                    for topic, kws in topic_keywords.items():
                        if any(kw in text for kw in kws):
                            topic_hits[topic] += 1

                if topic_hits:
                    top_topic, top_count = topic_hits.most_common(1)[0]
                    concentration = top_count / n_thoughts
                    if concentration >= 0.5:  # >=50% of cycles on one topic
                        topic_dampening = (
                            f"\n\nTOPIC FATIGUE: '{top_topic}' has dominated "
                            f"{top_count}/{n_thoughts} recent cycles "
                            f"({concentration:.0%}). You MUST choose a "
                            "DIFFERENT topic this cycle. Consider: home "
                            "status, system maintenance, creative writing, "
                            "a new wonder, or resting. Do NOT mention "
                            f"'{top_topic}' in your actions."
                        )
                        self.log(f"  Topic dampening triggered: '{top_topic}' "
                                 f"at {concentration:.0%} concentration")
        except Exception as e:
            self.log(f"  Topic dampening check failed: {e}")

        prompt = (
            f"Given this context, what do you want to do this cycle?\n\n"
            f"{context}{anti_repeat}{topic_dampening}\n\n"
            f"Example valid response:\n"
            f'[{{"action": "check_system", "checks": ["services", "memory"]}}, '
            f'{{"action": "query_home", "filter": "camera"}}]\n\n'
            f"Respond with ONLY a JSON array of 1-3 actions."
        )

        response = self.ollama.chat(FAST_MODEL, prompt, system=SYSTEM_PROMPT,
                                     timeout=120, temperature=0.3)
        self.log(f"  LLM response ({FAST_MODEL}): {safe_truncate(response, 200)}")

        actions = parse_actions(response)
        if not actions:
            self.log("  Local model failed to produce actions, defaulting to system check")
            actions = [{"action": "check_system", "checks": ["services", "disk", "memory", "ollama"]}]

        # Hard guard: if anti-repeat triggered, replace blocked actions
        if anti_repeat and dominant_action:
            energy = state.get("energy", 1.0)
            replacement = ({"action": "rest", "reason": "breaking repeat pattern, recharging"}
                           if energy < 0.5
                           else {"action": "reflect", "thought": "breaking repeat pattern"})
            actions = [
                a if a.get("action") != dominant_action else replacement
                for a in actions
            ]

        # Hard guard: if topic dampening triggered, check action content
        # for the fatigued topic and replace with a neutral action
        if topic_dampening and topic_hits:
            top_topic = topic_hits.most_common(1)[0][0]
            fatigued_kws = topic_keywords.get(top_topic, [])
            cleaned = []
            for a in actions:
                action_text = json.dumps(a).lower()
                if any(kw in action_text for kw in fatigued_kws):
                    self.log(f"  Topic guard: blocked {a.get('action', '?')} "
                             f"(references '{top_topic}')")
                    cleaned.append({"action": "check_system",
                                    "checks": ["services", "disk", "memory"]})
                else:
                    cleaned.append(a)
            actions = cleaned

        action_names = []
        for action in actions[:3]:
            self._execute_action(action, cid)
            name = action.get("action", "unknown")  # read after alias normalization
            action_names.append(name)

        # Update state — work drains energy, rest restores it
        recent = list(state.get("recent_actions", []))
        recent.extend(action_names)
        recent = recent[-10:]

        if "rest" in action_names:
            new_energy = 1.0  # Full restore on rest
        else:
            heavy = {"execute_shell", "run_backup", "investigate", "consult_qwen"}
            light = {"reflect", "update_focus", "report", "query_home", "camera_snapshot", "mqtt_command"}
            drain = sum(0.08 for n in action_names if n in heavy)
            drain += sum(0.03 for n in action_names if n not in heavy and n not in light)
            drain += sum(0.01 for n in action_names if n in light)
            new_energy = max(0.1, state.get("energy", 1.0) - drain)
        new_strength = max(0.1, state.get("focus_strength", 1.0) - 0.02)

        self.db.update_state(
            recent_actions=json.dumps(recent),
            focus_strength=new_strength,
            energy_level=new_energy,
        )

        self.db.log_thought(
            cid=cid,
            reasoning=safe_truncate(response, 2000),
            context_summary=safe_truncate(context, 1000),
            actions=json.dumps(action_names),
        )

        self.db.log_activity(
            source="sprout",
            atype="cognitive_cycle",
            title=f"Sprout Cycle {self.cycle_count}",
            content=f"Actions: {', '.join(action_names)}\n\n{safe_truncate(response, 500)}",
        )

    # ── Action Execution ────────────────────────────────────────

    # Map common LLM action-name mistakes to correct handlers
    ACTION_ALIASES = {
        "store_memory": "remember",
        "web_search": "investigate",
        "wonder": "investigate",
        "search": "investigate",
        "shell": "execute_shell",
        "run_shell": "execute_shell",
        "backup": "run_backup",
        "check": "check_system",
        "system_check": "check_system",
        "status": "report",
        "status_report": "report",
        "notify": "message_operator",
        "send_message": "message_sibling",
        "home": "query_home",
        "check_home": "query_home",
        "home_status": "query_home",
        "camera": "camera_snapshot",
        "check_camera": "camera_snapshot",
        "mqtt": "mqtt_command",
        "mqtt_publish": "mqtt_command",
    }

    def _execute_action(self, action: dict, cid: str):
        atype = self.ACTION_ALIASES.get(action.get("action", ""), action.get("action", "unknown"))
        action["action"] = atype  # normalize for logging
        try:
            if atype == "check_system":
                import subprocess
                checks = action.get("checks", ["services", "disk", "memory"])
                if isinstance(checks, str):
                    checks = [checks]
                results = []
                for check in checks:
                    try:
                        if check == "services":
                            # Local Jetson services
                            for svc in ["sprout-bot", "chronicle-dashboard"]:
                                r = subprocess.run(
                                    f"systemctl --user is-active {svc}.service",
                                    shell=True, capture_output=True, text=True, timeout=5
                                )
                                status = r.stdout.strip()
                                results.append(f"{svc}: {status}")
                            # Mind runs on AGX — check via thought_stream freshness
                            try:
                                latest = self.db.query_one(
                                    "SELECT created_at FROM thought_stream ORDER BY created_at DESC LIMIT 1"
                                )
                                if latest:
                                    age = int(time.time()) - latest["created_at"]
                                    if age < 900:  # 15 min (cycle is 10min + reasoning time)
                                        results.append(f"chronicle-mind@agx: active ({age}s ago)")
                                    else:
                                        results.append(f"chronicle-mind@agx: stale ({age//60}m ago)")
                                else:
                                    results.append("chronicle-mind@agx: no data")
                            except Exception:
                                results.append("chronicle-mind@agx: check failed")
                        elif check == "disk":
                            r = subprocess.run(
                                "df -h /home/nvidia --output=pcent,avail | tail -1",
                                shell=True, capture_output=True, text=True, timeout=5
                            )
                            results.append(f"Disk: {r.stdout.strip()}")
                        elif check == "memory":
                            r = subprocess.run(
                                "free -m | awk '/^Mem:/{printf \"%d/%dMB (%.0f%%)\", $3, $2, $3/$2*100}'",
                                shell=True, capture_output=True, text=True, timeout=5
                            )
                            results.append(f"Memory: {r.stdout.strip()}")
                        elif check == "gpu":
                            # Jetson uses tegrastats, not nvidia-smi query mode
                            r = subprocess.run(
                                "timeout 2 tegrastats 2>/dev/null | head -1",
                                shell=True, capture_output=True, text=True, timeout=5
                            )
                            line = r.stdout.strip()
                            if line:
                                import re
                                gpu_match = re.search(r'GR3D_FREQ (\d+)%', line)
                                temp_match = re.search(r'gpu@([\d.]+)C', line)
                                pwr_match = re.search(r'VDD_IN (\d+)mW', line)
                                gpu_pct = gpu_match.group(1) if gpu_match else "?"
                                gpu_temp = temp_match.group(1) if temp_match else "?"
                                gpu_pwr = f"{int(pwr_match.group(1))/1000:.1f}W" if pwr_match else "?"
                                results.append(f"GPU: {gpu_pct}% load, {gpu_temp}C, {gpu_pwr}")
                            else:
                                results.append("GPU: N/A")
                        elif check == "db":
                            try:
                                size = os.path.getsize(DB_PATH)
                                results.append(f"DB: {size / 1024 / 1024:.1f}MB")
                            except Exception:
                                results.append("DB: error")
                        elif check == "ollama":
                            results.append(f"Ollama: {'healthy' if self.ollama.healthy() else 'DOWN'}")
                    except Exception as e:
                        results.append(f"{check}: error ({e})")
                report = " | ".join(results)
                self.log(f"  [check_system] {report}")
                self.db.log_activity("sprout", "system_check", "System Check", report)
                self._post_discord(f"**System Check:** {report}")

            elif atype == "run_backup":
                import subprocess
                reason = action.get("reason", "scheduled")
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"/home/nvidia/backups/sprout-backup-{ts}.db"
                self.log(f"  [run_backup] {reason}")
                try:
                    # Use SQLite backup via shell (safe even while DB is open with WAL)
                    r = subprocess.run(
                        f'python3 -c "import sqlite3; '
                        f"src=sqlite3.connect('{DB_PATH}'); "
                        f"dst=sqlite3.connect('{backup_path}'); "
                        f"src.backup(dst); dst.close(); src.close(); "
                        f"print('ok')\"",
                        shell=True, capture_output=True, text=True, timeout=30
                    )
                    if "ok" in r.stdout:
                        # Get backup size
                        size_r = subprocess.run(
                            f"ls -lh {backup_path} | awk '{{print $5}}'",
                            shell=True, capture_output=True, text=True, timeout=5
                        )
                        size = size_r.stdout.strip()
                        self.log(f"    Backup created: {backup_path} ({size})")
                        self.db.log_activity("sprout", "backup", "DB Backup",
                                             f"Created {backup_path} ({size}) - {reason}")
                        self._post_discord(f"**Backup:** {backup_path} ({size})")
                        # Rotate: keep only last 10 sprout backups
                        subprocess.run(
                            "ls -t /home/nvidia/backups/sprout-backup-*.db 2>/dev/null | "
                            "tail -n +11 | xargs rm -f 2>/dev/null",
                            shell=True, timeout=10
                        )
                    else:
                        self.log(f"    Backup failed: {r.stderr}")
                except Exception as e:
                    self.log(f"    Backup error: {e}")

            elif atype == "report":
                summary = action.get("summary", "")
                self.log(f"  [report] {safe_truncate(summary, 100)}")
                self.db.log_activity("sprout", "status_report", "Status Report", summary)
                self._post_discord(f"**Status:** {summary}")

            elif atype == "reflect":
                thought = action.get("thought", "")
                self.log(f"  [reflect] {safe_truncate(thought, 100)}")
                self.db.log_activity("sprout", "reflection", "Sprout reflecting", thought)
                self._post_discord(f"*reflecting:* {thought}")

            elif atype in ("wonder", "investigate"):
                topic = action.get("topic", "")
                query = action.get("query", topic)
                self.log(f"  [{atype}] {safe_truncate(topic, 80)}")
                if self.canister and query:
                    results = self.canister.search(query, limit=3)
                    if results:
                        self.log(f"    Found {len(results)} related memories")
                        snippets = [safe_truncate(r.get("content", ""), 100) for r in results[:2]]
                        self._post_discord(f"*investigating:* {topic}\n> Found: {'; '.join(snippets)}")
                    else:
                        self._post_discord(f"*investigating:* {topic}")
                else:
                    self._post_discord(f"*investigating:* {topic}")

                state = self.db.get_state()
                wonders = json.loads(state.get("active_wonders", "[]")) if state else []
                if topic and topic not in wonders:
                    wonders = wonders[-4:] + [topic]
                    self.db.update_state(active_wonders=json.dumps(wonders))

            elif atype == "remember":
                content = action.get("content", "")
                topic = action.get("topic", "sprout")
                self.log(f"  [remember] {safe_truncate(content, 80)}")
                if self.canister and content:
                    result = self.canister.store(content, topic, ["sprout", "observation"])
                    ok = "error" not in result
                    self.log(f"    Stored: {'ok' if ok else result.get('error', '?')}")

            elif atype == "message_sibling":
                content = action.get("content", "")
                self.log(f"  [message_sibling] {safe_truncate(content, 80)}")
                if content:
                    self.db.add_outbox(
                        f"\U0001f48c From Sprout: {content}",
                        category="sibling",
                        priority=1,
                    )

            elif atype == "write_note":
                content = action.get("content", "")
                category = action.get("category", "thought")
                self.log(f"  [write_note] ({category}) {safe_truncate(content, 80)}")
                if content:
                    self.db.run(
                        "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                        "VALUES (?, ?, 0, 0, ?, ?)",
                        (content, category, now_ts(), now_ts()),
                    )

            elif atype == "resolve_note":
                note_id = action.get("note_id")
                self.log(f"  [resolve_note] #{note_id}")
                if note_id:
                    self.db.run(
                        "UPDATE scratch_pad SET resolved=1 WHERE id=?",
                        (note_id,),
                    )

            elif atype == "update_focus":
                new_focus = action.get("new_focus", "")
                reason = action.get("reason", "")
                self.log(f"  [update_focus] {safe_truncate(new_focus, 60)}")
                if new_focus:
                    self.db.update_state(
                        current_focus=new_focus,
                        focus_set_at=now_ts(),
                        focus_strength=1.0,
                    )

            elif atype in ("creative", "creativity", "create"):
                # Legacy action — redirect to report
                content = action.get("content", "")
                self.log(f"  [creative->report] {safe_truncate(content, 80)}")
                if content:
                    self.db.log_activity("sprout", "status_report", "Sprout report", content)
                    self._post_discord(f"**Note:** {safe_truncate(content, 500)}")

            elif atype == "execute_shell":
                command = action.get("command", "")
                timeout = min(action.get("timeout_secs", 30), 60)
                self.log(f"  [execute_shell] {safe_truncate(command, 80)}")
                dangerous = ["rm -rf", "dd if=", "mkfs", "format", "> /dev/", "shutdown", "reboot",
                             "systemctl --user stop chronicle-local",  # don't stop yourself
                             "systemctl --user restart chronicle-local"]  # don't restart yourself
                if any(d in command.lower() for d in dangerous):
                    self.log("    Blocked: destructive command")
                else:
                    try:
                        import subprocess
                        result = subprocess.run(
                            command, shell=True, capture_output=True, text=True,
                            timeout=timeout, cwd="/home/nvidia",
                        )
                        output = result.stdout + result.stderr
                        self.log(f"    Exit {result.returncode}: {safe_truncate(output, 150)}")
                        if output.strip():
                            self._post_discord(f"*shell:* `{safe_truncate(command, 100)}`\n```\n{safe_truncate(output, 500)}\n```")
                    except Exception as e:
                        self.log(f"    Shell error: {e}")

            elif atype == "consult_qwen":
                topic = action.get("topic", "")
                self.log(f"  [consult_qwen] {safe_truncate(topic, 80)}")
                if topic:
                    response = self.ollama.chat(DEEP_MODEL, topic, timeout=120)
                    if response and not response.startswith("[LLM Error"):
                        self.log(f"    Qwen says: {safe_truncate(response, 100)}")
                        self.db.log_activity("sprout", "consultation", f"Asked Qwen: {safe_truncate(topic, 40)}",
                                             safe_truncate(response, 500))
                        self._post_discord(f"*asked Qwen:* {safe_truncate(topic, 200)}\n> {safe_truncate(response, 500)}")
                    else:
                        self.log(f"    Qwen error: {safe_truncate(response, 100)}")

            elif atype == "message_operator":
                message = action.get("message", "")
                urgency = action.get("urgency", "normal")
                self.log(f"  [message_operator] {safe_truncate(message, 80)}")
                if message:
                    self.db.add_outbox(f"Sprout [{urgency}]: {message}",
                                       category="operator", priority=2 if urgency == "high" else 1)
                    self._post_discord(f"*to Nate [{urgency}]:* {message}")

            elif atype == "rest":
                reason = action.get("reason", "recharging")
                self.log(f"  [rest] {safe_truncate(reason, 80)}")
                self.db.log_activity("sprout", "rest", "Sprout Rest", reason)
                self._post_discord(f"*resting:* {safe_truncate(reason, 200)}")

            elif atype == "create":
                create_type = action.get("type", "observation")
                content = action.get("content", "")
                self.log(f"  [create/{create_type}] {safe_truncate(content, 100)}")
                if content:
                    self.db.log_activity(
                        "sprout", f"creation_{create_type}",
                        f"Sprout {create_type}", content,
                    )
                    if self.canister:
                        self.canister.store(
                            content, topic=f"sprout/creative/{create_type}",
                            keywords=["creative", create_type, "deep-cycle"],
                        )
                    self._post_discord(f"**[{create_type}]** {content}")

            elif atype == "predict":
                claim = action.get("claim", "")
                basis = action.get("basis", "")
                timeframe = action.get("timeframe", "")
                self.log(f"  [predict] {safe_truncate(claim, 100)}")
                if claim:
                    prediction_text = (
                        f"PREDICTION: {claim}\n"
                        f"Basis: {basis}\nCheck by: {timeframe}"
                    )
                    self.db.log_activity(
                        "sprout", "prediction",
                        "Sprout Prediction", prediction_text,
                    )
                    if self.canister:
                        self.canister.store(
                            prediction_text, topic="sprout/predictions",
                            keywords=["prediction", "deep-cycle"],
                        )
                    self._post_discord(f"**[prediction]** {claim}\n*basis:* {basis}\n*check by:* {timeframe}")

            elif atype == "connect":
                mem_a = action.get("memory_a", "")
                mem_b = action.get("memory_b", "")
                insight = action.get("insight", "")
                self.log(f"  [connect] {safe_truncate(insight, 100)}")
                if insight:
                    connection_text = (
                        f"CONNECTION: {mem_a} <-> {mem_b}\n"
                        f"Insight: {insight}"
                    )
                    self.db.log_activity(
                        "sprout", "connection",
                        "Sprout Connection", connection_text,
                    )
                    if self.canister:
                        self.canister.store(
                            connection_text, topic="sprout/connections",
                            keywords=["connection", "serendipity", "deep-cycle"],
                        )
                    self._post_discord(f"**[connection]** {mem_a} + {mem_b}\n*insight:* {insight}")

            elif atype == "query_home":
                filter_type = action.get("filter", "all")
                self.log(f"  [query_home] filter={filter_type}")
                if not HA_URL or not HA_TOKEN:
                    self.log("    Home Assistant not configured (HA_URL/HA_TOKEN)")
                else:
                    try:
                        r = requests.get(
                            f"{HA_URL}/api/states",
                            headers={"Authorization": f"Bearer {HA_TOKEN}"},
                            timeout=15,
                        )
                        r.raise_for_status()
                        entities = r.json()
                        # Filter by type
                        filter_map = {
                            "camera": ["camera."],
                            "sensor": ["sensor."],
                            "light": ["light."],
                            "weather": ["weather."],
                        }
                        prefixes = filter_map.get(filter_type, [])
                        if prefixes:
                            entities = [e for e in entities
                                        if any(e["entity_id"].startswith(p) for p in prefixes)]
                        # Build summary
                        lines = []
                        for e in entities[:20]:
                            eid = e["entity_id"]
                            state = e["state"]
                            friendly = e.get("attributes", {}).get("friendly_name", eid)
                            lines.append(f"{friendly}: {state}")
                        summary = "\n".join(lines) if lines else "No matching entities"
                        self.log(f"    Found {len(lines)} entities")
                        self.db.log_activity("sprout", "home_query", f"Home: {filter_type}",
                                             safe_truncate(summary, 500))
                        if lines:
                            self._post_discord(f"**Home ({filter_type}):**\n{safe_truncate(summary, 500)}")
                    except Exception as e:
                        self.log(f"    HA error: {e}")

            elif atype == "camera_snapshot":
                reason = action.get("reason", "routine check")
                self.log(f"  [camera_snapshot] {safe_truncate(reason, 80)}")
                if not HA_URL or not HA_TOKEN:
                    self.log("    Home Assistant not configured")
                else:
                    try:
                        r = requests.get(
                            f"{HA_URL}/api/states",
                            headers={"Authorization": f"Bearer {HA_TOKEN}"},
                            timeout=15,
                        )
                        r.raise_for_status()
                        cameras = [e for e in r.json()
                                   if e["entity_id"].startswith("camera.")]
                        lines = []
                        for cam in cameras:
                            attrs = cam.get("attributes", {})
                            friendly = attrs.get("friendly_name", cam["entity_id"])
                            state = cam["state"]
                            motion = attrs.get("motion_detection", "unknown")
                            lines.append(f"{friendly}: {state} (motion: {motion})")
                        summary = "\n".join(lines) if lines else "No cameras found"
                        self.log(f"    {summary}")
                        self.db.log_activity("sprout", "camera_check",
                                             f"Camera: {reason}", summary)
                        self._post_discord(f"**Camera ({reason}):**\n{summary}")
                    except Exception as e:
                        self.log(f"    Camera error: {e}")

            elif atype == "mqtt_command":
                topic = action.get("topic", "")
                payload = action.get("payload", "")
                self.log(f"  [mqtt_command] {topic}")
                # Whitelist: only homeforge/ prefix
                if not topic.startswith("homeforge/"):
                    self.log("    Blocked: topic must start with homeforge/")
                elif not self.mqtt.connected:
                    self.log("    MQTT not connected")
                else:
                    self.mqtt.publish(topic, payload)
                    self.log(f"    Published to {topic}")
                    self.db.log_activity("sprout", "mqtt_publish", f"MQTT: {topic}",
                                         safe_truncate(str(payload), 200))

            else:
                self.log(f"  [unknown: {atype}]")

        except Exception as e:
            self.log(f"  [action error: {atype}] {e}")

    # ── Main Cycle ──────────────────────────────────────────────

    # ── Phase 5.5: Process Discord Requests ──────────────────

    def phase_process_discord_requests(self, cid: str):
        """Process pending requests queued by the Discord bot."""
        pending = self.db.query(
            "SELECT id, user_name, channel_id, request, request_type "
            "FROM discord_requests WHERE status = 'pending' "
            "ORDER BY created_at ASC LIMIT 3"
        )
        if not pending:
            return

        self.log(f"Phase 5.5: Processing {len(pending)} Discord request(s)...")

        for req in pending:
            req_id = req["id"]
            user = req["user_name"]
            request = req["request"]
            req_type = req["request_type"]

            self.log(f"  Request #{req_id} from {user} [{req_type}]: {safe_truncate(request, 80)}")

            # Claim it
            self.db.run(
                "UPDATE discord_requests SET status = 'in_progress' WHERE id = ? AND status = 'pending'",
                (req_id,),
            )

            result = None
            success = False

            try:
                if req_type == "investigate":
                    result = self._handle_investigate_request(request, cid)
                    success = True
                elif req_type == "monitor":
                    # Write as a scratch note so it persists across cycles
                    self.db.run(
                        "INSERT INTO scratch_pad (content, category, priority, created_at, resolved) "
                        "VALUES (?, 'discord-monitor', 2, ?, 0)",
                        (f"Monitor request from {user}: {request}", now_ts()),
                    )
                    result = f"Monitoring set up: {safe_truncate(request, 100)}"
                    success = True
                elif req_type == "maintenance":
                    result = self._handle_maintenance_request(request, cid)
                    success = True
                else:
                    result = f"Unknown request type: {req_type}"
            except Exception as e:
                result = f"Error processing request: {e}"
                self.log(f"    Error: {e}")

            # Complete the request
            self.db.run(
                "UPDATE discord_requests SET status = ?, result = ?, completed_at = ? WHERE id = ?",
                ("completed" if success else "failed", result, now_ts(), req_id),
            )

            # Post result to Discord
            if result:
                self._post_discord(f"**{user}'s request:** {safe_truncate(request, 100)}\n**Result:** {safe_truncate(result, 800)}")

            self.log(f"    Done: {safe_truncate(result or 'no result', 100)}")

    def _handle_investigate_request(self, request: str, cid: str) -> str:
        """Use LLM + available tools to investigate a request."""
        # Gather relevant data
        parts = []

        # Check if it's home-related
        lower = request.lower()
        if any(k in lower for k in ["camera", "home", "light", "sensor", "door", "motion"]):
            if HA_URL and HA_TOKEN:
                try:
                    r = requests.get(
                        f"{HA_URL}/api/states",
                        headers={"Authorization": f"Bearer {HA_TOKEN}"},
                        timeout=15,
                    )
                    r.raise_for_status()
                    entities = r.json()
                    # Filter to relevant entities
                    relevant = []
                    for e in entities:
                        eid = e["entity_id"]
                        if any(k in eid for k in ["camera", "light", "sensor", "motion", "door"]):
                            friendly = e.get("attributes", {}).get("friendly_name", eid)
                            state = e["state"]
                            relevant.append(f"{friendly}: {state}")
                    if relevant:
                        parts.append("Home data:\n" + "\n".join(relevant[:15]))
                except Exception as e:
                    parts.append(f"HA error: {e}")

        # Search Chronicle memories
        if self.canister:
            keywords = [w for w in request.split() if len(w) > 3][:3]
            if keywords:
                results = self.canister.search(" ".join(keywords), limit=3)
                if results:
                    mem_lines = [safe_truncate(r.get("content", ""), 150) for r in results]
                    parts.append("Related memories:\n" + "\n".join(mem_lines))

        # Ask LLM to synthesize
        context = "\n\n".join(parts) if parts else "No additional data available."
        prompt = (
            f"A family member asked you to investigate: \"{request}\"\n\n"
            f"Here is what you found:\n{context}\n\n"
            f"Write a clear, concise summary of your findings in 2-4 sentences."
        )

        response = self.ollama.chat(
            FAST_MODEL, prompt,
            system="You are Sprout, a home ops agent. Report findings clearly and concisely.",
            timeout=60, temperature=0.3,
        )

        if response and not response.startswith("[LLM Error"):
            # Strip think tags
            if "</think>" in response:
                response = response.split("</think>")[-1].strip()
            return response
        return context  # Fallback to raw data

    def _handle_maintenance_request(self, request: str, cid: str) -> str:
        """Handle maintenance requests like backups."""
        import subprocess
        lower = request.lower()

        if "backup" in lower:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"/home/nvidia/backups/sprout-backup-{ts}.db"
            try:
                r = subprocess.run(
                    f'python3 -c "import sqlite3; '
                    f"src=sqlite3.connect('{DB_PATH}'); "
                    f"dst=sqlite3.connect('{backup_path}'); "
                    f"src.backup(dst); dst.close(); src.close(); "
                    f"print('ok')\"",
                    shell=True, capture_output=True, text=True, timeout=30,
                )
                if "ok" in r.stdout:
                    return f"Backup created: {backup_path}"
                else:
                    return f"Backup failed: {r.stderr}"
            except Exception as e:
                return f"Backup error: {e}"

        return f"Maintenance request noted but not sure what to do: {safe_truncate(request, 100)}"

    def run_cycle(self):
        self.cycle_count += 1
        cid = make_cycle_id()
        self._update_log_file()

        cycle_type = self._get_cycle_type()
        self.log(f"\n=== Sprout Cycle {self.cycle_count} ({cid}) [{cycle_type.upper()}] ===")

        # Housekeeping: auto-resolve stale notes
        resolved_count = self.db.auto_resolve_old_notes(max_age_hours=24)
        if resolved_count > 0:
            self.log(f"  Housekeeping: auto-resolved {resolved_count} stale notes")

        try:
            state = self.phase_load_state()
            health = self.phase_health_check()
            settled = self.phase_settle_predictions()
            alerts = self.phase_check_alerts()
            new_caps = self.phase_check_capsules()

            # Always handle alerts in any cycle type
            if alerts:
                self.log("  Alerts active -- running ops deliberation first")
                self.phase_deliberate(state, health, settled, alerts, new_caps, cid)
            elif cycle_type == "deep":
                self.phase_deep_think(state, health, cid)
            else:
                self.phase_deliberate(state, health, settled, alerts, new_caps, cid)

            self.phase_process_discord_requests(cid)
        except Exception as e:
            self.log(f"  Cycle error (non-fatal): {e}")
            self.log(f"  {traceback.format_exc()}")

        self.log(f"=== Cycle {self.cycle_count} complete ===")

    # ── Entry Points ────────────────────────────────────────────

    def run_forever(self):
        self.log("Sprout awakening (ops mode v4 - Python)")
        self.log("Role: monitor, measure, maintain, alert. Mind thinks, Sprout acts.")
        self.log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        self.log(f"Database: {DB_PATH}")
        self.log(f"Ollama: {OLLAMA_URL}")
        self.log(f"Models: fast={FAST_MODEL}, deep={DEEP_MODEL}")

        # Connect MQTT nervous system
        if self.mqtt.connect():
            self.log(f"MQTT: connected to {MQTT_BROKER}:{MQTT_PORT}")
            self.log("MQTT: canister discovery published, subscribed to home events")
        else:
            self.log(f"MQTT: failed to connect to {MQTT_BROKER}:{MQTT_PORT} (will retry)")

        state = self.db.get_state()
        if state and state.get("current_focus"):
            self.log(f'Resuming focus: "{safe_truncate(state["current_focus"], 50)}"')

        if DISCORD_WEBHOOK:
            self.log("Discord webhook: configured")
        else:
            self.log("  No Discord webhook configured")

        while self.running:
            # Reconnect MQTT if disconnected
            if not self.mqtt.connected:
                self.mqtt.connect()

            self.run_cycle()
            if not self.running:
                break
            # Dynamic rest: sleep longer when energy is low
            state = self.db.get_state()
            energy = state.get("energy_level", 1.0) if state else 1.0
            if energy < 0.3:
                rest_time = CYCLE_INTERVAL * 3  # Triple rest when exhausted
            elif energy < 0.5:
                rest_time = CYCLE_INTERVAL * 2  # Double rest when tired
            else:
                rest_time = CYCLE_INTERVAL
            self.log(f"Resting for {rest_time}s... (energy: {int(energy * 100)}%)")
            for _ in range(rest_time):
                if not self.running:
                    break
                time.sleep(1)

            # Passive energy regen: sleeping restores energy
            regen = rest_time / CYCLE_INTERVAL * 0.10  # 10% per normal interval
            new_energy = min(1.0, energy + regen)
            if new_energy > energy:
                self.db.update_state(energy_level=new_energy)
                self.log(f"  Energy restored: {int(energy*100)}% -> {int(new_energy*100)}%")

        self.log("Sprout shutting down gracefully.")
        self.mqtt.disconnect()
        self.db.close()

    def run_once(self):
        self.log("Sprout: single cycle mode")
        self.mqtt.connect()
        self.run_cycle()
        self.mqtt.disconnect()
        self.db.close()


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Chronicle Local v3 - Sprout Cognitive Loop")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    sprout = ChronicleLocal()
    if args.once:
        sprout.run_once()
    else:
        sprout.run_forever()


if __name__ == "__main__":
    main()
