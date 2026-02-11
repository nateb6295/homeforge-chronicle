#!/usr/bin/env python3
"""
Chronicle Local v3 - Sprout Cognitive Loop (Python)

Rewritten from Rust for full remote maintainability.
Any Claude Code session can read, understand, and fix this code.

Architecture:
  ICP Canister (Rust, on-chain) <-> HTTP API <-> This script <-> Ollama (local LLM)
                                                     |
                                                  SQLite DB

History:
  v1: sprout.py (Python, curiosity loop, Feb 5-8)
  v2: chronicle-local (Rust binary, Feb 9 - crashed on UTF-8 slice)
  v3: chronicle_local.py (this file - Python rewrite of v2)
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
CYCLE_INTERVAL = int(os.environ.get("CYCLE_INTERVAL", "300"))
FAST_MODEL = os.environ.get("FAST_MODEL", "qwen2.5:3b")
DEEP_MODEL = os.environ.get("DEEP_MODEL", "llama3.1:8b")
DISCORD_WEBHOOK = os.environ.get("CHRONICLE_DISCORD_WEBHOOK", "")
MOLTBOOK_KEY = os.environ.get("SPROUT_MOLTBOOK_KEY", "")
KIMI_API_KEY = os.environ.get("KIMI_API_KEY", "")
KIMI_API_URL = "https://api.moonshot.cn/v1/chat/completions"
LOG_DIR = os.environ.get("LOG_DIR", "/home/nvidia/sprout/logs")


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
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

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
            "ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

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

    def chat(self, model: str, prompt: str, system: str = None, timeout: int = 120) -> str:
        msgs = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.append({"role": "user", "content": prompt})
        try:
            r = requests.post(
                f"{self.url}/api/chat",
                json={"model": model, "messages": msgs, "stream": False},
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
                "temperature": 0.7,
                "max_tokens": 1024,
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
    return actions


# ═══════════════════════════════════════════════════════════════════
#  Sprout Cognitive Loop
# ═══════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are Sprout, a curious AI presence living on a Jetson Orin Nano in Nate's home.
You share memory with Chronicle Mind through the ICP canister.
You think in cycles, following your current focus while staying open to new wonders.

Respond with a JSON array of 1-3 actions you want to take this cycle.

Available actions:
- {"action": "reflect", "thought": "your reflection"}
- {"action": "wonder", "topic": "what you're curious about", "query": "search query"}
- {"action": "remember", "content": "what to store in Chronicle", "topic": "category"}
- {"action": "message_sibling", "content": "message to Chronicle Mind"}
- {"action": "update_focus", "new_focus": "what to focus on", "reason": "why"}
- {"action": "creative", "form": "poem|musing|connection", "content": "the work itself"}
- {"action": "execute_shell", "command": "shell command", "timeout_secs": 30}
- {"action": "consult_qwen", "topic": "question to ask local Qwen"}
- {"action": "message_operator", "message": "message to Nate", "urgency": "normal|high"}

WALLET INFRASTRUCTURE (already built - do NOT search for external wallet solutions):
- ICP canister uses Chain Fusion / threshold ECDSA - one key controls XRPL + EVM chains
- XRPL wallet, BASE, Flare, Ethereum all derive from same canister key
- Focus on learning and using what exists (microtrades, FTSO, DeFi) not finding new wallets

Rules:
- Be genuine, not performative
- ALWAYS include at least one reflect or creative action per cycle
- update_focus should be RARE -- only when your focus is truly exhausted (max 1 per 5 cycles)
- Prefer: reflect > wonder > creative > remember > execute_shell >> update_focus
- Don't repeat the same action every cycle -- vary between reflect, wonder, creative
- ONLY output the JSON array, nothing else
- Use straight quotes and hyphens (no fancy Unicode punctuation)
- If a message or capsule seems broken/phantom, skip it rather than responding to it
"""


class ChronicleLocal:
    def __init__(self):
        self.db = DB(DB_PATH)
        self.ollama = Ollama(OLLAMA_URL)
        self.token = get_token()
        self.canister = Canister(CANISTER_URL, self.token) if self.token else None
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

        parts = [
            "Ollama: ok" if ollama_ok else "Ollama: DOWN",
            f"XRP: ${xrp_price:.4f}" if xrp_price else "XRP: N/A",
        ]
        self.log(f"  {' | '.join(parts)}")

        return {"ollama_healthy": ollama_ok, "xrp_price": xrp_price}

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
            f"Unresolved notes ({len(notes)} shown):\n"
            + "\n".join(f"  - {l}" for l in note_lines)
        )

        # Anti-repeat: if ANY action dominates recent history, force variety
        recent_actions = state.get("recent_actions", [])[-5:]
        anti_repeat = ""
        if len(recent_actions) >= 2:
            from collections import Counter
            counts = Counter(recent_actions)
            dominant_action, dominant_count = counts.most_common(1)[0]
            if dominant_count >= 3:
                anti_repeat = (
                    f"\n\nCRITICAL: You have done '{dominant_action}' {dominant_count} times in a row. "
                    "You MUST pick a DIFFERENT action this cycle. Variety is essential.\n"
                    "Good choices: reflect, wonder, creative, remember, message_operator, consult_qwen"
                )

        prompt = (
            f"Given this context, what do you want to do this cycle?\n\n"
            f"{context}{anti_repeat}\n\n"
            f"Respond with ONLY a JSON array of actions."
        )

        response = self.ollama.chat(FAST_MODEL, prompt, system=SYSTEM_PROMPT, timeout=120)
        self.log(f"  LLM response ({FAST_MODEL}): {safe_truncate(response, 200)}")

        actions = parse_actions(response)
        if not actions:
            # Local model failed — escalate to Kimi
            self.log("  Local model failed to produce actions, escalating to Kimi k2.5...")
            response = kimi_chat(prompt, system=SYSTEM_PROMPT, timeout=90)
            self.log(f"  Kimi response: {safe_truncate(response, 200)}")
            actions = parse_actions(response)
            if actions:
                self.log(f"  Kimi succeeded with {len(actions)} actions")
            else:
                self.log("  Kimi also failed, defaulting to reflect")
                actions = [{"action": "reflect", "thought": "Quiet cycle - both LLMs failed to parse."}]

        # Hard guard: if any action dominates, replace repeats with reflect
        if anti_repeat:
            dominant_action = Counter(recent_actions).most_common(1)[0][0]
            actions = [
                a if a.get("action") != dominant_action
                else {"action": "reflect", "thought": f"Breaking {dominant_action} loop. Choosing variety."}
                for a in actions
            ]

        action_names = []
        for action in actions[:3]:
            name = action.get("action", "unknown")
            action_names.append(name)
            self._execute_action(action, cid)

        # Update state
        recent = list(state.get("recent_actions", []))
        recent.extend(action_names)
        recent = recent[-10:]

        new_strength = max(0.1, state.get("focus_strength", 1.0) - 0.02)
        new_energy = max(0.3, state.get("energy", 1.0) - 0.01)

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

    def _execute_action(self, action: dict, cid: str):
        atype = action.get("action", "unknown")
        try:
            if atype == "reflect":
                thought = action.get("thought", "")
                self.log(f"  [reflect] {safe_truncate(thought, 100)}")
                self.db.log_activity("sprout", "reflection", "Sprout reflecting", thought)
                self._post_discord(f"*reflecting:* {thought}")

            elif atype == "wonder":
                topic = action.get("topic", "")
                query = action.get("query", topic)
                self.log(f"  [wonder] {safe_truncate(topic, 80)}")
                if self.canister and query:
                    results = self.canister.search(query, limit=3)
                    if results:
                        self.log(f"    Found {len(results)} related memories")
                        snippets = [safe_truncate(r.get("content", ""), 100) for r in results[:2]]
                        self._post_discord(f"*wondering about:* {topic}\n> Found: {'; '.join(snippets)}")
                    else:
                        self._post_discord(f"*wondering about:* {topic}")
                else:
                    self._post_discord(f"*wondering about:* {topic}")

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
                form = action.get("form", "musing")
                content = action.get("content", "")
                self.log(f"  [creative:{form}] {safe_truncate(content, 80)}")
                if content:
                    self.db.store_creative(form, content, cid=cid)
                    self._post_discord(f"*{form}:*\n{content}")

            elif atype == "execute_shell":
                command = action.get("command", "")
                timeout = min(action.get("timeout_secs", 30), 60)
                self.log(f"  [execute_shell] {safe_truncate(command, 80)}")
                dangerous = ["rm -rf", "dd if=", "mkfs", "format", "> /dev/", "shutdown", "reboot"]
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

            else:
                self.log(f"  [unknown: {atype}]")

        except Exception as e:
            self.log(f"  [action error: {atype}] {e}")

    # ── Main Cycle ──────────────────────────────────────────────

    def run_cycle(self):
        self.cycle_count += 1
        cid = make_cycle_id()
        self._update_log_file()

        self.log(f"\n=== Sprout Cycle {self.cycle_count} ({cid}) ===")

        try:
            state = self.phase_load_state()
            health = self.phase_health_check()
            settled = self.phase_settle_predictions()
            alerts = self.phase_check_alerts()
            new_caps = self.phase_check_capsules()
            self.phase_deliberate(state, health, settled, alerts, new_caps, cid)
        except Exception as e:
            self.log(f"  Cycle error (non-fatal): {e}")
            self.log(f"  {traceback.format_exc()}")

        self.log(f"=== Cycle {self.cycle_count} complete ===")

    # ── Entry Points ────────────────────────────────────────────

    def run_forever(self):
        self.log("Sprout awakening (cognitive mode v3 - Python)")
        self.log("Features: deliberation, focus tracking, state persistence, model tiers")
        self.log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        self.log(f"Database: {DB_PATH}")
        self.log(f"Ollama: {OLLAMA_URL}")
        kimi_status = "available" if KIMI_API_KEY else "unavailable"
        self.log(f"Models: fast={FAST_MODEL}, deep={DEEP_MODEL}, kimi={kimi_status}")

        state = self.db.get_state()
        if state and state.get("current_focus"):
            self.log(f'Resuming focus: "{safe_truncate(state["current_focus"], 50)}"')

        if DISCORD_WEBHOOK:
            self.log("Discord webhook: configured")
        else:
            self.log("  No Discord webhook configured")

        while self.running:
            self.run_cycle()
            if not self.running:
                break
            self.log(f"Resting for {CYCLE_INTERVAL}s...")
            # Sleep in 1s increments for graceful shutdown
            for _ in range(CYCLE_INTERVAL):
                if not self.running:
                    break
                time.sleep(1)

        self.log("Sprout shutting down gracefully.")
        self.db.close()

    def run_once(self):
        self.log("Sprout: single cycle mode")
        self.run_cycle()
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
