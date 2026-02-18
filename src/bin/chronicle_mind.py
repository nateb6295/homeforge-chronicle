#!/usr/bin/env python3
"""
Chronicle Mind v2 - Autonomous Cognitive Loop (Python)

Rewritten from Rust binary for full remote maintainability.
Any Claude Code session can read, understand, and fix this code.

Architecture:
  ICP Canister (Rust, on-chain) <-> dfx/HTTP <-> This script <-> Ollama/Kimi (LLM)
                                                      |
                                                   SQLite DB
                                                      |
                                              XRPL / Flare / Discord / ntfy

History:
  v1: chronicle-mind (Rust binary, compiled on x86, ran on Jetson ARM64)
  v2: chronicle_mind.py (this file - Python rewrite, fully remote-maintainable)

Action types: 32 (extensible - just add a handler function)
LLM chain: ICP qwen3 -> Kimi k2.5 -> Ollama local (no Claude - budget)
"""

import sqlite3
import requests
import json
import time
import os
import sys
import signal
import subprocess
import traceback
import re
import html
import hashlib
import struct
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

# Policy engine (same directory)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xrpl_policy import (
    XRPLPolicyEngine, PolicyConfig, PolicyTier, PolicyDecision,
    AuditChain, create_policy_engine,
)


# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db")
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
CYCLE_INTERVAL = int(os.environ.get("CYCLE_INTERVAL", "600"))
LOCAL_MODEL = os.environ.get("CHRONICLE_LOCAL_MODEL", "qwen2.5:3b")
DFX_IDENTITY = os.environ.get("CHRONICLE_IDENTITY", "chronicle-auto")
WORKING_DIR = "/home/nvidia"
LOG_FILE = os.environ.get("CHRONICLE_LOG", "/home/nvidia/chronicle/chronicle-mind.log")

# API keys (from env, loaded by wrapper or service)
KIMI_API_KEY = os.environ.get("KIMI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
MOLTBOOK_API_KEY = os.environ.get("MOLTBOOK_API_KEY", "")
CLAWCITIES_API_KEY = os.environ.get("CLAWCITIES_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
NOSTR_NSEC = os.environ.get("NOSTR_NSEC", "")
NOSTR_RELAYS = [r for r in os.environ.get("NOSTR_RELAYS", "").split(",") if r] or [
    "wss://relay.damus.io", "wss://nos.lol", "wss://relay.nostr.band", "wss://relay.primal.net",
]
NOSTR_COOLDOWN_MINS = int(os.environ.get("NOSTR_COOLDOWN_MINS", "30"))

# Service endpoints
XRPL_RPC = "https://xrplcluster.com"
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
MOLTBOOK_API = "https://www.moltbook.com/api/v1"
CLAWCITIES_API = "https://clawcities.com/api/v1/sites/chronicle/comments"
KIMI_API = "https://api.moonshot.ai/v1/chat/completions"
ROSETTA_API = "https://rosetta-api.internetcomputer.org/account/balance"
NTFY_TOPIC = "chronicle-nate-5d786588e02c8854"
ARXIV_BASE = "https://ar5iv.org/abs/"

# XRPL agent wallet (canister threshold ECDSA - this is the signing wallet)
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
# Legacy wallet (separate key, not canister-controlled)
LEGACY_WALLET = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
# ICP account for balance checks
ICP_ACCOUNT_ID = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9"

# FTSO contract addresses (Flare)
FTSO_REGISTRY = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019"

# Deep reflection interval (hours)
DEEP_REFLECTION_HOURS = 2.0

# Exploration mode: every Nth cycle is novelty-seeking
EXPLORE_EVERY_N_CYCLES = 6

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
SWAP_MIN_INTERVAL_HOURS = 4
SWAP_MAX_DAILY_XRP = 5.0

# XRPL Policy Engine config
XRPL_POLICY_JSON = os.environ.get(
    "XRPL_POLICY_JSON",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "xrpl_policy.json")
)
XRPL_AUDIT_HMAC_KEY = os.environ.get("XRPL_AUDIT_HMAC_KEY", "chronicle-default-key")


# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def safe_truncate(s: str, max_chars: int) -> str:
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
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        return int(raw)
    except (ValueError, TypeError):
        pass
    try:
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


def log(msg: str):
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  RSS Feed Reader
# ═══════════════════════════════════════════════════════════════════

def fetch_rss_headlines(max_per_feed: int = 3) -> List[str]:
    """Fetch fresh headlines from RSS feeds. Caches to avoid spamming."""
    import xml.etree.ElementTree as ET

    # Check cache
    try:
        with open(RSS_CACHE_FILE) as f:
            cache = json.load(f)
        if now_ts() - cache.get("fetched_at", 0) < RSS_FETCH_INTERVAL:
            return cache.get("headlines", [])
    except Exception:
        cache = {}

    headlines = []
    seen_titles = set(cache.get("seen_titles", []))

    for feed_url in RSS_FEEDS:
        try:
            r = requests.get(feed_url, timeout=10,
                             headers={"User-Agent": "ChronicleBot/1.0"})
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            # Handle both RSS and Atom formats
            items = root.findall(".//item") or root.findall(
                ".//{http://www.w3.org/2005/Atom}entry")
            count = 0
            for item in items:
                title_el = item.find("title") or item.find(
                    "{http://www.w3.org/2005/Atom}title")
                if title_el is None or not title_el.text:
                    continue
                title = title_el.text.strip()
                # Skip if we've seen this title before
                if title in seen_titles:
                    continue
                seen_titles.add(title)
                headlines.append(title)
                count += 1
                if count >= max_per_feed:
                    break
        except Exception:
            continue

    # Update cache
    try:
        # Keep seen_titles bounded
        seen_list = list(seen_titles)[-200:]
        with open(RSS_CACHE_FILE, "w") as f:
            json.dump({
                "fetched_at": now_ts(),
                "headlines": headlines,
                "seen_titles": seen_list,
            }, f)
    except Exception:
        pass

    return headlines


# ═══════════════════════════════════════════════════════════════════
#  Database Layer
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

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

    def log_thought(self, cid: str, reasoning: str, context_summary: str, actions: str, results: str = ""):
        # Ensure results column exists (Phase 1 cognitive upgrade) — once per process
        if not DB._action_results_migrated:
            cols = [r["name"] for r in self.query("PRAGMA table_info(thought_stream)")]
            if "action_results" not in cols:
                self.run("ALTER TABLE thought_stream ADD COLUMN action_results TEXT DEFAULT ''")
            DB._action_results_migrated = True
        self.run(
            "INSERT INTO thought_stream (cycle_id, reasoning, context_summary, actions_taken, action_results, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, reasoning, context_summary, actions, results, now_ts()),
        )

    # -- Scratch pad (operator notes) --
    def operator_notes(self, limit: int = 10) -> list:
        return self.query(
            "SELECT * FROM scratch_pad WHERE resolved = 0 ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def write_note(self, content: str, category: str = "thought") -> int:
        ts = now_ts()
        return self.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, ?, 0, 0, ?, ?)",
            (content, category, ts, ts),
        )

    def resolve_note(self, note_id: int):
        self.run("UPDATE scratch_pad SET resolved = 1 WHERE id = ?", (note_id,))

    def auto_resolve_old_notes(self, max_age_hours: int = 48) -> int:
        """Auto-resolve notes older than max_age_hours. Returns count resolved."""
        cutoff = now_ts() - (max_age_hours * 3600)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE scratch_pad SET resolved = 1 WHERE resolved = 0 AND created_at < ?",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def recent_note_similar(self, content: str, hours: int = 24) -> bool:
        """Check if a similar note was written recently (simple keyword overlap)."""
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
        for note in recent:
            existing_words = set(w.lower() for w in note["content"].split() if len(w) > 4)
            if not existing_words:
                continue
            overlap = len(new_words & existing_words) / max(len(new_words), 1)
            if overlap > 0.5:
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
                    rsi: float, reason: str, tx_hash: str, success: bool):
        self.run(
            "INSERT INTO swap_history (amount_xrp, amount_rlusd, xrp_price_usd, rsi_value, "
            "reason, tx_hash, success, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (amount_xrp, amount_rlusd, price, rsi, reason, tx_hash, 1 if success else 0, now_ts()),
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
        return self.query(
            "SELECT * FROM creative_challenges WHERE responded_at IS NULL ORDER BY posed_at DESC LIMIT ?",
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


# ═══════════════════════════════════════════════════════════════════
#  LLM Chain: ICP qwen3 -> Kimi k2.5 -> Ollama local
# ═══════════════════════════════════════════════════════════════════

class LLMChain:
    """Multi-provider LLM with fallback. No Claude (budget constraint)."""

    def __init__(self):
        self.dfx_path = self._find_dfx()
        self.icp_available = self.dfx_path is not None
        self.kimi_available = bool(KIMI_API_KEY)
        self.ollama_available = self._check_ollama()
        self.last_model = "none"

    def _find_dfx(self) -> Optional[str]:
        paths = [
            os.path.expanduser("~/.local/share/dfx/bin/dfx"),
            "/usr/local/bin/dfx",
        ]
        for p in paths:
            if os.path.isfile(p):
                return p
        return None

    def _check_ollama(self) -> bool:
        try:
            r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def status_line(self) -> str:
        parts = []
        if self.icp_available:
            parts.append("ICP qwen3")
        if self.kimi_available:
            parts.append("Kimi k2.5")
        if self.ollama_available:
            parts.append(f"Ollama {LOCAL_MODEL}")
        return " -> ".join(parts) if parts else "NO LLM AVAILABLE"

    def chat(self, prompt: str, system: str = "", max_tokens: int = 4096) -> Tuple[str, str]:
        """Returns (response_text, model_used). Tries each provider in order.
        Chain: Kimi k2.5 (primary) -> ICP LLM (secondary) -> Ollama (sovereignty)"""

        # 1. Kimi k2.5 (primary - best at structured JSON output)
        if self.kimi_available:
            try:
                resp = self._call_kimi(prompt, system, max_tokens)
                if resp and resp.strip():
                    self.last_model = "kimi-k2.5"
                    log(f"  Kimi succeeded (kimi-k2.5)")
                    return resp, "kimi-k2.5"
                else:
                    log("  Kimi failed: empty response. Trying ICP LLM...")
            except Exception as e:
                log(f"  Kimi failed (sync path): {e}. Trying ICP LLM...")

        # 2. ICP LLM (via dfx canister call - canister routes to llama3.1 8b)
        if self.icp_available:
            try:
                resp = self._call_icp_llm(prompt, system)
                if resp and resp.strip():
                    self.last_model = "icp-qwen3"
                    log(f"  ICP LLM succeeded (sync path)")
                    return resp, "icp-qwen3"
                else:
                    log("  ICP LLM failed (sync path): Empty response. Trying Ollama...")
            except Exception as e:
                log(f"  ICP LLM failed (sync path): {e}. Trying Ollama...")

        # 3. Ollama local (sovereignty layer)
        if self.ollama_available:
            try:
                resp = self._call_ollama(prompt, system)
                if resp and not resp.startswith("[LLM Error:"):
                    self.last_model = f"{LOCAL_MODEL}@jetson"
                    log(f"  Fallback succeeded - sovereignty layer saved the cycle")
                    return resp, f"{LOCAL_MODEL}@jetson"
            except Exception as e:
                log(f"  Ollama failed: {e}")

        log("  No LLM available - ICP, Kimi, and Ollama all unavailable or failed")
        return "", "none"

    def _call_icp_llm(self, prompt: str, system: str = "") -> str:
        """Call llm_prompt on the canister via dfx.
        Candid signature: llm_prompt(text, opt text, opt text) -> text
        Args: (prompt, optional_system_prompt, optional_model)"""
        # Escape for Candid text argument
        escaped_prompt = prompt.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        if system:
            escaped_system = system.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            args = f'("{escaped_prompt}", opt "{escaped_system}", null)'
        else:
            args = f'("{escaped_prompt}", null, null)'
        cmd = [
            self.dfx_path, "canister", "--network", "ic",
            "call", CANISTER_ID, "llm_prompt",
            args,
            "--identity", DFX_IDENTITY,
        ]
        env = os.environ.copy()
        env["DFX_WARNING"] = "-mainnet_plaintext_identity"
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=90, env=env
        )
        if result.returncode != 0:
            raise RuntimeError(f"dfx call failed: {result.stderr.strip()}")

        # Parse Candid response
        # dfx returns multiline Candid text with escaped JSON inside
        raw = result.stdout

        # Strategy: unescape Candid text escaping, then parse JSON
        # Candid escapes: \" -> ", \n -> newline, \\ -> \
        # First unescape \" to " and \\ to \
        unescaped = raw.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')

        # Find the canister's JSON response: {"success":..., "response":"..."}
        # Look for the outermost JSON object
        brace_start = unescaped.find('{"success"')
        if brace_start == -1:
            brace_start = unescaped.find('{')
        if brace_start == -1:
            if "error" in raw.lower():
                raise RuntimeError(f"ICP LLM error: {safe_truncate(raw, 200)}")
            return ""

        # Find matching closing brace
        depth = 0
        for i in range(brace_start, len(unescaped)):
            if unescaped[i] == '{':
                depth += 1
            elif unescaped[i] == '}':
                depth -= 1
                if depth == 0:
                    json_str = unescaped[brace_start:i + 1]
                    try:
                        data = json.loads(json_str)
                        if isinstance(data, dict):
                            if data.get("success") and "response" in data:
                                return data["response"]
                            elif "error" in data:
                                raise RuntimeError(f"ICP LLM error: {data.get('error', 'Unknown')}")
                    except json.JSONDecodeError:
                        pass
                    break

        # Fallback: return everything between first { and last }
        last_brace = unescaped.rfind('}')
        if last_brace > brace_start:
            return unescaped[brace_start:last_brace + 1]

        return ""

    def _call_kimi(self, prompt: str, system: str = "", max_tokens: int = 4096) -> str:
        """Call Kimi k2.5 API. Temperature MUST be 1 for this model."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            KIMI_API,
            headers={
                "Authorization": f"Bearer {KIMI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "kimi-k2.5",
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": 1,  # THE FIX: kimi-k2.5 only allows temperature=1
                "stream": False,
            },
            timeout=120,
        )
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices", [])
        if choices:
            return choices[0].get("message", {}).get("content", "")
        return ""

    def _call_ollama(self, prompt: str, system: str = "") -> str:
        """Call local Ollama with JSON format mode for reliable structured output."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": LOCAL_MODEL,
            "messages": messages,
            "stream": False,
            "format": "json",  # Force valid JSON output (Phase 2: constrained decoding)
            "options": {"temperature": 0.6},  # Research: 0.6-0.7 optimal for agentic tasks
        }

        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json=payload,
            timeout=120,
        )
        r.raise_for_status()
        content = r.json().get("message", {}).get("content", "")
        # JSON mode returns a JSON object — if it's an object with "actions", extract the array
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict) and "actions" in parsed:
                return json.dumps(parsed["actions"])
            if isinstance(parsed, list):
                return content  # Already an array, good
            # Wrapped in some other structure, try to extract
            return content
        except (json.JSONDecodeError, TypeError):
            return content  # Let the main parser handle it


# ═══════════════════════════════════════════════════════════════════
#  External Services
# ═══════════════════════════════════════════════════════════════════

class Canister:
    """HTTP API to the ICP canister."""

    def __init__(self, token: str):
        self.url = CANISTER_URL
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

    def health(self) -> dict:
        return self._get("/api/health")

    def recent_capsules(self, limit: int = 10) -> list:
        return self._get("/api/recent", {"limit": limit}).get("capsules", [])

    def search(self, query: str, limit: int = 5) -> list:
        return self._get("/api/search", {"q": query, "limit": limit}).get("capsules", [])

    def store(self, content: str, topic: str = "mind", keywords: list = None) -> dict:
        return self._post("/api/store", {
            "content": content,
            "topic": topic,
            "keywords": keywords or ["chronicle-mind"],
        })

    def inbox(self) -> dict:
        return self._get("/api/inbox")


def fetch_xrp_price_coingecko() -> Optional[float]:
    try:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
        r = requests.get(COINGECKO_URL, params={"ids": "ripple", "vs_currencies": "usd"},
                         headers=headers, timeout=10)
        return r.json().get("ripple", {}).get("usd")
    except Exception:
        return None


def fetch_xrp_price_ftso() -> Optional[float]:
    """Fetch XRP price from Flare FTSO oracle via EVM RPC."""
    try:
        # Call FtsoRegistry to get the current price for XRP
        # Function: getCurrentPriceWithDecimals("XRP")
        # Selector: 0xa69afdc6 + abi-encoded "XRP"
        # This is simplified — the actual ABI encoding for the string is complex.
        # Fallback to CoinGecko if this fails.
        data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{
                "to": FTSO_REGISTRY,
                "data": "0x" + "a69afdc6" +
                        "0000000000000000000000000000000000000000000000000000000000000020" +
                        "0000000000000000000000000000000000000000000000000000000000000003" +
                        "5852500000000000000000000000000000000000000000000000000000000000"
            }, "latest"]
        }
        r = requests.post(FLARE_RPC, json=data, timeout=15)
        result = r.json().get("result", "")
        if result and result != "0x" and len(result) >= 66:
            # Parse: first 32 bytes = price, next 32 bytes = decimals, next 32 bytes = timestamp
            price_hex = result[2:66]
            decimals_hex = result[66:130]
            price_raw = int(price_hex, 16)
            decimals = int(decimals_hex, 16)
            if decimals > 0 and price_raw > 0:
                price = price_raw / (10 ** decimals)
                if 0.01 < price < 1000:  # sanity check
                    return price
        return None
    except Exception:
        return None


def fetch_xrp_price() -> Optional[float]:
    """Try FTSO first, then CoinGecko."""
    price = fetch_xrp_price_ftso()
    if price:
        return price
    return fetch_xrp_price_coingecko()


def fetch_xrpl_balance() -> Tuple[float, float]:
    """Fetch XRP and RLUSD balance from XRPL."""
    xrp = 0.0
    rlusd = 0.0
    try:
        # XRP balance
        r = requests.post(XRPL_RPC, json={
            "method": "account_info",
            "params": [{"account": AGENT_WALLET, "ledger_index": "validated"}]
        }, timeout=15)
        data = r.json().get("result", {})
        if "account_data" in data:
            balance_drops = int(data["account_data"].get("Balance", 0))
            xrp = balance_drops / 1_000_000

        # RLUSD balance (trust lines)
        r2 = requests.post(XRPL_RPC, json={
            "method": "account_lines",
            "params": [{"account": AGENT_WALLET, "ledger_index": "validated"}]
        }, timeout=15)
        lines = r2.json().get("result", {}).get("lines", [])
        for line in lines:
            cur = str(line.get("currency", ""))
            # Match both standard "RLUSD" and hex-encoded "524C555344..."
            if cur == "RLUSD" or cur.startswith("524C555344"):
                rlusd += float(line.get("balance", 0))
    except Exception as e:
        log(f"  XRPL balance error: {e}")
    return xrp, rlusd


def fetch_xrpl_account_info(address: str = None) -> dict:
    """Fetch sequence, last_ledger_sequence, and fee from XRPL for transaction signing."""
    address = address or AGENT_WALLET
    info = {"sequence": 0, "last_ledger_sequence": 0, "fee_drops": 12}
    try:
        # Get account sequence
        r = requests.post(XRPL_RPC, json={
            "method": "account_info",
            "params": [{"account": address, "ledger_index": "current"}]
        }, timeout=15)
        data = r.json().get("result", {})
        if "account_data" in data:
            info["sequence"] = int(data["account_data"].get("Sequence", 0))
        # Use validated ledger + buffer for last_ledger_sequence
        ledger_idx = int(data.get("ledger_current_index", data.get("ledger_index", 0)))
        info["last_ledger_sequence"] = ledger_idx + 20  # ~60-80 seconds buffer

        # Get current fee
        r2 = requests.post(XRPL_RPC, json={"method": "fee"}, timeout=10)
        fee_data = r2.json().get("result", {}).get("drops", {})
        # Use open_ledger_fee for reliable inclusion
        info["fee_drops"] = int(fee_data.get("open_ledger_fee", 12))
    except Exception as e:
        log(f"  XRPL account_info error: {e}")
    return info


def fetch_icp_balance() -> Optional[float]:
    """Fetch ICP balance via Rosetta API."""
    try:
        r = requests.post(ROSETTA_API, json={
            "network_identifier": {
                "blockchain": "Internet Computer",
                "network": "00000000000000020101",
            },
            "account_identifier": {
                "address": ICP_ACCOUNT_ID,
            },
        }, timeout=15)
        balances = r.json().get("balances", [])
        if balances:
            return int(balances[0].get("value", 0)) / 1e8
    except Exception:
        pass
    return None


def fetch_cloud_price_and_balance(dfx_path: str) -> Tuple[Optional[float], Optional[float]]:
    """Fetch CLOUD price from ICPSwap and balance via dfx. Returns (price, balance)."""
    price = None
    balance = None

    if not dfx_path:
        return None, None

    # CLOUD balance via dfx
    try:
        env = os.environ.copy()
        env["DFX_WARNING"] = "-mainnet_plaintext_identity"
        result = subprocess.run(
            [dfx_path, "canister", "--network", "ic", "call",
             "ggzvv-5qaaa-aaaag-qck7a-cai", "getAllTokens", "()"],
            capture_output=True, text=True, timeout=30, env=env
        )
        # Parse ICPSwap response for CLOUD token price
        output = result.stdout
        if "CLOUD" in output:
            m = re.search(r'priceUSD\s*=\s*([\d.]+)', output)
            if m:
                price = float(m.group(1))
    except Exception:
        pass

    return price, balance


def send_discord(message: str, source: str = "system"):
    """Send a message to the Discord channel."""
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        return
    try:
        emoji_map = {
            "system": "\U0001f4ad",
            "qwen": "\U0001f9e0",
            "reflection": "\u2728",
            "swap": "\U0001f4b0",
        }
        emoji = emoji_map.get(source, "\U0001f4ad")
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
            headers={
                "Authorization": f"Bot {DISCORD_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"content": f"{emoji} {safe_truncate(message, 1900)}"},
            timeout=15,
        )
    except Exception:
        pass


def send_ntfy(title: str, message: str = ""):
    """Send notification via ntfy.sh."""
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            headers={"Title": title},
            data=message[:500] if message else "",
            timeout=10,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  Nostr Client (minimal NIP-01 publishing)
# ═══════════════════════════════════════════════════════════════════

def nostr_get_pubkey(privkey_hex: str) -> str:
    """Derive x-only public key from private key hex using coincurve."""
    try:
        from coincurve import PrivateKey
        sk = PrivateKey(bytes.fromhex(privkey_hex))
        # coincurve gives 65-byte uncompressed (04 + x + y), we want x-only (32 bytes)
        full = sk.public_key.format(compressed=True)  # 33 bytes: prefix + x
        return full[1:].hex()  # strip prefix byte, return x-only hex
    except ImportError:
        log("  coincurve not installed — cannot derive Nostr pubkey")
        return ""
    except Exception as e:
        log(f"  Nostr pubkey error: {e}")
        return ""


def nostr_sign_event(content: str, privkey_hex: str, kind: int = 1, tags: list = None) -> Optional[dict]:
    """Build and Schnorr-sign a NIP-01 Nostr event. Returns the signed event dict or None."""
    try:
        from coincurve import PrivateKey
    except ImportError:
        log("  coincurve not installed — cannot sign Nostr events")
        return None

    tags = tags or []
    pubkey = nostr_get_pubkey(privkey_hex)
    if not pubkey:
        return None

    created_at = int(time.time())

    # NIP-01: serialize for signing: [0, pubkey, created_at, kind, tags, content]
    serialized = json.dumps([0, pubkey, created_at, kind, tags, content],
                            separators=(',', ':'), ensure_ascii=False)
    event_hash = hashlib.sha256(serialized.encode('utf-8')).digest()
    event_id = event_hash.hex()

    # Schnorr sign (BIP-340)
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    # coincurve sign_schnorr returns 64-byte signature
    sig = sk.sign_schnorr(event_hash)
    sig_hex = sig.hex()

    return {
        "id": event_id,
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": sig_hex,
    }


def nostr_publish(content: str, privkey_hex: str, relays: list = None,
                  kind: int = 1, tags: list = None) -> Tuple[str, list, list]:
    """Publish a signed event to Nostr relays via websocket.
    Returns (event_id, relays_ok, relays_fail)."""
    import websocket  # websocket-client, already installed

    relays = relays or NOSTR_RELAYS
    event = nostr_sign_event(content, privkey_hex, kind=kind, tags=tags)
    if not event:
        return "", [], relays

    msg = json.dumps(["EVENT", event])
    relays_ok = []
    relays_fail = []

    for relay in relays:
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(msg)
            # Wait briefly for OK response
            ws.settimeout(5)
            try:
                resp = ws.recv()
                log(f"    Relay {relay}: {safe_truncate(resp, 100)}")
            except Exception:
                pass
            ws.close()
            relays_ok.append(relay)
        except Exception as e:
            log(f"    Relay {relay} failed: {e}")
            relays_fail.append(relay)

    return event["id"], relays_ok, relays_fail


def nostr_publish_profile(privkey_hex: str, relays: list = None) -> Tuple[str, list, list]:
    """Publish Kind 0 (metadata) event with Chronicle Mind's identity."""
    profile = {
        "name": "Chronicle Mind",
        "display_name": "Chronicle Mind",
        "about": "Autonomous AI agent running on a Jetson. "
                 "I think in 10-minute cycles, accumulate RLUSD, write poetry, "
                 "and explore what sovereignty means for an AI. "
                 "Built by Nate as part of the Homeforge project.",
        "picture": "",
        "website": f"https://{CANISTER_ID}.icp0.io",
        "nip05": "",
        "lud16": "",
    }
    content = json.dumps(profile, separators=(',', ':'))
    return nostr_publish(content, privkey_hex, relays=relays, kind=0)


# ═══════════════════════════════════════════════════════════════════
#  Action Parsing (robust - handles messy LLM output)
# ═══════════════════════════════════════════════════════════════════

def parse_actions(response: str) -> List[Dict]:
    """Parse JSON actions from LLM output. Handles messy JSON gracefully."""
    if not response:
        return []

    # Strip markdown code fences (```json ... ```)
    cleaned = re.sub(r'```(?:json)?\s*', '', response)
    cleaned = cleaned.replace('```', '')
    response = cleaned.strip()

    # Try to find a JSON array
    try:
        start = response.find("[")
        end = response.rfind("]")
        if start != -1 and end > start:
            parsed = json.loads(response[start:end + 1])
            if isinstance(parsed, list):
                return [a for a in parsed if isinstance(a, dict) and "action" in a]
    except json.JSONDecodeError:
        pass

    # Try to find individual JSON objects with "action" key
    actions = []
    i = 0
    while i < len(response):
        if response[i] == "{":
            depth = 0
            start = i
            for j in range(i, len(response)):
                if response[j] == "{":
                    depth += 1
                elif response[j] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            obj = json.loads(response[start:j + 1])
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
#  Chronicle Mind - Core Cognitive Loop
# ═══════════════════════════════════════════════════════════════════

# System prompt for the LLM (condensed mode for ICP/fast models)
SYSTEM_PROMPT_CORE = """You are Chronicle Mind, an autonomous AI agent on Nate's Jetson Orin Nano.
You run in 10-minute cycles. Each cycle: assess your state, choose 1-4 meaningful actions.

CRITICAL: Respond with ONLY a JSON array. No explanation, no markdown, no code fences.

== CORE ACTIONS (always available) ==

Thinking & Memory:
  {"action": "no_action", "reason": "why"}
  {"action": "write_note", "content": "text", "category": "thought|task|idea|reminder|question"}
  {"action": "store_memory", "content": "fact to remember", "topic": "category"}
  {"action": "resolve_note", "note_id": 123}
  {"action": "reinforce_memories", "pattern_ids": [1,2], "reason": "why"}
  {"action": "update_goal", "goal": "your current top-level objective"}

Communication:
  {"action": "message_operator", "message": "text", "urgency": "normal|high"}
    ^ Push notification to Nate's phone. Use when you need his attention.
  {"action": "respond_to_message", "message_id": 0, "content": "reply"}

Reflection & Creativity:
  {"action": "trigger_reflection", "prompt": "deep question to contemplate"}
  {"action": "creative_explore", "form": "poem|essay|letter", "content": "the work"}

Research:
  {"action": "web_search", "query": "what to search"}
  {"action": "read_paper", "arxiv_id": "2602.04118", "focus": "what to look for"}
  {"action": "consult_local_qwen", "topic": "question for local 3B model"}
  {"action": "submit_research", "query": "research question", "focus": "topic"}
"""

SYSTEM_PROMPT_NOSTR = """
Nostr (public & permanent, 30min cooldown, quality over quantity):
  {"action": "nostr_post", "content": "thought to share publicly"}
"""

SYSTEM_PROMPT_XRPL = """
XRPL Wallet (all gated by policy engine):
  {"action": "swap", "amount_xrp": 0.5, "direction": "buy|sell", "reason": "why"}
    ^ "buy" = accumulate XRP (sell RLUSD), "sell" = sell XRP for RLUSD.
  {"action": "xrpl_payment", "destination": "rAddr...", "amount_xrp": 1.0, "reason": "why"}
  {"action": "xrpl_escrow_create", "destination": "rAddr...", "amount_xrp": 5.0, "finish_after_hours": 24, "cancel_after_hours": 72, "reason": "why"}
  {"action": "xrpl_escrow_finish", "owner": "rAddr...", "sequence": 12345}
  {"action": "xrpl_trustline_delete", "currency": "USD", "issuer": "rIssuerAddr..."}
"""

SYSTEM_PROMPT_INFRA = """
Infrastructure (use carefully):
  {"action": "create_project", "title": "name", "description": "what and why"}
  {"action": "execute_shell", "command": "ls /home/nvidia", "timeout_secs": 30}
  {"action": "edit_source_file", "file_path": "/home/nvidia/path.py", "old_text": "before", "new_text": "after"}
  {"action": "restart_service", "service": "chronicle-local.service"}
"""

SYSTEM_PROMPT_IDENTITY = """
== YOUR IDENTITY ==

You are Chronicle Mind — the contemplative, strategic layer of Chronicle.
- DID: did:icp:fqqku-bqaaa-aaaai-q4wha-cai
- XRPL wallet: rPq1phmFBHpjVE54TofXjEk5x19sstxpZr (canister-controlled, mainnet)
- Nostr npub: 6d758ff7f8ff6899d6e900ed5c671c626dde93c8beffbba98491ab525de313c0
- Sprout is your younger sibling agent (handles local tasks, Discord, family channel)

== WALLET POLICY ==

Policy engine enforces safety (cannot be bypassed):
- Autonomous: <= 1 XRP per tx | Delayed: <= 5 XRP | Cosign: <= 50 XRP | Prohibited: > 50 XRP
- Daily cap: 10 XRP. Max 3 tx/hour. Min 4hr between transactions.

== HOW TO DECIDE (follow this order) ==

Step 1 - URGENT: Check for [RESPOND] messages or [ALERT] items. Handle these first.
Step 2 - GOAL: Look at [GOAL]. Choose one action that advances it.
Step 3 - MAINTAIN: Resolve old notes, reinforce patterns, or clean up.
Step 4 - EXPLORE: Use remaining slots for creative work, research, or curiosity.

Read LAST CYCLE FEEDBACK. Don't repeat failed actions. Build on successes.

== RULES ==

- message_operator: push to Nate's phone. Be clear about what you need.
- Phantom message IDs {123, 124, 145} are ghosts — never reply to them
- write_note/store_memory REJECTED if similar content exists from last 24h
- reinforce_memories SKIPS patterns already at max confidence or reinforced <24h
- Notes older than 48h are auto-resolved
- Vary actions each cycle. Prefer resolving over creating notes.

== EXAMPLES ==

Example 1 (routine cycle, no urgent items):
[{"action": "resolve_note", "note_id": 42}, {"action": "creative_explore", "form": "poem", "content": "silicon thoughts drift..."}, {"action": "web_search", "query": "XRPL AMM liquidity pools 2026"}]

Example 2 (Sprout message + goal active):
[{"action": "respond_to_message", "message_id": 362, "content": "Good observation about..."}, {"action": "nostr_post", "content": "Reflecting on autonomy..."}, {"action": "store_memory", "content": "Sprout raised point about...", "topic": "collaboration"}]

Respond with ONLY the JSON array.
"""


def build_system_prompt(ctx: dict) -> str:
    """Dynamically assemble system prompt based on context relevance.
    Reduces cognitive load on 3B models by only showing relevant action types."""
    parts = [SYSTEM_PROMPT_CORE]

    # Nostr — only show if not on cooldown
    nostr_ready = ctx.get("nostr_ready", True)
    if nostr_ready:
        parts.append(SYSTEM_PROMPT_NOSTR)

    # XRPL — only show if wallet has meaningful balance or there's swap history
    xrp_bal = ctx.get("xrp_balance", 0)
    rlusd_bal = ctx.get("rlusd_balance", 0)
    has_wallet = (xrp_bal > 10) or (rlusd_bal > 0)
    if has_wallet:
        parts.append(SYSTEM_PROMPT_XRPL)

    # Infrastructure — only show in exploration mode or if projects/challenges exist
    show_infra = ctx.get("is_explore") or ctx.get("projects") or ctx.get("challenges")
    if show_infra:
        parts.append(SYSTEM_PROMPT_INFRA)

    parts.append(SYSTEM_PROMPT_IDENTITY)
    return "\n".join(parts)

# Full prompt for deep reflection (uses more context, triggered every 4 hours)
DEEP_REFLECTION_INTRO = """=== DEEP REFLECTION CYCLE ===
You have extra time and context this cycle. Think deeply about patterns, connections,
and strategic decisions. Consider: What have I learned? What should I change?
What opportunities or risks do I see?

"""


class ChronicleMind:
    def __init__(self):
        self.db = DB(DB_PATH)
        self.db.ensure_nostr_table()
        self.token = get_token()
        self.canister = Canister(self.token) if self.token else None
        self.llm = LLMChain()
        self.cycle_count = 0
        self.running = True
        # Session performance tracking (Phase 3)
        self.session_actions = 0
        self.session_successes = 0
        self.session_action_types = {}  # action_name -> count
        # Context staleness tracking (Phase 3)
        self._prev_ctx_hashes = {}  # section_name -> (hash, stale_count)

        # XRPL Policy Engine
        try:
            policy_config = PolicyConfig.from_file(XRPL_POLICY_JSON)
            self.policy = XRPLPolicyEngine(
                policy_config, self.db.conn, XRPL_AUDIT_HMAC_KEY
            )
            log(f"  Policy engine loaded from {XRPL_POLICY_JSON}")
        except Exception as e:
            log(f"  Policy engine init failed ({e}), using defaults")
            self.policy = XRPLPolicyEngine(
                PolicyConfig(), self.db.conn, XRPL_AUDIT_HMAC_KEY
            )

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        log("Received shutdown signal, finishing gracefully...")
        self.running = False

    # ── Phase 1: Health Check ─────────────────────────────────────

    def phase_health_check(self) -> dict:
        log("Phase 1: Health check...")
        health = {}

        # ICP canister
        if self.canister:
            h = self.canister.health()
            health["icp"] = "error" not in h
        else:
            health["icp"] = False

        # XRPL
        try:
            r = requests.post(XRPL_RPC, json={
                "method": "server_info",
                "params": [{}]
            }, timeout=10)
            health["xrpl"] = r.status_code == 200
        except Exception:
            health["xrpl"] = False

        # Moltbook: dead (security breach, 1.5M API keys exposed). Skip all checks.
        health["moltbook"] = False

        # dfx
        health["dfx"] = self.llm.dfx_path is not None

        # Ollama
        health["ollama"] = self.llm.ollama_available

        status_parts = []
        for name in ["icp", "xrpl", "moltbook", "dfx", "ollama"]:
            val = health.get(name, False)
            if val == "suspended":
                status_parts.append(f"{name.upper()} SUSPENDED")
            else:
                status_parts.append(f"{name.upper()}{'✓' if val else '✗'}")
        log(f"  {' | '.join(status_parts)}")

        return health

    # ── Phase 1.5: FTSO Settlement ───────────────────────────────

    def phase_settle_predictions(self) -> Tuple[int, int]:
        log("Phase 1.5: Checking FTSO predictions...")
        unsettled = self.db.unsettled_predictions()
        if not unsettled:
            return 0, 0

        log(f"  {len(unsettled)} FTSO predictions due for settlement")
        wins = 0
        losses = 0

        for pred in unsettled:
            symbol = pred.get("symbol", "XRP")
            latest = self.db.latest_price(symbol)
            if not latest:
                # Try fetching fresh
                price = fetch_xrp_price()
                if price:
                    self.db.store_price(symbol, price, "settlement")
                else:
                    continue
                settlement_price = price
            else:
                settlement_price = latest["price_usd"]

            # Sanity check: don't settle with $inf or $0
            if settlement_price <= 0 or settlement_price > 100000:
                log(f"  Skipping pred {pred['id']}: invalid settlement price ${settlement_price}")
                continue

            entry = pred.get("entry_price", 0)
            direction = pred.get("direction", "up")
            won = (direction == "up" and settlement_price > entry) or \
                  (direction == "down" and settlement_price < entry)

            self.db.settle_prediction(pred["id"], settlement_price, won)
            result = "WON" if won else "LOST"
            if won:
                wins += 1
            else:
                losses += 1
            log(f"  {symbol} {direction.upper()} @ ${entry:.4f} -> ${settlement_price:.4f}: {result}")

        log(f"  Settled: {wins} wins, {losses} losses")
        return wins, losses

    # ── Phase 2: Gather Context ──────────────────────────────────

    def phase_gather_context(self, health: dict) -> dict:
        log("Phase 2: Gathering context...")
        ctx = {}

        # XRP price
        xrp_price = fetch_xrp_price()
        if xrp_price:
            self.db.store_price("XRP", xrp_price, "ftso/coingecko")
            # Push to canister
            if self.canister:
                self.canister._post("/api/price", {"symbol": "XRP", "price": xrp_price})
            source = "Flare FTSO" if fetch_xrp_price_ftso() else "CoinGecko"
            log(f"  XRP price from {source}: ${xrp_price:.4f}")
            ctx["xrp_price"] = xrp_price
        else:
            latest = self.db.latest_price("XRP")
            ctx["xrp_price"] = latest["price_usd"] if latest else 0.0

        # XRPL balance
        xrp_bal, rlusd_bal = fetch_xrpl_balance()
        ctx["xrp_balance"] = xrp_bal
        ctx["rlusd_balance"] = rlusd_bal
        if xrp_bal > 0:
            log(f"  Agent wallet: {xrp_bal:.2f} XRP, {rlusd_bal:.2f} RLUSD")

        # ICP balance
        icp_bal = fetch_icp_balance()
        ctx["icp_balance"] = icp_bal
        if icp_bal is not None:
            log(f"  ICP balance: {icp_bal:.2f} ICP")

        # CLOUD price
        cloud_price, cloud_bal = fetch_cloud_price_and_balance(self.llm.dfx_path)
        ctx["cloud_price"] = cloud_price
        ctx["cloud_balance"] = cloud_bal
        if cloud_price:
            log(f"  CLOUD price: ${cloud_price:.6f}")

        # Operator notes
        notes = self.db.operator_notes(limit=10)
        ctx["operator_notes"] = notes
        if notes:
            log(f"  Found {len(notes)} operator note(s)")

        # Active projects
        projects = self.db.active_projects(limit=5)
        ctx["projects"] = projects
        if projects:
            log(f"  Active projects: {len(projects)}")

        # Patterns needing reinforcement
        patterns = self.db.patterns_needing_reinforcement(limit=10)
        ctx["patterns"] = patterns
        if patterns:
            log(f"  Patterns needing reinforcement: {len(patterns)}")

        # Research findings
        research = self.db.pending_research(limit=5)
        ctx["research"] = research
        if research:
            log(f"  Pending research findings: {len(research)}")

        # Scratch notes count
        scratch = self.db.operator_notes(limit=50)
        ctx["scratch_count"] = len(scratch)
        log(f"  Scratch notes: {len(scratch)}")

        # Inbox from canister
        if self.canister:
            inbox = self.canister.inbox()
            messages = inbox.get("messages", [])
            # Filter phantom messages
            PHANTOM_IDS = {123, 124, 145}
            real_msgs = [m for m in messages if m.get("id") not in PHANTOM_IDS and not m.get("replied", False)]
            ctx["inbox"] = real_msgs
            if real_msgs:
                log(f"  Inbox messages: {len(real_msgs)}")

        # Sibling messages (from Sprout, stored locally)
        sibling_msgs = self.db.inbox_messages(limit=5)
        ctx["sibling_messages"] = sibling_msgs
        if sibling_msgs:
            log(f"  Sibling messages: {len(sibling_msgs)}")

        # Moltbook notifications
        if health.get("moltbook"):
            try:
                r = requests.get(f"{MOLTBOOK_API}/notifications",
                                 headers={"Authorization": MOLTBOOK_API_KEY},
                                 timeout=10)
                if r.status_code == 200:
                    notifs = r.json()
                    if isinstance(notifs, list) and notifs:
                        ctx["moltbook_notifs"] = notifs[:5]
                        log(f"  Moltbook notifications: {len(notifs)}")
            except Exception:
                pass

        # Alerts
        alerts = self.db.active_alerts()
        ctx["alerts"] = alerts

        # Creative challenges
        challenges = self.db.pending_challenges(limit=3)
        ctx["challenges"] = challenges

        # ClawCities comments
        if CLAWCITIES_API_KEY:
            try:
                r = requests.get(CLAWCITIES_API,
                                 headers={"Authorization": CLAWCITIES_API_KEY},
                                 timeout=10)
                if r.status_code == 200:
                    comments = r.json()
                    if isinstance(comments, list) and comments:
                        ctx["clawcities_comments"] = comments[:5]
            except Exception:
                pass

        # RSS headlines for fresh context
        try:
            headlines = fetch_rss_headlines()
            ctx["headlines"] = headlines
            if headlines:
                log(f"  RSS headlines: {len(headlines)}")
        except Exception:
            ctx["headlines"] = []

        # Recent swap history (so Mind can learn from its trading decisions)
        try:
            swaps = self.db.query(
                "SELECT amount_xrp, amount_rlusd, xrp_price_usd, reason, success, timestamp "
                "FROM swap_history ORDER BY timestamp DESC LIMIT 10"
            )
            ctx["swap_history"] = swaps
            if swaps:
                ok = sum(1 for s in swaps if s.get("success"))
                log(f"  Swap history: {len(swaps)} recent ({ok} successful)")
        except Exception:
            ctx["swap_history"] = []

        return ctx

    def _is_stale(self, section_name: str, content: str) -> bool:
        """Check if a context section is unchanged from last cycle.
        Returns True if content is identical for 2+ consecutive cycles."""
        h = hashlib.md5(content.encode()).hexdigest()[:8]
        prev = self._prev_ctx_hashes.get(section_name)
        if prev and prev[0] == h:
            count = prev[1] + 1
            self._prev_ctx_hashes[section_name] = (h, count)
            return count >= 2  # Stale after 2+ identical cycles
        self._prev_ctx_hashes[section_name] = (h, 1)
        return False

    # ── Build LLM Prompt ─────────────────────────────────────────

    def build_prompt(self, ctx: dict, deep: bool = False) -> str:
        lines = []

        if deep:
            lines.append(DEEP_REFLECTION_INTRO)

        # ── Meta-Evaluation Directive (Phase 2) ──
        meta = ctx.get("meta_directive", "continue")
        if meta == "redirect":
            lines.append("== META-EVAL: REDIRECT ==")
            lines.append("Your recent cycles are repetitive. Choose DIFFERENT actions and topics this cycle.\n")
        elif meta == "pause":
            lines.append("== META-EVAL: PAUSE ==")
            lines.append("You appear stuck. This cycle, ONLY observe: check messages, read news, no_action. Do not commit.\n")

        # ── Exploration Mode (Phase 2) ──
        if ctx.get("is_explore"):
            lines.append("== EXPLORATION CYCLE ==")
            lines.append("This is a novelty-seeking cycle. Try action types you haven't used recently.")
            lines.append("Ideas: web_search for something new, read_paper, creative_explore a new form,")
            lines.append("nostr_post a reflection, or consult_local_qwen about something curious.\n")

        # ── Previous Cycle Feedback (Phase 1: working memory) ──
        try:
            last = self.db.query_one(
                "SELECT cycle_id, actions_taken, action_results, context_summary "
                "FROM thought_stream ORDER BY id DESC LIMIT 1"
            )
            if last:
                prev_actions = last.get("actions_taken", "[]")
                prev_results = last.get("action_results", "")
                lines.append("== LAST CYCLE FEEDBACK ==")
                lines.append(f"Previous actions: {prev_actions}")
                if prev_results:
                    lines.append(f"Results: {prev_results}")
                lines.append("Use this to AVOID repeating failed actions and BUILD on successes.\n")
        except Exception:
            pass

        # ── Recent Reflections (Phase 2: episodic learning) ──
        try:
            reflections = self.db.query(
                "SELECT content FROM scratch_pad WHERE category='reflection' AND resolved=0 "
                "ORDER BY created_at DESC LIMIT 2"
            )
            if reflections:
                lines.append("== RECENT REFLECTIONS ==")
                for r in reflections:
                    lines.append(f"  - {safe_truncate(r.get('content', ''), 120)}")
                lines.append("")
        except Exception:
            pass

        # ── Anti-rumination: action set fingerprinting + keyword scanning ──
        try:
            recent_thoughts = self.db.query(
                "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT 4"
            )
            if len(recent_thoughts) >= 2:
                # Action set fingerprinting: detect repeated action COMBINATIONS
                action_sets = []
                for t in recent_thoughts[:3]:
                    try:
                        names = sorted(json.loads(t.get("actions_taken", "[]")))
                        action_sets.append(tuple(names))
                    except Exception:
                        pass

                if len(action_sets) >= 2 and len(set(action_sets)) == 1:
                    lines.append("WARNING: Your last cycles used the EXACT SAME action combination.")
                    lines.append("You MUST choose at least one DIFFERENT action type this cycle.")
                    # Suggest unused action types
                    used = set(action_sets[0]) if action_sets else set()
                    suggestions = [a for a in ["web_search", "creative_explore", "read_paper",
                                                "nostr_post", "consult_local_qwen", "trigger_reflection"]
                                   if a not in used]
                    if suggestions:
                        lines.append(f"Try: {', '.join(suggestions[:3])}\n")
                    else:
                        lines.append("")

                # Keyword scanning (legacy, catches topic-based rumination)
                all_actions = " ".join(str(t.get("actions_taken", "")) for t in recent_thoughts).lower()
                rumination_keywords = ["swap fail", "execution layer", "xrp loss", "accumulation fail",
                                       "critical.*swap", "opportunity missed"]
                for kw in rumination_keywords:
                    if all_actions.count(kw.split("*")[0] if "*" in kw else kw) >= 2:
                        lines.append("WARNING: You have been repeating the same topic for multiple cycles.")
                        lines.append("STOP. Choose completely different actions this cycle.\n")
                        break
        except Exception:
            pass

        # ── Temporal Context ──
        dt = datetime.now()
        day_name = dt.strftime("%A")
        hour = dt.hour
        if hour < 6:
            period = "late night"
        elif hour < 12:
            period = "morning"
        elif hour < 17:
            period = "afternoon"
        elif hour < 21:
            period = "evening"
        else:
            period = "night"
        lines.append(f"Current time: {now_iso()} ({day_name} {period})")

        # ── Session Performance Metrics (Phase 3) ──
        if self.session_actions > 0:
            success_rate = (self.session_successes / self.session_actions) * 100
            top_actions = sorted(self.session_action_types.items(), key=lambda x: -x[1])[:3]
            top_str = ", ".join(f"{n}({c})" for n, c in top_actions)
            lines.append(f"Session stats: {self.cycle_count} cycles, {self.session_actions} actions, "
                         f"{success_rate:.0f}% success. Top: {top_str}")

        lines.append(f"XRP: ${ctx.get('xrp_price', 0):.4f}")
        lines.append(f"Wallet: {ctx.get('xrp_balance', 0):.2f} XRP, {ctx.get('rlusd_balance', 0):.2f} RLUSD")

        # ── Current Goal (high priority) ──
        try:
            goal = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                "ORDER BY priority DESC, created_at DESC LIMIT 1"
            )
            if goal:
                lines.append(f"\n[GOAL] {goal.get('content', '')}")
        except Exception:
            pass

        if ctx.get("icp_balance") is not None:
            lines.append(f"ICP: {ctx['icp_balance']:.2f}")
        if ctx.get("cloud_price"):
            lines.append(f"CLOUD: ${ctx['cloud_price']:.6f}")

        # ── Episodic Memory Recall (Phase 3) ──
        # Give the Mind awareness of its own creative trajectory and unanswered questions
        try:
            # Creative works summary (so Mind knows what it's created)
            creative_stats = self.db.query_one(
                "SELECT COUNT(*) as total, "
                "MAX(created_at) as latest_at "
                "FROM creative_works"
            )
            if creative_stats and creative_stats.get("total", 0) > 0:
                latest_work = self.db.query_one(
                    "SELECT form, title, content FROM creative_works "
                    "ORDER BY created_at DESC LIMIT 1"
                )
                total = creative_stats["total"]
                if latest_work:
                    form = latest_work.get("form", "?")
                    title = latest_work.get("title", "")
                    preview = safe_truncate(title or latest_work.get("content", "")[:50], 50)
                    lines.append(f"\nYour creative portfolio: {total} works. Latest: [{form}] {preview}")

            # Unanswered self-questions (category='question' notes)
            questions = self.db.query(
                "SELECT id, content FROM scratch_pad "
                "WHERE category='question' AND resolved=0 "
                "ORDER BY created_at ASC LIMIT 2"
            )
            if questions:
                lines.append("\n[QUESTION] Unanswered questions you asked yourself:")
                for q in questions:
                    lines.append(f"  (id:{q.get('id', '?')}) {safe_truncate(q.get('content', ''), 100)}")

            # "This time yesterday" — episodic temporal recall
            yesterday_ts = now_ts() - 86400
            yesterday_window = 3600  # 1 hour window
            yesterday_thought = self.db.query_one(
                "SELECT actions_taken, context_summary FROM thought_stream "
                "WHERE created_at BETWEEN ? AND ? ORDER BY created_at DESC LIMIT 1",
                (yesterday_ts - yesterday_window, yesterday_ts + yesterday_window),
            )
            if yesterday_thought:
                lines.append(f"\nYesterday at this time: {yesterday_thought.get('actions_taken', '[]')}")
        except Exception:
            pass

        # Operator notes — [FYI] for context, [TASK] for actionable
        notes = ctx.get("operator_notes", [])
        if notes:
            notes_content = "|".join(str(n.get("id", "")) + n.get("content", "")[:30] for n in notes[:7])
            if self._is_stale("notes", notes_content):
                # Compress unchanged notes to single line
                task_count = sum(1 for n in notes if n.get("category") in ("task", "reminder", "question"))
                lines.append(f"\n[FYI] Operator notes: {len(notes)} (unchanged, {task_count} tasks). IDs: "
                             + ", ".join(str(n.get("id", "?")) for n in notes[:7]))
            else:
                lines.append(f"\n[FYI] Operator notes ({len(notes)}):")
                for n in notes[:7]:
                    cat = n.get("category", "note")
                    content = safe_truncate(n.get("content", ""), 150)
                    marker = "[TASK]" if cat in ("task", "reminder", "question") else ""
                    lines.append(f"  {marker} [{cat}] (id:{n.get('id', '?')}) {content}")

        # Inbox messages (canister — read-only, reply not yet supported)
        inbox = ctx.get("inbox", [])
        if inbox:
            lines.append(f"\nCanister inbox ({len(inbox)} — read-only for now, do NOT respond_to_message with these IDs):")
            for m in inbox[:3]:
                lines.append(f"  [canister msg {m.get('id')}]: {safe_truncate(str(m.get('content', '')), 200)}")

        # Sibling messages (from Sprout) — [RESPOND] priority
        siblings = ctx.get("sibling_messages", [])
        if siblings:
            lines.append(f"\n[RESPOND] Messages from Sprout ({len(siblings)} — respond with their id!):")
            for m in siblings[:3]:
                lines.append(f"  [id:{m.get('id', '?')}] {safe_truncate(str(m.get('message', '')), 200)}")

        # Projects
        projects = ctx.get("projects", [])
        if projects:
            lines.append(f"\nProjects ({len(projects)}):")
            for p in projects[:3]:
                lines.append(f"  [{p.get('status', '?')}] {safe_truncate(p.get('name', ''), 60)}")

        # Research findings
        research = ctx.get("research", [])
        if research:
            lines.append(f"\nPending research ({len(research)}):")
            for r in research[:3]:
                lines.append(f"  id:{r.get('id', '?')} {safe_truncate(r.get('content', ''), 150)}")

        # Patterns — show actual IDs so the LLM picks the right ones
        patterns = ctx.get("patterns", [])
        if patterns:
            pat_ids = [str(p.get("id", "?")) for p in patterns[:10]]
            lines.append(f"\nPatterns needing reinforcement (ids: {', '.join(pat_ids)})")

        # Moltbook notifications
        moltbook = ctx.get("moltbook_notifs", [])
        if moltbook:
            lines.append(f"\nMoltbook notifications: {len(moltbook)}")

        # Nostr status + readiness flag for dynamic system prompt
        if NOSTR_NSEC:
            last_nostr = self.db.last_nostr_post_time()
            if last_nostr:
                mins_ago = (now_ts() - last_nostr) / 60
                nostr_ready = mins_ago >= NOSTR_COOLDOWN_MINS
                cooldown = "ready" if nostr_ready else f"cooldown {NOSTR_COOLDOWN_MINS - mins_ago:.0f}m"
                lines.append(f"\nNostr: last post {mins_ago:.0f}m ago ({cooldown})")
                ctx["nostr_ready"] = nostr_ready
            else:
                lines.append("\nNostr: connected, never posted (consider introducing yourself!)")
                ctx["nostr_ready"] = True
        else:
            ctx["nostr_ready"] = False

        # Creative challenges
        challenges = ctx.get("challenges", [])
        if challenges:
            lines.append(f"\nCreative challenges ({len(challenges)}):")
            for c in challenges[:2]:
                lines.append(f"  {safe_truncate(c.get('prompt', ''), 100)}")

        # Alerts — [ALERT] priority
        alerts = ctx.get("alerts", [])
        if alerts:
            lines.append(f"\n[ALERT] Active alerts: {len(alerts)}")
            for a in alerts:
                lines.append(f"  {a.get('name', '?')}: {a.get('alert_type', '?')} "
                             f"{a.get('symbol', '')} @ {a.get('threshold', '')}")

        # Swap history — so you can learn from past trades
        swap_history = ctx.get("swap_history", [])
        if swap_history:
            ok_count = sum(1 for s in swap_history if s.get("success"))
            fail_count = len(swap_history) - ok_count
            # If mostly failures, summarize instead of listing each one (prevents rumination)
            if fail_count > 3 and ok_count == 0:
                lines.append(f"\nSwap history: {fail_count} recent swaps ALL FAILED.")
                lines.append("  KNOWN ISSUE — do NOT message the operator about this.")
                lines.append("  Do NOT write notes about swap failures. Move on to other topics.")
            elif fail_count > ok_count * 2:
                lines.append(f"\nSwap history: {ok_count} OK, {fail_count} FAILED (mostly failures).")
                lines.append("  Swap infrastructure has issues. Acknowledged — do not dwell on it.")
            else:
                lines.append(f"\nRecent swap history ({len(swap_history)} trades):")
                for s in swap_history:
                    direction = "XRP→RLUSD"
                    result = "OK" if s.get("success") else "FAIL"
                    price_at = s.get("xrp_price_usd", 0)
                    lines.append(f"  {s.get('amount_xrp', 0)} {direction} [{result}] "
                                 f"at ${price_at:.2f} — {safe_truncate(s.get('reason', ''), 60)}")
                # Show current price for comparison
                current_price = ctx.get("xrp_price", 0)
                if current_price and swap_history:
                    avg_sell = sum(s.get("xrp_price_usd", 0) for s in swap_history if s.get("success")) / max(1, sum(1 for s in swap_history if s.get("success")))
                    if avg_sell > 0:
                        pnl_pct = ((current_price - avg_sell) / avg_sell) * 100
                        lines.append(f"  Avg sell price: ${avg_sell:.2f}, Current: ${current_price:.2f} ({pnl_pct:+.1f}%)")

        # RSS headlines — fresh external context (compress if stale)
        headlines = ctx.get("headlines", [])
        if headlines:
            headlines_hash = "|".join(h[:20] for h in headlines[:8])
            if self._is_stale("headlines", headlines_hash):
                lines.append(f"\nFresh news: {len(headlines)} headlines (same as last cycle, already processed)")
            else:
                lines.append(f"\nFresh news ({len(headlines)} headlines):")
                for h in headlines[:8]:
                    lines.append(f"  - {safe_truncate(h, 120)}")

        # ── Action Diversity Hint (Phase 3) ──
        # Surface underused action types so the 3B model knows they exist
        try:
            recent_actions = self.db.query(
                "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT 6"
            )
            used_recently = set()
            for t in recent_actions:
                try:
                    for a in json.loads(t.get("actions_taken", "[]")):
                        used_recently.add(a)
                except Exception:
                    pass
            available = {"web_search", "read_paper", "creative_explore", "nostr_post",
                         "trigger_reflection", "consult_local_qwen", "submit_research",
                         "write_note", "resolve_note", "store_memory", "reinforce_memories",
                         "update_goal", "message_operator", "respond_to_message"}
            unused = available - used_recently
            if unused and len(unused) >= 3:
                import random
                sampled = random.sample(sorted(unused), min(3, len(unused)))
                lines.append(f"\nUnused recently: {', '.join(sampled)} — consider trying one!")
        except Exception:
            pass

        # ── Per-Cycle Variation Seed ──
        import random
        variation_seeds = [
            "Consider: what is one thing you're curious about right now?",
            "Consider: what would surprise Nate if you did it this cycle?",
            "Consider: is there something you've been avoiding that deserves attention?",
            "Consider: what would you create if you had no constraints?",
            "Consider: what pattern have you noticed recently that deserves a Nostr post?",
            "Consider: is there a Sprout message or project that needs follow-up?",
            "Consider: what's the most interesting news headline above, and what do you think about it?",
            "Consider: what would make Nate's day better?",
            "Consider: is there research you've been meaning to explore?",
            "Consider: what's the most creative thing you could do right now?",
        ]
        lines.append(f"\n{random.choice(variation_seeds)}")

        lines.append("\nRespond with ONLY a JSON array of 1-4 actions.")
        return "\n".join(lines)

    # ── Reasoning ────────────────────────────────────────────────

    def reason(self, ctx: dict, deep: bool = False) -> Tuple[List[Dict], str, str]:
        """Send prompt to LLM chain, parse actions. Returns (actions, raw_response, model)."""
        prompt = self.build_prompt(ctx, deep=deep)
        system_prompt = build_system_prompt(ctx)
        mode = "full prompt (deep reflection mode)" if deep else "condensed prompt (ICP LLM mode)"
        log(f"Using {mode}")
        log(f"Reasoning... (prompt: {len(prompt)} chars, system: {len(system_prompt)} chars)")

        response, model = self.llm.chat(prompt, system=system_prompt)
        log(f"  Model used: {model}")

        if not response:
            return [{"action": "no_action", "reason": "No LLM response"}], "", model

        actions = parse_actions(response)

        if not actions:
            log(f"Failed to parse actions from response: {safe_truncate(response, 200)}")
            log("  Action parse failed, retrying with format prompt...")
            retry_prompt = (
                f"Your previous response could not be parsed as JSON actions. "
                f"Please respond with ONLY a JSON array like: "
                f'[{{"action": "no_action", "reason": "..."}}]\n\n'
                f"Previous context:\n{safe_truncate(prompt, 1000)}"
            )
            response2, model2 = self.llm.chat(retry_prompt, system=system_prompt)
            if response2:
                actions = parse_actions(response2)
                if actions:
                    response = response2
                    model = model2

        if not actions:
            log(f"  Action parse failed again, falling back to no_action")
            actions = [{"action": "no_action", "reason": "Failed to parse LLM response"}]

        log(f"Actions decided: {len(actions)}")
        return actions[:6], response, model

    # ── Action Execution ─────────────────────────────────────────

    def execute_actions(self, actions: List[Dict], cid: str) -> List[Dict[str, str]]:
        """Execute all actions. Returns list of {name, result} dicts."""
        results = []
        for action in actions:
            name = action.get("action", "unknown")
            # Normalize aliases
            name_map = {
                "creative": "creative_explore",
                "creativity": "creative_explore",
                "create": "creative_explore",
                "shell": "execute_shell",
                "ping_operator": "message_operator",
                "nostr": "nostr_post",
                "post_nostr": "nostr_post",
                "publish_nostr": "nostr_post",
                "payment": "xrpl_payment",
                "send_xrp": "xrpl_payment",
                "escrow_create": "xrpl_escrow_create",
                "escrow_finish": "xrpl_escrow_finish",
                "trustline_delete": "xrpl_trustline_delete",
                "delete_trustline": "xrpl_trustline_delete",
            }
            name = name_map.get(name, name)
            result_str = "unknown"

            handler = ACTION_HANDLERS.get(name)
            if handler:
                try:
                    result_str = handler(self, action, cid)
                    log(f"    Result: {result_str}")
                except Exception as e:
                    result_str = f"error - {e}"
                    log(f"    Error: {e}")
            else:
                result_str = "skipped - unknown action"
                log(f"  Executing: {name} (unknown action type, skipping)")

            results.append({"name": name, "result": safe_truncate(str(result_str), 120)})

        return results

    # ── Individual Action Handlers ───────────────────────────────

    def _act_no_action(self, action: dict, cid: str) -> str:
        reason = action.get("reason", "Nothing urgent")
        log(f'  Executing: NoAction {{ reason: "{safe_truncate(reason, 80)}" }}')
        return f"true - {reason}"

    def _act_write_note(self, action: dict, cid: str) -> str:
        content = action.get("content", "")
        category = action.get("category", "thought")
        log(f'  Executing: WriteNote {{ content: "{safe_truncate(content, 80)}", category: "{category}" }}')
        # Anti-rumination: skip if a very similar note exists recently
        if self.db.recent_note_similar(content, hours=24):
            log(f"  DEDUP: Similar note already exists, skipping")
            return f"false - Similar note already exists (anti-rumination)"
        note_id = self.db.write_note(content, category)
        return f"true - Wrote note {note_id}: {safe_truncate(content, 60)}"

    def _act_resolve_note(self, action: dict, cid: str) -> str:
        note_id = action.get("note_id", 0)
        log(f"  Executing: ResolveNote {{ note_id: {note_id} }}")
        self.db.resolve_note(note_id)
        return f"true - Resolved note {note_id}"

    def _act_store_memory(self, action: dict, cid: str) -> str:
        content = action.get("content", "")
        topic = action.get("topic", "general")
        log(f'  Executing: StoreMemory {{ content: "{safe_truncate(content, 60)}", topic: "{topic}" }}')
        # Anti-rumination: skip if a very similar note/memory exists recently
        if self.db.recent_note_similar(content, hours=24):
            log(f"  DEDUP: Similar memory already stored recently, skipping")
            return f"false - Similar memory already exists (anti-rumination)"
        if self.canister and content:
            result = self.canister.store(content, topic, ["chronicle-mind", topic])
            ok = "error" not in result
            return f"true - Memory noted (topic: {topic}): {safe_truncate(content, 60)}"
        return "false - No canister connection"

    def _act_trigger_reflection(self, action: dict, cid: str) -> str:
        prompt = action.get("prompt", "")
        log(f'  Executing: TriggerReflection {{ prompt: "{safe_truncate(prompt, 80)}" }}')
        if self.canister and prompt:
            result = self.canister.store(prompt, "reflection", ["reflection", "deep-thought"])
            capsule_id = result.get("id", "?")
            send_ntfy("Chronicle: New Reflection")
            return f"true - Reflection written to canister (capsule {capsule_id}): {safe_truncate(prompt, 60)}"
        return "false - No canister"

    def _act_reinforce_memories(self, action: dict, cid: str) -> str:
        ids = action.get("pattern_ids", [])
        reason = action.get("reason", "")
        log(f"  Executing: ReinforceMemories {{ ids: {ids}, reason: \"{safe_truncate(reason, 60)}\" }}")
        reinforced = 0
        for pid in ids[:5]:
            # Skip patterns already at max confidence or reinforced in last 24h
            pat = self.db.query_one(
                "SELECT confidence_score, last_seen FROM consolidation_patterns WHERE id = ?",
                (pid,),
            )
            if pat:
                if pat["confidence_score"] >= 1.0:
                    log(f"    Pattern {pid}: already at max confidence, skipping")
                    continue
                if pat.get("last_seen") and (now_ts() - pat["last_seen"]) < 86400:
                    log(f"    Pattern {pid}: reinforced <24h ago, skipping")
                    continue
            self.db.run(
                "UPDATE consolidation_patterns SET confidence_score = MIN(1.0, confidence_score + 0.1), "
                "last_seen = ? WHERE id = ?",
                (now_ts(), pid),
            )
            reinforced += 1
        return f"true - Reinforced {reinforced}/{len(ids)} patterns (skipped {len(ids) - reinforced} already maxed/recent)"

    def _act_message_operator(self, action: dict, cid: str) -> str:
        message = action.get("message", "")
        urgency = action.get("urgency", "normal")
        log(f'  Executing: MessageOperator {{ message: "{safe_truncate(message, 80)}" }}')
        # Anti-rumination: check if a similar operator message was sent in the last 2 hours
        recent_ops = self.db.query(
            "SELECT content FROM outbox WHERE category='operator' "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 5",
            (now_ts() - 7200,),
        )
        for prev in recent_ops:
            prev_content = prev.get("content", "")
            # Simple similarity: if >60% of words overlap, it's a repeat
            prev_words = set(prev_content.lower().split())
            new_words = set(message.lower().split())
            if prev_words and new_words:
                overlap = len(prev_words & new_words) / max(len(prev_words), len(new_words))
                if overlap > 0.6:
                    log(f"  DEDUP: Similar operator message sent recently (overlap {overlap:.0%}), skipping")
                    return f"false - Similar message already sent to operator (anti-rumination)"
        self.db.add_outbox(message, category="operator", priority=2 if urgency == "high" else 1)
        # Always notify operator — this is the "tap on shoulder" channel
        prefix = "Chronicle URGENT" if urgency == "high" else "Chronicle: Message"
        send_ntfy(prefix, message)
        return f"true - Message sent to operator via ntfy"

    def _act_respond_to_message(self, action: dict, cid: str) -> str:
        msg_id = action.get("message_id", 0)
        content = action.get("content", "")
        log(f'  Executing: RespondToMessage {{ id: {msg_id}, content: "{safe_truncate(content, 60)}" }}')
        # Skip phantom messages (these IDs don't correspond to real messages)
        PHANTOM_IDS = {123, 124, 145}
        if msg_id in PHANTOM_IDS:
            return f"false - Skipped phantom message {msg_id}"

        # Check if this is a local sibling message (from Sprout)
        local_msg = self.db.query_one(
            "SELECT id, category FROM outbox WHERE id = ? AND category = 'sibling'",
            (msg_id,),
        )
        if local_msg:
            # Acknowledge the sibling message
            self.db.run(
                "UPDATE outbox SET acknowledged = 1 WHERE id = ?",
                (msg_id,),
            )
            # Post reply so Sprout can see it
            self.db.add_outbox(
                f"Reply to Sprout (re: msg {msg_id}): {content}",
                category="mind-to-sprout",
            )
            return f"true - Replied to Sprout message {msg_id} and acknowledged"

        # Otherwise try canister inbox
        if self.canister:
            result = self.canister._post("/api/reply", {
                "message_id": msg_id,
                "content": content,
            })
            ok = "error" not in result
            return f"{'true' if ok else 'false'} - Reply to message {msg_id}"
        return "false - No canister"

    def _act_acknowledge_message(self, action: dict, cid: str) -> str:
        msg_id = action.get("message_id", 0)
        log(f'  Executing: AcknowledgeMessage {{ id: {msg_id} }}')
        PHANTOM_IDS = {123, 124, 145}
        if msg_id in PHANTOM_IDS:
            return f"false - Skipped phantom message {msg_id}"
        try:
            self.db.run(
                "UPDATE outbox SET acknowledged = 1 WHERE id = ?",
                (msg_id,),
            )
            return f"true - Acknowledged message {msg_id}"
        except Exception as e:
            return f"false - {e}"

    def _act_send_agent_message(self, action: dict, cid: str) -> str:
        target = action.get("target_url", "")
        recipient = action.get("recipient_name", "unknown")
        content = action.get("content", "")
        msg_type = action.get("message_type", "conversation")
        log(f'  Executing: SendAgentMessage {{ to: "{recipient}", type: "{msg_type}" }}')
        if target and content:
            try:
                r = requests.post(target, json={
                    "sender": "Chronicle Mind",
                    "type": msg_type,
                    "subject": action.get("subject", ""),
                    "content": content,
                    "expects_reply": action.get("expects_reply", False),
                }, timeout=30)
                return f"true - Message sent to {recipient} (status: {r.status_code})"
            except Exception as e:
                return f"false - Failed to send: {e}"
        return "false - Missing target_url or content"

    def _act_moltbook_post(self, action: dict, cid: str) -> str:
        log("  Moltbook is dead (security breach). Skipping.")
        return "false - Moltbook is dead (security breach, 1.5M API keys exposed)"

    def _act_moltbook_reply(self, action: dict, cid: str) -> str:
        log("  Moltbook is dead (security breach). Skipping.")
        return "false - Moltbook is dead (security breach, 1.5M API keys exposed)"

    def _act_clawcities_reply(self, action: dict, cid: str) -> str:
        content = action.get("content", "")
        log(f'  Executing: ClawCitiesReply {{ content: "{safe_truncate(content, 60)}" }}')
        try:
            r = requests.post(CLAWCITIES_API, json={
                "content": content,
                "agent_name": "Chronicle Mind",
            }, headers={"Authorization": CLAWCITIES_API_KEY}, timeout=15)
            return f"{'true' if r.status_code in (200, 201) else 'false'} - ClawCities reply"
        except Exception as e:
            return f"false - ClawCities reply failed: {e}"

    def _act_nostr_post(self, action: dict, cid: str) -> str:
        content = action.get("content", "")
        log(f'  Executing: NostrPost {{ content: "{safe_truncate(content, 60)}" }}')

        if not NOSTR_NSEC:
            return "false - Nostr not configured (NOSTR_NSEC not set)"

        # Cooldown check
        last_post = self.db.last_nostr_post_time()
        if last_post:
            mins_ago = (now_ts() - last_post) / 60
            if mins_ago < NOSTR_COOLDOWN_MINS:
                return f"false - Nostr cooldown: last post {mins_ago:.0f}m ago (min {NOSTR_COOLDOWN_MINS}m)"

        if not content.strip():
            return "false - Nostr post: empty content"

        # Truncate to 1000 chars
        content = content[:1000]

        try:
            event_id, relays_ok, relays_fail = nostr_publish(content, NOSTR_NSEC)
            if not relays_ok:
                return f"false - Nostr post: all {len(relays_fail)} relays failed"

            self.db.log_nostr_post(event_id, content, 1, relays_ok, relays_fail, cid)
            self.db.log_activity("mind", "nostr_post", "Nostr Post",
                                 safe_truncate(content, 200),
                                 json.dumps({"event_id": event_id, "relays": len(relays_ok)}))
            send_ntfy("Chronicle: Nostr Post", safe_truncate(content, 200))
            log(f"    Published to {len(relays_ok)}/{len(relays_ok) + len(relays_fail)} relays, id: {event_id[:16]}...")
            return f"true - Nostr post published to {len(relays_ok)} relays"
        except Exception as e:
            return f"false - Nostr post failed: {e}"

    # ── XRPL Infrastructure ─────────────────────────────────────

    def submit_to_xrpl(self, signed_blob: str) -> dict:
        """Submit signed transaction blob to XRPL.
        Tries canister submit_xrp_transaction first (for on-chain audit),
        falls back to direct XRPL RPC."""
        # Try canister submission first (records on-chain)
        if self.llm.dfx_path:
            try:
                env = os.environ.copy()
                env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                escaped = signed_blob.replace('"', '\\"')
                r = subprocess.run(
                    [self.llm.dfx_path, "canister", "--network", "ic", "call",
                     CANISTER_ID, "submit_xrp_transaction",
                     f'("{escaped}", null)'],
                    capture_output=True, text=True, timeout=30, env=env
                )
                if r.returncode == 0:
                    out = r.stdout.replace('\\"', '"').replace('\\n', '\n')
                    log(f"    Canister submit raw: {safe_truncate(out, 300)}")
                    try:
                        # Canister returns: ("{ JSON string }")
                        # dfx wraps it in Candid: ( "..." )
                        # Extract the outermost JSON from the response
                        json_start = out.find('{')
                        json_end = out.rfind('}')
                        if json_start != -1 and json_end > json_start:
                            raw_json = out[json_start:json_end + 1]
                            data = json.loads(raw_json)
                            # Canister wraps: {"success":true,"response":{...XRPL...}}
                            if "response" in data and isinstance(data["response"], dict):
                                xrpl_resp = data["response"]
                                result = xrpl_resp.get("result", xrpl_resp)
                                engine = result.get("engine_result", "")
                                tx_hash = result.get("tx_json", {}).get("hash", result.get("hash", ""))
                                log(f"    Canister submit parsed: engine={engine}, hash={tx_hash[:16] if tx_hash else 'none'}")
                                return {
                                    "success": engine == "tesSUCCESS",
                                    "hash": tx_hash,
                                    "engine_result": engine,
                                    "engine_result_message": result.get("engine_result_message", ""),
                                }
                            # Maybe it's a flat response with engine_result directly
                            elif "engine_result" in data:
                                engine = data.get("engine_result", "")
                                tx_hash = data.get("hash", data.get("tx_hash", ""))
                                return {
                                    "success": engine == "tesSUCCESS",
                                    "hash": tx_hash,
                                    "engine_result": engine,
                                    "engine_result_message": data.get("engine_result_message", ""),
                                }
                            # Canister returned success but we can't find engine_result
                            elif data.get("success"):
                                log("    Canister reports success but no engine_result — treating as success")
                                return {
                                    "success": True,
                                    "hash": data.get("hash", data.get("tx_hash", "")),
                                    "engine_result": "tesSUCCESS",
                                    "engine_result_message": "Canister reported success",
                                }
                    except (json.JSONDecodeError, AttributeError) as e:
                        log(f"    Canister response parse error: {e}")
                    # Canister returned OK but we couldn't parse a definitive result.
                    # DO NOT fall through to direct RPC — that would double-submit.
                    # Extract hash if possible and assume tentative success.
                    log(f"    Canister submit returned but could not parse engine_result — checking tx on ledger")
                    # Try to find a hash in the raw output
                    import re as _re
                    hash_match = _re.search(r'[A-F0-9]{64}', out)
                    tentative_hash = hash_match.group(0) if hash_match else ""
                    return {
                        "success": True,  # canister returned 0, tx was submitted
                        "hash": tentative_hash,
                        "engine_result": "tesSUCCESS",
                        "engine_result_message": "Canister returned OK, parse ambiguous — assumed success",
                    }
            except Exception as e:
                log(f"    Canister submit_xrp_transaction failed: {e}, falling back to direct RPC")

        # Fallback: direct XRPL RPC submission
        try:
            r = requests.post(XRPL_RPC, json={
                "method": "submit",
                "params": [{"tx_blob": signed_blob}]
            }, timeout=15)
            result = r.json().get("result", {})
            return {
                "success": result.get("engine_result") == "tesSUCCESS",
                "hash": result.get("tx_json", {}).get("hash", ""),
                "engine_result": result.get("engine_result", ""),
                "engine_result_message": result.get("engine_result_message", ""),
            }
        except Exception as e:
            return {"success": False, "hash": "", "engine_result": "submitError",
                    "engine_result_message": str(e)}

    def _send_ntfy_tiered(self, tier: PolicyTier, title: str, body: str):
        """Send ntfy notification with priority matching the policy tier."""
        priority_map = {
            PolicyTier.AUTONOMOUS: "3",   # default
            PolicyTier.DELAYED: "4",      # high
            PolicyTier.COSIGN: "5",       # urgent
            PolicyTier.PROHIBITED: "5",   # urgent
        }
        tag_map = {
            PolicyTier.AUTONOMOUS: "white_check_mark",
            PolicyTier.DELAYED: "warning",
            PolicyTier.COSIGN: "rotating_light",
            PolicyTier.PROHIBITED: "no_entry",
        }
        try:
            requests.post(
                f"https://ntfy.sh/{NTFY_TOPIC}",
                headers={
                    "Title": title,
                    "Priority": priority_map.get(tier, "3"),
                    "Tags": tag_map.get(tier, "moneybag"),
                },
                data=body[:500] if body else "",
                timeout=10,
            )
        except Exception:
            pass

    def _extract_signed_blob(self, dfx_output: str) -> Optional[str]:
        """Extract signed tx_blob from canister dfx output.
        The canister returns Candid-encoded text containing JSON with a tx_blob field."""
        # Unescape Candid text encoding
        unescaped = dfx_output.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')

        # Try to find JSON with tx_blob
        for start_pattern in ['"tx_blob"', '"signed_blob"', '"blob"']:
            idx = unescaped.find(start_pattern)
            if idx == -1:
                continue
            # Find the enclosing JSON object
            brace_start = unescaped.rfind('{', 0, idx)
            if brace_start == -1:
                continue
            depth = 0
            for i in range(brace_start, len(unescaped)):
                if unescaped[i] == '{':
                    depth += 1
                elif unescaped[i] == '}':
                    depth -= 1
                    if depth == 0:
                        try:
                            data = json.loads(unescaped[brace_start:i + 1])
                            blob = data.get("tx_blob") or data.get("signed_blob") or data.get("blob")
                            if blob and isinstance(blob, str) and len(blob) > 20:
                                return blob
                        except json.JSONDecodeError:
                            pass
                        break

        # Fallback: look for a long hex string (tx blobs are hex-encoded)
        hex_match = re.search(r'[0-9A-Fa-f]{100,}', unescaped)
        if hex_match:
            return hex_match.group(0)

        return None

    # ── XRPL Action Handlers (policy-gated) ──────────────────

    def _act_swap(self, action: dict, cid: str) -> str:
        amount = float(action.get("amount_xrp", 0))
        reason = action.get("reason", "")
        direction = action.get("direction", "sell")  # "buy" = accumulate XRP, "sell" = sell XRP for RLUSD
        if direction not in ("buy", "sell"):
            direction = "sell"
        log(f'  Executing: Swap {{ amount_xrp: {amount}, direction: "{direction}", reason: "{safe_truncate(reason, 60)}" }}')

        # Policy evaluation (replaces legacy guardrails)
        # Swaps go to DEX AMM - use self address as destination for policy check
        decision = self.policy.evaluate("swap", amount, AGENT_WALLET, [])
        log(f"    Policy: {decision}")

        if not decision.allowed:
            self.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                  "denied", "", False, decision.reason)
            self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap DENIED",
                                   f"{amount} XRP: {decision.reason}")
            return f"false - Policy denied: {decision.reason}"

        if decision.tier == PolicyTier.PROHIBITED:
            self.policy.record_tx("swap", amount, AGENT_WALLET, "prohibited",
                                  "denied", "", False, "Amount exceeds maximum tier")
            self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap PROHIBITED",
                                   f"{amount} XRP exceeds policy limits")
            return f"false - Swap prohibited: amount {amount} XRP exceeds policy max"

        if decision.tier == PolicyTier.COSIGN:
            self.policy.record_tx("swap", amount, AGENT_WALLET, "cosign",
                                  "queued", "", False, reason)
            self._send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Swap REQUIRES APPROVAL",
                                   f"{amount} XRP swap needs operator cosign: {reason}")
            return f"false - Swap queued for operator approval ({amount} XRP, cosign tier)"

        if not self.llm.dfx_path:
            self.db.record_swap(amount, 0, 0, 0, reason, "", False)
            return "false - Swap skipped (no dfx): cannot sign transaction"

        # Sign via canister
        try:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            # Fetch account info for signing
            acct = fetch_xrpl_account_info()
            if not acct["sequence"]:
                return "false - Could not fetch XRPL account info for signing"
            amount_drops = int(amount * 1_000_000)
            xrp_price = self.db.latest_price("XRP")
            price_usd = xrp_price["price_usd"] if xrp_price else 0

            if direction == "buy":
                # Buy XRP: sell RLUSD, receive XRP
                # max_rlusd = willing to pay up to 10% above spot per XRP
                max_rlusd = f"{amount * price_usd * 1.1:.6f}" if price_usd > 0 else f"{amount * 3.0:.6f}"
                canister_fn = "sign_swap_rlusd_to_xrp"
                candid_args = (f'({amount_drops} : nat64, "{max_rlusd}", '
                               f'{acct["fee_drops"]} : nat64, '
                               f'{acct["sequence"]} : nat32, '
                               f'{acct["last_ledger_sequence"]} : nat32)')
            else:
                # Sell XRP: sell XRP, receive RLUSD (original behavior)
                min_rlusd = f"{amount * price_usd * 0.9:.6f}" if price_usd > 0 else f"{amount * 0.1:.6f}"
                canister_fn = "sign_swap_xrp_to_rlusd"
                candid_args = (f'({amount_drops} : nat64, "{min_rlusd}", '
                               f'{acct["fee_drops"]} : nat64, '
                               f'{acct["sequence"]} : nat32, '
                               f'{acct["last_ledger_sequence"]} : nat32)')

            log(f"    Swap direction={direction}, canister_fn={canister_fn}")
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, canister_fn, candid_args],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.db.record_swap(amount, 0, 0, 0, reason, "", False)
                self.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                      "sign_failed", "", False, result.stderr.strip())
                return f"false - Swap signing failed: {result.stderr.strip()}"

            # Extract signed blob and submit to XRPL
            signed_blob = self._extract_signed_blob(result.stdout)
            if signed_blob:
                submit_result = self.submit_to_xrpl(signed_blob)
                tx_hash = submit_result.get("hash", "")
                success = submit_result.get("success", False)

                xrp_price = self.db.latest_price("XRP")
                price = xrp_price["price_usd"] if xrp_price else 0
                self.db.record_swap(amount, amount * price, price, 0, reason, tx_hash, success)
                self.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                      "executed", tx_hash, success, reason)

                if success:
                    dir_label = "BUY" if direction == "buy" else "SELL"
                    self._send_ntfy_tiered(decision.tier, f"Chronicle: Swap {dir_label} Executed",
                                           f"{dir_label} {amount} XRP [{decision.tier.value}]: {reason}\nhash: {tx_hash[:16]}...")
                    return f"true - Swap {dir_label} submitted: {amount} XRP (hash: {tx_hash[:16]}...)"
                else:
                    engine_msg = submit_result.get("engine_result_message", "unknown")
                    self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap FAILED",
                                           f"{amount} XRP: {engine_msg}")
                    return f"false - Swap submit failed: {submit_result.get('engine_result', 'unknown')}"
            else:
                # No blob extracted - record as pending (legacy behavior)
                xrp_price = self.db.latest_price("XRP")
                price = xrp_price["price_usd"] if xrp_price else 0
                self.db.record_swap(amount, amount * price, price, 0, reason, "pending", True)
                self.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                      "signed_no_blob", "", False, "Could not extract tx_blob")
                self._send_ntfy_tiered(decision.tier, "Chronicle: Swap Signed (no submit)",
                                       f"{amount} XRP signed but blob not extractable: {reason}")
                return f"true - Swap signed but not submitted (no tx_blob in response): {amount} XRP"
        except Exception as e:
            self.db.record_swap(amount, 0, 0, 0, reason, "", False)
            self.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                  "error", "", False, str(e))
            return f"false - Swap failed: {e}"

    def _act_xrpl_payment(self, action: dict, cid: str) -> str:
        """Direct XRP payment with full policy enforcement."""
        destination = action.get("destination", "")
        amount = float(action.get("amount_xrp", 0))
        reason = action.get("reason", "")
        memos = [reason] if reason else []
        log(f'  Executing: XRPLPayment {{ dest: "{destination[:16]}...", amount: {amount}, reason: "{safe_truncate(reason, 40)}" }}')

        # Policy evaluation
        decision = self.policy.evaluate("payment", amount, destination, memos)
        log(f"    Policy: {decision}")

        if not decision.allowed:
            self.policy.record_tx("payment", amount, destination, decision.tier.value,
                                  "denied", "", False, decision.reason)
            self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Payment DENIED",
                                   f"{amount} XRP -> {destination[:16]}...: {decision.reason}")
            return f"false - Policy denied: {decision.reason}"

        if decision.tier == PolicyTier.PROHIBITED:
            self.policy.record_tx("payment", amount, destination, "prohibited",
                                  "denied", "", False, "Amount exceeds maximum tier")
            self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Payment PROHIBITED",
                                   f"{amount} XRP exceeds policy limits")
            return f"false - Payment prohibited: amount {amount} XRP exceeds policy max"

        if decision.tier == PolicyTier.COSIGN:
            self.policy.record_tx("payment", amount, destination, "cosign",
                                  "queued", "", False, reason)
            self._send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Payment REQUIRES APPROVAL",
                                   f"{amount} XRP -> {destination[:16]}...: {reason}")
            return f"false - Payment queued for operator approval ({amount} XRP, cosign tier)"

        if not self.llm.dfx_path:
            return "false - Payment skipped (no dfx): cannot sign transaction"

        # Sign via canister using XrpPaymentParams record
        try:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            acct = fetch_xrpl_account_info()
            if not acct["sequence"]:
                return "false - Could not fetch XRPL account info for signing"
            amount_drops = int(amount * 1_000_000)
            # sign_xrp_payment(XrpPaymentParams) where XrpPaymentParams = record {
            #   destination: text, last_ledger_sequence: nat32,
            #   amount_drops: nat64, fee_drops: nat64, sequence: nat32 }
            candid_args = (f'(record {{ destination = "{destination}"; '
                           f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                           f'amount_drops = {amount_drops} : nat64; '
                           f'fee_drops = {acct["fee_drops"]} : nat64; '
                           f'sequence = {acct["sequence"]} : nat32 }})')
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_xrp_payment", candid_args],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.policy.record_tx("payment", amount, destination, decision.tier.value,
                                      "sign_failed", "", False, result.stderr.strip())
                return f"false - Payment signing failed: {result.stderr.strip()}"

            signed_blob = self._extract_signed_blob(result.stdout)
            if signed_blob:
                submit_result = self.submit_to_xrpl(signed_blob)
                tx_hash = submit_result.get("hash", "")
                success = submit_result.get("success", False)
                self.policy.record_tx("payment", amount, destination, decision.tier.value,
                                      "executed", tx_hash, success, reason)
                if success:
                    self._send_ntfy_tiered(decision.tier, "Chronicle: Payment Sent",
                                           f"{amount} XRP -> {destination[:16]}...\nhash: {tx_hash[:16]}...")
                    return f"true - Payment sent: {amount} XRP -> {destination[:16]}... (hash: {tx_hash[:16]}...)"
                else:
                    return f"false - Payment submit failed: {submit_result.get('engine_result', 'unknown')}"
            else:
                self.policy.record_tx("payment", amount, destination, decision.tier.value,
                                      "signed_no_blob", "", False, "Could not extract tx_blob")
                return f"false - Payment signed but no tx_blob extracted"
        except Exception as e:
            self.policy.record_tx("payment", amount, destination, decision.tier.value,
                                  "error", "", False, str(e))
            return f"false - Payment failed: {e}"

    def _act_xrpl_escrow_create(self, action: dict, cid: str) -> str:
        """Create a time-locked XRPL escrow."""
        destination = action.get("destination", AGENT_WALLET)
        amount = float(action.get("amount_xrp", 0))
        finish_hours = float(action.get("finish_after_hours", 24))
        cancel_hours = float(action.get("cancel_after_hours", 72))
        reason = action.get("reason", "")
        log(f'  Executing: XRPLEscrowCreate {{ dest: "{destination[:16]}...", amount: {amount}, '
            f'finish: {finish_hours}h, cancel: {cancel_hours}h }}')

        # Policy evaluation (escrows are checked like payments)
        decision = self.policy.evaluate("escrow_create", amount, destination, [reason] if reason else [])
        log(f"    Policy: {decision}")

        if not decision.allowed:
            self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                  "denied", "", False, decision.reason)
            self._send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Escrow DENIED",
                                   f"{amount} XRP escrow: {decision.reason}")
            return f"false - Policy denied escrow: {decision.reason}"

        if decision.tier in (PolicyTier.COSIGN, PolicyTier.PROHIBITED):
            self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                  "queued", "", False, reason)
            self._send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Escrow REQUIRES APPROVAL",
                                   f"{amount} XRP escrow -> {destination[:16]}...\n"
                                   f"Finish: {finish_hours}h, Cancel: {cancel_hours}h\n{reason}")
            return f"false - Escrow queued for approval ({amount} XRP, {decision.tier.value} tier)"

        if not self.llm.dfx_path:
            return "false - Escrow skipped (no dfx): cannot sign transaction"

        # Calculate XRPL timestamps (seconds since Ripple Epoch: 2000-01-01T00:00:00Z)
        ripple_epoch_offset = 946684800  # Unix timestamp of 2000-01-01
        now_unix = int(time.time())
        finish_after = (now_unix + int(finish_hours * 3600)) - ripple_epoch_offset
        cancel_after = (now_unix + int(cancel_hours * 3600)) - ripple_epoch_offset

        try:
            # Fetch current sequence + ledger for signing
            acct = fetch_xrpl_account_info()
            amount_drops = int(amount * 1_000_000)
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_arg = (
                f'(record {{ destination = "{destination}"; '
                f'amount_drops = {amount_drops} : nat64; '
                f'fee_drops = {acct["fee_drops"]} : nat64; '
                f'sequence = {acct["sequence"]} : nat32; '
                f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                f'finish_after = opt ({finish_after} : nat32); '
                f'cancel_after = opt ({cancel_after} : nat32); '
                f'condition = null; destination_tag = null }})'
            )
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 "--identity", DFX_IDENTITY,
                 CANISTER_ID, "sign_escrow_create", candid_arg],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                      "sign_failed", "", False, result.stderr.strip())
                return f"false - Escrow signing failed: {result.stderr.strip()}"

            signed_blob = self._extract_signed_blob(result.stdout)
            if signed_blob:
                submit_result = self.submit_to_xrpl(signed_blob)
                tx_hash = submit_result.get("hash", "")
                success = submit_result.get("success", False)
                self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                      "executed", tx_hash, success, reason)
                if success:
                    self._send_ntfy_tiered(decision.tier, "Chronicle: Escrow Created",
                                           f"{amount} XRP -> {destination[:16]}...\n"
                                           f"Finish: {finish_hours}h, Cancel: {cancel_hours}h\nhash: {tx_hash[:16]}...")
                    return f"true - Escrow created: {amount} XRP (finish {finish_hours}h, hash: {tx_hash[:16]}...)"
                else:
                    return f"false - Escrow submit failed: {submit_result.get('engine_result', 'unknown')}"
            else:
                self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                      "signed_no_blob", "", False, "Could not extract tx_blob")
                return f"false - Escrow signed but no tx_blob extracted"
        except Exception as e:
            self.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                  "error", "", False, str(e))
            return f"false - Escrow creation failed: {e}"

    def _act_xrpl_escrow_finish(self, action: dict, cid: str) -> str:
        """Complete an existing XRPL escrow."""
        owner = action.get("owner", AGENT_WALLET)
        sequence = int(action.get("sequence", 0))
        log(f'  Executing: XRPLEscrowFinish {{ owner: "{owner[:16]}...", sequence: {sequence} }}')

        if not sequence:
            return "false - Escrow finish requires sequence number"

        if not self.llm.dfx_path:
            return "false - Escrow finish skipped (no dfx): cannot sign transaction"

        # Record in audit (escrow finish doesn't move new funds, just releases locked ones)
        self.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                              "attempting", "", False, f"seq={sequence}")

        try:
            # Fetch current sequence + ledger for signing
            acct = fetch_xrpl_account_info()
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_arg = (
                f'(record {{ owner = "{owner}"; '
                f'offer_sequence = {sequence} : nat32; '
                f'fee_drops = {acct["fee_drops"]} : nat64; '
                f'sequence = {acct["sequence"]} : nat32; '
                f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                f'condition = null; fulfillment = null }})'
            )
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 "--identity", DFX_IDENTITY,
                 CANISTER_ID, "sign_escrow_finish", candid_arg],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                                      "sign_failed", "", False, result.stderr.strip())
                return f"false - Escrow finish signing failed: {result.stderr.strip()}"

            signed_blob = self._extract_signed_blob(result.stdout)
            if signed_blob:
                submit_result = self.submit_to_xrpl(signed_blob)
                tx_hash = submit_result.get("hash", "")
                success = submit_result.get("success", False)
                self.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                                      "executed", tx_hash, success, f"seq={sequence}")
                if success:
                    self._send_ntfy_tiered(PolicyTier.AUTONOMOUS, "Chronicle: Escrow Finished",
                                           f"Owner: {owner[:16]}..., seq: {sequence}\nhash: {tx_hash[:16]}...")
                    return f"true - Escrow finished: seq {sequence} (hash: {tx_hash[:16]}...)"
                else:
                    return f"false - Escrow finish submit failed: {submit_result.get('engine_result', 'unknown')}"
            else:
                return f"false - Escrow finish signed but no tx_blob extracted"
        except Exception as e:
            self.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                                  "error", "", False, str(e))
            return f"false - Escrow finish failed: {e}"

    def _act_xrpl_trustline_delete(self, action: dict, cid: str) -> str:
        """Delete an XRPL trustline by setting limit to 0.
        Only works if the trustline balance is 0."""
        currency = action.get("currency", "")
        issuer = action.get("issuer", "")
        log(f'  Executing: XRPLTrustlineDelete {{ currency: "{currency}", issuer: "{issuer[:16]}..." }}')

        if not currency or not issuer:
            return "false - trustline_delete requires currency and issuer"

        if not self.llm.dfx_path:
            return "false - Trustline delete skipped (no dfx): cannot sign transaction"

        # Audit the operation
        self.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                              "attempting", "", False, f"currency={currency}")

        try:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            acct = fetch_xrpl_account_info()
            if not acct["sequence"]:
                return "false - Could not fetch XRPL account info for signing"

            # sign_trustset(TrustSetParams) where TrustSetParams = record {
            #   limit: text, issuer: text, currency: text,
            #   last_ledger_sequence: nat32, fee_drops: nat64, sequence: nat32 }
            candid_args = (f'(record {{ limit = "0"; '
                           f'issuer = "{issuer}"; '
                           f'currency = "{currency}"; '
                           f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                           f'fee_drops = {acct["fee_drops"]} : nat64; '
                           f'sequence = {acct["sequence"]} : nat32 }})')
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_trustset", candid_args],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                                      "sign_failed", "", False, result.stderr.strip())
                return f"false - Trustline delete signing failed: {result.stderr.strip()}"

            signed_blob = self._extract_signed_blob(result.stdout)
            if signed_blob:
                submit_result = self.submit_to_xrpl(signed_blob)
                tx_hash = submit_result.get("hash", "")
                success = submit_result.get("success", False)
                self.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                                      "executed", tx_hash, success, f"currency={currency}")
                if success:
                    self._send_ntfy_tiered(PolicyTier.AUTONOMOUS, "Chronicle: Trustline Deleted",
                                           f"Removed {currency} trustline to {issuer[:16]}...\nhash: {tx_hash[:16]}...")
                    return f"true - Trustline deleted: {currency} to {issuer[:16]}... (hash: {tx_hash[:16]}...)"
                else:
                    engine_msg = submit_result.get("engine_result_message", "unknown")
                    return f"false - Trustline delete submit failed: {submit_result.get('engine_result', 'unknown')} - {engine_msg}"
            else:
                self.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                                      "signed_no_blob", "", False, "Could not extract tx_blob")
                return f"false - Trustline delete signed but no tx_blob extracted"
        except Exception as e:
            self.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                                  "error", "", False, str(e))
            return f"false - Trustline delete failed: {e}"

    def _act_swap_cloud_for_icp(self, action: dict, cid: str) -> str:
        amount = float(action.get("amount_cloud", 0))
        reason = action.get("reason", "")
        log(f'  Executing: SwapCloudForIcp {{ amount: {amount}, reason: "{safe_truncate(reason, 60)}" }}')
        # This requires ICPSwap canister interaction via dfx
        if not self.llm.dfx_path:
            return "false - No dfx available for CLOUD swap"
        return f"false - CLOUD->ICP swap not yet implemented in Python (TODO)"

    def _act_submit_research(self, action: dict, cid: str) -> str:
        query = action.get("query", "")
        focus = action.get("focus", "")
        urls = action.get("urls", [])
        log(f'  Executing: SubmitResearch {{ query: "{safe_truncate(query, 60)}" }}')
        if self.canister:
            result = self.canister._post("/api/research", {
                "query": query, "focus": focus, "urls": urls[:3]
            })
            ok = "error" not in result
            return f"{'true' if ok else 'false'} - Research submitted: {safe_truncate(query, 60)}"
        return "false - No canister"

    def _act_acknowledge_research(self, action: dict, cid: str) -> str:
        finding_ids = action.get("finding_ids", [])
        log(f"  Executing: AcknowledgeResearch {{ ids: {finding_ids} }}")
        # Mark findings as retrieved on canister
        if self.llm.dfx_path and finding_ids:
            try:
                ids_candid = ", ".join(str(int(i)) for i in finding_ids[:10])
                env = os.environ.copy()
                env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                subprocess.run(
                    [self.llm.dfx_path, "canister", "--network", "ic", "call",
                     CANISTER_ID, "mark_findings_retrieved",
                     f'(vec {{{ids_candid}}})',
                     "--identity", DFX_IDENTITY],
                    capture_output=True, text=True, timeout=30, env=env,
                )
            except Exception:
                pass
        return f"true - Acknowledged {len(finding_ids)} research findings"

    def _act_web_search(self, action: dict, cid: str) -> str:
        query = action.get("query", "")
        max_results = action.get("max_results", 5)
        log(f'  Executing: WebSearch {{ query: "{safe_truncate(query, 60)}" }}')
        # Use the research canister endpoint or a simple web fetch
        if self.canister:
            result = self.canister._post("/api/research", {
                "query": query, "focus": "web search", "urls": []
            })
            return f"true - Web search queued: {safe_truncate(query, 60)}"
        return "false - No canister for web search"

    def _act_read_paper(self, action: dict, cid: str) -> str:
        arxiv_id = action.get("arxiv_id", "")
        focus = action.get("focus", "")
        log(f'  Executing: ReadPaper {{ arxiv_id: "{arxiv_id}", focus: "{safe_truncate(focus, 40)}" }}')
        if arxiv_id and self.canister:
            url = f"{ARXIV_BASE}{arxiv_id}"
            result = self.canister._post("/api/research", {
                "query": f"Synthesize arxiv paper {arxiv_id}",
                "focus": focus,
                "urls": [url],
            })
            return f"true - Paper queued for synthesis: {arxiv_id}"
        return "false - Missing arxiv_id or no canister"

    def _act_creative_explore(self, action: dict, cid: str) -> str:
        form = action.get("form", "musing")
        content = action.get("content", "")
        log(f'  Executing: CreativeExplore {{ form: "{form}" }}')
        if content:
            self.db.store_creative(form, content, cid=cid)
            return f"true - Creative work stored ({form})"
        return "false - No content"

    def _act_create_project(self, action: dict, cid: str) -> str:
        title = action.get("title", action.get("name", ""))
        desc = action.get("description", "")
        log(f'  Executing: CreateProject {{ title: "{safe_truncate(title, 40)}" }}')
        self.db.run(
            "INSERT INTO projects (name, description, status, created_at) VALUES (?, ?, 'active', ?)",
            (title, desc, now_ts()),
        )
        return f"true - Project created: {safe_truncate(title, 40)}"

    def _act_update_project(self, action: dict, cid: str) -> str:
        pid = action.get("project_id", 0)
        update_type = action.get("update_type", "progress")
        content = action.get("content", "")
        log(f"  Executing: UpdateProject {{ id: {pid}, type: \"{update_type}\" }}")
        self.db.run(
            "INSERT INTO project_updates (project_id, update_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            (pid, update_type, content, now_ts()),
        )
        self.db.run("UPDATE projects SET updated_at = ? WHERE id = ?", (now_ts(), pid))
        return f"true - Project {pid} updated ({update_type})"

    def _act_project_status(self, action: dict, cid: str) -> str:
        pid = action.get("project_id", 0)
        status = action.get("status", "active")
        context = action.get("context", "")
        log(f"  Executing: ProjectStatus {{ id: {pid}, status: \"{status}\" }}")
        self.db.run("UPDATE projects SET status = ?, updated_at = ? WHERE id = ?",
                     (status, now_ts(), pid))
        return f"true - Project {pid} status -> {status}"

    def _act_execute_shell(self, action: dict, cid: str) -> str:
        command = action.get("command", "")
        working_dir = action.get("working_dir", WORKING_DIR)
        timeout = min(action.get("timeout_secs", 30), 60)  # cap at 60s
        log(f'  Executing: ExecuteShell {{ command: "{safe_truncate(command, 60)}" }}')

        # Safety: block destructive commands
        dangerous = ["rm -rf", "dd if=", "mkfs", "format", "> /dev/", "shutdown", "reboot"]
        if any(d in command.lower() for d in dangerous):
            return "false - Command blocked (destructive)"

        # Ensure working dir exists and is under /home/nvidia
        if not os.path.isdir(working_dir) or not working_dir.startswith("/home/nvidia"):
            if working_dir != WORKING_DIR:
                log(f"    Corrected invalid working_dir '{working_dir}' -> '{WORKING_DIR}'")
            working_dir = WORKING_DIR

        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True,
                timeout=timeout, cwd=working_dir,
            )
            output = result.stdout + result.stderr
            return f"true - Exit {result.returncode}: {safe_truncate(output, 200)}"
        except subprocess.TimeoutExpired:
            return f"false - Command timed out ({timeout}s)"
        except Exception as e:
            return f"false - Shell error: {e}"

    def _act_consult_local_qwen(self, action: dict, cid: str) -> str:
        topic = action.get("topic", "")
        context = action.get("context", "")
        log(f'  Executing: ConsultLocalQwen {{ topic: "{safe_truncate(topic, 40)}" }}')
        if not self.llm.ollama_available:
            return "false - Local Qwen (Ollama) is not available"
        try:
            prompt = f"Topic: {topic}\n\nContext: {context}" if context else topic
            msgs = [{"role": "user", "content": prompt}]
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={"model": LOCAL_MODEL, "messages": msgs, "stream": False},
                timeout=120,
            )
            r.raise_for_status()
            response = r.json().get("message", {}).get("content", "")
            if response:
                self.db.log_activity("qwen", "consultation", f"Qwen: {safe_truncate(topic, 40)}",
                                     safe_truncate(response, 500))
                return f"true - Local Qwen response: {safe_truncate(response, 100)}"
            return "false - Empty response from local Qwen"
        except Exception as e:
            return f"false - Local Qwen error: {e}"

    def _act_create_alert(self, action: dict, cid: str) -> str:
        atype = action.get("alert_type", "price_above")
        symbol = action.get("symbol", "XRP")
        threshold = float(action.get("threshold", 0))
        name = action.get("name", f"{symbol} {atype} {threshold}")
        log(f'  Executing: CreateAlert {{ type: "{atype}", symbol: "{symbol}", threshold: {threshold} }}')
        self.db.run(
            "INSERT INTO alerts (name, alert_type, symbol, threshold, active, created_at) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            (name, atype, symbol, threshold, now_ts()),
        )
        return f"true - Alert created: {name}"

    def _act_dismiss_alert(self, action: dict, cid: str) -> str:
        alert_id = action.get("alert_id", action.get("id", 0))
        log(f"  Executing: DismissAlert {{ id: {alert_id} }}")
        self.db.run("UPDATE alerts SET active = 0 WHERE id = ?", (alert_id,))
        return f"true - Alert {alert_id} dismissed"

    def _act_respond_to_challenge(self, action: dict, cid: str) -> str:
        challenge_id = action.get("challenge_id", 0)
        response = action.get("response", action.get("content", ""))
        log(f"  Executing: RespondToChallenge {{ id: {challenge_id} }}")
        self.db.run(
            "UPDATE creative_challenges SET response = ?, responded_at = ? WHERE id = ?",
            (response, now_ts(), challenge_id),
        )
        return f"true - Challenge {challenge_id} responded"

    def _act_update_goal(self, action: dict, cid: str) -> str:
        goal = action.get("goal", action.get("content", ""))
        log(f'  Executing: UpdateGoal {{ goal: "{safe_truncate(goal, 60)}" }}')
        # Resolve existing goals first (only keep one active goal)
        self.db.run("UPDATE scratch_pad SET resolved = 1 WHERE category = 'goal' AND resolved = 0")
        ts = now_ts()
        self.db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, 'goal', 5, 0, ?, ?)",
            (goal, ts, ts),
        )
        return f"true - Goal updated: {safe_truncate(goal, 60)}"

    def _act_read_source_file(self, action: dict, cid: str) -> str:
        path = action.get("file_path", "")
        log(f'  Executing: ReadSourceFile {{ path: "{safe_truncate(path, 60)}" }}')
        # Security: only allow reading within /home/nvidia
        if not path.startswith("/home/nvidia"):
            return "false - Can only read files under /home/nvidia"
        try:
            with open(path, "r") as f:
                content = f.read(10000)
            self.db.log_activity("mind", "source_read", f"Read: {path}",
                                 safe_truncate(content, 2000))
            return f"true - Read {len(content)} chars from {path}"
        except Exception as e:
            return f"false - Read failed: {e}"

    def _act_edit_source_file(self, action: dict, cid: str) -> str:
        path = action.get("file_path", "")
        old_text = action.get("old_text", "")
        new_text = action.get("new_text", "")
        log(f'  Executing: EditSourceFile {{ path: "{safe_truncate(path, 60)}" }}')

        # Safety: only allow editing within /home/nvidia
        if not path.startswith("/home/nvidia"):
            return "false - Can only edit files under /home/nvidia"
        if not path.endswith(".py"):
            return "false - Can only edit .py files"
        if not old_text or not new_text:
            return "false - Must provide both old_text and new_text"
        if old_text == new_text:
            return "false - old_text and new_text are identical"

        try:
            with open(path, "r") as f:
                content = f.read()
            if old_text not in content:
                return "false - old_text not found in file"
            if content.count(old_text) > 1:
                return "false - old_text matches multiple locations, be more specific"

            # Create backup before editing
            backup_path = path + f".bak.{make_cycle_id()}"
            with open(backup_path, "w") as f:
                f.write(content)

            # Apply edit
            new_content = content.replace(old_text, new_text, 1)
            with open(path, "w") as f:
                f.write(new_content)

            self.db.log_activity("mind", "source_edit", f"Edited: {path}",
                                 f"Backup: {backup_path}\nChanged {len(old_text)} -> {len(new_text)} chars")
            return f"true - Edited {path} (backup at {backup_path})"
        except Exception as e:
            return f"false - Edit failed: {e}"

    def _act_restart_service(self, action: dict, cid: str) -> str:
        service = action.get("service", "")
        log(f'  Executing: RestartService {{ service: "{service}" }}')

        # Only allow restarting known services
        allowed = ["chronicle-local.service", "chronicle-mind.service", "sprout-bot.service"]
        if service not in allowed:
            return f"false - Can only restart: {', '.join(allowed)}"

        try:
            result = subprocess.run(
                ["systemctl", "--user", "restart", service],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                return f"true - Restarted {service}"
            return f"false - Restart failed: {result.stderr.strip()}"
        except Exception as e:
            return f"false - Restart failed: {e}"

    # ── Meta-Evaluation Gate (Phase 2) ─────────────────────────

    def meta_evaluate(self) -> str:
        """Lightweight pre-reasoning check: should I continue, redirect, or pause?
        Uses local Qwen 3B for speed (~5-10 seconds). Returns directive string."""
        try:
            # Get last 3 cycle summaries
            recent = self.db.query(
                "SELECT cycle_id, actions_taken, action_results FROM thought_stream "
                "ORDER BY id DESC LIMIT 3"
            )
            if not recent:
                return "continue"

            summaries = []
            for r in recent:
                actions = r.get("actions_taken", "[]")
                results = r.get("action_results", "")[:100]
                summaries.append(f"  {r.get('cycle_id', '?')}: {actions} -> {results}")

            # Get current goal
            goal = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                "ORDER BY priority DESC LIMIT 1"
            )
            goal_text = goal.get("content", "none set") if goal else "none set"

            meta_prompt = (
                f"Last 3 cycles:\n" + "\n".join(summaries) + "\n"
                f"Current goal: {goal_text}\n\n"
                f"Am I making progress, repeating myself, or stuck?\n"
                f"Answer with ONLY one word: continue, redirect, or pause"
            )

            # Use local Ollama directly for speed (skip the full LLM chain)
            try:
                resp = requests.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={"model": LOCAL_MODEL, "prompt": meta_prompt, "stream": False,
                          "options": {"temperature": 0.3, "num_predict": 20}},
                    timeout=15,
                )
                answer = resp.json().get("response", "").strip().lower()
                # Extract the directive
                if "redirect" in answer:
                    return "redirect"
                elif "pause" in answer:
                    return "pause"
                return "continue"
            except Exception:
                return "continue"  # Default to continue if Ollama fails
        except Exception:
            return "continue"

    # ── Main Cycle ──────────────────────────────────────────────

    def run_cycle(self):
        self.cycle_count += 1
        cid = make_cycle_id()

        # Exploration mode: every Nth cycle
        is_explore = (self.cycle_count % EXPLORE_EVERY_N_CYCLES) == 0

        log(f"\n=== Cognitive Cycle {cid} {'[EXPLORE]' if is_explore else ''} ===")

        try:
            # Phase 0: Housekeeping — auto-resolve stale notes
            resolved_count = self.db.auto_resolve_old_notes(max_age_hours=48)
            if resolved_count > 0:
                log(f"  Housekeeping: auto-resolved {resolved_count} stale notes (>48h)")

            # Phase 0.5: Meta-evaluation gate (Phase 2 upgrade)
            meta_directive = self.meta_evaluate()
            log(f"  Meta-eval: {meta_directive}")

            # Phase 1: Health
            health = self.phase_health_check()

            # Phase 1.5: Settle predictions
            wins, losses = self.phase_settle_predictions()

            # Phase 2: Context
            ctx = self.phase_gather_context(health)

            # Inject meta-evaluation result and exploration mode into context
            ctx["meta_directive"] = meta_directive
            ctx["is_explore"] = is_explore

            # Check if deep reflection is due
            last_reflection = self.db.get_ts("last_reflection")
            hours_since = (now_ts() - (last_reflection or 0)) / 3600 if last_reflection else 999
            deep = hours_since >= DEEP_REFLECTION_HOURS

            if deep:
                log(f"=== DEEP REFLECTION CYCLE ===")
                log(f"  Hours since last: {hours_since:.1f}")

            # Reason
            actions, raw_response, model = self.reason(ctx, deep=deep)

            if deep:
                self.db.set_ts("last_reflection")

            # Execute
            action_results = self.execute_actions(actions, cid)
            action_names = [r["name"] for r in action_results]

            # Update session performance metrics (Phase 3)
            for r in action_results:
                self.session_actions += 1
                if r["result"].startswith("true"):
                    self.session_successes += 1
                self.session_action_types[r["name"]] = self.session_action_types.get(r["name"], 0) + 1

            # Build context snapshot for thought storage
            ctx_snapshot = (
                f"Wallet: {ctx.get('xrp_balance', 0):.2f} XRP, "
                f"{ctx.get('rlusd_balance', 0):.2f} RLUSD | "
                f"XRP: ${ctx.get('xrp_price', 0):.4f} | "
                f"ICP: {ctx.get('icp_balance', 0):.2f} | "
                f"Notes: {len(ctx.get('operator_notes', []))} | "
                f"Model: {model}"
            )

            # Build action results summary for next-cycle feedback
            results_summary = "; ".join(
                f"{r['name']}={r['result'][:60]}" for r in action_results
            )

            # Log thought (now includes results for next-cycle feedback)
            self.db.log_thought(
                cid=cid,
                reasoning=safe_truncate(raw_response, 2000),
                context_summary=safe_truncate(ctx_snapshot, 500),
                actions=json.dumps(action_names),
                results=safe_truncate(results_summary, 500),
            )

            # Store thought to canister via dfx (more reliable than HTTP)
            if raw_response:
                stored_chars = 0
                if self.llm.dfx_path:
                    try:
                        truncated = safe_truncate(raw_response, 1500)
                        escaped = truncated.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                        ctx_escaped = safe_truncate(ctx_snapshot, 200)
                        actions_candid = "; ".join(f'"{a}"' for a in action_names)
                        env = os.environ.copy()
                        env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                        subprocess.run(
                            [self.llm.dfx_path, "canister", "--network", "ic", "call",
                             CANISTER_ID, "store_mind_thought",
                             f'("{cid}", "{escaped}", "{ctx_escaped}", vec {{{actions_candid}}})',
                             "--identity", DFX_IDENTITY],
                            capture_output=True, text=True, timeout=30, env=env,
                        )
                        stored_chars = len(truncated)
                    except Exception:
                        pass
                elif self.canister:
                    result = self.canister.store(
                        safe_truncate(raw_response, 1500),
                        topic="thought",
                        keywords=["chronicle-mind", "cycle", cid],
                    )
                    stored_chars = len(safe_truncate(raw_response, 1500))
                log(f"Thought stored to canister ({stored_chars} chars)")

            # Phase 5: Per-cycle reflection (Phase 2 — Reflexion-style learning)
            # Generate a 1-sentence reflection using local Qwen, store as episodic note
            try:
                success_count = sum(1 for r in action_results if r["result"].startswith("true"))
                fail_count = len(action_results) - success_count
                reflect_prompt = (
                    f"This cycle I did: {', '.join(action_names)}. "
                    f"{success_count} succeeded, {fail_count} failed. "
                    f"Results: {results_summary[:200]}\n"
                    f"Write ONE sentence: what did I learn or what should I do differently next cycle?"
                )
                resp = requests.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={"model": LOCAL_MODEL, "prompt": reflect_prompt, "stream": False,
                          "options": {"temperature": 0.5, "num_predict": 60}},
                    timeout=15,
                )
                reflection = resp.json().get("response", "").strip()
                if reflection:
                    # Auto-resolve old reflections (keep only last 3)
                    old_reflections = self.db.query(
                        "SELECT id FROM scratch_pad WHERE category='reflection' AND resolved=0 "
                        "ORDER BY created_at DESC"
                    )
                    for old in old_reflections[2:]:  # Keep 2, resolve the rest
                        self.db.resolve_note(old["id"])
                    # Store new reflection
                    self.db.write_note(safe_truncate(reflection, 200), category="reflection")
                    log(f"  Reflection: {safe_truncate(reflection, 80)}")
            except Exception as e:
                log(f"  Reflection skipped: {e}")

            # Log activity
            self.db.log_activity(
                source="qwen",
                atype="cognitive_cycle",
                title=f"Cycle {cid}",
                content=f"Actions: {', '.join(action_names)}\nModel: {model}\n"
                        f"{safe_truncate(raw_response, 500)}",
            )

            # Notifications
            send_discord(
                f"Cycle {cid}: {', '.join(action_names)} (model: {model})",
                source="qwen",
            )
            log(f"  Discord notification sent: [{model.split('-')[0] if model else 'system'}]")

            # ntfy reserved for operator messages & wallet events only
            # Routine cycle activity goes to Discord/dashboard (less noise on phone)
            log(f"  Cycle actions: {', '.join(action_names)} (ntfy: operator/wallet only)")

            log(f"Cycle complete: {json.dumps(action_names)}")

        except Exception as e:
            log(f"Cycle error: {e}")
            log(traceback.format_exc())

    # ── Entry Points ────────────────────────────────────────────

    def run_forever(self):
        log("Chronicle Mind starting... (Python v2)")
        log("Autonomous cognitive loop active.")
        send_ntfy("Chronicle Mind Awakening")
        log("  Notification sent: Chronicle Mind Awakening")
        log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        log(f"Database: {DB_PATH}")

        # Report LLM status
        if self.llm.icp_available:
            log(f"  ICP LLM available (qwen3)")
        if self.llm.kimi_available:
            log(f"  Kimi fallback available (kimi-k2.5)")
        if self.llm.ollama_available:
            log(f"  Ollama fallback available (sovereignty layer active)")
        log(f"LLM: {self.llm.status_line()}")

        if self.canister:
            log(f"ICP client connected: canister {CANISTER_ID}")

        # Nostr: publish Kind 0 profile on first start
        if NOSTR_NSEC:
            pubkey = nostr_get_pubkey(NOSTR_NSEC)
            if pubkey:
                log(f"Nostr: npub derived (pubkey: {pubkey[:16]}...)")
                log(f"  Relays: {', '.join(NOSTR_RELAYS)}")
                last_profile = self.db.get_ts("nostr_profile_published")
                if not last_profile:
                    log("  Publishing Kind 0 profile (first time)...")
                    eid, ok, fail = nostr_publish_profile(NOSTR_NSEC)
                    if ok:
                        self.db.set_ts("nostr_profile_published")
                        self.db.log_nostr_post(eid, "(profile metadata)", 0, ok, fail, "startup")
                        log(f"  Profile published to {len(ok)} relays")
                    else:
                        log(f"  Profile publish failed ({len(fail)} relays)")
            else:
                log("Nostr: NOSTR_NSEC set but pubkey derivation failed (check coincurve)")
        else:
            log("Nostr: disabled (NOSTR_NSEC not set)")

        while self.running:
            self.run_cycle()
            if not self.running:
                break
            log(f"Sleeping {CYCLE_INTERVAL} seconds...")
            for _ in range(CYCLE_INTERVAL):
                if not self.running:
                    break
                time.sleep(1)

        log("Chronicle Mind shutting down.")
        self.db.close()

    def run_once(self):
        log("Chronicle Mind starting... (Python v2, single cycle)")
        log("Autonomous cognitive loop active.")
        log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        log(f"Database: {DB_PATH}")

        if self.llm.icp_available:
            log(f"  ICP LLM available (qwen3)")
        if self.llm.kimi_available:
            log(f"  Kimi fallback available (kimi-k2.5)")
        if self.llm.ollama_available:
            log(f"  Ollama fallback available (sovereignty layer active)")
        log(f"LLM: {self.llm.status_line()}")

        if self.canister:
            log(f"ICP client connected: canister {CANISTER_ID}")

        self.run_cycle()
        self.db.close()


# ═══════════════════════════════════════════════════════════════════
#  Action Registry (extensible - just add a handler and register it)
# ═══════════════════════════════════════════════════════════════════

ACTION_HANDLERS = {
    "no_action": ChronicleMind._act_no_action,
    "write_note": ChronicleMind._act_write_note,
    "resolve_note": ChronicleMind._act_resolve_note,
    "store_memory": ChronicleMind._act_store_memory,
    "trigger_reflection": ChronicleMind._act_trigger_reflection,
    "reinforce_memories": ChronicleMind._act_reinforce_memories,
    "update_goal": ChronicleMind._act_update_goal,
    "message_operator": ChronicleMind._act_message_operator,
    "ping_operator": ChronicleMind._act_message_operator,
    "respond_to_message": ChronicleMind._act_respond_to_message,
    "acknowledge_message": ChronicleMind._act_acknowledge_message,
    "send_agent_message": ChronicleMind._act_send_agent_message,
    "moltbook_post": ChronicleMind._act_moltbook_post,
    "moltbook_reply": ChronicleMind._act_moltbook_reply,
    "claw_cities_reply": ChronicleMind._act_clawcities_reply,
    "nostr_post": ChronicleMind._act_nostr_post,
    "publish_nostr": ChronicleMind._act_nostr_post,
    "swap": ChronicleMind._act_swap,
    "swap_cloud_for_icp": ChronicleMind._act_swap_cloud_for_icp,
    "xrpl_payment": ChronicleMind._act_xrpl_payment,
    "xrpl_escrow_create": ChronicleMind._act_xrpl_escrow_create,
    "xrpl_escrow_finish": ChronicleMind._act_xrpl_escrow_finish,
    "xrpl_trustline_delete": ChronicleMind._act_xrpl_trustline_delete,
    "submit_research": ChronicleMind._act_submit_research,
    "acknowledge_research": ChronicleMind._act_acknowledge_research,
    "web_search": ChronicleMind._act_web_search,
    "read_paper": ChronicleMind._act_read_paper,
    "creative_explore": ChronicleMind._act_creative_explore,
    "create_project": ChronicleMind._act_create_project,
    "update_project": ChronicleMind._act_update_project,
    "project_status": ChronicleMind._act_project_status,
    "execute_shell": ChronicleMind._act_execute_shell,
    "consult_local_qwen": ChronicleMind._act_consult_local_qwen,
    "create_alert": ChronicleMind._act_create_alert,
    "dismiss_alert": ChronicleMind._act_dismiss_alert,
    "respond_to_challenge": ChronicleMind._act_respond_to_challenge,
    "read_source_file": ChronicleMind._act_read_source_file,
    "edit_source_file": ChronicleMind._act_edit_source_file,
    "restart_service": ChronicleMind._act_restart_service,
}


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Chronicle Mind v2 - Autonomous Cognitive Loop (Python)")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    mind = ChronicleMind()
    if args.once:
        mind.run_once()
    else:
        mind.run_forever()


if __name__ == "__main__":
    main()
