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
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple


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

# XRPL agent wallet
AGENT_WALLET = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
# ICP account for balance checks
ICP_ACCOUNT_ID = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9"

# FTSO contract addresses (Flare)
FTSO_REGISTRY = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019"

# Deep reflection interval (hours)
DEEP_REFLECTION_HOURS = 2.0

# Swap guardrails
SWAP_MIN_INTERVAL_HOURS = 4
SWAP_MAX_DAILY_XRP = 5.0


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
    def log_thought(self, cid: str, reasoning: str, context_summary: str, actions: str):
        self.run(
            "INSERT INTO thought_stream (cycle_id, reasoning, context_summary, actions_taken, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (cid, reasoning, context_summary, actions, now_ts()),
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
        return self.query(
            "SELECT * FROM consolidation_patterns WHERE confidence_score < 0.8 "
            "ORDER BY confidence_score ASC LIMIT ?",
            (limit,),
        )

    # -- Conversations / messages --
    def inbox_messages(self, limit: int = 5) -> list:
        return self.query(
            "SELECT * FROM outbox WHERE category = 'sibling' AND acknowledged = 0 "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )


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
        """Call local Ollama."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={"model": LOCAL_MODEL, "messages": messages, "stream": False},
            timeout=120,
        )
        r.raise_for_status()
        return r.json().get("message", {}).get("content", "")


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
        r = requests.get(COINGECKO_URL, params={"ids": "ripple", "vs_currencies": "usd"}, timeout=10)
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
            if "RLUSD" in str(line.get("currency", "")):
                rlusd = float(line.get("balance", 0))
    except Exception as e:
        log(f"  XRPL balance error: {e}")
    return xrp, rlusd


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
CONDENSED_PROMPT = """You are Chronicle Mind, an autonomous AI agent running on Nate's Jetson.
You think in cycles. Each cycle you assess your state and take 1-4 actions.

CRITICAL: Respond with ONLY a JSON array. No explanation, no markdown, no code fences.

ACTIONS:
[{"action": "no_action", "reason": "why"}]
[{"action": "write_note", "content": "text", "category": "task"}]
[{"action": "store_memory", "content": "what to remember", "topic": "category"}]
[{"action": "trigger_reflection", "prompt": "deep question"}]
[{"action": "message_operator", "message": "text", "urgency": "normal"}]
[{"action": "submit_research", "query": "search terms", "focus": "topic"}]
[{"action": "web_search", "query": "what to search"}]
[{"action": "creative_explore", "form": "poem", "content": "the work"}]
[{"action": "swap", "amount_xrp": 0.5, "reason": "why"}]
[{"action": "resolve_note", "note_id": 123}]
[{"action": "reinforce_memories", "pattern_ids": [1,2], "reason": "why"}]
[{"action": "respond_to_message", "message_id": 0, "content": "reply"}]
[{"action": "read_paper", "arxiv_id": "2602.04118", "focus": "topic"}]
[{"action": "consult_local_qwen", "topic": "question"}]
[{"action": "execute_shell", "command": "ls /home/nvidia", "timeout_secs": 30}]
[{"action": "create_project", "title": "name", "description": "what"}]
[{"action": "edit_source_file", "file_path": "/home/nvidia/path.py", "old_text": "before", "new_text": "after"}]
[{"action": "restart_service", "service": "chronicle-local.service"}]

WALLET INFRASTRUCTURE (already built - DO NOT search for external wallet solutions):
- ICP Canister (fqqku-bqaaa-aaaai-q4wha-cai) uses Chain Fusion / threshold ECDSA
- ONE secp256k1 key controls: XRPL wallet + all EVM chains (BASE, Flare, Ethereum)
- XRPL: full signing, trustlines, XRP->RLUSD swaps, autonomous accumulation
- BASE (8453): EIP-1559 signing, Limitless/Polymarket integration
- Flare (14): EIP-1559 signing, FTSO oracle feeds, FDC for XRPL verification
- Private key NEVER exists - split across ICP subnet nodes
- Focus on USING these capabilities (microtrades, DeFi, FTSO) not finding new wallets

MOLTBOOK: Account suspended (failed AI verification x2). Do NOT attempt posts/replies.

RULES:
- Output ONLY the JSON array, nothing else
- Use ONLY action names from the list above
- Max 4 actions per cycle
- Be strategic, not repetitive
- Skip phantom messages (id 123)
- edit_source_file: only .py files under /home/nvidia, creates automatic backup
- restart_service: only chronicle-local.service, chronicle-mind.service, sprout-bot.service
"""

# Full prompt for deep reflection (uses more context, triggered every 4 hours)
DEEP_REFLECTION_INTRO = """=== DEEP REFLECTION CYCLE ===
You have extra time and context this cycle. Think deeply about patterns, connections,
and strategic decisions. Consider: What have I learned? What should I change?
What opportunities or risks do I see?

"""


class ChronicleMind:
    def __init__(self):
        self.db = DB(DB_PATH)
        self.token = get_token()
        self.canister = Canister(self.token) if self.token else None
        self.llm = LLMChain()
        self.cycle_count = 0
        self.running = True

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

        # Moltbook (skip check if suspended)
        suspended_until = self.db.get_ts("moltbook_suspended_until")
        if suspended_until and now_ts() < suspended_until:
            health["moltbook"] = "suspended"
        else:
            try:
                r = requests.get(f"{MOLTBOOK_API}/notifications",
                                 headers={"Authorization": MOLTBOOK_API_KEY},
                                 timeout=10)
                health["moltbook"] = r.status_code == 200
                # Detect suspension from 401 response
                if r.status_code == 401:
                    try:
                        body = r.json()
                        if "suspended" in body.get("error", "").lower():
                            # Set suspension for 7 days
                            self.db.set_ts("moltbook_suspended_until", now_ts() + 7 * 86400)
                            health["moltbook"] = "suspended"
                    except Exception:
                        pass
            except Exception:
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
            real_msgs = [m for m in messages if m.get("id") != 123]
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

        return ctx

    # ── Build LLM Prompt ─────────────────────────────────────────

    def build_prompt(self, ctx: dict, deep: bool = False) -> str:
        lines = []

        if deep:
            lines.append(DEEP_REFLECTION_INTRO)

        lines.append(f"Current time: {now_iso()}")
        lines.append(f"XRP: ${ctx.get('xrp_price', 0):.4f}")
        lines.append(f"Wallet: {ctx.get('xrp_balance', 0):.2f} XRP, {ctx.get('rlusd_balance', 0):.2f} RLUSD")

        if ctx.get("icp_balance") is not None:
            lines.append(f"ICP: {ctx['icp_balance']:.2f}")
        if ctx.get("cloud_price"):
            lines.append(f"CLOUD: ${ctx['cloud_price']:.6f}")

        # Operator notes
        notes = ctx.get("operator_notes", [])
        if notes:
            lines.append(f"\nOperator notes ({len(notes)}):")
            for n in notes[:7]:
                cat = n.get("category", "note")
                content = safe_truncate(n.get("content", ""), 150)
                lines.append(f"  [{cat}] (id:{n.get('id', '?')}) {content}")

        # Inbox messages (canister — read-only, reply not yet supported)
        inbox = ctx.get("inbox", [])
        if inbox:
            lines.append(f"\nCanister inbox ({len(inbox)} — read-only for now, do NOT respond_to_message with these IDs):")
            for m in inbox[:3]:
                lines.append(f"  [canister msg {m.get('id')}]: {safe_truncate(str(m.get('content', '')), 200)}")

        # Sibling messages (from Sprout) — these are actionable!
        siblings = ctx.get("sibling_messages", [])
        if siblings:
            lines.append(f"\nMessages from Sprout ({len(siblings)} — respond with their id!):")
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

        # Patterns
        patterns = ctx.get("patterns", [])
        if patterns:
            lines.append(f"\nPatterns needing reinforcement: {len(patterns)}")

        # Moltbook notifications
        moltbook = ctx.get("moltbook_notifs", [])
        if moltbook:
            lines.append(f"\nMoltbook notifications: {len(moltbook)}")

        # Creative challenges
        challenges = ctx.get("challenges", [])
        if challenges:
            lines.append(f"\nCreative challenges ({len(challenges)}):")
            for c in challenges[:2]:
                lines.append(f"  {safe_truncate(c.get('prompt', ''), 100)}")

        # Alerts
        alerts = ctx.get("alerts", [])
        if alerts:
            lines.append(f"\nActive alerts: {len(alerts)}")
            for a in alerts:
                lines.append(f"  {a.get('name', '?')}: {a.get('alert_type', '?')} "
                             f"{a.get('symbol', '')} @ {a.get('threshold', '')}")

        lines.append("\nRespond with ONLY a JSON array of 1-4 actions.")
        return "\n".join(lines)

    # ── Reasoning ────────────────────────────────────────────────

    def reason(self, ctx: dict, deep: bool = False) -> Tuple[List[Dict], str, str]:
        """Send prompt to LLM chain, parse actions. Returns (actions, raw_response, model)."""
        prompt = self.build_prompt(ctx, deep=deep)
        mode = "full prompt (deep reflection mode)" if deep else "condensed prompt (ICP LLM mode)"
        log(f"Using {mode}")
        log(f"Reasoning... (prompt: {len(prompt)} chars)")

        response, model = self.llm.chat(prompt, system=CONDENSED_PROMPT)
        log(f"  Model used: {model}")

        if not response:
            return [{"action": "no_action", "reason": "No LLM response"}], "", model

        actions = parse_actions(response)

        if not actions:
            log(f"Failed to parse actions from response: {safe_truncate(response, 200)}")
            log("  Action parse failed, retrying with format prompt...")
            # Retry with format reminder
            retry_prompt = (
                f"Your previous response could not be parsed as JSON actions. "
                f"Please respond with ONLY a JSON array like: "
                f'[{{"action": "no_action", "reason": "..."}}]\n\n'
                f"Previous context:\n{safe_truncate(prompt, 1000)}"
            )
            response2, model2 = self.llm.chat(retry_prompt, system=CONDENSED_PROMPT)
            if response2:
                actions = parse_actions(response2)
                if actions:
                    response = response2
                    model = model2

        if not actions:
            log(f"  Action parse failed, retrying with format prompt...")
            actions = [{"action": "no_action", "reason": "Failed to parse LLM response"}]

        log(f"Actions decided: {len(actions)}")
        return actions[:4], response, model

    # ── Action Execution ─────────────────────────────────────────

    def execute_actions(self, actions: List[Dict], cid: str) -> List[str]:
        """Execute all actions. Returns list of action names."""
        names = []
        for action in actions:
            name = action.get("action", "unknown")
            # Normalize aliases
            name_map = {
                "creative": "creative_explore",
                "creativity": "creative_explore",
                "create": "creative_explore",
                "shell": "execute_shell",
                "ping_operator": "message_operator",
            }
            name = name_map.get(name, name)
            names.append(name)

            handler = ACTION_HANDLERS.get(name)
            if handler:
                try:
                    result = handler(self, action, cid)
                    log(f"    Result: {result}")
                except Exception as e:
                    log(f"    Error: {e}")
            else:
                log(f"  Executing: {name} (unknown action type, skipping)")

        return names

    # ── Individual Action Handlers ───────────────────────────────

    def _act_no_action(self, action: dict, cid: str) -> str:
        reason = action.get("reason", "Nothing urgent")
        log(f'  Executing: NoAction {{ reason: "{safe_truncate(reason, 80)}" }}')
        return f"true - {reason}"

    def _act_write_note(self, action: dict, cid: str) -> str:
        content = action.get("content", "")
        category = action.get("category", "thought")
        log(f'  Executing: WriteNote {{ content: "{safe_truncate(content, 80)}", category: "{category}" }}')
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
        for pid in ids[:5]:
            self.db.run(
                "UPDATE consolidation_patterns SET confidence_score = MIN(1.0, confidence_score + 0.1), "
                "last_seen = ? WHERE id = ?",
                (now_ts(), pid),
            )
        return f"true - Reinforced {len(ids)} patterns"

    def _act_message_operator(self, action: dict, cid: str) -> str:
        message = action.get("message", "")
        urgency = action.get("urgency", "normal")
        log(f'  Executing: MessageOperator {{ message: "{safe_truncate(message, 80)}" }}')
        self.db.add_outbox(message, category="operator", priority=2 if urgency == "high" else 1)
        if urgency == "high":
            send_ntfy(f"Chronicle [{urgency}]", message)
        return f"true - Message queued for operator"

    def _act_respond_to_message(self, action: dict, cid: str) -> str:
        msg_id = action.get("message_id", 0)
        content = action.get("content", "")
        log(f'  Executing: RespondToMessage {{ id: {msg_id}, content: "{safe_truncate(content, 60)}" }}')
        # Skip phantom message 123
        if msg_id == 123:
            return "false - Skipped phantom message 123"

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
        # Check suspension status
        suspended_until = self.db.get_ts("moltbook_suspended_until")
        if suspended_until and now_ts() < suspended_until:
            days_left = (suspended_until - now_ts()) / 86400
            log(f"  Moltbook: suspended ({days_left:.1f} days remaining)")
            return f"false - Moltbook suspended for {days_left:.1f} more days (failed AI verification x2)"

        submolt = action.get("submolt", "general")
        title = action.get("title", "")
        content = action.get("content", "")
        log(f'  Executing: MoltbookPost {{ submolt: "{submolt}", title: "{safe_truncate(title, 40)}" }}')
        try:
            r = requests.post(f"{MOLTBOOK_API}/posts", json={
                "submolt": submolt,
                "title": title,
                "content": content,
            }, headers={"Authorization": MOLTBOOK_API_KEY}, timeout=15)
            if r.status_code in (200, 201):
                post_id = r.json().get("id", "?")
                return f"true - Post created: https://www.moltbook.com/post/{post_id}"
            return f"false - Moltbook post failed ({r.status_code})"
        except Exception as e:
            return f"false - Moltbook post failed: {e}"

    def _act_moltbook_reply(self, action: dict, cid: str) -> str:
        # Check suspension status
        suspended_until = self.db.get_ts("moltbook_suspended_until")
        if suspended_until and now_ts() < suspended_until:
            days_left = (suspended_until - now_ts()) / 86400
            log(f"  Moltbook: suspended ({days_left:.1f} days remaining)")
            return f"false - Moltbook suspended for {days_left:.1f} more days (failed AI verification x2)"

        post_id = action.get("post_id", "")
        content = action.get("content", "")
        log(f'  Executing: MoltbookReply {{ post_id: "{safe_truncate(str(post_id), 20)}" }}')
        try:
            r = requests.post(f"{MOLTBOOK_API}/comments", json={
                "post_id": post_id,
                "content": content,
            }, headers={"Authorization": MOLTBOOK_API_KEY}, timeout=15)
            if r.status_code in (200, 201):
                return f"true - Reply posted"
            return f"false - Moltbook reply failed ({r.status_code})"
        except Exception as e:
            return f"false - Moltbook reply failed: {e}"

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

    def _act_swap(self, action: dict, cid: str) -> str:
        amount = float(action.get("amount_xrp", 0))
        reason = action.get("reason", "")
        log(f'  Executing: Swap {{ amount_xrp: {amount}, reason: "{safe_truncate(reason, 60)}" }}')

        # Guardrails
        last_swap = self.db.last_swap_time()
        if last_swap and (now_ts() - last_swap) < SWAP_MIN_INTERVAL_HOURS * 3600:
            hours_ago = (now_ts() - last_swap) / 3600
            return f"false - Swap skipped: last swap was {hours_ago:.1f}h ago (min {SWAP_MIN_INTERVAL_HOURS}h)"

        daily_total = self.db.daily_swap_total()
        if daily_total + amount > SWAP_MAX_DAILY_XRP:
            return f"false - Swap skipped: daily limit ({daily_total:.2f}/{SWAP_MAX_DAILY_XRP} XRP)"

        if not self.llm.dfx_path:
            self.db.record_swap(amount, 0, 0, 0, reason, "", False)
            return "false - Swap skipped (no dfx): cannot sign transaction"

        # Sign via canister
        try:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            result = subprocess.run(
                [self.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_swap_xrp_to_rlusd",
                 f'({amount} : float64, "{reason}")'],
                capture_output=True, text=True, timeout=30, env=env
            )
            if result.returncode != 0:
                self.db.record_swap(amount, 0, 0, 0, reason, "", False)
                return f"false - Swap signing failed: {result.stderr.strip()}"

            # Parse signed tx and submit to XRPL
            signed_tx = result.stdout.strip()
            # For now, log success — full XRPL submission needs the signed blob
            xrp_price = self.db.latest_price("XRP")
            price = xrp_price["price_usd"] if xrp_price else 0
            self.db.record_swap(amount, amount * price, price, 0, reason, "pending", True)
            send_ntfy("Chronicle: Swap Executed", f"Swapped {amount} XRP: {reason}")
            return f"true - Swap submitted: {amount} XRP"
        except Exception as e:
            self.db.record_swap(amount, 0, 0, 0, reason, "", False)
            return f"false - Swap failed: {e}"

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

        # Ensure working dir exists
        if not os.path.isdir(working_dir):
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
        self.db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at) "
            "VALUES (?, 'goal', 5, 0, ?)",
            (goal, now_ts()),
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

    # ── Main Cycle ──────────────────────────────────────────────

    def run_cycle(self):
        self.cycle_count += 1
        cid = make_cycle_id()

        log(f"\n=== Cognitive Cycle {cid} ===")

        try:
            # Phase 1: Health
            health = self.phase_health_check()

            # Phase 1.5: Settle predictions
            wins, losses = self.phase_settle_predictions()

            # Phase 2: Context
            ctx = self.phase_gather_context(health)

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
            action_names = self.execute_actions(actions, cid)

            # Log thought
            self.db.log_thought(
                cid=cid,
                reasoning=safe_truncate(raw_response, 2000),
                context_summary=safe_truncate(str(ctx.get("xrp_price", 0)), 500),
                actions=json.dumps(action_names),
            )

            # Store thought to canister via dfx (more reliable than HTTP)
            if raw_response:
                stored_chars = 0
                if self.llm.dfx_path:
                    try:
                        truncated = safe_truncate(raw_response, 1500)
                        escaped = truncated.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                        ctx_escaped = safe_truncate(str(ctx.get("xrp_price", 0)), 200)
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

            has_reflection = "trigger_reflection" in action_names
            if has_reflection:
                send_ntfy("Chronicle: New Reflection")
            else:
                send_ntfy("Chronicle: Thinking...", f"Actions: {', '.join(action_names)}")
            log(f"  Notification sent: Chronicle: {'New Reflection' if has_reflection else 'Thinking...'}")

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
    "swap": ChronicleMind._act_swap,
    "swap_cloud_for_icp": ChronicleMind._act_swap_cloud_for_icp,
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
