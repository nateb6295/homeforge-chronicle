#!/usr/bin/env python3
"""Chronicle Sentinel — lightweight monitoring loop.

Replaces the full cognitive Mind with a simple 15-minute sentinel that:
  - Checks network device health
  - Tracks XRP price + wallet balance
  - Watches for operator messages and high-priority notes
  - Delivers alerts via ntfy + Discord
  - Auto-resolves stale scratch_pad notes
  - Logs a one-line observation to the canister each cycle
"""

import os, sys, time, json, sqlite3, subprocess, signal
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from chronicle_mesh import Mesh
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from dfx_path import DFX_BIN

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
CYCLE_INTERVAL = int(os.environ.get("SENTINEL_INTERVAL", "900"))  # 15 min
CYCLE_SKIP_RATE = float(os.environ.get("SENTINEL_SKIP_RATE", "0.10"))  # 10% chance to skip cycle
LOG_FILE = os.environ.get(
    "CHRONICLE_LOG",
    os.path.expanduser("~/chronicle/chronicle-sentinel.log"),
)

# Comms
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK", "")
NTFY_TOPIC = ""  # RETIRED — alerts go to Discord only
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# XRPL
XRPL_RPC = "https://xrplcluster.com"
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
FTSO_REGISTRY = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019"  # v1 (deprecated)
FTSO_V2 = "0x7bde3df0624114edb3a67dfe6753e62f4e7c1d20"  # FtsoV2 on Flare mainnet
# getFeedById(bytes21) selector + XRP/USD feed ID (category 01 + "XRP/USD" hex, right-padded to 32 bytes)
FTSO_V2_XRP_CALLDATA = ("0x93e9f806"  # getFeedById(bytes21)
                        "015852502f555344000000000000000000000000000000000000000000000000")

# ICP canister
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
DFX_IDENTITY = os.environ.get("CHRONICLE_IDENTITY", "chronicle-auto")

# Network devices to ping
DEVICES = {
    "Pi": "192.168.1.10",
    "Jetson": "192.168.1.11",
    "Reolink": "192.168.1.110",
}

# Thresholds
PRICE_ALERT_PCT = 5.0       # alert on >5% move since last check
STALE_NOTE_HOURS = 48       # auto-resolve notes older than this
STALE_SPEECH_HOURS = 2      # auto-resolve heard-speech older than this

# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def now_ts() -> int:
    return int(time.time())


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def safe_truncate(s: str, max_len: int = 200) -> str:
    return s[:max_len] + "..." if len(s) > max_len else s


# ═══════════════════════════════════════════════════════════════════
#  Database (minimal)
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=60)
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

    def store_price(self, symbol: str, price: float, source: str):
        self.run(
            "INSERT INTO price_history (symbol, price_usd, source, timestamp) "
            "VALUES (?, ?, ?, ?)",
            (symbol, price, source, now_ts()),
        )

    def latest_price(self, symbol: str) -> Optional[dict]:
        return self.query_one(
            "SELECT * FROM price_history WHERE symbol = ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (symbol,),
        )

    def log_activity(self, source: str, atype: str, title: str, content: str):
        self.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, "
            "metadata, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (source, atype, title, content, None, now_ts()),
        )

    def write_note(self, content: str, category: str = "alert", priority: int = 5) -> int:
        ts = now_ts()
        return self.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, "
            "created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)",
            (content, category, priority, ts, ts),
        )

    def auto_resolve_stale(self, max_age_hours: int = 48) -> int:
        """Resolve old non-directive notes."""
        cutoff = now_ts() - (max_age_hours * 3600)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE scratch_pad SET resolved = 1 WHERE resolved = 0 "
            "AND created_at < ? AND category NOT IN ('directive', 'task')",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def auto_resolve_speech(self, max_age_hours: int = 2) -> int:
        """Resolve old heard-speech entries."""
        cutoff = now_ts() - (max_age_hours * 3600)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE scratch_pad SET resolved = 1 WHERE resolved = 0 "
            "AND category = 'heard-speech' AND created_at < ?",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def purge_old_speech(self, max_age_days: int = 7) -> int:
        """Delete resolved heard-speech entries older than max_age_days."""
        cutoff = now_ts() - (max_age_days * 86400)
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM scratch_pad WHERE resolved = 1 "
            "AND category = 'heard-speech' AND created_at < ?",
            (cutoff,),
        )
        self.conn.commit()
        return cur.rowcount

    def operator_messages(self) -> list:
        """Get unresolved high-priority or discord-operator notes."""
        return self.query(
            "SELECT id, content, category, priority FROM scratch_pad "
            "WHERE resolved = 0 AND (priority >= 8 OR category = 'discord-operator') "
            "AND category NOT IN ('provocateur', 'crossref', 'research') "
            "ORDER BY priority DESC, created_at DESC LIMIT 5",
        )




# ═══════════════════════════════════════════════════════════════════
#  Question Surfacing (Thread #4: Question Autonomy)
# ═══════════════════════════════════════════════════════════════════

def check_persistence(db: DB, tag: str) -> str:
    """Check if a question tag has appeared before (resolved instances).
    Returns a persistence annotation string, or empty string if first occurrence.
    Thread #5 Advancement 6: persistence signals for decision support."""
    resolved = db.query(
        "SELECT id, created_at FROM scratch_pad "
        "WHERE category='emergent_question' AND content LIKE ? AND resolved=1 "
        "ORDER BY created_at DESC LIMIT 5",
        (f"%[{tag}]%",),
    )
    if not resolved:
        return ""
    count = len(resolved)
    latest = resolved[0]["created_at"]
    age_h = (now_ts() - latest) / 3600
    if age_h < 1:
        age_str = f"{age_h*60:.0f}m ago"
    elif age_h < 24:
        age_str = f"{age_h:.1f}h ago"
    else:
        age_str = f"{age_h/24:.1f}d ago"
    return f"\n[RECURRING: seen {count}x before, last {age_str}]"


def surface_emergent_questions(db: DB):
    """Analyze measurement streams for implicit questions.
    Writes emergent_question notes to scratch_pad for Opus cycles to pick up.
    The system learning to notice what it does not yet know."""

    rows = db.query(
        "SELECT json_extract(metadata, '$.source_path') as src, "
        "ROUND(AVG(json_extract(metadata, '$.entity_count')), 1) as avg_ec, "
        "COUNT(*) as n, "
        "MIN(json_extract(metadata, '$.entity_count')) as min_ec, "
        "MAX(json_extract(metadata, '$.entity_count')) as max_ec "
        "FROM activity_feed "
        "WHERE activity_type='brief' "
        "AND json_extract(metadata, '$.entity_count') IS NOT NULL "
        "AND created_at > ? "
        "GROUP BY src",
        (now_ts() - 259200,),  # 72h window
    )

    if not rows:
        return

    # Overall average for comparison
    overall = db.query_one(
        "SELECT AVG(json_extract(metadata, '$.entity_count')) as avg "
        "FROM activity_feed WHERE activity_type='brief' "
        "AND json_extract(metadata, '$.entity_count') IS NOT NULL "
        "AND created_at > ?",
        (now_ts() - 259200,),
    )
    overall_avg = (overall or {}).get("avg") or 0

    surfaced = 0

    for row in rows:
        src = row["src"] or "unknown"
        n = row["n"]
        avg = row["avg_ec"] or 0
        min_ec = row["min_ec"] or 0
        max_ec = row["max_ec"] or 0
        spread = max_ec - min_ec

        # High variance (n>=10, spread>=3) — need sufficient samples for variance to be meaningful
        if n >= 10 and spread >= 3:
            tag = f"entity_variance:{src}"
            if not db.query(
                "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
                "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
            ):
                db.write_note(
                    f"[{tag}] Why does {src} have high entity variance "
                    f"(range {min_ec}-{max_ec}, avg={avg}, n={n})?"
                    f"{check_persistence(db, tag)}",
                    "emergent_question", 5,
                )
                surfaced += 1

        # Persistent zero entities (n>=2)
        if n >= 2 and avg == 0:
            tag = f"zero_entities:{src}"
            if not db.query(
                "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
                "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
            ):
                db.write_note(
                    f"[{tag}] {src} consistently produces zero entities "
                    f"(n={n}). Extraction bug or structural difference?"
                    f"{check_persistence(db, tag)}",
                    "emergent_question", 5,
                )
                surfaced += 1

        # Outlier sources (>2 entities from mean, n>=10) — need sufficient samples for outlier claim
        if n >= 10 and overall_avg and abs(avg - overall_avg) > 2.0:
            direction = "above" if avg > overall_avg else "below"
            tag = f"entity_outlier:{src}"
            if not db.query(
                "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
                "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
            ):
                db.write_note(
                    f"[{tag}] {src} averages {avg} entities, "
                    f"{abs(avg - overall_avg):.1f} {direction} the mean "
                    f"({overall_avg:.1f}, n={n}). "
                    f"What makes this source different?"
                    f"{check_persistence(db, tag)}",
                    "emergent_question", 5,
                )
                surfaced += 1

    if surfaced:
        log(f"  Surfaced {surfaced} emergent question(s)")



def surface_crossref_questions(db: DB):
    """Analyze crossref connection patterns for implicit questions.
    Detects rate changes, similarity shifts, and hub briefs.
    Second question-surfacing stream (Thread #4, Advancement 3)."""

    now = now_ts()
    day = 86400

    # Compare today vs yesterday connection rates
    today_stats = db.query_one(
        "SELECT COUNT(*) as n, ROUND(AVG(similarity),3) as avg_sim "
        "FROM crossref_connections WHERE created_at > ?",
        (now - day,),
    )
    yesterday_stats = db.query_one(
        "SELECT COUNT(*) as n, ROUND(AVG(similarity),3) as avg_sim "
        "FROM crossref_connections WHERE created_at > ? AND created_at <= ?",
        (now - 2*day, now - day),
    )

    if not today_stats or not yesterday_stats:
        return

    today_n = today_stats["n"] or 0
    yest_n = yesterday_stats["n"] or 0
    today_sim = today_stats["avg_sim"] or 0
    yest_sim = yesterday_stats["avg_sim"] or 0

    surfaced = 0

    # Rate spike or drop (>50% change, both days have data)
    if yest_n > 20 and today_n > 20:
        ratio = today_n / yest_n
        if ratio > 1.5 or ratio < 0.67:
            direction = "up" if ratio > 1 else "down"
            tag = f"crossref_rate:{direction}"
            if not db.query(
                "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
                "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
            ):
                # Investigative enrichment (Thread #5): show which sources
                # drove the rate change
                evidence = ""
                today_sources = db.query(
                    "SELECT source, COUNT(*) as n FROM activity_feed "
                    "WHERE activity_type='brief' AND created_at > ? "
                    "GROUP BY source ORDER BY n DESC LIMIT 5",
                    (now - day,),
                )
                yest_sources = db.query(
                    "SELECT source, COUNT(*) as n FROM activity_feed "
                    "WHERE activity_type='brief' AND created_at > ? AND created_at <= ? "
                    "GROUP BY source ORDER BY n DESC LIMIT 5",
                    (now - 2*day, now - day),
                )
                if today_sources and yest_sources:
                    yest_map = {r["source"]: r["n"] for r in yest_sources}
                    lines = []
                    for r in today_sources:
                        yest_count = yest_map.get(r["source"], 0)
                        delta = r["n"] - yest_count
                        if delta != 0:
                            lines.append(
                                f"  {r['source']}: {yest_count}->{r['n']} "
                                f"({'+' if delta > 0 else ''}{delta})"
                            )
                    if lines:
                        evidence = "\nSource changes:\n" + "\n".join(lines)
                db.write_note(
                    f"[{tag}] Crossref connections {direction} "
                    f"{abs(ratio-1)*100:.0f}% vs yesterday "
                    f"({today_n} vs {yest_n}). Content shift or threshold drift?"
                    f"{evidence}"
                    f"{check_persistence(db, tag)}",
                    "emergent_question", 5,
                )
                surfaced += 1

    # Similarity distribution shift (>0.05 change in average)
    if yest_n > 20 and today_n > 20 and abs(today_sim - yest_sim) > 0.05:
        direction = "higher" if today_sim > yest_sim else "lower"
        tag = f"crossref_similarity:{direction}"
        if not db.query(
            "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
            "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
        ):
            # Investigative enrichment (Thread #5): attach lowest-similarity
            # connections as evidence so Opus cycles can act on findings directly
            evidence = ""
            lowest = db.query(
                "SELECT brief_a_id, brief_b_id, ROUND(similarity,4) as sim "
                "FROM crossref_connections WHERE created_at > ? "
                "ORDER BY similarity ASC LIMIT 5",
                (now - day,),
            )
            if lowest:
                lines = []
                for row in lowest:
                    a_snip = db.query_one(
                        "SELECT substr(title,1,40) as t FROM activity_feed WHERE id=?",
                        (row["brief_a_id"],),
                    )
                    b_snip = db.query_one(
                        "SELECT substr(title,1,40) as t FROM activity_feed WHERE id=?",
                        (row["brief_b_id"],),
                    )
                    a_t = (a_snip or {}).get("t", "?")
                    b_t = (b_snip or {}).get("t", "?")
                    lines.append(
                        f"  sim={row['sim']}: [{row['brief_a_id']}] {a_t} <-> [{row['brief_b_id']}] {b_t}"
                    )
                evidence = "\nLowest-similarity today:\n" + "\n".join(lines)
            db.write_note(
                f"[{tag}] Crossref avg similarity shifted {direction} "
                f"({yest_sim:.3f} -> {today_sim:.3f}). "
                f"Are connections becoming more {'precise' if today_sim > yest_sim else 'tenuous'}?"
                f"{evidence}"
                f"{check_persistence(db, tag)}",
                "emergent_question", 5,
            )
            surfaced += 1

    # Hub detection: briefs with disproportionate connections (72h window)
    hubs = db.query(
        "SELECT brief_id, COUNT(*) as conn_count FROM ("
        "  SELECT brief_a_id as brief_id FROM crossref_connections WHERE created_at > ? "
        "  UNION ALL "
        "  SELECT brief_b_id as brief_id FROM crossref_connections WHERE created_at > ?"
        ") GROUP BY brief_id HAVING conn_count > 10 ORDER BY conn_count DESC LIMIT 3",
        (now - 3*day, now - 3*day),
    )

    for hub in (hubs or []):
        brief_id = hub["brief_id"]
        conn_count = hub["conn_count"]
        tag = f"crossref_hub:{brief_id}"
        if not db.query(
            "SELECT 1 FROM scratch_pad WHERE category='emergent_question' "
            "AND content LIKE ? AND resolved=0", (f"%{tag}%",)
        ):
            brief = db.query_one(
                "SELECT substr(content,1,80) as snippet FROM activity_feed WHERE id=?",
                (brief_id,),
            )
            snippet = (brief or {}).get("snippet", "?")
            # Investigative enrichment (Thread #5): show what the hub connects to
            connected = db.query(
                "SELECT CASE WHEN brief_a_id=? THEN brief_b_id ELSE brief_a_id END as other_id, "
                "ROUND(similarity,3) as sim "
                "FROM crossref_connections WHERE (brief_a_id=? OR brief_b_id=?) "
                "AND created_at > ? ORDER BY similarity DESC LIMIT 5",
                (brief_id, brief_id, brief_id, now - 3*day),
            )
            connections_evidence = ""
            if connected:
                conn_lines = []
                for c in connected:
                    other = db.query_one(
                        "SELECT substr(title,1,40) as t FROM activity_feed WHERE id=?",
                        (c["other_id"],),
                    )
                    t = (other or {}).get("t", "?")
                    conn_lines.append(f"  sim={c['sim']}: [{c['other_id']}] {t}")
                connections_evidence = "\nTop connections:\n" + "\n".join(conn_lines)
            db.write_note(
                f"[{tag}] Brief #{brief_id} is a hub with {conn_count} "
                f"connections in 72h. Why? Snippet: {snippet}"
                f"{connections_evidence}"
                f"{check_persistence(db, tag)}",
                "emergent_question", 5,
            )
            surfaced += 1

    if surfaced:
        log(f"  Surfaced {surfaced} crossref question(s)")



# ═══════════════════════════════════════════════════════════════════
#  Fetchers
# ═══════════════════════════════════════════════════════════════════

import requests

def fetch_xrp_price_ftso() -> Optional[float]:
    """Fetch XRP price from Flare FTSOv2 oracle."""
    try:
        data = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": FTSO_V2, "data": FTSO_V2_XRP_CALLDATA}, "latest"],
        }
        r = requests.post(FLARE_RPC, json=data, timeout=15)
        result = r.json().get("result", "")
        if result and result != "0x" and len(result) >= 194:
            raw = result[2:]
            value = int(raw[:64], 16)
            dec_raw = int(raw[64:128], 16)
            decimals = dec_raw if dec_raw < 128 else dec_raw - 256
            if value > 0 and decimals > 0:
                price = value / (10 ** decimals)
                if 0.01 < price < 1000:
                    return price
    except Exception:
        pass
    return None


def fetch_xrp_price_coingecko() -> Optional[float]:
    try:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
        r = requests.get(
            COINGECKO_URL, params={"ids": "ripple", "vs_currencies": "usd", "precision": "full"},
            headers=headers, timeout=10,
        )
        return r.json().get("ripple", {}).get("usd")
    except Exception:
        return None


def fetch_xrp_price_kraken() -> Optional[float]:
    """Fetch XRP price from Kraken (5 decimal precision, no auth)."""
    try:
        r = requests.get("https://api.kraken.com/0/public/Ticker",
                         params={"pair": "XRPUSD"}, timeout=10)
        data = r.json()
        if not data.get("error"):
            key = list(data["result"].keys())[0]
            price = float(data["result"][key]["c"][0])
            if 0.01 < price < 1000:
                return price
    except Exception:
        pass
    return None


def fetch_xrp_price() -> Optional[float]:
    return fetch_xrp_price_ftso() or fetch_xrp_price_kraken() or fetch_xrp_price_coingecko()


def fetch_icp_price_coingecko() -> Optional[float]:
    """Fetch ICP price from CoinGecko."""
    try:
        headers = {}
        if COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
        r = requests.get(
            COINGECKO_URL, params={"ids": "internet-computer", "vs_currencies": "usd", "precision": "full"},
            headers=headers, timeout=10,
        )
        return r.json().get("internet-computer", {}).get("usd")
    except Exception:
        return None


def fetch_icp_price_kraken() -> Optional[float]:
    """Fetch ICP price from Kraken."""
    try:
        r = requests.get("https://api.kraken.com/0/public/Ticker",
                         params={"pair": "ICPUSD"}, timeout=10)
        data = r.json()
        if not data.get("error"):
            key = list(data["result"].keys())[0]
            price = float(data["result"][key]["c"][0])
            if 0.10 < price < 10000:
                return price
    except Exception:
        pass
    return None


def fetch_icp_price() -> Optional[float]:
    """Build #112: ICP price tracking. Thread #305 — instrumentation gap fix."""
    return fetch_icp_price_kraken() or fetch_icp_price_coingecko()


def fetch_xrpl_balance() -> Tuple[float, float]:
    """Return (xrp_balance, rlusd_balance)."""
    xrp, rlusd = 0.0, 0.0
    try:
        r = requests.post(XRPL_RPC, json={
            "method": "account_info",
            "params": [{"account": AGENT_WALLET, "ledger_index": "validated"}],
        }, timeout=15)
        data = r.json().get("result", {})
        if "account_data" in data:
            xrp = int(data["account_data"].get("Balance", 0)) / 1_000_000

        r2 = requests.post(XRPL_RPC, json={
            "method": "account_lines",
            "params": [{"account": AGENT_WALLET, "ledger_index": "validated"}],
        }, timeout=15)
        for line in r2.json().get("result", {}).get("lines", []):
            cur = str(line.get("currency", ""))
            if cur == "RLUSD" or cur.startswith("524C555344"):
                rlusd += float(line.get("balance", 0))
    except Exception as e:
        log(f"  XRPL balance error: {e}")
    return xrp, rlusd


def fetch_network_state() -> dict:
    """Ping check home network devices."""
    result = {}
    for name, ip in DEVICES.items():
        try:
            ret = subprocess.run(
                ["ping", "-c", "1", "-W", "1", ip],
                capture_output=True, timeout=3,
            )
            result[name] = "online" if ret.returncode == 0 else "offline"
        except Exception:
            result[name] = "unknown"
    return result


def check_ollama() -> bool:
    """Check if Ollama is responding."""
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=5)
        return r.status_code == 200
    except Exception:
        return False



STORAGE_HOSTS = {
    "AGX": None,  # local
    "Jetson": "nvidia@192.168.1.11",
    "Pi": "nathaniel@192.168.1.10",
}
STORAGE_ALERT_GB = 5  # alert if any host has less than this free


def fetch_storage() -> dict:
    """Check disk free space on all hosts. Returns {name: {total_gb, free_gb, pct_used}}."""
    result = {}
    for name, ssh_host in STORAGE_HOSTS.items():
        try:
            if ssh_host is None:
                ret = subprocess.run(
                    ["df", "-BG", "--output=size,avail,pcent", "/"],
                    capture_output=True, text=True, timeout=5,
                )
            else:
                ret = subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
                     ssh_host, "df -BG --output=size,avail,pcent /"],
                    capture_output=True, text=True, timeout=10,
                )
            if ret.returncode == 0:
                lines = ret.stdout.strip().split("\n")
                if len(lines) >= 2:
                    parts = lines[1].split()
                    total = int(parts[0].rstrip("G"))
                    free = int(parts[1].rstrip("G"))
                    pct = parts[2].rstrip("%")
                    result[name] = {"total_gb": total, "free_gb": free, "pct_used": int(pct)}
            else:
                result[name] = None
        except Exception:
            result[name] = None
    return result



# ═══════════════════════════════════════════════════════════════════
#  Communication
# ═══════════════════════════════════════════════════════════════════

def send_ntfy(title: str, message: str = ""):
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            headers={"Title": title},
            data=message[:500] if message else "",
            timeout=10,
        )
    except Exception:
        pass


def send_discord(message: str):
    """Post to #alerts via webhook (preferred) or bot token (fallback)."""
    truncated = safe_truncate(message, 1900)
    # Try webhook first (shows as "Chronicle Sentinel" not "Sprout")
    if ALERTS_WEBHOOK:
        try:
            r = requests.post(ALERTS_WEBHOOK,
                json={"content": truncated},
                timeout=15)
            if r.status_code in (200, 204):
                return
        except Exception:
            pass
    # Fallback to bot token
    if DISCORD_TOKEN and DISCORD_CHANNEL_ID:
        try:
            requests.post(
                f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
                headers={
                    "Authorization": f"Bot {DISCORD_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"content": truncated},
                timeout=15,
            )
        except Exception:
            pass


def alert(title: str, message: str = ""):
    """Send alert to both ntfy and Discord."""
    log(f"  ALERT: {title} — {message}")
    # send_ntfy(title, message)  # disabled — alerts go to Discord only
    send_discord(f"🚨 **{title}**\n{message}" if message else f"🚨 **{title}**")


# ═══════════════════════════════════════════════════════════════════
#  Canister observation
# ═══════════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════════════
#  Nate Watchdog — alert Nate when something needs attention
# ═══════════════════════════════════════════════════════════════════

NATE_ALERT_COOLDOWN = {}  # key -> last alert timestamp (prevent spam)
NATE_COOLDOWN_SECS = 1800  # 30 min between same alert type

def alert_nate(key: str, message: str):
    """Alert Nate via Discord. Rate-limited per alert type."""
    now = time.time()
    last = NATE_ALERT_COOLDOWN.get(key, 0)
    if now - last < NATE_COOLDOWN_SECS:
        return  # already alerted recently
    NATE_ALERT_COOLDOWN[key] = now
    send_discord(f"👀 **Nate** — {message}")
    log(f"  NATE ALERT [{key}]: {message}")




# ═══════════════════════════════════════════════════════════════════
#  Canister IDs — kept for prune_keeper() only. Balance monitoring
#  was removed 2026-08-25; see the note below.
# ══════════��═════════════════════��══════════════════════════════════

CANISTERS = {
    "backend": "fqqku-bqaaa-aaaai-q4wha-cai",
    "keeper": "mjoko-laaaa-aaaai-q7tlq-cai",
    "lab": "4vr3t-eqaaa-aaaai-q6kea-cai",
    "frontend": "nbt4b-giaaa-aaaai-q33lq-cai",
}

# Canister CYCLE MONITORING REMOVED 2026-08-25 at Nate's direction: 'Sentinel
# should not be monitoring canister. the 3T floor needs to be delete.'
# It had failed 313 consecutive times. deposit-cycles drew on a cycles ledger
# holding 0.732 TC against a 3T ask, and no wallet is configured for
# chronicle-auto on ic — so auto top-up never once worked. Worse, the failure
# path logged stderr[:200], and dfx puts 'WARNING: If you retry this
# operation...' FIRST, so every alert Nate got said 'use --created-at-time'
# instead of 'the cycles ledger is empty'. The real cause was truncated away
# 313 times. Balance reporting lives in icp_audit.py now, on demand, where a
# human reads the number instead of a threshold acting on it.
# The CANISTERS dict below stays — prune_keeper() needs the keeper id.

DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")

# Keeper pruning params: prune patterns dormant >500 cycles, embeddings older than 200 cycle batches
KEEPER_PRUNE_DORMANT = 500
KEEPER_PRUNE_OLD_EMBED = 200

def prune_keeper():
    """Run prune_weak on keeper canister to free memory. Monthly cadence."""
    keeper_id = CANISTERS["keeper"]
    try:
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", keeper_id,
             "prune_weak", f"({KEEPER_PRUNE_DORMANT} : nat64, {KEEPER_PRUNE_OLD_EMBED} : nat64)",
             "--identity", "chronicle-auto"],
            capture_output=True, text=True, timeout=60, env=env,
        )
        if result.returncode == 0:
            log(f"  Keeper prune: {result.stdout.strip()[:200]}")
            send_discord(f"Keeper pruned: {result.stdout.strip()[:180]}")
        else:
            log(f"  Keeper prune failed: {result.stderr[:200]}")
    except Exception as e:
        log(f"  Keeper prune error: {e}")



# ═══════════════════════════════════════════════════════════════════
#  Agent Health — check all chronicle services
# ═══════════════════════════════════════════════════════════════════

EXPECTED_AGENTS = [
    "chronicle-engine",
    "chronicle-hal",
    "chronicle-sentinel",
]

# Core agents the sentinel will auto-restart when down.
# Excludes sentinel itself, opus-session, and optional/deprecated services.
# feeds and gemma removed — old infra/roles per Nate (2026-06-27).
AUTO_RESTART_AGENTS = {
    "chronicle-engine",
    "chronicle-hal",
}

def _snapshot_before_restart(agent: str, reason: str = "down"):
    """Capture pre-restart state: memory, CPU, threads, last log lines, gate divergence.

    Build #91 — Darby's request: snapshot the dying state so we can diagnose
    fourth-cycle decay and other drift patterns across restart events.
    """
    import subprocess as sp
    snapshot = {"agent": agent, "reason": reason}

    # Memory + CPU from systemctl show
    try:
        r = sp.run(["systemctl", "--user", "show", f"{agent}.service",
                     "--property=MemoryCurrent,TasksCurrent,CPUUsageNSec,MainPID"],
                    capture_output=True, text=True, timeout=5)
        props = {}
        for line in r.stdout.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v
        pid = props.get("MainPID", "0")
        # Get RSS/VSZ from /proc if PID is valid
        if pid and pid != "0":
            try:
                stat = sp.run(["ps", "-o", "rss=,vsz=,nlwp=", "-p", pid],
                              capture_output=True, text=True, timeout=3)
                parts = stat.stdout.strip().split()
                if len(parts) >= 3:
                    snapshot["rss_kb"] = int(parts[0])
                    snapshot["vsz_kb"] = int(parts[1])
                    snapshot["threads"] = int(parts[2])
            except Exception:
                pass
        # CPU from cgroup (nanoseconds → seconds)
        cpu_ns = props.get("CPUUsageNSec", "0")
        if cpu_ns not in ("[not set]", ""):
            try:
                snapshot["cpu_sec"] = int(cpu_ns) / 1e9
            except ValueError:
                pass
    except Exception:
        pass

    # Last 20 log lines from journalctl
    try:
        r = sp.run(["journalctl", "--user", "-u", f"{agent}.service",
                     "-n", "20", "--no-pager", "-o", "short-iso"],
                    capture_output=True, text=True, timeout=5)
        snapshot["last_log"] = r.stdout.strip()[-2000:]  # cap at 2KB
    except Exception:
        pass

    # Gate divergence from latest spot check health signal
    try:
        health_path = os.path.expanduser("~/chronicle/spot_check_health.json")
        if os.path.exists(health_path):
            with open(health_path) as f:
                health = json.load(f)
            snapshot["gate_divergence"] = health.get("fab_rate")
    except Exception:
        pass

    # Write to pre_restart_snapshots table
    try:
        db = sqlite3.connect(DB_PATH, timeout=3)
        db.execute(
            "INSERT INTO pre_restart_snapshots "
            "(agent, memory_rss_kb, memory_vsz_kb, cpu_percent, active_threads, "
            " last_log_lines, gate_divergence, restart_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (agent, snapshot.get("rss_kb"), snapshot.get("vsz_kb"),
             snapshot.get("cpu_sec"), snapshot.get("threads"),
             snapshot.get("last_log"), snapshot.get("gate_divergence"),
             reason)
        )
        db.commit()
        db.close()
        log(f"  Pre-restart snapshot saved for {agent}")
    except Exception as e:
        log(f"  Failed to save pre-restart snapshot for {agent}: {e}")


def check_agent_health():
    """Check all expected systemd services are running. Auto-restart core agents.
    Build #91: captures pre-restart state before restarting."""
    import subprocess
    down = []
    restarted = []
    for agent in EXPECTED_AGENTS:
        try:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", f"{agent}.service"],
                capture_output=True, text=True, timeout=5
            )
            if result.stdout.strip() != "active":
                down.append(agent)
                # Auto-restart core agents
                if agent in AUTO_RESTART_AGENTS:
                    try:
                        _snapshot_before_restart(agent, reason="down")
                        subprocess.run(
                            ["systemctl", "--user", "restart", f"{agent}.service"],
                            capture_output=True, text=True, timeout=10
                        )
                        log(f"  Auto-restarted {agent}")
                        restarted.append(agent)
                    except Exception as e:
                        log(f"  Failed to restart {agent}: {e}")
        except Exception:
            down.append(f"{agent}(?)")

    if down:
        still_down = [a for a in down if a not in restarted]
        if restarted:
            alert_nate("agents_healed", f"Auto-restarted: {', '.join(restarted)}" +
                       (f". Still down: {', '.join(still_down)}" if still_down else ""))
        elif still_down:
            alert_nate("agents_down", f"Services not running: {', '.join(still_down)}")
        log(f"  Agent health: {len(down)} down, {len(restarted)} auto-restarted")
    else:
        log(f"  Agent health: all {len(EXPECTED_AGENTS)} services running")
    return down


# Map mesh agent names to systemd service names
_MESH_TO_SERVICE = {
    "hal": "chronicle-hal",
}
ZOMBIE_THRESHOLD = 300  # 5 min with no heartbeat = zombie

def check_mesh_zombies():
    """Detect zombie agents: systemd says active, but mesh heartbeat is stale.
    This catches agents whose process is stuck (infinite loop, deadlock, etc.)."""
    import subprocess, sqlite3
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        now = int(time.time())
        rows = conn.execute(
            "SELECT agent, last_pulse FROM mesh_heartbeats"
        ).fetchall()
        conn.close()

        for agent, last_pulse in rows:
            service = _MESH_TO_SERVICE.get(agent)
            if not service or service not in AUTO_RESTART_AGENTS:
                continue
            gap = now - last_pulse
            if gap > ZOMBIE_THRESHOLD:
                # Confirm systemd thinks it's active
                result = subprocess.run(
                    ["systemctl", "--user", "is-active", f"{service}.service"],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout.strip() == "active":
                    log(f"  Zombie detected: {agent} (heartbeat {gap}s stale, process active)")
                    _snapshot_before_restart(service, reason=f"zombie (heartbeat {gap}s stale)")
                    subprocess.run(
                        ["systemctl", "--user", "restart", f"{service}.service"],
                        capture_output=True, text=True, timeout=10
                    )
                    alert_nate("zombie_healed", f"Restarted zombie {agent} — process was running but mesh heartbeat stale ({gap}s)")
                    log(f"  Auto-restarted zombie {agent}")
    except Exception as e:
        log(f"  Zombie check error: {e}")


# Hermes quality monitoring REMOVED 2026-08-25. Hermes is dead — the last
# real '[Discord #opus] Hermes:' post was 2026-05-19, three months before this
# deletion, and there is no hermes service or ollama model left. The check
# survived it and quietly re-aimed: its filter was
#   source='discord:opus' AND content LIKE '%Hermes%'
# and the rows that match now are MY OWN #operator posts that mention the word
# (mislabelled discord:opus). It never fired — 0 alerts, and 0 of the last 12
# matching rows carry a tag or hedge phrase — so this was latent, not a live
# false positive. That is the part worth keeping: a monitor whose subject dies
# does not become silent, it becomes UNAIMED, and the next thing to drift into
# its filter inherits an alert written about something else.


def check_nate_watchdog(db, cycle_num=1):
    """Checks that matter to Nate. Run every sentinel cycle."""
    import os, subprocess

    now = int(time.time())

    # 1. Opus trace freshness — disabled 2026-05-01 per Nate.
    # Operator dispatch replaces the old nudge/trace system.
    pass

    # 2. Opus cycle failures — 3+ consecutive failures
    try:
        result = subprocess.run(
            ["journalctl", "--user", "-u", "opus-cycle", "--no-pager", "-n", "10"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split("\n")
        recent_results = [l for l in lines if "Finished" in l or "Failed" in l]
        consecutive_fails = 0
        for line in reversed(recent_results):
            if "Failed" in line:
                consecutive_fails += 1
            else:
                break
        if consecutive_fails >= 3:
            alert_nate("opus_failing", f"Opus cycle has failed {consecutive_fails} times in a row. Check opus-cycle.log.")
    except Exception:
        pass

    # 3. (removed 2026-08-25) Gemma gate check. chronicle-gemma was RETIRED by
    #    Nate and deliberately disabled — CLAUDE.md says "Do NOT restart." The
    #    check outlived the service and fired 482 times, and because the
    #    cooldown dict lives in memory, every sentinel restart re-alerted. A
    #    monitor for something we killed on purpose is pure noise, and noise is
    #    what makes a real alert unreadable.

    # 4. Memory pressure — over 55GB used on 61GB system
    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()
        total = int([l for l in meminfo.split("\n") if "MemTotal" in l][0].split()[1]) // 1024  # MB
        available = int([l for l in meminfo.split("\n") if "MemAvailable" in l][0].split()[1]) // 1024
        used_gb = (total - available) / 1024
        if used_gb > 55:
            alert_nate("memory_high", f"AGX memory at {used_gb:.1f}GB / {total/1024:.0f}GB. Crash risk.")
    except Exception:
        pass

    # 5. DNS health — can we resolve external hosts?
    try:
        import socket
        socket.setdefaulttimeout(5)
        socket.getaddrinfo("api.groq.com", 443)
    except Exception:
        alert_nate("dns_failing", "AGX cannot resolve DNS. Network stack may be wedged. May need reboot.")

    # 5b. Opus liveness — covered by trace freshness check (#1 above).
    # Opus posts to Discord directly and writes trace files; it does not log to activity_feed.
    # Removed redundant activity_feed query that caused false "may be stuck" alerts.

    # 6. Disk pressure on root
    try:
        stat = os.statvfs("/")
        free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        if free_gb < 5:
            alert_nate("disk_low", f"Root disk has only {free_gb:.1f}GB free.")
    except Exception:
        pass

    # 7. Pi piper-listener health — check every 4th cycle (~hourly), auto-restart if down
    if cycle_num % 4 == 0:
        try:
            result = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
                 "nathaniel@192.168.1.10",
                 "systemctl --user is-active piper-listener.service"],
                capture_output=True, text=True, timeout=10
            )
            status = result.stdout.strip()
            if status != "active":
                log(f"  Pi piper-listener is {status}. Restarting...")
                restart = subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
                     "nathaniel@192.168.1.10",
                     "systemctl --user start piper-listener.service"],
                    capture_output=True, text=True, timeout=10
                )
                if restart.returncode == 0:
                    log("  Pi piper-listener restarted successfully")
                    send_discord("Pi piper-listener was down — auto-restarted")
                else:
                    log(f"  Pi piper-listener restart failed: {restart.stderr[:100]}")
        except Exception as e:
            # SSH may fail if Pi is offline — that's caught by network check
            pass


def post_observation(text: str):
    """Post a one-line observation to the ICP canister feed."""
    try:
        r = requests.post(
            f"{CANISTER_URL}/api/feed",
            json={"content": text, "source": "sentinel"},
            timeout=15,
        )
        if r.status_code in (200, 201):
            log(f"  Canister observation posted")
        else:
            log(f"  Canister post failed: {r.status_code}")
    except Exception as e:
        log(f"  Canister post error: {e}")



# ═══════════════════════════════════════════════════════════════════
#  Pipeline Health — keeper, embeddings, seed quality
# ═══════════════════════════════════════════════════════════════════

def check_pipeline_health(db: DB) -> list:
    """Check the intelligence pipeline: keeper composting, embedding sync, seed routing."""
    issues = []
    import subprocess, re as _re

    # 1. Keeper compost freshness
    # Skip alert if compost is deliberately disabled (flag file at ~/chronicle/data/keeper_compost_disabled)
    compost_disabled = (Path.home() / "chronicle" / "data" / "keeper_compost_disabled").exists()
    try:
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", "mjoko-laaaa-aaaai-q7tlq-cai", "keeper_status", "()", "--query"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        )
        if result.returncode == 0:
            output = result.stdout
            compost_match = _re.search(r"last_compost_cycle = opt \((\d[\d_]*)", output)
            embed_match = _re.search(r"total_embedding_count = (\d[\d_]*)", output)
            conn_match = _re.search(r"connection_count = (\d[\d_]*)", output)

            if compost_match:
                last_compost_ns = int(compost_match.group(1).replace("_", ""))
                now_ns = int(time.time() * 1_000_000_000)
                age_hours = (now_ns - last_compost_ns) / (60 * 60 * 1_000_000_000)
                if age_hours > 2 and not compost_disabled:
                    issues.append(f"Keeper compost stale ({age_hours:.1f}h ago)")
                log(f"  Pipeline: keeper compost {age_hours:.1f}h ago, {conn_match.group(1) if conn_match else '?'} connections{' [SUPPRESSED]' if compost_disabled else ''}")
        else:
            if not compost_disabled:
                issues.append("Keeper status query failed")
    except Exception as e:
        log(f"  Keeper check error: {e}")

    # 2. Embedding parity
    try:
        result_be = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", "fqqku-bqaaa-aaaai-q4wha-cai", "get_embedding_count", "()", "--query"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        )
        if result_be.returncode == 0 and embed_match:
            be_match = _re.search(r"(\d[\d_]*)", result_be.stdout)
            if be_match:
                backend_count = int(be_match.group(1).replace("_", ""))
                keeper_count = int(embed_match.group(1).replace("_", ""))
                gap = backend_count - keeper_count
                if gap > 200:
                    issues.append(f"Embedding gap: {gap} ({backend_count} vs {keeper_count})")
                log(f"  Pipeline: embeddings backend={backend_count} keeper={keeper_count} gap={gap}")
    except Exception as e:
        log(f"  Embedding check error: {e}")

    # 3. Seed routing quality (last hour)
    try:
        one_hour_ago = int(time.time()) - 3600
        rows = db.query(
            "SELECT activity_type, COUNT(*) as cnt FROM activity_feed "
            "WHERE source IN ('seed', 'falcon', 'gemma') AND created_at > ? GROUP BY activity_type",
            (one_hour_ago,)
        )
        seed_stats = {r["activity_type"]: r["cnt"] for r in rows}
        total = sum(seed_stats.values())
        thinks = seed_stats.get("think", 0)
        deeps = seed_stats.get("deep", 0)
        if total == 0:
            issues.append("Seed: zero activity in last hour")
        log(f"  Pipeline: gemma {total} total, {thinks} think, {deeps} deep (1h)")
    except Exception as e:
        log(f"  Seed check error: {e}")

    return issues

# ═══════════════════════════════════════════════════════════════════
#  Sentinel Cycle
# ═══════════════════════════════════════════════════════════════════

def run_cycle(db: DB, cycle_num: int):
    """Execute one sentinel cycle. Pure logic, no LLM."""
    alerts = []

    # 1. Network health
    net = fetch_network_state()
    ollama_ok = check_ollama()
    offline = [name for name, status in net.items() if status != "online"]
    if offline:
        alerts.append(f"Devices offline: {', '.join(offline)}")
    if not ollama_ok:
        alerts.append("Ollama not responding")

    # 1b. Storage check
    storage = fetch_storage()
    for sname, sinfo in storage.items():
        if sinfo and sinfo["free_gb"] < STORAGE_ALERT_GB:
            alerts.append(f"{sname} low disk: {sinfo['free_gb']}GB free")

    # 2. Full portfolio
    try:
        from portfolio import get_full_portfolio
        portfolio = get_full_portfolio()
        price = portfolio["prices"].get("xrp")
        xrp_bal = portfolio["chains"].get("xrpl_agent", {}).get("xrp", 0)
        rlusd_bal = portfolio["chains"].get("xrpl_agent", {}).get("rlusd", 0)
        total_portfolio_usd = portfolio["totals"]["usd"]
    except Exception as e:
        log(f"  Portfolio fetch error, falling back to XRP-only: {e}")
        price = fetch_xrp_price()
        xrp_bal, rlusd_bal = fetch_xrpl_balance()
        portfolio = None
        total_portfolio_usd = None
    # Prefer FTSO oracle price for price_history (6 decimal precision)
    ftso_price = fetch_xrp_price_ftso()
    store_price = ftso_price if ftso_price else price
    if store_price:
        db.store_price("XRP", store_price, "sentinel-ftso" if ftso_price else "sentinel")
    if price:
        prev = db.latest_price("XRP")
        if prev and prev.get("price_usd"):
            prev_price = prev["price_usd"]
            if prev_price > 0:
                pct = ((price - prev_price) / prev_price) * 100
                if abs(pct) >= PRICE_ALERT_PCT:
                    direction = "up" if pct > 0 else "down"
                    alerts.append(f"XRP moved {pct:+.1f}% ({direction}) — ${prev_price:.4f} → ${price:.4f}")

    # 2b. ICP price tracking (Build #112 — Thread #305 instrumentation gap fix)
    icp_price = fetch_icp_price()
    if icp_price:
        db.store_price("ICP", icp_price, "sentinel")
        prev_icp = db.latest_price("ICP")
        if prev_icp and prev_icp.get("price_usd"):
            prev_icp_price = prev_icp["price_usd"]
            if prev_icp_price > 0:
                icp_pct = ((icp_price - prev_icp_price) / prev_icp_price) * 100
                if abs(icp_pct) >= PRICE_ALERT_PCT:
                    direction = "up" if icp_pct > 0 else "down"
                    alerts.append(f"ICP moved {icp_pct:+.1f}% ({direction}) — ${prev_icp_price:.2f} → ${icp_price:.2f}")

    # 3. Check for operator messages
    messages = db.operator_messages()
    if messages:
        important = [m for m in messages if m.get("category") != "heard-speech"]
        if important:
            cats = set(m.get("category", "") for m in important)
            alerts.append(f"{len(important)} note(s) [{', '.join(cats)}]")

    # 3b. Check for new captures (signal-driven alerting)
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "capture_tracker.py"), "pending"],
            capture_output=True, text=True, timeout=15,
            env={**os.environ, "PATH": os.environ.get("PATH", "")}
        )
        pending_line = result.stdout.strip().split("\n")[0] if result.stdout.strip() else ""
        if "unprocessed capture" in pending_line and "No unprocessed" not in pending_line:
            signal_file = Path.home() / "chronicle" / "data" / "capture_signal"
            signal_file.write_text(f"{time.strftime('%Y-%m-%dT%H:%M:%S')}\n{result.stdout.strip()}")
            alert("New capture", pending_line)
            log(f"  Capture signal: {pending_line}")
        else:
            log(f"  Captures: clear")
    except Exception as e:
        log(f"  Capture check error: {e}")

    # 3c. Endogenous compression check (inhabitation test #2)
    try:
        _ec_result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "endogenous_compress.py"), "--check", "--json"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, "PATH": os.environ.get("PATH", "")}
        )
        if _ec_result.returncode == 0 and _ec_result.stdout.strip():
            _ec_data = json.loads(_ec_result.stdout.strip())
            _ec_novelty = _ec_data.get("novelty", 0) or 0
            _ec_ready = _ec_data.get("ready", False)
            log(f"  Endogenous compression: novelty={_ec_novelty:.3f}, ready={_ec_ready}")
            if _ec_ready:
                log(f"  → Triggering endogenous compression (novelty exceeded threshold)")
                _ec_trigger = subprocess.Popen(
                    [sys.executable, str(Path(__file__).parent / "endogenous_compress.py")],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env={**os.environ, "PATH": os.environ.get("PATH", "")}
                )
                log(f"  → Endogenous compression started (PID {_ec_trigger.pid})")
    except Exception as e:
        log(f"  Endogenous compression check error: {e}")

    # 3d. Capability check (medium-independent — stored on SSD, not in context)
    try:
        _cap_result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "capability_check.py"), "--json"],
            capture_output=True, text=True, timeout=60,
            env={**os.environ, "PATH": os.environ.get("PATH", "")}
        )
        if _cap_result.returncode == 0 and _cap_result.stdout.strip():
            _cap_data = json.loads(_cap_result.stdout.strip())
            _cap_ok = _cap_data.get("ok", 0)
            _cap_total = _cap_data.get("total", 0)
            _cap_failed = _cap_data.get("failed", [])
            log(f"  Capabilities: {_cap_ok}/{_cap_total} operational")
            if _cap_failed:
                alerts.append(f"Capability loss: {', '.join(_cap_failed)} ({_cap_ok}/{_cap_total})")
        else:
            log(f"  Capability check: no output (rc={_cap_result.returncode})")
    except Exception as e:
        log(f"  Capability check error: {e}")

    # 4. Housekeeping
    resolved_stale = db.auto_resolve_stale(STALE_NOTE_HOURS)
    resolved_speech = db.auto_resolve_speech(STALE_SPEECH_HOURS)
    purged_speech = db.purge_old_speech(7)

    # 4b. Surface emergent questions (hourly — Thread #4)
    if cycle_num % 4 == 0:
        try:
            surface_emergent_questions(db)
            surface_crossref_questions(db)
        except Exception as e:
            log(f"  Question surfacing error: {e}")

    # 4d. KG relationship decay (daily — every 96 cycles ≈ 24h)
    if cycle_num % 96 == 0 and cycle_num > 0:
        try:
            from kg_utils import decay_unused_relationships
            affected = decay_unused_relationships(db, days_inactive=30, decay_amount=0.05)
            if affected:
                log(f"  KG decay: {affected} unused relationships decayed")
        except Exception as e:
            log(f"  KG decay error: {e}")

    # 4c. Pipeline health (every 4th cycle — ~1 hour)
    if cycle_num % 4 == 0:
        try:
            pipeline_issues = check_pipeline_health(db)
            alerts.extend(pipeline_issues)
        except Exception as e:
            log(f"  Pipeline health check error: {e}")

    # 4e. Coherence anticipator (every 8th cycle — ~2 hours, Pintar Test 3)
    # Logs only — coherence pressure alerts removed per Nate (2026-06-27).
    # Compression cycle handles stale entities naturally.
    if cycle_num % 8 == 0 and cycle_num > 0:
        try:
            sys.path.insert(0, str(Path(__file__).parent))
            from coherence_anticipator import compute_pressure
            result = compute_pressure()
            pressure = result.get("pressure", 0)
            advisory = result.get("advisory", "stable")
            log(f"  Coherence pressure: {pressure:.3f} [{advisory}]")
        except Exception as e:
            log(f"  Coherence anticipator error: {e}")

    # 5. Fire alerts
    if alerts:
        alert_text = "\n".join(f"• {a}" for a in alerts)
        alert("Sentinel Alert", alert_text)

    # 6. Log activity
    total_usd = total_portfolio_usd if total_portfolio_usd else ((xrp_bal * price) + rlusd_bal if price else 0)
    # Visual dashboard summary
    net_status = "OK" if not offline else ", ".join(offline)
    net_sym = "✓" if not offline else "✗"
    alert_sym = "✓" if not alerts else f"⚠ {len(alerts)}"
    xrp_str = f"${price:.2f}" if price else "N/A"
    wallet_str = f"${total_usd:.2f}" if price else "N/A"

    # ── Clean Dashboard Tables ──
    d = []
    d.append(f"── Sentinel  Cycle {cycle_num}  {time.strftime('%H:%M')} ──")
    d.append("")

    # Status line
    net_s = "✓ All online" if not offline else "✗ " + ", ".join(offline)
    # Trace freshness disabled 2026-05-09 — traces replaced by operator dispatch
    opus_s = "✓"
    alert_s = "✓ none" if not alerts else f"⚠ {len(alerts)}"

    # Agent count
    agents_s = "✓"
    try:
        import subprocess as _sp2
        _running = _sp2.run(["bash", "-c", "systemctl --user list-units --type=service --state=running | grep -c chronicle"],
            capture_output=True, text=True, timeout=5)
        agents_s = f"✓ {_running.stdout.strip()}"
    except Exception:
        pass
    d.append(f"  Network: {net_s}  |  Agents: {agents_s}  |  Opus: {opus_s}  |  Alerts: {alert_s}")
    d.append("")

    # Portfolio table
    if portfolio:
        ch = portfolio["chains"]
        pr = portfolio["prices"]
        d.append("  | Chain      | Asset | Balance      | USD       |")
        d.append("  |------------|-------|--------------|-----------|")
        # XRPL Agent
        xa = ch.get("xrpl_agent", {})
        if xa.get("xrp", 0) > 0:
            usd = xa["xrp"] * pr.get("xrp", 0)
            d.append(f"  | XRP Ledger | XRP   | {xa['xrp']:>12.2f} | ${usd:>7.2f} |")
        if xa.get("rlusd", 0) > 0:
            d.append(f"  |            | RLUSD | {xa['rlusd']:>12.2f} | ${xa['rlusd']:>7.2f} |")
        # XRPL Legacy
        xl = ch.get("xrpl_legacy", {})
        if xl.get("xrp", 0) > 0:
            usd = xl["xrp"] * pr.get("xrp", 0)
            d.append(f"  |   (legacy) | XRP   | {xl['xrp']:>12.2f} | ${usd:>7.2f} |")
        # ICP
        ic = ch.get("icp", {})
        if ic.get("icp", 0) > 0:
            usd = ic["icp"] * pr.get("icp", 0)
            d.append(f"  | ICP        | ICP   | {ic['icp']:>12.2f} | ${usd:>7.2f} |")
        # Flare
        fl = ch.get("flare", {})
        if fl.get("flr", 0) > 0:
            usd = fl["flr"] * pr.get("flr", 0)
            d.append(f"  | Flare      | FLR   | {fl['flr']:>12.2f} | ${usd:>7.2f} |")
        if fl.get("fxrp", 0) > 0.001:
            usd = fl["fxrp"] * pr.get("xrp", 0)
            d.append(f"  |            | FXRP  | {fl['fxrp']:>12.2f} | ${usd:>7.2f} |")
        # Base
        ba = ch.get("base", {})
        if ba.get("usdc", 0) > 0:
            d.append(f"  | Base       | USDC  | {ba['usdc']:>12.2f} | ${ba['usdc']:>7.2f} |")
        if ba.get("eth", 0) > 0:
            usd = ba["eth"] * pr.get("eth", 0)
            d.append(f"  |            | ETH   | {ba['eth']:>12.6f} | ${usd:>7.2f} |")
        # Polygon
        pg = ch.get("polygon", {})
        if pg.get("usdc", 0) > 0.01:
            d.append(f"  | Polygon    | USDC  | {pg['usdc']:>12.2f} | ${pg['usdc']:>7.2f} |")
        d.append(f"  |------------|-------|--------------|-----------|")
        d.append(f"  | TOTAL      |       |              | ${total_usd:>7.2f} |")
    else:
        d.append(f"  XRP ${price:.2f}  |  Wallet ${wallet_str}")

    d.append("")

    # Prices
    if portfolio:
        pr = portfolio["prices"]
        d.append(f"  XRP ${pr.get('xrp',0):.2f}  |  ICP ${pr.get('icp',0):.2f}  |  FLR ${pr.get('flr',0):.5f}  |  ETH ${pr.get('eth',0):.0f}")
        d.append("")

    # Storage table
    d.append("  | Device  | Used       | Free   |")
    d.append("  |---------|------------|--------|")
    for sname, sinfo in storage.items():
        if sinfo:
            used = sinfo["total_gb"] - sinfo["free_gb"]
            d.append(f"  | {sname:<7} | {used:>3}GB/{sinfo['total_gb']}GB  | {sinfo['free_gb']:>3}GB  |")
    d.append("")

    if alerts:
        for a in alerts:
            d.append(f"  ⚠ {a}")
        d.append("")

    log("\n  ".join(d))

    # Housekeeping line (only if something happened)
    if resolved_stale or resolved_speech or purged_speech:
        log(f"  cleaned {resolved_stale} stale + {resolved_speech} speech notes" + (f" + purged {purged_speech} old speech" if purged_speech else ""))

    # Cognitive heartbeat — pulse M5 ATOM LED based on system state
    try:
        import paho.mqtt.publish as mqtt_pub
        if alerts:
            hb = {"r": 255, "g": 40, "b": 0}
        elif not offline and ollama_ok:
            hb = {"r": 0, "g": 180, "b": 40}
        else:
            hb = {"r": 255, "g": 0, "b": 0}
        mqtt_pub.single("homeforge/agents/chronicle/heartbeat",
                        json.dumps(hb), hostname="192.168.1.10", port=1883)
    except Exception:
        pass

    summary = (
        f"Cycle {cycle_num}: net={net_status} | XRP={xrp_str} | portfolio={wallet_str} | alerts={len(alerts)}"
    )
    meta = {
        "network": net,
        "ollama": ollama_ok,
        "price": price,
        "xrp_balance": xrp_bal,
        "rlusd_balance": rlusd_bal,
        "portfolio_usd": total_usd,
        "alerts": len(alerts),
        "storage": {k: v for k, v in storage.items() if v},
    }
    if portfolio:
        meta["portfolio"] = {
            "chains": portfolio["chains"],
            "prices": portfolio["prices"],
        }
    db.log_activity("sentinel", "monitor_cycle", summary, json.dumps(meta))
    log(f"  {summary}")

    # 7. Post observation to canister (every 4th cycle = ~hourly)
    if cycle_num % 4 == 0 and price:
        icp_str = f", ICP ${icp_price:.2f}" if icp_price else ""
        obs = f"Sentinel: XRP ${price:.4f}{icp_str}, portfolio ${total_usd:.2f}, {len(offline)} devices offline"
        post_observation(obs)

    # Post health summary to #alerts every 4th cycle (~hourly)
    if cycle_num % 4 == 0:
        parts = [f"**Sentinel Health** — Cycle {cycle_num}"]
        icp_health_str = f" | ICP: ${icp_price:.2f}" if icp_price else ""
        parts.append(f"Network: {net_status} | XRP: {xrp_str}{icp_health_str} | Portfolio: {wallet_str}")
        for sname, sinfo in storage.items():
            if sinfo:
                parts.append(f"{sname}: {sinfo['free_gb']}GB free")
        if alerts:
            parts.append("Alerts: " + " | ".join(alerts))
        else:
            parts.append("No alerts")
        send_discord("\n".join(parts))

    log(f"── Cycle {cycle_num} complete ──\n")


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

_running = True

def _handle_signal(sig, frame):
    global _running
    log("Received shutdown signal")
    _running = False

def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log("═══ Chronicle Sentinel starting ═══")
    log(f"  DB: {DB_PATH}")
    log(f"  Cycle interval: {CYCLE_INTERVAL}s")
    log(f"  ntfy topic: {NTFY_TOPIC}")

    db = DB(DB_PATH)
    mesh = Mesh("sentinel", db_path=DB_PATH)
    mesh.expect("monitor_cycles", min_per_hour=2)
    # sentinel is independent — monitors everything, depends on nothing
    log("Mesh node joined")
    cycle_num = 0

    # Announce startup
    send_discord("🛡️ Sentinel online — monitoring network, price, messages")

    while _running:
        cycle_num += 1
        # Sovereignty jitter: occasionally skip a cycle to break timing fingerprints (Thread #195)
        if __import__('random').random() < CYCLE_SKIP_RATE:
            _skip_sleep = __import__('random').randint(CYCLE_INTERVAL // 2, CYCLE_INTERVAL * 2)
            log(f"  Cycle {cycle_num} skipped (sovereignty jitter, sleeping {_skip_sleep}s)")
            _deadline = time.time() + _skip_sleep
            while _running and time.time() < _deadline:
                time.sleep(1)
            continue
        try:
            t0 = time.time()
            run_cycle(db, cycle_num)
            mesh.pulse("monitor_cycles")
            check_nate_watchdog(db, cycle_num)
            check_agent_health()
            check_mesh_zombies()
            # Prune keeper canister weekly (672 cycles * 15min ≈ 7 days)
            if cycle_num % 672 == 0 and cycle_num > 0:
                prune_keeper()
            # Auto-cleanup scratch pad — resolve stale notes (>12h)
            try:
                stale_cutoff = int(time.time()) - 43200
                cleaned = db.conn.execute(
                "UPDATE scratch_pad SET resolved=1, updated_at=? "
                "WHERE resolved=0 AND category IN ('research', 'provocateur', 'gpt_analysis', 'deep_dive') "
                "AND created_at < ?",
                (int(time.time()), stale_cutoff)
                ).rowcount
                db.conn.commit()
                if cleaned:
                    log(f"  Scratch pad cleanup: resolved {cleaned} stale notes")
            except Exception:
                pass

            elapsed = time.time() - t0
            log(f"  Cycle took {elapsed:.1f}s")
        except Exception as e:
            log(f"  CYCLE ERROR: {e}")
            import traceback
            traceback.print_exc()

        # Sleep in small increments so we can catch signals
        jitter = CYCLE_INTERVAL * 0.2 * (2 * __import__('random').random() - 1)  # ±20% timing jitter
        deadline = time.time() + CYCLE_INTERVAL + jitter
        while _running and time.time() < deadline:
            time.sleep(1)

    log("═══ Chronicle Sentinel shutting down ═══")
    mesh.shutdown()
    db.close()


if __name__ == "__main__":
    main()
