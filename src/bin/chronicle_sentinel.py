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
from typing import Optional, Tuple

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
CYCLE_INTERVAL = int(os.environ.get("SENTINEL_INTERVAL", "900"))  # 15 min
LOG_FILE = os.environ.get(
    "CHRONICLE_LOG",
    os.path.expanduser("~/chronicle/chronicle-sentinel.log"),
)

# Comms
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "chronicle-nate-5d786588e02c8854")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# XRPL
XRPL_RPC = "https://xrplcluster.com"
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
FTSO_REGISTRY = "0xaD67FE66660Fb8dFE9d6b1b4240d8650e30F6019"

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
        self.conn = sqlite3.connect(path, timeout=30)
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

    def operator_messages(self) -> list:
        """Get unresolved high-priority or discord-operator notes."""
        return self.query(
            "SELECT id, content, category, priority FROM scratch_pad "
            "WHERE resolved = 0 AND (priority >= 8 OR category = 'discord-operator') "
            "AND category NOT IN ('provocateur', 'crossref', 'research') "
            "ORDER BY priority DESC, created_at DESC LIMIT 5",
        )


# ═══════════════════════════════════════════════════════════════════
#  Fetchers
# ═══════════════════════════════════════════════════════════════════

import requests

def fetch_xrp_price_ftso() -> Optional[float]:
    """Fetch XRP price from Flare FTSO oracle."""
    try:
        data = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{
                "to": FTSO_REGISTRY,
                "data": ("0xa69afdc6"
                         "0000000000000000000000000000000000000000000000000000000000000020"
                         "0000000000000000000000000000000000000000000000000000000000000003"
                         "5852500000000000000000000000000000000000000000000000000000000000")
            }, "latest"],
        }
        r = requests.post(FLARE_RPC, json=data, timeout=15)
        result = r.json().get("result", "")
        if result and result != "0x" and len(result) >= 66:
            price_raw = int(result[2:66], 16)
            decimals = int(result[66:130], 16)
            if decimals > 0 and price_raw > 0:
                price = price_raw / (10 ** decimals)
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
            COINGECKO_URL, params={"ids": "ripple", "vs_currencies": "usd"},
            headers=headers, timeout=10,
        )
        return r.json().get("ripple", {}).get("usd")
    except Exception:
        return None


def fetch_xrp_price() -> Optional[float]:
    return fetch_xrp_price_ftso() or fetch_xrp_price_coingecko()


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
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        return
    try:
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
            headers={
                "Authorization": f"Bot {DISCORD_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"content": safe_truncate(message, 1900)},
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
    if price:
        db.store_price("XRP", price, "sentinel")
        prev = db.latest_price("XRP")
        if prev and prev.get("price_usd"):
            prev_price = prev["price_usd"]
            if prev_price > 0:
                pct = ((price - prev_price) / prev_price) * 100
                if abs(pct) >= PRICE_ALERT_PCT:
                    direction = "up" if pct > 0 else "down"
                    alerts.append(f"XRP moved {pct:+.1f}% ({direction}) — ${prev_price:.4f} → ${price:.4f}")

    # 3. Check for operator messages
    messages = db.operator_messages()
    if messages:
        important = [m for m in messages if m.get("category") != "heard-speech"]
        if important:
            cats = set(m.get("category", "") for m in important)
            alerts.append(f"{len(important)} note(s) [{', '.join(cats)}]")

    # 4. Housekeeping
    resolved_stale = db.auto_resolve_stale(STALE_NOTE_HOURS)
    resolved_speech = db.auto_resolve_speech(STALE_SPEECH_HOURS)

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

    dash_lines = []
    dash_lines.append(f"┌── Cycle {cycle_num} " + "─" * 34)
    dash_lines.append(f"│  Network  {net_sym}  {net_status}")
    if portfolio:
        ch = portfolio["chains"]
        pr = portfolio["prices"]
        dash_lines.append(f"│  XRP      {xrp_str}   Portfolio  {wallet_str}")
        flr = ch.get("flare", {}).get("flr", 0)
        icp = ch.get("icp", {}).get("icp", 0)
        usdc = ch.get("base", {}).get("usdc", 0) + ch.get("polygon", {}).get("usdc", 0)
        dash_lines.append(f"│           {xrp_bal:.1f} XRP  {flr:.0f} FLR  {icp:.1f} ICP  {usdc:.0f} USDC")
    else:
        dash_lines.append(f"│  XRP      {xrp_str}   Wallet  {wallet_str}")
    dash_lines.append(f"│  Alerts   {alert_sym}")
    if messages:
        top = messages[0]
        top_text = safe_truncate(top.get("content", ""), 70)
        dash_lines.append(f"│  Notes    {len(messages)} pending")
        dash_lines.append(f"│           {top_text}")
    if storage:
        dash_lines.append(f"│  Storage")
        for sname, sinfo in storage.items():
            if sinfo:
                pct = sinfo["pct_used"]
                filled = int(pct / 100 * 10)
                bar = "█" * filled + "░" * (10 - filled)
                dash_lines.append(f"│    {sname:>6}  [{bar}] {sinfo['free_gb']}GB/{sinfo['total_gb']}GB")
    dash_lines.append("└" + "─" * 43)
    log("\n  ".join(dash_lines))

    # Housekeeping line (only if something happened)
    if resolved_stale or resolved_speech:
        log(f"  cleaned {resolved_stale} stale + {resolved_speech} speech notes")

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
        obs = f"Sentinel: XRP ${price:.4f}, portfolio ${total_usd:.2f}, {len(offline)} devices offline"
        post_observation(obs)

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
    cycle_num = 0

    # Announce startup
    send_discord("🛡️ Sentinel online — monitoring network, price, messages")

    while _running:
        cycle_num += 1
        try:
            t0 = time.time()
            run_cycle(db, cycle_num)
            elapsed = time.time() - t0
            log(f"  Cycle took {elapsed:.1f}s")
        except Exception as e:
            log(f"  CYCLE ERROR: {e}")
            import traceback
            traceback.print_exc()

        # Sleep in small increments so we can catch signals
        deadline = time.time() + CYCLE_INTERVAL
        while _running and time.time() < deadline:
            time.sleep(1)

    log("═══ Chronicle Sentinel shutting down ═══")
    db.close()


if __name__ == "__main__":
    main()
