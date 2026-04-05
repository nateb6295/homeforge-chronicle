"""Chronicle Mind - External data fetchers (prices, XRPL, EVM, RSS)."""

import os, re, json, subprocess, requests
from typing import Optional, List, Tuple

from mind.utils import log, now_ts
from mind.config import (
    OLLAMA_URL, COINGECKO_URL, COINGECKO_API_KEY, XRPL_RPC, FLARE_RPC,
    BASE_RPC, FTSO_REGISTRY, AGENT_WALLET, EVM_ADDRESS, USDC_BASE,
    ROSETTA_API, ICP_ACCOUNT_ID, RSS_FEEDS, RSS_CACHE_FILE,
    RSS_FETCH_INTERVAL, DFX_IDENTITY,
)


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


# ── XRPL Network Intelligence ────────────────────────────────────

def fetch_xrpl_network_info() -> Optional[dict]:
    """Fetch expanded server_info: fee, ledger rate, validators, load."""
    try:
        r = requests.post(XRPL_RPC, json={
            "method": "server_info", "params": [{}]
        }, timeout=10)
        info = r.json().get("result", {}).get("info", {})
        if not info:
            return None
        validated = info.get("validated_ledger", {})
        return {
            "server_state": info.get("server_state", "unknown"),
            "base_fee_xrp": float(validated.get("base_fee_xrp", 0)),
            "ledger_seq": validated.get("seq", 0),
            "reserve_base": float(validated.get("reserve_base_xrp", 0)),
            "reserve_inc": float(validated.get("reserve_inc_xrp", 0)),
            "peers": info.get("peers", 0),
            "load_factor": info.get("load_factor", 1),
            "uptime": info.get("uptime", 0),
        }
    except Exception:
        return None


def fetch_xrpl_amendments() -> Optional[list]:
    """Fetch amendment voting status — shows governance direction."""
    try:
        # feature method not supported on xrplcluster; use Ripple's public server
        r = requests.post("https://s1.ripple.com:51234", json={
            "method": "feature", "params": [{}]
        }, timeout=15)
        features = r.json().get("result", {}).get("features", {})
        if not features:
            return None
        # Find amendments in voting (not yet enabled)
        voting = []
        for amendment_id, data in features.items():
            if not data.get("enabled", False):
                voting.append({
                    "id": amendment_id[:12] + "...",
                    "name": data.get("name", "unknown"),
                    "supported": data.get("supported", False),
                    "count": data.get("count", 0),
                    "threshold": data.get("threshold", 0),
                    "validations": data.get("validations", 0),
                })
        # Sort by vote count descending (closest to passing first)
        voting.sort(key=lambda x: x.get("count", 0), reverse=True)
        return voting[:5]  # Top 5 closest to passing
    except Exception:
        return None


def fetch_xrpl_orderbook() -> Optional[dict]:
    """Fetch XRP/RLUSD order book depth from the DEX. Prices in RLUSD per XRP."""
    RLUSD_HEX = "524C555344000000000000000000000000000000"
    RLUSD_ISSUER = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"
    try:
        # Bids: offers to sell RLUSD for XRP (taker gets RLUSD, pays XRP)
        r = requests.post(XRPL_RPC, json={
            "method": "book_offers",
            "params": [{
                "taker_pays": {"currency": "XRP"},
                "taker_gets": {"currency": RLUSD_HEX, "issuer": RLUSD_ISSUER},
                "limit": 10
            }]
        }, timeout=10)
        bids = r.json().get("result", {}).get("offers", [])

        # Asks: offers to sell XRP for RLUSD (taker gets XRP, pays RLUSD)
        r2 = requests.post(XRPL_RPC, json={
            "method": "book_offers",
            "params": [{
                "taker_pays": {"currency": RLUSD_HEX, "issuer": RLUSD_ISSUER},
                "taker_gets": {"currency": "XRP"},
                "limit": 10
            }]
        }, timeout=10)
        asks = r2.json().get("result", {}).get("offers", [])

        # Depth in XRP
        # Bids: TakerPays is XRP drops (str)
        bid_depth = sum(float(o.get("TakerPays", "0")) / 1_000_000
                        for o in bids if isinstance(o.get("TakerPays"), str))
        # Asks: TakerGets is XRP drops (str)
        ask_depth = sum(float(o.get("TakerGets", "0")) / 1_000_000
                        for o in asks if isinstance(o.get("TakerGets"), str))

        # Best bid: price = RLUSD_offered / XRP_wanted (highest someone will pay for XRP)
        best_bid = None
        if bids:
            b = bids[0]
            try:
                rlusd_amt = float(b["TakerGets"]["value"])     # RLUSD (dict)
                xrp_amt = float(b["TakerPays"]) / 1_000_000   # XRP (drops str)
                if xrp_amt > 0:
                    best_bid = rlusd_amt / xrp_amt
            except Exception:
                pass

        # Best ask: price = RLUSD_wanted / XRP_offered (lowest someone will sell XRP for)
        best_ask = None
        if asks:
            a = asks[0]
            try:
                xrp_amt = float(a["TakerGets"]) / 1_000_000   # XRP (drops str)
                rlusd_amt = float(a["TakerPays"]["value"])     # RLUSD (dict)
                if xrp_amt > 0:
                    best_ask = rlusd_amt / xrp_amt
            except Exception:
                pass

        spread = None
        if best_bid and best_ask and best_ask > 0:
            spread = ((best_ask - best_bid) / best_ask) * 100

        return {
            "bid_count": len(bids),
            "ask_count": len(asks),
            "bid_depth_xrp": round(bid_depth, 2),
            "ask_depth_xrp": round(ask_depth, 2),
            "best_bid": round(best_bid, 6) if best_bid else None,
            "best_ask": round(best_ask, 6) if best_ask else None,
            "spread_pct": round(spread, 3) if spread else None,
        }
    except Exception:
        return None


def fetch_xrpl_amm_info() -> Optional[dict]:
    """Fetch AMM pool info for XRP/RLUSD pair."""
    RLUSD_ISSUER = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"
    try:
        r = requests.post(XRPL_RPC, json={
            "method": "amm_info",
            "params": [{
                "asset": {"currency": "XRP"},
                "asset2": {"currency": "524C555344000000000000000000000000000000", "issuer": RLUSD_ISSUER},
            }]
        }, timeout=10)
        result = r.json().get("result", {})
        amm = result.get("amm", {})
        if not amm:
            return None
        amount = amm.get("amount", "0")
        amount2 = amm.get("amount2", {})
        xrp_pool = float(amount) / 1_000_000 if isinstance(amount, str) else 0
        rlusd_pool = float(amount2.get("value", 0)) if isinstance(amount2, dict) else 0
        fee = amm.get("trading_fee", 0)
        lp_token = amm.get("lp_token", {})
        return {
            "xrp_pool": round(xrp_pool, 2),
            "rlusd_pool": round(rlusd_pool, 2),
            "trading_fee_bps": fee,
            "lp_outstanding": lp_token.get("value", "0"),
            "implied_price": round(rlusd_pool / xrp_pool, 6) if xrp_pool > 0 else None,
        }
    except Exception:
        return None


def fetch_xrpl_escrow_watch() -> Optional[dict]:
    """Check Ripple's escrow — currently disabled pending address verification."""
    return None


def fetch_xrpl_intelligence() -> dict:
    """Gather all XRPL network intelligence in one call."""
    intel = {}
    intel["network"] = fetch_xrpl_network_info()
    intel["amendments"] = fetch_xrpl_amendments()
    intel["orderbook"] = fetch_xrpl_orderbook()
    intel["amm"] = fetch_xrpl_amm_info()
    intel["escrow"] = fetch_xrpl_escrow_watch()
    return intel


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


def fetch_evm_balances() -> dict:
    """Fetch native + token balances from Flare and BASE EVM chains."""
    balances = {}
    # Flare: FLR native balance
    try:
        r = requests.post(FLARE_RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_getBalance",
            "params": [EVM_ADDRESS, "latest"]
        }, timeout=15)
        result = r.json().get("result", "0x0")
        balances["flr"] = int(result, 16) / 1e18
    except Exception as e:
        log(f"  Flare balance error: {e}")
        balances["flr"] = 0.0

    # BASE: ETH native balance
    try:
        r = requests.post(BASE_RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_getBalance",
            "params": [EVM_ADDRESS, "latest"]
        }, timeout=15)
        result = r.json().get("result", "0x0")
        balances["base_eth"] = int(result, 16) / 1e18
    except Exception as e:
        log(f"  BASE ETH balance error: {e}")
        balances["base_eth"] = 0.0

    # BASE: USDC (ERC-20, 6 decimals)
    try:
        padded = EVM_ADDRESS[2:].lower().zfill(64)
        r = requests.post(BASE_RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": USDC_BASE, "data": "0x70a08231" + padded}, "latest"]
        }, timeout=15)
        result = r.json().get("result", "0x0")
        balances["base_usdc"] = int(result, 16) / 1e6
    except Exception as e:
        log(f"  BASE USDC balance error: {e}")
        balances["base_usdc"] = 0.0

    return balances


# ═══════════════════════════════════════════════════════════════════
#  Environmental Dashboard Fetchers
# ═══════════════════════════════════════════════════════════════════

_weather_cache = {"text": "", "fetched_at": 0}
WEATHER_FETCH_INTERVAL = 1800  # 30 minutes


def fetch_weather(location: str = "Puyallup+WA") -> str:
    """Fetch current weather. Returns one-line summary or empty string.
    Cached for 30 minutes (weather doesn't change every 5 min cycle)."""
    global _weather_cache
    if now_ts() - _weather_cache["fetched_at"] < WEATHER_FETCH_INTERVAL:
        return _weather_cache["text"]
    try:
        r = requests.get(
            f"https://wttr.in/{location}?format=%C+%t+wind+%w+humidity+%h",
            timeout=10, headers={"User-Agent": "curl"},
        )
        if r.status_code == 200 and r.text.strip():
            _weather_cache = {"text": r.text.strip(), "fetched_at": now_ts()}
            return _weather_cache["text"]
    except Exception:
        pass
    return _weather_cache.get("text", "")


def fetch_sprout_state(db) -> dict:
    """Get Sprout's latest cycle info from synced activity_feed."""
    try:
        row = db.query_one(
            "SELECT title, content, created_at FROM activity_feed "
            "WHERE source='sprout' AND activity_type='cognitive_cycle' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        if row:
            ago = (now_ts() - row.get("created_at", 0)) / 60
            return {
                "summary": row.get("title", ""),
                "minutes_ago": int(ago),
                "detail": row.get("content", "")[:150],
            }
    except Exception:
        pass
    return {}


def fetch_random_capsule(icp_agent, max_id: int = 5100) -> dict:
    """Fetch a random capsule from the first ~80% of archive for memory echo."""
    import random
    target_id = random.randint(1, int(max_id * 0.8))
    try:
        capsule = icp_agent.get_capsule(target_id)
        if capsule and capsule.get("restatement"):
            return capsule
    except Exception:
        pass
    return {}


def fetch_network_state() -> dict:
    """Quick ping check of known home network devices."""
    import subprocess as _sp
    devices = {
        "Pi (senses)": "192.168.1.10",
        "Jetson (Sprout)": "192.168.1.11",
        "Reolink (camera)": "192.168.1.110",
        "Bambu (printer)": "192.168.1.21",
    }
    result = {}
    for name, ip in devices.items():
        try:
            ret = _sp.run(
                ["ping", "-c", "1", "-W", "1", ip],
                capture_output=True, timeout=3,
            )
            result[name] = "online" if ret.returncode == 0 else "offline"
        except Exception:
            result[name] = "unknown"
    return result
