#!/usr/bin/env python3
"""Chronicle Portfolio — unified wallet balance query.

One function, all chains, all tokens, parallel fetches.

Usage:
    python3 portfolio.py              # pretty-print summary
    python3 portfolio.py --json       # JSON output

    from portfolio import get_full_portfolio
    p = get_full_portfolio()
    print(p["totals"]["usd"])
"""

import json
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── RPC Endpoints ──
XRPL_RPC = "https://xrplcluster.com"
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
BASE_RPC = "https://mainnet.base.org"
POLYGON_RPC = "https://polygon-rpc.com"
ROSETTA_API = "https://rosetta-api.internetcomputer.org/account/balance"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

# ── Wallet Addresses ──
XRPL_AGENT = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
XRPL_LEGACY = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
FLARE_ADDR = "0x2C6D9E36d12fbb77dD8EDcA73739C0db075f078d"
BASE_ADDR = "0x80D07e16165576DBc17fe1FF865495fed4E9c387"
# Polygon: same EVM address as Base (canister-derived)
POLYGON_ADDR = BASE_ADDR
ICP_ACCOUNT_ID = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9"

# ── Token Contracts ──
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"       # 6 decimals
USDC_POLYGON = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"    # 6 decimals
FXRP_FLARE = "0xAd552A648C74D49E10027AB8a618A3ad4901c5bE"      # 18 decimals (FAsset)

TIMEOUT = 15


# ── Low-level fetchers ──

def _fetch_xrpl(address: str) -> dict:
    """Fetch XRP + RLUSD for any XRPL address."""
    result = {"xrp": 0.0, "rlusd": 0.0, "error": None}
    try:
        r = requests.post(XRPL_RPC, json={
            "method": "account_info",
            "params": [{"account": address, "ledger_index": "validated"}]
        }, timeout=TIMEOUT)
        data = r.json().get("result", {})
        if "account_data" in data:
            result["xrp"] = int(data["account_data"].get("Balance", 0)) / 1_000_000

        r2 = requests.post(XRPL_RPC, json={
            "method": "account_lines",
            "params": [{"account": address, "ledger_index": "validated"}]
        }, timeout=TIMEOUT)
        for line in r2.json().get("result", {}).get("lines", []):
            cur = str(line.get("currency", ""))
            if cur == "RLUSD" or cur.startswith("524C555344"):
                result["rlusd"] += float(line.get("balance", 0))
    except Exception as e:
        result["error"] = str(e)
    return result


def _fetch_evm_native(rpc_url: str, address: str) -> float:
    """Fetch native balance on any EVM chain."""
    r = requests.post(rpc_url, json={
        "jsonrpc": "2.0", "id": 1, "method": "eth_getBalance",
        "params": [address, "latest"]
    }, timeout=TIMEOUT)
    return int(r.json().get("result", "0x0"), 16) / 1e18


def _fetch_erc20(rpc_url: str, token: str, holder: str, decimals: int) -> float:
    """Fetch ERC-20 token balance via balanceOf."""
    padded = holder[2:].lower().zfill(64)
    r = requests.post(rpc_url, json={
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": token, "data": "0x70a08231" + padded}, "latest"]
    }, timeout=TIMEOUT)
    return int(r.json().get("result", "0x0"), 16) / (10 ** decimals)


def _fetch_icp() -> float:
    """Fetch ICP balance via Rosetta API."""
    r = requests.post(ROSETTA_API, json={
        "network_identifier": {
            "blockchain": "Internet Computer",
            "network": "00000000000000020101",
        },
        "account_identifier": {"address": ICP_ACCOUNT_ID},
    }, timeout=TIMEOUT)
    balances = r.json().get("balances", [])
    if balances:
        return int(balances[0].get("value", 0)) / 1e8
    return 0.0


def _fetch_prices() -> dict:
    """Fetch USD prices for all assets in one CoinGecko call."""
    prices = {"xrp": 0.0, "icp": 0.0, "flr": 0.0, "eth": 0.0, "pol": 0.0}
    try:
        r = requests.get(COINGECKO_URL, params={
            "ids": "ripple,internet-computer,flare-networks,ethereum,matic-network",
            "vs_currencies": "usd"
        }, timeout=TIMEOUT)
        data = r.json()
        prices["xrp"] = data.get("ripple", {}).get("usd", 0.0)
        prices["icp"] = data.get("internet-computer", {}).get("usd", 0.0)
        prices["flr"] = data.get("flare-networks", {}).get("usd", 0.0)
        prices["eth"] = data.get("ethereum", {}).get("usd", 0.0)
        prices["pol"] = data.get("matic-network", {}).get("usd", 0.0)
    except Exception:
        pass
    return prices


# ── Chain fetchers (each returns a labeled result) ──

def _chain_xrpl_agent():
    return ("xrpl_agent", _fetch_xrpl(XRPL_AGENT))

def _chain_xrpl_legacy():
    return ("xrpl_legacy", _fetch_xrpl(XRPL_LEGACY))

def _chain_icp():
    result = {"icp": 0.0, "error": None}
    try:
        result["icp"] = _fetch_icp()
    except Exception as e:
        result["error"] = str(e)
    return ("icp", result)

def _chain_flare():
    result = {"flr": 0.0, "fxrp": 0.0, "error": None}
    try:
        # Main FLR holdings are on the canister-derived EVM address
        result["flr"] = _fetch_evm_native(FLARE_RPC, BASE_ADDR)
        # Also check the legacy Flare address
        legacy_flr = _fetch_evm_native(FLARE_RPC, FLARE_ADDR)
        result["flr"] += legacy_flr
        # FXRP — check both addresses
        result["fxrp"] = _fetch_erc20(FLARE_RPC, FXRP_FLARE, BASE_ADDR, 18)
        result["fxrp"] += _fetch_erc20(FLARE_RPC, FXRP_FLARE, FLARE_ADDR, 18)
    except Exception as e:
        result["error"] = str(e)
    return ("flare", result)

def _chain_base():
    result = {"eth": 0.0, "usdc": 0.0, "error": None}
    try:
        result["eth"] = _fetch_evm_native(BASE_RPC, BASE_ADDR)
        result["usdc"] = _fetch_erc20(BASE_RPC, USDC_BASE, BASE_ADDR, 6)
    except Exception as e:
        result["error"] = str(e)
    return ("base", result)

def _chain_polygon():
    result = {"pol": 0.0, "usdc": 0.0, "error": None}
    try:
        result["pol"] = _fetch_evm_native(POLYGON_RPC, POLYGON_ADDR)
        result["usdc"] = _fetch_erc20(POLYGON_RPC, USDC_POLYGON, POLYGON_ADDR, 6)
    except Exception as e:
        result["error"] = str(e)
    return ("polygon", result)

def _chain_prices():
    return ("prices", _fetch_prices())


# ── Main entry point ──

def get_full_portfolio() -> dict:
    """Fetch complete portfolio across all chains in parallel."""
    portfolio = {
        "timestamp": int(time.time()),
        "chains": {},
        "prices": {},
        "totals": {"usd": 0.0, "breakdown": {}},
        "errors": [],
        # ICP neuron is hardcoded — stake doesn't change often
        "icp_neuron": {"staked_icp": 10.0, "state": "NotDissolving"},
    }

    fetchers = [
        _chain_xrpl_agent,
        _chain_xrpl_legacy,
        _chain_icp,
        _chain_flare,
        _chain_base,
        _chain_polygon,
        _chain_prices,
    ]

    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {pool.submit(fn): fn.__name__ for fn in fetchers}
        for future in as_completed(futures):
            try:
                name, data = future.result()
                if name == "prices":
                    portfolio["prices"] = data
                else:
                    portfolio["chains"][name] = data
                    if data.get("error"):
                        portfolio["errors"].append(f"{name}: {data['error']}")
            except Exception as e:
                fn_name = futures[future]
                portfolio["errors"].append(f"{fn_name}: {e}")

    # ── Calculate USD totals ──
    p = portfolio["prices"]
    chains = portfolio["chains"]
    breakdown = {}

    # XRP
    xrp_total = (chains.get("xrpl_agent", {}).get("xrp", 0) +
                 chains.get("xrpl_legacy", {}).get("xrp", 0))
    breakdown["xrp"] = {"amount": round(xrp_total, 2), "usd": round(xrp_total * p.get("xrp", 0), 2)}

    # RLUSD
    rlusd_total = (chains.get("xrpl_agent", {}).get("rlusd", 0) +
                   chains.get("xrpl_legacy", {}).get("rlusd", 0))
    breakdown["rlusd"] = {"amount": round(rlusd_total, 2), "usd": round(rlusd_total, 2)}

    # ICP (liquid + staked)
    icp_liquid = chains.get("icp", {}).get("icp", 0)
    icp_staked = portfolio["icp_neuron"]["staked_icp"]
    breakdown["icp_liquid"] = {"amount": round(icp_liquid, 2), "usd": round(icp_liquid * p.get("icp", 0), 2)}
    breakdown["icp_staked"] = {"amount": round(icp_staked, 2), "usd": round(icp_staked * p.get("icp", 0), 2)}

    # FLR
    flr = chains.get("flare", {}).get("flr", 0)
    breakdown["flr"] = {"amount": round(flr, 2), "usd": round(flr * p.get("flr", 0), 2)}

    # FXRP (priced at XRP rate)
    fxrp = chains.get("flare", {}).get("fxrp", 0)
    breakdown["fxrp"] = {"amount": round(fxrp, 2), "usd": round(fxrp * p.get("xrp", 0), 2)}

    # ETH on Base
    eth = chains.get("base", {}).get("eth", 0)
    breakdown["eth"] = {"amount": round(eth, 6), "usd": round(eth * p.get("eth", 0), 2)}

    # USDC (Base + Polygon)
    usdc_total = (chains.get("base", {}).get("usdc", 0) +
                  chains.get("polygon", {}).get("usdc", 0))
    breakdown["usdc"] = {"amount": round(usdc_total, 2), "usd": round(usdc_total, 2)}

    # POL
    pol = chains.get("polygon", {}).get("pol", 0)
    breakdown["pol"] = {"amount": round(pol, 2), "usd": round(pol * p.get("pol", 0), 2)}

    portfolio["totals"]["breakdown"] = breakdown
    portfolio["totals"]["usd"] = round(sum(v["usd"] for v in breakdown.values()), 2)

    return portfolio


def print_summary(p: dict) -> str:
    """Pretty-print portfolio summary."""
    chains = p["chains"]
    prices = p["prices"]
    bd = p["totals"]["breakdown"]
    neuron = p["icp_neuron"]

    lines = []
    lines.append("┌── Chronicle Portfolio ────────────────────────")

    # XRPL Agent
    xa = chains.get("xrpl_agent", {})
    lines.append(f"│  XRPL Agent    {xa.get('xrp', 0):>10.2f} XRP         (${bd.get('xrp', {}).get('usd', 0):>8.2f})")
    if xa.get("rlusd", 0) > 0:
        lines.append(f"│                {xa.get('rlusd', 0):>10.2f} RLUSD")

    # XRPL Legacy
    xl = chains.get("xrpl_legacy", {})
    if xl.get("xrp", 0) > 0:
        lines.append(f"│  XRPL Legacy   {xl.get('xrp', 0):>10.2f} XRP")

    # ICP
    icp_val = chains.get("icp", {}).get("icp", 0)
    lines.append(f"│  ICP           {icp_val:>10.2f} ICP         (${bd.get('icp_liquid', {}).get('usd', 0):>8.2f})")
    lines.append(f"│  ICP Neuron    {neuron['staked_icp']:>10.2f} ICP staked  (${bd.get('icp_staked', {}).get('usd', 0):>8.2f})")

    # Flare
    fl = chains.get("flare", {})
    lines.append(f"│  Flare         {fl.get('flr', 0):>10.2f} FLR         (${bd.get('flr', {}).get('usd', 0):>8.2f})")
    if fl.get("fxrp", 0) > 0:
        lines.append(f"│                {fl.get('fxrp', 0):>10.2f} FXRP        (${bd.get('fxrp', {}).get('usd', 0):>8.2f})")

    # Base
    ba = chains.get("base", {})
    lines.append(f"│  Base          {ba.get('eth', 0):>10.6f} ETH         (${bd.get('eth', {}).get('usd', 0):>8.2f})")
    lines.append(f"│                {ba.get('usdc', 0):>10.2f} USDC        (${ba.get('usdc', 0):>8.2f})")

    # Polygon
    pg = chains.get("polygon", {})
    if pg.get("pol", 0) > 0 or pg.get("usdc", 0) > 0:
        lines.append(f"│  Polygon       {pg.get('pol', 0):>10.2f} POL         (${bd.get('pol', {}).get('usd', 0):>8.2f})")
        if pg.get("usdc", 0) > 0:
            lines.append(f"│                {pg.get('usdc', 0):>10.2f} USDC        (${pg.get('usdc', 0):>8.2f})")

    lines.append("├───────────────────────────────────────────────")

    # Prices
    lines.append(f"│  XRP ${prices.get('xrp', 0):.2f}  ICP ${prices.get('icp', 0):.2f}  FLR ${prices.get('flr', 0):.4f}  ETH ${prices.get('eth', 0):.0f}")
    lines.append("├───────────────────────────────────────────────")
    lines.append(f"│  TOTAL                            ${p['totals']['usd']:>10.2f}")
    lines.append("└───────────────────────────────────────────────")

    if p["errors"]:
        lines.append("")
        for err in p["errors"]:
            lines.append(f"  ⚠ {err}")

    output = "\n".join(lines)
    return output


if __name__ == "__main__":
    portfolio = get_full_portfolio()
    if "--json" in sys.argv:
        print(json.dumps(portfolio, indent=2))
    else:
        print(print_summary(portfolio))
