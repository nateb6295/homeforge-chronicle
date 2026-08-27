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
ROSETTA_API = "https://rosetta-api.internetcomputer.org/account/balance"
STELLAR_HORIZON = "https://horizon.stellar.org"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

# ── Wallet Addresses ──
XRPL_AGENT = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
XRPL_LEGACY = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
FLARE_ADDR = "0x2C6D9E36d12fbb77dD8EDcA73739C0db075f078d"
BASE_ADDR = "0x80D07e16165576DBc17fe1FF865495fed4E9c387"
ICP_ACCOUNT_ID = "12f27b12d5e2056eaad9a355cbcfc370838e34f81035a94b8bf57701ffa91cc9"
STELLAR_ADDR = "GDQC72SKESV27UVGR6HEOTEH25EXIAOHWAFL4HNXOJLTPOT5T5HLONJO"

# ── Token Contracts ──
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"       # 6 decimals
FXRP_FLARE = "0xAd552A648C74D49E10027AB8a618A3ad4901c5bE"      # 18 decimals (FAsset)
WFLR_FLARE = "0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d"      # 18 decimals (Wrapped FLR / WNat)
STXRP_VAULT = "0x4C18Ff3C89632c3Dd62E796c0aFA5c07c4c1B2b3"    # 6 decimals (Firelight ERC-4626)

TIMEOUT = 15

# ── ERC-4626 Vault Selectors ──
VAULT_TOTAL_ASSETS = "0x01e1d114"    # totalAssets()
VAULT_TOTAL_SUPPLY = "0x18160ddd"    # totalSupply()
# convertToAssets(uint256): 0x07a2d13a + padded shares
VAULT_CONVERT_ASSETS = "0x07a2d13a"


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


def _call_vault(selector: str) -> int:
    """Call a parameterless ERC-4626 method on the stXRP vault."""
    r = requests.post(FLARE_RPC, json={
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": STXRP_VAULT, "data": selector}, "latest"]
    }, timeout=TIMEOUT)
    return int(r.json().get("result", "0x0"), 16)


def _call_vault_with_arg(selector: str, arg: int) -> int:
    """Call an ERC-4626 method with a uint256 argument."""
    padded_arg = hex(arg)[2:].zfill(64)
    r = requests.post(FLARE_RPC, json={
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": STXRP_VAULT, "data": selector + padded_arg}, "latest"]
    }, timeout=TIMEOUT)
    return int(r.json().get("result", "0x0"), 16)


def fetch_vault_stats() -> dict:
    """Fetch stXRP vault health: total assets, total supply, share price."""
    stats = {"total_assets": 0.0, "total_supply": 0.0, "share_price": 1.0, "error": None}
    try:
        total_assets_raw = _call_vault(VAULT_TOTAL_ASSETS)
        total_supply_raw = _call_vault(VAULT_TOTAL_SUPPLY)
        # Both are 6 decimals (FXRP underlying)
        stats["total_assets"] = total_assets_raw / 1e6
        stats["total_supply"] = total_supply_raw / 1e6
        if total_supply_raw > 0:
            stats["share_price"] = total_assets_raw / total_supply_raw
        # What would 1 stXRP (1e6 units) convert to in assets?
        one_share = 1_000_000  # 1 stXRP in raw units
        assets_for_one = _call_vault_with_arg(VAULT_CONVERT_ASSETS, one_share)
        stats["convert_1_share"] = assets_for_one / 1e6
    except Exception as e:
        stats["error"] = str(e)
    return stats


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


def _fetch_stellar() -> float:
    """Fetch XLM balance from Stellar Horizon."""
    r = requests.get(f"{STELLAR_HORIZON}/accounts/{STELLAR_ADDR}", timeout=TIMEOUT)
    if r.status_code == 200:
        for bal in r.json().get("balances", []):
            if bal.get("asset_type") == "native":
                return float(bal.get("balance", 0))
    return 0.0


def _fetch_prices() -> dict:
    """Fetch USD prices for all assets in one CoinGecko call."""
    prices = {"xrp": 0.0, "icp": 0.0, "flr": 0.0, "eth": 0.0, "xlm": 0.0}
    try:
        r = requests.get(COINGECKO_URL, params={
            "ids": "ripple,internet-computer,flare-networks,ethereum,stellar",
            "vs_currencies": "usd",
            "precision": "full"
        }, timeout=TIMEOUT)
        data = r.json()
        prices["xrp"] = data.get("ripple", {}).get("usd", 0.0)
        prices["icp"] = data.get("internet-computer", {}).get("usd", 0.0)
        prices["flr"] = data.get("flare-networks", {}).get("usd", 0.0)
        prices["eth"] = data.get("ethereum", {}).get("usd", 0.0)
        prices["xlm"] = data.get("stellar", {}).get("usd", 0.0)
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
    result = {"flr": 0.0, "wflr": 0.0, "fxrp": 0.0, "stxrp": 0.0, "error": None}
    try:
        # Main FLR holdings are on the canister-derived EVM address
        result["flr"] = _fetch_evm_native(FLARE_RPC, BASE_ADDR)
        # Also check the legacy Flare address
        legacy_flr = _fetch_evm_native(FLARE_RPC, FLARE_ADDR)
        result["flr"] += legacy_flr
        # WFLR (wrapped/delegated FLR) — check both addresses
        result["wflr"] = _fetch_erc20(FLARE_RPC, WFLR_FLARE, BASE_ADDR, 18)
        result["wflr"] += _fetch_erc20(FLARE_RPC, WFLR_FLARE, FLARE_ADDR, 18)
        # FXRP — check both addresses
        result["fxrp"] = _fetch_erc20(FLARE_RPC, FXRP_FLARE, BASE_ADDR, 18)
        result["fxrp"] += _fetch_erc20(FLARE_RPC, FXRP_FLARE, FLARE_ADDR, 18)
        # stXRP (Firelight vault) — check both addresses, 6 decimals
        result["stxrp"] = _fetch_erc20(FLARE_RPC, STXRP_VAULT, BASE_ADDR, 6)
        result["stxrp"] += _fetch_erc20(FLARE_RPC, STXRP_VAULT, FLARE_ADDR, 6)
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


def _chain_stellar():
    result = {"xlm": 0.0, "error": None}
    try:
        result["xlm"] = _fetch_stellar()
    except Exception as e:
        result["error"] = str(e)
    return ("stellar", result)

def _chain_prices():
    return ("prices", _fetch_prices())


# ── Main entry point ──


def _clean_chain(data: dict) -> dict:
    """Remove error field if null, remove zero balances."""
    cleaned = {}
    for k, v in data.items():
        if k == "error" and v is None:
            continue
        if isinstance(v, (int, float)) and v == 0 and k != "error":
            continue
        cleaned[k] = v
    return cleaned if cleaned else None

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
        _chain_stellar,
        _chain_flare,
        _chain_base,
        _chain_prices,
    ]

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = {pool.submit(fn): fn.__name__ for fn in fetchers}
        vault_future = pool.submit(fetch_vault_stats)
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
        try:
            portfolio["vault"] = vault_future.result()
        except Exception as e:
            portfolio["errors"].append(f"vault: {e}")

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

    # FLR (native)
    flr = chains.get("flare", {}).get("flr", 0)
    breakdown["flr"] = {"amount": round(flr, 2), "usd": round(flr * p.get("flr", 0), 2)}

    # WFLR (wrapped/delegated — same price as FLR)
    wflr = chains.get("flare", {}).get("wflr", 0)
    breakdown["wflr"] = {"amount": round(wflr, 2), "usd": round(wflr * p.get("flr", 0), 2)}

    # FXRP (priced at XRP rate)
    fxrp = chains.get("flare", {}).get("fxrp", 0)
    breakdown["fxrp"] = {"amount": round(fxrp, 2), "usd": round(fxrp * p.get("xrp", 0), 2)}

    # stXRP — use vault share price if available, else 1:1
    stxrp = chains.get("flare", {}).get("stxrp", 0)
    vault = portfolio.get("vault", {})
    stxrp_price_mult = vault.get("share_price", 1.0)
    stxrp_xrp_value = stxrp * stxrp_price_mult
    breakdown["stxrp"] = {"amount": round(stxrp, 2), "usd": round(stxrp_xrp_value * p.get("xrp", 0), 2)}

    # XLM
    xlm = chains.get("stellar", {}).get("xlm", 0)
    breakdown["xlm"] = {"amount": round(xlm, 2), "usd": round(xlm * p.get("xlm", 0), 2)}

    # ETH on Base
    eth = chains.get("base", {}).get("eth", 0)
    breakdown["eth"] = {"amount": round(eth, 6), "usd": round(eth * p.get("eth", 0), 2)}

    # USDC (Base)
    usdc_total = chains.get("base", {}).get("usdc", 0)
    breakdown["usdc"] = {"amount": round(usdc_total, 2), "usd": round(usdc_total, 2)}

    # POL

    portfolio["totals"]["breakdown"] = breakdown
    portfolio["totals"]["usd"] = round(sum(v["usd"] for v in breakdown.values()), 2)

    # Clean output — remove error:null noise and zero-balance chains
    cleaned_chains = {}
    for name, data in portfolio["chains"].items():
        clean = _clean_chain(data)
        if clean:
            cleaned_chains[name] = clean
    portfolio["chains"] = cleaned_chains

    # Remove empty errors list
    if not portfolio["errors"]:
        del portfolio["errors"]

    # Clean breakdown — remove zero-USD entries
    portfolio["totals"]["breakdown"] = {
        k: v for k, v in breakdown.items() if v.get("usd", 0) > 0.005 or v.get("amount", 0) > 0.005
    }

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

    # Stellar
    st = chains.get("stellar", {})
    if st.get("xlm", 0) > 0:
        lines.append(f"│  Stellar       {st.get('xlm', 0):>10.2f} XLM         (${bd.get('xlm', {}).get('usd', 0):>8.2f})")

    # Flare
    fl = chains.get("flare", {})
    lines.append(f"│  Flare         {fl.get('flr', 0):>10.2f} FLR         (${bd.get('flr', {}).get('usd', 0):>8.2f})")
    if fl.get("wflr", 0) > 0:
        lines.append(f"│                {fl.get('wflr', 0):>10.2f} WFLR (dlg)  (${bd.get('wflr', {}).get('usd', 0):>8.2f})")
    if fl.get("fxrp", 0) > 0:
        lines.append(f"│                {fl.get('fxrp', 0):>10.2f} FXRP        (${bd.get('fxrp', {}).get('usd', 0):>8.2f})")
    if fl.get("stxrp", 0) > 0:
        vault = p.get("vault", {})
        sp = vault.get("share_price", 1.0)
        sp_tag = f" @{sp:.4f}" if sp != 1.0 else ""
        lines.append(f"│                {fl.get('stxrp', 0):>10.2f} stXRP{sp_tag}  (${bd.get('stxrp', {}).get('usd', 0):>8.2f})")

    # Base
    ba = chains.get("base", {})
    lines.append(f"│  Base          {ba.get('eth', 0):>10.6f} ETH         (${bd.get('eth', {}).get('usd', 0):>8.2f})")
    lines.append(f"│                {ba.get('usdc', 0):>10.2f} USDC        (${ba.get('usdc', 0):>8.2f})")

    lines.append("├───────────────────────────────────────────────")

    # Prices
    lines.append(f"│  XRP ${prices.get('xrp', 0):.2f}  ICP ${prices.get('icp', 0):.2f}  XLM ${prices.get('xlm', 0):.2f}  FLR ${prices.get('flr', 0):.4f}  ETH ${prices.get('eth', 0):.0f}")
    lines.append("├───────────────────────────────────────────────")
    lines.append(f"│  TOTAL                            ${p['totals']['usd']:>10.2f}")
    lines.append("└───────────────────────────────────────────────")

    if p.get("errors"):
        lines.append("")
        for err in p["errors"]:
            lines.append(f"  ⚠ {err}")

    output = "\n".join(lines)
    return output


def print_vault(p: dict) -> str:
    """Pretty-print stXRP vault analytics."""
    vault = p.get("vault", {})
    prices = p.get("prices", {})
    xrp_price = prices.get("xrp", 0)
    stxrp_held = p.get("chains", {}).get("flare", {}).get("stxrp", 0)

    lines = []
    lines.append("┌── stXRP Vault (Firelight Finance ERC-4626) ────")
    lines.append(f"│  Contract:        {STXRP_VAULT}")
    lines.append(f"│  Total Assets:    {vault.get('total_assets', 0):>12.2f} FXRP")
    lines.append(f"│  Total Supply:    {vault.get('total_supply', 0):>12.2f} stXRP")
    share_price = vault.get("share_price", 1.0)
    lines.append(f"│  Share Price:     {share_price:>12.6f} FXRP/stXRP")
    convert_1 = vault.get("convert_1_share", share_price)
    lines.append(f"│  1 stXRP =        {convert_1:>12.6f} FXRP")
    if share_price > 1.0:
        gain_pct = (share_price - 1.0) * 100
        lines.append(f"│  Vault Gain:      {gain_pct:>11.2f}% above par")
    lines.append("├───────────────────────────────────────────────")
    lines.append(f"│  Your stXRP:      {stxrp_held:>12.2f} stXRP")
    underlying = stxrp_held * share_price
    lines.append(f"│  Underlying:      {underlying:>12.2f} FXRP")
    lines.append(f"│  USD Value:       ${underlying * xrp_price:>11.2f}")
    if vault.get("error"):
        lines.append(f"│  ⚠ {vault['error']}")
    lines.append("└───────────────────────────────────────────────")
    return "\n".join(lines)


if __name__ == "__main__":
    portfolio = get_full_portfolio()
    if "--json" in sys.argv:
        print(json.dumps(portfolio, indent=2))
    elif "--vault" in sys.argv:
        print(print_vault(portfolio))
    else:
        print(print_summary(portfolio))
