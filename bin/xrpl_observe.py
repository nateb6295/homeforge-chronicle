#!/usr/bin/env python3
"""XRPL Observer — wallet and market observation for Objective #6.

Subcommands:
  status   — balance, trustlines, price, portfolio value
  amm      — check AMM pools relevant to holdings
  history  — recent account transactions
  capacity — what the policy engine allows right now
  brief    — one-paragraph summary for presence layer
"""

import json
import os
import sys
import time
import sqlite3
import requests
from datetime import datetime

XRPL_RPC = "https://xrplcluster.com"
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
NATE_WALLET = "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")


def xrpl_request(method, params=None):
    """Make an XRPL JSON-RPC request."""
    payload = {"method": method, "params": [params or {}]}
    r = requests.post(XRPL_RPC, json=payload, timeout=20)
    r.raise_for_status()
    return r.json().get("result", {})


def get_xrp_price():
    """Get XRP price from CoinGecko."""
    try:
        r = requests.get(COINGECKO_URL, params={
            "ids": "ripple", "vs_currencies": "usd",
            "include_24hr_change": "true"
        }, timeout=10)
        data = r.json().get("ripple", {})
        return data.get("usd", 0), data.get("usd_24h_change", 0)
    except Exception as e:
        print(f"  Price fetch error: {e}")
        return 0, 0


def cmd_status():
    """Wallet status: balances, trustlines, value."""
    print("=== XRPL Wallet Status ===\n")

    # Account info
    try:
        info = xrpl_request("account_info", {
            "account": AGENT_WALLET, "ledger_index": "validated"
        })
        acct = info.get("account_data", {})
        xrp_drops = int(acct.get("Balance", 0))
        xrp_bal = xrp_drops / 1_000_000
        reserve = 10 + (acct.get("OwnerCount", 0) * 2)  # base + owner reserve
        spendable = max(0, xrp_bal - reserve)
        seq = acct.get("Sequence", 0)
        print(f"  Address: {AGENT_WALLET}")
        print(f"  XRP balance: {xrp_bal:.6f}")
        print(f"  Reserve: {reserve} XRP (base 10 + {acct.get('OwnerCount', 0)} objects * 2)")
        print(f"  Spendable: {spendable:.6f} XRP")
        print(f"  Sequence: {seq}")
    except Exception as e:
        print(f"  Account info error: {e}")
        xrp_bal = 0
        spendable = 0

    # Trustlines
    print(f"\n  Trustlines:")
    rlusd_bal = 0
    try:
        lines = xrpl_request("account_lines", {
            "account": AGENT_WALLET, "ledger_index": "validated"
        })
        trust_lines = lines.get("lines", [])
        if not trust_lines:
            print("    (none)")
        for line in trust_lines:
            cur = line.get("currency", "???")
            bal = float(line.get("balance", 0))
            limit = line.get("limit", "0")
            issuer = line.get("account", "")[:12]
            # Decode hex currency codes
            if len(cur) == 40:
                try:
                    cur = bytes.fromhex(cur).decode("ascii").rstrip("\x00")
                except Exception:
                    pass
            print(f"    {cur}: {bal} (limit {limit}, issuer {issuer}...)")
            if cur == "RLUSD":
                rlusd_bal = bal
    except Exception as e:
        print(f"    Error: {e}")

    # Price + portfolio value
    price, change_24h = get_xrp_price()
    print(f"\n  XRP price: ${price:.4f} ({change_24h:+.1f}% 24h)")
    total_usd = (xrp_bal * price) + rlusd_bal  # RLUSD = $1
    print(f"  Portfolio value: ${total_usd:.2f}")
    print(f"    XRP: ${xrp_bal * price:.2f}")
    if rlusd_bal > 0:
        print(f"    RLUSD: ${rlusd_bal:.2f}")

    # Nate's main wallet for comparison
    print(f"\n  Nate's wallet ({NATE_WALLET[:12]}...):")
    try:
        nate_info = xrpl_request("account_info", {
            "account": NATE_WALLET, "ledger_index": "validated"
        })
        nate_acct = nate_info.get("account_data", {})
        nate_xrp = int(nate_acct.get("Balance", 0)) / 1_000_000
        print(f"    XRP: {nate_xrp:.6f} (${nate_xrp * price:.2f})")
    except Exception as e:
        print(f"    Error: {e}")


def _get_trustline_info():
    """Get trustline details from the wallet."""
    try:
        lines = xrpl_request("account_lines", {
            "account": AGENT_WALLET, "ledger_index": "validated"
        })
        return lines.get("lines", [])
    except Exception:
        return []


def cmd_amm():
    """Check AMM pools relevant to XRP holdings."""
    print("=== XRPL AMM Pools ===\n")

    # Discover RLUSD issuer from wallet trustlines
    trustlines = _get_trustline_info()
    rlusd_currency = None
    rlusd_issuer = None
    for line in trustlines:
        cur = line.get("currency", "")
        decoded = cur
        if len(cur) == 40:
            try:
                decoded = bytes.fromhex(cur).decode("ascii").rstrip("\x00")
            except Exception:
                pass
        if decoded == "RLUSD":
            rlusd_currency = cur  # keep hex format for API
            rlusd_issuer = line.get("account")
            break

    if rlusd_currency and rlusd_issuer:
        print(f"  XRP/RLUSD AMM (issuer: {rlusd_issuer[:16]}...):")
        try:
            result = xrpl_request("amm_info", {
                "asset": {"currency": "XRP"},
                "asset2": {"currency": rlusd_currency, "issuer": rlusd_issuer}
            })
            amm = result.get("amm", {})
            if amm:
                pool_xrp = amm.get("amount", "0")
                if isinstance(pool_xrp, str):
                    pool_xrp = int(pool_xrp) / 1_000_000
                pool_rlusd = amm.get("amount2", {})
                if isinstance(pool_rlusd, dict):
                    pool_rlusd = float(pool_rlusd.get("value", 0))
                lp_token = amm.get("lp_token", {})
                lp_val = float(lp_token.get("value", 0)) if isinstance(lp_token, dict) else 0
                fee = amm.get("trading_fee", 0)
                vote_slots = amm.get("vote_slots", [])

                print(f"    Pool size: {pool_xrp:,.0f} XRP + {pool_rlusd:,.0f} RLUSD")
                print(f"    LP tokens: {lp_val:,.0f}")
                print(f"    Trading fee: {fee/1000:.3f}%")
                print(f"    Vote slots: {len(vote_slots)}/8")
                if pool_xrp > 0 and pool_rlusd > 0:
                    implied_price = pool_rlusd / pool_xrp
                    print(f"    Implied XRP price: ${implied_price:.4f}")

                    # Compare to market price
                    market_price, _ = get_xrp_price()
                    if market_price > 0:
                        spread = (implied_price - market_price) / market_price * 100
                        print(f"    Market price: ${market_price:.4f}")
                        print(f"    AMM spread: {spread:+.3f}%")
                        if abs(spread) > 0.5:
                            direction = "BUY XRP from AMM" if spread < 0 else "SELL XRP to AMM"
                            print(f"    → Arb opportunity: {direction} ({abs(spread):.3f}% edge)")

                    # Estimate LP yield from fee
                    # Simple estimate: fee * daily_volume / pool_size
                    print(f"\n    LP analysis (for 10 XRP deposit):")
                    deposit_xrp = 10
                    share = deposit_xrp / pool_xrp if pool_xrp > 0 else 0
                    print(f"      Pool share: {share*100:.6f}%")
                    print(f"      Fee tier: {fee/1000:.3f}%")
                    print(f"      Note: actual yield depends on trading volume")
            else:
                err = result.get("error_message", result.get("error", "no pool found"))
                print(f"    {err}")
        except Exception as e:
            print(f"    Error: {e}")
    else:
        print("  No RLUSD trustline found — cannot check XRP/RLUSD AMM")

    # Check for any AMMs the account participates in
    print(f"\n  Account AMM positions:")
    try:
        result = xrpl_request("account_objects", {
            "account": AGENT_WALLET,
            "type": "amm",
            "ledger_index": "validated"
        })
        objects = result.get("account_objects", [])
        if not objects:
            print("    (no AMM positions)")
        for obj in objects:
            print(f"    {json.dumps(obj, indent=4)[:300]}")
    except Exception as e:
        print(f"    Error: {e}")


def cmd_history():
    """Recent account transactions."""
    print("=== Recent Transactions ===\n")
    try:
        result = xrpl_request("account_tx", {
            "account": AGENT_WALLET,
            "limit": 10,
            "ledger_index_min": -1,
            "ledger_index_max": -1,
        })
        txs = result.get("transactions", [])
        if not txs:
            print("  (no transactions)")
        for tx_wrap in txs:
            tx = tx_wrap.get("tx_json", tx_wrap.get("tx", {}))
            meta = tx_wrap.get("meta", {})
            tt = tx.get("TransactionType", "???")
            date_raw = tx.get("date", 0)
            # XRPL epoch = 946684800 seconds after Unix epoch
            if date_raw:
                ts = datetime.utcfromtimestamp(date_raw + 946684800)
                date_str = ts.strftime("%Y-%m-%d %H:%M UTC")
            else:
                date_str = "unknown"

            fee = int(tx.get("Fee", 0)) / 1_000_000
            result_code = meta.get("TransactionResult", "???")

            if tt == "Payment":
                amount = tx.get("Amount", {})
                if isinstance(amount, str):
                    amt_str = f"{int(amount)/1_000_000:.6f} XRP"
                elif isinstance(amount, dict):
                    amt_str = f"{amount.get('value', '?')} {amount.get('currency', '?')}"
                else:
                    amt_str = str(amount)
                dest = tx.get("Destination", "???")
                direction = "OUT" if tx.get("Account") == AGENT_WALLET else "IN"
                print(f"  [{date_str}] Payment {direction}: {amt_str} -> {dest[:16]}... ({result_code})")
            elif tt == "TrustSet":
                limit = tx.get("LimitAmount", {})
                cur = limit.get("currency", "???")
                if len(cur) == 40:
                    try:
                        cur = bytes.fromhex(cur).decode("ascii").rstrip("\x00")
                    except Exception:
                        pass
                print(f"  [{date_str}] TrustSet: {cur} limit={limit.get('value', '?')} ({result_code})")
            elif tt == "AMMDeposit" or tt == "AMMWithdraw":
                print(f"  [{date_str}] {tt} ({result_code})")
            else:
                print(f"  [{date_str}] {tt} (fee: {fee} XRP) ({result_code})")
    except Exception as e:
        print(f"  Error: {e}")


def cmd_capacity():
    """What the policy engine allows right now."""
    print("=== Spending Capacity ===\n")
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from xrpl_policy import create_policy_engine
        engine = create_policy_engine()

        can_go, reason = engine.can_execute_now()
        print(f"  Can transact now: {'YES' if can_go else 'NO'}")
        if not can_go:
            print(f"  Reason: {reason}")

        # Show tier thresholds (balance-adjusted)
        cfg = engine.config
        auto, delayed, cosign = engine._balance_adjusted_thresholds()
        spendable = engine._get_spendable_balance()
        balance_tier = "NORMAL" if spendable >= 20 else "CONSERVATIVE" if spendable >= 10 else "CRITICAL"
        print(f"\n  Balance-aware policy: {balance_tier} (spendable: {spendable:.2f} XRP)")
        print(f"\n  Tier thresholds (adjusted):")
        print(f"    Autonomous (auto-execute): <= {auto:.2f} XRP")
        print(f"    Delayed (5-min veto):      <= {delayed:.2f} XRP")
        print(f"    Cosign (Nate approval):    <= {cosign:.2f} XRP")
        print(f"    Prohibited:                > {cosign:.2f} XRP")
        if balance_tier != "NORMAL":
            print(f"    ⚠ Thresholds reduced due to low balance (base: auto={cfg.autonomous_max}, delayed={cfg.delayed_max}, cosign={cfg.cosign_max})")
        print(f"\n  Rate limits:")
        print(f"    Daily volume: {cfg.daily_volume_max} XRP/day")
        print(f"    Max tx/hour: {cfg.max_tx_per_hour}")
        print(f"    Min interval: {cfg.min_interval_seconds}s")

        # Show daily usage
        conn = sqlite3.connect(DB_PATH)
        day_ago = int(time.time()) - 86400
        cur = conn.execute(
            "SELECT COALESCE(SUM(amount_xrp), 0) FROM xrpl_audit_log WHERE success=1 AND timestamp > ?",
            (day_ago,)
        )
        daily_used = cur.fetchone()[0]
        print(f"\n  Daily usage: {daily_used:.2f} / {cfg.daily_volume_max} XRP")
        print(f"  Remaining: {cfg.daily_volume_max - daily_used:.2f} XRP")

        # Recent audit entries
        recent = engine.audit.recent(5)
        if recent:
            print(f"\n  Recent audit entries:")
            for entry in recent:
                ts = datetime.fromtimestamp(entry["timestamp"]).strftime("%Y-%m-%d %H:%M")
                ok = "✓" if entry["success"] else "✗"
                print(f"    [{ts}] {ok} {entry['tx_type']} {entry['amount_xrp']} XRP ({entry['decision']})")

        # Verify chain integrity
        valid, msg = engine.audit.verify_chain()
        print(f"\n  Audit chain: {msg}")
        conn.close()
    except Exception as e:
        print(f"  Error: {e}")


def cmd_brief():
    """One-paragraph summary for presence layer integration."""
    parts = []

    # Balance
    try:
        info = xrpl_request("account_info", {
            "account": AGENT_WALLET, "ledger_index": "validated"
        })
        xrp_bal = int(info.get("account_data", {}).get("Balance", 0)) / 1_000_000
    except Exception:
        xrp_bal = 0

    # Price
    price, change = get_xrp_price()
    total = xrp_bal * price

    parts.append(f"Wallet: {xrp_bal:.2f} XRP (${total:.0f} @ ${price:.2f}, {change:+.1f}% 24h)")

    # AMM check — discover RLUSD from trustlines
    try:
        trustlines = _get_trustline_info()
        rlusd_cur, rlusd_iss = None, None
        for line in trustlines:
            cur = line.get("currency", "")
            decoded = cur
            if len(cur) == 40:
                try:
                    decoded = bytes.fromhex(cur).decode("ascii").rstrip("\x00")
                except Exception:
                    pass
            if decoded == "RLUSD":
                rlusd_cur, rlusd_iss = cur, line.get("account")
                break
        if rlusd_cur and rlusd_iss:
            result = xrpl_request("amm_info", {
                "asset": {"currency": "XRP"},
                "asset2": {"currency": rlusd_cur, "issuer": rlusd_iss}
            })
            amm = result.get("amm", {})
            if amm:
                pool_xrp = amm.get("amount", "0")
                if isinstance(pool_xrp, str):
                    pool_xrp = int(pool_xrp) / 1_000_000
                pool_rlusd = amm.get("amount2", {})
                if isinstance(pool_rlusd, dict):
                    pool_rlusd = float(pool_rlusd.get("value", 0))
                if pool_xrp > 0 and pool_rlusd > 0:
                    implied = pool_rlusd / pool_xrp
                    spread = (implied - price) / price * 100
                    parts.append(f"XRP/RLUSD AMM: ${implied:.4f} ({spread:+.2f}% vs market)")
    except Exception:
        pass

    # Policy capacity
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from xrpl_policy import create_policy_engine
        engine = create_policy_engine()
        can_go, _ = engine.can_execute_now()
        parts.append(f"Policy: {'ready' if can_go else 'rate-limited'}")
    except Exception:
        pass

    print(" | ".join(parts))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    cmds = {
        "status": cmd_status,
        "amm": cmd_amm,
        "history": cmd_history,
        "capacity": cmd_capacity,
        "brief": cmd_brief,
    }
    if cmd in cmds:
        cmds[cmd]()
    else:
        print(f"Usage: {sys.argv[0]} [status|amm|history|capacity|brief]")
        sys.exit(1)
