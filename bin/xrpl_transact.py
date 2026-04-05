#!/usr/bin/env python3
"""
XRPL Transactor — Objective #6: Economic Sovereignty

Signs and submits XRPL transactions via ICP canister threshold ECDSA.
All transactions gated by the policy engine (xrpl_policy.py).

Subcommands:
  swap sell <amount_xrp>   — Sell XRP for RLUSD via AMM
  swap buy <amount_xrp>    — Buy XRP with RLUSD from AMM
  payment <dest> <amount>  — Direct XRP payment (allowlisted destinations only)
  status                   — Wallet + policy + audit summary
  audit                    — Full audit chain with integrity check
  dry-run swap sell <amt>  — Evaluate policy without executing

Signing: dfx CLI → ICP canister (fqqku-bqaaa-aaaai-q4wha-cai) threshold ECDSA
Submission: Direct XRPL RPC (avoids sequence race with canister heartbeat)

Usage:
  python3 xrpl_transact.py swap sell 0.5
  python3 xrpl_transact.py dry-run payment rAddr... 1.0
  python3 xrpl_transact.py status
"""

import json
import os
import re
import subprocess
import sys
import time
import requests
from typing import Optional

# Import policy engine
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xrpl_policy import create_policy_engine, PolicyTier

CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
XRPL_RPC = "https://xrplcluster.com"
AGENT_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
NTFY_TOPIC = "chronicle-nate-5d786588e02c8854"

DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def xrpl_request(method, params=None):
    payload = {"method": method, "params": [params or {}]}
    r = requests.post(XRPL_RPC, json=payload, timeout=20)
    r.raise_for_status()
    return r.json().get("result", {})


def get_xrp_price():
    try:
        r = requests.get(COINGECKO_URL, params={
            "ids": "ripple", "vs_currencies": "usd",
            "include_24hr_change": "true"
        }, timeout=10)
        data = r.json().get("ripple", {})
        return data.get("usd", 0), data.get("usd_24h_change", 0)
    except Exception:
        return 0, 0


def fetch_account_info(address=None):
    """Fetch sequence, last_ledger_sequence, and fee for signing."""
    address = address or AGENT_WALLET
    info = {"sequence": 0, "last_ledger_sequence": 0, "fee_drops": 12}
    try:
        r = xrpl_request("account_info", {
            "account": address, "ledger_index": "current"
        })
        acct = r.get("account_data", {})
        info["sequence"] = int(acct.get("Sequence", 0))
        ledger_idx = int(r.get("ledger_current_index", r.get("ledger_index", 0)))
        info["last_ledger_sequence"] = ledger_idx + 20

        r2 = xrpl_request("fee")
        fee_data = r2.get("drops", {})
        info["fee_drops"] = int(fee_data.get("open_ledger_fee", 12))
    except Exception as e:
        log(f"  Account info error: {e}")
    return info


def ntfy(title, message, priority="default"):
    """Send notification via ntfy."""
    try:
        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message,
                      headers={"Title": title, "Priority": priority}, timeout=5)
    except Exception:
        pass


def dfx_call(method, candid_args, timeout=30):
    """Call canister method via dfx CLI. Returns (stdout, stderr, returncode)."""
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    env["PATH"] = os.path.dirname(DFX_BIN) + ":" + env.get("PATH", "")

    cmd = [DFX_BIN, "canister", "--network", "ic", "call",
           CANISTER_ID, method, candid_args]

    result = subprocess.run(cmd, capture_output=True, text=True,
                            timeout=timeout, env=env)
    return result.stdout, result.stderr, result.returncode


def extract_signed_blob(dfx_output):
    """Extract signed tx_blob from canister dfx output."""
    unescaped = dfx_output.replace('\\"', '"').replace('\\n', '\n').replace('\\\\', '\\')

    for pattern in ['"tx_blob"', '"signed_blob"', '"blob"']:
        idx = unescaped.find(pattern)
        if idx == -1:
            continue
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

    # Fallback: long hex string
    hex_match = re.search(r'[0-9A-Fa-f]{100,}', unescaped)
    return hex_match.group(0) if hex_match else None


def submit_to_xrpl(signed_blob):
    """Submit signed blob directly to XRPL RPC."""
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


# ═══════════════════════════════════════════════════════════════
#  Transaction Handlers
# ═══════════════════════════════════════════════════════════════

def do_swap(direction, amount_xrp, dry_run=False):
    """Execute an AMM swap (buy or sell XRP)."""
    engine = create_policy_engine()

    # Policy evaluation — swaps use self-address as destination
    decision = engine.evaluate("swap", amount_xrp, AGENT_WALLET, [])
    log(f"Policy: {decision}")

    if not decision.allowed:
        engine.record_tx("swap", amount_xrp, AGENT_WALLET, decision.tier.value,
                         "denied", "", False, decision.reason)
        log(f"DENIED: {decision.reason}")
        return False

    if decision.tier == PolicyTier.COSIGN:
        engine.record_tx("swap", amount_xrp, AGENT_WALLET, "cosign",
                         "queued", "", False, f"swap {direction}")
        ntfy("Chronicle: Swap REQUIRES APPROVAL",
             f"{amount_xrp} XRP swap ({direction}) needs operator cosign", "high")
        log(f"QUEUED for operator cosign ({amount_xrp} XRP)")
        return False

    if dry_run:
        log(f"DRY RUN: Would execute {direction} swap of {amount_xrp} XRP [{decision.tier.value}]")
        return True

    # Get account info for signing
    acct = fetch_account_info()
    if not acct["sequence"]:
        log("ERROR: Could not fetch XRPL account info")
        return False

    amount_drops = int(amount_xrp * 1_000_000)
    price, _ = get_xrp_price()

    # Sign via canister
    signed_blob = None
    sign_error = ""

    if direction == "sell":
        min_rlusd = f"{amount_xrp * price * 0.9:.6f}" if price > 0 else f"{amount_xrp * 0.1:.6f}"
        log(f"Signing: sell {amount_xrp} XRP for min {min_rlusd} RLUSD")
        candid_args = (f'({amount_drops} : nat64, "{min_rlusd}", '
                       f'{acct["fee_drops"]} : nat64, '
                       f'{acct["sequence"]} : nat32, '
                       f'{acct["last_ledger_sequence"]} : nat32)')
        stdout, stderr, rc = dfx_call("sign_swap_xrp_to_rlusd", candid_args)
    else:
        max_rlusd = f"{amount_xrp * price * 1.1:.6f}" if price > 0 else f"{amount_xrp * 3.0:.6f}"
        log(f"Signing: buy {amount_xrp} XRP for max {max_rlusd} RLUSD")
        candid_args = (f'({amount_drops} : nat64, "{max_rlusd}", '
                       f'{acct["fee_drops"]} : nat64, '
                       f'{acct["sequence"]} : nat32, '
                       f'{acct["last_ledger_sequence"]} : nat32)')
        stdout, stderr, rc = dfx_call("sign_swap_rlusd_to_xrp", candid_args)

    if rc != 0:
        sign_error = stderr.strip()
        log(f"Sign FAILED: {sign_error}")
    else:
        signed_blob = extract_signed_blob(stdout)

    if not signed_blob:
        engine.record_tx("swap", amount_xrp, AGENT_WALLET, decision.tier.value,
                         "sign_failed", "", False, sign_error or "no blob extracted")
        log(f"Sign FAILED: {sign_error or 'could not extract blob'}")
        return False

    log(f"Signed. Blob length: {len(signed_blob)}. Submitting to XRPL...")

    # Submit directly to XRPL (skip canister to avoid sequence race)
    result = submit_to_xrpl(signed_blob)
    tx_hash = result.get("hash", "")
    success = result.get("success", False)

    engine.record_tx("swap", amount_xrp, AGENT_WALLET, decision.tier.value,
                     "executed", tx_hash, success,
                     f"swap {direction} {amount_xrp} XRP @ ${price:.4f}")

    if success:
        dir_label = "SELL" if direction == "sell" else "BUY"
        ntfy(f"Chronicle: Swap {dir_label} Executed",
             f"{dir_label} {amount_xrp} XRP [{decision.tier.value}]\nhash: {tx_hash[:16]}...")
        log(f"SUCCESS: {dir_label} {amount_xrp} XRP. Hash: {tx_hash}")
        return True
    else:
        msg = result.get("engine_result_message", "unknown")
        ntfy("Chronicle: Swap FAILED", f"{amount_xrp} XRP: {msg}", "high")
        log(f"FAILED: {result.get('engine_result', 'unknown')} — {msg}")
        return False


def do_payment(destination, amount_xrp, reason="", dry_run=False):
    """Execute a direct XRP payment."""
    engine = create_policy_engine()
    memos = [reason] if reason else []

    decision = engine.evaluate("payment", amount_xrp, destination, memos)
    log(f"Policy: {decision}")

    if not decision.allowed:
        engine.record_tx("payment", amount_xrp, destination, decision.tier.value,
                         "denied", "", False, decision.reason)
        log(f"DENIED: {decision.reason}")
        return False

    if decision.tier == PolicyTier.COSIGN:
        engine.record_tx("payment", amount_xrp, destination, "cosign",
                         "queued", "", False, reason)
        ntfy("Chronicle: Payment REQUIRES APPROVAL",
             f"{amount_xrp} XRP -> {destination[:16]}...: {reason}", "high")
        log(f"QUEUED for operator cosign")
        return False

    if dry_run:
        log(f"DRY RUN: Would send {amount_xrp} XRP to {destination[:16]}... [{decision.tier.value}]")
        return True

    acct = fetch_account_info()
    if not acct["sequence"]:
        log("ERROR: Could not fetch XRPL account info")
        return False

    amount_drops = int(amount_xrp * 1_000_000)

    # Sign via canister
    candid_args = (f'(record {{ destination = "{destination}"; '
                   f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                   f'amount_drops = {amount_drops} : nat64; '
                   f'fee_drops = {acct["fee_drops"]} : nat64; '
                   f'sequence = {acct["sequence"]} : nat32 }})')
    stdout, stderr, rc = dfx_call("sign_xrp_payment", candid_args)

    signed_blob = None
    if rc != 0:
        log(f"Sign FAILED: {stderr.strip()}")
    else:
        signed_blob = extract_signed_blob(stdout)

    if not signed_blob:
        engine.record_tx("payment", amount_xrp, destination, decision.tier.value,
                         "sign_failed", "", False, stderr.strip() if rc != 0 else "no blob")
        return False

    log(f"Signed. Submitting...")
    result = submit_to_xrpl(signed_blob)
    tx_hash = result.get("hash", "")
    success = result.get("success", False)

    engine.record_tx("payment", amount_xrp, destination, decision.tier.value,
                     "executed", tx_hash, success, reason)

    if success:
        ntfy("Chronicle: Payment Executed",
             f"{amount_xrp} XRP -> {destination[:16]}...\nhash: {tx_hash[:16]}...")
        log(f"SUCCESS: Sent {amount_xrp} XRP. Hash: {tx_hash}")
        return True
    else:
        msg = result.get("engine_result_message", "unknown")
        log(f"FAILED: {result.get('engine_result', 'unknown')} — {msg}")
        return False


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def cmd_status():
    """Combined wallet + policy + audit status."""
    # Import and run observer status
    from xrpl_observe import cmd_status as obs_status, cmd_capacity as obs_cap
    obs_status()
    print()
    obs_cap()


def cmd_audit():
    """Show full audit chain with integrity verification."""
    engine = create_policy_engine()
    valid, msg = engine.audit.verify_chain()
    entries = engine.audit.recent(20)

    print(f"=== XRPL Audit Chain ===\n")
    print(f"  Integrity: {'VALID' if valid else 'BROKEN'} — {msg}\n")

    if not entries:
        print("  No audit entries.")
        return

    print(f"  Recent transactions ({len(entries)} shown):\n")
    for e in reversed(entries):
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(e["timestamp"]))
        status = "OK" if e["success"] else "FAIL"
        print(f"    [{ts}] {status} {e['tx_type']} {e['amount_xrp']} XRP "
              f"[{e['tier']}] {e['reason'][:60]}")
        if e["tx_hash"]:
            print(f"             hash: {e['tx_hash'][:32]}...")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    dry_run = False
    if args[0] == "dry-run":
        dry_run = True
        args = args[1:]

    cmd = args[0] if args else ""

    if cmd == "status":
        cmd_status()
    elif cmd == "audit":
        cmd_audit()
    elif cmd == "swap":
        if len(args) < 3:
            print("Usage: xrpl_transact.py [dry-run] swap <sell|buy> <amount_xrp>")
            return
        direction = args[1]
        if direction not in ("sell", "buy"):
            print(f"Invalid direction: {direction}. Use 'sell' or 'buy'.")
            return
        try:
            amount = float(args[2])
        except ValueError:
            print(f"Invalid amount: {args[2]}")
            return
        do_swap(direction, amount, dry_run=dry_run)
    elif cmd == "payment":
        if len(args) < 3:
            print("Usage: xrpl_transact.py [dry-run] payment <destination> <amount_xrp> [reason]")
            return
        destination = args[1]
        try:
            amount = float(args[2])
        except ValueError:
            print(f"Invalid amount: {args[2]}")
            return
        reason = " ".join(args[3:]) if len(args) > 3 else ""
        do_payment(destination, amount, reason, dry_run=dry_run)
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
