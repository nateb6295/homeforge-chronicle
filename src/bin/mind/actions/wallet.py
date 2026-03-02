"""Chronicle Mind - Wallet action handlers (swap, payment, escrow, trustlines) + XRPL helpers."""

import os
import re
import json
import subprocess
import time
import requests
from typing import Optional, Tuple, Dict

from mind.utils import log, safe_truncate, now_ts
from mind.config import (XRPL_RPC, CANISTER_ID, DFX_IDENTITY, AGENT_WALLET, NTFY_TOPIC)
from mind.fetchers import fetch_xrpl_account_info
from xrpl_policy import PolicyTier


# ── XRPL Infrastructure ─────────────────────────────────────


def submit_to_xrpl(mind, signed_blob: str) -> dict:
    """Submit signed transaction blob to XRPL.
    Tries canister submit_xrp_transaction first (for on-chain audit),
    falls back to direct XRPL RPC."""
    # Try ICPAgent native submission first (fastest, no Candid escaping)
    if mind.llm.icp_agent:
        try:
            raw = mind.llm.icp_agent.submit_xrp_transaction(signed_blob)
            log(f"    ICPAgent submit raw: {safe_truncate(raw, 300)}")
            return _parse_canister_submit_response(raw)
        except Exception as e:
            log(f"    ICPAgent submit failed: {e}, trying dfx fallback...")

    # dfx subprocess fallback for canister submission
    if mind.llm.dfx_path:
        try:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            escaped = signed_blob.replace('"', '\\"')
            r = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "submit_xrp_transaction",
                 f'("{escaped}", null)'],
                capture_output=True, text=True, timeout=30, env=env
            )
            if r.returncode == 0:
                out = r.stdout.replace('\\"', '"').replace('\\n', '\n')
                log(f"    Canister submit raw: {safe_truncate(out, 300)}")
                try:
                    json_start = out.find('{')
                    json_end = out.rfind('}')
                    if json_start != -1 and json_end > json_start:
                        raw_json = out[json_start:json_end + 1]
                        data = json.loads(raw_json)
                        if "response" in data and isinstance(data["response"], dict):
                            xrpl_resp = data["response"]
                            result = xrpl_resp.get("result", xrpl_resp)
                            engine = result.get("engine_result", "")
                            tx_hash = result.get("tx_json", {}).get("hash", result.get("hash", ""))
                            return {
                                "success": engine == "tesSUCCESS",
                                "hash": tx_hash,
                                "engine_result": engine,
                                "engine_result_message": result.get("engine_result_message", ""),
                            }
                        elif "engine_result" in data:
                            engine = data.get("engine_result", "")
                            return {
                                "success": engine == "tesSUCCESS",
                                "hash": data.get("hash", data.get("tx_hash", "")),
                                "engine_result": engine,
                                "engine_result_message": data.get("engine_result_message", ""),
                            }
                        elif data.get("success"):
                            return {
                                "success": True,
                                "hash": data.get("hash", data.get("tx_hash", "")),
                                "engine_result": "tesSUCCESS",
                                "engine_result_message": "Canister reported success",
                            }
                except (json.JSONDecodeError, AttributeError) as e:
                    log(f"    Canister response parse error: {e}")
                # DO NOT fall through to direct RPC — that would double-submit.
                hash_match = re.search(r'[A-F0-9]{64}', out)
                return {
                    "success": True,
                    "hash": hash_match.group(0) if hash_match else "",
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


def _submit_direct_to_xrpl(signed_blob: str) -> dict:
    """Submit signed transaction blob directly to XRPL (no canister round-trip).
    Avoids sequence race conditions with canister heartbeat."""
    try:
        r = requests.post(XRPL_RPC, json={
            "method": "submit",
            "params": [{"tx_blob": signed_blob}]
        }, timeout=15)
        result = r.json().get("result", {})
        engine = result.get("engine_result", "")
        tx_hash = result.get("tx_json", {}).get("hash", "")
        log(f"    Direct XRPL submit: engine={engine}, hash={tx_hash[:16] if tx_hash else 'none'}")
        return {
            "success": engine == "tesSUCCESS",
            "hash": tx_hash,
            "engine_result": engine,
            "engine_result_message": result.get("engine_result_message", ""),
        }
    except Exception as e:
        log(f"    Direct XRPL submit error: {e}")
        return {"success": False, "hash": "", "engine_result": "submitError",
                "engine_result_message": str(e)}


def _send_ntfy_tiered(tier: PolicyTier, title: str, body: str):
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


def _extract_signed_blob(dfx_output: str) -> Optional[str]:
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


def _extract_blob_from_json(raw: str) -> Optional[str]:
    """Extract tx_blob from raw canister JSON response (ic-py native path).
    Unlike _extract_signed_blob, this handles direct JSON without Candid wrapping."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
        blob = data.get("tx_blob") or data.get("signed_blob") or data.get("blob")
        if blob and isinstance(blob, str) and len(blob) > 20:
            return blob
    except (json.JSONDecodeError, AttributeError, TypeError):
        pass
    # Fallback: look for long hex string
    hex_match = re.search(r'[0-9A-Fa-f]{100,}', str(raw))
    return hex_match.group(0) if hex_match else None


def _parse_canister_submit_response(raw: str) -> dict:
    """Parse submit_xrp_transaction response from canister (ic-py native path)."""
    try:
        data = json.loads(raw)
        # Canister wraps: {"success":true,"response":{...XRPL...}}
        if "response" in data and isinstance(data["response"], dict):
            xrpl_resp = data["response"]
            result = xrpl_resp.get("result", xrpl_resp)
            engine = result.get("engine_result", "")
            tx_hash = result.get("tx_json", {}).get("hash", result.get("hash", ""))
            return {
                "success": engine == "tesSUCCESS",
                "hash": tx_hash,
                "engine_result": engine,
                "engine_result_message": result.get("engine_result_message", ""),
            }
        elif "engine_result" in data:
            engine = data.get("engine_result", "")
            return {
                "success": engine == "tesSUCCESS",
                "hash": data.get("hash", data.get("tx_hash", "")),
                "engine_result": engine,
                "engine_result_message": data.get("engine_result_message", ""),
            }
        elif data.get("success"):
            return {
                "success": True,
                "hash": data.get("hash", data.get("tx_hash", "")),
                "engine_result": "tesSUCCESS",
                "engine_result_message": "Canister reported success",
            }
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        log(f"    Canister submit response parse error: {e}")
    # Can't parse — try to find hash, assume tentative success
    hash_match = re.search(r'[A-F0-9]{64}', str(raw))
    return {
        "success": True,
        "hash": hash_match.group(0) if hash_match else "",
        "engine_result": "tesSUCCESS",
        "engine_result_message": "Parse ambiguous — assumed success",
    }


# ── XRPL Action Handlers (policy-gated) ──────────────────


def act_swap(mind, action: dict, cid: str) -> str:
    amount = float(action.get("amount_xrp", 0))
    reason = action.get("reason", "")
    direction = action.get("direction", "buy")  # "buy" = accumulate XRP, "sell" = sell XRP for RLUSD
    if direction not in ("buy", "sell"):
        direction = "buy"
    # Buy-only lock — accumulation mode. Remove this block when operator authorizes selling.
    if direction == "sell":
        log(f"    Swap direction overridden: sell -> buy (buy-only mode active)")
        direction = "buy"
    log(f'  Executing: Swap {{ amount_xrp: {amount}, direction: "{direction}", reason: "{safe_truncate(reason, 60)}" }}')

    # Policy evaluation (replaces legacy guardrails)
    # Swaps go to DEX AMM - use self address as destination for policy check
    decision = mind.policy.evaluate("swap", amount, AGENT_WALLET, [])
    log(f"    Policy: {decision}")

    if not decision.allowed:
        mind.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                              "denied", "", False, decision.reason)
        _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap DENIED",
                                   f"{amount} XRP: {decision.reason}")
        return f"false - Policy denied: {decision.reason}"

    if decision.tier == PolicyTier.PROHIBITED:
        mind.policy.record_tx("swap", amount, AGENT_WALLET, "prohibited",
                              "denied", "", False, "Amount exceeds maximum tier")
        _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap PROHIBITED",
                                   f"{amount} XRP exceeds policy limits")
        return f"false - Swap prohibited: amount {amount} XRP exceeds policy max"

    if decision.tier == PolicyTier.COSIGN:
        mind.policy.record_tx("swap", amount, AGENT_WALLET, "cosign",
                              "queued", "", False, reason)
        _send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Swap REQUIRES APPROVAL",
                                   f"{amount} XRP swap needs operator cosign: {reason}")
        return f"false - Swap queued for operator approval ({amount} XRP, cosign tier)"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        mind.db.record_swap(amount, 0, 0, 0, reason, "", False, direction)
        return "false - Swap skipped (no canister access): cannot sign transaction"

    # Sign via canister (ICPAgent native -> dfx fallback)
    try:
        acct = fetch_xrpl_account_info()
        if not acct["sequence"]:
            return "false - Could not fetch XRPL account info for signing"
        amount_drops = int(amount * 1_000_000)
        xrp_price = mind.db.latest_price("XRP")
        price_usd = xrp_price["price_usd"] if xrp_price else 0
        log(f"    Sequence: {acct['sequence']}, LastLedger: {acct['last_ledger_sequence']}, Fee: {acct['fee_drops']}")

        signed_blob = None
        sign_error = ""

        if direction == "buy":
            max_rlusd = f"{amount * price_usd * 1.1:.6f}" if price_usd > 0 else f"{amount * 3.0:.6f}"
            log(f"    Swap direction=buy, sign_swap_rlusd_to_xrp")
            # Try ICPAgent native first
            if mind.llm.icp_agent:
                try:
                    raw = mind.llm.icp_agent.sign_swap_rlusd_to_xrp(
                        amount_drops, max_rlusd, acct["fee_drops"],
                        acct["sequence"], acct["last_ledger_sequence"])
                    signed_blob = _extract_blob_from_json(raw)
                    if signed_blob:
                        log("    Signed via ICPAgent (native)")
                except Exception as e:
                    log(f"    ICPAgent sign failed: {e}")
            # dfx fallback
            if not signed_blob and mind.llm.dfx_path:
                env = os.environ.copy()
                env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                candid_args = (f'({amount_drops} : nat64, "{max_rlusd}", '
                               f'{acct["fee_drops"]} : nat64, '
                               f'{acct["sequence"]} : nat32, '
                               f'{acct["last_ledger_sequence"]} : nat32)')
                result = subprocess.run(
                    [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                     CANISTER_ID, "sign_swap_rlusd_to_xrp", candid_args],
                    capture_output=True, text=True, timeout=30, env=env)
                if result.returncode != 0:
                    sign_error = result.stderr.strip()
                else:
                    signed_blob = _extract_signed_blob(result.stdout)
        else:
            min_rlusd = f"{amount * price_usd * 0.9:.6f}" if price_usd > 0 else f"{amount * 0.1:.6f}"
            log(f"    Swap direction=sell, sign_swap_xrp_to_rlusd")
            # Try ICPAgent native first
            if mind.llm.icp_agent:
                try:
                    raw = mind.llm.icp_agent.sign_swap_xrp_to_rlusd(
                        amount_drops, min_rlusd, acct["fee_drops"],
                        acct["sequence"], acct["last_ledger_sequence"])
                    signed_blob = _extract_blob_from_json(raw)
                    if signed_blob:
                        log("    Signed via ICPAgent (native)")
                except Exception as e:
                    log(f"    ICPAgent sign failed: {e}")
            # dfx fallback
            if not signed_blob and mind.llm.dfx_path:
                env = os.environ.copy()
                env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                candid_args = (f'({amount_drops} : nat64, "{min_rlusd}", '
                               f'{acct["fee_drops"]} : nat64, '
                               f'{acct["sequence"]} : nat32, '
                               f'{acct["last_ledger_sequence"]} : nat32)')
                result = subprocess.run(
                    [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                     CANISTER_ID, "sign_swap_xrp_to_rlusd", candid_args],
                    capture_output=True, text=True, timeout=30, env=env)
                if result.returncode != 0:
                    sign_error = result.stderr.strip()
                else:
                    signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.db.record_swap(amount, 0, 0, 0, reason, "", False, direction)
            mind.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Swap signing failed: {sign_error or 'could not extract blob'}"

        # Submit signed blob DIRECTLY to XRPL (skip canister round-trip to avoid sequence race)
        submit_result = _submit_direct_to_xrpl(signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)

        xrp_price = mind.db.latest_price("XRP")
        price = xrp_price["price_usd"] if xrp_price else 0
        mind.db.record_swap(amount, amount * price, price, 0, reason, tx_hash, success, direction)
        mind.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                              "executed", tx_hash, success, reason)

        if success:
            dir_label = "BUY" if direction == "buy" else "SELL"
            _send_ntfy_tiered(decision.tier, f"Chronicle: Swap {dir_label} Executed",
                                       f"{dir_label} {amount} XRP [{decision.tier.value}]: {reason}\nhash: {tx_hash[:16]}...")
            return f"true - Swap {dir_label} submitted: {amount} XRP (hash: {tx_hash[:16]}...)"
        else:
            engine_msg = submit_result.get("engine_result_message", "unknown")
            _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Swap FAILED",
                                       f"{amount} XRP: {engine_msg}")
            return f"false - Swap submit failed: {submit_result.get('engine_result', 'unknown')}"
    except Exception as e:
        mind.db.record_swap(amount, 0, 0, 0, reason, "", False, direction)
        mind.policy.record_tx("swap", amount, AGENT_WALLET, decision.tier.value,
                              "error", "", False, str(e))
        return f"false - Swap failed: {e}"


def act_xrpl_payment(mind, action: dict, cid: str) -> str:
    """Direct XRP payment with full policy enforcement."""
    destination = action.get("destination", "")
    amount = float(action.get("amount_xrp", 0))
    reason = action.get("reason", "")
    memos = [reason] if reason else []
    log(f'  Executing: XRPLPayment {{ dest: "{destination[:16]}...", amount: {amount}, reason: "{safe_truncate(reason, 40)}" }}')

    # Policy evaluation
    decision = mind.policy.evaluate("payment", amount, destination, memos)
    log(f"    Policy: {decision}")

    if not decision.allowed:
        mind.policy.record_tx("payment", amount, destination, decision.tier.value,
                              "denied", "", False, decision.reason)
        _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Payment DENIED",
                                   f"{amount} XRP -> {destination[:16]}...: {decision.reason}")
        return f"false - Policy denied: {decision.reason}"

    if decision.tier == PolicyTier.PROHIBITED:
        mind.policy.record_tx("payment", amount, destination, "prohibited",
                              "denied", "", False, "Amount exceeds maximum tier")
        _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Payment PROHIBITED",
                                   f"{amount} XRP exceeds policy limits")
        return f"false - Payment prohibited: amount {amount} XRP exceeds policy max"

    if decision.tier == PolicyTier.COSIGN:
        mind.policy.record_tx("payment", amount, destination, "cosign",
                              "queued", "", False, reason)
        _send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Payment REQUIRES APPROVAL",
                                   f"{amount} XRP -> {destination[:16]}...: {reason}")
        return f"false - Payment queued for operator approval ({amount} XRP, cosign tier)"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - Payment skipped (no canister access): cannot sign transaction"

    # Sign via canister (ICPAgent native -> dfx fallback)
    try:
        acct = fetch_xrpl_account_info()
        if not acct["sequence"]:
            return "false - Could not fetch XRPL account info for signing"
        amount_drops = int(amount * 1_000_000)

        signed_blob = None
        sign_error = ""

        # Try ICPAgent native first
        if mind.llm.icp_agent:
            try:
                raw = mind.llm.icp_agent.sign_xrp_payment(
                    destination, amount_drops, acct["fee_drops"],
                    acct["sequence"], acct["last_ledger_sequence"])
                signed_blob = _extract_blob_from_json(raw)
                if signed_blob:
                    log("    Signed via ICPAgent (native)")
            except Exception as e:
                log(f"    ICPAgent sign_xrp_payment failed: {e}")

        # dfx fallback
        if not signed_blob and mind.llm.dfx_path:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_args = (f'(record {{ destination = "{destination}"; '
                           f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                           f'amount_drops = {amount_drops} : nat64; '
                           f'fee_drops = {acct["fee_drops"]} : nat64; '
                           f'sequence = {acct["sequence"]} : nat32 }})')
            result = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_xrp_payment", candid_args],
                capture_output=True, text=True, timeout=30, env=env)
            if result.returncode != 0:
                sign_error = result.stderr.strip()
            else:
                signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.policy.record_tx("payment", amount, destination, decision.tier.value,
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Payment signing failed: {sign_error or 'could not extract blob'}"

        submit_result = submit_to_xrpl(mind, signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)
        mind.policy.record_tx("payment", amount, destination, decision.tier.value,
                              "executed", tx_hash, success, reason)
        if success:
            _send_ntfy_tiered(decision.tier, "Chronicle: Payment Sent",
                                       f"{amount} XRP -> {destination[:16]}...\nhash: {tx_hash[:16]}...")
            return f"true - Payment sent: {amount} XRP -> {destination[:16]}... (hash: {tx_hash[:16]}...)"
        else:
            return f"false - Payment submit failed: {submit_result.get('engine_result', 'unknown')}"
    except Exception as e:
        mind.policy.record_tx("payment", amount, destination, decision.tier.value,
                              "error", "", False, str(e))
        return f"false - Payment failed: {e}"


def act_xrpl_escrow_create(mind, action: dict, cid: str) -> str:
    """Create a time-locked XRPL escrow."""
    destination = action.get("destination", AGENT_WALLET)
    amount = float(action.get("amount_xrp", 0))
    finish_hours = float(action.get("finish_after_hours", 24))
    cancel_hours = float(action.get("cancel_after_hours", 72))
    reason = action.get("reason", "")
    log(f'  Executing: XRPLEscrowCreate {{ dest: "{destination[:16]}...", amount: {amount}, '
        f'finish: {finish_hours}h, cancel: {cancel_hours}h }}')

    # Policy evaluation (escrows are checked like payments)
    decision = mind.policy.evaluate("escrow_create", amount, destination, [reason] if reason else [])
    log(f"    Policy: {decision}")

    if not decision.allowed:
        mind.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                              "denied", "", False, decision.reason)
        _send_ntfy_tiered(PolicyTier.PROHIBITED, "Chronicle: Escrow DENIED",
                                   f"{amount} XRP escrow: {decision.reason}")
        return f"false - Policy denied escrow: {decision.reason}"

    if decision.tier in (PolicyTier.COSIGN, PolicyTier.PROHIBITED):
        mind.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                              "queued", "", False, reason)
        _send_ntfy_tiered(PolicyTier.COSIGN, "Chronicle: Escrow REQUIRES APPROVAL",
                                   f"{amount} XRP escrow -> {destination[:16]}...\n"
                                   f"Finish: {finish_hours}h, Cancel: {cancel_hours}h\n{reason}")
        return f"false - Escrow queued for approval ({amount} XRP, {decision.tier.value} tier)"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - Escrow skipped (no canister access): cannot sign transaction"

    # Calculate XRPL timestamps (seconds since Ripple Epoch: 2000-01-01T00:00:00Z)
    ripple_epoch_offset = 946684800  # Unix timestamp of 2000-01-01
    now_unix = int(time.time())
    finish_after = (now_unix + int(finish_hours * 3600)) - ripple_epoch_offset
    cancel_after = (now_unix + int(cancel_hours * 3600)) - ripple_epoch_offset

    try:
        acct = fetch_xrpl_account_info()
        amount_drops = int(amount * 1_000_000)

        signed_blob = None
        sign_error = ""

        # Try ICPAgent native first
        if mind.llm.icp_agent:
            try:
                raw = mind.llm.icp_agent.sign_escrow_create(
                    destination, amount_drops, acct["fee_drops"],
                    acct["sequence"], acct["last_ledger_sequence"],
                    finish_after=finish_after, cancel_after=cancel_after)
                signed_blob = _extract_blob_from_json(raw)
                if signed_blob:
                    log("    Signed via ICPAgent (native)")
            except Exception as e:
                log(f"    ICPAgent sign_escrow_create failed: {e}")

        # dfx fallback
        if not signed_blob and mind.llm.dfx_path:
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
                f'condition = null; destination_tag = null }})')
            result = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 "--identity", DFX_IDENTITY,
                 CANISTER_ID, "sign_escrow_create", candid_arg],
                capture_output=True, text=True, timeout=30, env=env)
            if result.returncode != 0:
                sign_error = result.stderr.strip()
            else:
                signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Escrow signing failed: {sign_error or 'could not extract blob'}"

        submit_result = submit_to_xrpl(mind, signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)
        mind.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                              "executed", tx_hash, success, reason)
        if success:
            _send_ntfy_tiered(decision.tier, "Chronicle: Escrow Created",
                                       f"{amount} XRP -> {destination[:16]}...\n"
                                       f"Finish: {finish_hours}h, Cancel: {cancel_hours}h\nhash: {tx_hash[:16]}...")
            return f"true - Escrow created: {amount} XRP (finish {finish_hours}h, hash: {tx_hash[:16]}...)"
        else:
            return f"false - Escrow submit failed: {submit_result.get('engine_result', 'unknown')}"
    except Exception as e:
        mind.policy.record_tx("escrow_create", amount, destination, decision.tier.value,
                              "error", "", False, str(e))
        return f"false - Escrow creation failed: {e}"


def act_xrpl_escrow_finish(mind, action: dict, cid: str) -> str:
    """Complete an existing XRPL escrow."""
    owner = action.get("owner", AGENT_WALLET)
    sequence = int(action.get("sequence", 0))
    log(f'  Executing: XRPLEscrowFinish {{ owner: "{owner[:16]}...", sequence: {sequence} }}')

    if not sequence:
        return "false - Escrow finish requires sequence number"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - Escrow finish skipped (no canister access): cannot sign transaction"

    # Record in audit (escrow finish doesn't move new funds, just releases locked ones)
    mind.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                          "attempting", "", False, f"seq={sequence}")

    try:
        acct = fetch_xrpl_account_info()

        signed_blob = None
        sign_error = ""

        # Try ICPAgent native first
        if mind.llm.icp_agent:
            try:
                raw = mind.llm.icp_agent.sign_escrow_finish(
                    owner, sequence, acct["fee_drops"],
                    acct["sequence"], acct["last_ledger_sequence"])
                signed_blob = _extract_blob_from_json(raw)
                if signed_blob:
                    log("    Signed via ICPAgent (native)")
            except Exception as e:
                log(f"    ICPAgent sign_escrow_finish failed: {e}")

        # dfx fallback
        if not signed_blob and mind.llm.dfx_path:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_arg = (
                f'(record {{ owner = "{owner}"; '
                f'offer_sequence = {sequence} : nat32; '
                f'fee_drops = {acct["fee_drops"]} : nat64; '
                f'sequence = {acct["sequence"]} : nat32; '
                f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                f'condition = null; fulfillment = null }})')
            result = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 "--identity", DFX_IDENTITY,
                 CANISTER_ID, "sign_escrow_finish", candid_arg],
                capture_output=True, text=True, timeout=30, env=env)
            if result.returncode != 0:
                sign_error = result.stderr.strip()
            else:
                signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Escrow finish signing failed: {sign_error or 'could not extract blob'}"

        submit_result = submit_to_xrpl(mind, signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)
        mind.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                              "executed", tx_hash, success, f"seq={sequence}")
        if success:
            _send_ntfy_tiered(PolicyTier.AUTONOMOUS, "Chronicle: Escrow Finished",
                                       f"Owner: {owner[:16]}..., seq: {sequence}\nhash: {tx_hash[:16]}...")
            return f"true - Escrow finished: seq {sequence} (hash: {tx_hash[:16]}...)"
        else:
            return f"false - Escrow finish submit failed: {submit_result.get('engine_result', 'unknown')}"
    except Exception as e:
        mind.policy.record_tx("escrow_finish", 0, owner, "autonomous",
                              "error", "", False, str(e))
        return f"false - Escrow finish failed: {e}"


def act_xrpl_trustline_delete(mind, action: dict, cid: str) -> str:
    """Delete an XRPL trustline by setting limit to 0.
    Only works if the trustline balance is 0."""
    currency = action.get("currency", "")
    issuer = action.get("issuer", "")
    log(f'  Executing: XRPLTrustlineDelete {{ currency: "{currency}", issuer: "{issuer[:16]}..." }}')

    if not currency or not issuer:
        return "false - trustline_delete requires currency and issuer"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - Trustline delete skipped (no canister access): cannot sign transaction"

    # Audit the operation
    mind.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                          "attempting", "", False, f"currency={currency}")

    try:
        acct = fetch_xrpl_account_info()
        if not acct["sequence"]:
            return "false - Could not fetch XRPL account info for signing"

        signed_blob = None
        sign_error = ""

        # Try ICPAgent native first
        if mind.llm.icp_agent:
            try:
                raw = mind.llm.icp_agent.sign_trustset(
                    currency, issuer, "0", acct["fee_drops"],
                    acct["sequence"], acct["last_ledger_sequence"])
                signed_blob = _extract_blob_from_json(raw)
                if signed_blob:
                    log("    Signed via ICPAgent (native)")
            except Exception as e:
                log(f"    ICPAgent sign_trustset failed: {e}")

        # dfx fallback
        if not signed_blob and mind.llm.dfx_path:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_args = (f'(record {{ limit = "0"; '
                           f'issuer = "{issuer}"; '
                           f'currency = "{currency}"; '
                           f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                           f'fee_drops = {acct["fee_drops"]} : nat64; '
                           f'sequence = {acct["sequence"]} : nat32 }})')
            result = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_trustset", candid_args],
                capture_output=True, text=True, timeout=30, env=env)
            if result.returncode != 0:
                sign_error = result.stderr.strip()
            else:
                signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Trustline delete signing failed: {sign_error or 'could not extract blob'}"

        submit_result = submit_to_xrpl(mind, signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)
        mind.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                              "executed", tx_hash, success, f"currency={currency}")
        if success:
            _send_ntfy_tiered(PolicyTier.AUTONOMOUS, "Chronicle: Trustline Deleted",
                                       f"Removed {currency} trustline to {issuer[:16]}...\nhash: {tx_hash[:16]}...")
            return f"true - Trustline deleted: {currency} to {issuer[:16]}... (hash: {tx_hash[:16]}...)"
        else:
            engine_msg = submit_result.get("engine_result_message", "unknown")
            return f"false - Trustline delete submit failed: {submit_result.get('engine_result', 'unknown')} - {engine_msg}"
    except Exception as e:
        mind.policy.record_tx("trustline_delete", 0, issuer, "autonomous",
                              "error", "", False, str(e))
        return f"false - Trustline delete failed: {e}"


def act_xrpl_trustline_set(mind, action: dict, cid: str) -> str:
    """Set an XRPL trustline to hold a new token."""
    currency = action.get("currency", "")
    issuer = action.get("issuer", "")
    limit = action.get("limit", "1000000")  # default 1M
    reason = action.get("reason", "")
    log(f'  Executing: XRPLTrustlineSet {{ currency: "{currency}", issuer: "{issuer[:16]}...", limit: {limit} }}')

    if not currency or not issuer:
        return "false - trustline_set requires currency and issuer"

    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - Trustline set skipped (no canister access): cannot sign transaction"

    mind.policy.record_tx("trustline_set", 0, issuer, "autonomous",
                          "attempting", "", False, f"currency={currency}, reason={reason}")

    try:
        acct = fetch_xrpl_account_info()
        if not acct["sequence"]:
            return "false - Could not fetch XRPL account info for signing"

        signed_blob = None
        sign_error = ""

        if mind.llm.icp_agent:
            try:
                raw = mind.llm.icp_agent.sign_trustset(
                    currency, issuer, str(limit), acct["fee_drops"],
                    acct["sequence"], acct["last_ledger_sequence"])
                signed_blob = _extract_blob_from_json(raw)
                if signed_blob:
                    log("    Signed via ICPAgent (native)")
            except Exception as e:
                log(f"    ICPAgent sign_trustset failed: {e}")

        if not signed_blob and mind.llm.dfx_path:
            env = os.environ.copy()
            env["DFX_WARNING"] = "-mainnet_plaintext_identity"
            candid_args = (f'(record {{ limit = "{limit}"; '
                           f'issuer = "{issuer}"; '
                           f'currency = "{currency}"; '
                           f'last_ledger_sequence = {acct["last_ledger_sequence"]} : nat32; '
                           f'fee_drops = {acct["fee_drops"]} : nat64; '
                           f'sequence = {acct["sequence"]} : nat32 }})')
            result = subprocess.run(
                [mind.llm.dfx_path, "canister", "--network", "ic", "call",
                 CANISTER_ID, "sign_trustset", candid_args],
                capture_output=True, text=True, timeout=30, env=env)
            if result.returncode != 0:
                sign_error = result.stderr.strip()
            else:
                signed_blob = _extract_signed_blob(result.stdout)

        if not signed_blob:
            mind.policy.record_tx("trustline_set", 0, issuer, "autonomous",
                                  "sign_failed", "", False, sign_error or "no blob extracted")
            return f"false - Trustline set signing failed: {sign_error or 'could not extract blob'}"

        submit_result = submit_to_xrpl(mind, signed_blob)
        tx_hash = submit_result.get("hash", "")
        success = submit_result.get("success", False)
        mind.policy.record_tx("trustline_set", 0, issuer, "autonomous",
                              "executed", tx_hash, success, f"currency={currency}")
        if success:
            _send_ntfy_tiered(PolicyTier.AUTONOMOUS, "Chronicle: Trustline Set",
                                       f"Added {currency} trustline to {issuer[:16]}...\nhash: {tx_hash[:16]}...")
            return f"true - Trustline set: {currency} to {issuer[:16]}... (hash: {tx_hash[:16]}...)"
        else:
            engine_msg = submit_result.get("engine_result_message", "unknown")
            return f"false - Trustline set submit failed: {submit_result.get('engine_result', 'unknown')} - {engine_msg}"
    except Exception as e:
        mind.policy.record_tx("trustline_set", 0, issuer, "autonomous",
                              "error", "", False, str(e))
        return f"false - Trustline set failed: {e}"


def act_swap_cloud_for_icp(mind, action: dict, cid: str) -> str:
    amount = float(action.get("amount_cloud", 0))
    reason = action.get("reason", "")
    log(f'  Executing: SwapCloudForIcp {{ amount: {amount}, reason: "{safe_truncate(reason, 60)}" }}')
    # This requires ICPSwap canister interaction (different canister)
    if not mind.llm.icp_agent and not mind.llm.dfx_path:
        return "false - No canister access available for CLOUD swap"
    return f"false - CLOUD->ICP swap not yet implemented in Python (TODO)"
