#!/usr/bin/env python3
"""
One-time XRPL wallet consolidation script.
Moves funds from legacy wallet (r9bS...) to canister wallet (rPq1...).

Steps:
  1. Delete 2 empty USD trustlines (frees 4 XRP reserve)
  2. Send all RLUSD to canister wallet
  3. Delete now-empty RLUSD trustline (frees 2 XRP reserve)
  4. Send remaining free XRP to canister wallet
"""

import json
import sys
import time

from xrpl.clients import JsonRpcClient
from xrpl.wallet import Wallet
from xrpl.models import (
    TrustSet, Payment, IssuedCurrencyAmount, AccountInfo, AccountLines,
    AccountObjects,
)
from xrpl.transaction import submit_and_wait, autofill
from xrpl.utils import xrp_to_drops

# ── Config ──
XRPL_RPC = "https://xrplcluster.com"
CANISTER_WALLET = "rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"
SEED_FILE = "/home/nvidia/backups/xrpl-mainnet-wallet-20260208.json"

# Trustlines to delete (empty USD ones)
BITSTAMP = "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De"
GATEHUB  = "rGm7WCVp9gb4jZHWTEtGUr4dd74z2XuWhE"
RLUSD_HEX = "524C555344000000000000000000000000000000"


def main():
    # Load wallet
    with open(SEED_FILE) as f:
        data = json.load(f)
    wallet = Wallet.from_seed(data["seed"])
    print(f"Legacy wallet: {wallet.address}")
    assert wallet.address == "r9bSA9VWbumFq6G78feBbrgNwLza1KexUf", "Seed doesn't match expected address!"

    client = JsonRpcClient(XRPL_RPC)

    # ── Pre-flight check ──
    print("\n=== Pre-flight Check ===")
    acct_info = client.request(AccountInfo(account=wallet.address, ledger_index="validated"))
    balance_drops = int(acct_info.result["account_data"]["Balance"])
    owner_count = int(acct_info.result["account_data"]["OwnerCount"])
    print(f"  Balance: {balance_drops / 1_000_000:.6f} XRP")
    print(f"  OwnerCount: {owner_count}")
    print(f"  Reserve needed: {10 + owner_count * 2} XRP")

    lines_resp = client.request(AccountLines(account=wallet.address, ledger_index="validated"))
    lines = lines_resp.result.get("lines", [])
    print(f"  Trustlines: {len(lines)}")
    for line in lines:
        print(f"    {line['currency']} -> {line['account'][:16]}... balance={line['balance']}")

    # Check canister wallet can receive RLUSD
    canister_lines = client.request(AccountLines(account=CANISTER_WALLET, ledger_index="validated"))
    has_rlusd = any(l["currency"].startswith("524C5553") for l in canister_lines.result.get("lines", []))
    print(f"\n  Canister wallet has RLUSD trustline: {has_rlusd}")
    if not has_rlusd:
        print("ERROR: Canister wallet needs RLUSD trustline first!")
        sys.exit(1)

    # Check account objects (for NFTokenPage)
    objects_resp = client.request(AccountObjects(account=wallet.address, ledger_index="validated"))
    objects = objects_resp.result.get("account_objects", [])
    for obj in objects:
        print(f"  Object: {obj['LedgerEntryType']} (index: {obj.get('index', 'n/a')[:16]}...)")

    # ── Step 1: Delete empty USD trustline to Bitstamp ──
    print("\n=== Step 1: Delete empty USD trustline to Bitstamp ===")
    usd_bitstamp = [l for l in lines if l["account"] == BITSTAMP and l["currency"] == "USD"]
    if usd_bitstamp and float(usd_bitstamp[0]["balance"]) == 0:
        tx = TrustSet(
            account=wallet.address,
            limit_amount=IssuedCurrencyAmount(currency="USD", issuer=BITSTAMP, value="0"),
            flags=0x00020000,  # tfClearFreeze bit acts as delete when limit=0
        )
        print(f"  Submitting TrustSet(limit=0, USD, {BITSTAMP[:16]}...)...")
        response = submit_and_wait(tx, client, wallet)
        result = response.result.get("meta", {}).get("TransactionResult", "unknown")
        tx_hash = response.result.get("hash", "")
        print(f"  Result: {result} (hash: {tx_hash[:16]}...)")
        if result != "tesSUCCESS":
            print(f"  WARNING: Unexpected result: {result}")
    else:
        print("  Skipped (not found or non-zero balance)")

    # ── Step 2: Delete empty USD trustline to GateHub ──
    print("\n=== Step 2: Delete empty USD trustline to GateHub ===")
    usd_gatehub = [l for l in lines if l["account"] == GATEHUB and l["currency"] == "USD"]
    if usd_gatehub and float(usd_gatehub[0]["balance"]) == 0:
        tx = TrustSet(
            account=wallet.address,
            limit_amount=IssuedCurrencyAmount(currency="USD", issuer=GATEHUB, value="0"),
            flags=0x00020000,
        )
        print(f"  Submitting TrustSet(limit=0, USD, {GATEHUB[:16]}...)...")
        response = submit_and_wait(tx, client, wallet)
        result = response.result.get("meta", {}).get("TransactionResult", "unknown")
        tx_hash = response.result.get("hash", "")
        print(f"  Result: {result} (hash: {tx_hash[:16]}...)")
        if result != "tesSUCCESS":
            print(f"  WARNING: Unexpected result: {result}")
    else:
        print("  Skipped (not found or non-zero balance)")

    # ── Step 3: Send all RLUSD to canister wallet ──
    print("\n=== Step 3: Send RLUSD to canister wallet ===")
    rlusd_lines = [l for l in lines if l["currency"].startswith("524C5553")]
    if rlusd_lines and float(rlusd_lines[0]["balance"]) > 0:
        rlusd_balance = rlusd_lines[0]["balance"]
        rlusd_issuer = rlusd_lines[0]["account"]
        print(f"  Sending {rlusd_balance} RLUSD to {CANISTER_WALLET[:16]}...")
        tx = Payment(
            account=wallet.address,
            destination=CANISTER_WALLET,
            amount=IssuedCurrencyAmount(
                currency=RLUSD_HEX,
                issuer=rlusd_issuer,
                value=rlusd_balance,
            ),
        )
        response = submit_and_wait(tx, client, wallet)
        result = response.result.get("meta", {}).get("TransactionResult", "unknown")
        tx_hash = response.result.get("hash", "")
        print(f"  Result: {result} (hash: {tx_hash[:16]}...)")
        if result != "tesSUCCESS":
            print(f"  WARNING: Unexpected result: {result}")
    else:
        print("  Skipped (no RLUSD balance)")

    # ── Step 4: Delete now-empty RLUSD trustline ──
    print("\n=== Step 4: Delete RLUSD trustline ===")
    if rlusd_lines:
        rlusd_issuer = rlusd_lines[0]["account"]
        # Re-check balance after transfer
        time.sleep(2)
        lines_resp2 = client.request(AccountLines(account=wallet.address, ledger_index="validated"))
        lines2 = lines_resp2.result.get("lines", [])
        rlusd_now = [l for l in lines2 if l["currency"].startswith("524C5553")]
        if rlusd_now and float(rlusd_now[0]["balance"]) == 0:
            tx = TrustSet(
                account=wallet.address,
                limit_amount=IssuedCurrencyAmount(currency=RLUSD_HEX, issuer=rlusd_issuer, value="0"),
                flags=0x00020000,
            )
            print(f"  Submitting TrustSet(limit=0, RLUSD, {rlusd_issuer[:16]}...)...")
            response = submit_and_wait(tx, client, wallet)
            result = response.result.get("meta", {}).get("TransactionResult", "unknown")
            tx_hash = response.result.get("hash", "")
            print(f"  Result: {result} (hash: {tx_hash[:16]}...)")
        else:
            balance_left = rlusd_now[0]["balance"] if rlusd_now else "n/a"
            print(f"  Skipped (RLUSD balance still {balance_left})")
    else:
        print("  Skipped (no RLUSD trustline found)")

    # ── Step 5: Send free XRP to canister wallet ──
    print("\n=== Step 5: Send free XRP to canister wallet ===")
    # Re-check account state
    time.sleep(2)
    acct_info2 = client.request(AccountInfo(account=wallet.address, ledger_index="validated"))
    balance_drops2 = int(acct_info2.result["account_data"]["Balance"])
    owner_count2 = int(acct_info2.result["account_data"]["OwnerCount"])
    reserve_drops = (10 + owner_count2 * 2) * 1_000_000
    fee_drops = 12  # standard fee
    sendable_drops = balance_drops2 - reserve_drops - fee_drops - 1000  # 1000 drop safety margin

    print(f"  Current balance: {balance_drops2 / 1_000_000:.6f} XRP")
    print(f"  OwnerCount: {owner_count2}, Reserve: {reserve_drops / 1_000_000:.1f} XRP")
    print(f"  Sendable: {sendable_drops / 1_000_000:.6f} XRP")

    if sendable_drops > 10000:  # Only send if > 0.01 XRP
        tx = Payment(
            account=wallet.address,
            destination=CANISTER_WALLET,
            amount=str(sendable_drops),
        )
        print(f"  Sending {sendable_drops / 1_000_000:.6f} XRP to {CANISTER_WALLET[:16]}...")
        response = submit_and_wait(tx, client, wallet)
        result = response.result.get("meta", {}).get("TransactionResult", "unknown")
        tx_hash = response.result.get("hash", "")
        print(f"  Result: {result} (hash: {tx_hash[:16]}...)")
    else:
        print(f"  Skipped (only {sendable_drops} drops sendable, not worth it)")

    # ── Final state ──
    print("\n=== Final State ===")
    time.sleep(2)
    # Legacy wallet
    acct_final = client.request(AccountInfo(account=wallet.address, ledger_index="validated"))
    bal_legacy = int(acct_final.result["account_data"]["Balance"]) / 1_000_000
    oc_legacy = int(acct_final.result["account_data"]["OwnerCount"])
    print(f"  Legacy ({wallet.address[:16]}...): {bal_legacy:.6f} XRP, OwnerCount={oc_legacy}")

    # Canister wallet
    acct_canister = client.request(AccountInfo(account=CANISTER_WALLET, ledger_index="validated"))
    bal_canister = int(acct_canister.result["account_data"]["Balance"]) / 1_000_000
    oc_canister = int(acct_canister.result["account_data"]["OwnerCount"])

    canister_lines_final = client.request(AccountLines(account=CANISTER_WALLET, ledger_index="validated"))
    rlusd_final = 0
    for l in canister_lines_final.result.get("lines", []):
        if l["currency"].startswith("524C5553"):
            rlusd_final = float(l["balance"])

    print(f"  Canister ({CANISTER_WALLET[:16]}...): {bal_canister:.6f} XRP, {rlusd_final} RLUSD, OwnerCount={oc_canister}")
    print("\nDone!")


if __name__ == "__main__":
    main()
