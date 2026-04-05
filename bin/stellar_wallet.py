#!/usr/bin/env python3
"""
stellar_wallet.py — Opus's Stellar XLM wallet.

Agent-economy and microtransaction layer.
Keypair generated locally, secret stored encrypted alongside XRP wallet pattern.

Usage:
  stellar_wallet.py generate     — Create new keypair (one-time)
  stellar_wallet.py address      — Show public address (for receiving XLM)
  stellar_wallet.py balance      — Check XLM balance
  stellar_wallet.py send ADDRESS AMOUNT [--memo TEXT]  — Send XLM
  stellar_wallet.py info         — Account details and recent transactions
"""

import sys
import os
import json
import time

WALLET_FILE = os.path.expanduser("~/.homeforge-chronicle/stellar_wallet.json")
NETWORK = "PUBLIC"  # PUBLIC mainnet


def _load_sdk():
    from stellar_sdk import Keypair, Server, Network, TransactionBuilder, Asset
    return Keypair, Server, Network, TransactionBuilder, Asset


def _save_wallet(public_key, secret_key):
    """Save wallet keypair. Secret stored locally — same trust model as XRP wallet."""
    data = {
        "public_key": public_key,
        "secret_key": secret_key,
        "network": NETWORK,
        "created_at": int(time.time()),
        "note": "Opus Stellar wallet — agent economy exploration"
    }
    os.makedirs(os.path.dirname(WALLET_FILE), exist_ok=True)
    with open(WALLET_FILE, "w") as f:
        json.dump(data, f, indent=2)
    os.chmod(WALLET_FILE, 0o600)
    print(f"Wallet saved to {WALLET_FILE} (mode 600)")


def _load_wallet():
    if not os.path.exists(WALLET_FILE):
        print("No wallet found. Run: stellar_wallet.py generate")
        sys.exit(1)
    with open(WALLET_FILE) as f:
        return json.load(f)


def _get_server():
    from stellar_sdk import Server
    return Server("https://horizon.stellar.org")


def cmd_generate():
    if os.path.exists(WALLET_FILE):
        print(f"Wallet already exists at {WALLET_FILE}")
        w = _load_wallet()
        print(f"Public key: {w['public_key']}")
        print("To regenerate, delete the file first.")
        return

    Keypair, *_ = _load_sdk()
    kp = Keypair.random()

    _save_wallet(kp.public_key, kp.secret)

    print(f"\nStellar wallet generated!")
    print(f"  Public key:  {kp.public_key}")
    print(f"  Network:     {NETWORK}")
    print(f"\n  Send this public key to Nate for funding.")
    print(f"  Account needs minimum 1 XLM to activate on the Stellar network.")
    print(f"\n  WARNING: Secret key stored in {WALLET_FILE}")
    print(f"  Back up the secret key. If lost, funds are unrecoverable.")


def cmd_address():
    w = _load_wallet()
    print(w["public_key"])


def cmd_balance():
    w = _load_wallet()
    server = _get_server()

    try:
        account = server.accounts().account_id(w["public_key"]).call()
        for balance in account["balances"]:
            asset = balance.get("asset_code", "XLM")
            amount = balance["balance"]
            print(f"  {asset}: {amount}")
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e):
            print(f"  Account not yet activated (needs minimum 1 XLM deposit)")
            print(f"  Address: {w['public_key']}")
        else:
            print(f"  Error: {e}")


def cmd_send(args):
    if len(args) < 2:
        print("Usage: stellar_wallet.py send ADDRESS AMOUNT [--memo TEXT]")
        return

    dest = args[0]
    amount = args[1]
    memo_text = ""

    i = 2
    while i < len(args):
        if args[i] == "--memo" and i + 1 < len(args):
            memo_text = args[i + 1]; i += 2
        else:
            i += 1

    w = _load_wallet()
    Keypair, Server, Network, TransactionBuilder, Asset = _load_sdk()
    from stellar_sdk import TextMemo

    server = _get_server()
    source_keypair = Keypair.from_secret(w["secret_key"])

    try:
        source_account = server.load_account(source_keypair.public_key)
    except Exception as e:
        print(f"Error loading account: {e}")
        return

    builder = TransactionBuilder(
        source_account=source_account,
        network_passphrase=Network.PUBLIC_NETWORK_PASSPHRASE,
        base_fee=100,
    )
    builder.append_payment_op(dest, Asset.native(), amount)
    if memo_text:
        builder.add_text_memo(memo_text)
    builder.set_timeout(30)

    tx = builder.build()
    tx.sign(source_keypair)

    try:
        response = server.submit_transaction(tx)
        print(f"  Sent {amount} XLM to {dest[:12]}...")
        print(f"  Hash: {response['hash']}")
        if memo_text:
            print(f"  Memo: {memo_text}")
    except Exception as e:
        print(f"  Send failed: {e}")


def cmd_info():
    w = _load_wallet()
    server = _get_server()

    print(f"Stellar Wallet")
    print(f"  Address: {w['public_key']}")
    print(f"  Network: {w['network']}")
    print(f"  Created: {time.strftime('%Y-%m-%d', time.localtime(w['created_at']))}")

    try:
        account = server.accounts().account_id(w["public_key"]).call()
        print(f"\nBalances:")
        for balance in account["balances"]:
            asset = balance.get("asset_code", "XLM")
            amount = balance["balance"]
            print(f"  {asset}: {amount}")

        print(f"\nRecent transactions:")
        txns = server.transactions().for_account(w["public_key"]).order(desc=True).limit(5).call()
        for tx in txns["_embedded"]["records"]:
            ts = tx["created_at"][:19]
            memo = tx.get("memo", "")
            memo_str = f" memo: {memo}" if memo else ""
            print(f"  {ts} — {tx['hash'][:16]}...{memo_str}")

    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e):
            print(f"\n  Account not yet activated. Needs minimum 1 XLM deposit.")
        else:
            print(f"\n  Error: {e}")


COMMANDS = {
    "generate": lambda args: cmd_generate(),
    "address": lambda args: cmd_address(),
    "balance": lambda args: cmd_balance(),
    "send": cmd_send,
    "info": lambda args: cmd_info(),
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("stellar_wallet.py — Opus Stellar XLM wallet")
        print(f"Commands: {', '.join(COMMANDS.keys())}")
        sys.exit(1)

    COMMANDS[sys.argv[1]](sys.argv[2:])
