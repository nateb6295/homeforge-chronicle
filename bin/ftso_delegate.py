#!/usr/bin/env python3
"""FTSO Delegation Tool — Wrap FLR, delegate, and manage auto-claiming.

Uses canister threshold ECDSA via sign_evm_transaction to:
1. Wrap FLR → WFLR (via WNat.deposit())
2. Delegate WFLR to an FTSO data provider (via WNat.delegate())
3. Set up auto-claiming via ClaimSetupManager executors

Usage:
    ftso_delegate.py status         — Check FLR/WFLR balances, delegation, and auto-claim
    ftso_delegate.py wrap AMOUNT    — Wrap FLR → WFLR
    ftso_delegate.py delegate PROVIDER BIPS — Delegate WFLR to provider (10000=100%)
    ftso_delegate.py providers      — List known FTSO providers
    ftso_delegate.py rewards        — Check reward status
    ftso_delegate.py claim          — Claim all pending rewards (auto-compounds to WFLR)
    ftso_delegate.py claim --nowrap — Claim rewards as native FLR (no wrapping)
    ftso_delegate.py executors      — List available auto-claim executors and fees
    ftso_delegate.py autoclaim EXECUTOR_ADDR DEPOSIT_FLR — Set up third-party auto-claiming
"""
import os, sys, json, time, subprocess
import requests

# Flare config
FLARE_RPC = "https://flare-api.flare.network/ext/C/rpc"
WNAT_CONTRACT = "0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d"
WALLET_ADDR = "0x80D07e16165576DBc17fe1FF865495fed4E9c387"
CHAIN_ID = 14  # Flare mainnet

# Reward contracts (queried from AddressUpdater on-chain)
CLAIM_SETUP_MANAGER = "0xd56c0ea37b848939b59e6f5cda119b3fa473b5eb"
FTSO_REWARD_MANAGER = "0xa0ff992e0b33dbdc577e488dc917a042f7b42875"
REWARD_MANAGER_V2 = "0xc8f55c5aa2c752ee285bd872855c749f4ee6239b"

# Canister config
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
if not os.path.exists(DFX_BIN):
    DFX_BIN = "/usr/local/bin/dfx"

# Known FTSO data providers (Flare mainnet)
KNOWN_PROVIDERS = {
    "ftso-au": "0xbf61Db1CDb43D196309824472015adB8B6911947",
    "ftso-eu": "0x9a46864A3b0a7805B266c445289C3fAD1E48f18e",
    "linden": "0x27cb5f4eba81976617b75953053d33ef2002dadf",
    "catenalytica": "0xad918962795547a8c997f96f7babb822612a5ffe",
    "ftso-plus": "0x3d2c08ed9b2333cbce2b8a219e02f4aa31ebccd3",
    "alphaoracle": "0x47b6effe71abd4e8cdcc56f2341beb404f804b87",
}

BIN_DIR = os.path.dirname(os.path.abspath(__file__))

# Keccak256 for computing function selectors
try:
    import sha3 as _sha3
    def _keccak256(text):
        k = _sha3.keccak_256()
        k.update(text.encode() if isinstance(text, str) else text)
        return k.hexdigest()
except ImportError:
    from Crypto.Hash import keccak as _ck
    def _keccak256(text):
        k = _ck.new(digest_bits=256)
        k.update(text.encode() if isinstance(text, str) else text)
        return k.hexdigest()

def _selector(sig):
    """Get 4-byte function selector from signature string."""
    return "0x" + _keccak256(sig)[:8]


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def rpc_call(method, params=None):
    """Make an RPC call to Flare."""
    r = requests.post(FLARE_RPC, json={
        "jsonrpc": "2.0", "method": method,
        "params": params or [], "id": 1
    }, timeout=15)
    return r.json().get("result")


def eth_call(to, data):
    """Make an eth_call."""
    return rpc_call("eth_call", [{"to": to, "data": data}, "latest"])


def get_nonce():
    result = rpc_call("eth_getTransactionCount", [WALLET_ADDR, "latest"])
    return int(result, 16) if result else None


def get_gas_price():
    result = rpc_call("eth_gasPrice")
    return int(result, 16) if result else None


def get_flr_balance():
    result = rpc_call("eth_getBalance", [WALLET_ADDR, "latest"])
    return int(result, 16) / 1e18 if result else 0


def get_wflr_balance():
    data = "0x70a08231" + WALLET_ADDR[2:].lower().rjust(64, '0')
    result = eth_call(WNAT_CONTRACT, data)
    return int(result, 16) / 1e18 if result and result != "0x" else 0


def get_delegation_info():
    """Get current delegation info from WNat."""
    data = "0x7de5b8ed" + WALLET_ADDR[2:].lower().rjust(64, '0')  # delegatesOf(address)
    result = eth_call(WNAT_CONTRACT, data)
    if result and result != "0x" and len(result) > 66:
        return result  # Raw ABI-encoded data
    return None


def dfx_call(method, candid_args, timeout=60):
    """Call canister method via dfx."""
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    env["PATH"] = os.path.dirname(DFX_BIN) + ":" + env.get("PATH", "")
    cmd = [DFX_BIN, "canister", "--network", "ic", "call",
           CANISTER_ID, method, candid_args]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    return result.stdout, result.stderr, result.returncode


def sign_and_broadcast(to, value_wei, data_hex, gas_limit=100000):
    """Sign EVM transaction via canister and broadcast to Flare."""
    nonce = get_nonce()
    if nonce is None:
        log("ERROR: Could not fetch nonce")
        return None

    gas_price = get_gas_price()
    if gas_price is None:
        gas_price = 25_000_000_000  # 25 gwei default

    # max_fee_per_gas and max_priority_fee_per_gas in wei, hex-encoded
    # (canister tries hex parse first — decimal strings like "50000000002" get misread as hex)
    max_fee_hex = "0x" + hex(gas_price * 2)[2:]
    max_priority_hex = "0x" + hex(gas_price)[2:]

    # All numeric text params need "0x" prefix — canister tries hex parse first,
    # and decimal strings like "1000000000000000000" are valid hex (= 4722 FLR, not 1 FLR!)
    value_hex = "0x" + hex(int(value_wei))[2:]
    data_arg = f'opt "{data_hex}"' if data_hex else 'null'
    candid = (f'(variant {{ Flare }}, "{to}", "{value_hex}", '
              f'{nonce} : nat64, {gas_limit} : nat64, '
              f'"{max_fee_hex}", "{max_priority_hex}", {data_arg})')

    log(f"Signing: to={to}, value={value_wei}, nonce={nonce}, gas={gas_limit}")
    stdout, stderr, rc = dfx_call("sign_evm_transaction", candid)

    if rc != 0:
        log(f"Sign FAILED: {stderr.strip()}")
        return None

    # Extract signed tx from response
    raw = stdout.replace('\\"', '"').replace('\\n', '\n')
    # Look for hex blob in response
    import re
    match = re.search(r'"(0x[0-9a-fA-F]+)"', raw)
    if not match:
        # Try looking for raw hex
        match = re.search(r'(0x[0-9a-fA-F]{100,})', raw)
    if not match:
        log(f"Could not extract signed tx from: {stdout[:200]}")
        return None

    signed_tx = match.group(1)
    log(f"Signed tx: {signed_tx[:40]}...{signed_tx[-20:]}")

    # Broadcast
    result = rpc_call("eth_sendRawTransaction", [signed_tx])
    if result:
        log(f"TX broadcast: {result}")
        return result
    else:
        # Check for error
        r = requests.post(FLARE_RPC, json={
            "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
            "params": [signed_tx], "id": 1
        }, timeout=15)
        resp = r.json()
        if "error" in resp:
            log(f"TX error: {resp['error']}")
        return None


def cmd_status():
    """Show current FLR/WFLR balances and delegation."""
    flr = get_flr_balance()
    wflr = get_wflr_balance()
    total = flr + wflr
    log(f"Wallet: {WALLET_ADDR}")
    log(f"FLR (native): {flr:.4f}")
    log(f"WFLR (wrapped): {wflr:.4f}")
    log(f"Total: {total:.4f}")

    deleg = get_delegation_info()
    if deleg:
        raw = deleg[2:]  # strip 0x
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        count = int(chunks[2], 16) if len(chunks) > 2 else 0
        mode = int(chunks[3], 16) if len(chunks) > 3 else 0
        mode_name = {0: "NOT_SET", 1: "PERCENTAGE", 2: "AMOUNT"}.get(mode, str(mode))
        log(f"Delegation mode: {mode_name}, delegates: {count}")
        # Parse delegate addresses and bips from dynamic arrays
        if count > 0 and len(chunks) > 5 + count:
            for i in range(count):
                addr = "0x" + chunks[5 + i][24:]
                bips = int(chunks[5 + count + 1 + i], 16)
                name = next((n for n, a in KNOWN_PROVIDERS.items()
                            if a.lower() == addr.lower()), addr)
                log(f"  {name}: {bips/100:.1f}%")
    else:
        log("No delegation found")

    # Auto-claim executor status
    exec_sel = _selector("claimExecutors(address)")
    wallet_padded = WALLET_ADDR[2:].lower().rjust(64, "0")
    exec_result = eth_call(CLAIM_SETUP_MANAGER, exec_sel + wallet_padded)
    if exec_result and exec_result != "0x":
        exec_raw = exec_result[2:]
        exec_count = int(exec_raw[64:128], 16) if len(exec_raw) >= 128 else 0
        if exec_count > 0:
            log(f"Auto-claim: {exec_count} executor(s) active")
            for i in range(exec_count):
                s = 128 + i * 64
                addr = "0x" + exec_raw[s + 24:s + 64]
                log(f"  Executor: {addr}")
        else:
            log("Auto-claim: NOT SET UP (run: ftso_delegate.py executors)")
    else:
        log("Auto-claim: could not check")


def cmd_wrap(amount_flr):
    """Wrap FLR → WFLR by calling WNat.deposit()."""
    flr_balance = get_flr_balance()
    if amount_flr > flr_balance - 2:  # Keep 2 FLR for gas
        log(f"ERROR: Wrap {amount_flr} FLR but only {flr_balance:.4f} available (need 2 FLR for gas)")
        return False

    value_wei = str(int(amount_flr * 1e18))
    # deposit() function selector
    deposit_data = "0xd0e30db0"

    log(f"Wrapping {amount_flr} FLR → WFLR")
    tx_hash = sign_and_broadcast(WNAT_CONTRACT, value_wei, deposit_data, gas_limit=250000)

    if tx_hash:
        log(f"Wrap TX: {tx_hash}")
        log("Waiting for confirmation...")
        time.sleep(5)
        wflr = get_wflr_balance()
        log(f"WFLR balance after wrap: {wflr:.4f}")
        return True
    return False


def cmd_delegate(provider_addr, bips):
    """Delegate WFLR voting power to FTSO data provider."""
    wflr = get_wflr_balance()
    if wflr < 1:
        log(f"ERROR: Only {wflr:.4f} WFLR wrapped. Wrap first with: ftso_delegate.py wrap AMOUNT")
        return False

    # delegate(address _to, uint256 _bips)
    # Function selector: 0x5c19a95c (delegate(address,uint256))
    # Actually WNat.delegate is: delegate(address,uint256)
    # selector = keccak256("delegate(address,uint256)")[:4]
    selector = "0x026e402b"  # delegate(address _to, uint256 _bips)
    addr_padded = provider_addr[2:].lower().rjust(64, '0')
    bips_padded = hex(bips)[2:].rjust(64, '0')
    data = selector + addr_padded + bips_padded

    log(f"Delegating {bips/100:.1f}% of {wflr:.4f} WFLR to {provider_addr}")
    tx_hash = sign_and_broadcast(WNAT_CONTRACT, "0", data, gas_limit=250000)

    if tx_hash:
        log(f"Delegate TX: {tx_hash}")
        return True
    return False


def cmd_rewards():
    """Check FTSO delegation reward status."""
    log(f"Checking rewards for {WALLET_ADDR}")

    # Check balance via Flare explorer
    try:
        r = requests.get(
            f"https://flare-explorer.flare.network/api/v2/addresses/{WALLET_ADDR}",
            timeout=15)
        data = r.json()
        balance = int(data.get("coin_balance", "0")) / 1e18
        log(f"Native FLR balance: {balance:.4f}")
    except Exception as e:
        log(f"Explorer error: {e}")

    # Check internal transactions (rewards arrive as internal txs)
    try:
        r = requests.get(
            f"https://flare-explorer.flare.network/api/v2/addresses/{WALLET_ADDR}/internal-transactions",
            timeout=15)
        items = r.json().get("items", [])
        if items:
            total_rewards = sum(int(tx.get("value", "0")) for tx in items) / 1e18
            log(f"Internal txs (potential rewards): {len(items)}, total: {total_rewards:.4f} FLR")
            for tx in items[:5]:
                val = int(tx.get("value", "0")) / 1e18
                log(f"  {val:.6f} FLR — {tx.get('timestamp', '?')}")
        else:
            log("No reward transactions yet.")
            log("Rewards accumulate per epoch (~3.5 days). Delegation started ~1 day ago.")
            log("First rewards expected around April 1-2, 2026.")
    except Exception as e:
        log(f"Internal tx check error: {e}")

    # Current delegation for context
    wflr = get_wflr_balance()
    log(f"WFLR delegated: {wflr:.4f}")
    log("Provider: Linden (100%)")
    log("Claim manually at: https://portal.flare.network/")


def cmd_executors():
    """List registered auto-claim executors and their fees."""
    log("Querying registered executors...")

    # getRegisteredExecutors(uint256 start, uint256 end)
    sel = _selector("getRegisteredExecutors(uint256,uint256)")
    start_hex = "0".rjust(64, "0")
    end_hex = hex(20)[2:].rjust(64, "0")
    result = eth_call(CLAIM_SETUP_MANAGER, sel + start_hex + end_hex)

    if not result or result == "0x":
        log("ERROR: Could not fetch executors")
        return

    raw = result[2:]
    total = int(raw[64:128], 16)
    array_offset = int(raw[0:64], 16) * 2
    count = int(raw[array_offset:array_offset + 64], 16)
    log(f"Total registered: {total}, showing: {count}")

    fee_sel = _selector("getExecutorCurrentFeeValue(address)")
    for i in range(count):
        start = array_offset + 64 + i * 64
        addr = "0x" + raw[start + 24:start + 64]
        addr_padded = addr[2:].lower().rjust(64, "0")
        fee_result = eth_call(CLAIM_SETUP_MANAGER, fee_sel + addr_padded)
        if fee_result and fee_result != "0x":
            fee_flr = int(fee_result, 16) / 1e18
            log(f"  {addr}: {fee_flr:.4f} FLR/claim")
        else:
            log(f"  {addr}: fee unknown")

    # Check our current executor setup
    exec_sel = _selector("claimExecutors(address)")
    wallet_padded = WALLET_ADDR[2:].lower().rjust(64, "0")
    exec_result = eth_call(CLAIM_SETUP_MANAGER, exec_sel + wallet_padded)
    if exec_result and exec_result != "0x":
        exec_raw = exec_result[2:]
        exec_count = int(exec_raw[64:128], 16) if len(exec_raw) >= 128 else 0
        if exec_count > 0:
            log(f"\nOur wallet has {exec_count} executor(s):")
            for i in range(exec_count):
                s = 128 + i * 64
                addr = "0x" + exec_raw[s + 24:s + 64]
                log(f"  {addr}")
        else:
            log("\nNo auto-claim executor set for our wallet.")
    log("\nTo set up: ftso_delegate.py autoclaim EXECUTOR_ADDR DEPOSIT_FLR")


def cmd_autoclaim(executor_addr, deposit_flr):
    """Set up auto-claiming by authorizing an executor."""
    flr_balance = get_flr_balance()
    if deposit_flr > flr_balance - 2:
        log(f"ERROR: Need {deposit_flr} FLR deposit but only {flr_balance:.4f} available (keeping 2 FLR for gas)")
        return False

    log(f"Setting up auto-claim executor: {executor_addr}")
    log(f"Deposit: {deposit_flr} FLR (covers {int(deposit_flr / 0.1)} claims at 0.1 FLR/claim)")

    # setClaimExecutors(address[]) — payable
    sel = _selector("setClaimExecutors(address[])")
    # ABI encode: offset to array (0x20), array length (1), executor address
    addr_padded = executor_addr[2:].lower().rjust(64, "0")
    data = (sel
            + "0000000000000000000000000000000000000000000000000000000000000020"  # offset
            + "0000000000000000000000000000000000000000000000000000000000000001"  # length = 1
            + addr_padded)                                                        # executor

    value_wei = str(int(deposit_flr * 1e18))

    log(f"Signing setClaimExecutors transaction...")
    tx_hash = sign_and_broadcast(CLAIM_SETUP_MANAGER, value_wei, data, gas_limit=300000)

    if tx_hash:
        log(f"Auto-claim setup TX: {tx_hash}")
        log("Waiting for confirmation...")
        time.sleep(5)

        # Verify
        exec_sel = _selector("claimExecutors(address)")
        wallet_padded = WALLET_ADDR[2:].lower().rjust(64, "0")
        exec_result = eth_call(CLAIM_SETUP_MANAGER, exec_sel + wallet_padded)
        if exec_result and exec_result != "0x":
            exec_raw = exec_result[2:]
            exec_count = int(exec_raw[64:128], 16) if len(exec_raw) >= 128 else 0
            if exec_count > 0:
                log(f"SUCCESS: {exec_count} executor(s) now authorized")
                log("Auto-claiming will begin next reward epoch.")
                return True
            else:
                log("WARNING: TX sent but executor not yet confirmed. Check again in a few seconds.")
        return True
    else:
        log("FAILED: Could not send setClaimExecutors transaction")
        return False


def cmd_claim(wrap=True):
    """Claim all pending FTSO delegation rewards via RewardManager V2.

    For delegators, pass empty proofs array. Single TX claims all unclaimed epochs.
    If wrap=True, rewards auto-compound into WFLR.
    """
    log("Checking claimable reward epochs...")

    # getRewardEpochIdsWithClaimableRewards()
    sel_claimable = _selector("getRewardEpochIdsWithClaimableRewards()")
    result = eth_call(REWARD_MANAGER_V2, sel_claimable)
    if not result or result == "0x":
        log("ERROR: Could not query claimable epochs")
        return False

    raw = result[2:]
    start_epoch = int(raw[0:64], 16)
    end_epoch = int(raw[64:128], 16)
    log(f"Claimable epoch range: {start_epoch} - {end_epoch}")

    # getNextClaimableRewardEpochId(address) — what's our next unclaimed?
    sel_next = _selector("getNextClaimableRewardEpochId(address)")
    wallet_padded = WALLET_ADDR[2:].lower().rjust(64, "0")
    result2 = eth_call(REWARD_MANAGER_V2, sel_next + wallet_padded)
    if not result2 or result2 == "0x":
        log("ERROR: Could not query next claimable epoch")
        return False

    next_epoch = int(result2, 16)
    log(f"Our next unclaimed epoch: {next_epoch}")

    if next_epoch > end_epoch:
        log("No rewards to claim — all caught up.")
        log("(Delegation was recent; first rewards expected ~April 6)")
        return True

    epochs_to_claim = end_epoch - next_epoch + 1
    log(f"Claiming {epochs_to_claim} epoch(s) in one transaction (up to epoch {end_epoch})")

    # Build claim(address, address, uint24, bool, RewardClaimWithProof[])
    # Selector: 0x8e33aba5
    claim_sel = "0x8e33aba5"
    epoch_hex = hex(end_epoch)[2:].rjust(64, "0")
    wrap_hex = "1".rjust(64, "0") if wrap else "0".rjust(64, "0")
    # offset to proofs array (5th param, at position 0xa0 = 160 bytes from start of params)
    proofs_offset = "00000000000000000000000000000000000000000000000000000000000000a0"
    # proofs array: length = 0
    proofs_empty = "0000000000000000000000000000000000000000000000000000000000000000"

    data = (claim_sel
            + wallet_padded           # _rewardOwner
            + wallet_padded           # _recipient (same wallet)
            + epoch_hex               # _rewardEpochId
            + wrap_hex                # _wrap
            + proofs_offset           # offset to proofs
            + proofs_empty)           # proofs length = 0

    wflr_before = get_wflr_balance()
    log(f"WFLR before claim: {wflr_before:.4f}")
    log(f"Wrap rewards: {wrap} (auto-compound into WFLR)")

    tx_hash = sign_and_broadcast(REWARD_MANAGER_V2, "0", data, gas_limit=300000)

    if tx_hash:
        log(f"Claim TX: {tx_hash}")
        log("Waiting for confirmation...")
        time.sleep(8)
        wflr_after = get_wflr_balance()
        flr_after = get_flr_balance()
        reward = wflr_after - wflr_before
        log(f"WFLR after claim: {wflr_after:.4f} (reward: +{reward:.4f} WFLR)")
        log(f"FLR balance: {flr_after:.4f}")
        return True
    else:
        log("FAILED: Claim transaction could not be sent")
        return False


def cmd_providers():
    """List known FTSO data providers."""
    log("Known FTSO data providers:")
    for name, addr in KNOWN_PROVIDERS.items():
        log(f"  {name}: {addr}")
    log("\nFind more at: https://flaremetrics.io/ftso")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]

    if action == "status":
        cmd_status()
    elif action == "wrap":
        if len(sys.argv) < 3:
            print("Usage: ftso_delegate.py wrap AMOUNT_FLR")
            sys.exit(1)
        amount = float(sys.argv[2])
        cmd_wrap(amount)
    elif action == "delegate":
        if len(sys.argv) < 4:
            print("Usage: ftso_delegate.py delegate PROVIDER_ADDRESS BIPS")
            sys.exit(1)
        provider = sys.argv[2]
        bips = int(sys.argv[3])
        cmd_delegate(provider, bips)
    elif action == "rewards":
        cmd_rewards()
    elif action == "providers":
        cmd_providers()
    elif action == "claim":
        wrap = "--nowrap" not in sys.argv
        cmd_claim(wrap=wrap)
    elif action == "executors":
        cmd_executors()
    elif action == "autoclaim":
        if len(sys.argv) < 4:
            print("Usage: ftso_delegate.py autoclaim EXECUTOR_ADDR DEPOSIT_FLR")
            print("  EXECUTOR_ADDR: Address of registered executor (see: executors)")
            print("  DEPOSIT_FLR: FLR to deposit for executor fees (e.g., 1.0 = ~10 claims)")
            sys.exit(1)
        executor = sys.argv[2]
        deposit = float(sys.argv[3])
        cmd_autoclaim(executor, deposit)
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
