GOAL: FTSO delegation of 3204 FLR for passive yield (Objective #6)

CONTEXT:
The wallet has 3204 FLR ($24.57) sitting idle on Flare C-Chain at 0x2C6D9E36d12fbb77dD8EDcA73739C0db075f078d.
Nate Board says "FTSO with 3204 FLR. Reconnect, don't rebuild."
FTSO delegation = wrap FLR → WFLR → delegate to data provider → earn ~4-8% APR.

WHAT I FOUND:
- portfolio.py already reads FLR balance via Flare RPC
- chronicle_sentinel already fetches XRP price from FTSO oracle
- WNat contract (WFLR): 0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d
- Old mind.py had 28 FTSO micro-predictions (all settled)
- There IS NO EVM signing capability — xrpl_transact.py uses ICP canister ECDSA for XRPL only

WHAT I NEED FROM NATE:
1. **How does the Flare wallet sign transactions?** The address 0x2C6D... — is there a private key somewhere, or should I extend the canister's ECDSA to derive an EVM key?
2. **Which FTSO data provider to delegate to?** Or should I auto-select based on reward rate?
3. **Do you want me to build canister EVM signing?** That's "rebuild" territory. Alternative: use a local keystore with passphrase (simpler, less sovereign).

ESTIMATED WORK:
- With local key: ~2 cycles. Wrap FLR, pick provider, delegate, set up reward claiming cron.
- With canister ECDSA for EVM: ~5-8 cycles. Extend canister, test on testnet, then delegate.

RETURN: ~$1-2/year at current FLR price. The value is proving the pattern, not the dollar amount.
