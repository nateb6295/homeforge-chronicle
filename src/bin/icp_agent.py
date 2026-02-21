"""
ICPAgent — Native Python canister access for Chronicle Mind.
Replaces all dfx subprocess calls with ic-py library.

Requires: ic-py 1.0.1 (patched for secp256k1 low-S + cbor2 bytes keys)
Patches applied to: ~/.local/lib/python3.10/site-packages/ic/identity.py
                     ~/.local/lib/python3.10/site-packages/ic/agent.py
"""

import json
import os
import time
from typing import Optional, List, Any


class ICPAgent:
    """Native Python ICP canister agent.
    Uses ic-py Canister class for query calls and update calls.

    ic-py patches required (applied at install time):
    1. identity.py: secp256k1 sign_digest + low-S normalization
    2. agent.py: read_state_raw cbor2 bytes-key handling
    """

    def __init__(self, pem_path: str, canister_id: str, did_path: str, identity_name: str = "chronicle-auto"):
        self.canister_id = canister_id
        self.identity_name = identity_name
        self._canister = None
        self._available = False

        try:
            from ic.canister import Canister
            from ic.client import Client
            from ic.identity import Identity
            from ic.agent import Agent

            with open(pem_path, 'r') as f:
                pem_data = f.read()
            self._ident = Identity.from_pem(pem_data)

            with open(did_path, 'r') as f:
                did = f.read()

            self._client = Client(url='https://ic0.app')
            self._agent = Agent(self._ident, self._client)
            self._canister = Canister(
                agent=self._agent,
                canister_id=canister_id,
                candid=did,
            )
            self._available = True
        except Exception as e:
            print(f"[ICPAgent] Init failed: {e}")
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    def _opt(self, value):
        """Convert Python None to ic-py opt convention ([] = None, [val] = Some(val))."""
        if value is None:
            return []
        return value

    # ── Query methods (read-only, fast) ──────────────────────

    def health(self) -> str:
        result = self._canister.health()
        return result[0] if result else ""

    def get_capsule_count(self) -> int:
        result = self._canister.get_capsule_count()
        return result[0] if result else 0

    def get_wallet_address(self) -> str:
        """Get wallet address via update call (threshold ECDSA)."""
        result = self._canister.get_wallet_address()
        return result[0] if result else ""

    def get_mind_state(self) -> Optional[dict]:
        result = self._canister.get_mind_state()
        if result and result[0]:
            return result[0][0] if isinstance(result[0], list) and result[0] else result[0]
        return None

    def get_mind_thoughts(self, limit: int = 10) -> list:
        result = self._canister.get_mind_thoughts(limit)
        return result[0] if result else []

    # ── Update methods (write, authenticated) ────────────────

    def store_mind_thought(self, cycle_id: str, reasoning: str, context: str, actions: List[str]):
        """Store a thought on-chain."""
        self._canister.store_mind_thought(cycle_id, reasoning, context, actions)

    def llm_prompt(self, prompt: str, system: Optional[str] = None, model: Optional[str] = None) -> str:
        """Call ICP on-chain LLM."""
        result = self._canister.llm_prompt(prompt, self._opt(system), self._opt(model))
        if result:
            raw = result[0] if isinstance(result, list) else result
            # Unwrap canister response wrapper if present
            if isinstance(raw, str):
                try:
                    data = json.loads(raw)
                    if isinstance(data, dict) and data.get("success") and "response" in data:
                        return data["response"]
                except (json.JSONDecodeError, ValueError):
                    pass
            return raw
        return ""

    def submit_xrp_transaction(self, signed_blob: str, memo: Optional[str] = None) -> str:
        """Submit signed XRPL transaction via canister."""
        result = self._canister.submit_xrp_transaction(signed_blob, self._opt(memo))
        return result[0] if result else ""

    def sign_swap_xrp_to_rlusd(self, amount_drops: int, min_rlusd: str,
                                 fee_drops: int, sequence: int, last_ledger_seq: int) -> str:
        """Sign XRP→RLUSD swap transaction."""
        result = self._canister.sign_swap_xrp_to_rlusd(
            amount_drops, min_rlusd, fee_drops, sequence, last_ledger_seq
        )
        return result[0] if result else ""

    def sign_swap_rlusd_to_xrp(self, amount_drops: int, max_rlusd: str,
                                 fee_drops: int, sequence: int, last_ledger_seq: int) -> str:
        """Sign RLUSD→XRP swap transaction."""
        result = self._canister.sign_swap_rlusd_to_xrp(
            amount_drops, max_rlusd, fee_drops, sequence, last_ledger_seq
        )
        return result[0] if result else ""

    def sign_xrp_payment(self, destination: str, amount_drops: int,
                          fee_drops: int, sequence: int, last_ledger_seq: int) -> str:
        """Sign XRP payment transaction using record type."""
        result = self._canister.sign_xrp_payment({
            'destination': destination,
            'amount_drops': amount_drops,
            'fee_drops': fee_drops,
            'sequence': sequence,
            'last_ledger_sequence': last_ledger_seq,
        })
        return result[0] if result else ""

    def sign_escrow_create(self, destination: str, amount_drops: int,
                            fee_drops: int, sequence: int, last_ledger_seq: int,
                            finish_after: Optional[int] = None,
                            cancel_after: Optional[int] = None) -> str:
        """Sign XRPL escrow creation."""
        params = {
            'destination': destination,
            'amount_drops': amount_drops,
            'fee_drops': fee_drops,
            'sequence': sequence,
            'last_ledger_sequence': last_ledger_seq,
            'finish_after': [finish_after] if finish_after is not None else [],
            'cancel_after': [cancel_after] if cancel_after is not None else [],
            'condition': [],
            'destination_tag': [],
        }
        result = self._canister.sign_escrow_create(params)
        return result[0] if result else ""

    def sign_escrow_finish(self, owner: str, offer_sequence: int,
                            fee_drops: int, sequence: int, last_ledger_seq: int,
                            condition: Optional[str] = None,
                            fulfillment: Optional[str] = None) -> str:
        """Sign XRPL escrow finish."""
        params = {
            'owner': owner,
            'offer_sequence': offer_sequence,
            'fee_drops': fee_drops,
            'sequence': sequence,
            'last_ledger_sequence': last_ledger_seq,
            'condition': [condition] if condition else [],
            'fulfillment': [fulfillment] if fulfillment else [],
        }
        result = self._canister.sign_escrow_finish(params)
        return result[0] if result else ""

    def sign_trustset(self, currency: str, issuer: str, limit_value: str,
                       fee_drops: int, sequence: int, last_ledger_seq: int) -> str:
        """Sign XRPL TrustSet transaction."""
        params = {
            'currency': currency,
            'issuer': issuer,
            'limit_value': limit_value,
            'fee_drops': fee_drops,
            'sequence': sequence,
            'last_ledger_sequence': last_ledger_seq,
        }
        result = self._canister.sign_trustset(params)
        return result[0] if result else ""

    def mark_findings_retrieved(self, finding_ids: List[int]) -> str:
        """Mark research findings as retrieved."""
        result = self._canister.mark_findings_retrieved(finding_ids)
        return result[0] if result else ""

    def add_mind_thought(self, reasoning: str, context: str, summary: str, actions: List[str]) -> int:
        """Add thought via add_mind_thought (returns nat64 id)."""
        result = self._canister.add_mind_thought(reasoning, context, summary, actions)
        return result[0] if result else 0


def create_icp_agent(identity_name: str = "chronicle-auto",
                     canister_id: str = "fqqku-bqaaa-aaaai-q4wha-cai") -> Optional[ICPAgent]:
    """Factory function to create an ICPAgent with standard paths."""
    pem_path = os.path.expanduser(f"~/.config/dfx/identity/{identity_name}/identity.pem")
    did_path = os.path.expanduser("~/chronicle/chronicle_backend.did")

    if not os.path.isfile(pem_path):
        return None
    if not os.path.isfile(did_path):
        return None

    try:
        agent = ICPAgent(pem_path, canister_id, did_path, identity_name)
        return agent if agent.available else None
    except Exception:
        return None
