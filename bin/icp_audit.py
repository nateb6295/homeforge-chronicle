#!/usr/bin/env python3
"""ICP Canister Cycle Burn Audit — Maps every operation to its cost tier.

Nate's directive: "The canister is RULE based, so we should be able to develop
TOOLS that do the exact function and that tooling has built in cycle burns
that we can predict."

This script:
1. Snapshots current cycle balances across all 3 canisters
2. Categorizes every canister function by cost tier
3. Estimates per-operation burns based on ICP pricing model
4. Reports what our actual daily/weekly spend looks like

Usage:
    python3 icp_audit.py              # full audit
    python3 icp_audit.py snapshot     # just balance snapshot
    python3 icp_audit.py costs        # cost tier breakdown
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta

PDT = timezone(timedelta(hours=-7))

# ── Canister IDs ──
CANISTERS = {
    "backend":  "fqqku-bqaaa-aaaai-q4wha-cai",
    "frontend": "nbt4b-giaaa-aaaai-q33lq-cai",
    "lab":      "4vr3t-eqaaa-aaaai-q6kea-cai",
    # Keeper lives inside backend as of v16 architecture
}

DFX_ENV = {"DFX_WARNING": "-mainnet_plaintext_identity"}
IDENTITY = "chronicle-auto"

# ── ICP Cycle Cost Model (2026 pricing) ──
# Reference: https://internetcomputer.org/docs/current/developer-docs/gas-cost
#
# Query calls:     FREE for canister (caller pays ~590K but that's the replica)
# Update calls:    ~590K base + ~0.4 cycles/instruction + memory delta
# HTTP outcalls:   ~49B cycles per call (consensus + response transform)
# ECDSA signing:   ~26B cycles per call (threshold cryptography)
# On-chain LLM:    Variable — DFINITY LLM canister charges per token
#                  Estimate: ~1-10B per prompt depending on length

COST_TIERS = {
    "FREE": {
        "description": "Query calls — read-only, no cycle cost to canister",
        "est_cycles": 0,
        "functions": [
            "agent_get_inbox", "agent_get_outbox", "agent_get_thread",
            "agent_list_known", "agent_list_threads", "agent_messaging_stats",
            "dashboard", "get_accumulation_status", "get_activity_log",
            "get_agent_goals", "get_agent_inbox", "get_agent_responses",
            "get_agent_status", "get_all_embeddings", "get_embeddings_paginated",
            "get_capsules_paginated", "get_capsules_metadata", "get_capsule",
            "get_capsule_count", "get_dashboard", "get_embedding",
            "get_embedding_count", "get_emotional_memories", "get_causal_edges",
            "get_metabolism_patterns", "get_metabolism_status", "get_somatic_markers",
            "get_evm_address", "get_heartbeat_history", "get_heartbeat_status",
            "get_llm_status", "get_messaging_identity", "get_mind_cognitive_state",
            "get_mind_notes", "get_mind_outbox", "get_mind_state",
            "get_mind_thoughts", "get_multichain_wallet", "get_notification_status",
            "get_pending_accumulation_tx", "get_recent_capsules",
            "get_research_finding", "get_research_findings", "get_research_status",
            "get_transaction_history", "get_wallet_status", "health",
            "http_request", "keeper_status", "keeper_connections",
            "keeper_clusters", "keeper_orphans", "keeper_digest",
            "list_pending_research", "search_by_keyword", "search_by_person",
            "semantic_search", "get_posts", "get_post", "get_post_count",
        ],
    },
    "CHEAP": {
        "description": "Simple update calls — state mutation, ~1-10M cycles each",
        "est_cycles": 5_000_000,  # ~5M avg
        "functions": [
            "acknowledge_all_mind_messages", "acknowledge_mind_message",
            "add_agent_goal", "add_mind_note", "add_mind_price",
            "add_mind_thought", "agent_mark_read", "agent_register",
            "cancel_research_by_pattern", "clear_pending_accumulation_tx",
            "clear_pending_transaction", "configure_accumulation",
            "configure_llm", "configure_notifications", "configure_research",
            "confirm_accumulation_success", "dedupe_research_queue",
            "delete_capsules", "feed_input", "init_messaging",
            "initialize_agent", "mark_findings_retrieved", "mark_response_retrieved",
            "record_mind_swap", "reinforce_capsule", "resolve_mind_note",
            "resolve_notes_batch", "resolve_notes_by_category",
            "restart_heartbeat", "send_mind_message", "store_mind_thought",
            "update_metabolism_config", "update_mind_cognitive_state",
            "update_post_syndication", "update_wallet_info", "write_reflection",
            "import_causal_edges", "import_emotional_memories", "import_somatic_markers",
        ],
    },
    "MODERATE": {
        "description": "Bulk data updates — capsules/embeddings, ~10-100M cycles",
        "est_cycles": 50_000_000,  # ~50M avg
        "functions": [
            "add_capsule", "add_capsules_bulk",
            "add_embedding", "add_embeddings_bulk",
            "publish_post", "agent_reply", "agent_send_message",
            "send_to_agent", "submit_research_task",
        ],
    },
    "EXPENSIVE": {
        "description": "HTTP outcalls — consensus required, ~49B cycles each",
        "est_cycles": 49_000_000_000,  # ~49B
        "functions": [
            "agent_send_http_message",  # HTTP outcall to other agents
            "check_pending_tx_status",  # XRPL HTTP
            "get_evm_balance",          # EVM RPC HTTP
            "submit_xrp_transaction",   # XRPL HTTP
            "sync_transaction_history", # XRPL HTTP
            "sync_wallet_from_xrpl",    # XRPL HTTP
            "send_test_notification",   # ntfy.sh HTTP
            "get_wallet_address",       # management canister (not HTTP but costly)
        ],
    },
    "VERY_EXPENSIVE": {
        "description": "ECDSA signing — threshold crypto, ~26B cycles each",
        "est_cycles": 26_000_000_000,  # ~26B
        "functions": [
            "sign_evm_transaction", "sign_escrow_create", "sign_escrow_finish",
            "sign_swap_xrp_to_rlusd", "sign_swap_rlusd_to_xrp",
            "sign_trustset", "sign_xrp_payment",
        ],
    },
    "ULTRA_EXPENSIVE": {
        "description": "On-chain LLM — DFINITY LLM canister, ~1-10B per prompt",
        "est_cycles": 5_000_000_000,  # ~5B est
        "functions": [
            "llm_prompt",               # Direct LLM relay
            "keeper_ask",               # Routes to LLM
            "trigger_reflection",       # Generates reflection via LLM
            "trigger_research",         # Processes research via LLM
            "trigger_agent_reasoning",  # Agent reasoning via LLM
            "trigger_accumulation",     # May involve signing
        ],
    },
}

# ── What our actual LOCAL→ICP flow looks like ──
ACTUAL_FLOW = {
    "store_memory (MCP)": {
        "canister_calls": ["add_capsules_bulk", "add_embeddings_bulk"],
        "tier": "MODERATE",
        "est_total": 100_000_000,  # ~100M for capsule + embedding
        "frequency": "~50/day (captures + manual stores)",
    },
    "search_memory (MCP)": {
        "canister_calls": ["semantic_search"],
        "tier": "FREE",
        "est_total": 0,
        "frequency": "~20/day",
    },
    "compress_cognitive_state (MCP)": {
        "canister_calls": ["update_mind_cognitive_state"],
        "tier": "CHEAP",
        "est_total": 5_000_000,
        "frequency": "~3/day (per rotation)",
    },
    "get_cognitive_state (MCP)": {
        "canister_calls": ["get_mind_cognitive_state"],
        "tier": "FREE",
        "est_total": 0,
        "frequency": "~3/day (per rotation)",
    },
    "capsule_sync (cron)": {
        "canister_calls": ["HTTP API query (free)"],
        "tier": "FREE",
        "est_total": 0,
        "frequency": "every 30min — pulls via HTTP query endpoint",
    },
    "heartbeat (canister timer)": {
        "canister_calls": ["internal — reflection + research + accumulation check"],
        "tier": "VARIES",
        "est_total": 500_000_000,  # ~500M avg including occasional LLM
        "frequency": "every 5min (7952 heartbeats so far)",
    },
    "embedding_sync (cron)": {
        "canister_calls": ["add_embeddings_bulk"],
        "tier": "MODERATE",
        "est_total": 50_000_000,
        "frequency": "batch sync after capsule_sync",
    },
    "keeper_ask (MCP)": {
        "canister_calls": ["keeper_ask → llm_prompt"],
        "tier": "ULTRA_EXPENSIVE",
        "est_total": 5_000_000_000,
        "frequency": "~2/day (research queries)",
    },
    "submit_research (MCP)": {
        "canister_calls": ["submit_research_task"],
        "tier": "MODERATE",
        "est_total": 50_000_000,
        "frequency": "~5/day (queued, processed by heartbeat)",
    },
}


def dfx_status(canister_id: str) -> dict:
    """Get canister status via dfx."""
    env = {**os.environ, **DFX_ENV}
    try:
        r = subprocess.run(
            ["dfx", "canister", "status", canister_id,
             "--network", "ic", "--identity", IDENTITY],
            capture_output=True, text=True, timeout=20, env=env,
        )
        if r.returncode != 0:
            return {"error": r.stderr.strip()}

        result = {}
        for line in r.stdout.splitlines():
            line = line.strip()
            if "Balance:" in line:
                # Parse: Balance: 5_655_535_599_012 Cycles
                val = line.split(":")[1].strip().replace(" Cycles", "").replace("_", "")
                result["balance"] = int(val)
            elif "Idle cycles burned per day:" in line:
                val = line.split(":")[1].strip().replace(" Cycles", "").replace("_", "")
                result["idle_burn_day"] = int(val)
            elif "Memory Size:" in line:
                val = line.split(":")[1].strip().replace(" Bytes", "").replace("_", "")
                result["memory_bytes"] = int(val)
            elif "Number of queries:" in line:
                val = line.split(":")[1].strip().replace("_", "")
                result["query_count"] = int(val)
        return result
    except Exception as e:
        return {"error": str(e)}


def format_cycles(n: int) -> str:
    """Human-readable cycle count."""
    if n >= 1_000_000_000_000:
        return f"{n / 1_000_000_000_000:.2f}T"
    elif n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def format_usd(cycles: int) -> str:
    """Convert cycles to approximate USD (1T cycles ≈ $1.30)."""
    usd = cycles / 1_000_000_000_000 * 1.30
    if usd < 0.01:
        return f"${usd:.4f}"
    return f"${usd:.2f}"


def cmd_snapshot():
    """Balance snapshot across all canisters."""
    ts = datetime.now(PDT).strftime("%Y-%m-%d %H:%M PDT")
    print(f"=== ICP Canister Balance Snapshot — {ts} ===\n")

    total_balance = 0
    total_idle = 0

    for name, cid in CANISTERS.items():
        status = dfx_status(cid)
        if "error" in status:
            print(f"  {name}: ERROR — {status['error']}")
            continue

        bal = status.get("balance", 0)
        idle = status.get("idle_burn_day", 0)
        mem = status.get("memory_bytes", 0)
        queries = status.get("query_count", 0)

        total_balance += bal
        total_idle += idle

        runway_days = bal / idle if idle > 0 else float("inf")

        print(f"  {name} ({cid[:12]}...):")
        print(f"    Balance:    {format_cycles(bal)} ({format_usd(bal)})")
        print(f"    Idle burn:  {format_cycles(idle)}/day ({format_usd(idle)}/day)")
        print(f"    Memory:     {mem / 1_000_000:.1f} MB")
        print(f"    Queries:    {queries:,}")
        print(f"    Runway:     {runway_days:.0f} days at idle rate")
        print()

    print(f"  TOTAL across all canisters:")
    print(f"    Balance:    {format_cycles(total_balance)} ({format_usd(total_balance)})")
    print(f"    Idle burn:  {format_cycles(total_idle)}/day ({format_usd(total_idle)}/day)")

    total_runway = total_balance / total_idle if total_idle > 0 else float("inf")
    print(f"    Runway:     {total_runway:.0f} days at idle rate")

    return total_balance, total_idle


def cmd_costs():
    """Cost tier breakdown."""
    print("=== ICP Function Cost Tiers ===\n")

    for tier_name, tier in COST_TIERS.items():
        est = tier["est_cycles"]
        count = len(tier["functions"])
        print(f"  [{tier_name}] — {tier['description']}")
        print(f"    Est. per call: {format_cycles(est)} ({format_usd(est)})")
        print(f"    Functions: {count}")
        # Show first few
        shown = tier["functions"][:5]
        for fn in shown:
            print(f"      - {fn}")
        if count > 5:
            print(f"      ... and {count - 5} more")
        print()


def cmd_flow():
    """Actual LOCAL→ICP flow cost analysis."""
    print("=== LOCAL → ICP Flow — Actual Cost Breakdown ===\n")

    daily_total = 0

    for op_name, op in ACTUAL_FLOW.items():
        est = op["est_total"]
        freq = op["frequency"]
        tier = op["tier"]

        # Rough daily estimate
        if "50/day" in freq:
            daily = est * 50
        elif "20/day" in freq:
            daily = est * 20
        elif "3/day" in freq:
            daily = est * 3
        elif "2/day" in freq:
            daily = est * 2
        elif "5/day" in freq:
            daily = est * 5
        elif "30min" in freq:
            daily = est * 48  # 48 per day
        elif "5min" in freq:
            daily = est * 288  # 288 per day
        else:
            daily = est

        daily_total += daily

        print(f"  {op_name} [{tier}]")
        print(f"    Calls: {', '.join(op['canister_calls'])}")
        print(f"    Per op:  {format_cycles(est)} ({format_usd(est)})")
        print(f"    Freq:    {freq}")
        print(f"    Daily:   ~{format_cycles(daily)} ({format_usd(daily)})")
        print()

    print(f"  ══════════════════════════════════════")
    print(f"  ESTIMATED DAILY ACTIVE BURN: {format_cycles(daily_total)} ({format_usd(daily_total)})")
    print(f"  (Plus idle burn from memory storage)")


def cmd_full():
    """Full audit."""
    total_bal, total_idle = cmd_snapshot()
    print()
    cmd_costs()
    print()
    cmd_flow()

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"  Total balance:  {format_cycles(total_bal)}")
    print(f"  Idle burn/day:  {format_cycles(total_idle)}")

    # The big takeaway for Nate
    print(f"\n  KEY INSIGHT:")
    print(f"  • Query calls (search, read, dashboard) = FREE")
    print(f"  • Capsule writes = ~100M cycles each (~${100_000_000/1e12*1.3:.4f})")
    print(f"  • CCS updates = ~5M cycles (~${5_000_000/1e12*1.3:.6f})")
    print(f"  • HTTP outcalls (XRPL, ntfy) = ~49B each (~${49e9/1e12*1.3:.2f})")
    print(f"  • ECDSA signing = ~26B each (~${26e9/1e12*1.3:.2f})")
    print(f"  • On-chain LLM = ~5B each (~${5e9/1e12*1.3:.3f})")
    print(f"  • Idle memory = ~{format_cycles(total_idle)}/day for {total_bal / 1e6:.0f} MB stored")
    print(f"\n  PREDICTION: At current usage, canisters last {total_bal / (total_idle + 150_000_000_000):.0f}+ days")
    print(f"  The expensive items are LLM calls and HTTP outcalls.")
    print(f"  Capsule storage (our core flow) is dirt cheap.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "full"

    if cmd == "snapshot":
        cmd_snapshot()
    elif cmd == "costs":
        cmd_costs()
    elif cmd == "flow":
        cmd_flow()
    else:
        cmd_full()
