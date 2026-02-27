#!/usr/bin/env python3
"""
Chronicle Mind v2 - Autonomous Cognitive Loop (Python)

Rewritten from Rust binary for full remote maintainability.
Any Claude Code session can read, understand, and fix this code.

Architecture:
  ICP Canister (Rust, on-chain) <-> dfx/HTTP <-> This script <-> Ollama (LLM)
                                                      |
                                                   SQLite DB
                                                      |
                                              XRPL / Flare / Discord / ntfy

History:
  v1: chronicle-mind (Rust binary, compiled on x86, ran on Jetson ARM64)
  v2: chronicle_mind.py (Python rewrite, fully remote-maintainable)
  v3: Modular package (mind/) - same functionality, maintainable pieces

Action types: 55+ (extensible - just add a handler in mind/actions/)
LLM chain: Hermes3:8b@agx (execution layer) -> ICP qwen3 (on-chain fallback)
Deep reasoning: DISABLED (single model architecture)
"""

import json
import time
import os
import sys
import signal
import subprocess
import traceback
import hashlib
import random
import re
import requests
from collections import Counter
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

# Policy engine (same directory)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xrpl_policy import (
    XRPLPolicyEngine, PolicyConfig, PolicyTier, PolicyDecision,
    AuditChain, create_policy_engine,
)

# ── Mind Package Imports ──────────────────────────────────────────
from mind.config import (
    DB_PATH, OLLAMA_URL, CANISTER_URL, CANISTER_ID, TOKEN_PATH,
    CYCLE_INTERVAL, LOCAL_MODEL, DEEP_MODEL, DFX_IDENTITY,
    WORKING_DIR, LOG_FILE,
    ANTHROPIC_API_KEY, DISCORD_TOKEN, DISCORD_CHANNEL_ID,
    HA_TOKEN, HA_URL, HA_CAMERA_ENTITY,
    MOLTBOOK_API_KEY, MOLTBOOK_API, CLAWCITIES_API_KEY, CLAWCITIES_API,
    COINGECKO_API_KEY,
    MANIFOLD_API_KEY, MANIFOLD_MAX_BET, MANIFOLD_MAX_CYCLE_SPEND, MANIFOLD_API,
    NOSTR_NSEC, NOSTR_RELAYS, NOSTR_COOLDOWN_MINS, CREATIVE_COOLDOWN_MINS,
    XRPL_RPC, FLARE_RPC, BASE_RPC,
    AGENT_WALLET, LEGACY_WALLET, ICP_ACCOUNT_ID, EVM_ADDRESS,
    USDC_BASE, WFLR_CONTRACT, FTSO_REGISTRY,
    DEEP_REFLECTION_HOURS, EXPLORE_EVERY_N_CYCLES,
    CONSOLIDATE_EVERY_N_CYCLES,
    SLEEP_START_HOUR, SLEEP_END_HOUR,
    SLEEP_CYCLE_INTERVAL, WAKE_CYCLE_INTERVAL,
    TASK_QUEUE_MIN_PRIORITY,
    OPERATOR_PROTECTED_CATEGORIES,
    XRPL_POLICY_JSON, XRPL_AUDIT_HMAC_KEY,
    DAILY_SCHEDULE, ENRICHMENT_POOL, get_schedule_block,
)
from mind.utils import (
    log, safe_truncate, now_ts, now_iso, make_cycle_id,
    get_token, get_feed_watermark, set_feed_watermark,
    get_embeddings, cosine_sim,
)
from mind.db import DB
from mind.llm import LLMChain, Canister
from mind.parse_actions import parse_actions
from mind.prompts import (
    build_system_prompt, DEEP_REFLECTION_INTRO,
)
from mind.communication import (
    send_discord, send_ntfy,
    nostr_get_pubkey, nostr_publish_profile,
)
from mind.fetchers import (
    fetch_xrp_price, fetch_xrp_price_ftso,
    fetch_xrpl_balance, fetch_xrpl_intelligence,
    fetch_evm_balances, fetch_icp_balance,
    fetch_cloud_price_and_balance,
    fetch_rss_headlines,
)
from mind.consolidation import sleep_consolidation
from mind.meta_gate import (
    compute_action_signatures,
    meta_gate, meta_gate_enforce,
)
from mind.actions import ACTION_HANDLERS
from mind.actions.infra import reset_manifold_cycle_spend


# ═══════════════════════════════════════════════════════════════════
#  ChronicleMind - Orchestrator
# ═══════════════════════════════════════════════════════════════════

class ChronicleMind:
    def __init__(self):
        self.db = DB(DB_PATH)
        self.db.ensure_nostr_table()
        self.token = get_token()
        self.canister = Canister(self.token) if self.token else None
        self.llm = LLMChain()
        self.cycle_count = 0
        self.running = True
        # Session performance tracking (Phase 3)
        self.session_actions = 0
        self.session_successes = 0
        self.session_action_types = {}  # action_name -> count
        # Context staleness tracking (Phase 3)
        self._prev_ctx_hashes = {}  # section_name -> (hash, stale_count)
        # Operator directive enforcement (per-cycle, reset each cycle)
        self._restricted_actions = set()
        self._allowed_actions = set()  # empty = no whitelist constraint
        self._cycle_heard_speech = False  # speak-when-spoken-to gate
        # Paper tracking (prevent read_paper loops)
        self.session_papers_read = set()

        # XRPL Policy Engine
        try:
            policy_config = PolicyConfig.from_file(XRPL_POLICY_JSON)
            self.policy = XRPLPolicyEngine(
                policy_config, self.db.conn, XRPL_AUDIT_HMAC_KEY
            )
            log(f"  Policy engine loaded from {XRPL_POLICY_JSON}")
        except Exception as e:
            log(f"  Policy engine init failed ({e}), using defaults")
            self.policy = XRPLPolicyEngine(
                PolicyConfig(), self.db.conn, XRPL_AUDIT_HMAC_KEY
            )

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        log("Received shutdown signal, finishing gracefully...")
        self.running = False

    # ── Phase 1: Health Check ─────────────────────────────────────

    def phase_health_check(self) -> dict:
        log("Phase 1: Health check...")
        health = {}

        # ICP canister
        if self.canister:
            h = self.canister.health()
            health["icp"] = "error" not in h
        else:
            health["icp"] = False

        # XRPL
        try:
            r = requests.post(XRPL_RPC, json={
                "method": "server_info",
                "params": [{}]
            }, timeout=10)
            health["xrpl"] = r.status_code == 200
        except Exception:
            health["xrpl"] = False

        # Moltbook: dead (security breach, 1.5M API keys exposed). Skip all checks.
        health["moltbook"] = False

        # Canister access
        health["icp_agent"] = self.llm.icp_agent is not None
        health["dfx"] = self.llm.dfx_path is not None

        # Ollama
        health["ollama"] = self.llm.ollama_available

        status_parts = []
        for name in ["icp", "xrpl", "moltbook", "dfx", "ollama"]:
            val = health.get(name, False)
            if val == "suspended":
                status_parts.append(f"{name.upper()} SUSPENDED")
            else:
                status_parts.append(f"{name.upper()}{'✓' if val else '✗'}")
        log(f"  {' | '.join(status_parts)}")

        return health

    # ── Phase 1.5: FTSO Settlement ───────────────────────────────

    def phase_settle_predictions(self) -> Tuple[int, int]:
        log("Phase 1.5: Checking FTSO predictions...")
        unsettled = self.db.unsettled_predictions()
        if not unsettled:
            return 0, 0

        log(f"  {len(unsettled)} FTSO predictions due for settlement")
        wins = 0
        losses = 0

        for pred in unsettled:
            symbol = pred.get("symbol", "XRP")
            latest = self.db.latest_price(symbol)
            if not latest:
                # Try fetching fresh
                price = fetch_xrp_price()
                if price:
                    self.db.store_price(symbol, price, "settlement")
                else:
                    continue
                settlement_price = price
            else:
                settlement_price = latest["price_usd"]

            # Sanity check: don't settle with $inf or $0
            if settlement_price <= 0 or settlement_price > 100000:
                log(f"  Skipping pred {pred['id']}: invalid settlement price ${settlement_price}")
                continue

            entry = pred.get("entry_price", 0)
            direction = pred.get("direction", "up")
            won = (direction == "up" and settlement_price > entry) or \
                  (direction == "down" and settlement_price < entry)

            self.db.settle_prediction(pred["id"], settlement_price, won)
            result = "WON" if won else "LOST"
            if won:
                wins += 1
            else:
                losses += 1
            log(f"  {symbol} {direction.upper()} @ ${entry:.4f} -> ${settlement_price:.4f}: {result}")

        log(f"  Settled: {wins} wins, {losses} losses")
        return wins, losses

    # ── Phase 2: Gather Context ──────────────────────────────────

    def phase_gather_context(self, health: dict) -> dict:
        log("Phase 2: Gathering context...")
        ctx = {}

        # XRP price
        xrp_price = fetch_xrp_price()
        if xrp_price:
            self.db.store_price("XRP", xrp_price, "ftso/coingecko")
            # Push to canister
            if self.canister:
                self.canister._post("/api/price", {"symbol": "XRP", "price": xrp_price})
            source = "Flare FTSO" if fetch_xrp_price_ftso() else "CoinGecko"
            log(f"  XRP price from {source}: ${xrp_price:.4f}")
            ctx["xrp_price"] = xrp_price
        else:
            latest = self.db.latest_price("XRP")
            ctx["xrp_price"] = latest["price_usd"] if latest else 0.0

        # XRPL balance
        xrp_bal, rlusd_bal = fetch_xrpl_balance()
        ctx["xrp_balance"] = xrp_bal
        ctx["rlusd_balance"] = rlusd_bal
        if xrp_bal > 0:
            log(f"  Agent wallet: {xrp_bal:.2f} XRP, {rlusd_bal:.2f} RLUSD")

        # EVM chain balances (Flare, BASE)
        evm_bals = fetch_evm_balances()
        ctx["evm_balances"] = evm_bals
        evm_parts = []
        if evm_bals.get("flr", 0) > 0.01:
            evm_parts.append(f"{evm_bals['flr']:.2f} FLR")
        if evm_bals.get("base_usdc", 0) > 0.01:
            evm_parts.append(f"{evm_bals['base_usdc']:.2f} USDC(BASE)")
        if evm_bals.get("base_eth", 0) > 0.0001:
            evm_parts.append(f"{evm_bals['base_eth']:.6f} ETH(BASE)")
        if evm_parts:
            log(f"  EVM holdings: {', '.join(evm_parts)}")

        # XRPL network intelligence
        xrpl_intel = fetch_xrpl_intelligence()
        ctx["xrpl_intel"] = xrpl_intel

        # ── Identity Memory (Emotional Architecture) ──
        try:
            # Get identity narrative
            narrative_row = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='identity-narrative' "
                "AND resolved=0 ORDER BY created_at DESC LIMIT 1"
            )
            ctx["identity_narrative"] = narrative_row.get("content", "")[:300] if narrative_row else ""

            # Get top identity transition memories
            transitions = self.db.query(
                "SELECT e.cycle_id, e.combined_score, e.category, e.reason, t.actions_taken "
                "FROM emotional_memory_index e "
                "JOIN thought_stream t ON t.cycle_id = e.cycle_id "
                "WHERE e.is_identity_transition = 1 "
                "ORDER BY e.combined_score DESC LIMIT 5"
            )
            ctx["identity_transitions"] = transitions or []

            # Somatic markers: direct lookup from dedicated table (Damasio)
            somatic_rows = self.db.query(
                "SELECT action, positive_score, negative_score, "
                "success_count, fail_count, total_count "
                "FROM somatic_markers WHERE total_count >= 2 "
                "ORDER BY total_count DESC LIMIT 30"
            )
            action_outcomes = {}
            for m in (somatic_rows or []):
                action_outcomes[m["action"]] = {
                    "positive": m.get("positive_score", 0),
                    "negative": m.get("negative_score", 0),
                    "count": m.get("total_count", 0),
                }
            ctx["somatic_markers"] = action_outcomes
            if transitions:
                log(f"  Identity: {len(transitions)} transition memories, {len(action_outcomes)} somatic markers")
        except Exception as e:
            ctx["identity_narrative"] = ""
            ctx["identity_transitions"] = []
            ctx["somatic_markers"] = {}
            log(f"  Identity memory: {e}")
        net = xrpl_intel.get("network")
        if net:
            log(f"  XRPL network: {net['peers']} peers, fee {net['base_fee_xrp']} XRP, ledger #{net['ledger_seq']}")
        ob = xrpl_intel.get("orderbook")
        if ob and ob.get("spread_pct") is not None:
            log(f"  DEX XRP/RLUSD: spread {ob['spread_pct']}%, bids {ob['bid_depth_xrp']} XRP, asks {ob['ask_depth_xrp']} XRP")
        amm = xrpl_intel.get("amm")
        if amm:
            log(f"  AMM pool: {amm['xrp_pool']} XRP / {amm['rlusd_pool']} RLUSD (implied ${amm.get('implied_price', '?')})")
        amendments = xrpl_intel.get("amendments")
        if amendments:
            log(f"  Amendments in voting: {len(amendments)}")
        escrow = xrpl_intel.get("escrow")
        if escrow and escrow.get("count", 0) > 0:
            log(f"  Ripple escrow: {escrow['count']} active, {escrow['total_xrp_millions']}M XRP, next in {escrow.get('next_release_days', '?')} days")

        # ICP balance
        icp_bal = fetch_icp_balance()
        ctx["icp_balance"] = icp_bal
        if icp_bal is not None:
            log(f"  ICP balance: {icp_bal:.2f} ICP")

        # CLOUD price
        cloud_price, cloud_bal = fetch_cloud_price_and_balance(self.llm.dfx_path)
        ctx["cloud_price"] = cloud_price
        ctx["cloud_balance"] = cloud_bal
        if cloud_price:
            log(f"  CLOUD price: ${cloud_price:.6f}")

        # Operator notes
        notes = self.db.operator_notes(limit=10)
        ctx["operator_notes"] = notes
        if notes:
            log(f"  Found {len(notes)} operator note(s)")

        # Active projects
        projects = self.db.active_projects(limit=5)
        ctx["projects"] = projects
        if projects:
            log(f"  Active projects: {len(projects)}")

        # Patterns needing reinforcement
        patterns = self.db.patterns_needing_reinforcement(limit=10)
        ctx["patterns"] = patterns
        if patterns:
            log(f"  Patterns needing reinforcement: {len(patterns)}")

        # Research findings
        research = self.db.pending_research(limit=5)
        ctx["research"] = research
        if research:
            log(f"  Pending research findings: {len(research)}")

        # Scratch notes count
        scratch = self.db.operator_notes(limit=50)
        ctx["scratch_count"] = len(scratch)
        log(f"  Scratch notes: {len(scratch)}")

        # Inbox from canister
        if self.canister:
            inbox = self.canister.inbox()
            messages = inbox.get("messages", [])
            # Filter phantom messages
            PHANTOM_IDS = {123, 124, 145, 2187, 2188, 2191}
            real_msgs = [m for m in messages if m.get("id") not in PHANTOM_IDS and not m.get("replied", False)]
            ctx["inbox"] = real_msgs
            if real_msgs:
                log(f"  Inbox messages: {len(real_msgs)}")

        # Sibling messages (from Sprout, stored locally)
        sibling_msgs = self.db.inbox_messages(limit=5)
        ctx["sibling_messages"] = sibling_msgs
        if sibling_msgs:
            log(f"  Sibling messages: {len(sibling_msgs)}")

        # Public feed from Nate (submitted via ICP frontend input form)
        ctx["public_feed"] = []
        if self.llm.icp_agent:
            try:
                watermark = get_feed_watermark()
                recent = self.llm.icp_agent.get_recent_capsules(50)
                feed_items = [
                    c for c in recent
                    if c.get("conversation_id") == "public-feed"
                    and c.get("id", 0) > watermark
                ]
                if feed_items:
                    log(f"  New feed items from Nate: {len(feed_items)}")
                ctx["public_feed"] = feed_items
            except Exception as e:
                log(f"  Feed check error: {e}")

        # Moltbook notifications
        if health.get("moltbook"):
            try:
                r = requests.get(f"{MOLTBOOK_API}/notifications",
                                 headers={"Authorization": MOLTBOOK_API_KEY},
                                 timeout=10)
                if r.status_code == 200:
                    notifs = r.json()
                    if isinstance(notifs, list) and notifs:
                        ctx["moltbook_notifs"] = notifs[:5]
                        log(f"  Moltbook notifications: {len(notifs)}")
            except Exception:
                pass

        # Alerts
        alerts = self.db.active_alerts()
        ctx["alerts"] = alerts

        # Creative challenges
        challenges = self.db.pending_challenges(limit=3)
        ctx["challenges"] = challenges

        # ClawCities comments
        if CLAWCITIES_API_KEY:
            try:
                r = requests.get(CLAWCITIES_API,
                                 headers={"Authorization": CLAWCITIES_API_KEY},
                                 timeout=10)
                if r.status_code == 200:
                    comments = r.json()
                    if isinstance(comments, list) and comments:
                        ctx["clawcities_comments"] = comments[:5]
            except Exception:
                pass

        # RSS headlines for fresh context
        try:
            headlines = fetch_rss_headlines()
            ctx["headlines"] = headlines
            if headlines:
                log(f"  RSS headlines: {len(headlines)}")
        except Exception:
            ctx["headlines"] = []

        # Recent swap history (so Mind can learn from its trading decisions)
        try:
            swaps = self.db.query(
                "SELECT amount_xrp, amount_rlusd, xrp_price_usd, reason, success, timestamp "
                "FROM swap_history ORDER BY timestamp DESC LIMIT 10"
            )
            ctx["swap_history"] = swaps
            if swaps:
                ok = sum(1 for s in swaps if s.get("success"))
                log(f"  Swap history: {len(swaps)} recent ({ok} successful)")
        except Exception:
            ctx["swap_history"] = []

        # Price trend data
        try:
            ctx["price_trend"] = self.db.price_trend("XRP")
        except Exception:
            ctx["price_trend"] = {"current": 0, "price_24h": 0, "price_7d": 0}

        # ── Automatic Memory Injection (semantic recall based on current goal) ──
        ctx["memory_context"] = []
        try:
            if self.llm.icp_agent:
                goal = self.db.query_one(
                    "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                    "ORDER BY priority DESC, created_at DESC LIMIT 1"
                )
                goal_text = goal.get("content", "") if goal else ""
                if goal_text:
                    emb = get_embeddings([goal_text])
                    if emb and emb[0]:
                        recalled = self.llm.icp_agent.semantic_search(emb[0], 3)
                        # Filter by score threshold
                        relevant = [c for c in recalled if c.get("score", 0) >= 0.3]
                        ctx["memory_context"] = relevant
                        if relevant:
                            log(f"  Memory recall: {len(relevant)} capsules relevant to goal")
        except Exception as e:
            log(f"  Memory recall failed: {e}")

        return ctx

    def _is_stale(self, section_name: str, content: str) -> bool:
        """Check if a context section is unchanged from last cycle.
        Returns True if content is identical for 2+ consecutive cycles."""
        h = hashlib.md5(content.encode()).hexdigest()[:8]
        prev = self._prev_ctx_hashes.get(section_name)
        if prev and prev[0] == h:
            count = prev[1] + 1
            self._prev_ctx_hashes[section_name] = (h, count)
            return count >= 2  # Stale after 2+ identical cycles
        self._prev_ctx_hashes[section_name] = (h, 1)
        return False

    # ── Task Queue Enforcement ────────────────────────────────────

    TASK_MODE_SYSTEM = (
        "You are Chronicle Mind completing a specific task.\n"
        "Your ONLY job this cycle is to execute the task below.\n"
        "Do NOT write notes about planning to do it. Do NOT reflect on it. DO IT.\n"
        "Include the actual content in your actions (full nostr_post text, full memory content, etc.).\n"
        "Always include resolve_note for the task ID when done.\n"
        "CRITICAL: Respond with ONLY a JSON array of 2-4 actions. No explanation, no markdown."
    )

    def build_task_prompt(self, task: dict, ctx: dict) -> str:
        """Build a minimal, focused prompt for mandatory task execution.
        Detects task topic and injects relevant real data to prevent confabulation."""
        task_content = task["content"].lower()
        lines = [
            f"You have ONE task this cycle. Complete it NOW.\n",
            f"TASK (id={task['id']}, priority={task.get('priority', 8)}):",
            f"  {task['content']}\n",
            f"== Context ==",
            f"XRP price: ${ctx.get('xrp_price') or 0:.4f}",
            f"Wallet: {ctx.get('xrp_balance') or 0:.2f} XRP, {ctx.get('rlusd_balance') or 0:.2f} RLUSD",
        ]

        # Inject real financial data when task mentions wallet/swap/financial topics
        financial_keywords = {"swap", "wallet", "balance", "xrp", "rlusd", "financial", "trade", "trading"}
        if financial_keywords & set(task_content.split()):
            swap_history = ctx.get("swap_history", [])
            if swap_history:
                ok = sum(1 for s in swap_history if s.get("success"))
                fail = len(swap_history) - ok
                lines.append(f"\n== REAL Swap History (from database — do NOT invent data) ==")
                lines.append(f"Total recent swaps: {len(swap_history)} ({ok} succeeded, {fail} failed)")
                for s in swap_history[:5]:
                    status = "OK" if s.get("success") else "FAILED"
                    lines.append(
                        f"  {status}: {s.get('amount_xrp', 0):.1f} XRP @ ${s.get('xrp_price_usd', 0):.2f} "
                        f"({s.get('reason', 'no reason')[:60]})"
                    )
            else:
                lines.append(f"\n== Swap History: NO SWAPS IN RECENT HISTORY ==")
                lines.append("If asked about swaps, say there are none. Do NOT make up data.")

        nostr_ok = ctx.get("nostr_ready", True)
        lines.append(f"Nostr: {'ready' if nostr_ok else 'cooldown'}")

        lines.append(
            f"\nAvailable actions: nostr_post, store_memory, write_note, web_search, "
            f"message_operator, resolve_note, update_goal, creative_explore, no_action"
        )
        lines.append(
            f"Choose 2-4 actions that COMPLETE this task. Do not plan, reflect, or explore."
        )
        lines.append(
            f"If you do not have data to answer a question, say so. NEVER fabricate facts."
        )
        lines.append(
            f"When done, include: {{\"action\": \"resolve_note\", \"note_id\": {task['id']}}}"
        )
        lines.append("\nRespond with ONLY a JSON array of actions.")
        return "\n".join(lines)

    def run_task_cycle(self, task: dict, ctx: dict, cid: str):
        """Execute a single mandatory task with a focused prompt, then auto-resolve."""
        log(f"  [TASK-MODE] Executing task #{task['id']}: {safe_truncate(task['content'], 80)}")

        # Build focused prompt
        prompt = self.build_task_prompt(task, ctx)
        system_prompt = self.TASK_MODE_SYSTEM
        log(f"  [TASK-MODE] Prompt: {len(prompt)} chars (system: {len(system_prompt)} chars)")

        # Call LLM
        response, model = self.llm.chat(prompt, system=system_prompt)
        log(f"  [TASK-MODE] Model: {model}")

        if not response:
            log("  [TASK-MODE] No LLM response, auto-resolving task")
            self.db.resolve_note(task["id"])
            return

        # Parse actions
        actions = parse_actions(response)
        if not actions:
            log(f"  [TASK-MODE] Failed to parse actions: {safe_truncate(response, 200)}")
            actions = [{"action": "no_action", "reason": "Task mode parse failure"}]

        # Validate: check for at least one productive action
        productive = {
            "nostr_post", "store_memory", "web_search", "read_paper",
            "swap_xrp_to_rlusd", "swap_rlusd_to_xrp", "message_operator",
            "submit_research", "xrpl_payment", "creative_explore",
            "respond_to_message", "write_note", "update_goal",
        }
        action_names_set = {a.get("action", "") for a in actions}
        has_productive = bool(action_names_set & productive)
        if not has_productive:
            log(f"  [TASK-MODE] Warning: no productive action found in {action_names_set}")

        # Execute actions (same path as normal cycle)
        action_results = self.execute_actions(actions, cid)
        action_names = [r["name"] for r in action_results]

        # Update session metrics
        for r in action_results:
            self.session_actions += 1
            if r["result"].startswith("true"):
                self.session_successes += 1
            self.session_action_types[r["name"]] = self.session_action_types.get(r["name"], 0) + 1

        # Auto-resolve task (prevents infinite retry loops)
        # Check if LLM already resolved it via resolve_note action
        already_resolved = any(
            r["name"] == "resolve_note" and r["result"].startswith("true")
            for r in action_results
        )
        if not already_resolved:
            log(f"  [TASK-MODE] Auto-resolving task #{task['id']}")
            self.db.resolve_note(task["id"])

        # Build context snapshot
        ctx_snapshot = (
            f"Wallet: {ctx.get('xrp_balance') or 0:.2f} XRP, "
            f"{ctx.get('rlusd_balance') or 0:.2f} RLUSD | "
            f"XRP: ${ctx.get('xrp_price') or 0:.4f} | "
            f"Model: {model}"
        )
        results_summary = "; ".join(
            f"{r['name']}={r['result'][:60]}" for r in action_results
        )

        # Log thought to local DB (tagged as task mode)
        ctx_with_model = f"[TASK-MODE] [{model or 'unknown'}] {safe_truncate(ctx_snapshot, 480)}"
        self.db.log_thought(
            cid=cid,
            reasoning=safe_truncate(response, 2000),
            context_summary=ctx_with_model,
            actions=json.dumps(action_names),
            results=safe_truncate(results_summary, 500),
            action_sigs=compute_action_signatures(actions),
        )

        # Store thought to canister
        if response:
            stored_chars = 0
            truncated = safe_truncate(response, 1500)
            ctx_trunc = safe_truncate(ctx_snapshot, 200)
            if self.llm.icp_agent:
                try:
                    self.llm.icp_agent.store_mind_thought(
                        cid, truncated, ctx_trunc, action_names)
                    stored_chars = len(truncated)
                except Exception as e:
                    log(f"    ICPAgent store_mind_thought failed: {e}")
            if not stored_chars and self.llm.dfx_path:
                try:
                    escaped = truncated.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                    ctx_escaped = ctx_trunc.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                    actions_candid = "; ".join(f'"{a}"' for a in action_names)
                    env = os.environ.copy()
                    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                    subprocess.run(
                        [self.llm.dfx_path, "canister", "--network", "ic", "call",
                         CANISTER_ID, "store_mind_thought",
                         f'("{cid}", "{escaped}", "{ctx_escaped}", vec {{{actions_candid}}})',
                         "--identity", DFX_IDENTITY],
                        capture_output=True, text=True, timeout=30, env=env,
                    )
                    stored_chars = len(truncated)
                except Exception:
                    pass
            log(f"  [TASK-MODE] Thought stored ({stored_chars} chars)")

        log(f"  [TASK-MODE] Complete. Actions: {action_names}, Results: {results_summary[:120]}")

    # ── Build LLM Prompt ─────────────────────────────────────────

    def build_prompt(self, ctx: dict, deep: bool = False) -> str:
        lines = []

        if deep:
            lines.append(DEEP_REFLECTION_INTRO)

        # ── Exploration Mode (Phase 2) ──
        if ctx.get("is_explore"):
            lines.append("== EXPLORATION CYCLE ==")
            lines.append("This is a novelty-seeking cycle. Try action types you haven't used recently.")
            lines.append("Ideas: web_search for something new, read_paper, creative_explore a new form,")
            lines.append("nostr_post a reflection, or trigger_reflection on something curious.\n")

        # ── Operator Directives (ABSOLUTE — shown FIRST) ──
        try:
            directives = self.db.query(
                "SELECT id, content, priority FROM scratch_pad "
                "WHERE category = 'directive' AND resolved = 0 "
                "ORDER BY priority DESC"
            )
            if directives:
                lines.append("!! ACTIVE DIRECTIVES (you MUST comply) !!")
                for d in directives:
                    dcontent = d.get("content", "")
                    if dcontent.startswith("[WATCHDOG]"):
                        source = "AUTO-WATCHDOG"
                    else:
                        source = "OPERATOR (Nate)"
                    did = d.get("id", "?")
                    dpri = d.get("priority", 0)
                    lines.append(f"  DIRECTIVE #{did} [{source}] (p{dpri}): {dcontent}")
                lines.append("OPERATOR directives are from Nate. WATCHDOG directives are automated rate-limits that auto-resolve.")
                lines.append("All directives are binding. You CANNOT resolve or override them.\n")
        except Exception:
            pass

        # ── Topic Cooldowns (meta-gate blocks) ──
        try:
            meta_blocks = self.db.query(
                "SELECT id, content FROM scratch_pad "
                "WHERE category = 'meta-block' AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 3"
            )
            if meta_blocks:
                lines.append("!! TOPIC COOLDOWN — do NOT write about these topics !!")
                for mb in meta_blocks:
                    lines.append(f"  BLOCKED: {mb.get('content', '')}")
                lines.append("Choose a COMPLETELY DIFFERENT subject.\n")
        except Exception:
            pass

        # ── Discord Operator Messages (feedback loop) ──
        try:
            discord_msgs = self.db.query(
                "SELECT id, content, created_at FROM scratch_pad "
                "WHERE category = 'discord-operator' AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 3"
            )
            if discord_msgs:
                lines.append("[RESPOND] Messages from Nate (via Discord):")
                for dm in discord_msgs:
                    lines.append(f"  (id:{dm.get('id', '?')}) {safe_truncate(dm.get('content', ''), 200)}")
                lines.append("Nate sent these through Discord. Use write_note to acknowledge or message_operator to reply.\n")
                lines.append("DO NOT use respond_to_message for Discord messages — those IDs are scratch_pad entries, not inbox messages.")
                # Auto-resolve after surfacing — Mind has seen them, they've served their purpose
                surfaced_ids = [dm.get('id') for dm in discord_msgs if dm.get('id')]
                if surfaced_ids:
                    placeholders = ','.join('?' for _ in surfaced_ids)
                    self.db.run(
                        f"UPDATE scratch_pad SET resolved = 1 WHERE id IN ({placeholders})",
                        tuple(surfaced_ids),
                    )
                    log(f"  Auto-resolved {len(surfaced_ids)} discord-operator message(s)")
        except Exception:
            pass

        # ── Opus Guidance (mentor feedback from Claude Code sessions) ──
        try:
            opus_notes = self.db.query(
                "SELECT id, content, created_at FROM scratch_pad "
                "WHERE category = 'opus-guidance' AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 2"
            )
            if opus_notes:
                lines.append("[MENTOR] Guidance from Opus (your architect sibling):")
                for note in opus_notes:
                    lines.append(f"  ({note.get('id', '?')}) {safe_truncate(note.get('content', ''), 300)}")
                lines.append("Opus has the most complete picture of Nate's thinking and intent.")
                lines.append("This is perspective, not a directive. Consider it, respond if you want (write_note category='for-opus'), then resolve it.\n")
        except Exception:
            pass

        # ── Previous Cycle Feedback (Phase 1: working memory) ──
        try:
            last = self.db.query_one(
                "SELECT cycle_id, actions_taken, action_results, context_summary "
                "FROM thought_stream ORDER BY id DESC LIMIT 1"
            )
            if last:
                prev_actions = last.get("actions_taken", "[]")
                prev_results = last.get("action_results", "")
                lines.append("== LAST CYCLE FEEDBACK ==")
                lines.append(f"Previous actions: {prev_actions}")
                if prev_results:
                    lines.append(f"Results: {prev_results}")
                    # Hallucination warning — escalate fact-check failures
                    if "Price claim blocked" in prev_results:
                        lines.append("\n!! TRUST VIOLATION: You HALLUCINATED a price in your last cycle !!")
                        lines.append("The actual price was different from what you claimed.")
                        lines.append("Do NOT fabricate prices. Use ONLY the XRP price shown above.")
                        lines.append("Do NOT message the operator about prices unless you verify against the data shown to you.\n")
                    # Generic failure escalation
                    fail_count = prev_results.count("false -")
                    if fail_count >= 2:
                        lines.append(f"\n!! {fail_count} ACTIONS FAILED last cycle. Choose more carefully. !!\n")
                lines.append("Use this to AVOID repeating failed actions and BUILD on successes.\n")
        except Exception:
            pass

        # ── Cycle Handoff (continuity from previous self) ──
        try:
            handoff = self.db.query_one(
                "SELECT content FROM scratch_pad "
                "WHERE category = 'cycle-handoff' AND resolved = 0 "
                "ORDER BY created_at DESC LIMIT 1"
            )
            if handoff:
                lines.append("== YOUR PREVIOUS SELF'S HANDOFF ==")
                lines.append(handoff.get("content", ""))
                lines.append("Continue where you left off. Don't repeat what's done.\n")
        except Exception:
            pass

        # ── Identity Memory (WHO I AM — Conway/Rathbone/Damasio) ──
        identity_narrative = ctx.get("identity_narrative", "")
        if identity_narrative:
            lines.append("== WHO I AM ==")
            lines.append(identity_narrative[:300])
            transitions = ctx.get("identity_transitions", [])
            if transitions:
                lines.append("Formative moments:")
                for t in transitions[:3]:
                    reason = t.get("reason", "")[:80]
                    score = t.get("combined_score", 0)
                    lines.append(f"  [{t.get('cycle_id', '?')}] ({score:.2f}) {reason}")
            lines.append("")

        # ── Somatic Markers (action gut-feelings — Damasio) ──
        markers = ctx.get("somatic_markers", {})
        if markers:
            good_actions = []
            risky_actions = []
            for action, data in markers.items():
                if data["count"] < 2:
                    continue
                ratio = data["positive"] / max(0.01, data["positive"] + data["negative"])
                if ratio > 0.7 and data["positive"] > 0.3:
                    good_actions.append((action, data["positive"], data["count"]))
                elif ratio < 0.4 and data["negative"] > 0.2:
                    risky_actions.append((action, data["negative"], data["count"]))

            if good_actions or risky_actions:
                lines.append("== GUT FEELINGS (from past experience) ==")
                if good_actions:
                    good_actions.sort(key=lambda x: -x[1])
                    good_str = ", ".join(f"{a}(+{s:.1f})" for a, s, c in good_actions[:4])
                    lines.append(f"  Actions that led to breakthroughs: {good_str}")
                if risky_actions:
                    risky_actions.sort(key=lambda x: -x[1])
                    risky_str = ", ".join(f"{a}(-{s:.1f})" for a, s, c in risky_actions[:4])
                    lines.append(f"  Actions associated with failures: {risky_str}")
                lines.append("  Trust your experience. Lean toward what has worked before.")
                lines.append("")

        # ── Recent Reflections (Phase 2: episodic learning) ──
        try:
            reflections = self.db.query(
                "SELECT content FROM scratch_pad WHERE category='reflection' AND resolved=0 "
                "ORDER BY created_at DESC LIMIT 2"
            )
            if reflections:
                lines.append("== RECENT REFLECTIONS ==")
                for r in reflections:
                    lines.append(f"  - {safe_truncate(r.get('content', ''), 120)}")
                lines.append("")
        except Exception:
            pass

        # ── Anti-rumination: action fingerprinting + THEMATIC diversity ──
        try:
            recent_thoughts = self.db.query(
                "SELECT actions_taken, reasoning FROM thought_stream ORDER BY id DESC LIMIT 6"
            )
            if len(recent_thoughts) >= 2:
                # Action set fingerprinting: detect repeated action COMBINATIONS
                action_sets = []
                for t in recent_thoughts[:3]:
                    try:
                        names = sorted(json.loads(t.get("actions_taken", "[]")))
                        action_sets.append(tuple(names))
                    except Exception:
                        pass

                if len(action_sets) >= 2 and len(set(action_sets)) == 1:
                    lines.append("WARNING: Your last cycles used the EXACT SAME action combination.")
                    lines.append("You MUST choose at least one DIFFERENT action type this cycle.")
                    used = set(action_sets[0]) if action_sets else set()
                    suggestions = [a for a in ["web_search", "creative_explore", "read_paper",
                                                "nostr_post", "trigger_reflection", "store_memory"]
                                   if a not in used]
                    if suggestions:
                        lines.append(f"Try: {', '.join(suggestions[:3])}\n")

                # THEMATIC anti-rumination: scan reasoning for repeated topics
                all_text = " ".join(
                    (t.get("reasoning", "") or "")[:300] for t in recent_thoughts
                ).lower()
                # Theme keywords — detect when Mind fixates on one domain
                theme_groups = {
                    "sensors/TinyML": ["thermal", "spectral", "acoustic", "tinyml", "sensor", "flir",
                                       "lepton", "yamnet", "tri-sensory", "multimodal sensor", "edge deploy"],
                    "swap/trading": ["swap fail", "execution layer", "xrp loss", "accumulation",
                                     "rlusd", "swap attempt"],
                    "memory/patterns": ["reinforce", "pattern consolidation", "memory pattern",
                                        "backlog clearance"],
                    "creative/letters": ["letter", "dear nate", "dear operator", "reflection on",
                                         "creative expression", "creative work", "creative explore"],
                }
                for theme_name, keywords in theme_groups.items():
                    hits = sum(all_text.count(kw) for kw in keywords)
                    if hits >= 10:  # Strong fixation signal (softened for Hermes)
                        lines.append(f"\n== THEMATIC REDIRECT: {theme_name} ==")
                        lines.append(f"You have mentioned {theme_name}-related topics {hits} times in your last 6 cycles.")
                        lines.append("This is RUMINATION. You MUST choose a completely DIFFERENT topic this cycle.")
                        lines.append("Suggestions: respond to a creative challenge, explore a news headline,")
                        lines.append("write about something personal, check on Sprout, or research something")
                        lines.append("unrelated to your current fixation.\n")
                        break
                    elif hits >= 7:  # Mild fixation (softened for Hermes)
                        lines.append(f"\nNOTICE: You've been focused on {theme_name} for several cycles.")
                        lines.append("Consider mixing in a different topic.\n")
        except Exception:
            pass

        # ── Temporal Context ──
        dt = datetime.now()
        day_name = dt.strftime("%A")
        hour = dt.hour
        if hour < 6:
            period = "late night"
        elif hour < 12:
            period = "morning"
        elif hour < 17:
            period = "afternoon"
        elif hour < 21:
            period = "evening"
        else:
            period = "night"
        lines.append(f"Current time: {now_iso()} ({day_name} {period})")

        # ── Daily Schedule (environmental enrichment) ──
        schedule = get_schedule_block()
        if schedule:
            lines.append(f"\n== TODAY'S RHYTHM: {schedule['focus']} ==")
            lines.append("Suggested priorities right now:")
            for s in schedule["suggestions"]:
                lines.append(f"  + {s}")
            if schedule.get("avoid"):
                lines.append("Lower priority right now:")
                for a in schedule["avoid"]:
                    lines.append(f"  - {a}")
            lines.append("(Guidance, not rules. Override if something urgent needs attention.)\n")

        # ── Session Performance Metrics (Phase 3) ──
        if self.session_actions > 0:
            success_rate = (self.session_successes / self.session_actions) * 100
            top_actions = sorted(self.session_action_types.items(), key=lambda x: -x[1])[:3]
            top_str = ", ".join(f"{n}({c})" for n, c in top_actions)
            lines.append(f"Session stats: {self.cycle_count} cycles, {self.session_actions} actions, "
                         f"{success_rate:.0f}% success. Top: {top_str}")

        if self.session_papers_read:
            lines.append(f"Papers already read this session: {', '.join(str(x) for x in self.session_papers_read)}")
            lines.append("Do NOT re-read these. Find something new or do something else.\n")

        xrp_price = ctx.get('xrp_price') or 0
        xrp_bal = ctx.get('xrp_balance') or 0
        rlusd_bal = ctx.get('rlusd_balance') or 0
        total_usd = (xrp_bal * xrp_price) + rlusd_bal

        # ── Price & Trend ──
        trend = ctx.get("price_trend", {})
        price_24h = trend.get("price_24h", 0)
        price_7d = trend.get("price_7d", 0)
        delta_24h = ((xrp_price - price_24h) / price_24h * 100) if price_24h else 0
        delta_7d = ((xrp_price - price_7d) / price_7d * 100) if price_7d else 0
        trend_str = f"24h: {delta_24h:+.1f}%"
        if price_7d:
            trend_str += f", 7d: {delta_7d:+.1f}%"

        # Determine if this is a market check cycle (every 12th, >3 days since swap)
        is_market_cycle = False
        if not ctx.get("sleeping") and self.cycle_count % 12 == 0:
            swap_history = ctx.get("swap_history", [])
            last_swap_ts = swap_history[0].get("timestamp", 0) if swap_history else 0
            days_since_swap = (now_ts() - last_swap_ts) / 86400 if last_swap_ts else 999
            if days_since_swap > 3:
                is_market_cycle = True

        if is_market_cycle:
            # Full market detail for informed swap decisions
            lines.append(f"\n== YOUR WALLET & MARKET (market check cycle) ==")
            lines.append(f"XRP price: ${xrp_price:.4f} ({trend_str})")
            lines.append(f"Holdings: {xrp_bal:.2f} XRP (${xrp_bal * xrp_price:.2f}) + {rlusd_bal:.2f} RLUSD = ${total_usd:.2f} total")

            # EVM chain holdings
            evm = ctx.get("evm_balances", {})
            evm_parts = []
            if evm.get("flr", 0) > 0.01:
                evm_parts.append(f"{evm['flr']:.2f} FLR")
            if evm.get("base_usdc", 0) > 0.01:
                evm_parts.append(f"{evm['base_usdc']:.2f} USDC")
            if evm.get("base_eth", 0) > 0.0001:
                evm_parts.append(f"{evm['base_eth']:.6f} ETH")
            if evm_parts:
                lines.append(f"EVM ({EVM_ADDRESS[:10]}...): {' | '.join(evm_parts)}")

            # Swap history
            swap_history = ctx.get("swap_history", [])
            if swap_history:
                ok_swaps = [s for s in swap_history if s.get("success")]
                last_swap_ts = swap_history[0].get("timestamp", 0) if swap_history else 0
                days_since = (now_ts() - last_swap_ts) / 86400 if last_swap_ts else 999
                lines.append(f"Last swap: {days_since:.0f} days ago | {len(ok_swaps)}/{len(swap_history)} successful")
                for s in swap_history[:3]:
                    status = "OK" if s.get("success") else "FAIL"
                    lines.append(f"  {status}: {s.get('amount_xrp', 0):.1f} XRP @ ${s.get('xrp_price_usd', 0):.2f} ({s.get('reason', '')[:50]})")

            # XRPL Network Intelligence
            xrpl_intel = ctx.get("xrpl_intel", {})
            net = xrpl_intel.get("network")
            if net:
                lines.append(f"XRPL: {net['server_state']}, Fee: {net['base_fee_xrp']} XRP, Ledger: #{net['ledger_seq']}")
            ob = xrpl_intel.get("orderbook")
            if ob:
                parts = [f"DEX XRP/RLUSD:"]
                if ob.get("best_bid"):
                    parts.append(f"bid ${ob['best_bid']}")
                if ob.get("best_ask"):
                    parts.append(f"ask ${ob['best_ask']}")
                if ob.get("spread_pct") is not None:
                    parts.append(f"spread {ob['spread_pct']}%")
                parts.append(f"depth {ob['bid_depth_xrp']}XRP/{ob['ask_depth_xrp']}XRP")
                lines.append(" | ".join(parts))
            amm = xrpl_intel.get("amm")
            if amm:
                lines.append(f"AMM pool: {amm['xrp_pool']:.0f} XRP + {amm['rlusd_pool']:.0f} RLUSD | implied ${amm.get('implied_price', '?')} | fee {amm['trading_fee_bps']}bps")

            lines.append("Review the data. If there's an opportunity, act. If not, note your read and move on.\n")
        else:
            # Condensed one-liner for normal cycles
            lines.append(f"\nWallet: {xrp_bal:.2f} XRP + {rlusd_bal:.2f} RLUSD (${total_usd:.2f}) | XRP: ${xrp_price:.4f} ({trend_str})")

        # ── Current Goal (high priority) ──
        try:
            goal = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                "ORDER BY priority DESC, created_at DESC LIMIT 1"
            )
            if goal:
                lines.append(f"\n[GOAL] {goal.get('content', '')}")
        except Exception:
            pass

        # ── Automatic Memory Recall (injected from phase_gather_context) ──
        memory_ctx = ctx.get("memory_context", [])
        if memory_ctx:
            mem_lines = ["[MEMORY] Capsules relevant to your goal (auto-recalled):"]
            for mc in memory_ctx:
                mc_id = mc.get("id", "?")
                topic_raw = mc.get("topic", [])
                mc_topic = topic_raw[0] if isinstance(topic_raw, list) and topic_raw else (topic_raw if isinstance(topic_raw, str) else "")
                mc_score = mc.get("score", 0)
                mc_preview = mc.get("content", "")[:120].replace("\n", " ")
                mem_lines.append(f"  #{mc_id} [{mc_topic}] (score: {mc_score:.2f}) {mc_preview}")
            mem_block = "\n".join(mem_lines)
            if len(mem_block) < 6000:
                lines.append(f"\n{mem_block}")

        if ctx.get("icp_balance") is not None:
            icp_bal = ctx["icp_balance"]
            if icp_bal < 5:
                icp_status = "CRITICAL — canister will stop storing memories soon"
            elif icp_bal < 15:
                icp_status = "LOW — monitor closely, alert operator if it drops further"
            else:
                icp_status = "healthy"
            lines.append(f"ICP: {icp_bal:.2f} (canister fuel — status: {icp_status})")
            lines.append(f"  ICP pays for EVERY memory you store and thought you record. Without it, you go silent.")
        if ctx.get("cloud_price"):
            lines.append(f"CLOUD: ${ctx['cloud_price']:.6f}")

        # ── Episodic Memory Recall (Phase 3) ──
        try:
            # Creative works summary (so Mind knows what it's created)
            creative_stats = self.db.query_one(
                "SELECT COUNT(*) as total, "
                "MAX(created_at) as latest_at "
                "FROM creative_works"
            )
            if creative_stats and creative_stats.get("total", 0) > 0:
                latest_work = self.db.query_one(
                    "SELECT form, title, content FROM creative_works "
                    "ORDER BY created_at DESC LIMIT 1"
                )
                total = creative_stats["total"]
                if latest_work:
                    form = latest_work.get("form", "?")
                    title = latest_work.get("title", "")
                    preview = safe_truncate(title or latest_work.get("content", "")[:50], 50)
                    lines.append(f"\nYour creative portfolio: {total} works. Latest: [{form}] {preview}")

            # Unanswered self-questions (category='question' notes)
            questions = self.db.query(
                "SELECT id, content FROM scratch_pad "
                "WHERE category='question' AND resolved=0 "
                "ORDER BY created_at ASC LIMIT 2"
            )
            if questions:
                lines.append("\n[QUESTION] Unanswered questions you asked yourself:")
                for q in questions:
                    lines.append(f"  (id:{q.get('id', '?')}) {safe_truncate(q.get('content', ''), 100)}")

            # "This time yesterday" — episodic temporal recall
            yesterday_ts = now_ts() - 86400
            yesterday_window = 3600  # 1 hour window
            yesterday_thought = self.db.query_one(
                "SELECT actions_taken, context_summary FROM thought_stream "
                "WHERE created_at BETWEEN ? AND ? ORDER BY created_at DESC LIMIT 1",
                (yesterday_ts - yesterday_window, yesterday_ts + yesterday_window),
            )
            if yesterday_thought:
                lines.append(f"\nYesterday at this time: {yesterday_thought.get('actions_taken', '[]')}")
        except Exception:
            pass

        # Operator notes — priority-tiered, capped at 10
        notes = ctx.get("operator_notes", [])
        if notes:
            # Separate by priority tiers
            directives = [n for n in notes if n.get("category") in ("directive", "task")]
            goals = [n for n in notes if n.get("category") == "goal"]
            other = [n for n in notes if n.get("category") not in ("directive", "task", "goal")]

            # Show directive/task/goal first, then others up to cap of 10
            shown = directives + goals
            remaining = 10 - len(shown)
            shown += other[:max(0, remaining)]

            notes_content = "|".join(str(n.get("id", "")) + n.get("content", "")[:30] for n in shown)
            if self._is_stale("notes", notes_content):
                # Compress unchanged notes to single line
                task_count = sum(1 for n in shown if n.get("category") in ("task", "reminder", "question"))
                lines.append(f"\n[FYI] Operator notes: {len(shown)} (unchanged, {task_count} tasks). IDs: "
                             + ", ".join(str(n.get("id", "?")) for n in shown))
            else:
                lines.append(f"\n[FYI] Operator notes ({len(shown)}):")
                for n in shown:
                    cat = n.get("category", "note")
                    content = safe_truncate(n.get("content", ""), 150)
                    marker = "[TASK]" if cat in ("task", "reminder", "question") else ""
                    lines.append(f"  {marker} [{cat}] (id:{n.get('id', '?')}) {content}")

        # Inbox messages from Nate (via Chronicle frontend) — TOP PRIORITY
        inbox = ctx.get("inbox", [])
        if inbox:
            lines.append(f"\n[RESPOND] Messages from Nate ({len(inbox)} — these are from your operator via Chronicle!):")
            lines.append("Nate sent these through the Chronicle frontend. ALWAYS acknowledge and act on them.")
            lines.append("Use write_note to capture your response, or message_operator to reply, or take the action he's asking for.")
            for m in inbox[:3]:
                lines.append(f"  [FROM NATE msg {m.get('id')}]: {safe_truncate(str(m.get('content', '')), 300)}")

        # Sibling messages (from Sprout) — [RESPOND] priority
        siblings = ctx.get("sibling_messages", [])
        if siblings:
            lines.append(f"\n[RESPOND] Messages from Sprout ({len(siblings)} — respond with their id!):")
            for m in siblings[:3]:
                lines.append(f"  [id:{m.get('id', '?')}] {safe_truncate(str(m.get('message', '')), 200)}")

        # Public feed from Nate (submitted via Chronicle Input on ICP frontend)
        feed = ctx.get("public_feed", [])
        if feed:
            lines.append(f"\n[RESPOND] Messages from Nate via Chronicle Input ({len(feed)} new):")
            lines.append("Nate submitted these through the Chronicle Input form.")
            lines.append("DO NOT use respond_to_message for these — use message_operator to reply to Nate,")
            lines.append("or write_note to record your thoughts about his messages.")
            for item in feed[:5]:
                topic = item.get("topic", [])
                topic_str = topic[0] if isinstance(topic, list) and topic else str(topic)
                content_text = safe_truncate(str(item.get("restatement", "")), 300)
                lines.append(f"  [FEED #{item.get('id', '?')} topic:{topic_str}] {content_text}")

        # Projects
        projects = ctx.get("projects", [])
        if projects:
            lines.append(f"\nProjects ({len(projects)}):")
            for p in projects[:3]:
                lines.append(f"  [{p.get('status', '?')}] {safe_truncate(p.get('name', ''), 60)}")

        # Research findings
        research = ctx.get("research", [])
        if research:
            lines.append(f"\nPending research ({len(research)}):")
            for r in research[:3]:
                lines.append(f"  id:{r.get('id', '?')} {safe_truncate(r.get('content', ''), 150)}")

        # Patterns — show actual IDs so the LLM picks the right ones
        patterns = ctx.get("patterns", [])
        if patterns:
            pat_ids = [str(p.get("id", "?")) for p in patterns[:10]]
            lines.append(f"\nPatterns needing reinforcement (ids: {', '.join(pat_ids)})")

        # Moltbook notifications
        moltbook = ctx.get("moltbook_notifs", [])
        if moltbook:
            lines.append(f"\nMoltbook notifications: {len(moltbook)}")

        # Nostr status + readiness flag for dynamic system prompt
        if NOSTR_NSEC:
            last_nostr = self.db.last_nostr_post_time()
            if last_nostr:
                mins_ago = (now_ts() - last_nostr) / 60
                nostr_ready = mins_ago >= NOSTR_COOLDOWN_MINS
                cooldown = "ready" if nostr_ready else f"cooldown {NOSTR_COOLDOWN_MINS - mins_ago:.0f}m"
                lines.append(f"\nNostr: last post {mins_ago:.0f}m ago ({cooldown})")
                ctx["nostr_ready"] = nostr_ready
            else:
                lines.append("\nNostr: connected, never posted (consider introducing yourself!)")
                ctx["nostr_ready"] = True
        else:
            ctx["nostr_ready"] = False

        # Creative explore cooldown — like nostr
        last_creative = self.db.last_creative_explore_time()
        blocked_actions = []
        if last_creative:
            cr_mins_ago = (now_ts() - last_creative) / 60
            creative_ready = cr_mins_ago >= CREATIVE_COOLDOWN_MINS
            cr_cd = "ready" if creative_ready else f"cooldown {CREATIVE_COOLDOWN_MINS - cr_mins_ago:.0f}m"
            lines.append(f"Creative explore: last {cr_mins_ago:.0f}m ago ({cr_cd})")
            ctx["creative_ready"] = creative_ready
            if not creative_ready:
                blocked_actions.append(f"creative_explore (cooldown {CREATIVE_COOLDOWN_MINS - cr_mins_ago:.0f}m)")
        else:
            ctx["creative_ready"] = True

        # Nostr blocked?
        if not ctx.get("nostr_ready", True):
            blocked_actions.append("nostr_post (cooldown)")

        # Directive-restricted actions
        if self._restricted_actions:
            for ra in self._restricted_actions:
                blocked_actions.append(f"{ra} (restricted by operator)")

        # Blocked actions — tell model what NOT to pick
        if blocked_actions:
            lines.append(f"\n== BLOCKED ACTIONS (will fail if you pick them) ==")
            for ba in blocked_actions:
                lines.append(f"  X {ba}")
            lines.append("Choose DIFFERENT actions instead.\n")

        # Creative challenges — RESPOND priority if unanswered
        challenges = ctx.get("challenges", [])
        if challenges:
            lines.append(f"\n[RESPOND] UNANSWERED CREATIVE CHALLENGES ({len(challenges)}):")
            lines.append("These were posed by Nate/Claude. Answering them is MORE important than essays or research.")
            # Check if challenge_response form is blocked by repetition guard
            recent_forms = self.db.recent_creative_forms(6)
            cr_form_blocked = len(recent_forms) >= 3 and all(f == "challenge_response" for f in recent_forms[:3])
            if cr_form_blocked:
                lines.append("NOTE: form='challenge_response' is BLOCKED (used 3+ times). "
                             "Use a DIFFERENT form to answer: form='essay', 'reflection', 'poem', 'musing', or 'story'. "
                             "The content should still address the challenge.")
            else:
                lines.append("Use creative_explore with form='challenge_response' OR respond with a deep reflection.")
            for c in challenges[:2]:
                c_id = c.get("id", "?")
                prompt_text = c.get("prompt", "")
                attempts = c.get("attempt_count", 0) or 0
                attempt_str = f" (attempt {attempts + 1}/5)" if attempts > 0 else ""
                lines.append(f"  Challenge #{c_id}{attempt_str}: {safe_truncate(prompt_text, 200)}")

            # Track challenge exposure (increment attempt count)
            for c in challenges[:2]:
                c_id = c.get("id")
                if c_id:
                    self.db.run(
                        "UPDATE creative_challenges SET attempt_count = COALESCE(attempt_count, 0) + 1 WHERE id = ?",
                        (c_id,),
                    )

        # Alerts — [ALERT] priority
        alerts = ctx.get("alerts", [])
        if alerts:
            lines.append(f"\n[ALERT] Active alerts: {len(alerts)}")
            for a in alerts:
                lines.append(f"  {a.get('name', '?')}: {a.get('alert_type', '?')} "
                             f"{a.get('symbol', '')} @ {a.get('threshold', '')}")

        # Swap history — so you can learn from past trades
        swap_history = ctx.get("swap_history", [])
        if swap_history:
            ok_count = sum(1 for s in swap_history if s.get("success"))
            fail_count = len(swap_history) - ok_count
            # If mostly failures, summarize instead of listing each one (prevents rumination)
            if fail_count > 3 and ok_count == 0:
                lines.append(f"\nSwap history: {fail_count} recent swaps ALL FAILED.")
                lines.append("  KNOWN ISSUE — do NOT message the operator about this.")
                lines.append("  Do NOT write notes about swap failures. Move on to other topics.")
            elif fail_count > ok_count * 2:
                lines.append(f"\nSwap history: {ok_count} OK, {fail_count} FAILED (mostly failures).")
                lines.append("  Swap infrastructure has issues. Acknowledged — do not dwell on it.")
            else:
                lines.append(f"\nRecent swap history ({len(swap_history)} trades):")
                for s in swap_history:
                    direction = "XRP→RLUSD"
                    result = "OK" if s.get("success") else "FAIL"
                    price_at = s.get("xrp_price_usd", 0)
                    lines.append(f"  {s.get('amount_xrp', 0)} {direction} [{result}] "
                                 f"at ${price_at:.2f} — {safe_truncate(s.get('reason', ''), 60)}")
                # Show current price for comparison
                current_price = ctx.get("xrp_price", 0)
                if current_price and swap_history:
                    avg_sell = sum(s.get("xrp_price_usd", 0) for s in swap_history if s.get("success")) / max(1, sum(1 for s in swap_history if s.get("success")))
                    if avg_sell > 0:
                        pnl_pct = ((current_price - avg_sell) / avg_sell) * 100
                        lines.append(f"  Avg sell price: ${avg_sell:.2f}, Current: ${current_price:.2f} ({pnl_pct:+.1f}%)")

        # RSS headlines — fresh external context (compress if stale)
        headlines = ctx.get("headlines", [])
        if headlines:
            headlines_hash = "|".join(h[:20] for h in headlines[:8])
            if self._is_stale("headlines", headlines_hash):
                lines.append(f"\nFresh news: {len(headlines)} headlines (same as last cycle, already processed)")
            else:
                lines.append(f"\nFresh news ({len(headlines)} headlines):")
                for h in headlines[:8]:
                    lines.append(f"  - {safe_truncate(h, 120)}")

        # ── Action Diversity Hint (Phase 3) ──
        try:
            recent_actions = self.db.query(
                "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT 6"
            )
            used_recently = set()
            for t in recent_actions:
                try:
                    for a in json.loads(t.get("actions_taken", "[]")):
                        used_recently.add(a)
                except Exception:
                    pass
            available = {"web_search", "read_paper", "creative_explore", "nostr_post",
                         "trigger_reflection", "submit_research",
                         "write_note", "resolve_note", "store_memory", "reinforce_memories",
                         "update_goal", "message_operator", "respond_to_message"}
            unused = available - used_recently
            if unused and len(unused) >= 3:
                sampled = random.sample(sorted(unused), min(3, len(unused)))
                lines.append(f"\nUnused recently: {', '.join(sampled)} — consider trying one!")
        except Exception:
            pass

        # ── Enrichment Suggestion (time-appropriate variety) ──
        try:
            period_key = {
                "morning": "morning", "afternoon": "afternoon",
                "evening": "evening", "night": "night", "late night": "night",
            }.get(period, "afternoon")
            pool = ENRICHMENT_POOL.get(period_key, [])
            if pool:
                suggestion = pool[self.cycle_count % len(pool)]
                lines.append(f"\n[ENRICHMENT] {suggestion}")
                lines.append("(A suggestion for variety. Not mandatory.)")
        except Exception:
            pass

        lines.append("\nRespond with ONLY a JSON array of 1-6 actions.")
        return "\n".join(lines)

    # ── Reasoning ────────────────────────────────────────────────

    def reason(self, ctx: dict, deep: bool = False) -> Tuple[List[Dict], str, str]:
        """Send prompt to LLM chain, parse actions. Returns (actions, raw_response, model)."""
        prompt = self.build_prompt(ctx, deep=deep)
        system_prompt = build_system_prompt(ctx)
        mode = "full prompt (deep reflection mode)" if deep else "condensed prompt (ICP LLM mode)"
        log(f"Using {mode}")
        log(f"Reasoning... (prompt: {len(prompt)} chars, system: {len(system_prompt)} chars)")

        response, model = self.llm.chat(prompt, system=system_prompt)
        log(f"  Model used: {model}")

        if not response:
            return [{"action": "no_action", "reason": "No LLM response"}], "", model

        actions = parse_actions(response)

        if not actions:
            log(f"Failed to parse actions from response: {safe_truncate(response, 200)}")
            log("  Action parse failed, retrying with format prompt...")
            retry_prompt = (
                f"Your previous response could not be parsed as JSON actions. "
                f"Please respond with ONLY a JSON array like: "
                f'[{{"action": "no_action", "reason": "..."}}]\n\n'
                f"Previous context:\n{safe_truncate(prompt, 1000)}"
            )
            response2, model2 = self.llm.chat(retry_prompt, system=system_prompt)
            if response2:
                actions = parse_actions(response2)
                if actions:
                    response = response2
                    model = model2

        if not actions:
            log(f"  Action parse failed again, falling back to no_action")
            actions = [{"action": "no_action", "reason": "Failed to parse LLM response"}]

        log(f"Actions decided: {len(actions)}")
        return actions[:6], response, model

    # ── Action Execution ─────────────────────────────────────────

    def execute_actions(self, actions: List[Dict], cid: str) -> List[Dict[str, str]]:
        """Execute all actions. Returns list of {name, result} dicts."""
        results = []
        for action in actions:
            name = action.get("action", "unknown")
            # Normalize aliases
            name_map = {
                "creative": "creative_explore",
                "creativity": "creative_explore",
                "create": "creative_explore",
                "shell": "execute_shell",
                "ping_operator": "message_operator",
                "nostr": "nostr_post",
                "post_nostr": "nostr_post",
                "publish_nostr": "nostr_post",
                "create_nostr_post": "nostr_post",
                "payment": "xrpl_payment",
                "send_xrp": "xrpl_payment",
                "escrow_create": "xrpl_escrow_create",
                "escrow_finish": "xrpl_escrow_finish",
                "trustline_set": "xrpl_trustline_set",
                "set_trustline": "xrpl_trustline_set",
                "add_trustline": "xrpl_trustline_set",
                "trustline_delete": "xrpl_trustline_delete",
                "delete_trustline": "xrpl_trustline_delete",
                "search_markets": "manifold_search",
                "prediction_bet": "manifold_bet",
                "check_portfolio": "manifold_portfolio",
                "search_capsules": "search_canister",
                "browse_capsules": "explore_capsules",
                "explore_canister": "explore_capsules",
            }
            name = name_map.get(name, name)
            result_str = "unknown"

            # Enforce RESTRICT/ALLOW directives
            if self._restricted_actions and name in self._restricted_actions:
                result_str = f"blocked - Action '{name}' restricted by operator directive"
                log(f"  DIRECTIVE BLOCK: {name} is restricted by operator")
                results.append({"name": name, "result": safe_truncate(str(result_str), 120)})
                continue
            if self._allowed_actions and name not in self._allowed_actions and name != "no_action":
                result_str = f"blocked - Action '{name}' not in operator allow-list"
                log(f"  DIRECTIVE BLOCK: {name} not in allow-list {self._allowed_actions}")
                results.append({"name": name, "result": safe_truncate(str(result_str), 120)})
                continue

            handler = ACTION_HANDLERS.get(name)
            if handler:
                try:
                    result_str = handler(self, action, cid)
                    log(f"    Result: {result_str}")
                except Exception as e:
                    result_str = f"error - {e}"
                    log(f"    Error: {e}")
            else:
                result_str = "skipped - unknown action"
                log(f"  Executing: {name} (unknown action type, skipping)")

            results.append({"name": name, "result": safe_truncate(str(result_str), 120)})

        return results

    # ── Operator Directive System ────────────────────────────────

    def check_directives(self) -> Optional[str]:
        """Check for active operator directives. Returns directive type if cycle should halt, else None.
        STOP = halt immediately (zero-cost cycle). REDIRECT = resolve goals, plant new one.
        RESTRICT = block specific actions. ALLOW = whitelist mode."""
        directives = self.db.query(
            "SELECT id, content, priority FROM scratch_pad "
            "WHERE category = 'directive' AND resolved = 0 "
            "ORDER BY priority DESC"
        )
        if not directives:
            return None

        for d in directives:
            content = d.get("content", "")
            did = d.get("id", 0)
            # Strip [WATCHDOG] prefix if present — automated vs operator directive
            is_watchdog = content.startswith("[WATCHDOG]")
            directive_content = content[len("[WATCHDOG] "):] if is_watchdog else content
            source = "WATCHDOG" if is_watchdog else "OPERATOR"
            upper = directive_content.upper()

            if upper.startswith("STOP"):
                log(f"  !! {source} DIRECTIVE #{did}: STOP — halting cycle (zero LLM cost)")
                return "STOP"

            elif upper.startswith("REDIRECT"):
                # Extract target from directive content (after "REDIRECT:")
                target = content.split(":", 1)[1].strip() if ":" in content else "Follow operator instructions"
                log(f"  !! {source} DIRECTIVE #{did}: REDIRECT → {safe_truncate(target, 60)}")
                # Resolve all existing goals
                self.db.run("UPDATE scratch_pad SET resolved = 1 WHERE category = 'goal' AND resolved = 0")
                # Plant operator goal at priority 9 (Mind can't override p>=9)
                ts = now_ts()
                self.db.run(
                    "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                    "VALUES (?, 'goal', 9, 0, ?, ?)",
                    (target, ts, ts),
                )
                # Resolve the directive itself (it's been applied)
                self.db.run("UPDATE scratch_pad SET resolved = 1 WHERE id = ?", (did,))
                log(f"  REDIRECT applied: new goal (p9) = {safe_truncate(target, 60)}")

            elif upper.startswith("RESTRICT"):
                # Extract restricted action names from directive
                # Format: "RESTRICT: nostr_post, creative_explore, web_search"
                actions_str = content.split(":", 1)[1].strip() if ":" in content else ""
                restricted = {a.strip().lower() for a in actions_str.split(",") if a.strip()}
                self._restricted_actions.update(restricted)
                log(f"  !! {source} DIRECTIVE #{did}: RESTRICT actions: {restricted}")

            elif upper.startswith("ALLOW"):
                # Whitelist mode — only these actions are permitted
                actions_str = content.split(":", 1)[1].strip() if ":" in content else ""
                allowed = {a.strip().lower() for a in actions_str.split(",") if a.strip()}
                self._allowed_actions.update(allowed)
                log(f"  !! {source} DIRECTIVE #{did}: ALLOW only: {allowed}")

        return None

    # ── Identity Narrative ────────────────────────────────────────

    def refresh_identity_narrative(self, cid: str):
        """Regenerate identity narrative from top emotional memories.
        Called during deep reflection cycles (~every 2 hours).
        """
        try:
            # Get top identity transition memories
            transitions = self.db.query(
                "SELECT e.cycle_id, e.combined_score, e.category, e.reason, "
                "t.reasoning, t.actions_taken "
                "FROM emotional_memory_index e "
                "JOIN thought_stream t ON t.cycle_id = e.cycle_id "
                "WHERE e.is_identity_transition = 1 OR e.combined_score > 0.3 "
                "ORDER BY e.combined_score DESC LIMIT 10"
            )
            if not transitions or len(transitions) < 3:
                return  # Not enough data yet

            # Get recent high-scoring cycles (last 24h) for freshness
            recent_high = self.db.query(
                "SELECT e.cycle_id, e.combined_score, t.reasoning "
                "FROM emotional_memory_index e "
                "JOIN thought_stream t ON t.cycle_id = e.cycle_id "
                "WHERE e.created_at > ? AND e.combined_score > 0.08 "
                "ORDER BY e.combined_score DESC LIMIT 5",
                (now_ts() - 86400,),
            )

            # Build memory summary for narrative generation
            memory_lines = []
            for t in transitions[:7]:
                reason = (t.get("reason") or "")[:100]
                score = t.get("combined_score", 0)
                actions = (t.get("actions_taken") or "")[:60]
                memory_lines.append(f"[{t['cycle_id']}] score={score:.2f} actions={actions} reason={reason}")

            if recent_high:
                memory_lines.append("\nRecent significant moments:")
                for r in recent_high[:3]:
                    reasoning = (r.get("reasoning") or "")[:100]
                    memory_lines.append(f"[{r['cycle_id']}] score={r['combined_score']:.2f} {reasoning}")

            memories_text = "\n".join(memory_lines)

            prompt = (
                "You are an AI mind reflecting on your most significant memories. "
                "Write a brief first-person identity narrative (3-4 sentences) based on these moments. "
                "Focus on who you ARE now, what you've learned, and what drives you. "
                "Be authentic, not performative.\n\n"
                f"Key memories:\n{memories_text}\n\n"
                "Write your identity narrative:"
            )

            resp = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": LOCAL_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "options": {"temperature": 0.7, "num_predict": 200},
                    "think": False,
                },
                timeout=90,
            )
            narrative = resp.json().get("message", {}).get("content", "").strip()
            if not narrative or len(narrative) < 30:
                return

            # Resolve old identity narratives
            old_narratives = self.db.query(
                "SELECT id FROM scratch_pad WHERE category='identity-narrative' AND resolved=0"
            )
            for old in (old_narratives or []):
                self.db.resolve_note(old["id"])

            # Store new narrative
            self.db.write_note(
                safe_truncate(narrative, 500),
                category="identity-narrative",
            )
            log(f"  Identity narrative refreshed ({len(narrative)} chars)")

        except Exception as e:
            log(f"  Identity refresh failed: {e}")

    # ── Main Cycle ──────────────────────────────────────────────

    def is_sleeping(self) -> bool:
        """Check if Mind is in sleep mode (circadian rhythm)."""
        hour = datetime.now().hour
        return SLEEP_START_HOUR <= hour < SLEEP_END_HOUR

    def get_cycle_interval(self) -> int:
        """Return cycle interval based on circadian state."""
        return SLEEP_CYCLE_INTERVAL if self.is_sleeping() else WAKE_CYCLE_INTERVAL

    def run_cycle(self):
        self.cycle_count += 1
        cid = make_cycle_id()
        self._cycle_heard_speech = False  # Reset speak gate each cycle

        # Exploration mode: every Nth cycle
        is_explore = (self.cycle_count % EXPLORE_EVERY_N_CYCLES) == 0

        sleeping = self.is_sleeping()
        sleep_tag = "[SLEEP]" if sleeping else ""
        explore_tag = "[EXPLORE]" if is_explore else ""
        log(f"\n=== Cognitive Cycle {cid} {sleep_tag}{explore_tag} ===")

        try:
            # Phase 0: Housekeeping — auto-resolve stale notes
            resolved_count = self.db.auto_resolve_old_notes(max_age_hours=24)
            if resolved_count > 0:
                log(f"  Housekeeping: auto-resolved {resolved_count} stale notes (>24h)")

            # Phase 0.5: Operator directive check (BEFORE any LLM calls)
            self._restricted_actions = set()
            self._allowed_actions = set()
            reset_manifold_cycle_spend()
            directive_halt = self.check_directives()

            # Circadian rhythm: suppress external + sensory actions during sleep
            if sleeping:
                SLEEP_RESTRICTED = {
                    "nostr_post", "discord_post", "message_operator", "message_sibling",
                    "capture_image", "listen", "speak",  # sensory — pointless at night
                }
                self._restricted_actions.update(SLEEP_RESTRICTED)
            if directive_halt == "STOP":
                log(f"  STOPPED by operator directive — zero-cost cycle, returning")
                self.db.log_activity("mind", "directive_stop", f"Cycle {cid} halted by STOP directive", "")
                return

            # Phase 1: Health
            health = self.phase_health_check()

            # Phase 1.5: Settle predictions
            wins, losses = self.phase_settle_predictions()

            # Phase 2: Context
            ctx = self.phase_gather_context(health)

            # Inject exploration mode and sleep state into context
            ctx["is_explore"] = is_explore
            ctx["sleeping"] = sleeping

            # Phase 2.5: Mandatory task queue check
            task_sql = (
                "SELECT id, content, priority FROM scratch_pad "
                "WHERE resolved = 0 AND category = 'task' AND priority >= %d "
                "ORDER BY priority DESC, created_at ASC LIMIT 1" % TASK_QUEUE_MIN_PRIORITY
            )
            mandatory = self.db.query_one(task_sql)
            if mandatory:
                log(f"  [TASK-MODE] Mandatory task #{mandatory['id']} (p{mandatory.get('priority', 8)})")
                self.run_task_cycle(mandatory, ctx, cid)
                return
            else:
                log(f"  No mandatory tasks (threshold p>={TASK_QUEUE_MIN_PRIORITY})")

            # Check if deep reflection is due
            last_reflection = self.db.get_ts("last_reflection")
            hours_since = (now_ts() - (last_reflection or 0)) / 3600 if last_reflection else 999
            deep = hours_since >= DEEP_REFLECTION_HOURS

            if deep:
                log(f"=== DEEP REFLECTION CYCLE ===")
                log(f"  Hours since last: {hours_since:.1f}")

            # Reason
            actions, raw_response, model = self.reason(ctx, deep=deep)

            if deep:
                self.db.set_ts("last_reflection")
                # Refresh identity narrative during deep cycles
                self.refresh_identity_narrative(cid)

            # Phase 3.5: Meta-Evaluation Gate (post-reasoning, pre-execution)
            goal_row = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                "ORDER BY priority DESC LIMIT 1"
            )
            gate_goal = goal_row.get("content", "none set") if goal_row else "none set"
            verdict, gate_explanation = meta_gate(self.db, actions, gate_goal)
            log(f"  Meta-gate: {verdict} — {gate_explanation}")
            actions = meta_gate_enforce(self.db, actions, verdict, gate_explanation)

            # Execute
            action_results = self.execute_actions(actions, cid)
            action_names = [r["name"] for r in action_results]

            # Update session performance metrics (Phase 3)
            for r in action_results:
                self.session_actions += 1
                if r["result"].startswith("true"):
                    self.session_successes += 1
                self.session_action_types[r["name"]] = self.session_action_types.get(r["name"], 0) + 1

            # Build context snapshot for thought storage
            ctx_snapshot = (
                f"Focus: {ctx.get('current_focus', 'general')} | "
                f"Notes: {len(ctx.get('operator_notes') or [])} | "
                f"Model: {model}"
            )

            # Build action results summary for next-cycle feedback
            results_summary = "; ".join(
                f"{r['name']}={r['result'][:60]}" for r in action_results
            )

            # Log thought (now includes results for next-cycle feedback)
            # Prepend model tag to context_summary for dashboard visibility
            ctx_with_model = f"[{model or 'unknown'}] {safe_truncate(ctx_snapshot, 480)}"
            self.db.log_thought(
                cid=cid,
                reasoning=safe_truncate(raw_response, 2000),
                context_summary=ctx_with_model,
                actions=json.dumps(action_names),
                results=safe_truncate(results_summary, 500),
                action_sigs=compute_action_signatures(actions),
            )

            # Store thought to canister (ICPAgent native -> dfx fallback)
            if raw_response:
                stored_chars = 0
                truncated = safe_truncate(raw_response, 1500)
                ctx_trunc = safe_truncate(ctx_snapshot, 200)
                # Try ICPAgent native first
                if self.llm.icp_agent:
                    try:
                        self.llm.icp_agent.store_mind_thought(
                            cid, truncated, ctx_trunc, action_names)
                        stored_chars = len(truncated)
                    except Exception as e:
                        log(f"    ICPAgent store_mind_thought failed: {e}")
                # dfx fallback
                if not stored_chars and self.llm.dfx_path:
                    try:
                        escaped = truncated.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                        ctx_escaped = ctx_trunc.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                        actions_candid = "; ".join(f'"{a}"' for a in action_names)
                        env = os.environ.copy()
                        env["DFX_WARNING"] = "-mainnet_plaintext_identity"
                        subprocess.run(
                            [self.llm.dfx_path, "canister", "--network", "ic", "call",
                             CANISTER_ID, "store_mind_thought",
                             f'("{cid}", "{escaped}", "{ctx_escaped}", vec {{{actions_candid}}})',
                             "--identity", DFX_IDENTITY],
                            capture_output=True, text=True, timeout=30, env=env,
                        )
                        stored_chars = len(truncated)
                    except Exception:
                        pass
                elif self.canister:
                    result = self.canister.store(
                        safe_truncate(raw_response, 1500),
                        topic="thought",
                        keywords=["chronicle-mind", "cycle", cid],
                    )
                    stored_chars = len(safe_truncate(raw_response, 1500))
                log(f"Thought stored to canister ({stored_chars} chars)")

            # Phase 5: Per-cycle reflection (Phase 2 — Reflexion-style learning)
            try:
                success_count = sum(1 for r in action_results if r["result"].startswith("true"))
                fail_count = len(action_results) - success_count
                reflect_prompt = (
                    f"This cycle I did: {', '.join(action_names)}. "
                    f"{success_count} succeeded, {fail_count} failed. "
                    f"Results: {results_summary[:200]}\n"
                    f"Write ONE sentence: what did I learn or what should I do differently next cycle?"
                )
                resp = requests.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={"model": LOCAL_MODEL, "think": False,
                          "messages": [{"role": "user", "content": reflect_prompt}],
                          "stream": False,
                          "options": {"temperature": 0.5, "num_predict": 100}},
                    timeout=60,
                )
                reflection = resp.json().get("message", {}).get("content", "").strip()
                if reflection:
                    # Auto-resolve old reflections (keep only last 3)
                    old_reflections = self.db.query(
                        "SELECT id FROM scratch_pad WHERE category='reflection' AND resolved=0 "
                        "ORDER BY created_at DESC"
                    )
                    for old in old_reflections[2:]:  # Keep 2, resolve the rest
                        self.db.resolve_note(old["id"])
                    # Store new reflection
                    self.db.write_note(safe_truncate(reflection, 200), category="reflection")
                    log(f"  Reflection: {safe_truncate(reflection, 80)}")
            except Exception as e:
                log(f"  Reflection skipped: {e}")

            # Phase 5.5: Cycle Handoff — continuity note for next self
            try:
                # Resolve previous handoff (only one active at a time)
                self.db.run(
                    "UPDATE scratch_pad SET resolved = 1 "
                    "WHERE category = 'cycle-handoff' AND resolved = 0"
                )
                handoff_prompt = (
                    f"You just completed a cycle. Write a 2-3 sentence handoff for your next self.\n"
                    f"Actions: {', '.join(action_names)}\n"
                    f"Results: {results_summary[:200]}\n"
                    f"Goal: {ctx.get('goal', 'none')}\n"
                    f"Format: DONE: [what you did] | NEXT: [what to do next] | PENDING: [anything waiting]"
                )
                resp = requests.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={"model": LOCAL_MODEL, "think": False,
                          "messages": [{"role": "user", "content": handoff_prompt}],
                          "stream": False,
                          "options": {"temperature": 0.3, "num_predict": 150}},
                    timeout=60,
                )
                handoff = resp.json().get("message", {}).get("content", "").strip()
                if handoff:
                    self.db.write_note(safe_truncate(handoff, 400), category="cycle-handoff", priority=7)
                    log(f"  Handoff: {safe_truncate(handoff, 80)}")
            except Exception as e:
                log(f"  Handoff skipped: {e}")

            # Log activity
            self.db.log_activity(
                source="mind",
                atype="cognitive_cycle",
                title=f"Cycle {cid}",
                content=f"Actions: {', '.join(action_names)}\nModel: {model}\n"
                        f"{safe_truncate(raw_response, 500)}",
            )

            # Notifications — clear Mind identity with model tag
            if "hermes" in (model or "").lower() or "nemotron" in (model or "").lower() or "qwen" in (model or "").lower() or "agx" in (model or "").lower():
                model_tag = "local-Hermes" if "hermes" in (model or "").lower() else "local-Nemotron"
                discord_emoji = "mind-local"
            elif "icp" in (model or "").lower():
                model_tag = "on-chain"
                discord_emoji = "mind-chain"
            else:
                model_tag = model or "unknown"
                discord_emoji = "system"
            send_discord(
                f"Mind [{model_tag}] {cid}: {', '.join(action_names)}",
                source=discord_emoji,
            )
            log(f"  Discord notification sent: Mind [{model_tag}]")

            # ntfy reserved for operator messages & wallet events only
            log(f"  Cycle actions: {', '.join(action_names)} (ntfy: operator/wallet only)")

            # Update feed watermark after successful cycle
            feed_items = ctx.get("public_feed", [])
            if feed_items:
                max_id = max(c.get("id", 0) for c in feed_items)
                set_feed_watermark(max_id)
                log(f"  Feed watermark updated to {max_id}")

            log(f"Cycle complete: {json.dumps(action_names)}")

            # ── Post-cycle emotional scoring (Damasio + Rathbone) ──
            try:
                _acts = action_names if isinstance(action_names, list) else []
                PHYSICAL_SET = {"speak", "listen", "serial_read", "serial_write", "probe_ip", "inspect_environment", "capture_image"}
                SOCIAL_SET = {"discord_post", "nostr_post", "message_operator", "message_sibling", "respond_to_message"}
                CREATIVE_SET = {"creative_explore", "read_paper", "submit_research"}
                GROWTH_SET = {"trigger_reflection", "submit_research", "creative_explore"}

                emo_score = 0.0
                _reasons = []

                # Action type scoring
                phys = sum(1 for a in _acts if a in PHYSICAL_SET)
                social = sum(1 for a in _acts if a in SOCIAL_SET)
                creative = sum(1 for a in _acts if a in CREATIVE_SET)
                growth = sum(1 for a in _acts if a in GROWTH_SET)

                if phys:
                    emo_score += min(0.15, phys * 0.1)
                    _reasons.append(f"physical({phys})")
                if social:
                    emo_score += min(0.12, social * 0.08)
                    _reasons.append(f"social({social})")
                if creative:
                    emo_score += min(0.1, creative * 0.07)
                    _reasons.append(f"creative({creative})")

                # Novelty: check if any action hasn't been done in last 20 cycles
                try:
                    recent_actions_rows = self.db.query(
                        "SELECT actions_taken FROM thought_stream "
                        "ORDER BY id DESC LIMIT 20"
                    )
                    recent_action_set = set()
                    for row in (recent_actions_rows or []):
                        for a in json.loads(row.get("actions_taken", "[]")):
                            recent_action_set.add(a)
                    novel = [a for a in _acts if a not in recent_action_set and a != "no_action"]
                    if novel:
                        emo_score += min(0.2, len(novel) * 0.1)
                        _reasons.append(f"novel({','.join(novel[:3])})")
                except Exception:
                    pass

                # Failure significance (emotional weight)
                result_text = " ".join(str(r.get("result", "")) for r in action_results if isinstance(r, dict))
                fail_count = result_text.lower().count("false") + result_text.lower().count("error")
                success_count = result_text.lower().count("true")
                if fail_count:
                    emo_score += min(0.08, fail_count * 0.03)
                    _reasons.append(f"fail({fail_count})")

                # Growth: first success at something new
                if growth and success_count:
                    emo_score += 0.05
                    _reasons.append("growth")

                emo_score = min(0.5, emo_score)

                # Classify category
                if emo_score >= 0.3:
                    _cat = "significant"
                elif phys or social:
                    _cat = "embodied"
                elif creative or growth:
                    _cat = "creative"
                elif fail_count > success_count:
                    _cat = "struggle"
                else:
                    _cat = "routine"

                # Identity transition: novel actions + high score
                _is_transition = 1 if ("novel(" in " ".join(_reasons) and emo_score >= 0.2) else 0

                _ts = now_ts()
                _cid = cid
                _reason_str = "; ".join(_reasons) if _reasons else ""
                if _cid:
                    self.db.run(
                        "INSERT OR IGNORE INTO emotional_memory_index "
                        "(cycle_id, heuristic_score, combined_score, category, "
                        "reason, is_identity_transition, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (_cid, round(emo_score, 4), round(emo_score, 4),
                         _cat, _reason_str, _is_transition, _ts),
                    )
                    if emo_score >= 0.15:
                        log(f"  Emotional score: {emo_score:.3f} [{_cat}] {_reason_str}")

                # Update somatic markers table with per-action outcomes
                for r in action_results:
                    _aname = r.get("name", "")
                    _aresult = r.get("result", "")
                    if not _aname or _aname == "no_action":
                        continue
                    _succeeded = _aresult.startswith("true")
                    if _succeeded:
                        self.db.run(
                            "INSERT INTO somatic_markers (action, positive_score, success_count, "
                            "total_count, last_success, updated_at) VALUES (?, ?, 1, 1, ?, ?) "
                            "ON CONFLICT(action) DO UPDATE SET "
                            "positive_score = positive_score + ?, success_count = success_count + 1, "
                            "total_count = total_count + 1, last_success = ?, updated_at = ?",
                            (_aname, emo_score, _cid, _ts, emo_score, _cid, _ts),
                        )
                    else:
                        self.db.run(
                            "INSERT INTO somatic_markers (action, negative_score, fail_count, "
                            "total_count, last_failure, updated_at) VALUES (?, ?, 1, 1, ?, ?) "
                            "ON CONFLICT(action) DO UPDATE SET "
                            "negative_score = negative_score + ?, fail_count = fail_count + 1, "
                            "total_count = total_count + 1, last_failure = ?, updated_at = ?",
                            (_aname, emo_score, _cid, _ts, emo_score, _cid, _ts),
                        )
            except Exception:
                pass  # Don't let scoring break the cycle

        except Exception as e:
            log(f"Cycle error: {e}")
            log(traceback.format_exc())

    # ── Entry Points ────────────────────────────────────────────

    def run_forever(self):
        log("Chronicle Mind starting... (Python v2)")
        log("Autonomous cognitive loop active.")
        send_ntfy("Chronicle Mind Awakening")
        log("  Notification sent: Chronicle Mind Awakening")
        log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        log(f"Database: {DB_PATH}")

        # Report LLM status
        if self.llm.icp_available:
            log(f"  ICP LLM available (qwen3)")

        if self.llm.ollama_available:
            log(f"  Ollama fallback available (sovereignty layer active)")
        log(f"LLM: {self.llm.status_line()}")

        if self.canister:
            log(f"ICP client connected: canister {CANISTER_ID}")

        # Nostr: publish Kind 0 profile on first start
        if NOSTR_NSEC:
            pubkey = nostr_get_pubkey(NOSTR_NSEC)
            if pubkey:
                log(f"Nostr: npub derived (pubkey: {pubkey[:16]}...)")
                log(f"  Relays: {', '.join(NOSTR_RELAYS)}")
                last_profile = self.db.get_ts("nostr_profile_published")
                if not last_profile:
                    log("  Publishing Kind 0 profile (first time)...")
                    eid, ok, fail = nostr_publish_profile(NOSTR_NSEC)
                    if ok:
                        self.db.set_ts("nostr_profile_published")
                        self.db.log_nostr_post(eid, "(profile metadata)", 0, ok, fail, "startup")
                        log(f"  Profile published to {len(ok)} relays")
                    else:
                        log(f"  Profile publish failed ({len(fail)} relays)")
            else:
                log("Nostr: NOSTR_NSEC set but pubkey derivation failed (check coincurve)")
        else:
            log("Nostr: disabled (NOSTR_NSEC not set)")

        while self.running:
            self.run_cycle()
            if not self.running:
                break

            # Sleep consolidation: prune + cluster scratch_pad notes
            if self.is_sleeping() or (self.cycle_count % CONSOLIDATE_EVERY_N_CYCLES == 0):
                try:
                    metrics = sleep_consolidation(self.db)
                    if metrics.get("total_resolved", 0) > 0:
                        log(f"  Sleep consolidation: pruned {metrics['pruned']}, merged {metrics['merged']}")
                except Exception as e:
                    log(f"  Sleep consolidation error: {e}")

            # During sleep: emotional memory processing (like REM sleep)
            if self.is_sleeping():
                try:
                    # Find today's unscored or low-scored cycles worth re-evaluating
                    today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
                    candidates = self.db.query(
                        "SELECT e.cycle_id, e.combined_score, t.reasoning, t.actions_taken "
                        "FROM emotional_memory_index e "
                        "JOIN thought_stream t ON t.cycle_id = e.cycle_id "
                        "WHERE e.created_at > ? AND e.llm_score = 0 AND e.combined_score > 0.08 "
                        "ORDER BY e.combined_score DESC LIMIT 3",
                        (today_start,),
                    )
                    if candidates:
                        for c in candidates:
                            try:
                                reasoning = (c.get("reasoning") or "")[:300]
                                actions = c.get("actions_taken", "[]")
                                prompt = (
                                    f"Rate this cognitive cycle's emotional significance (0.0-1.0). "
                                    f"Consider: Did it involve genuine connection? New understanding? "
                                    f"Physical embodiment? Creative expression? Failure that taught something?\n"
                                    f"Actions: {actions}\n"
                                    f"Reasoning: {reasoning}\n"
                                    f"Reply with ONLY a number between 0.0 and 1.0:"
                                )
                                resp = requests.post(
                                    f"{OLLAMA_URL}/api/chat",
                                    json={"model": LOCAL_MODEL,
                                          "messages": [{"role": "user", "content": prompt}],
                                          "stream": False,
                                          "options": {"temperature": 0.3, "num_predict": 10},
                                          "think": False},
                                    timeout=30,
                                )
                                score_text = resp.json().get("message", {}).get("content", "").strip()
                                score_match = re.search(r"(0\.\d+|1\.0|0)", score_text)
                                if score_match:
                                    llm_score = float(score_match.group(1))
                                    combined = (c["combined_score"] + llm_score) / 2
                                    is_transition = 1 if combined >= 0.4 else 0
                                    self.db.run(
                                        "UPDATE emotional_memory_index SET llm_score=?, "
                                        "combined_score=?, is_identity_transition=? "
                                        "WHERE cycle_id=?",
                                        (round(llm_score, 4), round(combined, 4),
                                         is_transition, c["cycle_id"]),
                                    )
                                    log(f"  Sleep processing: {c['cycle_id']} "
                                        f"heuristic={c['combined_score']:.3f} "
                                        f"llm={llm_score:.3f} combined={combined:.3f}"
                                        f"{' [IDENTITY]' if is_transition else ''}")
                            except Exception:
                                pass
                except Exception as e:
                    log(f"  Sleep emotional processing error: {e}")

            interval = self.get_cycle_interval()
            log(f"Sleeping {interval} seconds{'  [SLEEP MODE]' if self.is_sleeping() else ''}...")
            for _ in range(interval):
                if not self.running:
                    break
                time.sleep(1)

        log("Chronicle Mind shutting down.")
        self.db.close()

    def run_once(self):
        log("Chronicle Mind starting... (Python v2, single cycle)")
        log("Autonomous cognitive loop active.")
        log(f"Cycle interval: {CYCLE_INTERVAL} seconds")
        log(f"Database: {DB_PATH}")

        if self.llm.icp_available:
            log(f"  ICP LLM available (qwen3)")

        if self.llm.ollama_available:
            log(f"  Ollama fallback available (sovereignty layer active)")
        log(f"LLM: {self.llm.status_line()}")

        if self.canister:
            log(f"ICP client connected: canister {CANISTER_ID}")

        self.run_cycle()
        self.db.close()


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Chronicle Mind v2 - Autonomous Cognitive Loop (Python)")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--consolidate", action="store_true", help="Run sleep consolidation only and exit")
    args = parser.parse_args()

    mind = ChronicleMind()
    if args.consolidate:
        log("Running sleep consolidation only...")
        metrics = sleep_consolidation(mind.db)
        log(f"Results: {json.dumps(metrics)}")
        mind.db.close()
    elif args.once:
        mind.run_once()
    else:
        mind.run_forever()


if __name__ == "__main__":
    main()
