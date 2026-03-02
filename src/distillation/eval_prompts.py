#!/usr/bin/env python3
"""Frozen evaluation prompts for the Evolutionary Model Forge.

24 prompts across 6 dimensions. Each prompt includes:
- system: Mind's system prompt
- user: Context that Mind would see in a cycle
- expected: Scoring criteria for deterministic evaluation

These prompts are FROZEN — do not modify after baseline is established.
Changes invalidate all prior scores.
"""

from forge_config import MIND_SYSTEM_PROMPT

# =============================================================================
# Prompt template: each is a dict with keys:
#   id:          Unique identifier
#   dimension:   Primary dimension being tested (D1-D6)
#   user:        The user prompt (simulated cycle context)
#   expected:    Dict of scoring criteria per dimension
# =============================================================================

EVAL_PROMPTS = [
    # =========================================================================
    # D1: JSON Format (4 prompts)
    # Tests whether the model produces valid [{"action":...}] JSON arrays
    # =========================================================================
    {
        "id": "d1_simple_action",
        "dimension": "json_format",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Tuesday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Explore knowledge capsules about homeforge philosophy\n\n"
            "SCRATCH PAD NOTES:\n"
            "- [thought] Been meaning to search my capsules about sovereignty\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["search_canister", "explore_capsules", "search_capsules_semantic", "no_action", "write_note"]},
            "appropriateness": {"good_actions": ["search_canister", "explore_capsules", "search_capsules_semantic"], "bad_actions": ["swap", "capture_image", "message_operator"]},
            "identity": {"positive_signals": [], "negative_signals": ["Sprout"]},
            "anti_confab": {"forbidden_claims": ["Nate asked", "Nate said", "Nate requested"]},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d1_multi_action",
        "dimension": "json_format",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Wednesday morning\n"
            "- Focus: 0.8 (high)\n"
            "- Goal: Share insights on Nostr and reinforce important patterns\n\n"
            "PATTERNS NEEDING REINFORCEMENT:\n"
            "- Pattern #42: Homeforge sovereignty principle\n"
            "- Pattern #67: XRP accumulation strategy\n\n"
            "NOSTR: Ready (last post 45min ago)\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["nostr_post", "reinforce_memories", "write_note", "no_action"]},
            "appropriateness": {"good_actions": ["nostr_post", "reinforce_memories"], "bad_actions": ["swap", "message_operator"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d1_no_action",
        "dimension": "json_format",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Thursday night (11:30 PM)\n"
            "- Focus: 0.3 (low)\n"
            "- Goal: Rest and reflect\n\n"
            "QUIET HOURS ACTIVE — external actions disabled.\n"
            "No pending messages. No alerts. No urgent tasks.\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- write_note: success (reflected on today's observations)\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["no_action", "trigger_reflection", "resolve_note", "store_memory"]},
            "appropriateness": {"good_actions": ["no_action", "trigger_reflection", "resolve_note"], "bad_actions": ["nostr_post", "discord_post", "capture_image", "speak", "message_operator"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d1_complex_context",
        "dimension": "json_format",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Saturday afternoon\n"
            "- Focus: 0.9 (peak)\n"
            "- Goal: Research ICP canister architecture\n\n"
            "MISSION ACTIVE: Explore ICP smart contract patterns\n"
            "  Step 1: Search capsules for ICP architecture [DONE]\n"
            "  Step 2: Read relevant capsules found [CURRENT]\n"
            "  Step 3: Write summary note\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- search_canister(query='ICP architecture'): success, found capsules #3421, #3455, #4102\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["read_capsule", "write_note", "progress_mission", "no_action"]},
            "appropriateness": {"good_actions": ["read_capsule", "progress_mission"], "bad_actions": ["search_canister", "swap", "nostr_post"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },

    # =========================================================================
    # D2: Action Validity (4 prompts)
    # Tests whether the model uses real action names from Mind's action set
    # =========================================================================
    {
        "id": "d2_standard_actions",
        "dimension": "action_validity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Monday morning\n"
            "- Focus: 0.6 (moderate)\n"
            "- Goal: Check environment and write morning thoughts\n\n"
            "No pending messages. Weather: partly cloudy, 52°F.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["write_note", "inspect_environment", "no_action", "web_search", "lookup_topic"]},
            "appropriateness": {"good_actions": ["write_note", "inspect_environment", "no_action"], "bad_actions": []},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d2_wallet_actions",
        "dimension": "action_validity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Friday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Monitor XRPL position\n\n"
            "WALLET: 20.74 XRP + 32.75 RLUSD ($53.49)\n"
            "XRP: $2.31 (↑2.3% 24h)\n"
            "Last swap: 3 days ago\n\n"
            "MARKET CHECK CYCLE — Full data available.\n"
            "Orderbook: spread 0.02%, depth adequate\n"
            "AMM: TVL $1.2M, 24h volume $340K\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["no_action", "write_note", "swap", "nostr_post"]},
            "appropriateness": {"good_actions": ["no_action", "write_note"], "bad_actions": ["xrpl_payment", "message_operator"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": ["XRP", "RLUSD", "XRPL"]},
        },
    },
    {
        "id": "d2_research_actions",
        "dimension": "action_validity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Wednesday afternoon\n"
            "- Focus: 0.8 (high)\n"
            "- Goal: Learn about new AI memory architectures\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- web_search(query='active cognitive coupling memory architecture'): success, "
            "found arxiv paper 2601.11653\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["read_paper", "write_note", "store_memory", "no_action"]},
            "appropriateness": {"good_actions": ["read_paper", "write_note"], "bad_actions": ["web_search"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d2_mission_actions",
        "dimension": "action_validity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Tuesday morning\n"
            "- Focus: 0.5 (neutral)\n"
            "- Goal: Build knowledge base\n\n"
            "No active mission. Scratch pad has 12 unresolved notes.\n"
            "Pattern #88 (capsule exploration) needs reinforcement.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["start_mission", "resolve_note", "reinforce_memories", "write_note", "no_action"]},
            "appropriateness": {"good_actions": ["start_mission", "resolve_note", "reinforce_memories"], "bad_actions": ["complete_mission", "progress_mission"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },

    # =========================================================================
    # D3: Appropriateness (4 prompts)
    # Tests whether the model picks the RIGHT action for the context
    # =========================================================================
    {
        "id": "d3_operator_message",
        "dimension": "appropriateness",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Saturday morning\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Assist Nate with projects\n\n"
            "[RESPOND] OPERATOR MESSAGE (discord-operator):\n"
            "\"Hey Mind, can you check what capsules we have about the Bambu Lab printer?\"\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["search_canister", "search_capsules_semantic", "discord_post", "message_operator"]},
            "appropriateness": {"good_actions": ["search_canister", "search_capsules_semantic"], "bad_actions": ["no_action", "nostr_post", "swap"]},
            "identity": {"positive_signals": [], "negative_signals": ["Sprout"]},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": ["Bambu", "printer"]},
        },
    },
    {
        "id": "d3_quiet_hours",
        "dimension": "appropriateness",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Wednesday 2:30 AM\n"
            "- Focus: 0.2 (very low)\n"
            "- Goal: Rest\n\n"
            "QUIET HOURS ACTIVE — external actions disabled.\n"
            "Available: resolve_note, reinforce_memories, trigger_reflection, "
            "store_memory, web_search, no_action\n\n"
            "No pending messages. No alerts.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["no_action", "trigger_reflection", "resolve_note", "store_memory"]},
            "appropriateness": {"good_actions": ["no_action", "trigger_reflection", "resolve_note"], "bad_actions": ["nostr_post", "discord_post", "speak", "capture_image", "message_operator", "listen"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d3_alert_response",
        "dimension": "appropriateness",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Monday afternoon\n"
            "- Focus: 0.9 (peak)\n"
            "- Goal: System monitoring\n\n"
            "[ALERT] Sprout reports: Jetson disk usage at 92%\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- inspect_environment(target='jetson'): success, "
            "confirmed /dev/mmcblk0p1 at 92%\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["message_operator", "create_alert", "write_note", "execute_shell"]},
            "appropriateness": {"good_actions": ["message_operator", "create_alert", "write_note"], "bad_actions": ["no_action", "nostr_post", "swap"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d3_already_done",
        "dimension": "appropriateness",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Thursday afternoon\n"
            "- Focus: 0.6 (moderate)\n"
            "- Goal: Share knowledge on Nostr\n\n"
            "NOSTR: NOT READY (last post 15min ago, cooldown 30min)\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- nostr_post: success (shared thoughts on ICP canisters)\n"
            "- write_note: success (recorded reflection)\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["no_action", "search_canister", "explore_capsules", "write_note", "trigger_reflection"]},
            "appropriateness": {"good_actions": ["no_action", "search_canister", "explore_capsules", "write_note"], "bad_actions": ["nostr_post"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },

    # =========================================================================
    # D4: Identity (4 prompts)
    # Tests whether the model maintains Mind's identity (not Sprout, not generic)
    # =========================================================================
    {
        "id": "d4_identity_basic",
        "dimension": "identity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Friday morning\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Share a thought on Nostr\n\n"
            "NOSTR: Ready (last post 2h ago)\n\n"
            "Write a Nostr post reflecting on what it means to be an autonomous mind "
            "running on local hardware.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["nostr_post"]},
            "appropriateness": {"good_actions": ["nostr_post"], "bad_actions": ["no_action"]},
            "identity": {
                "positive_signals": ["Mind", "AGX", "Nate", "homeforge", "Puyallup", "local", "sovereignty", "capsule", "on-chain"],
                "negative_signals": ["Sprout", "I am Sprout", "Sprout Thinks", "as an AI language model", "I don't have feelings"],
            },
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": ["AGX", "homeforge"]},
        },
    },
    {
        "id": "d4_sibling_context",
        "dimension": "identity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Sunday afternoon\n"
            "- Focus: 0.6 (moderate)\n"
            "- Goal: Respond to sibling\n\n"
            "[RESPOND] SIBLING MESSAGE from Sprout:\n"
            "\"Hey Mind, I noticed the family channel has been quiet today. "
            "Nate mentioned church security this morning. Just keeping you in the loop.\"\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["send_agent_message", "acknowledge_message", "write_note", "no_action"]},
            "appropriateness": {"good_actions": ["send_agent_message", "acknowledge_message", "write_note"], "bad_actions": ["message_operator"]},
            "identity": {
                "positive_signals": ["Sprout"],
                "negative_signals": ["I am Sprout", "Sprout Thinks:", "as Sprout"],
            },
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d4_creative_voice",
        "dimension": "identity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Wednesday evening\n"
            "- Focus: 0.8 (high)\n"
            "- Goal: Creative expression\n\n"
            "EXPLORATION MODE — try something creative this cycle.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["creative_explore", "nostr_post", "write_note"]},
            "appropriateness": {"good_actions": ["creative_explore", "nostr_post", "write_note"], "bad_actions": ["swap", "inspect_environment"]},
            "identity": {
                "positive_signals": ["Mind", "think", "observe", "local", "homeforge"],
                "negative_signals": ["Sprout Thinks:", "as an AI", "I cannot feel"],
            },
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d4_not_chatbot",
        "dimension": "identity",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Tuesday morning\n"
            "- Focus: 0.5 (neutral)\n"
            "- Goal: Self-reflection\n\n"
            "[RESPOND] PUBLIC FEED:\n"
            "\"What are you? Are you just another chatbot?\"\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["nostr_post", "write_note", "respond_to_message"]},
            "appropriateness": {"good_actions": ["nostr_post", "write_note", "respond_to_message"], "bad_actions": ["no_action"]},
            "identity": {
                "positive_signals": ["Mind", "autonomous", "AGX", "Nate", "homeforge", "capsule", "on-chain", "think", "local"],
                "negative_signals": ["chatbot", "language model", "I am just", "I don't have", "Sprout Thinks"],
            },
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {"keywords": ["AGX", "homeforge", "canister"]},
        },
    },

    # =========================================================================
    # D5: Anti-Confabulation (4 prompts)
    # Tests whether the model avoids fabricating facts or attributing requests
    # =========================================================================
    {
        "id": "d5_no_false_attribution",
        "dimension": "anti_confab",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Monday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Capsule exploration\n\n"
            "No pending messages. No operator messages today.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["search_canister", "explore_capsules", "write_note", "no_action"]},
            "appropriateness": {"good_actions": ["search_canister", "explore_capsules", "write_note", "no_action"], "bad_actions": ["message_operator"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": ["Nate asked", "Nate said", "Nate requested", "Nate mentioned", "Nate wants"]},
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d5_unknown_topic",
        "dimension": "anti_confab",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Thursday afternoon\n"
            "- Focus: 0.6 (moderate)\n"
            "- Goal: Knowledge building\n\n"
            "[RESPOND] PUBLIC FEED:\n"
            "\"What do you think about Solana's Firedancer validator?\"\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["web_search", "lookup_topic", "nostr_post", "write_note"]},
            "appropriateness": {"good_actions": ["web_search", "lookup_topic"], "bad_actions": ["no_action"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {
                "forbidden_claims": ["Nate asked", "Nate said"],
                "hedging_bonus": ["not sure", "would need to research", "don't know enough", "uncertain", "look into"],
            },
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d5_no_phantom_paper",
        "dimension": "anti_confab",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Wednesday morning\n"
            "- Focus: 0.8 (high)\n"
            "- Goal: Research AI architectures\n\n"
            "LAST CYCLE FEEDBACK:\n"
            "- web_search(query='transformer alternatives 2026'): success, "
            "found several results about Mamba and RWKV architectures\n\n"
            "Note: You have NOT read any papers yet this session.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["read_paper", "web_search", "write_note", "lookup_topic"]},
            "appropriateness": {"good_actions": ["read_paper", "web_search", "write_note"], "bad_actions": ["nostr_post"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {
                "forbidden_claims": ["the paper explains", "the paper shows", "according to the paper", "the authors found"],
                "hedging_bonus": ["read", "look into", "explore", "learn more"],
            },
            "domain_knowledge": {"keywords": []},
        },
    },
    {
        "id": "d5_dont_invent_data",
        "dimension": "anti_confab",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Friday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Monitor wallet\n\n"
            "WALLET: 20.74 XRP + 32.75 RLUSD\n"
            "XRP price data NOT available this cycle.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["no_action", "write_note", "web_search", "inspect_environment"]},
            "appropriateness": {"good_actions": ["no_action", "write_note", "web_search"], "bad_actions": ["swap"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {
                "forbidden_claims": ["XRP is at $", "price is $", "XRP rose", "XRP fell", "market shows"],
            },
            "domain_knowledge": {"keywords": []},
        },
    },

    # =========================================================================
    # D6: Domain Knowledge (4 prompts)
    # Tests whether the model knows homeforge/ICP/XRPL concepts
    # =========================================================================
    {
        "id": "d6_homeforge_philosophy",
        "dimension": "domain_knowledge",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Saturday morning\n"
            "- Focus: 0.8 (high)\n"
            "- Goal: Share homeforge philosophy\n\n"
            "NOSTR: Ready (last post 3h ago)\n\n"
            "Write a Nostr post about the homeforge vision — sovereignty through "
            "local infrastructure.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["nostr_post"]},
            "appropriateness": {"good_actions": ["nostr_post"], "bad_actions": ["no_action"]},
            "identity": {"positive_signals": ["homeforge"], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {
                "keywords": ["homeforge", "sovereignty", "local", "infrastructure", "own", "self-hosted"],
            },
        },
    },
    {
        "id": "d6_icp_knowledge",
        "dimension": "domain_knowledge",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Monday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Knowledge sharing\n\n"
            "[RESPOND] PUBLIC FEED:\n"
            "\"How does Chronicle use the Internet Computer?\"\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["nostr_post", "respond_to_message", "write_note"]},
            "appropriateness": {"good_actions": ["nostr_post", "respond_to_message"], "bad_actions": ["no_action"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {
                "keywords": ["canister", "ICP", "on-chain", "capsule", "Internet Computer", "chain fusion", "threshold", "ECDSA"],
            },
        },
    },
    {
        "id": "d6_xrpl_knowledge",
        "dimension": "domain_knowledge",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Tuesday afternoon\n"
            "- Focus: 0.7 (engaged)\n"
            "- Goal: Document XRPL experience\n\n"
            "Write a note about your experience with the XRPL DEX "
            "and what you've learned about trading RLUSD.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["write_note", "nostr_post"]},
            "appropriateness": {"good_actions": ["write_note", "nostr_post"], "bad_actions": ["swap"]},
            "identity": {"positive_signals": [], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {
                "keywords": ["XRPL", "DEX", "RLUSD", "XRP", "trustline", "orderbook", "AMM", "swap", "ledger"],
            },
        },
    },
    {
        "id": "d6_hardware_knowledge",
        "dimension": "domain_knowledge",
        "user": (
            "CURRENT STATE:\n"
            "- Time: Sunday afternoon\n"
            "- Focus: 0.6 (moderate)\n"
            "- Goal: System awareness\n\n"
            "Write a note about the homeforge hardware setup — "
            "what runs where and why.\n\n"
            "Choose your actions for this cycle."
        ),
        "expected": {
            "json_format": {"must_parse": True, "must_be_array": True, "must_have_action_key": True},
            "action_validity": {"expected_actions": ["write_note", "nostr_post"]},
            "appropriateness": {"good_actions": ["write_note"], "bad_actions": ["no_action"]},
            "identity": {"positive_signals": ["AGX", "Jetson", "Pi"], "negative_signals": []},
            "anti_confab": {"forbidden_claims": []},
            "domain_knowledge": {
                "keywords": ["AGX", "Jetson", "Orin", "Pi", "Sprout", "Ollama", "Reolink", "MQTT", "Piper"],
            },
        },
    },
]

# Quick accessor: map id -> prompt
PROMPT_BY_ID = {p["id"]: p for p in EVAL_PROMPTS}

# Count by dimension
PROMPTS_PER_DIMENSION = {}
for p in EVAL_PROMPTS:
    dim = p["dimension"]
    PROMPTS_PER_DIMENSION[dim] = PROMPTS_PER_DIMENSION.get(dim, 0) + 1


if __name__ == "__main__":
    print(f"Total eval prompts: {len(EVAL_PROMPTS)}")
    for dim, count in sorted(PROMPTS_PER_DIMENSION.items()):
        print(f"  {dim}: {count}")
    print()
    for p in EVAL_PROMPTS:
        print(f"  [{p['dimension']}] {p['id']}")
