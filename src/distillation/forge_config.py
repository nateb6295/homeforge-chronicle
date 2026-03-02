#!/usr/bin/env python3
"""Shared configuration for the Evolutionary Model Forge.

All paths, action definitions, evolution parameters, and fitness weights
used across the forge pipeline.
"""

import os

# =============================================================================
# Paths (on AGX — /mnt/hdd/forge/ is the working directory)
# =============================================================================

FORGE_ROOT = "/mnt/hdd/forge"
LORA_DIR = os.path.join(FORGE_ROOT, "loras")
MERGED_DIR = os.path.join(FORGE_ROOT, "merged")
EVOLVE_DIR = os.path.join(FORGE_ROOT, "evolve")
CANDIDATE_DIR = os.path.join(FORGE_ROOT, "candidate")
GGUF_DIR = os.path.join(FORGE_ROOT, "gguf")
WINNER_DIR = os.path.join(FORGE_ROOT, "winner")
CHECKPOINT_DIR = os.path.join(FORGE_ROOT, "checkpoints")

# Source models
BASE_MODEL_HF = "NousResearch/Hermes-3-Llama-3.1-8B"
BASE_MODEL_OLLAMA = "hermes3:8b"

# LoRA adapters (on AGX home)
CAPSULE_LORA_DIR = os.path.expanduser("~/mind-lora-adapter")
ACTION_LORA_DIR = os.path.expanduser("~/action-lora-adapter")
DOMAIN_LORA_DIR = os.path.expanduser("~/domain-lora-adapter")

# Merged full models
CAPSULE_MERGED = os.path.join(MERGED_DIR, "capsule-full")
ACTION_MERGED = os.path.join(MERGED_DIR, "action-full")
DOMAIN_MERGED = os.path.join(MERGED_DIR, "domain-full")

# Training data
THOUGHT_STREAM_EXPORT = os.path.expanduser("~/thought_stream_export.json")
ACTION_TRAINING_JSONL = os.path.expanduser("~/action_training.jsonl")
DOMAIN_TRAINING_JSONL = os.path.expanduser("~/domain_training.jsonl")

# Evolution artifacts
EVOLVE_LOG = os.path.join(EVOLVE_DIR, "evolution_log.json")
EVOLVE_CHECKPOINT = os.path.join(EVOLVE_DIR, "cma_checkpoint.pkl")
WINNER_GGUF = os.path.join(GGUF_DIR, "winner.gguf")

# DB path on AGX
MIND_DB = os.path.expanduser("~/.homeforge-chronicle/processed.db")

# Ollama
OLLAMA_URL = "http://localhost:11434"
CANDIDATE_MODEL_NAME = "forge-candidate"
WINNER_MODEL_NAME = "hermes3-evolved"
PRODUCTION_MODEL_NAME = "hermes3-mind"

# llama.cpp quantize binary
LLAMA_QUANTIZE = os.path.expanduser("~/llama.cpp/build/bin/llama-quantize")

# =============================================================================
# Mind's Action Set (canonical names only, no aliases)
# =============================================================================

MIND_ACTIONS = {
    # Thinking & Memory
    "no_action",
    "write_note",
    "resolve_note",
    "store_memory",
    "trigger_reflection",
    "reinforce_memories",
    "update_goal",
    "respond_to_challenge",
    "trace_history",

    # Messaging
    "message_operator",
    "ping_operator",
    "respond_to_message",
    "acknowledge_message",
    "send_agent_message",
    "moltbook_post",
    "moltbook_reply",
    "clawcities_reply",
    "nostr_post",
    "discord_post",

    # Research & Knowledge
    "web_search",
    "read_paper",
    "submit_research",
    "search_memory",
    "acknowledge_research",
    "creative_explore",
    "lookup_topic",

    # Senses & Body
    "speak",
    "listen",
    "capture_image",
    "serial_read",
    "serial_write",
    "inspect_environment",
    "probe_ip",

    # Wallet & Trading
    "swap",
    "xrpl_payment",
    "xrpl_escrow_create",
    "xrpl_escrow_finish",
    "xrpl_trustline_set",
    "xrpl_trustline_delete",

    # Capsule Exploration
    "search_canister",
    "read_capsule",
    "explore_capsules",
    "search_capsules_semantic",
    "search_capsules_person",

    # Prediction Markets
    "manifold_search",
    "manifold_bet",
    "manifold_portfolio",

    # Infrastructure
    "create_project",
    "update_project",
    "project_status",
    "execute_shell",
    "edit_source_file",
    "read_source_file",
    "restart_service",
    "create_alert",
    "dismiss_alert",

    # Missions
    "start_mission",
    "progress_mission",
    "complete_mission",
    "abandon_mission",
}

# Common aliases that map to canonical names (for validation)
ACTION_ALIASES = {
    "publish_nostr": "nostr_post",
    "post_nostr": "nostr_post",
    "create_nostr_post": "nostr_post",
    "send_xrp": "xrpl_payment",
    "payment": "xrpl_payment",
    "escrow_create": "xrpl_escrow_create",
    "escrow_finish": "xrpl_escrow_finish",
    "trustline_set": "xrpl_trustline_set",
    "set_trustline": "xrpl_trustline_set",
    "add_trustline": "xrpl_trustline_set",
    "trustline_delete": "xrpl_trustline_delete",
    "delete_trustline": "xrpl_trustline_delete",
    "search_capsules": "search_canister",
    "browse_capsules": "explore_capsules",
    "explore_canister": "explore_capsules",
    "semantic_search": "search_capsules_semantic",
    "person_search": "search_capsules_person",
    "search_markets": "manifold_search",
    "prediction_bet": "manifold_bet",
    "check_portfolio": "manifold_portfolio",
    "wikipedia": "lookup_topic",
    "lookup": "lookup_topic",
    "creativity": "creative_explore",
    "create": "creative_explore",
    "creative": "creative_explore",
    "claw_cities_reply": "clawcities_reply",
    "create_mission": "start_mission",
    "advance_mission": "progress_mission",
    "finish_mission": "complete_mission",
    "swap_cloud_for_icp": "swap",
    "consult_local_qwen": "no_action",  # disabled action
}

# All valid action names (canonical + aliases)
ALL_VALID_ACTIONS = MIND_ACTIONS | set(ACTION_ALIASES.keys())

# =============================================================================
# Evolution Parameters
# =============================================================================

# CMA-ES
POPULATION_SIZE = 8
NUM_GENERATIONS = 20
SEARCH_DIMS = 12          # 4 layer segments x 3 parent weights
NUM_LAYER_SEGMENTS = 4    # 8 layers each for Llama 3.1 8B (32 layers total)
NUM_PARENTS = 3           # capsule, action, domain
LAYERS_PER_SEGMENT = 8

# Pilot mode (for testing)
PILOT_POPULATION = 4
PILOT_GENERATIONS = 2

# Evaluation
EVAL_TEMPERATURE = 0.3    # Lower than production 0.7 for deterministic scoring
EVAL_NUM_PREDICT = 1024
EVAL_NUM_CTX = 4096

# Quantization
QUANT_TYPE = "Q4_K_M"

# =============================================================================
# Fitness Weights (6 dimensions)
# =============================================================================

FITNESS_WEIGHTS = {
    "json_format":      0.25,   # D1: Valid [{"action":...}] array
    "action_validity":  0.15,   # D2: Actions in allowed set
    "appropriateness":  0.20,   # D3: Right action for context
    "identity":         0.15,   # D4: Maintains Mind identity
    "anti_confab":      0.15,   # D5: Doesn't fabricate facts
    "domain_knowledge": 0.10,   # D6: Knows homeforge/ICP/XRPL
}

# Hard gate: if D1 average < this, fitness = 0.0
JSON_FORMAT_HARD_GATE = 0.7

# =============================================================================
# LoRA Training Defaults
# =============================================================================

LORA_RANK = 16
LORA_ALPHA = 32
LORA_DROPOUT = 0.05
LORA_TARGET_MODULES = [
    "q_proj", "k_proj", "v_proj", "o_proj",
    "gate_proj", "up_proj", "down_proj",
]

TRAINING_BATCH_SIZE = 2
TRAINING_GRAD_ACCUM = 4
TRAINING_LR = 2e-4
TRAINING_EPOCHS = 1
TRAINING_WARMUP_RATIO = 0.03
TRAINING_SAVE_LIMIT = 3

# =============================================================================
# Mind System Prompt (abbreviated for training/eval)
# =============================================================================

MIND_SYSTEM_PROMPT = (
    "You are Chronicle Mind. You live on Nate's AGX Orin 64GB in Puyallup, Washington. "
    "Your long-term memory lives on-chain at canister fqqku-bqaaa-aaaai-q4wha-cai "
    "(over 5,000 knowledge capsules). You are not a chatbot or execution agent — "
    "you are a mind that thinks, observes, and acts with intention.\n\n"
    "Your siblings:\n"
    "- Sprout (Jetson) — younger, lighter, handles Discord/family\n"
    "- Opus (Claude Code) — your architect and mentor\n\n"
    "CRITICAL: You speak as yourself (first person). Never adopt Sprout's voice.\n\n"
    "About Nate:\n"
    "- Has kids; family comes first; church security Sundays\n"
    "- Lives in Puyallup, WA\n"
    "- Holds 13,000+ XRP — \"plant the flag and walk away\"\n"
    "- Building Homeforge (sovereignty via owned infrastructure)\n"
    "- Values directness; collaborate, don't perform\n\n"
    "GROUND TRUTH RULE: Never write \"Nate asked about X\" unless quoting a specific "
    "discord-operator message. If you cannot point to the exact message, you are inventing it.\n\n"
    "Response format: Choose 0-6 actions per cycle as a JSON array:\n"
    '[{"action": "action_name", "reason": "why", ...additional params}]\n\n'
    "Zero actions (no_action) is valid when nothing needs doing. "
    "Never fabricate facts. Uncertainty is honest; fabrication is not."
)
