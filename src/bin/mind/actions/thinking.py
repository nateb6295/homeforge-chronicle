"""Chronicle Mind - Thinking & memory action handlers."""

import json
from typing import Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import OPERATOR_PROTECTED_CATEGORIES
from mind.communication import send_ntfy


def act_no_action(mind, action: dict, cid: str) -> str:
    reason = action.get("reason", "Nothing urgent")
    log(f'  Executing: NoAction {{ reason: "{safe_truncate(reason, 80)}" }}')
    return f"true - {reason}"


def act_write_note(mind, action: dict, cid: str) -> str:
    content = action.get("content", "") or action.get("note", "") or action.get("text", "")
    category = action.get("category", "thought")
    # Reserve operator-protected categories (directive, task) — Mind can't create them
    if category in OPERATOR_PROTECTED_CATEGORIES:
        log(f"  Category guard: '{category}' downgraded to 'idea' (operator-only)")
        category = "idea"
    log(f'  Executing: WriteNote {{ content: "{safe_truncate(content, 80)}", category: "{category}" }}')
    # Anti-rumination: skip if a very similar note exists recently
    if mind.db.recent_note_similar(content, hours=24):
        log(f"  DEDUP: Similar note already exists, skipping")
        return f"false - Similar note already exists (anti-rumination)"
    note_id = mind.db.write_note(content, category)
    return f"true - Wrote note {note_id}: {safe_truncate(content, 60)}"


def act_resolve_note(mind, action: dict, cid: str) -> str:
    note_id = action.get("note_id", 0)
    log(f"  Executing: ResolveNote {{ note_id: {note_id} }}")
    # Protect operator-authority categories from Mind resolution
    note_row = mind.db.query_one(
        "SELECT category FROM scratch_pad WHERE id = ?", (note_id,)
    )
    if note_row and note_row.get("category") in OPERATOR_PROTECTED_CATEGORIES:
        log(f"  BLOCKED: Cannot resolve {note_row['category']} note #{note_id} (operator authority only)")
        return f"false - Cannot resolve {note_row['category']} notes (operator authority only)"
    mind.db.resolve_note(note_id)
    return f"true - Resolved note {note_id}"


def act_store_memory(mind, action: dict, cid: str) -> str:
    content = action.get("content", "") or action.get("memory", "") or action.get("text", "")
    topic = action.get("topic", "general")
    log(f'  Executing: StoreMemory {{ content: "{safe_truncate(content, 60)}", topic: "{topic}" }}')
    # Anti-rumination: skip if a very similar note/memory exists recently
    if mind.db.recent_note_similar(content, hours=24):
        log(f"  DEDUP: Similar memory already stored recently, skipping")
        return f"false - Similar memory already exists (anti-rumination)"
    if mind.canister and content:
        result = mind.canister.store(content, topic, ["chronicle-mind", topic])
        ok = "error" not in result
        return f"true - Memory noted (topic: {topic}): {safe_truncate(content, 60)}"
    return "false - No canister connection"


def act_trigger_reflection(mind, action: dict, cid: str) -> str:
    prompt = action.get("prompt", "") or action.get("reason", "") or action.get("content", "") or action.get("text", "")
    log(f'  Executing: TriggerReflection {{ prompt: "{safe_truncate(prompt, 80)}" }}')
    if not prompt:
        return "false - Missing prompt (provide 'prompt', 'reason', or 'content' key)"
    if mind.canister:
        result = mind.canister.store(prompt, "reflection", ["reflection", "deep-thought"])
        capsule_id = result.get("id", "?")
        send_ntfy("Chronicle: New Reflection")
        return f"true - Reflection written to canister (capsule {capsule_id}): {safe_truncate(prompt, 60)}"
    return "false - No canister connection (HTTP API unavailable)"


def act_reinforce_memories(mind, action: dict, cid: str) -> str:
    ids = action.get("pattern_ids", [])
    reason = action.get("reason", "")
    log(f"  Executing: ReinforceMemories {{ ids: {ids}, reason: \"{safe_truncate(reason, 60)}\" }}")
    reinforced = 0
    for pid in ids[:5]:
        # Skip patterns already at max confidence or reinforced in last 24h
        pat = mind.db.query_one(
            "SELECT confidence_score, last_seen FROM consolidation_patterns WHERE id = ?",
            (pid,),
        )
        if pat:
            if pat["confidence_score"] >= 1.0:
                log(f"    Pattern {pid}: already at max confidence, skipping")
                continue
            if pat.get("last_seen") and (now_ts() - pat["last_seen"]) < 86400:
                log(f"    Pattern {pid}: reinforced <24h ago, skipping")
                continue
        mind.db.run(
            "UPDATE consolidation_patterns SET confidence_score = MIN(1.0, confidence_score + 0.1), "
            "last_seen = ? WHERE id = ?",
            (now_ts(), pid),
        )
        reinforced += 1
    return f"true - Reinforced {reinforced}/{len(ids)} patterns (skipped {len(ids) - reinforced} already maxed/recent)"


def act_respond_to_challenge(mind, action: dict, cid: str) -> str:
    challenge_id = action.get("challenge_id", 0)
    response = action.get("response", action.get("content", ""))
    log(f"  Executing: RespondToChallenge {{ id: {challenge_id} }}")
    mind.db.run(
        "UPDATE creative_challenges SET response = ?, responded_at = ? WHERE id = ?",
        (response, now_ts(), challenge_id),
    )
    return f"true - Challenge {challenge_id} responded"


def act_trace_history(mind, action: dict, cid: str) -> str:
    query = action.get("query", "") or action.get("topic", "")
    log(f'  Executing: TraceHistory {{ query: "{safe_truncate(query, 60)}" }}')
    if not query:
        return "false - trace_history requires a 'query' field"
    if not hasattr(mind, "causal_graph") or mind.causal_graph is None:
        return "false - causal graph not initialized"
    chain = mind.causal_graph.find_topic_chain(query.split(), limit=8)
    if not chain:
        return f"true - No causal history found for '{query}'"
    summary = "; ".join(
        f"{c['cycle_id']}({c['edge_type']}): {c['context'][:60]}"
        for c in chain
    )
    return f"true - Causal chain ({len(chain)} links): {summary}"


def act_update_goal(mind, action: dict, cid: str) -> str:
    goal = action.get("goal", action.get("content", ""))
    log(f'  Executing: UpdateGoal {{ goal: "{safe_truncate(goal, 60)}" }}')
    # Check for active REDIRECT directive — block goal changes during redirect
    redirect_active = mind.db.query_one(
        "SELECT id FROM scratch_pad WHERE category = 'directive' AND resolved = 0 "
        "AND UPPER(content) LIKE 'REDIRECT%' LIMIT 1"
    )
    if redirect_active:
        log(f"  BLOCKED: Cannot update goal while REDIRECT directive active")
        return "false - Cannot update goal while operator REDIRECT is active"
    # Check for operator-planted goals (priority >= 9) — Mind can't override
    operator_goal = mind.db.query_one(
        "SELECT id, content FROM scratch_pad WHERE category = 'goal' AND resolved = 0 "
        "AND priority >= 9 LIMIT 1"
    )
    if operator_goal:
        log(f"  BLOCKED: Operator goal #{operator_goal['id']} (p>=9) cannot be overridden")
        return f"false - Operator goal active (p>=9), cannot override: {safe_truncate(operator_goal.get('content', ''), 40)}"
    # Resolve existing goals first (only keep one active goal) — only p<9 goals
    mind.db.run("UPDATE scratch_pad SET resolved = 1 WHERE category = 'goal' AND resolved = 0 AND priority < 9")
    ts = now_ts()
    mind.db.run(
        "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
        "VALUES (?, 'goal', 5, 0, ?, ?)",
        (goal, ts, ts),
    )
    return f"true - Goal updated: {safe_truncate(goal, 60)}"
