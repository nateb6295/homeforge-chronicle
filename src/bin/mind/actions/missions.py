"""Chronicle Mind - Mission action handlers.

Missions are scoped multi-cycle objectives (5-15 cycles) with ordered steps.
They sit between goals (abstract) and tasks (single-cycle) in the hierarchy.

Stored as JSON in scratch_pad.content with category='mission'.
Only ONE active mission at a time.
Authority: p7 = Mind-created (can abandon), p9 = operator-created (cannot abandon).
"""

import json
from typing import Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import MISSION_MAX_STEPS, MISSION_MAX_CYCLES


def _get_active_mission(mind) -> Optional[dict]:
    """Load the single active mission, if any."""
    row = mind.db.query_one(
        "SELECT id, content, priority, source FROM scratch_pad "
        "WHERE category = 'mission' AND resolved = 0 "
        "ORDER BY priority DESC, created_at DESC LIMIT 1"
    )
    if not row:
        return None
    try:
        mission = json.loads(row["content"])
        mission["_db_id"] = row["id"]
        mission["_priority"] = row.get("priority", 7)
        return mission
    except (json.JSONDecodeError, TypeError):
        return None


def _save_mission(mind, db_id: int, mission: dict):
    """Update the mission JSON in scratch_pad."""
    # Strip internal keys before saving
    clean = {k: v for k, v in mission.items() if not k.startswith("_")}
    mind.db.run(
        "UPDATE scratch_pad SET content = ?, updated_at = ? WHERE id = ?",
        (json.dumps(clean), now_ts(), db_id),
    )


_STOP_WORDS = {
    "your", "this", "that", "with", "from", "about", "what", "when",
    "them", "then", "they", "their", "will", "have", "been", "some",
    "into", "also", "more", "make", "like", "just", "over", "such",
    "take", "each", "which", "does", "most", "only", "find", "here",
    "thing", "many", "well", "back", "after", "should", "could", "would",
    "these", "those", "being", "using", "related", "current", "recent",
}


def _actions_match_step(action_names: list, results_text: str, step_text: str) -> bool:
    """Keyword heuristic: do this cycle's actions relate to the current mission step?
    Conservative — only matches on substantive keywords, not common words.
    no_action cycles are never mission-relevant (their results parrot step text)."""
    # no_action is by definition not mission-relevant
    real_actions = [n for n in action_names if n != "no_action"]
    if not real_actions:
        return False

    step_lower = step_text.lower()
    # Extract meaningful words (>4 chars, not stopwords)
    step_words = [w for w in step_lower.split() if len(w) > 4 and w not in _STOP_WORDS]
    if len(step_words) < 2:
        return False

    combined = " ".join(real_actions).lower() + " " + results_text.lower()
    hits = sum(1 for w in step_words if w in combined)
    # Need at least 2 keyword matches AND 50% of step words
    return hits >= 2 and (hits / len(step_words) >= 0.5)


def act_start_mission(mind, action: dict, cid: str) -> str:
    """Create a new mission from title + steps list."""
    title = action.get("title", "")
    steps_raw = action.get("steps", [])
    log(f'  Executing: StartMission {{ title: "{safe_truncate(title, 60)}", steps: {len(steps_raw)} }}')

    if not title:
        return "false - Missing 'title' for mission"
    if not steps_raw or not isinstance(steps_raw, list):
        return "false - Missing 'steps' (must be a list of step descriptions)"
    if len(steps_raw) < 2:
        return "false - Mission needs at least 2 steps"
    if len(steps_raw) > MISSION_MAX_STEPS:
        return f"false - Too many steps (max {MISSION_MAX_STEPS})"

    # Check for existing active mission
    existing = _get_active_mission(mind)
    if existing:
        return f"false - Mission already active: \"{safe_truncate(existing.get('title', ''), 40)}\". Complete or abandon it first."

    steps = []
    for i, s in enumerate(steps_raw, 1):
        text = s if isinstance(s, str) else str(s)
        steps.append({"id": i, "action": text, "done": False})

    mission = {
        "title": title,
        "steps": steps,
        "current_step": 1,
        "focus_strength": 1.0,
        "cycles_active": 0,
        "cycles_no_progress": 0,
        "max_cycles": MISSION_MAX_CYCLES,
        "created_by": "mind",
        "causal_root": cid,
    }

    ts = now_ts()
    mind.db.run(
        "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
        "VALUES (?, 'mission', 7, 0, ?, ?)",
        (json.dumps(mission), ts, ts),
    )
    log(f"  Mission created: \"{title}\" with {len(steps)} steps")
    return f"true - Mission started: \"{safe_truncate(title, 50)}\" ({len(steps)} steps)"


def act_progress_mission(mind, action: dict, cid: str) -> str:
    """Mark current step done and advance to next."""
    result_text = action.get("result", "") or action.get("reason", "")
    log(f'  Executing: ProgressMission {{ result: "{safe_truncate(result_text, 80)}" }}')

    mission = _get_active_mission(mind)
    if not mission:
        return "false - No active mission to progress"

    db_id = mission["_db_id"]
    current = mission.get("current_step", 1)
    steps = mission.get("steps", [])

    # Mark current step done and store its result for next step context
    for step in steps:
        if step["id"] == current:
            step["done"] = True
            if result_text:
                step["result"] = result_text[:200]
            break

    # Check if all steps done
    all_done = all(s["done"] for s in steps)
    if all_done:
        mission["current_step"] = len(steps)
        _save_mission(mind, db_id, mission)
        # Auto-complete
        mind.db.run(
            "UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?",
            (now_ts(), db_id),
        )
        title = mission.get("title", "")
        log(f"  Mission auto-completed: \"{title}\" (all {len(steps)} steps done)")
        return f"true - Mission complete! All {len(steps)} steps of \"{safe_truncate(title, 40)}\" done."

    # Advance to next step
    mission["current_step"] = current + 1
    # Focus boost
    focus = min(1.0, mission.get("focus_strength", 1.0) + 0.15)
    mission["focus_strength"] = round(focus, 2)
    mission["cycles_no_progress"] = 0
    _save_mission(mind, db_id, mission)

    next_step = None
    for s in steps:
        if s["id"] == current + 1:
            next_step = s
            break

    next_desc = safe_truncate(next_step["action"], 60) if next_step else "?"
    done_count = sum(1 for s in steps if s["done"])
    log(f"  Mission step {current}/{len(steps)} done. Next: {next_desc}")
    return f"true - Step {current} done ({done_count}/{len(steps)}). Next: {next_desc}"


def act_complete_mission(mind, action: dict, cid: str) -> str:
    """Mark mission complete with summary."""
    summary = action.get("summary", "") or action.get("reason", "")
    log(f'  Executing: CompleteMission {{ summary: "{safe_truncate(summary, 80)}" }}')

    mission = _get_active_mission(mind)
    if not mission:
        return "false - No active mission to complete"

    db_id = mission["_db_id"]
    title = mission.get("title", "")
    steps = mission.get("steps", [])
    done_count = sum(1 for s in steps if s["done"])

    # Resolve
    mind.db.run(
        "UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?",
        (now_ts(), db_id),
    )
    log(f"  Mission completed: \"{title}\" ({done_count}/{len(steps)} steps done)")
    return f"true - Mission \"{safe_truncate(title, 40)}\" completed ({done_count}/{len(steps)} steps). {safe_truncate(summary, 60)}"


def act_abandon_mission(mind, action: dict, cid: str) -> str:
    """Abandon active mission with reason. Blocked for p>=9 (operator missions)."""
    reason = action.get("reason", "")
    log(f'  Executing: AbandonMission {{ reason: "{safe_truncate(reason, 80)}" }}')

    mission = _get_active_mission(mind)
    if not mission:
        return "false - No active mission to abandon"

    db_id = mission["_db_id"]
    priority = mission.get("_priority", 7)
    title = mission.get("title", "")

    if priority >= 9:
        log(f"  BLOCKED: Cannot abandon operator mission (p{priority})")
        return f"false - Cannot abandon operator-assigned mission \"{safe_truncate(title, 40)}\" (p{priority})"

    mind.db.run(
        "UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?",
        (now_ts(), db_id),
    )
    log(f"  Mission abandoned: \"{title}\" — {reason}")
    return f"true - Mission \"{safe_truncate(title, 40)}\" abandoned. {safe_truncate(reason, 60)}"
