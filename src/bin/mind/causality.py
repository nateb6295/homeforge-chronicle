"""Chronicle Mind - Causal Memory Graph.

Deterministic causal edge extraction between cognitive cycles.
No LLM needed — all edges detected by Python code from ctx flags.

Table: causal_edges (source_id, target_id, edge_type, strength, context, created_at)
Column: thought_stream.trigger_tags (JSON list of trigger descriptors)
"""

import json
import hashlib
from typing import List, Dict, Tuple, Optional

from mind.utils import log, now_ts


class CausalGraph:
    """Builds and queries a causal edge graph over Mind's cognitive cycles."""

    def __init__(self, db):
        self.db = db
        self._prev_env_hash = ""

    # ── Schema ────────────────────────────────────────────────────

    def ensure_tables(self):
        """Create causal_edges table and add trigger_tags column to thought_stream."""
        self.db.run(
            "CREATE TABLE IF NOT EXISTS causal_edges ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  source_id TEXT NOT NULL,"
            "  target_id TEXT NOT NULL,"
            "  edge_type TEXT NOT NULL,"
            "  strength REAL DEFAULT 1.0,"
            "  context TEXT DEFAULT '',"
            "  created_at INTEGER NOT NULL"
            ")"
        )
        self.db.run(
            "CREATE INDEX IF NOT EXISTS idx_causal_source ON causal_edges(source_id)"
        )
        self.db.run(
            "CREATE INDEX IF NOT EXISTS idx_causal_target ON causal_edges(target_id)"
        )
        self.db.run(
            "CREATE INDEX IF NOT EXISTS idx_causal_type ON causal_edges(edge_type)"
        )
        # Add trigger_tags column to thought_stream (auto-migrate)
        try:
            cols = [r["name"] for r in self.db.query("PRAGMA table_info(thought_stream)")]
            if "trigger_tags" not in cols:
                self.db.run(
                    "ALTER TABLE thought_stream ADD COLUMN trigger_tags TEXT DEFAULT '[]'"
                )
                log("  Causal: added trigger_tags column to thought_stream")
        except Exception:
            pass

    # ── Edge Extraction (deterministic, no LLM) ──────────────────

    def extract_edges(self, cid: str, ctx: dict,
                      actions: List[Dict], results: List[Dict]
                      ) -> Tuple[List[tuple], List[str]]:
        """Extract causal edges after a cycle completes.

        Returns (edges, trigger_tags) where:
          edges = [(source_id, target_id, edge_type, strength, context), ...]
          trigger_tags = ["continued:prev_cid", "operator:discord-42", ...]
        """
        edges = []
        tags = []
        ts = now_ts()
        action_names = [a.get("action", "") for a in actions]

        # 1. continued — always link to previous cycle
        prev_cid = ctx.get("prev_cycle_id")
        if prev_cid:
            edges.append((prev_cid, cid, "continued", 1.0, "", ts))
            tags.append(f"continued:{prev_cid}")

        # 2. responded_to:operator — discord-operator messages in ctx
        discord_msgs = ctx.get("discord_operator_msgs", [])
        if discord_msgs:
            for dm in discord_msgs:
                dm_id = dm.get("id", "?")
                preview = (dm.get("content", "") or "")[:60]
                edges.append((
                    f"operator:discord-{dm_id}", cid,
                    "responded_to:operator", 1.0, preview, ts
                ))
                tags.append(f"operator:discord-{dm_id}")

        # 3. responded_to:feed — public feed items in ctx
        feed_items = ctx.get("public_feed", [])
        if feed_items:
            for item in feed_items:
                fid = item.get("id", "?")
                edges.append((
                    f"feed:{fid}", cid,
                    "responded_to:feed", 1.0, "", ts
                ))
                tags.append(f"feed:{fid}")

        # 4. responded_to:sibling — sibling messages + respond action
        siblings = ctx.get("sibling_messages", [])
        if siblings:
            has_respond = any(
                a in ("respond_to_message", "acknowledge_message")
                for a in action_names
            )
            if has_respond:
                for sm in siblings:
                    sm_id = sm.get("id", "?")
                    edges.append((
                        f"sibling:{sm_id}", cid,
                        "responded_to:sibling", 1.0, "", ts
                    ))
                    tags.append(f"sibling:{sm_id}")

        # 5. triggered_by:alert — active alerts in ctx
        alerts = ctx.get("alerts", [])
        if alerts:
            for a in alerts:
                aname = a.get("name", "?")
                edges.append((
                    f"alert:{aname}", cid,
                    "triggered_by:alert", 1.0, aname, ts
                ))
                tags.append(f"alert:{aname}")

        # 6. triggered_by:mentor — opus guidance in ctx
        opus_notes = ctx.get("opus_guidance_notes", [])
        if opus_notes:
            for note in opus_notes:
                nid = note.get("id", "?")
                preview = (note.get("content", "") or "")[:60]
                edges.append((
                    f"mentor:{nid}", cid,
                    "triggered_by:mentor", 1.0, preview, ts
                ))
                tags.append(f"mentor:{nid}")

        # 7. inspired_by:memory — memory echo capsule in ctx
        echo = ctx.get("memory_echo", {})
        if echo and echo.get("id"):
            cap_id = echo["id"]
            edges.append((
                f"capsule:{cap_id}", cid,
                "inspired_by:memory", 0.7, "", ts
            ))
            tags.append(f"memory_echo:{cap_id}")

        # 8. recalled:goal — auto-recalled capsules from memory_context
        memory_ctx = ctx.get("memory_context", [])
        if memory_ctx:
            for mc in memory_ctx:
                mc_id = mc.get("id", "?")
                edges.append((
                    f"capsule:{mc_id}", cid,
                    "recalled:goal", 0.8, "", ts
                ))
            tags.append(f"recalled:{len(memory_ctx)}_capsules")

        # 9. retry_after:failure — prev cycle failed + same action retried
        prev_actions = ctx.get("prev_action_names", [])
        prev_results_str = ctx.get("prev_action_results", "")
        if prev_actions and prev_results_str and "false" in prev_results_str.lower():
            retried = set(action_names) & set(prev_actions)
            if retried and prev_cid:
                edges.append((
                    prev_cid, cid,
                    "retry_after:failure", 0.9,
                    f"retried: {', '.join(retried)}", ts
                ))
                tags.append(f"retry:{','.join(retried)}")

        # 10. observed:env — environmental data changed from prev cycle
        env_data = _env_fingerprint(ctx)
        env_hash = hashlib.md5(env_data.encode()).hexdigest()[:8]
        if self._prev_env_hash and env_hash != self._prev_env_hash:
            edges.append((
                f"env:{env_hash}", cid,
                "observed:env", 0.6, env_data[:80], ts
            ))
            tags.append(f"env:{env_hash}")
        self._prev_env_hash = env_hash

        # 11. mission_progress / mission_context — active mission tracking
        active_mission = ctx.get("active_mission")
        if active_mission:
            mission_id = active_mission.get("_db_id", "?")
            mission_step = active_mission.get("current_step", 1)
            has_progress = any(
                n in ("progress_mission", "advance_mission", "complete_mission", "finish_mission")
                for n in action_names
            )
            if has_progress:
                edges.append((
                    f"mission:{mission_id}:step{mission_step}", cid,
                    "mission_progress", 1.0,
                    f"step {mission_step}: {active_mission.get('title', '')[:40]}", ts
                ))
                tags.append(f"mission_progress:{mission_id}:step{mission_step}")
            else:
                edges.append((
                    f"mission:{mission_id}", cid,
                    "mission_context", 0.5, "", ts
                ))
                tags.append(f"mission_context:{mission_id}")

        # 12. self_initiated — fallback when no external triggers
        external_types = {
            "responded_to:operator", "responded_to:feed",
            "responded_to:sibling", "triggered_by:alert",
            "triggered_by:mentor",
        }
        has_external = any(e[2] in external_types for e in edges)
        if not has_external and not tags:
            edges.append(("self", cid, "self_initiated", 0.5, "", ts))
            tags.append("self_initiated")

        return edges, tags

    # ── Storage ───────────────────────────────────────────────────

    def store_edges(self, edges: List[tuple]):
        """Batch INSERT causal edges."""
        for edge in edges:
            source_id, target_id, edge_type, strength, context, created_at = edge
            self.db.run(
                "INSERT INTO causal_edges "
                "(source_id, target_id, edge_type, strength, context, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (source_id, target_id, edge_type, strength, context, created_at),
            )

    # ── Traversal ─────────────────────────────────────────────────

    def trace_backward(self, cycle_id: str, depth: int = 5) -> List[dict]:
        """Follow edges backward from a cycle. Returns ordered chain."""
        visited = set()
        chain = []
        frontier = [cycle_id]

        for _ in range(depth):
            if not frontier:
                break
            current = frontier.pop(0)
            if current in visited:
                continue
            visited.add(current)

            rows = self.db.query(
                "SELECT source_id, target_id, edge_type, strength, context "
                "FROM causal_edges WHERE target_id = ? "
                "ORDER BY created_at DESC",
                (current,),
            )
            for r in rows:
                src = r["source_id"]
                chain.append({
                    "cycle_id": src,
                    "target": current,
                    "edge_type": r["edge_type"],
                    "strength": r["strength"],
                    "context": r["context"],
                })
                if src not in visited and not src.startswith(("self", "env:", "alert:",
                                                              "operator:", "feed:",
                                                              "sibling:", "mentor:",
                                                              "capsule:")):
                    frontier.append(src)

        return chain

    def trace_forward(self, cycle_id: str, depth: int = 5) -> List[dict]:
        """Follow edges forward from a cycle."""
        visited = set()
        chain = []
        frontier = [cycle_id]

        for _ in range(depth):
            if not frontier:
                break
            current = frontier.pop(0)
            if current in visited:
                continue
            visited.add(current)

            rows = self.db.query(
                "SELECT source_id, target_id, edge_type, strength, context "
                "FROM causal_edges WHERE source_id = ? "
                "ORDER BY created_at ASC",
                (current,),
            )
            for r in rows:
                tgt = r["target_id"]
                chain.append({
                    "cycle_id": tgt,
                    "source": current,
                    "edge_type": r["edge_type"],
                    "strength": r["strength"],
                    "context": r["context"],
                })
                if tgt not in visited:
                    frontier.append(tgt)

        return chain

    def find_topic_chain(self, keywords: List[str], limit: int = 10) -> List[dict]:
        """Find cycles mentioning keywords, connect via causal edges."""
        if not keywords:
            return []

        # Search thought_stream reasoning for keyword matches
        conditions = " OR ".join(["reasoning LIKE ?" for _ in keywords])
        params = tuple(f"%{kw}%" for kw in keywords)
        matching = self.db.query(
            f"SELECT cycle_id FROM thought_stream WHERE {conditions} "
            f"ORDER BY id DESC LIMIT ?",
            params + (limit * 2,),
        )
        if not matching:
            return []

        cycle_ids = [r["cycle_id"] for r in matching]
        chain = []

        for cid in cycle_ids[:limit]:
            # Get edges for this cycle
            edges = self.db.query(
                "SELECT source_id, edge_type, strength, context "
                "FROM causal_edges WHERE target_id = ? "
                "ORDER BY strength DESC LIMIT 3",
                (cid,),
            )
            edge_types = [e["edge_type"] for e in edges]
            context_bits = [e["context"] for e in edges if e["context"]]
            chain.append({
                "cycle_id": cid,
                "edge_type": edge_types[0] if edge_types else "isolated",
                "context": "; ".join(context_bits)[:80] if context_bits else "",
            })

        return chain

    def get_goal_trail(self, goal_text: str, recent_cycles: int = 20) -> List[str]:
        """Trace how Mind arrived at its current goal. Returns human-readable lines."""
        if not goal_text:
            return []

        # Find recent cycles with goal-related actions
        rows = self.db.query(
            "SELECT cycle_id, actions_taken, action_results "
            "FROM thought_stream ORDER BY id DESC LIMIT ?",
            (recent_cycles,),
        )

        goal_cycles = []
        goal_words = set(w.lower() for w in goal_text.split() if len(w) > 3)

        for r in rows:
            actions_str = r.get("actions_taken", "[]")
            results_str = r.get("action_results", "")
            try:
                action_list = json.loads(actions_str)
            except (json.JSONDecodeError, TypeError):
                action_list = []

            # Check if cycle relates to the goal
            combined = (actions_str + " " + results_str).lower()
            overlap = sum(1 for w in goal_words if w in combined)
            has_goal_action = "update_goal" in action_list

            if has_goal_action or overlap >= 2:
                goal_cycles.append({
                    "cycle_id": r["cycle_id"],
                    "actions": action_list,
                    "result_preview": (results_str or "")[:60],
                })

        if not goal_cycles:
            return []

        lines = []
        goal_cycles.reverse()  # chronological
        n = len(goal_cycles)
        lines.append(f'Goal trail: "{goal_text[:60]}" ({n} related cycles)')
        for i, gc in enumerate(goal_cycles[-5:]):  # last 5
            age = n - i
            actions = ", ".join(gc["actions"][:3])
            preview = gc["result_preview"]
            lines.append(f"  -> {gc['cycle_id']}: {actions}" +
                         (f" ({preview})" if preview else ""))

        return lines

    def get_operator_chain(self, limit: int = 3) -> List[str]:
        """Trace recent operator interactions and Mind's responses."""
        rows = self.db.query(
            "SELECT target_id, context FROM causal_edges "
            "WHERE edge_type = 'responded_to:operator' "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        if not rows:
            return []

        lines = []
        for r in rows:
            cid = r["target_id"]
            preview = r["context"][:60] if r["context"] else ""
            # Look up what Mind did in response
            thought = self.db.query_one(
                "SELECT actions_taken FROM thought_stream WHERE cycle_id = ?",
                (cid,),
            )
            actions = ""
            if thought:
                try:
                    acts = json.loads(thought.get("actions_taken", "[]"))
                    actions = ", ".join(acts[:3])
                except (json.JSONDecodeError, TypeError):
                    pass
            line = f"  Nate: \"{preview}\"" if preview else f"  Nate sent message"
            if actions:
                line += f" -> you did: {actions}"
            lines.append(line)

        return lines

    def get_causal_context_for_prompt(self, ctx: dict, limit: int = 5) -> List[str]:
        """Main entry point for prompt building.
        Returns human-readable lines for the HOW YOU GOT HERE section."""
        lines = []

        # Goal trail
        try:
            goal_row = self.db.query_one(
                "SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 "
                "AND (source IS NULL OR source != 'sprout') "
                "ORDER BY priority DESC, created_at DESC LIMIT 1"
            )
            if goal_row:
                trail = self.get_goal_trail(goal_row["content"])
                if trail:
                    lines.extend(trail)
        except Exception:
            pass

        # Operator interaction chain
        try:
            op_chain = self.get_operator_chain(limit=3)
            if op_chain:
                lines.append("Recent operator interactions:")
                lines.extend(op_chain)
        except Exception:
            pass

        # Mission progress trail
        try:
            mission_edges = self.db.query(
                "SELECT source_id, target_id, context FROM causal_edges "
                "WHERE edge_type = 'mission_progress' "
                "ORDER BY created_at DESC LIMIT 5"
            )
            if mission_edges:
                lines.append("Mission progress trail:")
                for me in reversed(mission_edges):
                    ctx_str = me.get("context", "")[:60]
                    lines.append(f"  {me['source_id']} -> {me['target_id']}: {ctx_str}")
        except Exception:
            pass

        # Topic persistence detection
        try:
            rows = self.db.query(
                "SELECT edge_type, COUNT(*) as cnt FROM causal_edges "
                "WHERE created_at > ? GROUP BY edge_type "
                "ORDER BY cnt DESC LIMIT 5",
                (now_ts() - 86400,),
            )
            if rows:
                notable = [
                    r for r in rows
                    if r["cnt"] >= 3 and r["edge_type"] not in ("continued", "observed:env")
                ]
                for r in notable:
                    lines.append(
                        f"Pattern: {r['edge_type']} occurred {r['cnt']}x in last 24h"
                    )
        except Exception:
            pass

        return lines

    # ── Maintenance ───────────────────────────────────────────────

    def prune(self, max_age_days: int = 90):
        """Remove edges older than max_age_days."""
        cutoff = now_ts() - (max_age_days * 86400)
        self.db.run(
            "DELETE FROM causal_edges WHERE created_at < ?", (cutoff,)
        )


# ── Helpers ───────────────────────────────────────────────────────

def _env_fingerprint(ctx: dict) -> str:
    """Create a short fingerprint of environmental state for change detection."""
    parts = []
    weather = ctx.get("weather", "")
    if weather:
        parts.append(f"w:{weather[:30]}")
    net = ctx.get("network_state", {})
    if net:
        online = sorted(k for k, v in net.items() if v == "online")
        parts.append(f"net:{','.join(online)}")
    sprout = ctx.get("sprout_state", {})
    if sprout:
        parts.append(f"sprout:{sprout.get('minutes_ago', '?')}m")
    return "|".join(parts) if parts else "no_env"
