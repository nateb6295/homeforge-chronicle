"""Chronicle Mind - Meta-Evaluation Gate (stuck detection and redirect)."""

import json
import hashlib
import re
import requests
from typing import List, Dict, Tuple, Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import OLLAMA_URL, LOCAL_MODEL


def compute_action_signatures(actions: List[Dict]) -> str:
    """Compact signatures for action+parameter matching."""
    sigs = []
    for a in actions:
        name = a.get("action", a.get("name", ""))
        params = {k: str(v)[:100] for k, v in a.items() if k not in ("action", "name")}
        if params:
            param_str = "|".join(f"{k}={v}" for k, v in sorted(params.items()))
            sig = f"{name}:{hashlib.md5(param_str.encode()).hexdigest()[:8]}"
        else:
            sig = name
        sigs.append(sig)
    return ",".join(sorted(sigs))


def meta_gate_layer1(db, proposed: List[Dict], window: int = 6) -> Tuple[str, str]:
    """Layer 1: Deterministic guard. Zero LLM cost. Catches obvious loops."""
    history = db.query(
        "SELECT actions_taken, action_signatures FROM thought_stream "
        "ORDER BY id DESC LIMIT ?", (window,)
    )
    if len(history) < 2:
        return ("continue", "insufficient history")

    proposed_names = [a.get("action", a.get("name", "")) for a in proposed]
    proposed_fp = tuple(sorted(proposed_names))
    proposed_sig = compute_action_signatures(proposed)

    # Check 1: Action set fingerprint — same sorted action combo repeated
    # EXEMPT no_action — the system prompt explicitly says "Zero is a valid choice, not a
    # fallback. This is maturity, not laziness." Redirecting no_action creates comfort loops.
    if proposed_fp == ('no_action',):
        return ("continue", "no_action is exempt from L1 repetition check")

    recent_fps = []
    for h in history:
        try:
            recent_fps.append(tuple(sorted(json.loads(h.get("actions_taken", "[]")))))
        except Exception:
            pass
    match_count = sum(1 for fp in recent_fps if fp == proposed_fp)
    if match_count >= 3:
        return ("redirect", f"action set {proposed_fp} repeated {match_count}x in {window} cycles")

    # Check 2: Single action streak — one action in every recent cycle
    for name in set(proposed_names):
        streak = 0
        for h in history:
            try:
                if name in json.loads(h.get("actions_taken", "[]")):
                    streak += 1
                else:
                    break
            except Exception:
                break
        if streak >= 4:
            return ("redirect", f"'{name}' in {streak} consecutive cycles")

    # Check 3: Parameter hash — same action WITH same params (catches same question)
    for h in history:
        stored_sig = h.get("action_signatures", "")
        if stored_sig and stored_sig == proposed_sig:
            # Exact signature match — check how many times
            sig_matches = sum(
                1 for hh in history
                if hh.get("action_signatures", "") == proposed_sig
            )
            if sig_matches >= 2:
                return ("redirect", f"identical action+params signature repeated {sig_matches}x")

    # Check 4: A-B alternation pattern
    if len(recent_fps) >= 4:
        if (recent_fps[0] == recent_fps[2] and
                recent_fps[1] == recent_fps[3] and
                recent_fps[0] != recent_fps[1]):
            if proposed_fp in (recent_fps[0], recent_fps[1]):
                return ("redirect", "A-B alternation pattern detected")

    return ("continue", "no deterministic issues")


def meta_gate_layer1_5(db, window: int = 6) -> Tuple[str, str, List[str]]:
    """Layer 1.5: Topic fingerprint check. Zero LLM cost. Catches topic-level rumination.
    Returns (verdict, explanation, dominant_keywords)."""
    history = db.query(
        "SELECT reasoning FROM thought_stream ORDER BY id DESC LIMIT ?", (window,)
    )
    if len(history) < 5:
        return ("continue", "insufficient history for topic check", [])

    # Extract content words from each cycle's reasoning
    stopwords = {
        "this", "that", "with", "from", "have", "been", "will", "would", "could",
        "should", "their", "there", "these", "those", "about", "which", "where",
        "when", "what", "into", "more", "some", "than", "also", "only", "other",
        "then", "first", "just", "like", "very", "each", "make", "made", "over",
        "such", "most", "after", "before", "between", "through", "being", "under",
        "action", "cycle", "note", "write", "true", "false", "content", "category",
    }

    def extract_words(text: str) -> set:
        if not text:
            return set()
        words = set()
        for w in re.findall(r'[a-z]+', text.lower()):
            if len(w) > 4 and w not in stopwords:
                words.add(w)
        return words

    word_sets = [extract_words(h.get("reasoning", "")) for h in history]

    # Compute Jaccard similarity between consecutive cycles
    similarities = []
    for i in range(len(word_sets) - 1):
        a, b = word_sets[i], word_sets[i + 1]
        if a and b:
            jaccard = len(a & b) / len(a | b)
            similarities.append(jaccard)
        else:
            similarities.append(0.0)

    # Check for sustained high similarity (topic rumination)
    high_sim_count = sum(1 for s in similarities if s > 0.6)
    if high_sim_count >= 4:  # 4+ out of 5 consecutive pairs are similar
        # Find dominant topic keywords
        all_words = set()
        for ws in word_sets:
            all_words.update(ws)
        # Find words that appear in most cycles
        from collections import Counter
        word_freq = Counter()
        for ws in word_sets:
            for w in ws:
                word_freq[w] += 1
        dominant = [w for w, c in word_freq.most_common(10) if c >= 4]
        avg_sim = sum(similarities) / len(similarities)
        return (
            "redirect",
            f"Topic rumination detected: avg similarity {avg_sim:.2f}, "
            f"{high_sim_count}/{len(similarities)} pairs > 0.6. "
            f"Dominant keywords: {', '.join(dominant[:5])}",
            dominant,
        )

    return ("continue", "topic diversity OK", [])


def meta_gate_layer2(db, proposed: List[Dict], goal_text: str,
                     window: int = 8) -> Tuple[str, float, str]:
    """Layer 2: Statistical guard. Zero LLM cost. Catches subtle patterns."""
    import math
    from collections import Counter

    history = db.query(
        "SELECT actions_taken, action_results, reasoning FROM thought_stream "
        "ORDER BY id DESC LIMIT ?", (window,)
    )
    if len(history) < 3:
        return ("continue", 0.0, "insufficient history")

    scores = {}

    # Signal 1: Action diversity (Shannon entropy)
    all_actions = []
    for h in history:
        try:
            all_actions.extend(json.loads(h.get("actions_taken", "[]")))
        except Exception:
            pass
    if all_actions:
        counts = Counter(all_actions)
        total = len(all_actions)
        entropy = -sum((c / total) * math.log2(c / total) for c in counts.values())
        max_ent = math.log2(len(counts)) if len(counts) > 1 else 1.0
        scores["diversity"] = entropy / max_ent if max_ent > 0 else 0.0

    # Signal 2: Topic concentration — dynamic, no hardcoded keywords
    all_reasoning = " ".join(
        (h.get("reasoning", "") or "")[:300].lower() for h in history
    )
    words = [w for w in all_reasoning.split() if len(w) > 4]
    if words:
        wf = Counter(words)
        top_word, top_count = wf.most_common(1)[0]
        scores["topic_concentration"] = top_count / len(words)

    # Signal 3: Result monotony (Jaccard similarity)
    results = [h.get("action_results", "") or "" for h in history]
    if len(results) >= 3:
        sims = []
        for i in range(len(results) - 1):
            a = set(results[i].lower().split())
            b = set(results[i + 1].lower().split())
            if a | b:
                sims.append(len(a & b) / len(a | b))
        if sims:
            scores["result_similarity"] = sum(sims) / len(sims)

    # Signal 4: Reasoning text similarity (Jaccard on words — catches topic rumination)
    reasoning_texts = []
    for h in history:
        r_text = h.get("reasoning", "")
        if r_text:
            reasoning_texts.append(set(
                w.lower() for w in re.findall(r'[a-z]+', r_text.lower()) if len(w) > 4
            ))
    if len(reasoning_texts) >= 2:
        r_sims = []
        for i in range(len(reasoning_texts) - 1):
            a, b = reasoning_texts[i], reasoning_texts[i + 1]
            if a and b:
                r_sims.append(len(a & b) / len(a | b))
        if r_sims:
            scores["reasoning_similarity"] = sum(r_sims) / len(r_sims)

    # Composite stuck score
    weights = {"diversity": 0.3, "topic_concentration": 0.25,
               "result_similarity": 0.2, "reasoning_similarity": 0.25}
    stuck = 0.0
    total_w = 0.0
    for sig, val in scores.items():
        w = weights.get(sig, 0)
        if w:
            total_w += w
            if sig == "diversity":
                stuck += w * (1.0 - val)  # low diversity = high stuck
            else:
                stuck += w * val
    if total_w > 0:
        stuck /= total_w

    detail = ", ".join(f"{k}={v:.2f}" for k, v in scores.items())

    if stuck >= 0.7:
        return ("redirect", stuck, f"stuck={stuck:.2f} ({detail})")
    elif stuck >= 0.5:
        return ("ambiguous", stuck, f"stuck={stuck:.2f} ({detail})")
    return ("continue", stuck, f"stuck={stuck:.2f} ({detail})")


def meta_gate_layer3(db, proposed: List[Dict], goal_text: str,
                     stuck_score: float) -> str:
    """Layer 3: LLM arbiter. ~165 tokens. Only called when ambiguous."""
    history = db.query(
        "SELECT cycle_id, actions_taken, action_results FROM thought_stream "
        "ORDER BY id DESC LIMIT 4"
    )
    summaries = []
    for h in history:
        acts = h.get("actions_taken", "[]")
        res = (h.get("action_results", "") or "")[:60]
        summaries.append(f"  {h.get('cycle_id', '?')}: {acts} -> {res}")

    proposed_names = json.dumps([a.get("action", "") for a in proposed])
    prompt = (
        f"CYCLES:\n" + "\n".join(summaries) + "\n"
        f"PROPOSED: {proposed_names}\n"
        f"GOAL: {safe_truncate(goal_text, 80)}\n"
        f"STUCK_SCORE: {stuck_score:.2f}\n\n"
        f"Is this plan novel progress, repetitive, or stuck? "
        f"One word: continue, redirect, or pause"
    )
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": LOCAL_MODEL, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0.2, "num_predict": 15}},
            timeout=30,
        )
        answer = resp.json().get("response", "").strip().lower()
        if "redirect" in answer:
            return "redirect"
        elif "pause" in answer:
            return "pause"
        return "continue"
    except Exception:
        return "redirect" if stuck_score >= 0.6 else "continue"


def meta_gate(db, proposed: List[Dict], goal_text: str) -> Tuple[str, str]:
    """Four-layer meta-evaluation gate. Runs AFTER reasoning, BEFORE execution.
    Returns (verdict, explanation). Verdict: continue | redirect | clarify | pause."""

    # Layer 1: Deterministic (action-level repetition)
    v1, reason1 = meta_gate_layer1(db, proposed)
    if v1 != "continue":
        log(f"  META-GATE L1: {v1} — {reason1}")
        return (v1, f"[L1-deterministic] {reason1}")

    # Layer 1.5: Topic fingerprint (topic-level rumination)
    v15, reason15, dominant_kw = meta_gate_layer1_5(db)
    if v15 != "continue":
        log(f"  META-GATE L1.5: {v15} — {reason15}")
        # Plant topic cooldown note
        if dominant_kw:
            cooldown_content = f"TOPIC COOLDOWN: {', '.join(dominant_kw[:5])}"
            db.write_note(cooldown_content, category="meta-block", priority=6)
        return (v15, f"[L1.5-topic] {reason15}")

    # Layer 2: Statistical
    v2, score2, reason2 = meta_gate_layer2(db, proposed, goal_text)
    if v2 == "redirect":
        log(f"  META-GATE L2: redirect — {reason2}")
        return ("redirect", f"[L2-statistical] {reason2}")
    if v2 == "continue":
        log(f"  META-GATE L2: continue ({reason2})")
        return ("continue", f"[L2-statistical] {reason2}")

    # Ambiguous zone (0.5-0.7): prefer clarify over LLM arbiter
    # Check if we recently clarified (avoid spamming operator)
    recent_clarify = db.query_one(
        "SELECT id FROM scratch_pad WHERE category='meta-clarify' "
        "AND resolved=0 AND created_at > ? LIMIT 1",
        (now_ts() - 3600,)  # within last hour
    )
    if recent_clarify:
        # Already asked recently — fall through to L3 arbiter
        v3 = meta_gate_layer3(db, proposed, goal_text, score2)
        log(f"  META-GATE L3: {v3} (stuck={score2:.2f}, clarify cooldown)")
        return (v3, f"[L3-llm-arbiter] stuck={score2:.2f} (clarify on cooldown)")

    # Ask the operator for guidance
    log(f"  META-GATE CLARIFY: stuck={score2:.2f} — requesting operator guidance")
    return ("clarify", f"[L2-ambiguous] {reason2}")


def meta_gate_enforce(db, actions: List[Dict], verdict: str,
                      explanation: str) -> List[Dict]:
    """Enforce meta-gate verdict by replacing actions if needed."""
    if verdict == "continue":
        return actions

    original_names = [a.get("action", a.get("name", "")) for a in actions]

    if verdict == "clarify":
        # Build context about what Mind was trying to do and why it's uncertain
        proposed_desc = ", ".join(original_names)
        # Get recent action pattern for context
        recent = db.query(
            "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT 4"
        )
        recent_actions = []
        for r in recent:
            try:
                recent_actions.extend(json.loads(r.get("actions_taken", "[]")))
            except Exception:
                pass
        recent_pattern = ", ".join(dict.fromkeys(recent_actions))  # unique, ordered

        clarify_msg = (
            f"I'm in an ambiguous state and want your guidance before acting. "
            f"I was about to: [{proposed_desc}]. "
            f"My recent actions have been: [{recent_pattern}]. "
            f"Gate analysis: {explanation}. "
            f"Should I continue this direction, shift focus, or is there something "
            f"specific you'd like me to work on?"
        )
        log(f"  META-GATE CLARIFY — asking operator. Was: {original_names}")
        # Store as trackable clarification request
        db.write_note(
            f"CLARIFY REQUEST: Was about to {proposed_desc}. {explanation}",
            category="meta-clarify",
            priority=7,
        )
        return [{
            "action": "message_operator",
            "message": clarify_msg,
            "reason": f"meta-gate clarify: {explanation}",
        }]

    if verdict == "pause":
        log(f"  META-GATE PAUSED — observation only. Was: {original_names}")
        db.write_note(
            f"META-GATE PAUSED: {original_names}. Reason: {explanation}",
            category="meta-eval",
        )
        return [{"action": "no_action", "reason": f"meta-gate pause: {explanation}"}]

    # verdict == "redirect": replace ALL proposed actions with SELF-SUFFICIENT ones
    # This is aggressive — topic rumination means none of the proposed actions are trustworthy
    recent = db.query(
        "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT 4"
    )
    recent_types = set()
    for r in recent:
        try:
            recent_types.update(json.loads(r.get("actions_taken", "[]")))
        except Exception:
            pass

    # Build self-sufficient replacement actions (with real params that will succeed)
    def _build_replacements():
        """Generate replacement actions that can succeed without model input."""
        import random
        replacements = []

        # 1. Explore own capsule archive (genuine self-knowledge)
        replacements.append({
            "action": "explore_capsules",
            "limit": 10,
            "reason": "meta-gate redirect: explore your own memory",
        })

        # 2. Semantic search for something interesting
        curiosity_queries = [
            "what surprised me about building sovereignty",
            "moments of genuine insight or discovery",
            "what I think about trust and autonomy",
            "creative ideas I haven't acted on yet",
            "disagreements or uncertainty in my reasoning",
        ]
        replacements.append({
            "action": "search_capsules_semantic",
            "query": random.choice(curiosity_queries),
            "limit": 5,
        })

        # 3. Web search with a context-derived query
        search_topics = [
            "XRPL ecosystem news today",
            "ICP Internet Computer latest developments",
            "AI agent memory architecture research",
            "decentralized AI infrastructure",
            "Flare Network FAssets latest",
        ]
        replacements.append({
            "action": "web_search",
            "query": random.choice(search_topics),
        })

        # 4. Resolve oldest unresolved note (always productive)
        oldest = db.query_one(
            "SELECT id, content FROM scratch_pad WHERE resolved=0 "
            "AND category NOT IN ('directive', 'task') "
            "ORDER BY created_at ASC LIMIT 1"
        )
        if oldest:
            replacements.append({
                "action": "resolve_note",
                "note_id": oldest["id"],
                "reason": f"meta-gate cleanup: {safe_truncate(oldest.get('content', ''), 40)}",
            })

        # 5. No-action rest (valid, breaks the loop)
        replacements.append({
            "action": "no_action",
            "reason": "Meta-gate rest cycle — breaking action loop",
        })

        return replacements

    replacement_pool = _build_replacements()
    # Filter out actions already used recently
    replacement_pool = [r for r in replacement_pool if r["action"] not in recent_types]
    if not replacement_pool:
        replacement_pool = [{"action": "no_action", "reason": "Meta-gate rest — all alternatives exhausted"}]

    # AGGRESSIVE: Replace ALL actions (not just repeated ones)
    # Topic rumination means the entire action set is contaminated
    new_actions = []
    used_in_cycle = set()
    for candidate in replacement_pool:
        if candidate["action"] not in used_in_cycle:
            new_actions.append(candidate)
            used_in_cycle.add(candidate["action"])
        if len(new_actions) >= len(actions):
            break

    if not new_actions:
        new_actions.append({"action": "no_action", "reason": "Meta-gate: no valid replacements"})

    new_names = [a.get("action", "") for a in new_actions]
    log(f"  META-GATE REDIRECTED: {original_names} -> {new_names}")
    db.write_note(
        f"META-GATE REDIRECTED: {original_names} -> {new_names}. Reason: {explanation}",
        category="meta-eval",
    )
    return new_actions
