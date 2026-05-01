#!/usr/bin/env python3
"""Scaffold Health — Thread self-reference detector (Build #99, #102, #103).

From Thread #300: "When does the scaffold become the subject?"
Gemma's diagnostic: "sustained amplitude without external gain."
Darby's frame: chronic pain as pathological closure — prediction loops
that refuse disconfirmation.

Build #102 (Thread #301): Perturbation response metric.
Gemma's breathing test: "When a loop stops breathing with the larger
network — insulated from correction — its signal degrades into echo."
Two types of non-subtractability: pathological (self-defending) vs
constitutive (open to perturbation). The discriminating variable is
whether the scaffold integrates challenges or deflects them.

Build #103 (Nate's question): Convergence detector.
The complement to breathing: when has breathing done its work?
Measures challenge novelty decay, thesis stability, and provocateur
cycling. Thread #301 scored closure=0.00 with a complete thesis —
the immune system prevented premature closure but also prevented
healthy completion. This fills that gap.

Measures per thread:
  - Self-reference ratio: how much advancement content echoes prior advancements
    vs. introduces genuinely new material from external sources
  - Novelty trend: is self-reference increasing (loop closing) or stable (breathing)?
  - Source diversity: how many distinct external sources feed each advancement?
  - Correction response: does the thread change direction after challenges?
  - Amplitude isolation: is the thread generating content decoupled from input?
  - Perturbation response: does the advancement engage with the challenge's
    argument (embedding similarity) and shift direction? (Build #102)
  - Convergence: are challenges repeating, thesis stable, question space
    exhausted? (Build #103)

Usage:
  scaffold_health.py [THREAD_ID]       # Analyze specific thread
  scaffold_health.py --recent [N]      # Analyze N most recent threads (default 5)
  scaffold_health.py --all             # Analyze all threads with 3+ advancements
  scaffold_health.py --json            # Output JSON (for integration with spot_check)
  scaffold_health.py --perturbation ID # Deep perturbation analysis for one thread
  scaffold_health.py --convergence ID  # Convergence detector for one thread
"""

import json
import math
import os
import re
import sqlite3
import sys
from collections import Counter

import requests

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "qwen3-embedding:0.6b")

# Words to ignore in overlap calculation
STOP_WORDS = set("""
the a an is are was were be been being have has had do does did will would
shall should may might can could of in to for on with at by from as into
through during before after above below between among not no nor but or
and so yet both either neither each every all any few more most other some
such than too very just also only own same that this these those it its
he she they them their his her my your our which what who whom how when
where why been have has had do does did will would shall should may might
can could the a an
""".split())

# Source markers in thread history
EXTERNAL_SOURCES = {
    "intern": "research",
    "darby": "family",
    "ada": "family",
    "gemma": "family",
    "nate": "curator",
    "provocateur": "challenge",
    "crossref": "connection",
}


def tokenize(text):
    """Simple word tokenization, lowercased, stop words removed."""
    words = re.findall(r'[a-z]+', text.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) > 2]


def jaccard(set_a, set_b):
    """Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0


def classify_source(source_str):
    """Classify a thread_history source string into external type."""
    source_lower = source_str.lower()
    for key, label in EXTERNAL_SOURCES.items():
        if key in source_lower:
            return label
    if "opus" in source_lower:
        return "self"
    return "unknown"


def embed_text(text, max_chars=2000):
    """Get embedding from Ollama. Returns list of floats or None on failure."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": text[:max_chars]},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            embs = data.get("embeddings")
            if embs and len(embs) > 0:
                return embs[0]
    except Exception:
        pass
    return None


def cosine_sim(a, b):
    """Cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def analyze_thread(db, thread_id, use_embeddings=False):
    """Analyze a single thread for self-reference patterns."""
    # Get thread info
    thread = db.execute(
        "SELECT id, title, status FROM cognitive_threads WHERE id=?",
        (thread_id,)
    ).fetchone()
    if not thread:
        return None

    tid, title, status = thread

    # Get all events
    events = db.execute(
        "SELECT event_type, content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at",
        (thread_id,)
    ).fetchall()

    advancements = []
    challenges = []
    research = []
    all_events = []

    for etype, content, source, ts in events:
        entry = {
            "type": etype,
            "content": content,
            "source": source,
            "source_class": classify_source(source),
            "ts": ts,
            "tokens": set(tokenize(content)),
        }
        all_events.append(entry)
        if etype == "advanced":
            advancements.append(entry)
        elif etype == "challenge":
            challenges.append(entry)
        elif etype == "research":
            research.append(entry)

    if len(advancements) < 2:
        return {
            "thread_id": tid,
            "title": title,
            "status": status,
            "advancements": len(advancements),
            "skip": True,
            "reason": "fewer than 2 advancements",
        }

    # === Metric 1: Self-reference ratio per advancement ===
    # For each advancement, compute overlap with all PRIOR advancements
    self_ref_ratios = []
    prior_tokens = set()

    for i, adv in enumerate(advancements):
        if i == 0:
            prior_tokens = adv["tokens"]
            self_ref_ratios.append(0.0)  # first advancement has no prior
            continue

        overlap = jaccard(adv["tokens"], prior_tokens)
        self_ref_ratios.append(overlap)
        prior_tokens = prior_tokens | adv["tokens"]

    # === Metric 1b: Semantic self-reference (embedding-based) ===
    # Catches the self-play attack: new words, same ideas
    semantic_self_ref = []
    if use_embeddings:
        embeddings = []
        for adv in advancements:
            emb = embed_text(adv["content"])
            embeddings.append(emb)

        for i in range(len(advancements)):
            if i == 0 or not embeddings[i]:
                semantic_self_ref.append(0.0)
                continue
            # Average cosine similarity with ALL prior advancements
            sims = []
            for j in range(i):
                if embeddings[j]:
                    sims.append(cosine_sim(embeddings[i], embeddings[j]))
            avg_sim = sum(sims) / len(sims) if sims else 0.0
            semantic_self_ref.append(avg_sim)

    # === Metric 2: External source diversity per advancement ===
    source_diversity = []
    for i, adv in enumerate(advancements):
        # Find external events between this and previous advancement
        prev_ts = advancements[i-1]["ts"] if i > 0 else 0
        curr_ts = adv["ts"]

        external_in_window = set()
        for evt in all_events:
            if evt["ts"] > prev_ts and evt["ts"] <= curr_ts:
                if evt["source_class"] != "self":
                    external_in_window.add(evt["source_class"])

        source_diversity.append(len(external_in_window))

    # === Metric 3: Correction response ===
    # After a challenge, does the next advancement show vocabulary shift?
    corrections = 0
    challenge_opportunities = 0

    for i, adv in enumerate(advancements):
        if i == 0:
            continue
        # Were there challenges between this and previous advancement?
        prev_ts = advancements[i-1]["ts"]
        challenges_in_window = [
            c for c in challenges
            if c["ts"] > prev_ts and c["ts"] <= adv["ts"]
        ]
        if challenges_in_window:
            challenge_opportunities += 1
            # Measure vocabulary shift: tokens in this advancement
            # that appear in the challenges but NOT in prior advancements
            challenge_tokens = set()
            for c in challenges_in_window:
                challenge_tokens |= c["tokens"]

            prior_adv_tokens = set()
            for prev_adv in advancements[:i]:
                prior_adv_tokens |= prev_adv["tokens"]

            # New tokens from challenges adopted in this advancement
            adopted = adv["tokens"] & challenge_tokens - prior_adv_tokens
            if len(adopted) > 3:  # meaningful adoption threshold
                corrections += 1

    correction_rate = corrections / max(challenge_opportunities, 1)

    # === Metric 4: Novelty trend (is self-reference increasing?) ===
    # Simple: compare average of first half vs second half
    if len(self_ref_ratios) >= 4:
        mid = len(self_ref_ratios) // 2
        first_half_avg = sum(self_ref_ratios[1:mid]) / max(mid - 1, 1)
        second_half_avg = sum(self_ref_ratios[mid:]) / max(len(self_ref_ratios) - mid, 1)
        trend = second_half_avg - first_half_avg
    else:
        trend = 0.0

    # === Metric 5: Amplitude isolation ===
    # Ratio of advancements with zero external sources in their window
    isolated_count = sum(1 for d in source_diversity if d == 0)
    isolation_rate = isolated_count / max(len(source_diversity), 1)

    # === Composite health score ===
    # Lower = healthier (more open, more responsive, more diverse input)
    avg_self_ref = sum(self_ref_ratios[1:]) / max(len(self_ref_ratios) - 1, 1)

    # Semantic self-reference (if computed)
    avg_semantic = 0.0
    semantic_trend = 0.0
    if semantic_self_ref:
        vals = [s for s in semantic_self_ref[1:] if s > 0]
        avg_semantic = sum(vals) / len(vals) if vals else 0.0
        if len(semantic_self_ref) >= 4:
            mid = len(semantic_self_ref) // 2
            s_first = [s for s in semantic_self_ref[1:mid] if s > 0]
            s_second = [s for s in semantic_self_ref[mid:] if s > 0]
            s_first_avg = sum(s_first) / len(s_first) if s_first else 0.0
            s_second_avg = sum(s_second) / len(s_second) if s_second else 0.0
            semantic_trend = s_second_avg - s_first_avg

    closure_score = 0.0
    if avg_self_ref > 0.4:
        closure_score += 0.3
    elif avg_self_ref > 0.25:
        closure_score += 0.15

    if trend > 0.1:
        closure_score += 0.25
    elif trend > 0.05:
        closure_score += 0.1

    if correction_rate < 0.3 and challenge_opportunities >= 2:
        closure_score += 0.25
    elif correction_rate < 0.5 and challenge_opportunities >= 2:
        closure_score += 0.1

    if isolation_rate > 0.5:
        closure_score += 0.2
    elif isolation_rate > 0.3:
        closure_score += 0.1

    # Semantic closure bonus: high embedding similarity = same ideas, different words
    # This catches the self-play attack the provocateur identified
    if semantic_self_ref and avg_semantic > 0.85:
        closure_score += 0.2
    elif semantic_self_ref and avg_semantic > 0.75:
        closure_score += 0.1

    # Classify
    if closure_score >= 0.6:
        label = "CLOSING"
        icon = "🔴"
    elif closure_score >= 0.3:
        label = "NARROWING"
        icon = "🟡"
    else:
        label = "BREATHING"
        icon = "🟢"

    result = {
        "thread_id": tid,
        "title": title,
        "status": status,
        "advancements": len(advancements),
        "challenges": len(challenges),
        "research_inputs": len(research),
        "skip": False,
        "self_ref_ratios": [round(r, 3) for r in self_ref_ratios],
        "avg_self_ref": round(avg_self_ref, 3),
        "trend": round(trend, 3),
        "source_diversity": source_diversity,
        "avg_diversity": round(sum(source_diversity) / max(len(source_diversity), 1), 2),
        "correction_rate": round(correction_rate, 3),
        "challenge_opportunities": challenge_opportunities,
        "isolation_rate": round(isolation_rate, 3),
        "closure_score": round(closure_score, 2),
        "label": label,
        "icon": icon,
    }
    if semantic_self_ref:
        result["semantic_self_ref"] = [round(s, 3) for s in semantic_self_ref]
        result["avg_semantic"] = round(avg_semantic, 3)
        result["semantic_trend"] = round(semantic_trend, 3)
    return result


def extract_conclusion(text, chars=300):
    """Extract the concluding position from an advancement.
    Takes last paragraph or last N chars, whichever is shorter."""
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    if paragraphs:
        conclusion = paragraphs[-1]
        if len(conclusion) > chars:
            conclusion = conclusion[-chars:]
        return conclusion
    return text[-chars:] if len(text) > chars else text


def perturbation_analysis(db, thread_id):
    """Deep perturbation response analysis for a thread (Build #102).

    For each challenge-advancement pair, measures:
    - Engagement depth: semantic similarity between challenge and advancement
    - Direction shift: semantic distance between consecutive advancement conclusions
    - Classification: integration / engagement / deflection / ignoring
    """
    events = db.execute(
        "SELECT event_type, content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at",
        (thread_id,)
    ).fetchall()

    thread = db.execute(
        "SELECT title, status FROM cognitive_threads WHERE id=?",
        (thread_id,)
    ).fetchone()
    if not thread:
        return None
    title, status = thread

    advancements = []
    challenges = []

    for etype, content, source, ts in events:
        entry = {
            "type": etype, "content": content,
            "source": source, "ts": ts,
            "source_class": classify_source(source),
        }
        if etype == "advanced":
            advancements.append(entry)
        elif etype == "challenge":
            challenges.append(entry)

    if len(advancements) < 2 or not challenges:
        return {
            "thread_id": thread_id, "title": title,
            "skip": True, "reason": "need 2+ advancements and 1+ challenges",
        }

    # Embed all advancements and challenges
    adv_embeddings = []
    adv_conclusion_embeddings = []
    for adv in advancements:
        emb = embed_text(adv["content"])
        adv_embeddings.append(emb)
        conclusion = extract_conclusion(adv["content"])
        cemb = embed_text(conclusion)
        adv_conclusion_embeddings.append(cemb)

    challenge_embeddings = []
    for ch in challenges:
        emb = embed_text(ch["content"])
        challenge_embeddings.append(emb)

    # For each advancement (after the first), find challenges in the window
    pairs = []
    for i in range(1, len(advancements)):
        prev_ts = advancements[i - 1]["ts"]
        curr_ts = advancements[i]["ts"]

        window_challenges = []
        for j, ch in enumerate(challenges):
            if ch["ts"] > prev_ts and ch["ts"] <= curr_ts:
                window_challenges.append((j, ch))

        if not window_challenges:
            continue

        # Direction shift: cosine distance between this and previous conclusion
        direction_shift = 0.0
        if adv_conclusion_embeddings[i] and adv_conclusion_embeddings[i - 1]:
            sim = cosine_sim(
                adv_conclusion_embeddings[i],
                adv_conclusion_embeddings[i - 1]
            )
            direction_shift = 1.0 - sim  # higher = more shift

        # Embed advancement paragraphs for chunk-level matching
        adv_paragraphs = [
            p.strip() for p in advancements[i]["content"].split('\n\n')
            if p.strip() and len(p.strip()) > 50
        ]
        para_embeddings = [embed_text(p) for p in adv_paragraphs]

        for j, ch in window_challenges:
            # Engagement depth: best paragraph-level similarity
            engagement = 0.0
            # Whole-text baseline
            if challenge_embeddings[j] and adv_embeddings[i]:
                engagement = cosine_sim(
                    challenge_embeddings[j], adv_embeddings[i]
                )
            # Chunk-level: find most relevant paragraph
            if challenge_embeddings[j] and para_embeddings:
                chunk_sims = [
                    cosine_sim(challenge_embeddings[j], pe)
                    for pe in para_embeddings if pe
                ]
                if chunk_sims:
                    best_chunk = max(chunk_sims)
                    # Use chunk score if better than whole-text
                    engagement = max(engagement, best_chunk)

            # Classify the pair
            if engagement > 0.55 and direction_shift > 0.1:
                label = "INTEGRATION"
                icon = "✅"
            elif engagement > 0.55:
                label = "ENGAGED"
                icon = "🔵"
            elif engagement > 0.35:
                label = "ACKNOWLEDGED"
                icon = "🟡"
            else:
                label = "IGNORED"
                icon = "🔴"

            pairs.append({
                "advancement_idx": i,
                "advancement_source": advancements[i]["source"],
                "challenge_idx": j,
                "challenge_source": ch["source"],
                "challenge_snippet": ch["content"][:120],
                "engagement": round(engagement, 3),
                "direction_shift": round(direction_shift, 3),
                "label": label,
                "icon": icon,
            })

    # Compute aggregate breathing score
    if pairs:
        integrated = sum(1 for p in pairs if p["label"] == "INTEGRATION")
        engaged = sum(1 for p in pairs if p["label"] == "ENGAGED")
        deflected = sum(1 for p in pairs
                        if p["label"] in ("DEFLECTION", "ACKNOWLEDGED"))
        ignored = sum(1 for p in pairs if p["label"] == "IGNORED")
        total = len(pairs)

        # Breathing = (integration + engaged) / total
        breathing_score = (integrated + engaged) / total
        avg_engagement = sum(p["engagement"] for p in pairs) / total
        avg_shift = sum(p["direction_shift"] for p in pairs) / total
    else:
        integrated = engaged = deflected = ignored = 0
        total = 0
        breathing_score = 0.0
        avg_engagement = 0.0
        avg_shift = 0.0

    return {
        "thread_id": thread_id,
        "title": title,
        "status": status,
        "skip": False,
        "advancements": len(advancements),
        "challenges": len(challenges),
        "pairs_analyzed": total,
        "integrated": integrated,
        "engaged": engaged,
        "deflected": deflected,
        "ignored": ignored,
        "breathing_score": round(breathing_score, 3),
        "avg_engagement": round(avg_engagement, 3),
        "avg_direction_shift": round(avg_shift, 3),
        "pairs": pairs,
    }


def convergence_analysis(db, thread_id):
    """Convergence detector — when has breathing done its work? (Build #103).

    Measures:
    - Challenge novelty decay: are recent challenges semantically similar to earlier ones?
    - Thesis stability: have the last N advancements maintained the same core position?
    - Provocateur cycling: is the provocateur repeating angles?

    When all three converge, the thread's question space is exhausted.
    """
    events = db.execute(
        "SELECT event_type, content, source, created_at "
        "FROM thread_history WHERE thread_id=? ORDER BY created_at",
        (thread_id,)
    ).fetchall()

    thread = db.execute(
        "SELECT title, status FROM cognitive_threads WHERE id=?",
        (thread_id,)
    ).fetchone()
    if not thread:
        return None
    title, status = thread

    advancements = []
    challenges = []

    for etype, content, source, ts in events:
        entry = {"type": etype, "content": content, "source": source, "ts": ts}
        if etype == "advanced":
            advancements.append(entry)
        elif etype == "challenge":
            challenges.append(entry)

    if len(advancements) < 3 or len(challenges) < 3:
        return {
            "thread_id": thread_id, "title": title, "skip": True,
            "reason": "need 3+ advancements and 3+ challenges",
        }

    # === 1. Challenge novelty decay ===
    # Embed all challenges, measure pairwise similarity in sliding windows
    challenge_embeddings = [embed_text(c["content"]) for c in challenges]

    # Compare each challenge to all PRIOR challenges
    challenge_novelty = []
    for i in range(len(challenges)):
        if i == 0 or not challenge_embeddings[i]:
            challenge_novelty.append(1.0)  # first challenge is fully novel
            continue
        sims = []
        for j in range(i):
            if challenge_embeddings[j]:
                sims.append(cosine_sim(challenge_embeddings[i], challenge_embeddings[j]))
        max_sim = max(sims) if sims else 0.0
        # Novelty = 1 - max similarity to any prior challenge
        challenge_novelty.append(1.0 - max_sim)

    # Recent novelty vs early novelty
    mid = len(challenge_novelty) // 2
    early_novelty = sum(challenge_novelty[:mid]) / max(mid, 1)
    late_novelty = sum(challenge_novelty[mid:]) / max(len(challenge_novelty) - mid, 1)
    novelty_decay = early_novelty - late_novelty  # positive = novelty declining

    # === 2. Thesis stability ===
    # Embed advancement conclusions, measure how stable recent ones are
    adv_conclusions = [extract_conclusion(a["content"]) for a in advancements]
    adv_conclusion_embs = [embed_text(c) for c in adv_conclusions]

    # Pairwise similarity of last 3 advancement conclusions
    recent_adv = adv_conclusion_embs[-3:]
    stability_sims = []
    for i in range(len(recent_adv)):
        for j in range(i + 1, len(recent_adv)):
            if recent_adv[i] and recent_adv[j]:
                stability_sims.append(cosine_sim(recent_adv[i], recent_adv[j]))
    thesis_stability = sum(stability_sims) / len(stability_sims) if stability_sims else 0.0

    # === 3. Provocateur cycling ===
    # Check if the last 3 challenges are more similar to each other than earlier ones
    if len(challenge_embeddings) >= 3:
        last3 = challenge_embeddings[-3:]
        cycle_sims = []
        for i in range(len(last3)):
            for j in range(i + 1, len(last3)):
                if last3[i] and last3[j]:
                    cycle_sims.append(cosine_sim(last3[i], last3[j]))
        cycling_score = sum(cycle_sims) / len(cycle_sims) if cycle_sims else 0.0
    else:
        cycling_score = 0.0

    # === Composite convergence score ===
    # Build #103 calibration: smooth linear interpolation replaces binary steps.
    # Calibrated against Threads #300-301 (completed, published, previously mis-scored).

    def _smooth(value, floor, ceiling, max_pts):
        if value <= floor:
            return 0.0
        if value >= ceiling:
            return max_pts
        return (value - floor) / (ceiling - floor) * max_pts

    convergence = 0.0
    convergence += _smooth(novelty_decay, 0.0, 0.12, 0.35)
    convergence += _smooth(thesis_stability, 0.3, 0.75, 0.35)
    convergence += _smooth(cycling_score, 0.3, 0.7, 0.30)

    # Classify
    if convergence >= 0.50:
        label = "COMPLETION READY"
        icon = "🏁"
    elif convergence >= 0.25:
        label = "CONVERGING"
        icon = "🔶"
    else:
        label = "EXPLORING"
        icon = "🔵"

    return {
        "thread_id": thread_id,
        "title": title,
        "status": status,
        "skip": False,
        "advancements": len(advancements),
        "challenges": len(challenges),
        "challenge_novelty": [round(n, 3) for n in challenge_novelty],
        "early_novelty": round(early_novelty, 3),
        "late_novelty": round(late_novelty, 3),
        "novelty_decay": round(novelty_decay, 3),
        "thesis_stability": round(thesis_stability, 3),
        "cycling_score": round(cycling_score, 3),
        "convergence": round(convergence, 2),
        "label": label,
        "icon": icon,
    }


def print_convergence(result):
    """Pretty-print convergence analysis."""
    if not result or result.get("skip"):
        print(f"  Skipped: {result.get('reason', 'unknown')}")
        return

    r = result
    print(f"=== Convergence Detector: Thread #{r['thread_id']} ===")
    print(f"  {r['title'][:70]}")
    print(f"  {r['advancements']} advancements, {r['challenges']} challenges")
    print()
    print(f"  {r['icon']} {r['label']} (convergence={r['convergence']:.2f})")
    print()
    print(f"  Challenge novelty decay: {r['novelty_decay']:+.3f}")
    print(f"    Early avg: {r['early_novelty']:.3f}, Late avg: {r['late_novelty']:.3f}")
    nd = r['novelty_decay']
    print(f"    {'Challenges repeating' if nd > 0.06 else 'Mild decay' if nd > 0.02 else 'Challenges still novel'}")
    print()
    print(f"  Thesis stability: {r['thesis_stability']:.3f}")
    ts = r['thesis_stability']
    print(f"    {'Thesis locked' if ts > 0.7 else 'Thesis stabilizing' if ts > 0.45 else 'Thesis still evolving'}")
    print()
    print(f"  Provocateur cycling: {r['cycling_score']:.3f}")
    cs = r['cycling_score']
    print(f"    {'Provocateur exhausted' if cs > 0.65 else 'Same-axis challenges' if cs > 0.45 else 'Challenges diverse'}")
    print()


def print_perturbation(result):
    """Pretty-print perturbation analysis."""
    if not result or result.get("skip"):
        print(f"  Skipped: {result.get('reason', 'unknown')}")
        return

    r = result
    print(f"=== Perturbation Response: Thread #{r['thread_id']} ===")
    print(f"  {r['title'][:70]}")
    print(f"  {r['advancements']} advancements, {r['challenges']} challenges, "
          f"{r['pairs_analyzed']} pairs analyzed")
    print()

    # Breathing verdict
    bs = r['breathing_score']
    if bs >= 0.7:
        verdict = "BREATHING — scaffold is open to perturbation"
        icon = "🟢"
    elif bs >= 0.4:
        verdict = "MIXED — some engagement, some deflection"
        icon = "🟡"
    else:
        verdict = "INSULATED — scaffold may be defending itself"
        icon = "🔴"
    print(f"  {icon} {verdict}")
    print(f"  Breathing score: {bs:.1%}")
    print(f"  Avg engagement depth: {r['avg_engagement']:.3f}")
    print(f"  Avg direction shift: {r['avg_direction_shift']:.3f}")
    print(f"  Integration: {r['integrated']}, Engaged: {r['engaged']}, "
          f"Acknowledged: {r['deflected']}, Ignored: {r['ignored']}")
    print()

    # Per-pair detail
    for p in r["pairs"]:
        print(f"  {p['icon']} {p['label']} (engagement={p['engagement']:.3f}, "
              f"shift={p['direction_shift']:.3f})")
        print(f"    Challenge: {p['challenge_snippet']}...")
        print(f"    Advancement #{p['advancement_idx']} "
              f"({p['advancement_source'][:40]})")
        print()


def print_analysis(result):
    """Pretty-print thread analysis."""
    if result.get("skip"):
        print(f"  #{result['thread_id']} — {result['title'][:60]}")
        print(f"    Skipped: {result['reason']}")
        return

    r = result
    print(f"  #{r['thread_id']} {r['icon']} {r['label']} "
          f"(closure={r['closure_score']:.2f}) [{r['status']}]")
    print(f"    {r['title'][:70]}")
    print(f"    Advancements: {r['advancements']}, "
          f"Challenges: {r['challenges']}, "
          f"Research: {r['research_inputs']}")
    print(f"    Self-ref: avg={r['avg_self_ref']:.3f}, "
          f"trend={'↑' if r['trend'] > 0.02 else '↓' if r['trend'] < -0.02 else '→'}"
          f"{r['trend']:+.3f}")
    print(f"    Trajectory: {' → '.join(f'{x:.2f}' for x in r['self_ref_ratios'])}")
    print(f"    Source diversity: {r['source_diversity']} "
          f"(avg={r['avg_diversity']:.1f})")
    print(f"    Correction response: {r['correction_rate']:.0%} "
          f"({r['challenge_opportunities']} opportunities)")
    print(f"    Isolation: {r['isolation_rate']:.0%} of advancements "
          f"had no external input")
    if "semantic_self_ref" in r:
        print(f"    Semantic: avg={r['avg_semantic']:.3f}, "
              f"trend={'↑' if r['semantic_trend'] > 0.02 else '↓' if r['semantic_trend'] < -0.02 else '→'}"
              f"{r['semantic_trend']:+.3f}")
        print(f"    Semantic trajectory: {' → '.join(f'{x:.2f}' for x in r['semantic_self_ref'])}")
    print()


def main():
    args = sys.argv[1:]
    output_json = "--json" in args
    use_embeddings = "--embed" in args
    do_perturbation = "--perturbation" in args
    do_convergence = "--convergence" in args
    args = [a for a in args
            if a not in ("--json", "--embed", "--perturbation", "--convergence")]

    db = sqlite3.connect(DB_PATH)

    # Build #103: Convergence detector
    if do_convergence:
        tid = int(args[0]) if args and args[0].isdigit() else None
        if not tid:
            row = db.execute(
                "SELECT id FROM cognitive_threads "
                "WHERE status='active' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            tid = row[0] if row else None
        if not tid:
            # Try most recent completed thread
            row = db.execute(
                "SELECT id FROM cognitive_threads "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            tid = row[0] if row else None
        if not tid:
            print("No thread found for convergence analysis.")
            db.close()
            return
        result = convergence_analysis(db, tid)
        if output_json:
            print(json.dumps(result, indent=2))
        else:
            print_convergence(result)
        db.close()
        return

    # Build #102: Deep perturbation analysis
    if do_perturbation:
        tid = int(args[0]) if args and args[0].isdigit() else None
        if not tid:
            # Default: most recent active thread
            row = db.execute(
                "SELECT id FROM cognitive_threads "
                "WHERE status='active' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            tid = row[0] if row else None
        if not tid:
            print("No thread found for perturbation analysis.")
            db.close()
            return
        result = perturbation_analysis(db, tid)
        if output_json:
            print(json.dumps(result, indent=2))
        else:
            print_perturbation(result)
        db.close()
        return

    if args and args[0] == "--all":
        # All threads with 3+ advancements
        threads = db.execute(
            "SELECT DISTINCT ct.id FROM cognitive_threads ct "
            "JOIN thread_history th ON th.thread_id = ct.id "
            "WHERE th.event_type='advanced' "
            "GROUP BY ct.id HAVING COUNT(*) >= 3 "
            "ORDER BY ct.id DESC"
        ).fetchall()
        thread_ids = [t[0] for t in threads]

    elif args and args[0] == "--recent":
        n = int(args[1]) if len(args) > 1 else 5
        threads = db.execute(
            "SELECT id FROM cognitive_threads ORDER BY id DESC LIMIT ?",
            (n,)
        ).fetchall()
        thread_ids = [t[0] for t in threads]

    elif args and args[0].isdigit():
        thread_ids = [int(args[0])]

    else:
        # Default: recent 5
        threads = db.execute(
            "SELECT id FROM cognitive_threads ORDER BY id DESC LIMIT 5"
        ).fetchall()
        thread_ids = [t[0] for t in threads]

    if not output_json:
        print("=== Scaffold Health (Thread #300 Diagnostic) ===")
        print("Measuring: amplitude without external gain")
        print("Gemma: 'sustained amplitude, input channel dark = cannibalism'")
        print()

    results = []
    for tid in thread_ids:
        result = analyze_thread(db, tid, use_embeddings=use_embeddings)
        if result:
            results.append(result)
            if not output_json:
                print_analysis(result)

    if not output_json and results:
        # Summary statistics
        analyzed = [r for r in results if not r.get("skip")]
        if analyzed:
            avg_closure = sum(r["closure_score"] for r in analyzed) / len(analyzed)
            closing = sum(1 for r in analyzed if r["label"] == "CLOSING")
            narrowing = sum(1 for r in analyzed if r["label"] == "NARROWING")
            breathing = sum(1 for r in analyzed if r["label"] == "BREATHING")

            print("--- Summary ---")
            print(f"Analyzed: {len(analyzed)} threads")
            print(f"Breathing: {breathing}, Narrowing: {narrowing}, Closing: {closing}")
            print(f"Avg closure: {avg_closure:.2f}")

            if closing > 0:
                print("\n⚠️  CLOSING threads may need external input injection")
                print("   or the loop is breeding without external gain.")
    elif output_json:
        print(json.dumps(results, indent=2))

    db.close()


if __name__ == "__main__":
    main()
