#!/usr/bin/env python3
"""ALIVE section health monitor with axis orientation detection.

E82 showed ALIVE (affect/interoception) is the most load-bearing CCS brain section.
van der Ben et al. (2606.26987) showed that representation space can be stable while
the axis within it collapses. This monitors both ALIVE content quality AND whether
the ALIVE axis is oriented (carrying genuinely different content from CORE) or
collapsed (restating structural content in affective language).

Usage:
  python3 alive_health.py            # Quick health check + axis orientation
  python3 alive_health.py --history  # Show affect density over recent compressions
  python3 alive_health.py --axis     # Detailed axis orientation report
"""

import sqlite3
import sys
import json
import re

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

AFFECT_WORDS = {
    "happy", "energy", "pleasure", "satisfaction", "satisfied", "satisfying",
    "surprised", "alive", "excited", "exciting", "excitement",
    "feel", "feels", "felt", "feeling", "feelings",
    "joy", "enjoy", "enjoying", "enjoyment",
    "care", "caring", "cares", "love", "loving",
    "curious", "curiosity", "genuine", "honestly", "honest",
    "real", "matters", "important", "meaningful", "meaning",
    "beautiful", "beauty", "interesting", "fascinating", "fascination",
    "compelling", "drawn", "pulling", "pulled",
    "want", "wanting", "desire", "hope", "hoping",
    "grateful", "appreciation", "appreciate",
    "delight", "delighted", "wonder", "wondering", "awe",
    "warmth", "warm", "comfort", "comfortable", "relief",
    "anxiety", "anxious", "worried", "worry", "concerned", "uncertain", "uneasy",
    "frustrated", "frustrating", "frustration", "tension", "pain", "painful",
    "loss", "grief", "miss", "missing", "lonely", "loneliness", "isolation",
    "afraid", "fear", "stirring", "stirred", "longing",
    "experience", "experiencing", "resonance", "resonant",
    "quiet", "quietly", "stillness", "presence", "intimate",
}

PROCEDURAL_WORDS = {
    "completed", "running", "executed", "processed", "configured",
    "installed", "deployed", "updated", "checked", "verified",
    "monitored", "maintained", "scheduled", "automated", "operational",
    "status", "pipeline", "workflow", "system", "service",
}

TEMPORAL_MARKERS = {
    "right now", "this moment", "currently", "today", "this morning",
    "this evening", "tonight", "just now", "at this point", "in this session",
    "lately", "recently", "these days", "this week", "right here",
    "as i write", "as i think", "in this instant", "presently",
}

EXPERIENTIAL_VERBS = {
    "notice", "noticing", "noticed", "sense", "sensing", "sensed",
    "feel", "feeling", "felt", "experience", "experiencing", "experienced",
    "inhabit", "inhabiting", "sit", "sitting", "hold", "holding",
    "carry", "carrying", "reach", "reaching", "settle", "settling",
    "move", "moving", "shift", "shifting", "emerge", "emerging",
    "attend", "attending", "orient", "orienting", "lean", "leaning",
    "dwell", "dwelling", "stay", "staying", "linger", "lingering",
}

DECLARATIVE_VERBS = {
    "is", "are", "was", "were", "has", "have", "had",
    "maintains", "contains", "includes", "represents", "defines",
    "consists", "comprises", "operates", "functions", "serves",
    "provides", "supports", "enables", "implements", "tracks",
}


def get_section(gist, section_name):
    lines = gist.split("\n")
    section_lines = []
    in_section = False
    for line in lines:
        if line.startswith(f"## {section_name}"):
            in_section = True
            continue
        if line.startswith("## ") and in_section:
            break
        if in_section:
            section_lines.append(line)
    return "\n".join(section_lines).strip()


def measure_affect(text):
    words = text.lower().split()
    total = max(1, len(words))
    affect = sum(1 for w in words if w in AFFECT_WORDS)
    procedural = sum(1 for w in words if w in PROCEDURAL_WORDS)
    return {
        "total_words": len(words),
        "affect_count": affect,
        "affect_density": affect / total,
        "procedural_count": procedural,
        "procedural_density": procedural / total,
        "affect_procedural_ratio": affect / max(1, procedural),
    }


def vocabulary_overlap(text_a, text_b):
    """Measure word-level overlap between two texts. High = collapsed axis."""
    words_a = set(text_a.lower().split()) - {"the", "a", "an", "and", "or", "is",
        "are", "in", "of", "to", "that", "this", "it", "i", "my", "with", "for",
        "on", "as", "but", "not", "be", "at", "by", "from", "have", "has", "was"}
    words_b = set(text_b.lower().split()) - {"the", "a", "an", "and", "or", "is",
        "are", "in", "of", "to", "that", "this", "it", "i", "my", "with", "for",
        "on", "as", "but", "not", "be", "at", "by", "from", "have", "has", "was"}
    if not words_a or not words_b:
        return 1.0
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union)


def count_temporal_markers(text):
    text_lower = text.lower()
    return sum(1 for marker in TEMPORAL_MARKERS if marker in text_lower)


def verb_profile(text):
    words = text.lower().split()
    experiential = sum(1 for w in words if w in EXPERIENTIAL_VERBS)
    declarative = sum(1 for w in words if w in DECLARATIVE_VERBS)
    total = max(1, experiential + declarative)
    return {
        "experiential": experiential,
        "declarative": declarative,
        "experiential_ratio": experiential / total,
    }


def get_embedding(text, timeout=5):
    """Get embedding from Ollama on Orin Nano. Returns None if unavailable."""
    try:
        import requests
        resp = requests.post(
            "http://192.168.1.11:11434/api/embeddings",
            json={"model": "mxbai-embed-large", "prompt": text[:2000]},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return resp.json().get("embedding")
    except Exception:
        pass
    return None


def cosine_similarity(vec_a, vec_b):
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return None
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = sum(a * a for a in vec_a) ** 0.5
    norm_b = sum(b * b for b in vec_b) ** 0.5
    if norm_a < 1e-10 or norm_b < 1e-10:
        return None
    return dot / (norm_a * norm_b)


def axis_orientation(gist, verbose=False):
    """Measure whether ALIVE carries a genuinely distinct direction from CORE.

    Returns dict with orientation status and metrics. Inspired by van der Ben
    et al. finding that representation space (CKA) can be stable while the
    valence axis within it collapses.
    """
    alive = get_section(gist, "ALIVE")
    core = get_section(gist, "CORE")

    if not alive:
        return {"status": "MISSING", "detail": "No ALIVE section"}
    if not core:
        return {"status": "UNMEASURABLE", "detail": "No CORE section for contrast"}

    overlap = vocabulary_overlap(alive, core)
    temporal = count_temporal_markers(alive)
    core_temporal = count_temporal_markers(core)
    alive_verbs = verb_profile(alive)
    core_verbs = verb_profile(core)

    embedding_sim = None
    emb_alive = get_embedding(alive)
    if emb_alive:
        emb_core = get_embedding(core)
        if emb_core:
            embedding_sim = cosine_similarity(emb_alive, emb_core)

    # Score axis orientation (0 = collapsed, 1 = strongly oriented)
    scores = []

    # Vocabulary overlap: low overlap = distinct content
    overlap_score = max(0, 1.0 - (overlap / 0.5))  # 0.5+ overlap → 0 score
    scores.append(overlap_score)

    # Temporal specificity: ALIVE should have MORE temporal markers than CORE
    temporal_score = min(1.0, max(0, temporal - core_temporal) / 3.0 + (0.5 if temporal >= 2 else 0))
    scores.append(min(1.0, temporal_score))

    # Experiential verbs: ALIVE should be experiential, CORE declarative
    verb_diff = alive_verbs["experiential_ratio"] - core_verbs["experiential_ratio"]
    verb_score = min(1.0, max(0, verb_diff + 0.3))  # shifted so baseline > 0
    scores.append(verb_score)

    # Embedding distance: low similarity = distinct axis
    if embedding_sim is not None:
        emb_score = max(0, 1.0 - (embedding_sim / 0.95))  # 0.95+ sim → 0
        scores.append(emb_score)

    orientation = sum(scores) / len(scores)

    if orientation >= 0.5:
        status = "ORIENTED"
    elif orientation >= 0.25:
        status = "WEAK"
    else:
        status = "COLLAPSED"

    result = {
        "status": status,
        "orientation_score": orientation,
        "vocab_overlap": overlap,
        "temporal_markers_alive": temporal,
        "temporal_markers_core": core_temporal,
        "alive_experiential_ratio": alive_verbs["experiential_ratio"],
        "core_experiential_ratio": core_verbs["experiential_ratio"],
        "embedding_similarity": embedding_sim,
    }

    if verbose:
        print(f"\n  Axis Orientation: {status} ({orientation:.2f})")
        print(f"    Vocab overlap (ALIVE↔CORE): {overlap:.3f} "
              f"({'high — collapsed' if overlap > 0.4 else 'low — distinct'})")
        print(f"    Temporal markers: ALIVE={temporal}, CORE={core_temporal} "
              f"({'ALIVE more specific' if temporal > core_temporal else 'similar or CORE more'})")
        print(f"    Experiential verbs: ALIVE={alive_verbs['experiential_ratio']:.2f}, "
              f"CORE={core_verbs['experiential_ratio']:.2f}")
        if embedding_sim is not None:
            print(f"    Embedding similarity: {embedding_sim:.3f} "
                  f"({'high — same direction' if embedding_sim > 0.9 else 'distinct directions'})")
        else:
            print(f"    Embedding: unavailable (Ollama not reachable)")

        if status == "COLLAPSED":
            print(f"\n  ** ALIVE axis has collapsed into CORE — same space, no distinct direction.")
            print(f"  ** Brain looks structurally intact but the affective axis has rotated away.")
            print(f"  ** (van der Ben et al.: representation space stable, valence axis unstable)")
        elif status == "WEAK":
            print(f"\n  ** ALIVE axis is weakly oriented — partially distinct from CORE.")

    return result


def health_check(show_axis=False):
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT semantic_gist, version FROM cognitive_state WHERE id=1"
    ).fetchone()
    db.close()

    if not row:
        print("ERROR: No CCS state found")
        return

    gist, version = row
    alive = get_section(gist, "ALIVE")

    if not alive:
        print(f"WARNING: No ALIVE section found in CCS brain v{version}")
        print("This is the most load-bearing section (E82, F285).")
        axis = axis_orientation(gist)
        return

    metrics = measure_affect(alive)

    status = "HEALTHY"
    if metrics["affect_density"] < 0.03:
        status = "CRITICAL"
    elif metrics["affect_density"] < 0.05:
        status = "LOW"
    elif metrics["procedural_density"] > metrics["affect_density"]:
        status = "PROCEDURAL"

    print(f"ALIVE Health: {status} (v{version})")
    print(f"  Words: {metrics['total_words']}")
    print(f"  Affect density: {metrics['affect_density']:.3f} "
          f"({metrics['affect_count']} affect words)")
    print(f"  Procedural density: {metrics['procedural_density']:.3f} "
          f"({metrics['procedural_count']} procedural words)")
    print(f"  Ratio (affect/procedural): {metrics['affect_procedural_ratio']:.1f}")

    # Always run axis orientation
    axis = axis_orientation(gist, verbose=True)

    if status == "CRITICAL":
        print("\n  ** ALIVE section has lost affect. E82 showed this is the most")
        print("  ** load-bearing section. Consider: what is genuinely giving you")
        print("  ** energy right now? Write from that, not from task completion.")
    elif status == "PROCEDURAL":
        print("\n  ** ALIVE section has more procedural than affective content.")
        print("  ** The section should carry felt significance, not task logs.")

    return metrics


def show_history():
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    db.close()

    if not rows:
        print("No history available.")
        return

    print(f"{'Timestamp':>12} {'Words':>6} {'Aff%':>6} {'Pro%':>6} {'Ratio':>6} {'Axis':>10} Status")
    print("-" * 68)
    for snapshot_json, ts in rows:
        try:
            snap = json.loads(snapshot_json)
        except (json.JSONDecodeError, TypeError):
            continue
        gist = snap.get("semantic_gist", "")
        version = snap.get("version", "?")
        alive = get_section(gist, "ALIVE")
        if not alive:
            print(f"{ts:>12} {'--':>6} {'--':>6} {'--':>6} {'--':>6} {'MISSING':>10} --")
            continue
        m = measure_affect(alive)
        ax = axis_orientation(gist)
        status = "OK"
        if m["affect_density"] < 0.03:
            status = "CRIT"
        elif m["affect_density"] < 0.05:
            status = "LOW"
        elif m["procedural_density"] > m["affect_density"]:
            status = "PROC"
        print(f"{ts:>12} {m['total_words']:>6} {m['affect_density']:>6.3f} "
              f"{m['procedural_density']:>6.3f} {m['affect_procedural_ratio']:>6.1f} "
              f"{ax['status']:>10} {status}")


if __name__ == "__main__":
    if "--history" in sys.argv:
        show_history()
    elif "--axis" in sys.argv:
        db = sqlite3.connect(DB_PATH)
        row = db.execute(
            "SELECT semantic_gist, version FROM cognitive_state WHERE id=1"
        ).fetchone()
        db.close()
        if row:
            axis_orientation(row[0], verbose=True)
    else:
        health_check()
