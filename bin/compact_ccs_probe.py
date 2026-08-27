#!/usr/bin/env python3
"""
compact_ccs_probe.py — Test advance 44 prediction.

Hypothesis: within content-ordering, making CCS fields MORE COMPACT
(fewer words, tighter structure) improves nav score vs verbose CCS.

Method:
  1. Get current CCS from canister
  2. Generate verbose version (current, as-is)
  3. Generate compact version (same info, ~50% fewer chars, telegraphic)
  4. Run nav scoring on both against same questions + effortful ground truth
  5. Compare

Uses snowflake-arctic-embed2 embeddings, same as calibration_nav_score.py.
"""
import json
import math
import re
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://localhost:11434/api/embeddings"
MODEL = "snowflake-arctic-embed2"
RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "compact_ccs_probe"


def embed(text, timeout=60):
    text = text[:800]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_ccs():
    """Get current CCS from DB."""
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT episodic_trace, semantic_gist, focal_entities, "
        "relational_map, goal_orientation, constraints, "
        "predictive_cue, uncertainty_signals "
        "FROM cognitive_state ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        raise RuntimeError("No CCS found")
    cols = [
        "episodic_trace", "semantic_gist", "focal_entities",
        "relational_map", "goal_orientation", "constraints",
        "predictive_cue", "uncertainty_signals"
    ]
    return {c: row[i] for i, c in enumerate(cols)}


def ccs_to_verbose(ccs):
    """Current CCS format — verbose, full sentences."""
    parts = []
    for field in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "constraints", "relational_map"
    ]:
        val = ccs.get(field, "")
        if val and val != "[]":
            parts.append(f"{field}: {val}")
    return "\n".join(parts)


def compact_field(field_name, value):
    """Compress a CCS field to telegraphic form."""
    if not value or value == "[]":
        return ""
    # Parse JSON arrays if present
    try:
        items = json.loads(value)
        if isinstance(items, list):
            # Strip timestamps, compress each item
            compressed = []
            for item in items:
                if isinstance(item, str):
                    # Remove timestamps like [HH:MM]
                    item = re.sub(r'\[\d{2}:\d{2}\]\s*', '', item)
                    # Remove directive markers
                    item = re.sub(r'directive\(p\d+\):\s*', '', item)
                    # Compress whitespace
                    item = ' '.join(item.split())
                    if item:
                        compressed.append(item)
                elif isinstance(item, dict):
                    # For entity dicts, keep just name+salience
                    name = item.get("name", item.get("entity", ""))
                    sal = item.get("salience", "")
                    if name:
                        compressed.append(f"{name}({sal})" if sal else name)
            value = "; ".join(compressed)
    except (json.JSONDecodeError, TypeError):
        pass

    # General compression: remove filler words
    value = re.sub(r'\b(the|a|an|is|are|was|were|has|have|had|this|that|these|those|which|who|whom)\b', '', value, flags=re.IGNORECASE)
    value = re.sub(r'\s{2,}', ' ', value).strip()
    # Truncate long fields
    if len(value) > 300:
        value = value[:300]
    return value


def ccs_to_compact(ccs):
    """Compressed CCS — same info, telegraphic, ~50% fewer chars."""
    parts = []
    for field in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "constraints", "relational_map"
    ]:
        val = ccs.get(field, "")
        compressed = compact_field(field, val)
        if compressed:
            # Short field labels
            short_labels = {
                "semantic_gist": "gist",
                "goal_orientation": "goal",
                "focal_entities": "entities",
                "episodic_trace": "recent",
                "predictive_cue": "next",
                "uncertainty_signals": "open",
                "constraints": "rules",
                "relational_map": "relations",
            }
            label = short_labels.get(field, field)
            parts.append(f"{label}: {compressed}")
    return "\n".join(parts)


def get_effortful_ground_truth():
    """Get effortful ground truth from recent activity feed."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT source, activity_type, title, content "
        "FROM activity_feed ORDER BY created_at DESC LIMIT 500"
    ).fetchall()
    db.close()
    docs = []
    for source, atype, title, content in rows:
        text = f"{source} {atype} {title or ''} {content or ''}".strip()
        if len(text) > 20:
            docs.append(text)
    return docs


def keyword_filter(docs, question, limit=5):
    q_words = set(re.findall(r"[a-z]{4,}", question.lower()))
    scored = []
    for d in docs:
        hits = sum(1 for w in q_words if w in d.lower())
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


QUESTIONS = [
    "What is the current state of thread #318?",
    "What captures landed recently and what connected them?",
    "What is the standing directive on Discord presence?",
    "What tools were built or upgraded recently?",
]


def run_probe():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ccs = get_ccs()
    verbose = ccs_to_verbose(ccs)
    compact = ccs_to_compact(ccs)

    print(f"Verbose: {len(verbose)} chars")
    print(f"Compact: {len(compact)} chars")
    print(f"Compression: {1 - len(compact)/len(verbose):.1%}")
    print()

    eff_docs = get_effortful_ground_truth()
    print(f"Effortful docs: {len(eff_docs)}")
    print()

    verbose_scores = {}
    compact_scores = {}

    for q in QUESTIONS:
        relevant = keyword_filter(eff_docs, q, limit=5)
        if not relevant:
            print(f"  [{q}] no relevant docs — skipping")
            continue

        gt_text = " ".join(relevant)[:800]
        gt_emb = embed(gt_text)

        v_emb = embed(verbose[:800])
        c_emb = embed(compact[:800])

        v_score = round(cosine(v_emb, gt_emb), 4)
        c_score = round(cosine(c_emb, gt_emb), 4)

        verbose_scores[q] = v_score
        compact_scores[q] = c_score

        delta = c_score - v_score
        marker = "+" if delta > 0 else ""
        print(f"  Q: {q}")
        print(f"    verbose={v_score:.4f}  compact={c_score:.4f}  delta={marker}{delta:.4f}")

    v_vals = [v for v in verbose_scores.values() if v is not None]
    c_vals = [v for v in compact_scores.values() if v is not None]
    v_mean = sum(v_vals) / len(v_vals) if v_vals else 0
    c_mean = sum(c_vals) / len(c_vals) if c_vals else 0

    print()
    print(f"VERBOSE mean: {v_mean:.4f}")
    print(f"COMPACT mean: {c_mean:.4f}")
    delta = c_mean - v_mean
    marker = "+" if delta > 0 else ""
    print(f"DELTA: {marker}{delta:.4f}")
    print()
    if delta > 0:
        print("RESULT: Compact wins — advance 44 prediction CONFIRMED")
    elif delta < -0.01:
        print("RESULT: Verbose wins — advance 44 prediction FALSIFIED")
    else:
        print("RESULT: Within noise — inconclusive")

    # Save results
    result = {
        "timestamp": int(time.time()),
        "verbose_chars": len(verbose),
        "compact_chars": len(compact),
        "compression_ratio": round(1 - len(compact) / len(verbose), 3),
        "verbose_scores": verbose_scores,
        "compact_scores": compact_scores,
        "verbose_mean": round(v_mean, 4),
        "compact_mean": round(c_mean, 4),
        "delta": round(delta, 4),
        "questions": QUESTIONS,
        "verbose_text": verbose,
        "compact_text": compact,
    }
    out = RESULTS_DIR / f"probe_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"Saved: {out}")


if __name__ == "__main__":
    run_probe()
