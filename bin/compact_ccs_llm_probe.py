#!/usr/bin/env python3
"""
compact_ccs_llm_probe.py — Test advance 44 at the LLM-processing level.

compact_ccs_probe.py tested at the embedding level (cosine similarity) and was
inconclusive. Two JOCN papers (lateral frontal pole, phase dynamics) suggest the
advantage of compactness operates at the processing level, not similarity level.

This probe gives an LLM (qwen2.5:3b) the same questions with compact vs verbose
CCS as context, then scores the answers against ground truth using embeddings.

Method:
  1. Get current CCS, generate verbose and compact versions
  2. For each question: ask LLM with verbose context, ask with compact context
  3. Embed each answer, compare to effortful ground truth embedding
  4. Compare: does compact context produce better answers?
"""
import json
import math
import re
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434"
EMBED_MODEL = "mxbai-embed-large"
GEN_MODEL = "qwen2.5:3b"
RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "compact_ccs_probe"


def embed(text, timeout=60):
    text = text[:800]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def generate(prompt, timeout=120):
    body = json.dumps({
        "model": GEN_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 200}
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    data = json.loads(resp.read())
    return data.get("response", "")


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def get_ccs():
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
    if not value or value == "[]":
        return ""
    try:
        items = json.loads(value)
        if isinstance(items, list):
            compressed = []
            for item in items:
                if isinstance(item, str):
                    item = re.sub(r'\[\d{2}:\d{2}\]\s*', '', item)
                    item = re.sub(r'directive\(p\d+\):\s*', '', item)
                    item = ' '.join(item.split())
                    if item:
                        compressed.append(item)
                elif isinstance(item, dict):
                    name = item.get("name", item.get("entity", ""))
                    sal = item.get("salience", "")
                    if name:
                        compressed.append(f"{name}({sal})" if sal else name)
            value = "; ".join(compressed)
    except (json.JSONDecodeError, TypeError):
        pass
    value = re.sub(
        r'\b(the|a|an|is|are|was|were|has|have|had|this|that|these|those|which|who|whom)\b',
        '', value, flags=re.IGNORECASE
    )
    value = re.sub(r'\s{2,}', ' ', value).strip()
    if len(value) > 300:
        value = value[:300]
    return value


def ccs_to_compact(ccs):
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
    parts = []
    for field in [
        "semantic_gist", "goal_orientation", "focal_entities",
        "episodic_trace", "predictive_cue", "uncertainty_signals",
        "constraints", "relational_map"
    ]:
        val = ccs.get(field, "")
        compressed = compact_field(field, val)
        if compressed:
            parts.append(f"{short_labels.get(field, field)}: {compressed}")
    return "\n".join(parts)


def get_ground_truth_docs():
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


def keyword_filter(docs, question, limit=8):
    q_words = set(re.findall(r"[a-z]{3,}", question.lower()))
    scored = []
    for d in docs:
        d_lower = d.lower()
        hits = sum(1 for w in q_words if w in d_lower)
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


QUESTIONS = [
    "What is the current state of the active research thread?",
    "What captures arrived recently and what topics do they cover?",
    "What is the system's current operational status?",
    "What tools or probes were built recently?",
    "What is the relationship between Opus and Nate?",
    "What open questions or uncertainties exist right now?",
]


def run_probe():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading CCS...")
    ccs = get_ccs()
    verbose = ccs_to_verbose(ccs)
    compact = ccs_to_compact(ccs)

    print(f"Verbose: {len(verbose)} chars")
    print(f"Compact: {len(compact)} chars")
    print(f"Compression: {1 - len(compact)/len(verbose):.1%}")
    print()

    gt_docs = get_ground_truth_docs()
    print(f"Ground truth docs: {len(gt_docs)}")
    print()

    verbose_scores = {}
    compact_scores = {}
    answers = {}

    for q in QUESTIONS:
        print(f"  Q: {q}")

        # Get ground truth
        relevant = keyword_filter(gt_docs, q, limit=8)
        if not relevant:
            print(f"    [no relevant docs — skipping]")
            continue

        gt_text = " ".join(relevant)[:800]
        gt_emb = embed(gt_text)

        # Generate answers with verbose context
        v_prompt = f"Context:\n{verbose[:1500]}\n\nQuestion: {q}\n\nAnswer concisely:"
        v_answer = generate(v_prompt)
        print(f"    verbose answer: {v_answer[:80]}...")

        # Generate answers with compact context
        c_prompt = f"Context:\n{compact[:1500]}\n\nQuestion: {q}\n\nAnswer concisely:"
        c_answer = generate(c_prompt)
        print(f"    compact answer: {c_answer[:80]}...")

        # Embed answers and compare to ground truth
        v_emb = embed(v_answer[:800])
        c_emb = embed(c_answer[:800])

        v_score = round(cosine(v_emb, gt_emb), 4)
        c_score = round(cosine(c_emb, gt_emb), 4)

        verbose_scores[q] = v_score
        compact_scores[q] = c_score

        answers[q] = {
            "verbose_answer": v_answer[:500],
            "compact_answer": c_answer[:500],
        }

        delta = c_score - v_score
        marker = "+" if delta > 0 else ""
        print(f"    verbose={v_score:.4f}  compact={c_score:.4f}  delta={marker}{delta:.4f}")
        print()

    v_vals = list(verbose_scores.values())
    c_vals = list(compact_scores.values())
    v_mean = sum(v_vals) / len(v_vals) if v_vals else 0
    c_mean = sum(c_vals) / len(c_vals) if c_vals else 0

    print(f"VERBOSE mean: {v_mean:.4f}")
    print(f"COMPACT mean: {c_mean:.4f}")
    delta = c_mean - v_mean
    marker = "+" if delta > 0 else ""
    print(f"DELTA: {marker}{delta:.4f}")
    print()

    # Count wins
    c_wins = sum(1 for q in verbose_scores if compact_scores[q] > verbose_scores[q])
    v_wins = sum(1 for q in verbose_scores if verbose_scores[q] > compact_scores[q])
    ties = len(verbose_scores) - c_wins - v_wins
    print(f"Compact wins: {c_wins}  Verbose wins: {v_wins}  Ties: {ties}")

    if delta > 0.005:
        print("RESULT: Compact wins at LLM level — advance 44+45 prediction CONFIRMED")
    elif delta < -0.005:
        print("RESULT: Verbose wins — compactness hurts LLM processing")
    else:
        print("RESULT: Within noise — inconclusive at LLM level too")

    result = {
        "timestamp": int(time.time()),
        "probe_type": "llm_level",
        "gen_model": GEN_MODEL,
        "embed_model": EMBED_MODEL,
        "verbose_chars": len(verbose),
        "compact_chars": len(compact),
        "compression_ratio": round(1 - len(compact) / len(verbose), 3),
        "verbose_scores": verbose_scores,
        "compact_scores": compact_scores,
        "verbose_mean": round(v_mean, 4),
        "compact_mean": round(c_mean, 4),
        "delta": round(delta, 4),
        "compact_wins": c_wins,
        "verbose_wins": v_wins,
        "questions": QUESTIONS,
        "answers": answers,
    }
    out = RESULTS_DIR / f"llm_probe_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"Saved: {out}")


if __name__ == "__main__":
    run_probe()
