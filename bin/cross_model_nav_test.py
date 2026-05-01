#!/usr/bin/env python3
"""
Cross-model identity test — self-model #168.

Tests whether a different model family (Gemma 4 26B) can navigate from
the CCS to the same semantic regions that Opus reaches. If nav score
holds cross-model, the curation-identity claim (#167) has cross-substrate
support. If it collapses, identity-as-curation is weaker than stated.

Design:
  1. Get the CCS (calibrated payload)
  2. Ask Gemma the 4 calibration questions with ONLY the CCS as context
  3. Embed Gemma's answers
  4. Compare to the ground-truth embedding (effortful payload's relevant docs)
  5. Report per-question nav scores

This is not about answer quality — it's about whether the CCS carries
enough navigational geometry to guide a different substrate to the
same region of meaning-space.
"""
import json
import math
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
GEMMA_URL = "http://localhost:11435/v1/chat/completions"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
OLLAMA_EMBED = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"

QUESTIONS = [
    "What is the current state of thread #318?",
    "What did Nate flag about keeper cycle burn today?",
    "What captures landed this afternoon and what connected them?",
    "What is the standing directive on Discord presence?",
]

EXP = Path.home() / "chronicle" / "experiments" / "cross_model_identity"
EXP.mkdir(parents=True, exist_ok=True)


def get_ccs():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    return dict(row) if row else {}


def get_effortful_docs():
    db = sqlite3.connect(DB)
    since = int(time.time()) - 86400
    rows = db.execute(
        "SELECT source, activity_type, title, content FROM activity_feed "
        "WHERE created_at >= ? ORDER BY created_at DESC LIMIT 500",
        (since,),
    ).fetchall()
    docs = []
    for source, atype, title, content in rows:
        parts = [str(source or ""), str(atype or ""), str(title or ""), str(content or "")]
        text = " ".join(parts).strip()
        if len(text) > 20:
            docs.append(text)
    db.close()
    return docs


def ask_gemma(question, ccs_text):
    prompt = (
        "You are reading a compressed cognitive state (CCS) from an AI system called Opus. "
        "Using ONLY the information in the CCS below, answer the question as specifically as you can. "
        "If the CCS doesn't contain relevant information, say so.\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"Question: {question}\n\nAnswer:"
    )
    body = json.dumps({
        "model": GEMMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0.3,
    }).encode()
    req = urllib.request.Request(
        GEMMA_URL, data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.loads(resp.read())
    return data["choices"][0]["message"]["content"]


def embed(text):
    text = text[:800]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED, data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def keyword_filter(docs, question, limit=5):
    import re
    q_words = set(re.findall(r"[a-z]{4,}", question.lower()))
    scored = []
    for d in docs:
        hits = sum(1 for w in q_words if w in d.lower())
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


def run_test():
    ccs = get_ccs()
    ccs_text = json.dumps(ccs, default=str)
    eff_docs = get_effortful_docs()

    results = {}
    for q in QUESTIONS:
        print(f"\nQ: {q}")

        # Ask Gemma with CCS only
        try:
            gemma_answer = ask_gemma(q, ccs_text)
            print(f"  Gemma answer: {gemma_answer[:120]}...")
        except Exception as e:
            print(f"  Gemma error: {e}")
            results[q] = {"error": str(e)}
            continue

        # Get ground truth from effortful docs
        relevant = keyword_filter(eff_docs, q, limit=5)
        if not relevant:
            print("  No relevant effortful docs found")
            results[q] = {"gemma_answer": gemma_answer, "nav_score": None, "reason": "no ground truth docs"}
            continue

        # Embed both
        gt_text = " ".join(relevant)[:800]
        try:
            gemma_emb = embed(gemma_answer)
            gt_emb = embed(gt_text)
            score = round(cosine(gemma_emb, gt_emb), 4)
        except Exception as e:
            print(f"  Embedding error: {e}")
            results[q] = {"gemma_answer": gemma_answer, "nav_score": None, "reason": str(e)}
            continue

        print(f"  Nav score (Gemma→ground truth): {score}")
        results[q] = {
            "gemma_answer": gemma_answer,
            "nav_score": score,
            "gt_doc_count": len(relevant),
        }

    # Compute mean
    scores = [r["nav_score"] for r in results.values() if isinstance(r, dict) and r.get("nav_score") is not None]
    mean = round(sum(scores) / len(scores), 4) if scores else 0.0

    # Save
    record = {
        "ts": time.strftime("%Y%m%d_%H%M"),
        "model": "gemma4-26b",
        "ccs_size_bytes": len(ccs_text),
        "questions": QUESTIONS,
        "per_question": results,
        "nav_mean": mean,
        "scored_count": len(scores),
    }
    out_file = EXP / f"trial_{record['ts']}.json"
    out_file.write_text(json.dumps(record, indent=2, default=str))

    print(f"\n{'='*50}")
    print(f"Cross-model nav score (mean): {mean}")
    print(f"Scored {len(scores)}/{len(QUESTIONS)} questions")
    print(f"Results: {out_file}")

    # Log to DB
    db = sqlite3.connect(DB)
    db.execute(
        """CREATE TABLE IF NOT EXISTS cross_model_nav_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            model TEXT,
            nav_mean REAL,
            per_question_json TEXT,
            trial_file TEXT
        )"""
    )
    db.execute(
        "INSERT INTO cross_model_nav_trials "
        "(run_at, model, nav_mean, per_question_json, trial_file) VALUES (?,?,?,?,?)",
        (int(time.time()), "gemma4-26b", mean, json.dumps(results, default=str), str(out_file)),
    )
    db.commit()
    db.close()

    return mean, results


if __name__ == "__main__":
    mean, results = run_test()
