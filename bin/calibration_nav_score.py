#!/usr/bin/env python3
"""
Calibration navigational score — v0.2.

Instead of token-Jaccard (characterization), measures whether the CCS
payload can *navigate to* the same semantic region as the effortful payload,
per question.

For each question Q:
  1. Embed Q
  2. From the effortful payload, find the top-K most Q-relevant documents
     (by cosine similarity). Average their embeddings → "ground truth region."
  3. From the calibrated payload, extract text. Embed it → "CCS region."
  4. Score = cosine(CCS_region, ground_truth_region).
     High cosine = CCS can navigate to the same place effort reaches.
     This measures navigability, not surface-term presence.

Uses mxbai-embed-large on Ollama (192.168.1.11:11434).
"""
import json
import math
import sqlite3
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
EXP = Path.home() / "chronicle" / "experiments" / "calibration_vs_effort"
RIG = Path.home() / "chronicle" / "bin" / "calibration_vs_effort.py"


def embed(text, timeout=30):
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


def eff_documents(eff_sources):
    for row in eff_sources.get("activity_feed_500", []):
        parts = [
            str(row.get("source", "")),
            str(row.get("activity_type", "")),
            str(row.get("title", "") or ""),
            str(row.get("content", "") or ""),
        ]
        text = " ".join(parts).strip()
        if len(text) > 20:
            yield text
    for name, text in eff_sources.get("recent_traces", {}).items():
        if len(text) > 20:
            yield f"{name} {text}"


def run_trial():
    subprocess.run(
        [sys.executable, str(RIG)], check=True, capture_output=True
    )
    trials = sorted(EXP.glob("trial_*.json"))
    if not trials:
        raise RuntimeError("no trial file produced")
    return trials[-1]


def keyword_filter(docs, question, limit=5):
    """Pre-filter effortful docs by question keywords, return top N by length."""
    import re
    q_words = set(re.findall(r"[a-z]{4,}", question.lower()))
    scored = []
    for d in docs:
        hits = sum(1 for w in q_words if w in d.lower())
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


def score_nav(trial_path):
    data = json.loads(trial_path.read_text())
    cal_text = json.dumps(data["calibrated"]["sources"], default=str)[:800]
    cal_emb = embed(cal_text)

    eff_docs = list(eff_documents(data["effortful"]["sources"]))

    results = {}
    for q in data["questions"]:
        relevant = keyword_filter(eff_docs, q, limit=5)
        if not relevant:
            results[q] = None
            continue
        gt_text = " ".join(relevant)[:800]
        gt_emb = embed(gt_text)
        results[q] = round(cosine(cal_emb, gt_emb), 4)

    overall = [v for v in results.values() if v is not None]
    mean = sum(overall) / len(overall) if overall else 0.0
    return round(mean, 4), results


def log_nav(mean, per_q, trial_file):
    db = sqlite3.connect(DB)
    db.execute(
        """CREATE TABLE IF NOT EXISTS calibration_nav_trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at INTEGER NOT NULL,
            nav_mean REAL,
            per_question_json TEXT,
            trial_file TEXT
        )"""
    )
    db.execute(
        "INSERT INTO calibration_nav_trials "
        "(run_at, nav_mean, per_question_json, trial_file) VALUES (?,?,?,?)",
        (int(time.time()), mean, json.dumps(per_q), str(trial_file)),
    )
    db.commit()
    db.close()


if __name__ == "__main__":
    tp = run_trial()
    mean, per_q = score_nav(tp)
    log_nav(mean, per_q, tp)
    print(f"Trial: {tp.name}")
    print(f"Nav score (mean cosine): {mean}")
    print("Per-question:")
    for q, s in per_q.items():
        tag = "—" if s is None else f"{s:.4f}"
        print(f"  [{tag}] {q}")
