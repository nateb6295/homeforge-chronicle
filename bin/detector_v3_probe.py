#!/usr/bin/env python3
"""
Detector v3: logprob-weighted verifier halt signal.

Failed halt signals (prior):
- ans-to-ans cosine >= 0.95 — never triggered; prose varies at steady state
- score-delta plateau — too noisy at n=4 questions

New signal (from webbigdata LLM-as-Verifier capture 20260416_0602):
  Have Gemma score its OWN iter-k answer 1-8 for coverage.
  Weight by top-1 token logprob (confidence).
  confidence_weighted_score = score * exp(top_logprob)

  Hypothesis: when the composition saturates, the model's self-score
  plateaus AND its confidence in that score rises. Halt when
  confidence-weighted score stops gaining for 2 consecutive iters.

Usage:
  python3 detector_v3_probe.py
  reads latest adaptive trial JSON, re-verifies each scratch at each iter,
  emits per-question per-iter verifier score + logprob to a new JSON.
"""
import json
import math
import re
import sys
import time
import urllib.request
from pathlib import Path

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"

EXP = Path.home() / "chronicle" / "experiments" / "recurrent_nav"


def verifier_score(question, scratch, ccs_text):
    """Ask Gemma to rate scratch coverage 1-8 with logprobs on."""
    prompt = (
        "You are a verifier. Below is a compressed cognitive state (CCS) "
        "and a candidate answer to a question about it.\n"
        "Rate the answer's coverage of the CCS on a scale of 1 to 8, where:\n"
        "  1 = misses most of what CCS contains on this topic\n"
        "  4 = covers the main points but leaves composition unexplored\n"
        "  8 = fully composes every relevant fact in the CCS\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"--- CANDIDATE ANSWER ---\n{scratch[:1200]}\n--- END ---\n\n"
        f"Question: {question}\n\n"
        "Reply with exactly one digit 1-8. Nothing else."
    )
    body = json.dumps({
        "model": GEMMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 3,
        "temperature": 0.0,
        "logprobs": True,
        "top_logprobs": 5,
    }).encode()
    req = urllib.request.Request(
        GEMMA_URL, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.loads(resp.read())
    content = data["choices"][0]["message"]["content"]
    lp = data["choices"][0].get("logprobs", {}).get("content", [])

    m = re.search(r"[1-8]", content)
    score = int(m.group()) if m else None

    # Find the first token that is a digit; use its top_logprobs
    top_lp = None
    full_dist = None
    for tok in lp:
        if re.match(r"\s*[1-8]\s*$", tok.get("token", "")):
            top_lp = tok.get("logprob")
            full_dist = [
                (t.get("token", "").strip(), t.get("logprob"))
                for t in tok.get("top_logprobs", [])
            ]
            break

    # Expected score across top_logprobs distribution (over digits 1-8)
    exp_score = None
    if full_dist:
        num = 0.0
        den = 0.0
        for tok, logp in full_dist:
            if re.match(r"^[1-8]$", tok):
                p = math.exp(logp)
                num += int(tok) * p
                den += p
        if den > 0:
            exp_score = num / den

    return {
        "raw_content": content,
        "score": score,
        "top_logprob": top_lp,
        "top_confidence": math.exp(top_lp) if top_lp is not None else None,
        "full_dist": full_dist,
        "expected_score": exp_score,
    }


def latest_trial():
    files = sorted(EXP.glob("trial_adaptive_*.json"))
    if not files:
        return None
    return files[-1]


def load_ccs_text():
    import sqlite3
    db = sqlite3.connect("/mnt/hdd/chronicle-data/processed.db")
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT * FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    if not row:
        return ""
    d = dict(row)
    return json.dumps(d, default=str)


def main():
    trial_path = latest_trial()
    if not trial_path:
        print("no adaptive trial found")
        sys.exit(1)
    print(f"probing: {trial_path.name}")
    trial = json.loads(trial_path.read_text())

    ccs_text = load_ccs_text()
    if not ccs_text:
        print("no CCS available")
        sys.exit(1)

    per_q = trial.get("per_question", {})
    out_entries = []
    t0 = time.time()
    for q, entry in per_q.items():
        print(f"\nQ: {q[:60]}")
        vents = []
        for k in range(1, 6):
            ans_key = f"iter{k}_answer"
            ans = entry.get(ans_key)
            if not ans:
                continue
            try:
                v = verifier_score(q, ans, ccs_text)
            except Exception as e:
                print(f"  iter{k}: ERR {e}")
                continue
            v["k"] = k
            v["nav_score"] = entry.get(f"iter{k}_score")
            vents.append(v)
            conf = v["top_confidence"] or 0
            exp_s = v["expected_score"] or 0
            cw = (v["score"] or 0) * conf
            print(f"  iter{k}: score={v['score']} conf={conf:.3f} "
                  f"exp={exp_s:.2f} cw={cw:.2f} nav={v['nav_score']}")
        out_entries.append({"question": q, "verifications": vents})

    ts = time.strftime("%Y%m%d_%H%M")
    out_path = EXP / f"detector_v3_{ts}.json"
    out_path.write_text(json.dumps({
        "source_trial": trial_path.name,
        "entries": out_entries,
        "elapsed_s": time.time() - t0,
    }, indent=2))
    print(f"\nwrote: {out_path}")


if __name__ == "__main__":
    main()
