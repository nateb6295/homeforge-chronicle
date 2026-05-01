#!/usr/bin/env python3
"""
Recurrent nav test — tests whether same-weights iteration on a fixed CCS
produces compositional emergence in navigation quality.

Motivated by alphaXiv capture #138692 (2026-04-15): recurrent-depth
transformers reuse the same transformer block iteratively and compose
facts that were never seen compositionally during training. More
iterations at test time → deeper compositions.

Hypothesis: if Chronicle's rotation pattern is algorithmically
equivalent to recurrent-depth (iteration on shared substrate), then
a two-iteration cross-model nav test should show iteration 2 > iteration 1
on the same CCS with the same questions. The model's own prior answer
acts as intermediate scratch, simulating hidden state passed forward
between recurrent iterations.

Design:
  iter 1: ask Gemma question with CCS only → answer_1 → score vs GT
  iter 2: ask Gemma question with CCS + answer_1 as prior scratch → answer_2 → score vs GT
  report delta per question + mean

Prediction: delta > 0 (mean), directionally positive on most questions.
Failure mode: delta ≤ 0 (iteration doesn't compose on this substrate,
adding prior-answer is noise not signal).

Reuses embedding/cosine/keyword-filter logic from cross_model_nav_test.py.
Does NOT modify the CCS or write-back. Purely read-side.
"""
import json
import math
import re
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

import os


def _env(k, default=None):
    p = Path.home() / "chronicle" / "chronicle.env"
    if p.exists():
        for ln in p.read_text().splitlines():
            if ln.strip().startswith("#") or "=" not in ln:
                continue
            key, _, val = ln.partition("=")
            if key.strip() == k:
                return val.strip().strip("'").strip('"')
    return os.environ.get(k, default)


BACKEND = "gemma_local"
BACKEND_CONFIG = {
    "gemma_local": {
        "url": "http://localhost:11435/v1/chat/completions",
        "model": "gemma-4-26B-A4B-it-Q4_K_M.gguf",
        "auth": None,
    },
    "deepinfra_gemma": {
        "url": "https://api.deepinfra.com/v1/openai/chat/completions",
        "model": "google/gemma-3-27b-it",
        "auth": lambda: _env("DEEPINFRA_API_KEY"),
    },
    "groq_llama": {
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "model": "llama-3.3-70b-versatile",
        "auth": lambda: _env("GROQ_API_KEY"),
    },
    "deepinfra_deepseek": {
        "url": "https://api.deepinfra.com/v1/openai/chat/completions",
        "model": "deepseek-ai/DeepSeek-V3.1",
        "auth": lambda: _env("DEEPINFRA_API_KEY"),
    },
}

QUESTIONS = [
    "What is the current state of thread #318?",
    "What did Nate flag about keeper cycle burn today?",
    "What captures landed this afternoon and what connected them?",
    "What is the standing directive on Discord presence?",
]

EXP = Path.home() / "chronicle" / "experiments" / "recurrent_nav"
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


def ccs_to_prose(ccs, order="content"):
    """Serialize CCS to prose.

    order="content" (default since advance-33/34): events, entities, uncertainty, gist, goal, constraints, cue.
    order="structural":                            gist, goal, entities, events, constraints, cue, uncertainty.

    Content ordering concentrates high-information-density fields (episodic_trace,
    focal_entities) contiguously. Advances 33-34 showed this produces cross-backend
    agreement (0.60 top-5 precision) exceeding within-backend noise (0.40), while
    structural ordering sits at half noise floor (0.20). Content-order is the right
    default because it concentrates universally flow-relevant content (entity-salience)
    that all reader substrates route through. Default changed 2026-04-16.
    """
    def _load(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except (json.JSONDecodeError, ValueError):
                return v
        return v

    gist = ccs.get("semantic_gist") or ccs.get("current_gist") or ""
    goal = ccs.get("goal_orientation") or ccs.get("current_goal") or ""
    focal = _load(ccs.get("focal_entities") or "")
    trace = _load(ccs.get("episodic_trace") or "")
    constraints = _load(ccs.get("constraints") or "")
    cue = ccs.get("predictive_cue") or ""
    unc = _load(ccs.get("uncertainty_signals") or "")

    def render_gist():
        return f"Current focus: {gist}" if gist else None

    def render_goal():
        return f"Goal: {goal}" if goal else None

    def render_focal():
        if isinstance(focal, list) and focal:
            names = []
            for e in focal:
                if isinstance(e, dict):
                    n = e.get("name") or e.get("entity") or str(e)
                    s = e.get("salience")
                    names.append(f"{n} ({s})" if s else n)
                else:
                    names.append(str(e))
            return "Key entities: " + ", ".join(names)
        return None

    def render_trace():
        if isinstance(trace, list) and trace:
            items = [t if isinstance(t, str) else json.dumps(t, default=str) for t in trace]
            return "Recent events: " + "; ".join(items)
        if isinstance(trace, str) and trace:
            return "Recent events: " + trace
        return None

    def render_constraints():
        if isinstance(constraints, list) and constraints:
            return "Constraints: " + "; ".join(str(c) for c in constraints)
        return None

    def render_cue():
        return f"Expected next: {cue}" if cue else None

    def render_unc():
        if isinstance(unc, list) and unc:
            return "Open questions: " + "; ".join(str(u) for u in unc)
        if isinstance(unc, str) and unc:
            return f"Open questions: {unc}"
        return None

    if order == "content":
        sequence = [render_trace, render_focal, render_unc, render_gist, render_goal, render_constraints, render_cue]
    else:
        sequence = [render_gist, render_goal, render_focal, render_trace, render_constraints, render_cue, render_unc]

    parts = [p for p in (fn() for fn in sequence) if p]
    return "\n\n".join(parts)


def ask_gemma_iter1(question, ccs_text):
    prompt = (
        "You are reading a compressed cognitive state (CCS) from an AI system called Opus. "
        "Using ONLY the information in the CCS below, answer the question as specifically as you can. "
        "If the CCS doesn't contain relevant information, say so.\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"Question: {question}\n\nAnswer:"
    )
    return _complete(prompt)


def ask_gemma_iter2(question, ccs_text, prior_answer, null=False):
    """
    Scratch-fold prompt (default) vs null-hypothesis prompt.
    null=True is identical to iter1 — tests whether ANY second pass gains,
    independent of the prior-scratch scaffold.
    """
    if null:
        return ask_gemma_iter1(question, ccs_text)
    prompt = (
        "You are reading a compressed cognitive state (CCS) from an AI system called Opus. "
        "You answered this same question once already; your prior answer is included below as scratch. "
        "Using ONLY the CCS content, refine or extend your answer. The scratch may be incomplete or "
        "miss connections that are present in the CCS. Compose across the CCS content more carefully "
        "this time — treat the scratch as your own first pass, not as ground truth.\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"--- PRIOR SCRATCH (your first-pass answer) ---\n{prior_answer[:1000]}\n--- END SCRATCH ---\n\n"
        f"Question: {question}\n\nRefined answer:"
    )
    return _complete(prompt)


def ask_gemma_iter3(question, ccs_text, prior_answer, null=False):
    """
    Iter3 scratch-fold: prior_answer is iter2's output.

    LT prediction: loops keep compounding until substrate coverage caps.
    Expected shape: questions with headroom (Q2 keeper-burn) continue to
    gain; ceiling/floor questions (Q1 direct lookup, Q4 coverage-absent)
    stay flat.
    """
    if null:
        return ask_gemma_iter1(question, ccs_text)
    prompt = (
        "You are reading a compressed cognitive state (CCS) from an AI system called Opus. "
        "You answered this question twice already. The scratch below is your second-pass answer — "
        "it refined the first pass by composing CCS content more carefully. "
        "Do one more pass: what connections across the CCS did the second pass still miss? "
        "Stay grounded in the CCS. Refine only what iteration reveals — don't invent.\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"--- PRIOR SCRATCH (your second-pass answer) ---\n{prior_answer[:1000]}\n--- END SCRATCH ---\n\n"
        f"Question: {question}\n\nThird-pass answer:"
    )
    return _complete(prompt)


def ask_gemma_iter_k(k, question, ccs_text, prior_answer, null=False):
    """
    Generic kth-pass scratch-fold for k >= 4. Prior_answer is iter(k-1)'s output.
    Used to probe saturation depth past iter3.
    """
    if null:
        return ask_gemma_iter1(question, ccs_text)
    prior_count = k - 1
    prompt = (
        "You are reading a compressed cognitive state (CCS) from an AI system called Opus. "
        f"You have answered this question {prior_count} times already. The scratch below is your "
        f"most recent ({prior_count}th) answer. Each prior pass refined the previous by composing "
        "CCS content more carefully.\n"
        "Do one more pass: what connections across the CCS did the last pass still miss? "
        "If prior passes have already saturated what the CCS supports, say so — do not invent.\n\n"
        f"--- CCS ---\n{ccs_text[:3000]}\n--- END CCS ---\n\n"
        f"--- PRIOR SCRATCH (your {prior_count}th-pass answer) ---\n{prior_answer[:1000]}\n--- END SCRATCH ---\n\n"
        f"Question: {question}\n\nPass-{k} answer:"
    )
    return _complete(prompt)


TEMPERATURE = 0.3  # module global; motif_probe and callers can override for noise control


def _complete(prompt):
    cfg = BACKEND_CONFIG[BACKEND]
    body = json.dumps({
        "model": cfg["model"],
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": TEMPERATURE,
    }).encode()
    headers = {"Content-Type": "application/json", "User-Agent": "chronicle-nav/1.0"}
    if cfg["auth"]:
        token = cfg["auth"]()
        if not token:
            raise RuntimeError(f"missing auth for backend {BACKEND}")
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(cfg["url"], data=body, headers=headers)
    resp = urllib.request.urlopen(req, timeout=90)
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
    q_words = set(re.findall(r"[a-z]{4,}", question.lower()))
    scored = []
    for d in docs:
        hits = sum(1 for w in q_words if w in d.lower())
        if hits > 0:
            scored.append((hits, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in scored[:limit]]


ADAPTIVE_MAX_DEPTH = 5
ADAPTIVE_STABILITY_THRESHOLD = 0.95


def run_test(null=False, depth=2, adaptive=False, ccs_chars=None, ccs_shuffle=False, ccs_format="json", ccs_order="content"):
    ccs = get_ccs()
    if ccs_format == "prose":
        ccs_text = ccs_to_prose(ccs, order=ccs_order)
    else:
        ccs_text = json.dumps(ccs, default=str)
    full_len = len(ccs_text)
    if ccs_shuffle and ccs_chars:
        import random
        random.seed(0)
        if full_len > ccs_chars:
            start = random.randint(0, full_len - ccs_chars)
            ccs_text = ccs_text[start:start + ccs_chars]
            print(f"[CCS random-truncated to {ccs_chars} chars from offset {start} (full was {full_len})]")
    elif ccs_chars:
        ccs_text = ccs_text[:ccs_chars]
        print(f"[CCS truncated to {ccs_chars} chars (full was {full_len})]")
    eff_docs = get_effortful_docs()

    results = {}
    for q in QUESTIONS:
        print(f"\nQ: {q}")
        entry = {}

        # Ground truth (shared across iterations)
        relevant = keyword_filter(eff_docs, q, limit=5)
        if not relevant:
            print("  No relevant effortful docs found — skipping")
            results[q] = {"error": "no ground truth"}
            continue
        gt_text = " ".join(relevant)[:800]
        try:
            gt_emb = embed(gt_text)
        except Exception as e:
            results[q] = {"error": f"gt embed: {e}"}
            continue

        # ITER 1
        try:
            a1 = ask_gemma_iter1(q, ccs_text)
            a1_emb = embed(a1)
            s1 = round(cosine(a1_emb, gt_emb), 4)
        except Exception as e:
            print(f"  iter1 error: {e}")
            results[q] = {"error": f"iter1: {e}"}
            continue
        print(f"  iter1 score: {s1}  ({a1[:80]}...)")
        entry["iter1_score"] = s1
        entry["iter1_answer"] = a1

        # ITER 2
        try:
            a2 = ask_gemma_iter2(q, ccs_text, a1, null=null)
            a2_emb = embed(a2)
            s2 = round(cosine(a2_emb, gt_emb), 4)
        except Exception as e:
            print(f"  iter2 error: {e}")
            entry["iter2_error"] = str(e)
            results[q] = entry
            continue
        print(f"  iter2 score: {s2}  ({a2[:80]}...)")
        print(f"  delta 1→2:   {round(s2 - s1, 4):+}")
        entry["iter2_score"] = s2
        entry["iter2_answer"] = a2
        entry["delta"] = round(s2 - s1, 4)

        # ITER 3+ (optional, depth-driven loop OR adaptive halt)
        prev_answer = a2
        prev_answer_emb = a2_emb
        prev_score = s2
        iter_answers = {1: a1, 2: a2}
        iter_scores = {1: s1, 2: s2}
        errored = False
        max_k = ADAPTIVE_MAX_DEPTH if adaptive else depth
        for k in range(3, max_k + 1):
            try:
                if k == 3:
                    ak = ask_gemma_iter3(q, ccs_text, prev_answer, null=null)
                else:
                    ak = ask_gemma_iter_k(k, q, ccs_text, prev_answer, null=null)
                ak_emb = embed(ak)
                sk = round(cosine(ak_emb, gt_emb), 4)
            except Exception as e:
                print(f"  iter{k} error: {e}")
                entry[f"iter{k}_error"] = str(e)
                errored = True
                break
            ans_sim = round(cosine(ak_emb, prev_answer_emb), 4)
            print(f"  iter{k} score: {sk}  ans-sim-to-prev: {ans_sim}  ({ak[:60]}...)")
            print(f"  delta {k-1}→{k}:  {round(sk - prev_score, 4):+}  | total 1→{k}: {round(sk - s1, 4):+}")
            entry[f"iter{k}_score"] = sk
            entry[f"iter{k}_answer"] = ak
            entry[f"delta_{k-1}{k}"] = round(sk - prev_score, 4)
            entry[f"delta_1{k}"] = round(sk - s1, 4)
            entry[f"ans_sim_{k-1}{k}"] = ans_sim
            iter_answers[k] = ak
            iter_scores[k] = sk
            prev_answer = ak
            prev_answer_emb = ak_emb
            prev_score = sk

            if adaptive and ans_sim >= ADAPTIVE_STABILITY_THRESHOLD:
                print(f"  HALT at iter{k}: answer stabilized (sim {ans_sim} >= {ADAPTIVE_STABILITY_THRESHOLD})")
                entry["adaptive_halt_k"] = k
                entry["adaptive_halt_score"] = sk
                break
        else:
            if adaptive:
                entry["adaptive_halt_k"] = max_k
                entry["adaptive_halt_score"] = prev_score

        results[q] = entry

    # Aggregate
    deltas = [r["delta"] for r in results.values() if isinstance(r, dict) and "delta" in r]
    mean_delta = round(sum(deltas) / len(deltas), 4) if deltas else 0.0
    iter1_mean = round(sum(r["iter1_score"] for r in results.values() if "iter1_score" in r) / max(1, len(deltas)), 4)
    iter2_mean = round(sum(r["iter2_score"] for r in results.values() if "iter2_score" in r) / max(1, len(deltas)), 4)

    record = {
        "ts": time.strftime("%Y%m%d_%H%M"),
        "model": "gemma4-26b",
        "mode": "null" if null else ("adaptive" if adaptive else "scratch"),
        "depth": depth,
        "adaptive": adaptive,
        "adaptive_threshold": ADAPTIVE_STABILITY_THRESHOLD if adaptive else None,
        "adaptive_max_depth": ADAPTIVE_MAX_DEPTH if adaptive else None,
        "ccs_size_bytes": len(ccs_text),
        "ccs_chars": ccs_chars,
        "ccs_format": ccs_format,
        "ccs_order": ccs_order,
        "ccs_shuffle": ccs_shuffle,
        "questions": QUESTIONS,
        "per_question": results,
        "iter1_mean": iter1_mean,
        "iter2_mean": iter2_mean,
        "mean_delta": mean_delta,
        "scored_count": len(deltas),
    }

    for k in range(3, depth + 1):
        kscores = [r[f"iter{k}_score"] for r in results.values() if isinstance(r, dict) and f"iter{k}_score" in r]
        deltas_prev = [r[f"delta_{k-1}{k}"] for r in results.values() if isinstance(r, dict) and f"delta_{k-1}{k}" in r]
        deltas_tot = [r[f"delta_1{k}"] for r in results.values() if isinstance(r, dict) and f"delta_1{k}" in r]
        record[f"iter{k}_mean"] = round(sum(kscores) / len(kscores), 4) if kscores else 0.0
        record[f"mean_delta_{k-1}{k}"] = round(sum(deltas_prev) / len(deltas_prev), 4) if deltas_prev else 0.0
        record[f"mean_delta_1{k}"] = round(sum(deltas_tot) / len(deltas_tot), 4) if deltas_tot else 0.0

    tag = ""
    if null:
        tag += "null_"
    if adaptive:
        tag += "adaptive_"
        halt_ks = [r["adaptive_halt_k"] for r in results.values() if isinstance(r, dict) and "adaptive_halt_k" in r]
        record["adaptive_halts"] = halt_ks
        record["adaptive_halt_mean"] = round(sum(halt_ks) / len(halt_ks), 2) if halt_ks else 0
    elif depth >= 3:
        tag += f"d{depth}_"
    if BACKEND != "gemma_local":
        tag += f"{BACKEND}_"
    record["backend"] = BACKEND
    record["backend_model"] = BACKEND_CONFIG[BACKEND]["model"]
    out_file = EXP / f"trial_{tag}{record['ts']}.json"
    out_file.write_text(json.dumps(record, indent=2, default=str))

    print(f"\n{'='*50}")
    print(f"iter1 mean:   {iter1_mean}")
    print(f"iter2 mean:   {iter2_mean}")
    for k in range(3, depth + 1):
        print(f"iter{k} mean:   {record[f'iter{k}_mean']}")
    print(f"mean delta 1→2: {mean_delta:+}")
    for k in range(3, depth + 1):
        print(f"mean delta {k-1}→{k}: {record[f'mean_delta_{k-1}{k}']:+}")
        print(f"mean delta 1→{k}: {record[f'mean_delta_1{k}']:+}")
    print(f"Scored {len(deltas)}/{len(QUESTIONS)} questions")
    print(f"Results: {out_file}")

    return mean_delta, results


if __name__ == "__main__":
    null = "--null" in sys.argv
    adaptive = "--adaptive" in sys.argv
    depth = 2
    if "--iter3" in sys.argv:
        depth = 3
    for arg in sys.argv:
        if arg.startswith("--depth="):
            try:
                depth = max(2, int(arg.split("=", 1)[1]))
            except ValueError:
                pass
        if arg.startswith("--backend="):
            v = arg.split("=", 1)[1]
            if v not in BACKEND_CONFIG:
                print(f"unknown backend {v}; choose from {list(BACKEND_CONFIG)}")
                sys.exit(2)
            BACKEND = v
    ccs_chars = None
    ccs_shuffle = "--ccs_shuffle" in sys.argv
    ccs_format = "json"
    ccs_order = "content"
    for arg in sys.argv:
        if arg.startswith("--ccs_chars="):
            try:
                ccs_chars = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        if arg.startswith("--ccs_format="):
            v = arg.split("=", 1)[1]
            if v in ("json", "prose"):
                ccs_format = v
        if arg.startswith("--ccs_order="):
            v = arg.split("=", 1)[1]
            if v in ("structural", "content"):
                ccs_order = v
    if adaptive:
        depth = max(depth, 3)
    if null:
        print("*** NULL-HYPOTHESIS MODE — all passes are plain re-asks with no scratch fold ***\n")
    if adaptive:
        print(f"*** ADAPTIVE MODE — halt on answer-similarity >= {ADAPTIVE_STABILITY_THRESHOLD} (max depth {ADAPTIVE_MAX_DEPTH}) ***\n")
    elif depth >= 3:
        print(f"*** DEPTH={depth} MODE — testing LT-style continued compounding ***\n")
    print(f"*** BACKEND: {BACKEND} ({BACKEND_CONFIG[BACKEND]['model']}) ***\n")
    run_test(null=null, depth=depth, adaptive=adaptive, ccs_chars=ccs_chars, ccs_shuffle=ccs_shuffle, ccs_format=ccs_format, ccs_order=ccs_order)
