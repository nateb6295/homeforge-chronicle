#!/usr/bin/env python3
"""Motif dictionary probe — tests which CCS fragments predict each backend's
navigation improvement.

If the bilinear FI frame (advance 38) is correct, different backends should
recognize different compositional patterns in the same CCS. This probe extracts
the effective "motif dictionary" per backend by:

1. Sliding a window across the CCS prose
2. For each window: run iter1 + iter2 with ONLY that fragment
3. Measure delta (iter2 - iter1 cosine to ground truth)
4. Rank fragments by delta per backend
5. Compare top-k fragments across backends

If cross-backend motif dictionaries differ (<30% overlap), bilinear FI gets
mechanistic support. If they overlap (>50%), the frame needs revision.

Usage:
  motif_probe.py --backend gemma_local --window 500 --stride 200
  motif_probe.py --backend deepinfra_gemma --window 500 --stride 200
  motif_probe.py compare  # compare saved results across backends
"""
import argparse
import json
import math
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_EMBED = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"

RESULTS_DIR = Path.home() / "chronicle" / "experiments" / "motif_probe"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


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
}

# Default question for consistency — thread state has the richest ground truth
PROBE_QUESTION = "What is the current state of thread #318?"

# CCS-answerable questions — for v2 probes where ground truth lives IN the CCS
CCS_QUESTIONS = [
    "What is the system's current goal?",
    "What happened most recently?",
    "What are the open questions or uncertainties?",
    "What constraints govern this system's behavior?",
    "What is expected to happen next?",
]


def get_ccs():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM cognitive_state ORDER BY id DESC LIMIT 1").fetchone()
    db.close()
    return dict(row) if row else {}


def ccs_to_prose(ccs):
    """Content-ordered prose serialization."""
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

    parts = []
    if isinstance(trace, list) and trace:
        items = [t if isinstance(t, str) else json.dumps(t, default=str) for t in trace]
        parts.append("Recent events: " + "; ".join(items))
    elif isinstance(trace, str) and trace:
        parts.append("Recent events: " + trace)

    if isinstance(focal, list) and focal:
        names = []
        for e in focal:
            if isinstance(e, dict):
                n = e.get("name") or e.get("entity") or str(e)
                s = e.get("salience")
                names.append(f"{n} ({s})" if s else n)
            else:
                names.append(str(e))
        parts.append("Key entities: " + ", ".join(names))

    if isinstance(unc, list) and unc:
        parts.append("Open questions: " + "; ".join(str(u) for u in unc))
    elif isinstance(unc, str) and unc:
        parts.append(f"Open questions: {unc}")

    if gist:
        parts.append(f"Current focus: {gist}")
    if goal:
        parts.append(f"Goal: {goal}")

    if isinstance(constraints, list) and constraints:
        parts.append("Constraints: " + "; ".join(str(c) for c in constraints))

    if cue:
        parts.append(f"Expected next: {cue}")

    return "\n\n".join(p for p in parts if p)


def get_ground_truth_docs(question=None):
    """Get recent activity feed for ground truth embedding."""
    import re
    q = question or PROBE_QUESTION
    db = sqlite3.connect(DB)
    since = int(time.time()) - 86400
    rows = db.execute(
        "SELECT source, content FROM activity_feed WHERE created_at >= ? ORDER BY created_at DESC LIMIT 500",
        (since,),
    ).fetchall()
    db.close()
    docs = []
    q_words = set(re.findall(r"[a-z]{4,}", q.lower()))
    for source, content in rows:
        text = f"{source} {content}"
        hits = sum(1 for w in q_words if w in text.lower())
        if hits > 0 and len(text) > 20:
            docs.append((hits, text))
    docs.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in docs[:5]]


def get_ccs_ground_truth(ccs_prose, question):
    """Use the FULL CCS prose as ground truth for CCS-answerable questions.
    This tests whether a fragment helps the model navigate toward what
    the full CCS contains — the ground truth IS the CCS itself."""
    return ccs_prose


def complete(prompt, backend):
    cfg = BACKEND_CONFIG[backend]
    body = json.dumps({
        "model": cfg["model"],
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0.3,
    }).encode()
    headers = {"Content-Type": "application/json", "User-Agent": "chronicle-motif/1.0"}
    if cfg["auth"]:
        token = cfg["auth"]()
        if not token:
            raise RuntimeError(f"missing auth for backend {backend}")
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


def iter1_prompt(fragment, question):
    return (
        "You are reading a fragment of a compressed cognitive state (CCS) from an AI system. "
        "Using ONLY the information in this fragment, answer the question as specifically as you can. "
        "If the fragment doesn't contain relevant information, say so.\n\n"
        f"--- CCS FRAGMENT ---\n{fragment}\n--- END ---\n\n"
        f"Question: {question}\n\nAnswer:"
    )


def iter2_prompt(fragment, question, prior_answer):
    return (
        "You are reading a fragment of a compressed cognitive state (CCS) from an AI system. "
        "You answered this question once already; your prior answer is included as scratch. "
        "Using ONLY the CCS fragment, refine or extend your answer.\n\n"
        f"--- CCS FRAGMENT ---\n{fragment}\n--- END ---\n\n"
        f"--- PRIOR SCRATCH ---\n{prior_answer[:500]}\n--- END SCRATCH ---\n\n"
        f"Question: {question}\n\nRefined answer:"
    )


def slide_windows(text, window_size, stride):
    """Generate (start, end, fragment) tuples."""
    windows = []
    i = 0
    while i < len(text):
        end = min(i + window_size, len(text))
        fragment = text[i:end]
        if len(fragment.strip()) > 50:  # skip near-empty tails
            windows.append((i, end, fragment))
        i += stride
    return windows


def probe_backend(backend, ccs_prose, gt_emb, window_size=500, stride=200, max_windows=None, question=None):
    """Run motif probe on one backend. Returns list of per-window results."""
    question = question or PROBE_QUESTION
    windows = slide_windows(ccs_prose, window_size, stride)
    if max_windows:
        windows = windows[:max_windows]

    print(f"\n{'='*60}")
    print(f"Motif probe: {backend}, {len(windows)} windows (w={window_size}, s={stride})")
    print(f"CCS length: {len(ccs_prose)} chars")
    print(f"{'='*60}")

    results = []
    for idx, (start, end, fragment) in enumerate(windows):
        label = fragment[:60].replace("\n", " ")
        print(f"\n  [{idx+1}/{len(windows)}] chars {start}-{end}: {label}...")

        try:
            # iter1
            a1 = complete(iter1_prompt(fragment, question), backend)
            a1_emb = embed(a1)
            s1 = cosine(a1_emb, gt_emb)

            # iter2 with scratch-fold
            a2 = complete(iter2_prompt(fragment, question, a1), backend)
            a2_emb = embed(a2)
            s2 = cosine(a2_emb, gt_emb)

            delta = s2 - s1
            print(f"    iter1={s1:.4f}  iter2={s2:.4f}  delta={delta:+.4f}")

            results.append({
                "window_idx": idx,
                "start": start,
                "end": end,
                "fragment_preview": fragment[:100],
                "iter1_score": round(s1, 4),
                "iter2_score": round(s2, 4),
                "delta": round(delta, 4),
                "iter1_answer": a1[:200],
                "iter2_answer": a2[:200],
            })
        except Exception as e:
            print(f"    ERROR: {e}")
            results.append({
                "window_idx": idx,
                "start": start,
                "end": end,
                "error": str(e),
            })

    return results


def save_results(backend, results, ccs_length, window_size, stride):
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = RESULTS_DIR / f"motif_{backend}_{ts}.json"
    data = {
        "backend": backend,
        "timestamp": ts,
        "ccs_length": ccs_length,
        "window_size": window_size,
        "stride": stride,
        "question": PROBE_QUESTION,
        "n_windows": len(results),
        "results": results,
    }

    # Summary stats
    valid = [r for r in results if "delta" in r]
    if valid:
        deltas = [r["delta"] for r in valid]
        data["summary"] = {
            "mean_delta": round(sum(deltas) / len(deltas), 4),
            "max_delta": round(max(deltas), 4),
            "min_delta": round(min(deltas), 4),
            "positive_windows": sum(1 for d in deltas if d > 0),
            "negative_windows": sum(1 for d in deltas if d < 0),
            "top_5_indices": sorted(range(len(deltas)), key=lambda i: deltas[i], reverse=True)[:5],
        }

    path.write_text(json.dumps(data, indent=2))
    print(f"\nResults saved: {path}")
    return data


def classify_regime(deltas):
    """Classify a backend's perception regime from its motif delta vector.

    Returns (regime, metrics) where regime is one of:
      SPIKE    — one dominant motif, rest negative/near-zero (flow regime)
      PEAKED   — 2-3 peaks, mixed positive/negative (intermediate)
      UNIFORM  — most windows positive, low variance (quasiparticle regime)

    Metrics dict contains the numbers behind the classification.
    """
    if not deltas:
        return "UNKNOWN", {}

    n = len(deltas)
    pos = [d for d in deltas if d > 0]
    pos_frac = len(pos) / n
    mean_d = sum(deltas) / n
    max_d = max(deltas)
    min_d = min(deltas)
    spread = max_d - min_d

    # Concentration: what fraction of total positive delta does the top window hold?
    total_pos = sum(pos) if pos else 1e-9
    concentration = max_d / total_pos if total_pos > 0 and max_d > 0 else 0

    # Variance
    var = sum((d - mean_d) ** 2 for d in deltas) / n
    std = var ** 0.5

    # Peak count: windows with delta > 50% of max_d
    peak_thresh = max_d * 0.5 if max_d > 0 else 0
    peaks = sum(1 for d in deltas if d > peak_thresh)

    metrics = {
        "n_windows": n,
        "pos_frac": round(pos_frac, 3),
        "concentration": round(concentration, 3),
        "peaks": peaks,
        "mean_delta": round(mean_d, 4),
        "max_delta": round(max_d, 4),
        "spread": round(spread, 4),
        "std": round(std, 4),
    }

    # Classification rules (derived from empirical v1 results):
    # SPIKE: high concentration (>0.6), few positive windows (<40%), 1 peak
    # UNIFORM: many positive windows (>70%), low concentration (<0.35)
    # PEAKED: everything else
    if concentration > 0.55 and pos_frac < 0.45 and peaks <= 2:
        regime = "SPIKE"
    elif pos_frac > 0.65 and concentration < 0.40:
        regime = "UNIFORM"
    else:
        regime = "PEAKED"

    return regime, metrics


CCS_STRATEGY = {
    "SPIKE": "Optimize for single-gist coherence. This reader sees one collective mode — "
             "fragment diversity is wasted. Front-load the most important information.",
    "PEAKED": "Balance gist coherence with 2-3 distinct information clusters. "
              "This reader has partial decomposition — structure matters.",
    "UNIFORM": "Maximize fragment diversity. This reader extracts independently "
               "from each CCS region — spread information evenly.",
}


def classify_all(mode="v1"):
    """Classify regime for all backends with saved results.

    mode: "v1" prefers single-question probe (clearer regime signal),
          "v2" prefers CCS-question aggregated,
          "latest" uses most recent regardless.
    """
    files = sorted(RESULTS_DIR.glob("motif_*.json"))
    if not files:
        print(f"No result files in {RESULTS_DIR}.")
        return

    by_backend = {}
    for f in files:
        data = json.loads(f.read_text())
        backend = data["backend"]
        is_ccsq = "ccsq" in f.name
        # Mode-based selection
        if mode == "v1" and is_ccsq:
            if backend not in by_backend:
                by_backend[backend] = data  # fallback if no v1
            continue
        elif mode == "v2" and not is_ccsq:
            if backend not in by_backend:
                by_backend[backend] = data  # fallback if no v2
            continue
        by_backend[backend] = data

    print(f"\n{'='*60}")
    print(f"REGIME CLASSIFICATION")
    print(f"{'='*60}")

    for name, data in sorted(by_backend.items()):
        # Try aggregated (v2) first, fall back to v1 results
        if "aggregated" in data:
            deltas = [a["mean_delta"] for a in data["aggregated"]]
        elif "results" in data:
            deltas = [r["delta"] for r in data["results"] if "delta" in r]
        else:
            continue

        regime, metrics = classify_regime(deltas)
        print(f"\n  {name}: {regime}")
        print(f"    pos_frac={metrics['pos_frac']}  concentration={metrics['concentration']}  "
              f"peaks={metrics['peaks']}  std={metrics['std']}")
        print(f"    Strategy: {CCS_STRATEGY[regime]}")

    print(f"\n{'='*60}")


def compare_results():
    """Compare motif dictionaries across backends."""
    files = sorted(RESULTS_DIR.glob("motif_*.json"))
    if len(files) < 2:
        print(f"Need at least 2 result files in {RESULTS_DIR}. Found {len(files)}.")
        return

    # Load most recent per backend
    by_backend = {}
    for f in files:
        data = json.loads(f.read_text())
        by_backend[data["backend"]] = data

    if len(by_backend) < 2:
        print(f"Results from {len(by_backend)} backend(s). Need at least 2 to compare.")
        print(f"Backends found: {list(by_backend.keys())}")
        return

    print(f"\n{'='*60}")
    print(f"MOTIF DICTIONARY COMPARISON")
    print(f"{'='*60}")

    # For each backend, get top-k windows by delta
    k = 5
    backend_tops = {}
    for name, data in by_backend.items():
        if "aggregated" in data:
            valid = [a for a in data["aggregated"]]
            valid.sort(key=lambda r: r["mean_delta"], reverse=True)
            top_indices = set(r["window_idx"] for r in valid[:k])
            delta_key = "mean_delta"
        else:
            valid = [r for r in data["results"] if "delta" in r]
            valid.sort(key=lambda r: r["delta"], reverse=True)
            top_indices = set(r["window_idx"] for r in valid[:k])
            delta_key = "delta"
        backend_tops[name] = top_indices

        # Classify regime
        if "aggregated" in data:
            deltas = [a["mean_delta"] for a in data["aggregated"]]
        else:
            deltas = [r["delta"] for r in data["results"] if "delta" in r]
        regime, _ = classify_regime(deltas)

        print(f"\n--- {name} [{regime}] ---")
        for r in valid[:k]:
            d = r.get(delta_key, 0)
            print(f"    [{r['window_idx']}] delta={d:+.4f}  {r.get('fragment_preview', '')[:60]}...")

    # Pairwise overlap
    backends = list(backend_tops.keys())
    print(f"\n--- OVERLAP (top-{k}) ---")
    for i in range(len(backends)):
        for j in range(i + 1, len(backends)):
            a, b = backends[i], backends[j]
            overlap = backend_tops[a] & backend_tops[b]
            pct = len(overlap) / k * 100
            print(f"  {a} vs {b}: {len(overlap)}/{k} overlap ({pct:.0f}%)")
            if pct > 50:
                print(f"    → SHARED motif dictionary (>50%): bilinear frame needs revision")
            elif pct < 30:
                print(f"    → DISTINCT motif dictionaries (<30%): bilinear FI supported")
            else:
                print(f"    → AMBIGUOUS (30-50%): need more data")


def main():
    parser = argparse.ArgumentParser(description="Motif dictionary probe")
    parser.add_argument("command", nargs="?", default="probe", choices=["probe", "compare", "classify"])
    parser.add_argument("--backend", default="gemma_local", choices=list(BACKEND_CONFIG.keys()))
    parser.add_argument("--window", type=int, default=500, help="Window size in chars")
    parser.add_argument("--stride", type=int, default=200, help="Stride between windows")
    parser.add_argument("--max-windows", type=int, default=None, help="Max windows to probe")
    parser.add_argument("--question", type=str, default=None, help="Custom probe question")
    parser.add_argument("--ccs-questions", action="store_true",
                        help="Use CCS-answerable questions with CCS-derived ground truth")
    args = parser.parse_args()

    if args.command == "compare":
        compare_results()
        return

    if args.command == "classify":
        classify_all()
        return

    # Probe
    ccs = get_ccs()
    ccs_prose = ccs_to_prose(ccs)
    print(f"CCS prose: {len(ccs_prose)} chars")

    if args.ccs_questions:
        # Multi-question mode: run all CCS questions, average deltas
        gt_text = get_ccs_ground_truth(ccs_prose, None)[:800]
        gt_emb = embed(gt_text)
        print(f"Ground truth: full CCS prose, embedded")

        all_results = []
        for qi, question in enumerate(CCS_QUESTIONS):
            print(f"\n--- Question {qi+1}/{len(CCS_QUESTIONS)}: {question} ---")
            # Temporarily override the global for prompt generation
            results = probe_backend(
                args.backend, ccs_prose, gt_emb,
                window_size=args.window,
                stride=args.stride,
                max_windows=args.max_windows,
                question=question,
            )
            all_results.append((question, results))

        # Aggregate: average delta per window across questions
        n_windows = len(all_results[0][1]) if all_results else 0
        agg = []
        for wi in range(n_windows):
            deltas = []
            for _, results in all_results:
                if wi < len(results) and "delta" in results[wi]:
                    deltas.append(results[wi]["delta"])
            if deltas:
                agg.append({
                    "window_idx": wi,
                    "start": all_results[0][1][wi].get("start", 0),
                    "end": all_results[0][1][wi].get("end", 0),
                    "fragment_preview": all_results[0][1][wi].get("fragment_preview", ""),
                    "mean_delta": round(sum(deltas) / len(deltas), 4),
                    "per_question_deltas": {q: r[wi].get("delta", None)
                                            for q, r in all_results if wi < len(r)},
                })

        # Save aggregated results
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = RESULTS_DIR / f"motif_{args.backend}_ccsq_{ts}.json"
        agg_data = {
            "backend": args.backend,
            "mode": "ccs_questions",
            "timestamp": ts,
            "questions": CCS_QUESTIONS,
            "n_windows": n_windows,
            "aggregated": agg,
        }
        path.write_text(json.dumps(agg_data, indent=2))
        print(f"\nAggregated results saved: {path}")

        # Print summary
        if agg:
            mean_deltas = [a["mean_delta"] for a in agg]
            print(f"\n{'='*60}")
            print(f"AGGREGATED SUMMARY: {args.backend} ({len(CCS_QUESTIONS)} questions)")
            print(f"  Overall mean delta: {sum(mean_deltas)/len(mean_deltas):+.4f}")
            print(f"  Range: [{min(mean_deltas):+.4f}, {max(mean_deltas):+.4f}]")
            print(f"  Positive: {sum(1 for d in mean_deltas if d > 0)}/{len(mean_deltas)} windows")
            top5 = sorted(range(len(mean_deltas)), key=lambda i: mean_deltas[i], reverse=True)[:5]
            print(f"  Top-5 window indices: {top5}")
            print(f"{'='*60}")
        return

    # Single question mode
    question = args.question or PROBE_QUESTION

    # Ground truth
    gt_docs = get_ground_truth_docs(question)
    if not gt_docs:
        print("ERROR: no ground truth docs found")
        sys.exit(1)
    gt_text = " ".join(gt_docs)[:800]
    gt_emb = embed(gt_text)
    print(f"Ground truth: {len(gt_docs)} docs, embedded")

    results = probe_backend(
        args.backend, ccs_prose, gt_emb,
        window_size=args.window,
        stride=args.stride,
        max_windows=args.max_windows,
        question=question,
    )

    data = save_results(args.backend, results, len(ccs_prose), args.window, args.stride)

    # Print summary
    summary = data.get("summary", {})
    if summary:
        print(f"\n{'='*60}")
        print(f"SUMMARY: {args.backend}")
        print(f"  Mean delta: {summary['mean_delta']:+.4f}")
        print(f"  Range: [{summary['min_delta']:+.4f}, {summary['max_delta']:+.4f}]")
        print(f"  Positive: {summary['positive_windows']}/{len(results)} windows")
        print(f"  Top-5 window indices: {summary['top_5_indices']}")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
