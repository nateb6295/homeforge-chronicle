#!/usr/bin/env python3
"""Motif probe — per-model effective motif dictionary via window masking.

Built 2026-04-16 as the rig promised in thread #318 advance 30. The matrix
showed a model-specific flip on content-vs-structural ordering; the thread
claim is that different readers recognise different "spiking motifs" in the
same CCS. This probe measures that directly.

Method:
  1. Fix CCS, question, backend.
  2. Baseline: iter1 → iter2, embed each, measure shift vector v = e2 - e1.
  3. For each length-k window at stride s: blank it out, rerun iter1→iter2,
     measure v_masked. Attribution score = ||v - v_masked||.
  4. High-attribution windows = the ones this backend uses most.
  5. Per-backend motif dictionary = top-K attributions. Compare dictionaries
     across backends to test the "different readers see different motifs" claim.

Cost: W windows × (2 LLM calls + 2 embeds) per question per backend.
  ccs=1500, k=64, stride=32 → ~45 windows. Cloud ~30 min, local ~1-2h.

This script does the backend × question × window loop and writes a CSV of
attributions. Aggregation + cross-backend comparison is a follow-on.
"""

import json
import os
import sys
import time
import csv
import math
import argparse
from pathlib import Path

# Reuse the recurrent_nav_test primitives instead of re-implementing.
sys.path.insert(0, os.path.expanduser("~/chronicle/bin"))
import recurrent_nav_test as rnt  # type: ignore
from recurrent_nav_test import (  # type: ignore
    get_ccs, ccs_to_prose, ask_gemma_iter1, ask_gemma_iter2,
    embed, cosine, BACKEND_CONFIG
)

OUT_DIR = Path(os.path.expanduser("~/chronicle/experiments/recurrent_nav"))
MASK_CHAR = "\u2588"  # full block — visually obvious, single char


def mask_window(text, start, length):
    return text[:start] + (MASK_CHAR * length) + text[start + length:]


def vec_dist(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def run_probe(backend, question, ccs_text, window_k, stride, max_windows=None, temperature=None):
    if backend not in BACKEND_CONFIG:
        raise ValueError(f"unknown backend {backend}; choose from {list(BACKEND_CONFIG)}")
    rnt.BACKEND = backend  # mutate the module global so _complete() routes correctly
    if temperature is not None:
        rnt.TEMPERATURE = temperature
    os.environ["NAV_BACKEND"] = backend
    full_len = len(ccs_text)

    actual_model = BACKEND_CONFIG[backend]["model"]
    print(f"[baseline] backend={backend} model={actual_model} temp={rnt.TEMPERATURE} ccs_len={full_len} q={question[:60]}", flush=True)
    t0 = time.time()
    a1 = ask_gemma_iter1(question, ccs_text)
    a2 = ask_gemma_iter2(question, ccs_text, a1, null=False)
    e1 = embed(a1)
    e2 = embed(a2)
    shift = [y - x for x, y in zip(e1, e2)]
    shift_norm = math.sqrt(sum(s * s for s in shift))
    print(f"  baseline shift norm: {shift_norm:.4f} ({time.time()-t0:.1f}s)")

    rows = [{
        "window_start": -1, "window_end": -1,
        "masked_text": "(baseline)",
        "a1_len": len(a1), "a2_len": len(a2),
        "shift_norm": shift_norm,
        "attribution": 0.0,
        "elapsed_s": round(time.time() - t0, 2),
    }]

    offsets = list(range(0, full_len - window_k + 1, stride))
    if max_windows:
        offsets = offsets[:max_windows]
    print(f"  probing {len(offsets)} windows at k={window_k}, stride={stride}")

    for i, start in enumerate(offsets):
        tw = time.time()
        masked = mask_window(ccs_text, start, window_k)
        try:
            am1 = ask_gemma_iter1(question, masked)
            am2 = ask_gemma_iter2(question, masked, am1, null=False)
            em1 = embed(am1)
            em2 = embed(am2)
            shift_m = [y - x for x, y in zip(em1, em2)]
            attribution = vec_dist(shift, shift_m)
            shift_m_norm = math.sqrt(sum(s * s for s in shift_m))
        except Exception as e:
            print(f"  [w{i}] start={start} ERROR: {e}")
            continue
        window_text = ccs_text[start:start + window_k]
        rows.append({
            "window_start": start,
            "window_end": start + window_k,
            "masked_text": window_text.replace("\n", "\\n"),
            "a1_len": len(am1), "a2_len": len(am2),
            "shift_norm": round(shift_m_norm, 4),
            "attribution": round(attribution, 4),
            "elapsed_s": round(time.time() - tw, 2),
        })
        print(f"  [w{i:3d}] start={start:4d} attr={attribution:.4f} "
              f"shift={shift_m_norm:.4f} ({time.time()-tw:.1f}s) "
              f"{window_text[:40]!r}", flush=True)

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", default="deepinfra_gemma",
                    choices=list(BACKEND_CONFIG.keys()))
    ap.add_argument("--question", default="What is the current state of thread #318?")
    ap.add_argument("--ccs_chars", type=int, default=1500)
    ap.add_argument("--ccs_order", default="structural",
                    choices=["structural", "content"])
    ap.add_argument("--window_k", type=int, default=64)
    ap.add_argument("--stride", type=int, default=32)
    ap.add_argument("--max_windows", type=int, default=None,
                    help="Cap for pilot runs")
    ap.add_argument("--temp", type=float, default=None,
                    help="Override temperature (default 0.3 from module)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--ccs_file", default=None,
                    help="Use prepared text file as CCS directly, bypass get_ccs")
    args = ap.parse_args()

    if args.ccs_file:
        with open(args.ccs_file) as f:
            ccs_text = f.read()
    else:
        ccs = get_ccs()
        ccs_text = ccs_to_prose(ccs, order=args.ccs_order)
    if len(ccs_text) > args.ccs_chars:
        ccs_text = ccs_text[:args.ccs_chars]

    rows = run_probe(args.backend, args.question, ccs_text,
                     args.window_k, args.stride, args.max_windows,
                     temperature=args.temp)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    out = args.out or str(OUT_DIR / f"motif_probe_{args.backend}_k{args.window_k}_{stamp}.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\nWrote {len(rows)} rows -> {out}")

    ranked = sorted([r for r in rows if r["window_start"] >= 0],
                    key=lambda r: r["attribution"], reverse=True)
    print("\nTop 5 windows by attribution:")
    for r in ranked[:5]:
        print(f"  attr={r['attribution']:.4f} @ {r['window_start']:4d} "
              f": {r['masked_text'][:60]!r}")


if __name__ == "__main__":
    main()
