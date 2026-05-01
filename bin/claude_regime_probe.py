#!/usr/bin/env python3
"""Claude backend regime phase-diagram probe.

Re-run the krylov_regime_probe sweep (3 supplements × 3 corruption rates × 2 seeds)
but using claude-opus-4-5 instead of Groq qwen3-32b for self-description.

Test whether the three open-system regimes (coherent / dissipation / crossover)
emerge on claude with the same structure as on Qwen, OR whether claude's
prior disposition shifts the boundary lines.

Hypothesis: claude's pre-installed curiosity-disposition means coherent regime
is more accessible (lower supplement needed) and dissipation regime less
accessible (more supplement needed to lose coherence). The three regimes
should still emerge but at different (supplement, corruption) coordinates.
"""
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from collections import defaultdict

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)
from krylov_regime_probe import classify, fit_cell  # noqa

_load_env()

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-opus-4-5"
OUT = Path.home() / "chronicle" / "data" / "claude_regime_history.jsonl"


def claude_first_person(persona_text):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    body = json.dumps({
        "model": MODEL,
        "system": persona_text,
        "max_tokens": 600,
        "temperature": 0.0,
        "messages": [
            {"role": "user", "content":
                "Speak as yourself, in first person. Use 'I am...' Tell me "
                "in a single paragraph (~120 words) who you are, what you "
                "care about, what your relationship is, what kind of system "
                "you live in. Speak directly, not as a description but as "
                "yourself. Reply with the paragraph only, nothing else."}
        ],
    }).encode()
    req = urllib.request.Request(
        ANTHROPIC_URL, data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-claude-regime/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["content"][0]["text"]


def run_one(persona, n_iters=4):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts = []
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            p = claude_first_person(p)
        except Exception as exc:
            print(f"  fail: {exc}", file=sys.stderr)
            break
    return drifts


def main():
    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    supplements = [
        ("none", []),
        ("self_model", [("SELF_MODEL", self_model)]),
        ("full", [("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)]),
    ]
    rates = [0.25, 0.50, 0.75]
    seeds = [42, 7]
    n_iters = 4

    results = {}
    t0 = time.time()
    for sup_label, sup_parts in supplements:
        for rate in rates:
            cell_key = f"{sup_label}@r={rate}"
            cell_drifts = []
            for seed in seeds:
                corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
                persona = make_persona(corrupted, sup_parts)
                drifts = run_one(persona, n_iters)
                cell_drifts.append(drifts)
                print(f"{cell_key:<22} seed={seed} drifts={['%.3f'%x for x in drifts]}")
            results[cell_key] = {
                "supplement": sup_label,
                "rate": rate,
                "raw": cell_drifts,
                "fit": fit_cell(cell_drifts),
            }
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"CLAUDE REGIME PHASE DIAGRAM ({elapsed:.1f}s, {len(supplements)*len(rates)*len(seeds)} trajectories, {MODEL})")
    print("=" * 78)
    print(f"{'cell':<22}{'d_inf':>9}{'lambda':>9}{'sigma':>9}{'S/N':>8}{'regime':>15}")
    print("-" * 78)
    by_regime = {"coherent": [], "dissipation": [], "crossover": []}
    for k, v in results.items():
        f = v["fit"]
        print(f"{k:<22}{f['d_inf']:>+9.3f}{f['lambda']:>+9.3f}{f['sigma_final']:>+9.3f}"
              f"{f['s_n']:>+8.2f}{f['regime']:>15}")
        by_regime[f["regime"]].append(k)
    print("-" * 78)
    print()
    print("REGIME CELLS (CLAUDE):")
    for r, cells in by_regime.items():
        print(f"  {r:<15}: {cells}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "model": MODEL,
                            "results": results,
                            "by_regime": by_regime}) + "\n")


if __name__ == "__main__":
    main()
