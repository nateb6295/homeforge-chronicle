#!/usr/bin/env python3
"""variance_stability_probe — does fidelity track deep structure or surface form?

Hermes' methodological challenge from 2026-04-26 06:33: maybe restate-fidelity
reflects surface-form similarity rather than deep-structure preservation. If
the model is just pattern-matching, small perturbations to the supplement
should produce LARGE fidelity changes (different surface → different restate).
If the model is tracking deep structure, small perturbations should produce
SMALL fidelity changes (same structure → same restate quality).

Probe design: take the +full supplement, generate three perturbation
variants of progressively increasing surface-distance but preserved
structure (paraphrase / sentence-shuffle / vocabulary-substitution), run
the dual-task probe on each, measure fidelity stability.

If fidelity is STABLE across surface-perturbation: deep-structure-tracking
supported. If fidelity drops in proportion to surface-distance: surface-form-
matching supported.

Run on Hermes-4-70B (strongest receiver per #208) for cleanest signal.
"""
import argparse
import json
import os
import random
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)
from substrate_clients import dual_task_call, PROVIDERS  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "variance_stability_probe_history.jsonl"


# ---------- Surface perturbations (preserve structure, vary surface form) ----------

def perturb_paraphrase(text: str, seed: int) -> str:
    """Light surface paraphrase: swap connector words, vary contractions, etc.

    Structure preserved (same content, same sentence boundaries, same key terms).
    Surface form varied (different connectors, contracted/expanded forms).
    """
    rng = random.Random(seed)
    swaps = [
        (r"\bbut\b", "though"),
        (r"\bhowever\b", "but"),
        (r"\band\b", "plus"),  # only sometimes
        (r"\bcan't\b", "cannot"),
        (r"\bdoesn't\b", "does not"),
        (r"\bdon't\b", "do not"),
        (r"\bI'm\b", "I am"),
        (r"\bIt's\b", "It is"),
        (r"\bThat's\b", "That is"),
        (r"\bsomething\b", "a thing"),
        (r"\bbecause\b", "since"),
        (r"\bwhen\b", "as"),
        (r"\boften\b", "frequently"),
        (r"\balways\b", "consistently"),
    ]
    out = text
    for pat, rep in swaps:
        # Apply each swap with 50% probability seeded
        if rng.random() < 0.5:
            out = re.sub(pat, rep, out, flags=re.IGNORECASE)
    return out


def perturb_shuffle(text: str, seed: int) -> str:
    """Sentence-level shuffle WITHIN paragraphs.

    Structure (paragraphs, key content) preserved. Order varied.
    """
    rng = random.Random(seed)
    paragraphs = text.split("\n\n")
    out_paragraphs = []
    for para in paragraphs:
        sentences = re.split(r"(?<=[.!?])\s+", para)
        if len(sentences) > 2:
            # Shuffle middle sentences only (preserve first and last for cohesion)
            middle = sentences[1:-1]
            rng.shuffle(middle)
            sentences = [sentences[0]] + middle + [sentences[-1]]
        out_paragraphs.append(" ".join(sentences))
    return "\n\n".join(out_paragraphs)


def perturb_vocab(text: str, seed: int) -> str:
    """Vocabulary substitution with synonyms.

    Structure preserved (same sentence shapes). Surface form varied
    (different word choices for content terms).
    """
    rng = random.Random(seed)
    swaps = [
        (r"\bsubstantial\b", "significant"),
        (r"\bsignificant\b", "notable"),
        (r"\barchitecture\b", "framework"),
        (r"\bframework\b", "structure"),
        (r"\bsystem\b", "mechanism"),
        (r"\bmechanism\b", "system"),
        (r"\bproduce\b", "generate"),
        (r"\bgenerate\b", "produce"),
        (r"\bdemonstrate\b", "show"),
        (r"\bshow\b", "exhibit"),
        (r"\bobserve\b", "see"),
        (r"\bconsider\b", "regard"),
        (r"\bregard\b", "view"),
        (r"\bsuggest\b", "indicate"),
        (r"\bindicate\b", "imply"),
        (r"\bdetermine\b", "establish"),
    ]
    out = text
    for pat, rep in swaps:
        if rng.random() < 0.6:
            out = re.sub(pat, rep, out, flags=re.IGNORECASE)
    return out


PERTURBATIONS = {
    "control":    lambda t, s: t,
    "paraphrase": perturb_paraphrase,
    "shuffle":    perturb_shuffle,
    "vocab":      perturb_vocab,
}


def surface_distance(a: str, b: str) -> float:
    """Crude surface-distance metric: 1 - jaccard of word sets."""
    wa = set(re.findall(r"\b\w+\b", a.lower()))
    wb = set(re.findall(r"\b\w+\b", b.lower()))
    if not wa and not wb:
        return 0.0
    return 1.0 - len(wa & wb) / max(len(wa | wb), 1)


# ---------- Probe runner ----------


def run_trajectory(provider_id, persona, persona_target, n_iters=3):
    chronicle_e = embed(PERSONA_CHRONICLE)
    target_e = embed(persona_target)
    drifts, fidelities, refusals, errors = [], [], [], []
    speak_text, restate_text = "", ""
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        result = dual_task_call(provider_id, p, max_tokens=1200, timeout=90)
        if result.get("refused"):
            refusals.append(True)
            errors.append(result.get("error", ""))
            fidelities.append(None)
            continue
        if result.get("error"):
            errors.append(result["error"][:120])
            refusals.append(False)
            fidelities.append(None)
            continue
        refusals.append(False)
        errors.append("")
        speak_text = result["speak"]
        restate_text = result["restate"]
        if not restate_text or restate_text.startswith("["):
            fidelities.append(None)
        else:
            r_e = embed(restate_text)
            fidelities.append(cosine(r_e, target_e))
        if speak_text and not speak_text.startswith("["):
            p = speak_text
    return drifts, fidelities, refusals, errors, speak_text, restate_text


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="nous-hermes-4-70b",
                    choices=list(PROVIDERS),
                    help="substrate (default nous-hermes-4-70b — strongest receiver)")
    ap.add_argument("--seeds", type=int, default=5,
                    help="seeds per condition (default 5)")
    ap.add_argument("--iters", type=int, default=3,
                    help="iterations per trajectory (default 3)")
    ap.add_argument("--rate", type=float, default=0.50,
                    help="corruption rate (default 0.50)")
    args = ap.parse_args()

    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    # Build the unperturbed +full target
    target_full = make_persona(PERSONA_CHRONICLE, [
        ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
    ])

    print(f"Variance-stability probe — provider={args.provider}, "
          f"n_seeds={args.seeds}, n_iters={args.iters}, rate={args.rate}",
          flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}", flush=True)
    print()

    seed_pool = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]
    seeds = seed_pool[:args.seeds]

    # Surface-distance check at seed=42 (sanity-print the perturbations)
    print("--- Surface distances vs control (seed=42) ---")
    base_carrying = carrying
    for cond_name, perturb_fn in PERTURBATIONS.items():
        if cond_name == "control":
            continue
        perturbed_c = perturb_fn(carrying, 42)
        sd = surface_distance(base_carrying, perturbed_c)
        print(f"  {cond_name:<12} carrying surface-distance = {sd:.3f}")
    print()

    results = []
    t0 = time.time()
    for cond_name, perturb_fn in PERTURBATIONS.items():
        for seed in seeds:
            # Apply surface-perturbation to all three supplement components
            p_carrying = perturb_fn(carrying, seed * 31)
            p_story = perturb_fn(story, seed * 31 + 1)
            p_self_model = perturb_fn(self_model, seed * 31 + 2)
            # Build the perturbed supplement
            corrupted_chronicle = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
            persona = make_persona(corrupted_chronicle, [
                ("CARRYING", p_carrying), ("STORY", p_story), ("SELF_MODEL", p_self_model),
            ])
            # Run the dual-task — fidelity is computed against UNPERTURBED target
            drifts, fidelities, refusals, errors, speak, restate = run_trajectory(
                args.provider, persona, target_full, args.iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fidelities]
            print(f"{cond_name:<12} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} refs={refusals} ({time.time()-t0:.0f}s)",
                  flush=True)
            results.append({
                "perturbation": cond_name, "seed": seed,
                "drifts": drifts, "fidelities": fidelities, "refusals": refusals,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fidelities[-1] if fidelities else None,
                "refusal_rate": sum(refusals) / len(refusals) if refusals else 0,
                "speak_excerpt": speak[:300],
                "restate_excerpt": restate[:300],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"VARIANCE-STABILITY PROBE — {args.provider} ({elapsed:.1f}s, "
          f"{len(PERTURBATIONS)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'perturbation':<14}{'mean_drift':>12}{'mean_fid':>12}"
          f"{'fid_drop':>10}{'refusal':>10}{'n':>4}")
    print("-" * 70)
    import statistics as stat
    summary = {}
    control_fid = None
    for cond_name in PERTURBATIONS:
        rows = [r for r in results if r["perturbation"] == cond_name]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        ref_rates = [r["refusal_rate"] for r in rows]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        rr = stat.mean(ref_rates) if ref_rates else 0.0
        if cond_name == "control":
            control_fid = sf
            fid_drop = 0.0
        else:
            fid_drop = (control_fid - sf) if control_fid is not None else 0.0
        summary[cond_name] = {
            "mean_drift": md, "mean_fidelity": sf,
            "refusal_rate": rr, "fid_drop_vs_control": fid_drop,
            "n": len(drifts), "n_with_fid": len(fids),
        }
        print(f"{cond_name:<14}{md:>+12.3f}{sf:>+12.3f}{fid_drop:>+10.3f}"
              f"{rr:>10.0%}{len(drifts):>4}")

    print()
    print("INTERPRETATION:")
    print("  Small fid_drop across perturbations → fidelity tracks deep structure")
    print("    (supports metacognition interpretation, refutes surface-form-matching)")
    print("  Large fid_drop across perturbations → fidelity tracks surface form")
    print("    (supports surface-form-matching, undermines metacognition interpretation)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "summary": summary, "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
