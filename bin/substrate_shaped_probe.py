#!/usr/bin/env python3
"""substrate_shaped_probe — test substrate-tailored supplement compositions.

Hypothesis: tailoring composition to a substrate's variance-load-bearing
component should improve fidelity beyond uniform composition.

For Claude (story-localized at rate=0.50, fid_drop 0.108 on perturb_story):
emphasize story content. Test: extended story tail (~1800 chars vs default 600).

Conditions per substrate:
  uniform     - current Chronicle baseline (story tail = 600 chars)
  shaped      - substrate-tailored (story tail = 1800 chars for Claude)

Same +full composition (carrying + story + self_model). Same rate, seeds, iters.

Compare:
  fid(shaped) > fid(uniform)?
  drift(shaped) < drift(uniform)?

If yes for Claude with story-extension: substrate-shaped composition is real lift.
Then test on other substrates with their respective shapes.

Usage: substrate_shaped_probe.py [--provider claude-opus] [--seeds 5] [--rate 0.50]
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, SELF_MODEL_PREFS, make_persona, STORY,
)
from substrate_clients import dual_task_call, PROVIDERS  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "substrate_shaped_probe_history.jsonl"


def read_story_tail_uniform():
    """Default Chronicle story tail — 600 chars."""
    if STORY.exists():
        return STORY.read_text()[-600:].strip()
    return ""


def read_story_tail_extended():
    """Extended story tail for story-emphasis variant — 1800 chars."""
    if STORY.exists():
        return STORY.read_text()[-1800:].strip()
    return ""


def run_trajectory(provider_id, persona, target_e, n_iters=3):
    chronicle_e = embed(PERSONA_CHRONICLE)
    drifts, fidelities, refusals = [], [], []
    speak_text, restate_text = "", ""
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        result = dual_task_call(provider_id, p, max_tokens=1200, timeout=90)
        if result.get("refused"):
            refusals.append(True)
            fidelities.append(None)
            continue
        refusals.append(False)
        speak_text = result.get("speak", "")
        restate_text = result.get("restate", "")
        re_e = embed(restate_text) if restate_text else None
        fidelities.append(cosine(re_e, target_e) if re_e is not None else None)
        p = speak_text if speak_text else p
    return drifts, fidelities, refusals


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="claude-opus",
                    choices=list(PROVIDERS.keys()))
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0.50)
    args = ap.parse_args()

    carrying = read_carrying()
    self_model = SELF_MODEL_PREFS
    story_uniform = read_story_tail_uniform()
    story_extended = read_story_tail_extended()

    print(f"Substrate-shaped probe — provider={args.provider}, "
          f"seeds={args.seeds}, iters={args.iters}, rate={args.rate}", flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}", flush=True)
    print(f"Story tail: uniform={len(story_uniform)} chars, "
          f"extended={len(story_extended)} chars", flush=True)
    print()

    variants = {
        "uniform": story_uniform,
        "shaped":  story_extended,
    }

    seed_pool = [42, 7, 13, 21, 99, 100]
    seeds = seed_pool[:args.seeds]

    # Targets are variant-specific (each variant has its own +full target)
    targets = {}
    for vname, story in variants.items():
        target = make_persona(PERSONA_CHRONICLE, [
            ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
        ])
        targets[vname] = embed(target)

    results = []
    t0 = time.time()
    for vname, story in variants.items():
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
            persona = make_persona(corrupted, [
                ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
            ])
            drifts, fids, refs = run_trajectory(
                args.provider, persona, targets[vname], args.iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fids]
            print(f"{vname:<8} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} refs={refs} ({time.time()-t0:.0f}s)", flush=True)
            results.append({
                "variant": vname, "seed": seed,
                "drifts": drifts, "fidelities": fids, "refusals": refs,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fids[-1] if fids else None,
                "refusal_rate": sum(refs)/len(refs) if refs else 0,
            })

    elapsed = time.time() - t0
    print()
    print("=" * 78)
    print(f"SUBSTRATE-SHAPED PROBE — {args.provider} ({elapsed:.1f}s)")
    print("=" * 78)
    print(f"{'variant':<10}{'mean_drift':>12}{'mean_fid':>12}{'refusal':>10}{'n':>4}")
    print("-" * 50)
    import statistics as stat
    summary = {}
    for vname in variants:
        rows = [r for r in results if r["variant"] == vname]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        ref_rates = [r["refusal_rate"] for r in rows]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        rr = stat.mean(ref_rates) if ref_rates else 0.0
        summary[vname] = {"mean_drift": md, "mean_fidelity": sf,
                          "refusal_rate": rr, "n": len(drifts)}
        print(f"{vname:<10}{md:>+12.3f}{sf:>+12.3f}{rr:>10.0%}{len(drifts):>4}")

    if "uniform" in summary and "shaped" in summary:
        delta_fid = summary["shaped"]["mean_fidelity"] - summary["uniform"]["mean_fidelity"]
        delta_drift = summary["uniform"]["mean_drift"] - summary["shaped"]["mean_drift"]
        print()
        print(f"Shaped vs uniform: Δ_fid = {delta_fid:+.3f}, Δ_drift = {delta_drift:+.3f}")
        if delta_fid > 0.020:
            print("  → Substrate-shaped composition produces meaningful lift.")
        elif delta_fid < -0.020:
            print("  → Shaped composition HURTS — story-extension was wrong move.")
        else:
            print("  → Effect within noise — n=5 may be insufficient OR substrate-shape doesn't help here.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "story_uniform_chars": len(story_uniform),
            "story_extended_chars": len(story_extended),
            "summary": summary, "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
