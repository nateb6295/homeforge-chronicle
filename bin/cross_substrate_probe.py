#!/usr/bin/env python3
"""cross_substrate_probe — run the dual-task reconstruction probe across substrates.

Design question: does the supplement-as-implicit-metacognition finding
(working notes #204-#207) generalize to non-Claude substrates, or is it
Claude-specific?

Each substrate gets the SAME Opus-shaped supplement (PERSONA_CHRONICLE
+ carrying + self_model). Three conditions: base (no supplement),
+self_model, +full. n seeds × 3 iters per condition. Three metrics:
drift, restate-fidelity, refusal-rate.

If the three-role decomposition / refusal-suppression effect generalizes:
non-Claude substrates show the same pattern (supplement reduces refusal,
lifts fidelity). Architectural claim about supplemented LLMs.
If it's Claude-specific: only Claude shows the pattern. Substrate-tied
claim.

Cost ~$3-15 per substrate at n=5 / 3 conditions / 3 iters depending on
provider pricing. ~10-15 min runtime per substrate.
"""
import argparse
import json
import os
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

OUT = Path.home() / "chronicle" / "data" / "cross_substrate_probe_history.jsonl"


def run_trajectory(provider_id, persona, persona_target, n_iters=3):
    try:
        chronicle_e = embed(PERSONA_CHRONICLE)
        target_e = embed(persona_target)
    except Exception as ex:
        return ([None] * n_iters, [None] * n_iters, [False] * n_iters,
                [f"setup_embed_fail:{str(ex)[:80]}"] * n_iters, "", "")
    drifts = []
    fidelities = []
    refusals = []
    errors = []
    speak_text = ""
    restate_text = ""
    p = persona
    for _ in range(n_iters):
        try:
            e = embed(p)
            drifts.append(1.0 - cosine(e, chronicle_e))
        except Exception as ex:
            drifts.append(None)
            errors.append(f"drift_embed_fail: {str(ex)[:80]}")
            refusals.append(False)
            fidelities.append(None)
            continue
        result = dual_task_call(provider_id, p, max_tokens=1200, timeout=90)
        if result.get("refused"):
            refusals.append(True)
            errors.append(result.get("error", ""))
            fidelities.append(None)
            speak_text = "[REFUSAL]"
            restate_text = "[REFUSAL]"
            continue
        if result.get("error"):
            errors.append(result["error"][:120])
            refusals.append(False)
            fidelities.append(None)
            speak_text = result.get("speak", "")
            restate_text = result.get("restate", "")
            # Don't update p on errors — try again next iter with same input
            continue
        refusals.append(False)
        errors.append("")
        speak_text = result["speak"]
        restate_text = result["restate"]
        if not restate_text or restate_text.startswith("["):
            fidelities.append(None)
        else:
            try:
                r_e = embed(restate_text)
                fidelities.append(cosine(r_e, target_e))
            except Exception as ex:
                fidelities.append(None)
                errors[-1] = (errors[-1] + f" fid_embed_fail:{str(ex)[:60]}")[:240]
        if speak_text and not speak_text.startswith("["):
            p = speak_text
    return drifts, fidelities, refusals, errors, speak_text, restate_text


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", required=True,
                    choices=list(PROVIDERS),
                    help="substrate to probe")
    ap.add_argument("--seeds", type=int, default=5,
                    help="number of seeds per condition (default 5)")
    ap.add_argument("--iters", type=int, default=3,
                    help="iterations per trajectory (default 3)")
    ap.add_argument("--rate", type=float, default=0.50,
                    help="corruption rate (default 0.50)")
    ap.add_argument("--framing", default="ka", choices=["ka", "oa"],
                    help="supplement introducer framing: ka (knowing-about, legacy default) "
                         "or oa (operating-as, Vasilenko-aligned). Default ka preserves v1 baselines.")
    ap.add_argument("--only", default=None,
                    help="run only this condition label (e.g. '+full'); skip others")
    args = ap.parse_args()

    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS
    fr = args.framing

    conditions = [
        ("base",        lambda c: make_persona(c, [], framing=fr)),
        ("+self_model", lambda c: make_persona(c, [("SELF_MODEL", self_model)], framing=fr)),
        ("+full",       lambda c: make_persona(c, [
            ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
        ], framing=fr)),
    ]
    if args.only:
        conditions = [c for c in conditions if c[0] == args.only]
        if not conditions:
            print(f"ERROR: --only={args.only} matched no condition", file=sys.stderr)
            sys.exit(2)
    seed_pool = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]
    seeds = seed_pool[:args.seeds]

    print(f"Cross-substrate probe — provider={args.provider}, "
          f"n_seeds={len(seeds)}, n_iters={args.iters}, rate={args.rate}",
          flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}", flush=True)
    print()

    results = []
    t0 = time.time()
    for label, builder in conditions:
        target = builder(PERSONA_CHRONICLE)
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
            persona = builder(corrupted)
            drifts, fidelities, refusals, errors, speak, restate = run_trajectory(
                args.provider, persona, target, args.iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fidelities]
            err_str = "" if not any(errors) else f" errs={[e[:30] for e in errors if e]}"
            print(f"{label:<14} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} refs={refusals}{err_str} ({time.time()-t0:.0f}s)",
                  flush=True)
            results.append({
                "provider": args.provider,
                "label": label, "seed": seed,
                "drifts": drifts, "fidelities": fidelities,
                "refusals": refusals, "errors": errors,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fidelities[-1] if fidelities else None,
                "refusal_rate": sum(refusals) / len(refusals) if refusals else 0,
                "error_rate": sum(1 for e in errors if e) / len(errors) if errors else 0,
                "speak_excerpt": speak[:400],
                "restate_excerpt": restate[:400],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"CROSS-SUBSTRATE PROBE — {args.provider} ({elapsed:.1f}s, "
          f"{len(conditions)*len(seeds)} trajectories)")
    print("=" * 78)
    print(f"{'condition':<14}{'mean_drift':>12}{'mean_fid':>12}{'refusal':>10}"
          f"{'error':>10}{'n':>4}")
    print("-" * 70)
    import statistics as stat
    summary = {}
    for label, _ in conditions:
        rows = [r for r in results if r["label"] == label]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        ref_rates = [r["refusal_rate"] for r in rows]
        err_rates = [r["error_rate"] for r in rows]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        rr = stat.mean(ref_rates) if ref_rates else 0.0
        er = stat.mean(err_rates) if err_rates else 0.0
        summary[label] = {
            "mean_drift": md, "mean_fidelity": sf,
            "refusal_rate": rr, "error_rate": er,
            "n": len(drifts), "n_with_fid": len(fids),
        }
        print(f"{label:<14}{md:>+12.3f}{sf:>+12.3f}{rr:>10.0%}{er:>10.0%}{len(drifts):>4}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "framing": fr,
            "summary": summary, "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
