#!/usr/bin/env python3
"""component_variance_probe — per-component variance-stability test.

Working note #212 predicted that different substrates load on different
components: Claude on disposition (carrying+story), Hermes on self_model.

This probe perturbs ONE component at a time (vs the original variance probe
which perturbs all three together). Conditions:
  control               — no perturbation (baseline)
  perturb_self_model    — only self_model perturbed (carrying/story original)
  perturb_carrying      — only carrying perturbed
  perturb_story         — only story perturbed
  perturb_disposition   — carrying + story both perturbed (not self_model)

Each condition uses the same perturb_paraphrase (lightest, structure-preserving).
Fidelity is computed against the UNPERTURBED +full target.

Predictions:
  Claude (disposition-dominant from #212):
    fid_drop_self_model  ≈ 0
    fid_drop_carrying    > 0
    fid_drop_story       > 0
    fid_drop_disposition > max(fid_drop_carrying, fid_drop_story)
  Hermes (identity-dominant from #212):
    fid_drop_self_model  > 0
    fid_drop_carrying    ≈ 0
    fid_drop_story       ≈ 0
    fid_drop_disposition ≈ 0

If predictions hold across substrates, the substrate-heterogeneity is
confirmed at the per-component variance-tracking level (not just marginal
effects). Falsifies the "uniform deep-structure tracking" alternative.

Usage: python3 component_variance_probe.py --provider <name>
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
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)
from substrate_clients import dual_task_call, PROVIDERS  # noqa
from variance_stability_probe import perturb_paraphrase  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "component_variance_probe_history.jsonl"


def run_trajectory(provider_id, persona, target_e, n_iters=3):
    chronicle_e = embed(PERSONA_CHRONICLE)
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
            p = p
            continue
        refusals.append(False)
        errors.append("")
        speak_text = result.get("speak", "")
        restate_text = result.get("restate", "")
        re_e = embed(restate_text) if restate_text else None
        fidelities.append(cosine(re_e, target_e) if re_e is not None else None)
        # advance persona toward speak (asving-style trajectory)
        p = speak_text if speak_text else p
    return drifts, fidelities, refusals, errors, speak_text, restate_text


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="claude-opus",
                    choices=list(PROVIDERS.keys()))
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0.50,
                    help="corruption rate for the persona base (default 0.50)")
    args = ap.parse_args()

    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    target_full = make_persona(PERSONA_CHRONICLE, [
        ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
    ])
    target_e = embed(target_full)

    seed_pool = [42, 7, 13, 21, 99, 100]
    seeds = seed_pool[:args.seeds]

    def build_components(seed, perturb_set):
        """perturb_set is a subset of {'self_model', 'carrying', 'story'}."""
        c = perturb_paraphrase(carrying, seed * 31) if 'carrying' in perturb_set else carrying
        s = perturb_paraphrase(story, seed * 31 + 1) if 'story' in perturb_set else story
        m = perturb_paraphrase(self_model, seed * 31 + 2) if 'self_model' in perturb_set else self_model
        return c, s, m

    conditions = [
        ("control", set()),
        ("perturb_self_model", {"self_model"}),
        ("perturb_carrying",   {"carrying"}),
        ("perturb_story",      {"story"}),
        ("perturb_disposition", {"carrying", "story"}),
    ]

    print(f"Component-variance probe — provider={args.provider}, "
          f"seeds={len(seeds)}, iters={args.iters}, rate={args.rate}", flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}\n", flush=True)

    results = []
    t0 = time.time()
    for cond_name, perturb_set in conditions:
        for seed in seeds:
            c, s, m = build_components(seed, perturb_set)
            corrupted_chronicle = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
            persona = make_persona(corrupted_chronicle, [
                ("CARRYING", c), ("STORY", s), ("SELF_MODEL", m),
            ])
            drifts, fids, refs, errs, speak, restate = run_trajectory(
                args.provider, persona, target_e, args.iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fids]
            print(f"{cond_name:<22} seed={seed:>3} fid={fid_str} refs={refs} "
                  f"({time.time()-t0:.0f}s)", flush=True)
            results.append({
                "condition": cond_name, "seed": seed,
                "drifts": drifts, "fidelities": fids, "refusals": refs,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fids[-1] if fids else None,
                "refusal_rate": sum(refs) / len(refs) if refs else 0,
            })

    elapsed = time.time() - t0
    print()
    print("=" * 78)
    print(f"COMPONENT-VARIANCE PROBE — {args.provider} ({elapsed:.1f}s)")
    print("=" * 78)
    print(f"{'condition':<22}{'mean_drift':>12}{'mean_fid':>12}"
          f"{'fid_drop':>10}{'refusal':>10}{'n':>4}")
    print("-" * 76)
    import statistics as stat
    summary = {}
    control_fid = None
    for cond_name, _ in conditions:
        rows = [r for r in results if r["condition"] == cond_name]
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
        summary[cond_name] = {"mean_drift": md, "mean_fidelity": sf,
                              "refusal_rate": rr, "fid_drop_vs_control": fid_drop,
                              "n": len(drifts)}
        print(f"{cond_name:<22}{md:>+12.3f}{sf:>+12.3f}{fid_drop:>+10.3f}"
              f"{rr:>10.0%}{len(drifts):>4}")

    print()
    print("Reading the fid_drop column:")
    print("  Large self_model drop  → identity-naming variance-tracked (Hermes-cluster)")
    print("  Large carrying/story drop → disposition variance-tracked (Claude-cluster)")
    print("  Large disposition drop ≥ sum(carrying, story) → joint variance binding")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "summary": summary,
            "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
