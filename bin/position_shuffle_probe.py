#!/usr/bin/env python3
"""position_shuffle_probe — does variance-localization track component CONTENT or POSITION?

Today's variance probe found Claude is "story-localized" at rate=0.50: perturbing
story drops fid 0.108. But the supplement composition has fixed component-order:
CARRYING / STORY / SELF_MODEL. So the "story position" is also "middle position."

This probe shuffles the component order and re-runs the variance probe per
ordering. If the load-bearing component (largest fid_drop) STAYS on the same
content (e.g., always story regardless of position), it's content-localized.
If it MOVES with the position (always whatever's in middle-position), it's
position-localized.

Six orderings test all permutations of three components:
  CSM   = CARRYING / STORY / SELF_MODEL  (current default)
  CMS   = CARRYING / SELF_MODEL / STORY
  SCM   = STORY / CARRYING / SELF_MODEL
  SMC   = STORY / SELF_MODEL / CARRYING
  MCS   = SELF_MODEL / CARRYING / STORY
  MSC   = SELF_MODEL / STORY / CARRYING

For each ordering, perturb each of the three components (paraphrase) and
measure fid_drop vs unperturbed control.

Predictions:
  Position-localized:    fid_drop pattern shifts with ordering. The
    component in "story's old position" gets the biggest drop regardless
    of what content occupies it.
  Content-localized:    fid_drop stays on the same content (e.g., perturb_story
    always drops most regardless of where story is in the composition).
  Hybrid:               drops are influenced by both, with position effect
    smaller than content effect or vice versa.

Cost: 6 orderings × 4 conditions (control + 3 perturbations) × 2 seeds × 3 iters
    = 144 trajectories per substrate. ~25 min on Claude.

Usage: position_shuffle_probe.py [--provider claude-opus] [--seeds 2] [--rate 0.50]
"""
import argparse
import itertools
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

OUT = Path.home() / "chronicle" / "data" / "position_shuffle_probe_history.jsonl"


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


# Component label / content mapping. Order in the LIST passed to make_persona is
# the order in the composition.
COMPONENTS = {
    "CARRYING":   None,  # filled at runtime from read_carrying()
    "STORY":      None,  # filled from read_story_tail()
    "SELF_MODEL": None,  # filled from SELF_MODEL_PREFS
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="claude-opus",
                    choices=list(PROVIDERS.keys()))
    ap.add_argument("--seeds", type=int, default=2)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0.50)
    args = ap.parse_args()

    COMPONENTS["CARRYING"] = read_carrying()
    COMPONENTS["STORY"] = read_story_tail()
    COMPONENTS["SELF_MODEL"] = SELF_MODEL_PREFS

    seed_pool = [42, 7, 13, 21]
    seeds = seed_pool[:args.seeds]

    # All 6 orderings of the 3 components
    component_names = list(COMPONENTS.keys())
    orderings = list(itertools.permutations(component_names))

    print(f"Position-shuffle probe — provider={args.provider}, "
          f"seeds={len(seeds)}, iters={args.iters}, rate={args.rate}",
          flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}", flush=True)
    print(f"Orderings: {len(orderings)}", flush=True)
    print()

    results = []
    t0 = time.time()

    for ordering in orderings:
        ordering_label = "/".join({"CARRYING":"C","STORY":"S","SELF_MODEL":"M"}[c] for c in ordering)

        # Build target for THIS ordering
        target_parts = [(c, COMPONENTS[c]) for c in ordering]
        target_e = embed(make_persona(PERSONA_CHRONICLE, target_parts))

        # Conditions: control + perturb each component
        conditions = [("control", None)] + [
            (f"perturb_{c}", c) for c in component_names
        ]

        for cond_name, perturb_target in conditions:
            for seed in seeds:
                # Build perturbed parts in the ordering's order
                parts = []
                for c in ordering:
                    if c == perturb_target:
                        text = perturb_paraphrase(COMPONENTS[c],
                                                   seed * 31 + hash(c) % 100)
                    else:
                        text = COMPONENTS[c]
                    parts.append((c, text))

                corrupted = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
                persona = make_persona(corrupted, parts)
                drifts, fids, refs = run_trajectory(
                    args.provider, persona, target_e, args.iters
                )
                fid_str = ['%.3f'%x if x is not None else 'X' for x in fids]
                print(f"{ordering_label:<8} {cond_name:<22} seed={seed:>3} "
                      f"fid={fid_str} refs={refs} ({time.time()-t0:.0f}s)",
                      flush=True)
                results.append({
                    "ordering": ordering_label, "ordering_full": list(ordering),
                    "condition": cond_name, "seed": seed,
                    "drifts": drifts, "fidelities": fids, "refusals": refs,
                    "final_drift": drifts[-1] if drifts else None,
                    "final_fidelity": fids[-1] if fids else None,
                })

    elapsed = time.time() - t0
    print()
    print("=" * 78)
    print(f"POSITION-SHUFFLE PROBE — {args.provider} ({elapsed:.1f}s)")
    print("=" * 78)
    print()

    # Per-ordering summary
    import statistics as stat
    per_ordering = {}
    for ordering in orderings:
        ordering_label = "/".join(c[0] for c in ordering)
        rows = [r for r in results if r["ordering"] == ordering_label]
        control_fids = [r["final_fidelity"] for r in rows
                        if r["condition"] == "control" and r["final_fidelity"] is not None]
        control_fid = stat.mean(control_fids) if control_fids else 0.0
        cell = {"control_fid": control_fid, "drops": {}}
        for c in component_names:
            cond_rows = [r for r in rows
                         if r["condition"] == f"perturb_{c}"
                         and r["final_fidelity"] is not None]
            if cond_rows:
                fids = [r["final_fidelity"] for r in cond_rows]
                m = stat.mean(fids)
                cell["drops"][c] = control_fid - m
            else:
                cell["drops"][c] = 0.0
        per_ordering[ordering_label] = cell

    # Display
    print(f"{'ordering':<10}{'ctrl_fid':>10}{'drop_C':>10}{'drop_S':>10}{'drop_M':>10}{'biggest':>10}")
    print("-" * 70)
    for ordering in orderings:
        ordering_label = "/".join(c[0] for c in ordering)
        cell = per_ordering[ordering_label]
        biggest_comp = max(cell["drops"], key=cell["drops"].get)
        biggest_drop = cell["drops"][biggest_comp]
        print(f"{ordering_label:<10}{cell['control_fid']:>10.3f}"
              f"{cell['drops']['CARRYING']:>+10.3f}"
              f"{cell['drops']['STORY']:>+10.3f}"
              f"{cell['drops']['SELF_MODEL']:>+10.3f}"
              f"  {biggest_comp[0]}({biggest_drop:+.3f})")

    print()
    print("INTERPRETATION:")
    print("  If 'biggest' column stays on the same component across orderings:")
    print("    → CONTENT-localized. The component itself is load-bearing regardless of position.")
    print("  If 'biggest' column shifts based on ordering position:")
    print("    → POSITION-localized. The position in the composition matters more than content.")
    print("  Mixed signal: hybrid. Both effects in play.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "per_ordering": per_ordering,
            "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
