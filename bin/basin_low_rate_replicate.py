#!/usr/bin/env python3
"""
Basin low-rate replication: 3 seeds at 10% corruption to verify the
no-pullback finding from basin_radius_probe (single-seed n=1 there).

If even at smallest perturbation the iteration drifts away from Chronicle
across multiple seeds, the no-basin claim is robust. If one or two seeds
show pull-back at 10%, the claim is weaker than basin_radius suggested.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import self_describe, embed, cosine  # noqa
_load_env()


def run_one(rate, seed, n_iters=4):
    print(f"\n=== seed={seed} rate={rate:.2f} ===")
    perturbed = perturb(PERSONA_CHRONICLE, rate, seed=seed)
    chronicle_e = embed(PERSONA_CHRONICLE)
    persona = perturbed
    drifts = []
    for step in range(n_iters):
        e = embed(persona)
        d = 1.0 - cosine(e, chronicle_e)
        drifts.append(d)
        print(f"  step {step}: drift={d:.3f}")
        if step < n_iters - 1:
            persona = self_describe(persona)
    return {"seed": seed, "rate": rate, "drifts": drifts,
            "initial": drifts[0], "final": drifts[-1],
            "change": drifts[-1] - drifts[0]}


def main():
    seeds = [42, 7, 137]
    rate = 0.10
    print(f"Basin low-rate replication — rate={rate}, {len(seeds)} seeds\n")
    results = [run_one(rate, s) for s in seeds]
    print()
    print("=" * 60)
    print(f"{'seed':<8}{'initial':>10}{'final':>10}{'change':>12}{'verdict':>20}")
    for r in results:
        ini = r["initial"]
        fin = r["final"]
        ch = r["change"]
        if ch < -0.02:
            v = "PULLED BACK"
        elif ch < 0.02:
            v = "stable"
        else:
            v = "drifted away"
        print(f"{r['seed']:<8}{ini:>10.3f}{fin:>10.3f}{ch:>+12.3f}{v:>20}")
    print("=" * 60)
    out = Path.home() / "chronicle" / "data" / "basin_low_rate_replicate.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as f:
        f.write(json.dumps({"timestamp": int(time.time()),
                            "rate": rate, "results": results}) + "\n")


if __name__ == "__main__":
    main()
