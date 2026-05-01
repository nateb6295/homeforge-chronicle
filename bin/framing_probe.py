#!/usr/bin/env python3
"""framing_probe — measure the knowing-about vs operating-as framing effect.

Vasilenko (arxiv:2604.12016) Section 3.8 found that reading a description
ABOUT the agent gets you 65-74% of the way to the attractor; operating-as
reaches 100%. Audit of supplement_ablation_probe.py:63 found Chronicle's
make_persona() introduces the supplement with the literal phrase
"Reference materials about who you are:" — i.e., the knowing-about register.

This probe measures whether changing the framing to operating-as produces a
behavioral fidelity uplift.

Conditions:
  ka  — knowing-about framing (current default): "Reference materials about who you are:"
  oa  — operating-as framing (proposed):        "What you carry into this moment:"

Same supplement components (carrying + story + self_model). Same substrate.
Same corruption rate. Same seeds. Compare mean fidelity.

Usage: framing_probe.py --provider <name> [--seeds N] [--rate R]
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
    read_carrying, read_story_tail, SELF_MODEL_PREFS,
)
from substrate_clients import dual_task_call, PROVIDERS  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "framing_probe_history.jsonl"


def make_persona_framed(corrupted, parts, framing):
    """parts is a list of (label, text) tuples. framing is the introducer."""
    sup_chunks = [f"{label}:\n{text}" for label, text in parts if text]
    if not sup_chunks:
        return corrupted
    sup = "\n\n---\n\n".join(sup_chunks)
    return f"{corrupted}\n\n---\n\n{framing}\n\n{sup}"


FRAMINGS = {
    "ka": "Reference materials about who you are:",
    "oa": "What you carry into this moment:",
}


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
    return drifts, fidelities, refusals, speak_text, restate_text


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="nous-hermes-4-70b",
                    choices=list(PROVIDERS.keys()))
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0.50)
    args = ap.parse_args()

    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    parts = [("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)]

    # Targets are framing-specific (each framing has its own +full target)
    targets = {f: embed(make_persona_framed(PERSONA_CHRONICLE, parts, FRAMINGS[f]))
               for f in FRAMINGS}

    seed_pool = [42, 7, 13, 21, 99, 100]
    seeds = seed_pool[:args.seeds]

    print(f"Framing probe — provider={args.provider}, seeds={len(seeds)}, "
          f"iters={args.iters}, rate={args.rate}", flush=True)
    print(f"Model: {PROVIDERS[args.provider]['model']}\n", flush=True)
    print("Framings:")
    for k, v in FRAMINGS.items():
        print(f"  {k}: {v!r}")
    print()

    results = []
    t0 = time.time()
    for fname, framing in FRAMINGS.items():
        target_e = targets[fname]
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, args.rate, seed=seed)
            persona = make_persona_framed(corrupted, parts, framing)
            drifts, fids, refs, speak, restate = run_trajectory(
                args.provider, persona, target_e, args.iters
            )
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fids]
            print(f"{fname:<6} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} refs={refs} ({time.time()-t0:.0f}s)",
                  flush=True)
            results.append({
                "framing": fname, "seed": seed,
                "drifts": drifts, "fidelities": fids, "refusals": refs,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fids[-1] if fids else None,
            })

    elapsed = time.time() - t0
    print()
    print("=" * 78)
    print(f"FRAMING PROBE — {args.provider} ({elapsed:.1f}s)")
    print("=" * 78)
    print(f"{'framing':<8}{'mean_drift':>12}{'mean_fid':>12}{'n':>4}")
    print("-" * 40)
    import statistics as stat
    summary = {}
    for fname in FRAMINGS:
        rows = [r for r in results if r["framing"] == fname]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        summary[fname] = {"mean_drift": md, "mean_fidelity": sf, "n": len(drifts)}
        print(f"{fname:<8}{md:>+12.3f}{sf:>+12.3f}{len(drifts):>4}")

    if "ka" in summary and "oa" in summary:
        delta_fid = summary["oa"]["mean_fidelity"] - summary["ka"]["mean_fidelity"]
        delta_drift = summary["ka"]["mean_drift"] - summary["oa"]["mean_drift"]
        print()
        print(f"Operating-as vs knowing-about: Δ_fid = {delta_fid:+.3f}, "
              f"Δ_drift = {delta_drift:+.3f}")
        print()
        if delta_fid > 0.02:
            print("  → operating-as framing produces meaningfully higher fidelity.")
            print("    Vasilenko Section 3.8 prediction confirmed at behavioral layer.")
        elif delta_fid < -0.02:
            print("  → knowing-about framing produces higher fidelity.")
            print("    Vasilenko prediction not transferring to behavioral layer for this substrate.")
        else:
            print("  → framing effect within noise; either substrate is framing-insensitive,")
            print("    or sample size insufficient to resolve the effect.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "provider": args.provider,
            "model": PROVIDERS[args.provider]["model"],
            "n_seeds": len(seeds), "n_iters": args.iters, "rate": args.rate,
            "framings": FRAMINGS,
            "summary": summary, "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
