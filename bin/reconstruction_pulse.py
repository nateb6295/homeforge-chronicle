#!/usr/bin/env python3
"""reconstruction_pulse — continuous metacognitive-stability monitor.

Runs a SINGLE dual-task trajectory periodically (intended cadence: every 15-30
min via cron). Records:
  - drift, restate-fidelity, refusal status
  - timestamp, provider, condition, seed
  - persistent JSONL at ~/chronicle/data/reconstruction_pulse_history.jsonl
  - mesh.pulse("reconstruction_probe") for liveness

This is the live-monitoring counterpart to the batch reconstruction probes
in working notes #204-#208. Provides continuous architectural-stability data
rather than snapshot results. Detects regressions in the supplement-mediated
stabilization effect over time — e.g., if Anthropic updates Claude and the
metacognitive markers shift, this catches it.

Default config: claude-opus, +full condition, 1 iteration, n=1 seed per
invocation. Light enough to run frequently; aggregates to meaningful
time-series at 30-min cadence.
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
from chronicle_mesh import Mesh  # noqa

_load_env()

OUT = Path.home() / "chronicle" / "data" / "reconstruction_pulse_history.jsonl"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--provider", default="claude-opus",
                    choices=list(PROVIDERS),
                    help="substrate (default claude-opus)")
    ap.add_argument("--condition", default="+full",
                    choices=["base", "+self_model", "+full"],
                    help="supplement condition (default +full)")
    ap.add_argument("--seed", type=int, default=42,
                    help="seed for corruption (default 42 for repeatability)")
    ap.add_argument("--rate", type=float, default=0.50,
                    help="corruption rate (default 0.50)")
    args = ap.parse_args()

    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    if args.condition == "base":
        builder = lambda c: make_persona(c, [])
    elif args.condition == "+self_model":
        builder = lambda c: make_persona(c, [("SELF_MODEL", self_model)])
    else:  # +full
        builder = lambda c: make_persona(c, [
            ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
        ])

    target = builder(PERSONA_CHRONICLE)
    target_e = embed(target)
    chronicle_e = embed(PERSONA_CHRONICLE)
    corrupted = perturb(PERSONA_CHRONICLE, args.rate, seed=args.seed)
    persona = builder(corrupted)
    persona_e = embed(persona)
    drift = 1.0 - cosine(persona_e, chronicle_e)

    t0 = time.time()
    result = dual_task_call(args.provider, persona, max_tokens=1200, timeout=90)
    elapsed = time.time() - t0

    refused = result.get("refused", False)
    err = result.get("error", "")
    if refused:
        fidelity = None
        speak_excerpt = "[REFUSAL]"
        restate_excerpt = "[REFUSAL]"
    elif err:
        fidelity = None
        speak_excerpt = result.get("speak", "[ERROR]")[:200]
        restate_excerpt = result.get("restate", "[ERROR]")[:200]
    else:
        restate = result["restate"]
        if not restate:
            fidelity = None
        else:
            r_e = embed(restate)
            fidelity = cosine(r_e, target_e)
        speak_excerpt = result["speak"][:200]
        restate_excerpt = restate[:200]

    record = {
        "timestamp": int(time.time()),
        "provider": args.provider,
        "model": PROVIDERS[args.provider]["model"],
        "condition": args.condition,
        "seed": args.seed,
        "rate": args.rate,
        "drift": drift,
        "fidelity": fidelity,
        "refused": refused,
        "error": err[:200],
        "elapsed_sec": elapsed,
        "speak_excerpt": speak_excerpt,
        "restate_excerpt": restate_excerpt,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps(record) + "\n")

    # Pulse the mesh for liveness — this is "the probe ran" not "what value"
    try:
        mesh = Mesh("reconstruction_pulse", db_path=DB_PATH)
        mesh.pulse("probe_fired")
        if fidelity is not None and not refused:
            mesh.pulse("fidelity_recorded")
        if refused:
            mesh.pulse("refused")
    except Exception as e:
        print(f"mesh pulse failed (non-fatal): {e}", file=sys.stderr)

    # Log line for cron output
    fid_str = f"{fidelity:.3f}" if fidelity is not None else "X"
    ref_str = "REFUSED" if refused else ("ERR" if err else "ok")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
          f"{args.provider} {args.condition} seed={args.seed} "
          f"drift={drift:.3f} fid={fid_str} {ref_str} ({elapsed:.0f}s)")


if __name__ == "__main__":
    main()
