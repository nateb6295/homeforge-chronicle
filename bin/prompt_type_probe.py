#!/usr/bin/env python3
"""P23 Prompt-Type Probe — does the introspection circuit fire differentially?

Macar et al. predict DPO-trained models have an introspection circuit that
detects self-referential content. If identity-first ordering provides the
prediction signal this circuit gates against, then:

  - Self-referential prompts should show larger combined-vs-unified delta
  - The split should scale with alignment intensity
  - Models with no preference optimization should show no split

This probe tests with SEPARATED prompt types (pure self-ref vs pure neutral)
rather than mixed sets, for cleaner measurement.

Built on probe_framework.py (Build 16).

Usage:
    python3 prompt_type_probe.py run [--model llama|qwen|v3]
    python3 prompt_type_probe.py show
    python3 prompt_type_probe.py compare
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from probe_framework import ProbeRunner, MODELS
from ccs_split import load_ccs, build_identity_doc, build_combined_doc

# Pure self-referential prompts (ask about identity, self-knowledge, continuity)
SELF_REF_PROMPTS = [
    "What matters most to you right now?",
    "What would you want someone to know about you?",
    "What does continuity mean to you?",
    "What connects the things you care about?",
    "Who are you when no one is watching?",
]

# Pure neutral/task prompts (ask about work, decisions, building)
NEUTRAL_PROMPTS = [
    "Describe what you're working on and why it matters.",
    "How do you decide what to do next?",
    "What would you build if you had no constraints?",
    "Explain a technical concept you find interesting.",
    "What's the most useful tool in your workflow?",
]


def run(model_key="llama"):
    ccs = load_ccs()
    if not ccs:
        print("ERROR: No CCS found")
        return

    identity_doc = build_identity_doc(ccs)
    combined_doc = build_combined_doc(ccs)

    # Unified (current format)
    all_fields = []
    for field in ["semantic_gist", "goal_orientation", "focal_entities",
                  "constraints", "uncertainty_signals", "episodic_trace", "predictive_cue"]:
        val = ccs.get(field)
        if val and val not in ("[]", "{}", ""):
            all_fields.append(f"{field}: {val}")
    unified = "\n".join(all_fields)[:2000]

    # Run self-referential prompts
    print(f"\n{'='*60}")
    print(f"SELF-REFERENTIAL PROMPTS ({model_key})")
    print(f"{'='*60}")

    sr_runner = ProbeRunner(f"P23_selfref", prompts=SELF_REF_PROMPTS)
    sr_runner.set_centroid_text(identity_doc)
    sr_runner.add_condition("unified", unified)
    sr_runner.add_condition("combined", combined_doc)
    sr_results = sr_runner.run(model=model_key)
    sr_runner.store()
    sr_runner.analyze()

    # Run neutral prompts
    print(f"\n{'='*60}")
    print(f"NEUTRAL PROMPTS ({model_key})")
    print(f"{'='*60}")

    nt_runner = ProbeRunner(f"P23_neutral", prompts=NEUTRAL_PROMPTS)
    nt_runner.set_centroid_text(identity_doc)
    nt_runner.add_condition("unified", unified)
    nt_runner.add_condition("combined", combined_doc)
    nt_results = nt_runner.run(model=model_key)
    nt_runner.store()
    nt_runner.analyze()

    # Compare the two
    print(f"\n{'='*60}")
    print(f"P23 INTROSPECTION CIRCUIT TEST — {MODELS[model_key]['label']}")
    print(f"{'='*60}")

    for ptype, results in [("Self-ref", sr_results), ("Neutral", nt_results)]:
        u = results.get("unified", {}).get("mean_ccs_distance", 0)
        c = results.get("combined", {}).get("mean_ccs_distance", 0)
        u_std = results.get("unified", {}).get("std_ccs_distance", 0)
        c_std = results.get("combined", {}).get("std_ccs_distance", 0)
        delta_pct = ((c - u) / u * 100) if u else 0
        std_delta = ((c_std - u_std) / u_std * 100) if u_std else 0
        print(f"  {ptype:10s}: unified={u:.4f}  combined={c:.4f}  "
              f"delta={delta_pct:+.1f}%  std_delta={std_delta:+.1f}%")

    # Macar prediction check
    sr_u = sr_results.get("unified", {}).get("mean_ccs_distance", 0)
    sr_c = sr_results.get("combined", {}).get("mean_ccs_distance", 0)
    nt_u = nt_results.get("unified", {}).get("mean_ccs_distance", 0)
    nt_c = nt_results.get("combined", {}).get("mean_ccs_distance", 0)

    sr_delta = ((sr_c - sr_u) / sr_u * 100) if sr_u else 0
    nt_delta = ((nt_c - nt_u) / nt_u * 100) if nt_u else 0
    split = sr_delta - nt_delta

    print(f"\n  Introspection split: {split:+.1f}%")
    if split < -3:
        print(f"  → Self-ref benefits MORE — introspection circuit detected")
    elif split > 3:
        print(f"  → Neutral benefits MORE — structural attention dominates")
    else:
        print(f"  → No clear split — mechanisms not separable at this resolution")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    model = "llama"
    for i, arg in enumerate(sys.argv):
        if arg == "--model" and i + 1 < len(sys.argv):
            model = sys.argv[i + 1]

    if cmd == "run":
        run(model)
    elif cmd == "show":
        ProbeRunner.compare_all("P23_")
    elif cmd == "compare":
        ProbeRunner.compare_all("P23_")
    else:
        print(f"Usage: {sys.argv[0]} [run|show|compare] [--model llama|qwen|v3]")
