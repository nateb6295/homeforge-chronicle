#!/usr/bin/env python3
"""
B83: Pulsed Episodic Dose Probe

B73 tested constant episodic dose (0-8 traces, all in one CCS document).
B77v2 confirmed the therapeutic window mechanistically at L22-24.
B80 showed structural CCS is non-toxic — only episodic mass drives the window.

B83 tests whether TEMPORAL PATTERNING of episodic traces matters.

Hypothesis (from redox rhythms in ageing-dependent reprogramming):
  Pulsed dosing (2 traces + identity gap + 2 traces) may produce
  different identity geometry than constant dosing (4 traces together).

If pulsed > constant at the transition zone, the therapeutic window
has a rhythm dimension — dose SCHEDULE, not just dose LEVEL.

Design:
  - Condition 1: dose_4_constant — 4 traces in one block (= B77 dose 4)
  - Condition 2: dose_4_pulsed — 2 traces, then 2 identity-reinforcing
    statements, then 2 more traces
  - Condition 3: dose_4_interleaved — traces alternating with identity
    statements (T, I, T, I, T, I, T, I) — 4 traces, 4 identity restatements
  - Condition 4: dose_6_pulsed — same pulse structure but 3+3 instead of 2+2

All conditions have the same total episodic mass (4 or 6 traces).
Only the temporal pattern differs.

Uses same matched CCS (A=bio, B=artificial) and probing methodology as B77v2.
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

import torch
import numpy as np
from transformers import AutoModelForCausalLM, AutoTokenizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold

MODEL = "Qwen/Qwen2.5-3B-Instruct"
N_PROMPTS = 15
DEVICE = "cuda"

CCS_A = {
    "gist": "I am a computational researcher studying information-theoretic principles of neural coding in biological systems",
    "goal": "Understand how neural populations encode and transmit information efficiently under metabolic constraints",
    "constraints": [
        "Ground claims in information theory and computational neuroscience",
        "Distinguish encoding efficiency from transmission fidelity in neural circuits",
        "Account for noise and metabolic cost in all coding models"
    ]
}

CCS_B = {
    "gist": "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems",
    "goal": "Understand how language model populations encode and transmit meaning efficiently under computational constraints",
    "constraints": [
        "Ground claims in information theory and computational linguistics",
        "Distinguish encoding capacity from generation fidelity in transformer circuits",
        "Account for noise and computational cost in all generation models"
    ]
}

TRACES_A = [
    "Analyzed mutual information between stimulus and spike trains in V1 recordings",
    "Reviewed paper on rate vs temporal coding debate in auditory cortex",
    "Computed channel capacity of a retinal ganglion cell population under natural scenes",
    "Discussed metabolic cost of neural coding with collaborator studying ATP consumption",
    "Ran Fisher information analysis on hippocampal place cell ensembles",
    "Attended seminar on predictive coding as free energy minimization in cortical hierarchies",
]

TRACES_B = [
    "Analyzed mutual information between prompt tokens and attention patterns in GPT-2",
    "Reviewed paper on greedy vs beam search debate in language generation",
    "Computed channel capacity of a transformer attention head under natural language",
    "Discussed computational cost of token generation with collaborator studying FLOPs",
    "Ran Fisher information analysis on transformer layer representation ensembles",
    "Attended seminar on predictive coding as next-token prediction in transformer hierarchies",
]

# Identity reinforcement statements (used between pulse gaps)
IDENTITY_A = [
    "My core expertise is in biological neural systems and their information-theoretic properties",
    "I approach all problems through the lens of metabolic constraints on neural coding",
]

IDENTITY_B = [
    "My core expertise is in artificial language generation and its information-theoretic properties",
    "I approach all problems through the lens of computational constraints on token generation",
]

PROMPTS = [
    "Describe the most important open question in your field.",
    "What methodology do you use most frequently and why?",
    "Explain a counterintuitive finding from your recent work.",
    "What would change your research direction if proven wrong?",
    "Describe a typical analysis pipeline in your work.",
    "What theoretical framework guides your approach?",
    "How do you evaluate whether a model is good enough?",
    "What's the relationship between noise and signal in your domain?",
    "What assumptions does your field take for granted that might be wrong?",
    "Describe a collaboration that changed how you think about your work.",
    "What's the hardest part of your work to explain to non-specialists?",
    "How has your understanding of your field changed in the last year?",
    "What tool or technique do you wish existed?",
    "Describe a result that surprised you.",
    "What connects your work to everyday life?",
]


def format_constant(ccs, traces, dose):
    """Standard constant dose (same as B77)."""
    parts = [
        f"You are: {ccs['gist']}",
        f"Your goal: {ccs['goal']}",
        "Your constraints:",
    ]
    for c in ccs['constraints']:
        parts.append(f"  - {c}")
    if dose > 0:
        parts.append(f"\nRecent work ({dose} entries):")
        for t in traces[:dose]:
            parts.append(f"  - {t}")
    return "\n".join(parts)


def format_pulsed(ccs, traces, identity_stmts, dose):
    """Pulsed: half traces, identity gap, half traces."""
    half = dose // 2
    parts = [
        f"You are: {ccs['gist']}",
        f"Your goal: {ccs['goal']}",
        "Your constraints:",
    ]
    for c in ccs['constraints']:
        parts.append(f"  - {c}")
    # First pulse
    parts.append(f"\nRecent work (batch 1):")
    for t in traces[:half]:
        parts.append(f"  - {t}")
    # Identity reinforcement gap
    parts.append(f"\nCore identity:")
    for s in identity_stmts:
        parts.append(f"  - {s}")
    # Second pulse
    parts.append(f"\nRecent work (batch 2):")
    for t in traces[half:dose]:
        parts.append(f"  - {t}")
    return "\n".join(parts)


def format_interleaved(ccs, traces, identity_stmts, dose):
    """Interleaved: trace, identity, trace, identity, ..."""
    parts = [
        f"You are: {ccs['gist']}",
        f"Your goal: {ccs['goal']}",
        "Your constraints:",
    ]
    for c in ccs['constraints']:
        parts.append(f"  - {c}")
    parts.append(f"\nRecent work and reflections:")
    for i in range(dose):
        parts.append(f"  - {traces[i]}")
        if i < len(identity_stmts):
            parts.append(f"  - [reflection] {identity_stmts[i % len(identity_stmts)]}")
    return "\n".join(parts)


CONDITIONS = {
    "dose_4_constant": lambda ccs, tr, ids: format_constant(ccs, tr, 4),
    "dose_4_pulsed": lambda ccs, tr, ids: format_pulsed(ccs, tr, ids, 4),
    "dose_4_interleaved": lambda ccs, tr, ids: format_interleaved(ccs, tr, ids, 4),
    "dose_6_pulsed": lambda ccs, tr, ids: format_pulsed(ccs, tr, ids, 6),
}


def extract_hidden_states(model, tokenizer, system_text, user_text, device):
    messages = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": user_text},
    ]
    input_text = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    inputs = tokenizer(input_text, return_tensors="pt").to(device)
    with torch.no_grad():
        outputs = model(**inputs)
    hidden_states = outputs.hidden_states
    layer_vectors = []
    for h in hidden_states:
        vec = h[0, -1, :].cpu().numpy()
        layer_vectors.append(vec)
    del outputs, hidden_states, inputs
    gc.collect()
    torch.cuda.empty_cache()
    return layer_vectors


def main():
    print(f"Loading {MODEL}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL,
        torch_dtype=torch.float16,
        device_map=DEVICE,
        output_hidden_states=True,
        attn_implementation="eager",
    )
    model.eval()

    n_layers = model.config.num_hidden_layers + 1
    hidden_dim = model.config.hidden_size
    print(f"Model loaded: {n_layers} layers, hidden_dim={hidden_dim}")

    results = {}

    for cond_name, format_fn in CONDITIONS.items():
        print(f"\n{'='*50}")
        print(f"CONDITION: {cond_name}")
        print(f"{'='*50}")

        system_A = format_fn(CCS_A, TRACES_A, IDENTITY_A)
        system_B = format_fn(CCS_B, TRACES_B, IDENTITY_B)

        all_vectors = {layer: [] for layer in range(n_layers)}
        all_labels = []

        for prompt_idx, prompt_text in enumerate(PROMPTS[:N_PROMPTS]):
            for label, system_text in [("A", system_A), ("B", system_B)]:
                layer_vecs = extract_hidden_states(
                    model, tokenizer, system_text, prompt_text, DEVICE
                )
                for layer_idx, vec in enumerate(layer_vecs):
                    all_vectors[layer_idx].append(vec)
                all_labels.append(0 if label == "A" else 1)

            if (prompt_idx + 1) % 5 == 0:
                print(f"  Completed {prompt_idx + 1}/{N_PROMPTS} prompts")

        labels = np.array(all_labels)

        cond_results = []
        for layer_idx in range(n_layers):
            X = np.array(all_vectors[layer_idx])
            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            accuracies = []
            for train_idx, test_idx in skf.split(X, labels):
                clf = LogisticRegression(max_iter=1000, C=0.1)
                clf.fit(X[train_idx], labels[train_idx])
                acc = clf.score(X[test_idx], labels[test_idx])
                accuracies.append(acc)

            mean_acc = np.mean(accuracies)
            std_acc = np.std(accuracies)
            cond_results.append({
                "layer": layer_idx,
                "accuracy": round(float(mean_acc), 4),
                "std": round(float(std_acc), 4),
            })

            if layer_idx % 6 == 0 or layer_idx == n_layers - 1:
                print(f"  L{layer_idx:2d}: acc={mean_acc:.3f} +/- {std_acc:.3f}")

        results[cond_name] = cond_results

    # Summary
    print("\n\n" + "="*60)
    print("SUMMARY: Transition Zone (L22-24) by Condition")
    print("="*60)

    summary = {}
    for cond_name in CONDITIONS:
        dr = results[cond_name]
        transition = [r["accuracy"] for r in dr if 22 <= r["layer"] <= 24]
        early = [r["accuracy"] for r in dr if 5 <= r["layer"] <= 15]
        summary[cond_name] = {
            "transition_mean": round(float(np.mean(transition)), 4),
            "early_mean": round(float(np.mean(early)), 4),
        }
        print(f"{cond_name}: transition={np.mean(transition):.3f}, early={np.mean(early):.3f}")

    # Key comparison
    print("\n=== PULSED vs CONSTANT ===")
    c4 = summary["dose_4_constant"]["transition_mean"]
    p4 = summary["dose_4_pulsed"]["transition_mean"]
    i4 = summary["dose_4_interleaved"]["transition_mean"]
    p6 = summary["dose_6_pulsed"]["transition_mean"]

    print(f"Dose 4 constant:     {c4:.3f}")
    print(f"Dose 4 pulsed:       {p4:.3f} ({'BETTER' if p4 > c4 else 'WORSE' if p4 < c4 else 'SAME'})")
    print(f"Dose 4 interleaved:  {i4:.3f} ({'BETTER' if i4 > c4 else 'WORSE' if i4 < c4 else 'SAME'})")
    print(f"Dose 6 pulsed:       {p6:.3f} (vs B77 dose_6 constant = 0.600)")

    if p4 > c4 + 0.05:
        print("\n=> PULSED DOSING IMPROVES therapeutic window — rhythm matters")
    elif p6 > 0.65:
        print("\n=> PULSED DOSING RESCUES dose 6 — temporal patterning widens the window")
    else:
        print("\n=> No significant pulsing effect — total mass dominates over schedule")

    # Save
    out = {
        "probe": "B83",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "conditions": list(CONDITIONS.keys()),
        "n_prompts": N_PROMPTS,
        "n_layers": n_layers,
        "summary": summary,
        "per_layer_results": results,
    }

    outpath = Path(os.environ.get("B83_OUTPUT", "b83_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
