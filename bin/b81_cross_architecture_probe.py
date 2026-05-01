#!/usr/bin/env python3
"""
B81: Cross-Architecture Therapeutic Window Replication

B77v2 found the therapeutic window at L22-24 in Qwen2.5-3B-Instruct.
B79 found RLHF creates the read/write separation.

This probe tests whether the therapeutic window (dose-dependent identity
accuracy degradation at the transition zone) replicates across architectures.

Tests: Mistral-7B-Instruct-v0.3 (different architecture, larger, open-weight)

If the window replicates: it's a general property of instruction-tuned transformers.
If it doesn't: it's Qwen-specific and the paper needs qualification.

Uses same methodology as B77v2 (logistic regression on full hidden states)
with doses 0, 4, 6 to test the critical window points efficiently.
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

MODEL = "mistralai/Mistral-7B-Instruct-v0.3"
DOSES = [0, 4, 6]  # critical window points only
N_PROMPTS = 10
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
]


def format_prompt(ccs, traces, dose):
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
    )
    model.eval()

    n_layers = model.config.num_hidden_layers + 1
    print(f"Model loaded: {n_layers} layers (Mistral-7B has 32 transformer layers)")

    results = {}

    for dose in DOSES:
        print(f"\n{'='*50}")
        print(f"DOSE {dose}")
        print(f"{'='*50}")

        system_A = format_prompt(CCS_A, TRACES_A, dose)
        system_B = format_prompt(CCS_B, TRACES_B, dose)

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

        dose_results = []
        for layer_idx in range(n_layers):
            X = np.array(all_vectors[layer_idx])
            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            accuracies = []
            for train_idx, test_idx in skf.split(X, labels):
                clf = LogisticRegression(max_iter=1000, C=1.0)
                clf.fit(X[train_idx], labels[train_idx])
                acc = clf.score(X[test_idx], labels[test_idx])
                accuracies.append(acc)

            mean_acc = np.mean(accuracies)
            std_acc = np.std(accuracies)
            dose_results.append({
                "layer": layer_idx,
                "accuracy": round(float(mean_acc), 4),
                "std": round(float(std_acc), 4),
            })

            if layer_idx % 6 == 0 or layer_idx == n_layers - 1:
                print(f"  L{layer_idx:2d}: acc={mean_acc:.3f} ± {std_acc:.3f}")

        results[f"dose_{dose}"] = dose_results

    # Summary — Mistral has 32 layers, so regions scale
    # Qwen 3B: 36 layers → early L5-15, conflict L17-19, transition L22-24, late L28-35
    # Mistral 7B: 32 layers → early L4-12, conflict L14-16, transition L19-22, late L25-31
    # Scale factor: 32/36 ≈ 0.89
    print("\n\n" + "="*60)
    print("SUMMARY: Accuracy by Layer Region and Dose")
    print("="*60)

    summary = {}
    for dose in DOSES:
        key = f"dose_{dose}"
        dr = results[key]

        early = [r["accuracy"] for r in dr if 4 <= r["layer"] <= 12]
        conflict = [r["accuracy"] for r in dr if 14 <= r["layer"] <= 16]
        transition = [r["accuracy"] for r in dr if 19 <= r["layer"] <= 22]
        late = [r["accuracy"] for r in dr if 25 <= r["layer"] <= 31]

        summary[key] = {
            "dose": dose,
            "early_mean": round(float(np.mean(early)), 4) if early else None,
            "conflict_mean": round(float(np.mean(conflict)), 4) if conflict else None,
            "transition_mean": round(float(np.mean(transition)), 4) if transition else None,
            "late_mean": round(float(np.mean(late)), 4) if late else None,
        }

        print(f"Dose {dose}: early={np.mean(early):.3f} | conflict={np.mean(conflict):.3f} | transition={np.mean(transition):.3f} | late={np.mean(late):.3f}")

    # Therapeutic window check
    print("\n=== THERAPEUTIC WINDOW REPLICATION CHECK ===")
    t0 = summary["dose_0"]["transition_mean"]
    t4 = summary["dose_4"]["transition_mean"]
    t6 = summary["dose_6"]["transition_mean"]

    if t4 and t6:
        if t4 > t6:
            print(f"✓ REPLICATED: Transition zone shows therapeutic window")
            print(f"  Dose 0: {t0:.3f}, Dose 4: {t4:.3f}, Dose 6: {t6:.3f}")
            print(f"  Window confirmed on Mistral-7B (different architecture from Qwen-3B)")
        else:
            print(f"✗ NOT REPLICATED: Dose 6 >= Dose 4 at transition")
            print(f"  Dose 0: {t0:.3f}, Dose 4: {t4:.3f}, Dose 6: {t6:.3f}")

    # Early layer stability
    e0 = summary["dose_0"]["early_mean"]
    e6 = summary["dose_6"]["early_mean"]
    if e0 and e6:
        print(f"\nEarly layers: dose 0={e0:.3f}, dose 6={e6:.3f} (diff={abs(e0-e6):.3f})")
        if abs(e0 - e6) < 0.15:
            print("  ✓ Early layers stable across doses (replicates Qwen finding)")
        elif e6 > e0:
            print("  ✓ Early layers IMPROVE with dose (replicates Qwen finding)")

    # Save
    out = {
        "probe": "B81",
        "model": MODEL,
        "architecture": "Mistral-7B",
        "timestamp": datetime.datetime.now().isoformat(),
        "doses": DOSES,
        "n_prompts": N_PROMPTS,
        "n_layers": n_layers,
        "layer_regions": {
            "early": "L4-12",
            "conflict": "L14-16",
            "transition": "L19-22",
            "late": "L25-31",
            "note": "Scaled from Qwen-3B regions by 32/36 factor",
        },
        "summary": summary,
        "per_layer_results": results,
    }

    outpath = Path(os.environ.get("B81_OUTPUT", "b81_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
