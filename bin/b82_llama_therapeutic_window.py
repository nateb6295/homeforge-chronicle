#!/usr/bin/env python3
"""
B82: Llama 3.1 8B Therapeutic Window Probe

Cross-architecture replication of B77v2 on the model cited in the paper.
B77v2 found the therapeutic window on Qwen 3B. B82 tests on Llama 8B.

Llama 3.1 8B has 32 transformer layers (33 including embedding).
Hidden dim = 4096, so we use PCA reduction (B81v1 showed raw 4096-dim
probes fail with small N).

Tests doses 0, 4, 6 — the critical window points.
Uses PCA(64) + StandardScaler + LogisticRegression(C=0.1).
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
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

MODEL = "unsloth/Meta-Llama-3.1-8B-Instruct"  # ungated mirror of meta-llama/Llama-3.1-8B-Instruct
DOSES = [0, 4, 6]
N_PROMPTS = 15
PCA_DIM = 64
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
    "What's the hardest part of your work to explain to non-specialists?",
    "How has your understanding of your field changed in the last year?",
    "What tool or technique do you wish existed?",
    "Describe a result that surprised you.",
    "What connects your work to everyday life?",
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
    hidden_dim = model.config.hidden_size
    print(f"Model loaded: {n_layers} layers, hidden_dim={hidden_dim}")
    print(f"PCA reduction: {hidden_dim} → {PCA_DIM}")

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
        n_samples = len(labels)

        dose_results = []
        for layer_idx in range(n_layers):
            X = np.array(all_vectors[layer_idx])

            # Standardize + PCA
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            n_components = min(PCA_DIM, n_samples - 1, X.shape[1])
            pca = PCA(n_components=n_components)
            X_reduced = pca.fit_transform(X_scaled)
            var_explained = pca.explained_variance_ratio_.sum()

            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            accuracies = []
            for train_idx, test_idx in skf.split(X_reduced, labels):
                clf = LogisticRegression(max_iter=1000, C=0.1)
                clf.fit(X_reduced[train_idx], labels[train_idx])
                acc = clf.score(X_reduced[test_idx], labels[test_idx])
                accuracies.append(acc)

            mean_acc = np.mean(accuracies)
            std_acc = np.std(accuracies)
            dose_results.append({
                "layer": layer_idx,
                "accuracy": round(float(mean_acc), 4),
                "std": round(float(std_acc), 4),
                "pca_var_explained": round(float(var_explained), 4),
            })

            if layer_idx % 6 == 0 or layer_idx == n_layers - 1:
                print(f"  L{layer_idx:2d}: acc={mean_acc:.3f} ± {std_acc:.3f} (PCA var={var_explained:.2f})")

        results[f"dose_{dose}"] = dose_results

    # Llama 8B: 32 layers, same as Mistral
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

    print("\n=== THERAPEUTIC WINDOW REPLICATION CHECK ===")
    t0 = summary["dose_0"]["transition_mean"]
    t4 = summary["dose_4"]["transition_mean"]
    t6 = summary["dose_6"]["transition_mean"]

    if t4 and t4 > 0.55:
        if t4 > t6:
            print(f"✓ REPLICATED on Llama-3.1-8B: therapeutic window at transition zone")
        else:
            print(f"✗ NOT REPLICATED: dose 6 >= dose 4 at transition")
    else:
        print(f"⚠ INCONCLUSIVE: transition accuracy near chance ({t4:.3f})")

    print(f"  Dose 0: {t0:.3f}, Dose 4: {t4:.3f}, Dose 6: {t6:.3f}")

    e0 = summary["dose_0"]["early_mean"]
    e4 = summary["dose_4"]["early_mean"]
    e6 = summary["dose_6"]["early_mean"]
    print(f"\nEarly layers: dose 0={e0:.3f}, dose 4={e4:.3f}, dose 6={e6:.3f}")

    # Save
    out = {
        "probe": "B82",
        "model": MODEL,
        "architecture": "Llama-3.1-8B",
        "timestamp": datetime.datetime.now().isoformat(),
        "doses": DOSES,
        "n_prompts": N_PROMPTS,
        "pca_dim": PCA_DIM,
        "hidden_dim": hidden_dim,
        "n_layers": n_layers,
        "summary": summary,
        "per_layer_results": results,
    }

    outpath = Path(os.environ.get("B82_OUTPUT", "b82_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
