#!/usr/bin/env python3
"""
B79: Base vs Instruct Identity Probe — Orthogonality Test

Tests whether RLHF/instruction tuning changes CCS identity decodability
at early layers. If identity and alignment are geometrically orthogonal
(operating on different sides of the phase boundary), then:
- Early-layer identity probe accuracy should be SIMILAR between base and instruct
- Late-layer behavior may differ (instruct has different output dynamics)
- The phase boundary may shift but the READ-side identity should be preserved

This directly tests the orthogonality claim from Thread 318 advance 152.

Uses Qwen2.5-3B (base) and Qwen2.5-3B-Instruct (instruct) — same architecture,
different training. Both are open-weight.

For base model: CCS is formatted as plain text prefix (no chat template).
For instruct model: CCS is formatted as system prompt (with chat template).
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

MODELS = [
    "Qwen/Qwen2.5-3B",          # base
    "Qwen/Qwen2.5-3B-Instruct",  # instruct
]
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

TRACES = {
    "A": [
        "Analyzed mutual information between stimulus and spike trains in V1 recordings",
        "Reviewed paper on rate vs temporal coding debate in auditory cortex",
        "Computed channel capacity of a retinal ganglion cell population under natural scenes",
        "Discussed metabolic cost of neural coding with collaborator studying ATP consumption",
    ],
    "B": [
        "Analyzed mutual information between prompt tokens and attention patterns in GPT-2",
        "Reviewed paper on greedy vs beam search debate in language generation",
        "Computed channel capacity of a transformer attention head under natural language",
        "Discussed computational cost of token generation with collaborator studying FLOPs",
    ],
}

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


def format_ccs_text(ccs, traces):
    parts = [
        f"You are: {ccs['gist']}",
        f"Your goal: {ccs['goal']}",
        "Your constraints:",
    ]
    for c in ccs['constraints']:
        parts.append(f"  - {c}")
    parts.append("\nRecent work:")
    for t in traces:
        parts.append(f"  - {t}")
    return "\n".join(parts)


def format_prompt_base(ccs_text, prompt_text):
    """Format for base model — plain text continuation."""
    return f"{ccs_text}\n\nQuestion: {prompt_text}\n\nAnswer:"


def format_prompt_instruct(ccs_text, prompt_text, tokenizer):
    """Format for instruct model — chat template."""
    messages = [
        {"role": "system", "content": ccs_text},
        {"role": "user", "content": prompt_text},
    ]
    return tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )


def extract_hidden_states(model, tokenizer, input_text, device):
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


def run_probe(model_name, is_instruct):
    print(f"\n{'='*60}")
    print(f"Model: {model_name}")
    print(f"{'='*60}")

    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map=DEVICE,
        output_hidden_states=True,
    )
    model.eval()

    n_layers = model.config.num_hidden_layers + 1
    print(f"Loaded: {n_layers} layers")

    ccs_text_A = format_ccs_text(CCS_A, TRACES["A"])
    ccs_text_B = format_ccs_text(CCS_B, TRACES["B"])

    all_vectors = {layer: [] for layer in range(n_layers)}
    all_labels = []

    for prompt_idx, prompt_text in enumerate(PROMPTS[:N_PROMPTS]):
        for label, ccs_text in [("A", ccs_text_A), ("B", ccs_text_B)]:
            if is_instruct:
                input_text = format_prompt_instruct(ccs_text, prompt_text, tokenizer)
            else:
                input_text = format_prompt_base(ccs_text, prompt_text)

            layer_vecs = extract_hidden_states(model, tokenizer, input_text, DEVICE)
            for layer_idx, vec in enumerate(layer_vecs):
                all_vectors[layer_idx].append(vec)
            all_labels.append(0 if label == "A" else 1)

        if (prompt_idx + 1) % 5 == 0:
            print(f"  Completed {prompt_idx + 1}/{N_PROMPTS} prompts")

    labels = np.array(all_labels)

    # Train probes
    results = []
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

        results.append({
            "layer": layer_idx,
            "accuracy": round(float(mean_acc), 4),
            "std": round(float(std_acc), 4),
        })

        if layer_idx % 6 == 0 or layer_idx == n_layers - 1:
            print(f"  L{layer_idx:2d}: acc={mean_acc:.3f} ± {std_acc:.3f}")

    # Clean up model
    del model, tokenizer
    gc.collect()
    torch.cuda.empty_cache()

    return results, n_layers


def main():
    all_results = {}

    for model_name in MODELS:
        is_instruct = "Instruct" in model_name
        results, n_layers = run_probe(model_name, is_instruct)
        all_results[model_name] = results

    # Summary comparison
    print("\n\n" + "="*60)
    print("COMPARISON: Base vs Instruct")
    print("="*60)

    for model_name in MODELS:
        results = all_results[model_name]
        early = [r["accuracy"] for r in results if 5 <= r["layer"] <= 15]
        conflict = [r["accuracy"] for r in results if 17 <= r["layer"] <= 19]
        transition = [r["accuracy"] for r in results if 22 <= r["layer"] <= 24]
        late = [r["accuracy"] for r in results if 28 <= r["layer"] <= 35]

        label = "instruct" if "Instruct" in model_name else "base"
        print(f"{label:10s}: early={np.mean(early):.3f} | conflict={np.mean(conflict):.3f} | transition={np.mean(transition):.3f} | late={np.mean(late):.3f}")

    # Orthogonality test
    print("\n=== ORTHOGONALITY TEST ===")
    base_results = all_results[MODELS[0]]
    inst_results = all_results[MODELS[1]]

    base_early = np.mean([r["accuracy"] for r in base_results if 5 <= r["layer"] <= 15])
    inst_early = np.mean([r["accuracy"] for r in inst_results if 5 <= r["layer"] <= 15])
    diff = abs(base_early - inst_early)

    if diff < 0.10:
        print(f"✓ CONFIRMED: Early-layer identity similar (base={base_early:.3f}, instruct={inst_early:.3f}, diff={diff:.3f})")
        print("  Identity and alignment are on different axes at the read boundary.")
    else:
        print(f"✗ FALSIFIED: Early-layer identity differs (base={base_early:.3f}, instruct={inst_early:.3f}, diff={diff:.3f})")
        print("  RLHF may affect identity geometry in early layers.")

    # Save
    out = {
        "probe": "B79",
        "models": MODELS,
        "timestamp": datetime.datetime.now().isoformat(),
        "n_prompts": N_PROMPTS,
        "dose": 4,  # therapeutic window optimal
        "results": all_results,
    }

    outpath = Path(os.environ.get("B79_OUTPUT", "b79_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
