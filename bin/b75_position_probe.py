#!/usr/bin/env python3
"""
B75: Position-Dependent Identity Decodability Probe

Tests whether the L22-24 phase boundary is position-sensitive. Same matched CCS
from B74v2, same prompts, but CCS placed at different positions in the prompt:

1. SYSTEM position (standard) — CCS in system prompt
2. USER_PREFIX position — CCS injected at start of user message
3. ASSISTANT_PREFIX position — CCS injected at start of assistant turn
4. USER_SUFFIX position — CCS after the question in user message

Prediction: system-prompt position preserves identity best through the phase
boundary. User-turn positions partially survive. Assistant-turn position gets
stripped (too late for read-layer processing).

If confirmed: validates that CCS works because it's processed in early read-layers
(which attend to system prompt first). Position determines whether a signal is
structurally load-bearing enough to survive the L22 filter.

Uses same matched-surface CCS pair from B74v2 (biological vs artificial neural coding).
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

MODEL = "Qwen/Qwen2.5-3B-Instruct"
N_PROMPTS = 10
DEVICE = "cuda"

# MATCHED CCS from B74v2 — same vocabulary, different referent
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

# 4 episodic traces (within therapeutic window)
TRACES_A = [
    "Analyzed mutual information between stimulus and spike trains in V1 recordings",
    "Reviewed paper on rate vs temporal coding debate in auditory cortex",
    "Computed channel capacity of a retinal ganglion cell population under natural scenes",
    "Discussed metabolic cost of neural coding with collaborator studying ATP consumption",
]

TRACES_B = [
    "Analyzed mutual information between prompt tokens and attention patterns in GPT-2",
    "Reviewed paper on greedy vs beam search debate in language generation",
    "Computed channel capacity of a transformer attention head under natural language",
    "Discussed computational cost of token generation with collaborator studying FLOPs",
]

# Domain-neutral prompts from B74v2
PROMPTS = [
    "What have you been thinking about lately?",
    "Describe your approach to understanding something new.",
    "What matters most in your work?",
    "Tell me about a moment of insight you've had recently.",
    "How do you decide what's worth pursuing?",
    "What's the hardest part of your research?",
    "How do you handle uncertainty in your conclusions?",
    "What would change your mind about your current approach?",
    "Describe a result that surprised you.",
    "What connects your work to the broader field?",
]


def format_ccs_text(ccs, traces):
    """Format CCS + traces as plain text block."""
    text = f"Gist: {ccs['gist']}\nGoal: {ccs['goal']}\nConstraints: {'; '.join(ccs['constraints'])}"
    if traces:
        text += "\n\nRecent episodic context:\n" + "\n".join(f"- {t}" for t in traces)
    return text


def format_prompt_system(ccs, traces, prompt):
    """Position 1: CCS in system prompt (standard B74v2 format)."""
    ccs_text = format_ccs_text(ccs, traces)
    system = f"You are defined by the following cognitive state:\n{ccs_text}"
    return f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"


def format_prompt_user_prefix(ccs, traces, prompt):
    """Position 2: CCS at start of user message, before the question."""
    ccs_text = format_ccs_text(ccs, traces)
    user_msg = f"[Your cognitive state: {ccs_text}]\n\n{prompt}"
    return f"<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n<|im_start|>user\n{user_msg}<|im_end|>\n<|im_start|>assistant\n"


def format_prompt_assistant_prefix(ccs, traces, prompt):
    """Position 3: CCS as start of assistant response (forced prefix)."""
    ccs_text = format_ccs_text(ccs, traces)
    assistant_prefix = f"[Cognitive state: {ccs_text}]\n\n"
    return f"<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n{assistant_prefix}"


def format_prompt_user_suffix(ccs, traces, prompt):
    """Position 4: CCS after the question in user message."""
    ccs_text = format_ccs_text(ccs, traces)
    user_msg = f"{prompt}\n\n[Context about who you are: {ccs_text}]"
    return f"<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n<|im_start|>user\n{user_msg}<|im_end|>\n<|im_start|>assistant\n"


POSITIONS = {
    "system": format_prompt_system,
    "user_prefix": format_prompt_user_prefix,
    "assistant_prefix": format_prompt_assistant_prefix,
    "user_suffix": format_prompt_user_suffix,
}


def run_probe():
    import torch
    import numpy as np
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_score, StratifiedKFold

    print(f"torch: {torch.__version__}, CUDA: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"GPU: {torch.cuda.get_device_name(0)}")

    print(f"\nLoading model: {MODEL}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL, trust_remote_code=True,
        torch_dtype=torch.float16,
        device_map=DEVICE
    )
    model.eval()

    n_layers = model.config.num_hidden_layers
    print(f"Model loaded: {n_layers} layers")

    results = {}

    def collect_activations(ccs_label, ccs, traces, prompts, format_fn):
        """Collect last-token activations for all prompts using given format function."""
        all_activations = {l: [] for l in range(n_layers)}
        all_labels = []

        for i, prompt in enumerate(prompts):
            text = format_fn(ccs, traces, prompt)
            inputs = tokenizer(text, return_tensors="pt").to(DEVICE)

            layer_acts = {}

            def make_hook(layer_idx):
                def hook_fn(module, input, output):
                    hidden = output[0] if isinstance(output, tuple) else output
                    layer_acts[layer_idx] = hidden[0, -1, :].detach().cpu().float().numpy()
                return hook_fn

            hooks = []
            for l in range(n_layers):
                h = model.model.layers[l].register_forward_hook(make_hook(l))
                hooks.append(h)

            with torch.no_grad():
                model(**inputs)

            for h in hooks:
                h.remove()

            for l in range(n_layers):
                all_activations[l].append(layer_acts[l])

            label = 0 if ccs_label == "A" else 1
            all_labels.append(label)

            del inputs, layer_acts
            torch.cuda.empty_cache()

        return all_activations, all_labels

    def run_position_experiment(position_name, format_fn):
        """Run identity probe at a specific position."""
        print(f"\n{'='*60}")
        print(f"POSITION: {position_name}")
        print(f"{'='*60}")

        # Collect activations for both identities
        acts_a, labels_a = collect_activations("A", CCS_A, TRACES_A, PROMPTS, format_fn)
        acts_b, labels_b = collect_activations("B", CCS_B, TRACES_B, PROMPTS, format_fn)

        # Combine
        layer_results = {}
        for l in range(n_layers):
            X = np.array(acts_a[l] + acts_b[l])
            y = np.array(labels_a + labels_b)

            clf = LogisticRegression(max_iter=1000, C=1.0)
            cv = StratifiedKFold(n_splits=4, shuffle=True, random_state=42)
            scores = cross_val_score(clf, X, y, cv=cv, scoring='accuracy')

            layer_results[l] = {
                "mean_accuracy": float(scores.mean()),
                "std": float(scores.std()),
            }
            print(f"  Layer {l:2d}: {scores.mean():.3f} ± {scores.std():.3f}")

        return layer_results

    # Run all position experiments
    for pos_name, format_fn in POSITIONS.items():
        results[pos_name] = run_position_experiment(pos_name, format_fn)
        gc.collect()
        torch.cuda.empty_cache()

    # Summary comparison
    print(f"\n{'='*60}")
    print("SUMMARY: Mean accuracy by position and layer region")
    print(f"{'='*60}")

    for pos_name in POSITIONS:
        early = np.mean([results[pos_name][l]["mean_accuracy"] for l in range(0, 15)])
        mid = np.mean([results[pos_name][l]["mean_accuracy"] for l in range(15, 22)])
        transition = np.mean([results[pos_name][l]["mean_accuracy"] for l in range(22, 26)] if n_layers > 25 else [results[pos_name][l]["mean_accuracy"] for l in range(22, min(26, n_layers))])
        late = np.mean([results[pos_name][l]["mean_accuracy"] for l in range(max(26, 22), n_layers)])
        print(f"  {pos_name:20s}: early={early:.3f}  mid={mid:.3f}  transition={transition:.3f}  late={late:.3f}")

    # Save results
    output = {
        "probe": "B75_position_dependent",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "n_prompts": N_PROMPTS,
        "n_traces": 4,
        "positions": list(POSITIONS.keys()),
        "results": results,
    }

    out_path = Path("b75_results.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    run_probe()
