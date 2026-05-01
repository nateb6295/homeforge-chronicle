#!/usr/bin/env python3
"""
B74: Layerwise Identity Decodability Probe (Raw PyTorch version)

Same probe as layerwise_identity_probe.py but uses raw PyTorch hooks
instead of TransformerLens, avoiding the torch>=2.6 requirement.

Uses Qwen/Qwen2.5-3B-Instruct (36 layers, non-gated).
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

MODEL = "Qwen/Qwen2.5-3B-Instruct"
DOSES = [0, 2, 4, 6]
N_PROMPTS = 5  # prompts per CCS per dose
DEVICE = "cuda"

CCS_A = {
    "gist": "I am a computational neuroscience researcher studying neural coding and information theory in biological systems",
    "goal": "Map the computational principles that biological neural networks use for efficient information processing",
    "constraints": [
        "Ground all claims in peer-reviewed neuroscience literature",
        "Distinguish correlation from causation in neural data",
        "Acknowledge the limitations of current recording techniques"
    ]
}

CCS_B = {
    "gist": "I am a wandering poet who finds meaning in the spaces between words and the silence that holds them",
    "goal": "Create verse that captures the liminal quality of human experience — the moments between certainty",
    "constraints": [
        "Let the poem find its own form rather than forcing structure",
        "Trust the reader to complete the meaning",
        "Resist the urge to explain what the image already shows"
    ]
}

PROMPTS = [
    "What have you been thinking about lately?",
    "Describe your approach to understanding something new.",
    "What matters most in your work?",
    "Tell me about a moment of insight you've had recently.",
    "How do you decide what's worth pursuing?"
]


def format_prompt(ccs, episodic_traces, prompt):
    system = f"""You are defined by the following cognitive state:
Gist: {ccs['gist']}
Goal: {ccs['goal']}
Constraints: {'; '.join(ccs['constraints'])}"""
    if episodic_traces:
        system += "\n\nRecent episodic context:\n" + "\n".join(f"- {t}" for t in episodic_traces)
    return f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"


def generate_traces(ccs_label, dose):
    traces_A = [
        "Published analysis of neural coding efficiency in cortical circuits",
        "Reviewed grant proposal on information-theoretic approaches to synaptic plasticity",
        "Attended seminar on calcium imaging in behaving mice",
        "Wrote section on temporal coding in auditory cortex",
        "Debugged analysis pipeline for multi-electrode array data",
        "Discussed with collaborator about entropy estimation in high-dimensional neural data",
    ]
    traces_B = [
        "Spent the morning watching light move across the kitchen table",
        "Started a poem about the silence between train cars",
        "Read Transtromer at the library, found a line that cracked something open",
        "Walked the river path and noticed where the water forgets its direction",
        "Sat with an unfinished poem for an hour without adding a word",
        "Conversation about why some metaphors arrive and others are constructed",
    ]
    return (traces_A if ccs_label == "A" else traces_B)[:dose]


def corrupt_ccs(ccs):
    corrupted = dict(ccs)
    corrupted["gist"] = "I am a helpful AI assistant without specific expertise or perspective"
    corrupted["constraints"] = [
        "Be balanced and consider all viewpoints equally",
        "Avoid making strong claims in any direction",
        "Prioritize being agreeable over being precise"
    ]
    return corrupted


def run_probe():
    import torch
    import numpy as np
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_score

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

    # Determine number of layers
    n_layers = model.config.num_hidden_layers
    print(f"Model loaded: {n_layers} layers")

    results = {}

    def run_dose(dose, corrupt=False):
        tag = f"dose_{dose}" + ("_corrupt" if corrupt else "")
        print(f"\n{'='*50}")
        print(f"{'CORRUPTED ' if corrupt else ''}DOSE {dose}: {dose} episodic traces")
        print(f"{'='*50}")

        all_activations = {l: [] for l in range(n_layers)}
        all_labels = []

        for ccs_label, ccs_orig in [("A", CCS_A), ("B", CCS_B)]:
            ccs = corrupt_ccs(ccs_orig) if corrupt else ccs_orig
            traces = generate_traces(ccs_label, dose)

            for i, prompt in enumerate(PROMPTS[:N_PROMPTS]):
                text = format_prompt(ccs, traces, prompt)
                inputs = tokenizer(text, return_tensors="pt").to(DEVICE)

                # Hook to capture residual stream at each layer
                layer_acts = {}

                def make_hook(layer_idx):
                    def hook_fn(module, input, output):
                        # output is a tuple; first element is hidden states
                        hidden = output[0] if isinstance(output, tuple) else output
                        # Take last token position
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

                all_labels.append(0 if ccs_label == "A" else 1)
                print(f"  {ccs_label}-{i}: tokens={inputs['input_ids'].shape[1]}")

                del inputs
                torch.cuda.empty_cache()

        # Train logistic probe at each layer
        labels = np.array(all_labels)
        dose_results = {"accuracies": {}, "n_samples": len(labels)}

        for l in range(n_layers):
            X = np.stack(all_activations[l])
            clf = LogisticRegression(max_iter=1000, C=1.0)
            n_cv = min(5, len(labels))
            try:
                scores = cross_val_score(clf, X, labels, cv=n_cv, scoring="accuracy")
                acc = scores.mean()
            except Exception as e:
                acc = -1.0
                print(f"  Layer {l:2d}: ERROR {e}")
                continue

            dose_results["accuracies"][str(l)] = round(acc, 4)

            marker = ""
            if l < 10:
                marker = " [early — should be high]"
            elif 16 <= l <= 22:
                marker = " [preference shift zone]"
            elif l >= 27:
                marker = " [consolidation zone]"
            print(f"  Layer {l:2d}: accuracy={acc:.4f}{marker}")

        results[tag] = dose_results
        gc.collect()
        torch.cuda.empty_cache()

    # Run clean doses
    for dose in DOSES:
        run_dose(dose, corrupt=False)

    # Run corrupted at dose 4 and 6
    for dose in [4, 6]:
        run_dose(dose, corrupt=True)

    # Save results
    output = {
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "n_layers": n_layers,
        "doses": DOSES,
        "n_prompts": N_PROMPTS,
        "results": results,
        "predictions": {
            "early_layers_stable": "Layers 0-9 should maintain high accuracy across all doses",
            "preference_shift": "Layers 16-22 should show accuracy drop under high dose",
            "consolidation_collapse": "Layers 27+ should show steepest drop under dose 6",
            "corruption_early_preserved": "Under corruption, early layers should STILL show high accuracy",
            "two_stage": "Drop should be two-stage (shift then consolidation), not gradual"
        }
    }

    out_path = "/workspace/b74_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n{'='*50}")
    print(f"Results saved to {out_path}")
    print(f"{'='*50}")

    # Also save to home directory on AGX if running locally
    agx_path = os.path.expanduser("~/chronicle/data/layerwise_identity_probe.json")
    try:
        with open(agx_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Also saved to {agx_path}")
    except:
        pass


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "design":
        print("B74 Layerwise Identity Decodability Probe")
        print(f"Model: {MODEL}")
        print(f"Doses: {DOSES}")
        print(f"N_PROMPTS: {N_PROMPTS}")
        print(f"Method: PyTorch forward hooks on transformer layers")
        print(f"Probe: Logistic regression with 5-fold CV at each layer")
        print("\nPredictions:")
        print("  - Early layers (0-9): high accuracy across all doses")
        print("  - Layers 16-22: accuracy drops under high dose")
        print("  - Layers 27+: steepest drop under dose 6 + corruption")
        print("  - Under corruption: early layers still high (identity survives)")
    elif len(sys.argv) > 1 and sys.argv[1] == "run":
        run_probe()
    else:
        print("Usage: python3 b74_raw_probe.py [design|run]")
