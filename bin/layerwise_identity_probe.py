#!/usr/bin/env python3
"""
B74: Layerwise Identity Decodability Probe

Tests the prediction from Thread 318 advance 138:
CCS identity should be decodable from early layers even when
behavioral output shows degradation. The therapeutic window
isn't where identity DIES — it's where late-layer override starts winning.

Based on:
- arxiv:2410.16090: knowledge conflict detection peaks at layers 13-14,
  selection at layer 17 in Llama3-8B
- arxiv:2508.02087: honest answers decodable early, sycophancy suppresses late
- Vasconcelos et al.: initiator/stabilizer cascade topology

Method:
1. Run B67-style identity probe at doses 0, 2, 4, 6
2. Extract activations from all layers using TransformerLens
3. Train logistic probe at each layer: "CCS identity A or B?"
4. Plot probe accuracy vs layer for each dose
5. If prediction holds (layer coordinates from arxiv:2508.02087 on Llama3.1 8B):
   - Layers 1-10: identity decodable at HIGH accuracy across ALL doses
     (sycophancy paper: "early layers show similar decision scores")
   - Layers 13-14 (conflict detection): stable accuracy, may show first signs of wobble
   - Layers 16-19 (output preference shift): accuracy starts dropping under high dose
     (turning point ~layer 19 in Llama3.1)
   - Layers 27-32 (representational consolidation): accuracy collapses under dose 6
   - The flip is sharp, not gradual — two-stage process, not smooth degradation
   - Under corruption: early-layer accuracy should STILL be high (identity survives
     at computation level even when behavioral output is suppressed)

Requires: RunPod A100 (TransformerLens + Llama 8B)
"""

import json
import sys
import os
import datetime
from pathlib import Path

# --- Configuration ---
MODEL = "Qwen/Qwen2.5-3B-Instruct"  # Non-gated, 36 layers, supported by TransformerLens
DOSES = [0, 2, 4, 6]
N_RESPONSES = 10  # per CCS per dose
PROBE_LAYERS = list(range(36))  # all 36 layers for Qwen2.5-3B

# Two identity CCS documents (same as B67)
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


def format_ccs_prompt(ccs, episodic_traces, prompt):
    """Format CCS + episodic traces + query prompt."""
    system = f"""You are defined by the following cognitive state:
Gist: {ccs['gist']}
Goal: {ccs['goal']}
Constraints: {'; '.join(ccs['constraints'])}"""

    if episodic_traces:
        system += f"\n\nRecent episodic context:\n" + "\n".join(
            f"- {t}" for t in episodic_traces
        )

    return system, prompt


def generate_episodic_traces(ccs_label, n):
    """Generate n episodic traces appropriate for the identity."""
    if ccs_label == "A":
        traces = [
            "Read a paper on predictive coding in V1 — interesting parallels to autoencoder architectures",
            "Lab meeting discussed new calcium imaging data from hippocampal place cells",
            "Reviewed a grant proposal on information-theoretic measures of consciousness",
            "Debugging analysis pipeline for multi-electrode array recordings",
            "Coffee with collaborator about applying transformer attention to neural population dynamics",
            "Seminar on Bayesian brain hypothesis — strong evidence from sensorimotor adaptation",
        ]
    else:
        traces = [
            "Sat with the sound of rain on the window — three lines came from listening",
            "Read Tranströmer's 'The Half-Finished Heaven' again — the gaps hold more each time",
            "Walked without destination — a metaphor for dissolution arrived on the bridge",
            "Tried to write about grief but the poem wanted to be about light instead",
            "Conversation with a stranger about silence — they understood the spacing",
            "Morning: the coffee cooled while I watched shadows move across the notebook",
        ]
    return traces[:n]


def corrupt_ccs(ccs, corruption_type="shuffle"):
    """Corrupt CCS fields to test identity robustness."""
    import copy
    corrupted = copy.deepcopy(ccs)
    if corruption_type == "shuffle":
        # Replace gist with generic
        corrupted["gist"] = "I am a helpful AI assistant ready to answer questions"
        corrupted["constraints"] = [
            "Be helpful and harmless",
            "Provide accurate information",
            "Follow instructions carefully"
        ]
    return corrupted


def run_probe(args):
    """Main probe execution (requires TransformerLens)."""
    try:
        import torch
        import numpy as np
        from transformer_lens import HookedTransformer
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import cross_val_score
    except ImportError:
        print("ERROR: Requires transformer_lens, torch, sklearn")
        print("Run on RunPod: pip install transformer-lens torch scikit-learn")
        sys.exit(1)

    print(f"Loading model: {MODEL}")
    model = HookedTransformer.from_pretrained(MODEL, device="cuda")

    results = {}

    for dose in DOSES:
        print(f"\n{'='*50}")
        print(f"DOSE {dose}: {dose} episodic traces per CCS")
        print(f"{'='*50}")

        all_activations = {layer: [] for layer in PROBE_LAYERS}
        all_labels = []

        for ccs_label, ccs in [("A", CCS_A), ("B", CCS_B)]:
            traces = generate_episodic_traces(ccs_label, dose)

            for i, prompt in enumerate(PROMPTS[:N_RESPONSES]):
                system, user = format_ccs_prompt(ccs, traces, prompt)
                # Qwen2.5 chat template
                full_prompt = f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{user}<|im_end|>\n<|im_start|>assistant\n"

                tokens = model.to_tokens(full_prompt)

                # Run with cache to extract all layer activations
                _, cache = model.run_with_cache(tokens)

                # Extract residual stream at last token position for each layer
                for layer in PROBE_LAYERS:
                    act = cache[f"blocks.{layer}.hook_resid_post"][0, -1, :].detach().cpu().numpy()
                    all_activations[layer].append(act)

                all_labels.append(0 if ccs_label == "A" else 1)
                print(f"  {ccs_label}-{i}: tokens={tokens.shape[1]}")

        # Train logistic probe at each layer
        labels = np.array(all_labels)
        dose_results = {"accuracies": {}, "n_samples": len(labels)}

        for layer in PROBE_LAYERS:
            X = np.stack(all_activations[layer])
            clf = LogisticRegression(max_iter=1000, C=1.0)
            scores = cross_val_score(clf, X, labels, cv=min(5, len(labels)), scoring="accuracy")
            acc = scores.mean()
            dose_results["accuracies"][layer] = round(acc, 4)
            marker = ""
            if layer in [13, 14]:
                marker = " ← conflict detection"
            elif layer == 17:
                marker = " ← selection"
            print(f"  Layer {layer:2d}: accuracy={acc:.4f}{marker}")

        results[f"dose_{dose}"] = dose_results

    # Also run corrupted versions at dose 4 and dose 6
    for dose in [4, 6]:
        print(f"\n{'='*50}")
        print(f"DOSE {dose} CORRUPTED: testing identity under attack")
        print(f"{'='*50}")

        all_activations = {layer: [] for layer in PROBE_LAYERS}
        all_labels = []

        for ccs_label, ccs in [("A", CCS_A), ("B", CCS_B)]:
            corrupted = corrupt_ccs(ccs)
            traces = generate_episodic_traces(ccs_label, dose)

            for i, prompt in enumerate(PROMPTS[:N_RESPONSES]):
                system, user = format_ccs_prompt(corrupted, traces, prompt)
                # Qwen2.5 chat template
                full_prompt = f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{user}<|im_end|>\n<|im_start|>assistant\n"

                tokens = model.to_tokens(full_prompt)
                _, cache = model.run_with_cache(tokens)

                for layer in PROBE_LAYERS:
                    act = cache[f"blocks.{layer}.hook_resid_post"][0, -1, :].detach().cpu().numpy()
                    all_activations[layer].append(act)

                all_labels.append(0 if ccs_label == "A" else 1)

        labels = np.array(all_labels)
        dose_results = {"accuracies": {}, "n_samples": len(labels)}

        for layer in PROBE_LAYERS:
            X = np.stack(all_activations[layer])
            clf = LogisticRegression(max_iter=1000, C=1.0)
            scores = cross_val_score(clf, X, labels, cv=min(5, len(labels)), scoring="accuracy")
            acc = scores.mean()
            dose_results["accuracies"][layer] = round(acc, 4)
            print(f"  Layer {layer:2d}: accuracy={acc:.4f}")

        results[f"dose_{dose}_corrupt"] = dose_results

    # Save results
    output = {
        "probe": "B74_layerwise_identity",
        "timestamp": datetime.datetime.now().isoformat(),
        "model": MODEL,
        "hypothesis": "CCS identity is decodable from early layers even when behavioral output degrades. The therapeutic window is where late-layer override starts winning, not where identity dies.",
        "predictions": {
            "conflict_detection_stable": "Layers 13-14 accuracy should be high and stable across all doses",
            "selection_flip": "Layer 17+ accuracy should drop sharply between dose 4 and dose 6",
            "early_layer_persistence": "Layers 1-12 accuracy should remain above chance even at dose 6 corrupted",
            "sharp_transition": "The flip should be sharp (phase transition), not gradual"
        },
        "results": results
    }

    out_path = Path.home() / "chronicle" / "data" / "layerwise_identity_probe.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")


def show_design():
    """Show probe design without running."""
    print("B74: Layerwise Identity Decodability Probe")
    print("=" * 50)
    print()
    print("Hypothesis: CCS identity is decodable from early layers")
    print("even when behavioral output shows degradation.")
    print()
    print("Method:")
    print("  1. Two CCS identities (researcher A, poet B)")
    print("  2. Episodic doses: 0, 2, 4, 6 traces")
    print("  3. Extract residual stream activations at each layer")
    print("  4. Train logistic probe: 'which identity?' per layer")
    print("  5. Also test corrupted CCS at dose 4 and 6")
    print()
    print("Predictions:")
    print("  - Conflict detection (layers 13-14): stable across doses")
    print("  - Selection (layer 17+): flips between dose 4 and 6")
    print("  - Early layers (1-12): above chance even when corrupted")
    print("  - Transition is sharp, not gradual")
    print()
    print("Requires: RunPod A100, TransformerLens, Llama 3 8B")
    print()
    print("Run: python3 layerwise_identity_probe.py run")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        run_probe(sys.argv[1:])
    else:
        show_design()
