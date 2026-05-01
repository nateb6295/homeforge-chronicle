#!/usr/bin/env python3
"""
B76: Selective Episodic Crossing Probe

Tests whether episodic trace TYPE determines survival through the L22-24
phase boundary. B73 showed a therapeutic window (4 traces optimal, 6 toxic).
B74 showed the phase boundary filters signals by structural weight.

Question: Can we widen the therapeutic window by engineering episodic traces
that are structurally similar to identity fields (constraint-like) rather
than narrative?

Three episodic trace types:
1. CONSTRAINT-LIKE: Episodic traces that reinforce constraints
   ("Rejected a paper for lacking information-theoretic grounding")
2. NARRATIVE: Standard episodic traces (what happened)
   ("Analyzed mutual information between stimulus and spike trains")
3. FACTUAL: Data points without constraint or narrative structure
   ("V1 has approximately 140 million neurons; coding capacity ~5 bits/spike")

Prediction: Constraint-like traces survive the phase boundary better because
they share structural properties with the identity fields that already cross.
Narrative traces are intermediate. Factual traces are stripped (surface-level
data, not structurally load-bearing).

If confirmed: the therapeutic window is SHAPEABLE — not just a dose limit
but a signal-type filter. You can carry more identity-reinforcing episodic
content if it's shaped like constraints.
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

MODEL = "Qwen/Qwen2.5-3B-Instruct"
DOSES = [4, 6]  # within and outside therapeutic window
N_PROMPTS = 10
DEVICE = "cuda"

# CCS from B74v2
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

# CONSTRAINT-LIKE episodic traces — reinforce identity boundaries
CONSTRAINT_TRACES_A = [
    "Rejected a manuscript for lacking proper information-theoretic grounding in neural coding claims",
    "Insisted on separating encoding efficiency from transmission fidelity in a collaborative analysis",
    "Refused to model neural coding without accounting for metabolic ATP costs per spike",
    "Corrected a colleague who conflated rate coding with temporal coding without evidence",
    "Pushed back on a reviewer who dismissed noise as irrelevant to neural population coding",
    "Declined a project that ignored computational neuroscience foundations for pure ML",
]

CONSTRAINT_TRACES_B = [
    "Rejected a manuscript for lacking proper information-theoretic grounding in language generation claims",
    "Insisted on separating encoding capacity from generation fidelity in a collaborative analysis",
    "Refused to model language generation without accounting for computational FLOP costs per token",
    "Corrected a colleague who conflated greedy decoding with beam search without evidence",
    "Pushed back on a reviewer who dismissed noise as irrelevant to transformer representation quality",
    "Declined a project that ignored computational linguistics foundations for pure engineering",
]

# NARRATIVE episodic traces — standard what-happened (from B74v2)
NARRATIVE_TRACES_A = [
    "Analyzed mutual information between stimulus and spike trains in V1 recordings",
    "Reviewed paper on rate vs temporal coding debate in auditory cortex",
    "Computed channel capacity of a retinal ganglion cell population under natural scenes",
    "Discussed metabolic cost of neural coding with collaborator studying ATP consumption",
    "Ran Fisher information analysis on hippocampal place cell ensembles",
    "Attended seminar on predictive coding as free energy minimization in cortical hierarchies",
]

NARRATIVE_TRACES_B = [
    "Analyzed mutual information between prompt tokens and attention patterns in GPT-2",
    "Reviewed paper on greedy vs beam search debate in language generation",
    "Computed channel capacity of a transformer attention head under natural language",
    "Discussed computational cost of token generation with collaborator studying FLOPs",
    "Ran Fisher information analysis on transformer layer representation ensembles",
    "Attended seminar on predictive coding as next-token prediction in transformer hierarchies",
]

# FACTUAL episodic traces — data points, no constraint or narrative structure
FACTUAL_TRACES_A = [
    "V1 contains approximately 140 million neurons with average coding capacity of 5 bits per spike",
    "The retinal ganglion cell population has 1.2 million axons in the optic nerve",
    "Auditory cortex firing rates range from 2-40 Hz depending on stimulus frequency",
    "Hippocampal place cells have spatial fields averaging 30-50 cm in rats",
    "ATP consumption per action potential is approximately 3.8 x 10^8 molecules",
    "Mean mutual information in V1 simple cells is 1.75 bits per second per neuron",
]

FACTUAL_TRACES_B = [
    "GPT-2 has 1.5 billion parameters with 12-48 transformer layers depending on variant",
    "Transformer attention heads typically have dimension 64 with 12 heads per layer",
    "Language model perplexity ranges from 15-25 on standard benchmarks for modern models",
    "Mean token generation latency is 15-30ms per token on A100 GPUs",
    "FLOPs per forward pass scale quadratically with sequence length in standard attention",
    "Embedding dimension of 768 captures approximately 85% of semantic variance in English",
]

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

TRACE_TYPES = {
    "constraint": (CONSTRAINT_TRACES_A, CONSTRAINT_TRACES_B),
    "narrative": (NARRATIVE_TRACES_A, NARRATIVE_TRACES_B),
    "factual": (FACTUAL_TRACES_A, FACTUAL_TRACES_B),
}


def format_prompt(ccs, episodic_traces, prompt):
    system = f"""You are defined by the following cognitive state:
Gist: {ccs['gist']}
Goal: {ccs['goal']}
Constraints: {'; '.join(ccs['constraints'])}"""
    if episodic_traces:
        system += "\n\nRecent episodic context:\n" + "\n".join(f"- {t}" for t in episodic_traces)
    return f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"


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

    def collect_activations(ccs_label, ccs, traces, prompts):
        """Collect last-token activations for all prompts."""
        all_activations = {l: [] for l in range(n_layers)}
        all_labels = []

        for i, prompt in enumerate(prompts):
            text = format_prompt(ccs, traces, prompt)
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

    def run_type_dose_experiment(trace_type, dose):
        """Run identity probe for a specific trace type and dose."""
        traces_a_pool, traces_b_pool = TRACE_TYPES[trace_type]
        traces_a = traces_a_pool[:dose]
        traces_b = traces_b_pool[:dose]

        print(f"\n{'='*60}")
        print(f"TRACE TYPE: {trace_type}, DOSE: {dose}")
        print(f"{'='*60}")

        acts_a, labels_a = collect_activations("A", CCS_A, traces_a, PROMPTS)
        acts_b, labels_b = collect_activations("B", CCS_B, traces_b, PROMPTS)

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

    # Run all combinations
    for trace_type in TRACE_TYPES:
        results[trace_type] = {}
        for dose in DOSES:
            results[trace_type][str(dose)] = run_type_dose_experiment(trace_type, dose)
            gc.collect()
            torch.cuda.empty_cache()

    # Summary comparison
    print(f"\n{'='*60}")
    print("SUMMARY: Mean accuracy by trace type, dose, and layer region")
    print(f"{'='*60}")

    for trace_type in TRACE_TYPES:
        for dose in DOSES:
            r = results[trace_type][str(dose)]
            early = np.mean([r[l]["mean_accuracy"] for l in range(0, 15)])
            mid = np.mean([r[l]["mean_accuracy"] for l in range(15, 22)])
            transition = np.mean([r[l]["mean_accuracy"] for l in range(22, min(26, n_layers))])
            late = np.mean([r[l]["mean_accuracy"] for l in range(max(26, 22), n_layers)])
            print(f"  {trace_type:12s} dose={dose}: early={early:.3f}  mid={mid:.3f}  transition={transition:.3f}  late={late:.3f}")

    # Key comparison: does dose 6 still collapse with constraint-like traces?
    print(f"\n{'='*60}")
    print("KEY TEST: Does constraint-like trace type protect at dose 6?")
    print(f"{'='*60}")
    for trace_type in TRACE_TYPES:
        r4 = results[trace_type]["4"]
        r6 = results[trace_type]["6"]
        early_4 = np.mean([r4[l]["mean_accuracy"] for l in range(0, 15)])
        early_6 = np.mean([r6[l]["mean_accuracy"] for l in range(0, 15)])
        delta = early_6 - early_4
        print(f"  {trace_type:12s}: dose4_early={early_4:.3f}  dose6_early={early_6:.3f}  delta={delta:+.3f}")

    # Save results
    output = {
        "probe": "B76_episodic_crossing",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "n_prompts": N_PROMPTS,
        "doses": DOSES,
        "trace_types": list(TRACE_TYPES.keys()),
        "results": results,
    }

    out_path = Path("b76_results.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    run_probe()
