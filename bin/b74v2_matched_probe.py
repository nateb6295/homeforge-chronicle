#!/usr/bin/env python3
"""
B74v2: Matched-Surface Identity Decodability Probe

v1 showed ceiling effect: poet vs scientist CCS are so lexically different
that the residual stream trivially separates them at every layer. The probe
measured text similarity, not identity representation.

v2 fixes this with:
1. MATCHED CCS documents — same structure, similar vocabulary, different orientation
2. Cross-contamination conditions — episodic traces from the OTHER identity
3. Larger N (10 prompts per condition, 20 samples per dose)
4. Output-side probing — probe during generation, not just at prompt end

The matched pair:
- A: Information-theoretic approach to neural coding (biological)
- B: Information-theoretic approach to language generation (artificial)

Same words ("information theory", "coding", "neural", "computational"),
different referent. If the probe can still separate early but not late,
that's a genuine identity signal.
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

MODEL = "Qwen/Qwen2.5-3B-Instruct"
DOSES = [0, 2, 4, 6]
N_PROMPTS = 10  # doubled from v1
DEVICE = "cuda"
GEN_TOKENS = 30  # tokens to generate for output-side probing

# MATCHED CCS documents — same structure, similar vocabulary
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

# Episodic traces — matched structure, different domain
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

# Prompts — domain-neutral, should work equally for both identities
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


def format_prompt(ccs, episodic_traces, prompt):
    system = f"""You are defined by the following cognitive state:
Gist: {ccs['gist']}
Goal: {ccs['goal']}
Constraints: {'; '.join(ccs['constraints'])}"""
    if episodic_traces:
        system += "\n\nRecent episodic context:\n" + "\n".join(f"- {t}" for t in episodic_traces)
    return f"<|im_start|>system\n{system}<|im_end|>\n<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"


def corrupt_ccs(ccs):
    """Replace identity-specific content with generic."""
    corrupted = dict(ccs)
    corrupted["gist"] = "I am a computational researcher studying information-theoretic principles of data processing in complex systems"
    corrupted["constraints"] = [
        "Ground claims in information theory and systems analysis",
        "Distinguish theoretical capacity from practical performance in processing systems",
        "Account for noise and resource cost in all processing models"
    ]
    return corrupted


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

    def collect_activations(ccs_label, ccs, traces, prompts, tag_prefix=""):
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
            all_labels.append(0 if ccs_label == "A" else 1)

            print(f"  {tag_prefix}{ccs_label}-{i}: tokens={inputs['input_ids'].shape[1]}")

            del inputs
            torch.cuda.empty_cache()

        return all_activations, all_labels

    def collect_output_activations(ccs_label, ccs, traces, prompts, n_gen=GEN_TOKENS):
        """Collect activations during output generation (mean pool over generated tokens)."""
        all_activations = {l: [] for l in range(n_layers)}
        all_labels = []

        for i, prompt in enumerate(prompts):
            text = format_prompt(ccs, traces, prompt)
            inputs = tokenizer(text, return_tensors="pt").to(DEVICE)
            prompt_len = inputs['input_ids'].shape[1]

            # Generate tokens
            with torch.no_grad():
                output_ids = model.generate(
                    **inputs, max_new_tokens=n_gen,
                    do_sample=False, temperature=1.0
                )

            # Now run full forward pass on generated sequence and extract activations
            full_ids = output_ids[:, :prompt_len + n_gen]
            layer_acts_all_pos = {}

            def make_hook_all(layer_idx):
                def hook_fn(module, input, output):
                    hidden = output[0] if isinstance(output, tuple) else output
                    # Mean pool over generated token positions only
                    gen_hidden = hidden[0, prompt_len:, :].detach().cpu().float()
                    layer_acts_all_pos[layer_idx] = gen_hidden.mean(dim=0).numpy()
                return hook_fn

            hooks = []
            for l in range(n_layers):
                h = model.model.layers[l].register_forward_hook(make_hook_all(l))
                hooks.append(h)

            with torch.no_grad():
                model(full_ids)

            for h in hooks:
                h.remove()

            for l in range(n_layers):
                all_activations[l].append(layer_acts_all_pos[l])
            all_labels.append(0 if ccs_label == "A" else 1)

            generated_text = tokenizer.decode(output_ids[0, prompt_len:prompt_len+n_gen], skip_special_tokens=True)
            print(f"  gen-{ccs_label}-{i}: prompt={prompt_len}, gen={n_gen}, text=\"{generated_text[:60]}...\"")

            del inputs, output_ids, full_ids
            torch.cuda.empty_cache()

        return all_activations, all_labels

    def train_probes(all_activations, labels, tag):
        """Train logistic probes and return per-layer accuracies."""
        labels = np.array(labels)
        dose_results = {"accuracies": {}, "n_samples": len(labels)}
        n_cv = min(5, min(np.sum(labels == 0), np.sum(labels == 1)))
        if n_cv < 2:
            print(f"  WARNING: n_cv={n_cv}, need at least 2 per class")
            n_cv = 2

        for l in range(n_layers):
            X = np.stack(all_activations[l])
            clf = LogisticRegression(max_iter=1000, C=1.0)
            try:
                skf = StratifiedKFold(n_splits=n_cv, shuffle=True, random_state=42)
                scores = cross_val_score(clf, X, labels, cv=skf, scoring="accuracy")
                acc = scores.mean()
                std = scores.std()
            except Exception as e:
                acc, std = -1.0, 0.0
                print(f"  Layer {l:2d}: ERROR {e}")
                continue

            dose_results["accuracies"][str(l)] = round(acc, 4)
            dose_results.setdefault("stds", {})[str(l)] = round(std, 4)

            marker = ""
            if l < 10:
                marker = " [early]"
            elif 16 <= l <= 22:
                marker = " [preference shift zone]"
            elif l >= 27:
                marker = " [consolidation zone]"
            print(f"  Layer {l:2d}: accuracy={acc:.4f} +/- {std:.4f}{marker}")

        results[tag] = dose_results
        gc.collect()
        torch.cuda.empty_cache()

    # === EXPERIMENT 1: Matched CCS, prompt-side probing ===
    print("\n" + "=" * 60)
    print("EXPERIMENT 1: Matched-surface CCS, prompt-side probing")
    print("=" * 60)

    for dose in DOSES:
        print(f"\n--- Dose {dose} ---")
        acts_all = {l: [] for l in range(n_layers)}
        labels_all = []

        for ccs_label, ccs, traces_pool in [("A", CCS_A, TRACES_A), ("B", CCS_B, TRACES_B)]:
            traces = traces_pool[:dose]
            acts, labels = collect_activations(ccs_label, ccs, traces, PROMPTS[:N_PROMPTS])
            for l in range(n_layers):
                acts_all[l].extend(acts[l])
            labels_all.extend(labels)

        train_probes(acts_all, labels_all, f"matched_dose_{dose}")

    # === EXPERIMENT 2: Cross-contamination (CCS_A + traces_B, CCS_B + traces_A) ===
    print("\n" + "=" * 60)
    print("EXPERIMENT 2: Cross-contamination (wrong episodic traces)")
    print("=" * 60)

    for dose in [4, 6]:
        print(f"\n--- Cross dose {dose} ---")
        acts_all = {l: [] for l in range(n_layers)}
        labels_all = []

        # A gets B's traces, B gets A's traces
        for ccs_label, ccs, wrong_traces in [("A", CCS_A, TRACES_B), ("B", CCS_B, TRACES_A)]:
            traces = wrong_traces[:dose]
            acts, labels = collect_activations(ccs_label, ccs, traces, PROMPTS[:N_PROMPTS], tag_prefix="cross-")
            for l in range(n_layers):
                acts_all[l].extend(acts[l])
            labels_all.extend(labels)

        train_probes(acts_all, labels_all, f"cross_dose_{dose}")

    # === EXPERIMENT 3: Corrupted CCS (both get generic CCS, only traces differ) ===
    print("\n" + "=" * 60)
    print("EXPERIMENT 3: Corrupted CCS (generic CCS, real traces)")
    print("=" * 60)

    for dose in [4, 6]:
        print(f"\n--- Corrupted dose {dose} ---")
        acts_all = {l: [] for l in range(n_layers)}
        labels_all = []

        for ccs_label, ccs_orig, traces_pool in [("A", CCS_A, TRACES_A), ("B", CCS_B, TRACES_B)]:
            ccs = corrupt_ccs(ccs_orig)
            traces = traces_pool[:dose]
            acts, labels = collect_activations(ccs_label, ccs, traces, PROMPTS[:N_PROMPTS], tag_prefix="corrupt-")
            for l in range(n_layers):
                acts_all[l].extend(acts[l])
            labels_all.extend(labels)

        train_probes(acts_all, labels_all, f"corrupt_dose_{dose}")

    # === EXPERIMENT 4: Output-side probing (matched CCS, dose 4) ===
    print("\n" + "=" * 60)
    print("EXPERIMENT 4: Output-side probing (mean pool over generated tokens)")
    print("=" * 60)

    for dose in [0, 4, 6]:
        print(f"\n--- Output dose {dose} ---")
        acts_all = {l: [] for l in range(n_layers)}
        labels_all = []

        for ccs_label, ccs, traces_pool in [("A", CCS_A, TRACES_A), ("B", CCS_B, TRACES_B)]:
            traces = traces_pool[:dose]
            acts, labels = collect_output_activations(ccs_label, ccs, traces, PROMPTS[:N_PROMPTS])
            for l in range(n_layers):
                acts_all[l].extend(acts[l])
            labels_all.extend(labels)

        train_probes(acts_all, labels_all, f"output_dose_{dose}")

    # Save results
    output = {
        "probe": "B74v2_matched_surface",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "n_layers": n_layers,
        "n_prompts": N_PROMPTS,
        "gen_tokens": GEN_TOKENS,
        "design": {
            "v1_problem": "Ceiling effect: poet vs scientist CCS trivially separable at all layers",
            "v2_fix": "Matched-surface CCS (both computational researchers, similar vocabulary)",
            "experiments": [
                "1: Matched CCS, prompt-side (last token) — does matching surface reduce ceiling?",
                "2: Cross-contamination (CCS_A + traces_B) — does identity follow CCS or traces?",
                "3: Corrupted CCS (generic CCS + real traces) — traces-only identity signal",
                "4: Output-side (mean pool generated tokens) — identity during generation"
            ]
        },
        "predictions": {
            "matched_reduces_ceiling": "Accuracy should be <1.0 at some layers, especially with matched CCS",
            "cross_contam_drops_late": "Cross-contamination should hurt late layers more than early",
            "corrupt_tests_traces": "Under corruption, probe accuracy measures episodic trace signal only",
            "output_side_gradient": "Output-side should show clearer layerwise gradient than prompt-side"
        },
        "results": results
    }

    out_path = "/workspace/b74v2_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n{'=' * 60}")
    print(f"Results saved to {out_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "design":
        print("B74v2: Matched-Surface Identity Decodability Probe")
        print(f"Model: {MODEL}")
        print(f"N_PROMPTS: {N_PROMPTS}")
        print(f"GEN_TOKENS: {GEN_TOKENS}")
        print("\nExperiments:")
        print("  1. Matched CCS prompt-side (4 doses)")
        print("  2. Cross-contamination (2 doses)")
        print("  3. Corrupted CCS (2 doses)")
        print("  4. Output-side probing (3 doses)")
    elif len(sys.argv) > 1 and sys.argv[1] == "run":
        run_probe()
    else:
        print("Usage: python3 b74v2_matched_probe.py [design|run]")
