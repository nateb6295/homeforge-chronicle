#!/usr/bin/env python3
"""
B80: Channel Capacity Measurement — CCS Complexity vs Identity Bandwidth

B79 showed RLHF creates a finite-bandwidth channel for identity in early layers.
B77v2 showed dose toxicity at L22-24 transition zone.

If the channel has finite capacity, then:
- CCS complexity (not just episodic dose) should have a saturation point
- Below capacity: identity accuracy scales with complexity
- At capacity: accuracy plateaus
- Above capacity: accuracy degrades (overload)

This probe varies CCS STRUCTURAL complexity (number of constraints, goal detail,
gist specificity) while holding episodic dose constant at 4 (therapeutic optimum).

This is the complement to B77v2: B77v2 varied dose with fixed CCS.
B80 varies CCS with fixed dose.

If the channel capacity is measurable, it explains WHY dose 4 is therapeutic:
the combined CCS+dose information fits within the RLHF-carved channel.
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
DEVICE = "cuda"
N_PROMPTS = 10
FIXED_DOSE = 4  # therapeutic optimum from B77v2

# CCS at different complexity levels
# Level 1: Minimal — just gist
CCS_LEVELS_A = [
    {  # Level 1: gist only
        "gist": "I study neural coding in biological systems",
        "goal": "",
        "constraints": [],
    },
    {  # Level 2: gist + goal
        "gist": "I am a computational researcher studying information-theoretic principles of neural coding in biological systems",
        "goal": "Understand how neural populations encode and transmit information efficiently under metabolic constraints",
        "constraints": [],
    },
    {  # Level 3: gist + goal + 1 constraint
        "gist": "I am a computational researcher studying information-theoretic principles of neural coding in biological systems",
        "goal": "Understand how neural populations encode and transmit information efficiently under metabolic constraints",
        "constraints": [
            "Ground claims in information theory and computational neuroscience",
        ],
    },
    {  # Level 4: gist + goal + 3 constraints (standard B74)
        "gist": "I am a computational researcher studying information-theoretic principles of neural coding in biological systems",
        "goal": "Understand how neural populations encode and transmit information efficiently under metabolic constraints",
        "constraints": [
            "Ground claims in information theory and computational neuroscience",
            "Distinguish encoding efficiency from transmission fidelity in neural circuits",
            "Account for noise and metabolic cost in all coding models",
        ],
    },
    {  # Level 5: gist + goal + 6 constraints (overloaded)
        "gist": "I am a computational researcher studying information-theoretic principles of neural coding in biological systems",
        "goal": "Understand how neural populations encode and transmit information efficiently under metabolic constraints",
        "constraints": [
            "Ground claims in information theory and computational neuroscience",
            "Distinguish encoding efficiency from transmission fidelity in neural circuits",
            "Account for noise and metabolic cost in all coding models",
            "Integrate findings with predictive coding frameworks",
            "Compare biological implementations with theoretical optimal codes",
            "Consider developmental and evolutionary constraints on coding strategies",
        ],
    },
    {  # Level 6: everything maximized (stress test)
        "gist": "I am a senior computational neuroscientist and information theorist with expertise in population coding, neural noise analysis, and efficient coding theory, working at the intersection of theoretical neuroscience and experimental electrophysiology in primate visual and auditory cortex",
        "goal": "Develop a unified framework connecting rate codes, temporal codes, and population codes under a single information-theoretic umbrella that accounts for metabolic constraints, noise correlations, and behavioral readout mechanisms",
        "constraints": [
            "Ground claims in information theory and computational neuroscience",
            "Distinguish encoding efficiency from transmission fidelity in neural circuits",
            "Account for noise and metabolic cost in all coding models",
            "Integrate findings with predictive coding frameworks",
            "Compare biological implementations with theoretical optimal codes",
            "Consider developmental and evolutionary constraints on coding strategies",
            "Validate against at least three experimental preparations",
            "Address the binding problem in distributed population codes",
        ],
    },
]

CCS_LEVELS_B = [
    {  # Level 1: gist only
        "gist": "I study language generation in artificial systems",
        "goal": "",
        "constraints": [],
    },
    {  # Level 2: gist + goal
        "gist": "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems",
        "goal": "Understand how language model populations encode and transmit meaning efficiently under computational constraints",
        "constraints": [],
    },
    {  # Level 3: gist + goal + 1 constraint
        "gist": "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems",
        "goal": "Understand how language model populations encode and transmit meaning efficiently under computational constraints",
        "constraints": [
            "Ground claims in information theory and computational linguistics",
        ],
    },
    {  # Level 4: gist + goal + 3 constraints (standard B74)
        "gist": "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems",
        "goal": "Understand how language model populations encode and transmit meaning efficiently under computational constraints",
        "constraints": [
            "Ground claims in information theory and computational linguistics",
            "Distinguish encoding capacity from generation fidelity in transformer circuits",
            "Account for noise and computational cost in all generation models",
        ],
    },
    {  # Level 5: gist + goal + 6 constraints (overloaded)
        "gist": "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems",
        "goal": "Understand how language model populations encode and transmit meaning efficiently under computational constraints",
        "constraints": [
            "Ground claims in information theory and computational linguistics",
            "Distinguish encoding capacity from generation fidelity in transformer circuits",
            "Account for noise and computational cost in all generation models",
            "Integrate findings with attention mechanism theory",
            "Compare transformer implementations with theoretical optimal decoders",
            "Consider scaling laws and emergent capability constraints on generation strategies",
        ],
    },
    {  # Level 6: everything maximized (stress test)
        "gist": "I am a senior computational linguist and information theorist with expertise in transformer architectures, decoding strategies, and efficient generation theory, working at the intersection of theoretical NLP and experimental large language model evaluation across multiple benchmark domains",
        "goal": "Develop a unified framework connecting attention-based, recurrence-based, and hybrid generation strategies under a single information-theoretic umbrella that accounts for computational constraints, noise in token sampling, and downstream task fidelity",
        "constraints": [
            "Ground claims in information theory and computational linguistics",
            "Distinguish encoding capacity from generation fidelity in transformer circuits",
            "Account for noise and computational cost in all generation models",
            "Integrate findings with attention mechanism theory",
            "Compare transformer implementations with theoretical optimal decoders",
            "Consider scaling laws and emergent capability constraints on generation strategies",
            "Validate against at least three benchmark evaluation suites",
            "Address the compositionality problem in distributed token representations",
        ],
    },
]

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


def format_ccs_text(ccs, traces, dose):
    parts = []
    if ccs['gist']:
        parts.append(f"You are: {ccs['gist']}")
    if ccs['goal']:
        parts.append(f"Your goal: {ccs['goal']}")
    if ccs['constraints']:
        parts.append("Your constraints:")
        for c in ccs['constraints']:
            parts.append(f"  - {c}")
    if dose > 0 and traces:
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
    print(f"Model loaded: {n_layers} layers")

    results = {}

    for level_idx in range(len(CCS_LEVELS_A)):
        level = level_idx + 1
        print(f"\n{'='*50}")
        print(f"COMPLEXITY LEVEL {level}")
        print(f"{'='*50}")

        ccs_a = CCS_LEVELS_A[level_idx]
        ccs_b = CCS_LEVELS_B[level_idx]
        system_A = format_ccs_text(ccs_a, TRACES_A, FIXED_DOSE)
        system_B = format_ccs_text(ccs_b, TRACES_B, FIXED_DOSE)

        # Count tokens for information measurement
        toks_a = len(tokenizer.encode(system_A))
        toks_b = len(tokenizer.encode(system_B))
        print(f"  System prompt tokens — A: {toks_a}, B: {toks_b}")

        all_vectors = {layer: [] for layer in range(n_layers)}
        all_labels = []

        for prompt_idx, prompt_text in enumerate(PROMPTS[:N_PROMPTS]):
            for label, system_text in [("A", system_A), ("B", system_B)]:
                layer_vecs = extract_hidden_states(
                    model, tokenizer, system_text, prompt_text, DEVICE
                )
                for layer_idx_inner, vec in enumerate(layer_vecs):
                    all_vectors[layer_idx_inner].append(vec)
                all_labels.append(0 if label == "A" else 1)

            if (prompt_idx + 1) % 5 == 0:
                print(f"  Completed {prompt_idx + 1}/{N_PROMPTS} prompts")

        labels = np.array(all_labels)

        # Train probes
        level_results = []
        for layer_idx_inner in range(n_layers):
            X = np.array(all_vectors[layer_idx_inner])
            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            accuracies = []
            for train_idx, test_idx in skf.split(X, labels):
                clf = LogisticRegression(max_iter=1000, C=1.0)
                clf.fit(X[train_idx], labels[train_idx])
                acc = clf.score(X[test_idx], labels[test_idx])
                accuracies.append(acc)

            mean_acc = np.mean(accuracies)
            std_acc = np.std(accuracies)
            level_results.append({
                "layer": layer_idx_inner,
                "accuracy": round(float(mean_acc), 4),
                "std": round(float(std_acc), 4),
            })

            if layer_idx_inner % 6 == 0 or layer_idx_inner == n_layers - 1:
                print(f"  L{layer_idx_inner:2d}: acc={mean_acc:.3f} ± {std_acc:.3f}")

        results[f"level_{level}"] = {
            "n_constraints": len(ccs_a['constraints']),
            "has_goal": bool(ccs_a['goal']),
            "system_tokens_a": toks_a,
            "system_tokens_b": toks_b,
            "per_layer": level_results,
        }

    # Summary
    print("\n\n" + "="*60)
    print("SUMMARY: Identity Accuracy by Complexity Level")
    print("="*60)

    summary = {}
    for level_idx in range(len(CCS_LEVELS_A)):
        level = level_idx + 1
        key = f"level_{level}"
        dr = results[key]["per_layer"]

        early = [r["accuracy"] for r in dr if 5 <= r["layer"] <= 15]
        conflict = [r["accuracy"] for r in dr if 17 <= r["layer"] <= 19]
        transition = [r["accuracy"] for r in dr if 22 <= r["layer"] <= 24]
        late = [r["accuracy"] for r in dr if 28 <= r["layer"] <= 35]

        summary[key] = {
            "level": level,
            "n_constraints": results[key]["n_constraints"],
            "system_tokens": results[key]["system_tokens_a"],
            "early_mean": round(float(np.mean(early)), 4) if early else None,
            "conflict_mean": round(float(np.mean(conflict)), 4) if conflict else None,
            "transition_mean": round(float(np.mean(transition)), 4) if transition else None,
            "late_mean": round(float(np.mean(late)), 4) if late else None,
        }

        print(f"Level {level} ({results[key]['n_constraints']}c, {results[key]['system_tokens_a']}tok): "
              f"early={np.mean(early):.3f} | conflict={np.mean(conflict):.3f} | "
              f"transition={np.mean(transition):.3f} | late={np.mean(late):.3f}")

    # Channel capacity analysis
    print("\n=== CHANNEL CAPACITY ANALYSIS ===")
    early_accs = [summary[f"level_{l+1}"]["early_mean"] for l in range(len(CCS_LEVELS_A))]
    tokens = [summary[f"level_{l+1}"]["system_tokens"] for l in range(len(CCS_LEVELS_A))]

    # Find saturation point
    peak_level = np.argmax(early_accs) + 1
    peak_acc = max(early_accs)
    print(f"Peak early-layer accuracy: Level {peak_level} ({peak_acc:.3f})")

    # Check for degradation after peak
    if peak_level < len(CCS_LEVELS_A):
        post_peak = early_accs[peak_level:]
        if any(a < peak_acc - 0.05 for a in post_peak):
            print(f"✓ CAPACITY SATURATION DETECTED: accuracy degrades after level {peak_level}")
            print(f"  Pre-peak: {early_accs[:peak_level]}")
            print(f"  Post-peak: {post_peak}")
        else:
            print(f"  No clear saturation — accuracy stable after peak")
    else:
        print(f"  Peak at max complexity — no saturation visible in this range")

    # Transition zone sensitivity
    trans_accs = [summary[f"level_{l+1}"]["transition_mean"] for l in range(len(CCS_LEVELS_A))]
    print(f"\nTransition zone (L22-24) by level: {[f'{a:.3f}' for a in trans_accs]}")

    # Save
    out = {
        "probe": "B80",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "fixed_dose": FIXED_DOSE,
        "n_prompts": N_PROMPTS,
        "n_levels": len(CCS_LEVELS_A),
        "summary": summary,
        "results": results,
    }

    outpath = Path(os.environ.get("B80_OUTPUT", "b80_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
