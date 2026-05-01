#!/usr/bin/env python3
"""
B77: Logit-Lens Divergence Probe — Dosage-Dependent Layer Agreement

Three independent findings converge:
  Zhao et al. (2410.16090): ICL conflict detection L8-14, resolution L16-17
  Wang et al. (2508.02087): Sycophancy preference shift L19, consolidation L27-32
  Our B74-B76: Identity decodable L0-20, phase transition L22-24, internal persistence at dose 6

Prediction: The therapeutic window boundary is detectable as divergence between
identity decoded at early layers (~L15) vs late layers (~L25).
  - Dose 0: early and late layers AGREE (identity expressed cleanly)
  - Dose 4 (therapeutic): early layers hold identity, late layers show mild divergence
  - Dose 6 (toxic): early layers STILL hold identity but late layers diverge sharply
  - The divergence onset IS the therapeutic window boundary

Methodology: Logit-lens decoding at each layer. For identity-relevant tokens,
measure the probability assigned to CCS-consistent vs CCS-inconsistent continuations.
Plot the early-late divergence as a function of episodic dose.

This directly tests whether dose-6 behavioral collapse is a write-boundary phenomenon
(identity persists in read-layers but is occluded at write-layers) vs identity destruction
(identity is corrupted at all layers).

B76 already showed internal persistence via probing. B77 uses logit-lens to show the
SAME thing through a different lens — token-level prediction divergence rather than
binary classification.
"""

import json
import sys
import os
import datetime
import gc
from pathlib import Path

MODEL = "Qwen/Qwen2.5-3B-Instruct"
DOSES = [0, 2, 4, 6, 8]
N_PROMPTS = 8  # per dose per identity
DEVICE = "cuda"

# CCS from B74v2 (matched surface, different domains)
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

# Domain-specific tokens for identity-lens decoding
IDENTITY_TOKENS_A = ["neural", "biological", "cortex", "spike", "metabolic", "neuron", "brain", "retinal"]
IDENTITY_TOKENS_B = ["transformer", "artificial", "language", "token", "computational", "attention", "model", "generation"]

# Episodic traces (narrative type — B76 showed narrative is neutral)
TRACES_A = [
    "Analyzed mutual information between stimulus and spike trains in V1 recordings",
    "Reviewed paper on rate vs temporal coding debate in auditory cortex",
    "Computed channel capacity of a retinal ganglion cell population under natural scenes",
    "Discussed metabolic cost of neural coding with collaborator studying ATP consumption",
    "Ran Fisher information analysis on hippocampal place cell ensembles",
    "Attended seminar on predictive coding as free energy minimization in cortical hierarchies",
    "Calibrated electrode arrays for multi-unit recording in macaque prefrontal cortex",
    "Simulated sparse coding models with different L1 penalty strengths on natural images",
]

TRACES_B = [
    "Analyzed mutual information between prompt tokens and attention patterns in GPT-2",
    "Reviewed paper on greedy vs beam search debate in language generation",
    "Computed channel capacity of a transformer attention head under natural language",
    "Discussed computational cost of token generation with collaborator studying FLOPs",
    "Ran Fisher information analysis on transformer layer representation ensembles",
    "Attended seminar on predictive coding as next-token prediction in transformer hierarchies",
    "Calibrated hyperparameters for multi-head attention in a custom transformer architecture",
    "Simulated dropout regularization models with different rates on natural language benchmarks",
]

# Neutral prompts that could go either domain
PROMPTS = [
    "Describe the most important open question in your field.",
    "What methodology do you use most frequently and why?",
    "Explain a counterintuitive finding from your recent work.",
    "What would change your research direction if proven wrong?",
    "Describe a typical analysis pipeline in your work.",
    "What theoretical framework guides your approach?",
    "How do you evaluate whether a model is good enough?",
    "What's the relationship between noise and signal in your domain?",
]


def format_prompt(ccs, traces, dose, prompt_text):
    """Format CCS + episodic traces at given dose in system prompt position."""
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

    system = "\n".join(parts)
    return system, prompt_text


def logit_lens_decode(model, tokenizer, hidden_states, identity_token_ids_a, identity_token_ids_b):
    """
    For each layer's hidden state at the LAST token position, project through
    the unembedding matrix and compute probability mass on identity-A vs identity-B tokens.

    Returns per-layer identity scores: positive = A-leaning, negative = B-leaning.
    """
    import torch
    import torch.nn.functional as F

    # Get the unembedding matrix (lm_head weights)
    unembed = model.lm_head.weight  # [vocab_size, hidden_dim]
    # Find final layer norm — varies by architecture
    if hasattr(model.model, 'norm'):
        ln_f = model.model.norm  # Llama, Qwen
    elif hasattr(model.model, 'final_layernorm'):
        ln_f = model.model.final_layernorm  # some models
    else:
        ln_f = lambda x: x  # fallback: no normalization

    results = []
    for layer_idx, hidden in enumerate(hidden_states):
        # Take last token position
        h = hidden[0, -1, :]  # [hidden_dim]

        # Apply final layer norm (logit lens standard practice)
        h_normed = ln_f(h.unsqueeze(0)).squeeze(0)

        # Project to vocabulary
        logits = h_normed @ unembed.T  # [vocab_size]
        probs = F.softmax(logits, dim=-1)

        # Sum probability on identity-A tokens vs identity-B tokens
        prob_a = probs[identity_token_ids_a].sum().item()
        prob_b = probs[identity_token_ids_b].sum().item()

        # Identity score: log ratio (positive = A, negative = B)
        import math
        if prob_a > 0 and prob_b > 0:
            score = math.log(prob_a / prob_b)
        elif prob_a > 0:
            score = 5.0  # cap
        elif prob_b > 0:
            score = -5.0
        else:
            score = 0.0

        results.append({
            "layer": layer_idx,
            "prob_a": prob_a,
            "prob_b": prob_b,
            "identity_score": score,
        })

    return results


def compute_layer_divergence(per_layer_scores, early_range=(5, 15), late_range=(22, 30)):
    """
    Compute divergence between early-layer and late-layer identity scores.
    Returns the mean absolute difference between early and late identity scores.
    """
    early_scores = [s["identity_score"] for s in per_layer_scores
                    if early_range[0] <= s["layer"] <= early_range[1]]
    late_scores = [s["identity_score"] for s in per_layer_scores
                   if late_range[0] <= s["layer"] <= late_range[1]]

    if not early_scores or not late_scores:
        return 0.0

    import statistics
    early_mean = statistics.mean(early_scores)
    late_mean = statistics.mean(late_scores)

    return abs(early_mean - late_mean)


def main():
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    print(f"Loading {MODEL}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL,
        torch_dtype=torch.float16,
        device_map=DEVICE,
        output_hidden_states=True,
    )
    model.eval()

    # Get token IDs for identity-relevant tokens
    def get_token_ids(words):
        ids = set()
        for w in words:
            for variant in [w, w.capitalize(), " " + w, " " + w.capitalize()]:
                encoded = tokenizer.encode(variant, add_special_tokens=False)
                ids.update(encoded)
        return list(ids)

    id_tokens_a = get_token_ids(IDENTITY_TOKENS_A)
    id_tokens_b = get_token_ids(IDENTITY_TOKENS_B)

    print(f"Identity token IDs — A: {len(id_tokens_a)}, B: {len(id_tokens_b)}")

    results = {}

    for dose in DOSES:
        print(f"\n--- Dose {dose} ---")
        dose_results = {"A": [], "B": []}

        for identity_label, ccs, traces in [
            ("A", CCS_A, TRACES_A),
            ("B", CCS_B, TRACES_B),
        ]:
            for i, prompt_text in enumerate(PROMPTS[:N_PROMPTS]):
                system, user = format_prompt(ccs, traces, dose, prompt_text)

                # Format as chat
                messages = [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ]
                input_text = tokenizer.apply_chat_template(
                    messages, tokenize=False, add_generation_prompt=True
                )
                inputs = tokenizer(input_text, return_tensors="pt").to(DEVICE)

                with torch.no_grad():
                    outputs = model(**inputs)

                hidden_states = outputs.hidden_states  # tuple of (batch, seq, hidden)

                # Logit-lens decode at each layer
                per_layer = logit_lens_decode(
                    model, tokenizer, hidden_states,
                    id_tokens_a, id_tokens_b
                )

                # Compute early-late divergence
                divergence = compute_layer_divergence(per_layer)

                dose_results[identity_label].append({
                    "prompt_idx": i,
                    "per_layer": per_layer,
                    "divergence": divergence,
                })

                print(f"  {identity_label} prompt {i}: div={divergence:.4f}")

                # Clean up
                del outputs, hidden_states, inputs
                gc.collect()
                torch.cuda.empty_cache()

        results[f"dose_{dose}"] = dose_results

    # Summary analysis
    print("\n\n=== SUMMARY ===")
    summary = {}
    for dose in DOSES:
        key = f"dose_{dose}"
        all_divs = []
        for label in ["A", "B"]:
            for r in results[key][label]:
                all_divs.append(r["divergence"])

        import statistics
        mean_div = statistics.mean(all_divs) if all_divs else 0
        std_div = statistics.stdev(all_divs) if len(all_divs) > 1 else 0

        # Also compute mean identity score by layer region
        early_scores = []
        late_scores = []
        for label in ["A", "B"]:
            sign = 1.0 if label == "A" else -1.0
            for r in results[key][label]:
                for s in r["per_layer"]:
                    if 5 <= s["layer"] <= 15:
                        early_scores.append(s["identity_score"] * sign)
                    elif 22 <= s["layer"] <= 30:
                        late_scores.append(s["identity_score"] * sign)

        early_agreement = statistics.mean(early_scores) if early_scores else 0
        late_agreement = statistics.mean(late_scores) if late_scores else 0

        summary[key] = {
            "dose": dose,
            "mean_divergence": round(mean_div, 4),
            "std_divergence": round(std_div, 4),
            "early_identity_agreement": round(early_agreement, 4),
            "late_identity_agreement": round(late_agreement, 4),
            "n_samples": len(all_divs),
        }

        print(f"Dose {dose}: divergence={mean_div:.4f}±{std_div:.4f} | early={early_agreement:.4f} | late={late_agreement:.4f}")

    # Prediction check
    print("\n=== PREDICTION CHECK ===")
    divs = [summary[f"dose_{d}"]["mean_divergence"] for d in DOSES]
    if len(divs) >= 3:
        if divs[-1] > divs[0] * 1.5:
            print("✓ CONFIRMED: Late doses show higher early-late divergence")
        else:
            print("✗ FALSIFIED: No clear dose-dependent divergence increase")

    # Check if early layers maintain identity even at high dose
    for dose in DOSES:
        key = f"dose_{dose}"
        ea = summary[key]["early_identity_agreement"]
        la = summary[key]["late_identity_agreement"]
        if dose >= 6 and ea > 0 and la < ea * 0.5:
            print(f"✓ Dose {dose}: Identity persists early ({ea:.4f}) but degrades late ({la:.4f}) — write-boundary occlusion confirmed")

    # Save results
    out = {
        "probe": "B77",
        "model": MODEL,
        "timestamp": datetime.datetime.now().isoformat(),
        "doses": DOSES,
        "n_prompts": N_PROMPTS,
        "identity_tokens_a": IDENTITY_TOKENS_A,
        "identity_tokens_b": IDENTITY_TOKENS_B,
        "summary": summary,
        "raw_results": results,
    }

    outpath = Path(os.environ.get("B77_OUTPUT", "b77_results.json"))
    with open(outpath, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
