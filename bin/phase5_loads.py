"""LOADS: Label set Optimization via Activation Distribution kurtosiS

Implementation of Ahmed et al. (2024) for Gemma 4 26B routing.

Measures FFN activation kurtosis in the final decoder layer for different
candidate label tokens. Lower kurtosis = fewer outlier neurons = less
pre-training prior interference = better classification.

Uses our actual routing observations as unlabeled samples (no labels needed,
just forward passes with each candidate label to measure activation patterns).

Run on H100:
  python3 phase5_loads.py
"""
import json
import torch
import numpy as np
from pathlib import Path
from scipy import stats

MODEL_NAME = "google/gemma-4-26B-A4B-it"
WORKSPACE = "/workspace/phase5_semantic"
TRACE_FILE = f"{WORKSPACE}/phase5_routing_traces.jsonl"

# Candidate label pairs to test: (noise_label, signal_label)
CANDIDATE_PAIRS = [
    ("1", "2"),
    ("noise", "signal"),
    ("foo", "bar"),
    ("A", "B"),
    ("low", "high"),
    ("skip", "keep"),
    ("pass", "flag"),
    ("0", "1"),
    ("no", "yes"),
    ("drop", "take"),
    ("x", "y"),
    ("cold", "hot"),
    ("mute", "alert"),
    ("gray", "red"),
    ("null", "true"),
]

N_SAMPLES = 100


def load_observations():
    """Load observations from traces (we only need source+content, not labels)."""
    records = []
    for line in open(TRACE_FILE):
        try:
            r = json.loads(line)
            records.append({"source": r["source"], "content": r["content"]})
        except:
            pass
    np.random.seed(42)
    indices = np.random.choice(len(records), min(N_SAMPLES, len(records)), replace=False)
    return [records[i] for i in indices]


def measure_kurtosis_for_pair(model, tokenizer, observations, noise_label, signal_label):
    """Measure FFN activation kurtosis for a label pair.

    For each observation, we do two forward passes:
    - One with noise_label as the next token
    - One with signal_label as the next token
    Then measure kurtosis of activations in the last FFN layer.
    """
    kurtosis_values = []

    hook_activations = {}

    def get_last_ffn_hook(name):
        def hook_fn(module, input, output):
            hook_activations[name] = output.detach()
        return hook_fn

    # Gemma 4 26B is MoE: model.model.language_model.layers[-1].mlp
    last_layer = model.model.language_model.layers[-1]
    mlp = last_layer.mlp

    # Hook into the down_proj (final projection of FFN)
    handle = mlp.down_proj.register_forward_hook(get_last_ffn_hook("last_ffn"))

    try:
        for obs in observations:
            user_msg = f"Source: {obs['source']}\nObservation: {obs['content']}"

            for label in [noise_label, signal_label]:
                prompt = (
                    f"<start_of_turn>user\n{user_msg}<end_of_turn>\n"
                    f"<start_of_turn>model\n{label}"
                )

                ids = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512).to(model.device)

                with torch.no_grad():
                    model(**ids)

                if "last_ffn" in hook_activations:
                    # Get activations for the last token position
                    act = hook_activations["last_ffn"][0, -1, :].cpu().float().numpy()
                    k = stats.kurtosis(act, fisher=True)
                    kurtosis_values.append(k)
                    hook_activations.clear()
    finally:
        handle.remove()

    mean_kurtosis = np.mean(kurtosis_values) if kurtosis_values else float('inf')
    std_kurtosis = np.std(kurtosis_values) if kurtosis_values else 0

    return mean_kurtosis, std_kurtosis, len(kurtosis_values)


def main():
    from transformers import AutoModelForCausalLM, AutoTokenizer

    print(f"Loading {MODEL_NAME}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        attn_implementation="sdpa",
    )
    model.eval()

    print(f"Loading {N_SAMPLES} observations...", flush=True)
    observations = load_observations()
    print(f"  Got {len(observations)} observations", flush=True)

    # Verify candidate labels are single tokens
    print("\nToken verification:", flush=True)
    for noise_l, signal_l in CANDIDATE_PAIRS:
        n_ids = tokenizer.encode(noise_l, add_special_tokens=False)
        s_ids = tokenizer.encode(signal_l, add_special_tokens=False)
        n_tok = len(n_ids)
        s_tok = len(s_ids)
        marker = " *" if n_tok > 1 or s_tok > 1 else ""
        print(f"  {noise_l}/{signal_l}: {n_tok}/{s_tok} tokens{marker}", flush=True)

    print(f"\n{'='*60}", flush=True)
    print(f"LOADS: Measuring FFN kurtosis for {len(CANDIDATE_PAIRS)} label pairs", flush=True)
    print(f"{'='*60}\n", flush=True)

    results = []

    for i, (noise_l, signal_l) in enumerate(CANDIDATE_PAIRS):
        print(f"[{i+1}/{len(CANDIDATE_PAIRS)}] Testing {noise_l}/{signal_l}...", end=" ", flush=True)

        mean_k, std_k, n = measure_kurtosis_for_pair(
            model, tokenizer, observations, noise_l, signal_l
        )

        results.append({
            "noise_label": noise_l,
            "signal_label": signal_l,
            "mean_kurtosis": float(mean_k),
            "std_kurtosis": float(std_k),
            "n_measurements": n,
        })

        print(f"kurtosis={mean_k:.2f} ± {std_k:.2f} (n={n})", flush=True)

    # Sort by kurtosis (lower is better)
    results.sort(key=lambda x: x["mean_kurtosis"])

    print(f"\n{'='*60}", flush=True)
    print("RANKING (lower kurtosis = better for classification)", flush=True)
    print(f"{'='*60}", flush=True)

    for i, r in enumerate(results):
        marker = " ← BEST" if i == 0 else (" ← CURRENT" if r["noise_label"] == "1" and r["signal_label"] == "2" else "")
        print(f"  {i+1}. {r['noise_label']:6s}/{r['signal_label']:6s}  kurtosis={r['mean_kurtosis']:8.2f}{marker}")

    # Save results
    results_path = f"{WORKSPACE}/loads_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results saved to {results_path}", flush=True)

    # Summary
    best = results[0]
    current = next((r for r in results if r["noise_label"] == "1" and r["signal_label"] == "2"), None)
    if current:
        improvement = current["mean_kurtosis"] - best["mean_kurtosis"]
        print(f"\nBest pair: {best['noise_label']}/{best['signal_label']} (kurtosis={best['mean_kurtosis']:.2f})")
        print(f"Current (1/2): kurtosis={current['mean_kurtosis']:.2f}")
        print(f"Potential improvement: {improvement:.2f} kurtosis points")


if __name__ == "__main__":
    main()
