#!/usr/bin/env python3
"""F660b: Null distribution for rotation test — addresses Kimi corrections.

Runs N rotations (default 50) on vanilla ONLY to establish the random-alignment
null distribution. Tests whether vanilla baseline (K=8.7) sits significantly
below the null (suppressive) or within it (no organization).

Also checks logit quality under rotation (Kimi's computational confound):
if rotation destroys output quality, K collapse could be generic disruption.
"""
import argparse
import json
import os
import sys
import time

import numpy as np
import torch

os.environ["PYTHONUNBUFFERED"] = "1"
os.environ.setdefault("OMP_NUM_THREADS", "16")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

sys.path.insert(0, os.path.dirname(__file__))
from kreiss_probe import (
    get_layers, get_block_jacobian, compute_kreiss_constant, compute_henrici,
    CCS_SYSTEM, CCS_PROMPT, NEUTRAL_PROMPT,
)


def random_orthogonal_full(dim, seed=None):
    rng = np.random.RandomState(seed)
    A = rng.randn(dim, dim).astype(np.float32)
    Q, _ = np.linalg.qr(A)
    return Q


def measure_logit_quality(model, tokenizer, layers, prompt, system_prompt,
                          rotation_layer_idx=None, Q_matrix=None):
    """Measure top-5 logit entropy and argmax token with/without rotation."""
    if system_prompt:
        messages = [{"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}]
        text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    else:
        text = prompt

    inputs = tokenizer(text, return_tensors="pt")
    input_ids = inputs["input_ids"]

    hook = None
    Q_tensor_holder = [None]
    if rotation_layer_idx is not None and Q_matrix is not None:
        def hook_fn(module, input, output):
            if Q_tensor_holder[0] is None:
                Q_tensor_holder[0] = torch.from_numpy(Q_matrix.T).to(
                    output[0].device if isinstance(output, tuple) else output.device,
                    output[0].dtype if isinstance(output, tuple) else output.dtype)
            if isinstance(output, tuple):
                return (output[0] @ Q_tensor_holder[0],) + output[1:]
            return output @ Q_tensor_holder[0]
        hook = layers[rotation_layer_idx].register_forward_hook(hook_fn)

    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits[0, -1]

    if hook:
        hook.remove()

    probs = torch.softmax(logits, dim=0)
    top5 = torch.topk(probs, 5)
    entropy = -torch.sum(probs * torch.log(probs + 1e-10)).item()
    top_token = tokenizer.decode(top5.indices[0].item())
    top_prob = top5.values[0].item()

    return {"entropy": round(entropy, 4), "top_token": top_token,
            "top_prob": round(top_prob, 6), "top5_probs": [round(p.item(), 6) for p in top5.values]}


def measure_with_rotation(model, tokenizer, layers, block_start, block_size,
                           prompt, system_prompt, sample_dim,
                           rotation_layer_idx, Q_matrix):
    Q_tensor = None

    def make_rotation_hook(Q_np):
        def hook_fn(module, input, output):
            nonlocal Q_tensor
            if Q_tensor is None:
                Q_tensor = torch.from_numpy(Q_np.T).to(output[0].device if isinstance(output, tuple) else output.device,
                                                        output[0].dtype if isinstance(output, tuple) else output.dtype)
            if isinstance(output, tuple):
                return (output[0] @ Q_tensor,) + output[1:]
            return output @ Q_tensor
        return hook_fn

    hook = layers[rotation_layer_idx].register_forward_hook(make_rotation_hook(Q_matrix))
    try:
        J = get_block_jacobian(model, tokenizer, layers, block_start, block_size,
                                prompt, system_prompt, sample_dim)
    finally:
        hook.remove()
        Q_tensor = None

    return J


def main():
    parser = argparse.ArgumentParser(description="F660b: Rotation null distribution")
    parser.add_argument("model", nargs="?", default="Qwen/Qwen2.5-1.5B")
    parser.add_argument("--block-start", type=int, default=14)
    parser.add_argument("--block-size", type=int, default=7)
    parser.add_argument("--rotation-layer", type=int, default=17)
    parser.add_argument("--sample-dim", type=int, default=32)
    parser.add_argument("--n-rotations", type=int, default=50)
    parser.add_argument("--condition", type=str, default="vanilla",
                        choices=["vanilla", "ccs", "both"])
    parser.add_argument("--save", type=str, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--logit-check", action="store_true",
                        help="Check logit quality under rotation")
    args = parser.parse_args()

    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    from transformers import AutoTokenizer, AutoModelForCausalLM

    print(f"Loading {args.model}...")
    tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        args.model, trust_remote_code=True,
        torch_dtype=torch.float32,
        device_map="cpu",
        attn_implementation="eager",
    )
    model.eval()

    layers = get_layers(model)
    d = model.config.hidden_size

    conditions = []
    if args.condition in ("vanilla", "both"):
        conditions.append(("vanilla", NEUTRAL_PROMPT, None))
    if args.condition in ("ccs", "both"):
        conditions.append(("ccs", CCS_PROMPT, CCS_SYSTEM))

    print(f"\n{'='*60}")
    print(f"  NULL DISTRIBUTION (n={args.n_rotations})")
    print(f"  Block: L{args.block_start}-L{args.block_start+args.block_size-1}")
    print(f"  Rotation at: L{args.rotation_layer}")
    print(f"  Hidden dim: {d}")
    print(f"  Conditions: {[c[0] for c in conditions]}")
    print(f"{'='*60}")

    results = {"model": args.model, "block_start": args.block_start,
               "block_size": args.block_size, "rotation_layer": args.rotation_layer,
               "hidden_dim": d, "n_rotations": args.n_rotations,
               "conditions": {}}

    for cond_name, prompt, system in conditions:
        print(f"\n--- {cond_name} (n={args.n_rotations}) ---")

        print(f"  Baseline...", end=" ", flush=True)
        t0 = time.time()
        J_baseline = get_block_jacobian(model, tokenizer, layers, args.block_start,
                                         args.block_size, prompt, system, args.sample_dim)
        K_baseline = compute_kreiss_constant(J_baseline)
        H_baseline = compute_henrici(J_baseline)
        print(f"K={K_baseline:.3f}, H={H_baseline:.5f} ({time.time()-t0:.1f}s)")

        if args.logit_check:
            lq_baseline = measure_logit_quality(model, tokenizer, layers, prompt, system)
            print(f"  Logit baseline: entropy={lq_baseline['entropy']}, top='{lq_baseline['top_token']}' ({lq_baseline['top_prob']:.4f})")

        scrambled_Ks = []
        scrambled_Hs = []
        logit_qualities = []

        for i in range(args.n_rotations):
            if (i+1) % 10 == 0 or i < 5:
                print(f"  Rotation {i+1}/{args.n_rotations}...", end=" ", flush=True)
            t0 = time.time()
            Q = random_orthogonal_full(d, seed=100 + i)
            J_s = measure_with_rotation(
                model, tokenizer, layers, args.block_start, args.block_size,
                prompt, system, args.sample_dim, args.rotation_layer, Q)
            K_s = compute_kreiss_constant(J_s)
            H_s = compute_henrici(J_s)
            scrambled_Ks.append(K_s)
            scrambled_Hs.append(H_s)

            if args.logit_check and i < 5:
                lq = measure_logit_quality(model, tokenizer, layers, prompt, system,
                                            args.rotation_layer, Q)
                logit_qualities.append(lq)

            if (i+1) % 10 == 0 or i < 5:
                print(f"K={K_s:.3f} ({time.time()-t0:.1f}s)")

        mean_K = np.mean(scrambled_Ks)
        std_K = np.std(scrambled_Ks)
        median_K = np.median(scrambled_Ks)
        percentiles = np.percentile(scrambled_Ks, [5, 25, 50, 75, 95])

        below_baseline = sum(1 for k in scrambled_Ks if k < K_baseline)
        pval = below_baseline / args.n_rotations

        print(f"\n  RESULTS for {cond_name}:")
        print(f"    Baseline K: {K_baseline:.3f}")
        print(f"    Scrambled: mean={mean_K:.3f} ± {std_K:.3f}, median={median_K:.3f}")
        print(f"    Percentiles: 5%={percentiles[0]:.1f}, 25%={percentiles[1]:.1f}, "
              f"50%={percentiles[2]:.1f}, 75%={percentiles[3]:.1f}, 95%={percentiles[4]:.1f}")
        print(f"    Baseline < scrambled in {args.n_rotations - below_baseline}/{args.n_rotations} cases")
        print(f"    p(K_scrambled < K_baseline) = {pval:.3f}")

        if pval > 0.95:
            print(f"    => SUPPRESSIVE (baseline significantly below random)")
        elif pval < 0.05:
            print(f"    => CONSTRUCTIVE (baseline significantly above random)")
        else:
            print(f"    => NOT SIGNIFICANT (baseline within random distribution)")

        cond_result = {
            "K_baseline": round(K_baseline, 4),
            "H_baseline": round(H_baseline, 6),
            "K_scrambled_mean": round(mean_K, 4),
            "K_scrambled_std": round(std_K, 4),
            "K_scrambled_median": round(median_K, 4),
            "K_scrambled_all": [round(k, 4) for k in scrambled_Ks],
            "percentiles": {str(p): round(v, 4) for p, v in zip([5, 25, 50, 75, 95], percentiles)},
            "p_below_baseline": round(pval, 4),
            "n_below_baseline": below_baseline,
        }

        if args.logit_check:
            cond_result["logit_baseline"] = lq_baseline
            cond_result["logit_rotated"] = logit_qualities

        results["conditions"][cond_name] = cond_result

    if args.save:
        with open(args.save, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nSaved to {args.save}")


if __name__ == "__main__":
    main()
