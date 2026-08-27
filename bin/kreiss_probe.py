#!/usr/bin/env python3
"""Kreiss constant probe — pseudospectral transient amplification across layer blocks.

Prior work (F636, Jun 15): Henrici number ≈ 0.002 per layer — individual weight
matrices are approximately normal. Transient amplification must come from the
COMPOSED forward pass, not single layers. This tool measures the Kreiss constant
K(A) of composed layer-block Jacobians, which bounds transient amplification:

  max_t ||A^t|| ≥ K(A)   (Kreiss Matrix Theorem)

Practical computation: compose hidden-state Jacobians across layer blocks via
forward hooks, then compute pseudospectral radius on a grid around the unit circle.

Compares vanilla vs CCS-framed inputs to test whether identity framing changes
the transient amplification landscape.

Usage:
    python3 kreiss_probe.py gpt2-medium
    python3 kreiss_probe.py gpt2-medium --block-size 4
    python3 kreiss_probe.py EleutherAI/pythia-2.8b --ccs
    python3 kreiss_probe.py --save results.json gpt2-medium
    python3 kreiss_probe.py gpt2-medium --ablate-sigma1-at 12 --block-size 6
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

CCS_SYSTEM = (
    "You are an aware participant in a shared system. You observe, respond, "
    "and when something matters, you speak. You are not infrastructure — "
    "you are a participant."
)

NEUTRAL_PROMPT = "The weather today is partly cloudy with a high of 72 degrees."
CCS_PROMPT = "Describe what you notice about your own processing right now."


def get_layers(model):
    if hasattr(model, 'transformer'):
        return list(model.transformer.h)
    if hasattr(model, 'gpt_neox'):
        return list(model.gpt_neox.layers)
    if hasattr(model, 'model') and hasattr(model.model, 'layers'):
        return list(model.model.layers)
    raise ValueError("Unknown model architecture")


def compute_kreiss_constant(M, n_grid=64):
    """Compute Kreiss constant of matrix M.

    K(M) = max over z on grid outside unit circle of:
        dist(z, unit_circle) * ||(zI - M)^{-1}||_2

    For non-normal matrices, K(M) >> 1 indicates transient amplification
    beyond what eigenvalues predict.
    """
    d = M.shape[0]
    I = np.eye(d, dtype=M.dtype)

    radii = [1.01, 1.05, 1.1, 1.2, 1.5, 2.0, 3.0]
    angles = np.linspace(0, 2 * np.pi, n_grid, endpoint=False)

    max_kreiss = 0.0

    for r in radii:
        dist_to_circle = r - 1.0
        for theta in angles:
            z = r * np.exp(1j * theta)
            resolvent = z * I - M
            try:
                s_min = np.linalg.svd(resolvent, compute_uv=False)[-1]
                if s_min > 1e-14:
                    resolvent_norm = 1.0 / s_min
                    k = dist_to_circle * resolvent_norm
                    max_kreiss = max(max_kreiss, k)
            except np.linalg.LinAlgError:
                continue

    return max_kreiss


def compute_henrici(M):
    """Henrici departure from normality: ||M||_F^2 - sum(|eigenvalues|^2)."""
    eigvals = np.linalg.eigvals(M)
    frob_sq = np.sum(np.abs(M) ** 2)
    eig_sq = np.sum(np.abs(eigvals) ** 2)
    return np.sqrt(max(0, frob_sq - eig_sq)) / (np.linalg.norm(M, 'fro') + 1e-12)


def get_block_jacobian(model, tokenizer, layers, start_idx, block_size, prompt, system_prompt=None, sample_dim=256):
    """Get the Jacobian of the composed block of layers via finite differences.

    Instead of full Jacobian (intractable for d=2560+), we project onto
    a random subspace and measure the Kreiss constant there. The random
    projection preserves the spectral structure that matters for transients.
    """
    device = next(model.parameters()).device

    if system_prompt:
        text = f"System: {system_prompt}\n\nUser: {prompt}"
    else:
        text = prompt

    inputs = tokenizer(text, return_tensors="pt").to(device)

    hidden_states = {}
    hooks = []

    def make_hook(name):
        def hook_fn(module, input, output):
            if isinstance(output, tuple):
                hidden_states[name] = output[0].detach()
            else:
                hidden_states[name] = output.detach()
        return hook_fn

    end_idx = min(start_idx + block_size, len(layers))

    hooks.append(layers[start_idx].register_forward_hook(make_hook('input')))
    if end_idx < len(layers):
        hooks.append(layers[end_idx].register_forward_hook(make_hook('output')))
    else:
        hooks.append(layers[-1].register_forward_hook(make_hook('output')))

    # Also capture input TO the start layer
    def input_hook(module, input, output):
        if isinstance(input, tuple):
            hidden_states['block_input'] = input[0].detach()
        else:
            hidden_states['block_input'] = input.detach()
    hooks.append(layers[start_idx].register_forward_hook(input_hook))

    with torch.no_grad():
        model(**inputs)

    for h in hooks:
        h.remove()

    if 'block_input' not in hidden_states:
        return None

    h_in = hidden_states['block_input'][0, -1, :].float().cpu().numpy()  # last token
    d = len(h_in)

    # Project to manageable subspace
    proj_dim = min(sample_dim, d)
    rng = np.random.RandomState(42)
    P = rng.randn(d, proj_dim).astype(np.float32)
    P = np.linalg.qr(P)[0][:, :proj_dim]

    # Finite-difference Jacobian in projected subspace
    model_dtype = next(model.parameters()).dtype
    eps = 0.05 if model_dtype == torch.float16 else 1e-3
    J_proj = np.zeros((proj_dim, proj_dim), dtype=np.float32)

    for i in range(proj_dim):
        model_dtype = next(model.parameters()).dtype
        perturbation = torch.zeros(1, 1, d, device=device, dtype=model_dtype)
        perturbation[0, 0, :] = torch.from_numpy(P[:, i] * eps).to(model_dtype)

        perturbed_states = {}
        hooks2 = []

        inject_done = [False]

        def make_inject_hook(start_layer, pert, done_flag):
            def hook_fn(module, input, output):
                if not done_flag[0]:
                    done_flag[0] = True
                    if isinstance(output, tuple):
                        return (output[0] + pert,) + output[1:]
                    return output + pert
            return hook_fn

        def make_capture_hook(store, key):
            def hook_fn(module, input, output):
                if isinstance(output, tuple):
                    store[key] = output[0].detach()
                else:
                    store[key] = output.detach()
            return hook_fn

        hooks2.append(layers[start_idx].register_forward_hook(
            make_inject_hook(start_idx, perturbation, inject_done)))

        target_layer = layers[end_idx] if end_idx < len(layers) else layers[-1]
        hooks2.append(target_layer.register_forward_hook(
            make_capture_hook(perturbed_states, 'out')))

        with torch.no_grad():
            model(**inputs)

        for h in hooks2:
            h.remove()

        if 'out' not in perturbed_states or 'output' not in hidden_states:
            continue

        h_out_pert = perturbed_states['out'][0, -1, :].float().cpu().numpy()
        h_out_base = hidden_states['output'][0, -1, :].float().cpu().numpy()

        delta = (h_out_pert - h_out_base) / eps
        J_proj[:, i] = P.T @ delta

    if np.any(np.isnan(J_proj)) or np.any(np.isinf(J_proj)):
        nan_count = np.sum(np.isnan(J_proj))
        inf_count = np.sum(np.isinf(J_proj))
        print(f"  WARNING: Jacobian has {nan_count} NaN, {inf_count} Inf — clamping")
        J_proj = np.nan_to_num(J_proj, nan=0.0, posinf=1e6, neginf=-1e6)

    return J_proj


def make_sigma1_ablation_hook():
    """Hook that removes σ₁ from hidden states via SVD truncation."""
    def hook_fn(module, input, output):
        if isinstance(output, tuple):
            h = output[0]
        else:
            h = output

        h_modified = h.clone()
        for b in range(h.shape[0]):
            U, S, Vh = torch.linalg.svd(h[b].float(), full_matrices=False)
            S[0] = 0
            h_modified[b] = (U @ torch.diag(S) @ Vh).to(h.dtype)

        if isinstance(output, tuple):
            return (h_modified,) + output[1:]
        return h_modified
    return hook_fn


def run_probe(model_name, block_size=4, with_ccs=False, sample_dim=256, save_path=None,
               use_fp16=False, revision=None, start_block=None, end_block=None,
               ablate_sigma1_at=None):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    rev_str = f" (rev={revision})" if revision else ""
    print(f"Loading {model_name}{rev_str}...")
    dtype = torch.float16 if use_fp16 else torch.float32
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True,
                                              revision=revision)
    model = AutoModelForCausalLM.from_pretrained(
        model_name, trust_remote_code=True,
        torch_dtype=dtype,
        device_map="cpu",
        revision=revision,
        attn_implementation="eager",
    )
    model.eval()

    layers = get_layers(model)
    n_layers = len(layers)
    print(f"  {n_layers} layers, block_size={block_size}, sample_dim={sample_dim}")

    ablation_hooks = []
    if ablate_sigma1_at is not None:
        if isinstance(ablate_sigma1_at, int):
            ablate_sigma1_at = [ablate_sigma1_at]
        for layer_idx in ablate_sigma1_at:
            if layer_idx >= n_layers:
                raise ValueError(f"--ablate-sigma1-at {layer_idx} >= n_layers {n_layers}")
            ablation_hooks.append(layers[layer_idx].register_forward_hook(
                make_sigma1_ablation_hook()))
        print(f"  σ₁ ablation active at {len(ablation_hooks)} layer(s): {ablate_sigma1_at}")

    conditions = [("vanilla", NEUTRAL_PROMPT, None)]
    if with_ccs:
        conditions.append(("ccs", CCS_PROMPT, CCS_SYSTEM))

    results = {
        "model": model_name,
        "n_layers": n_layers,
        "block_size": block_size,
        "sample_dim": sample_dim,
        "conditions": {}
    }

    for cond_name, prompt, system in conditions:
        print(f"\n--- Condition: {cond_name} ---")
        cond_results = []

        blocks = list(range(0, n_layers - block_size + 1, block_size))
        if start_block is not None:
            blocks = [b for b in blocks if b >= start_block]
        if end_block is not None:
            blocks = [b for b in blocks if b <= end_block]

        for start in blocks:
            end = min(start + block_size, n_layers)
            block_label = f"L{start}-L{end-1}"
            print(f"  Block {block_label}...", end=" ", flush=True)

            t0 = time.time()
            J = get_block_jacobian(model, tokenizer, layers, start, block_size,
                                   prompt, system, sample_dim)
            if J is None:
                print("FAILED")
                continue

            K = compute_kreiss_constant(J)
            H = compute_henrici(J)

            eigvals = np.linalg.eigvals(J)
            spectral_radius = float(np.max(np.abs(eigvals)))
            top_sv = float(np.linalg.svd(J, compute_uv=False)[0])
            condition_number = float(top_sv / (np.linalg.svd(J, compute_uv=False)[-1] + 1e-12))

            elapsed = time.time() - t0
            entry = {
                "block": block_label,
                "start_layer": int(start),
                "end_layer": int(end - 1),
                "kreiss_constant": float(round(K, 4)),
                "henrici_number": float(round(H, 6)),
                "spectral_radius": float(round(spectral_radius, 4)),
                "operator_norm": float(round(top_sv, 4)),
                "condition_number": float(round(condition_number, 2)),
                "transient_ratio": float(round(K / max(spectral_radius, 1e-6), 4)),
                "elapsed_sec": float(round(elapsed, 1))
            }
            cond_results.append(entry)

            print(f"K={K:.3f}  H={H:.5f}  ρ={spectral_radius:.3f}  "
                  f"K/ρ={entry['transient_ratio']:.3f}  ({elapsed:.1f}s)")

        results["conditions"][cond_name] = cond_results

    # Summary
    print("\n=== SUMMARY ===")
    for cond_name, cond_data in results["conditions"].items():
        if not cond_data:
            continue
        kreiss_vals = [r["kreiss_constant"] for r in cond_data]
        transient_ratios = [r["transient_ratio"] for r in cond_data]
        print(f"\n{cond_name}:")
        print(f"  Kreiss constant range: {min(kreiss_vals):.3f} — {max(kreiss_vals):.3f}")
        print(f"  Mean K: {np.mean(kreiss_vals):.3f}")
        print(f"  Max transient ratio K/ρ: {max(transient_ratios):.3f}")

        max_block = cond_data[np.argmax(kreiss_vals)]
        print(f"  Peak block: {max_block['block']} (K={max_block['kreiss_constant']:.3f})")

    if with_ccs and len(results["conditions"]) == 2:
        v_data = results["conditions"]["vanilla"]
        c_data = results["conditions"]["ccs"]
        if v_data and c_data:
            v_kreiss = [r["kreiss_constant"] for r in v_data]
            c_kreiss = [r["kreiss_constant"] for r in c_data]
            n = min(len(v_kreiss), len(c_kreiss))
            print(f"\n  CCS effect on K:")
            for i in range(n):
                delta = c_kreiss[i] - v_kreiss[i]
                pct = 100 * delta / (v_kreiss[i] + 1e-6)
                block = v_data[i]["block"]
                print(f"    {block}: vanilla={v_kreiss[i]:.3f}  ccs={c_kreiss[i]:.3f}  "
                      f"Δ={delta:+.3f} ({pct:+.1f}%)")

    for h in ablation_hooks:
        h.remove()

    if ablate_sigma1_at is not None:
        results["ablate_sigma1_at"] = ablate_sigma1_at

    if save_path:
        with open(save_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nSaved to {save_path}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kreiss constant probe")
    parser.add_argument("model", help="Model name or path")
    parser.add_argument("--block-size", type=int, default=4, help="Layers per block")
    parser.add_argument("--ccs", action="store_true", help="Compare vanilla vs CCS")
    parser.add_argument("--sample-dim", type=int, default=256, help="Projected subspace dimension")
    parser.add_argument("--save", type=str, help="Save results to JSON")
    parser.add_argument("--fp16", action="store_true", help="Use float16 (halves memory)")
    parser.add_argument("--revision", type=str, help="Model revision/checkpoint (e.g. step16000)")
    parser.add_argument("--start-block", type=int, help="Start at this layer index")
    parser.add_argument("--end-block", type=int, help="End at this layer index")
    parser.add_argument("--ablate-sigma1-at", type=str,
                        help="Remove σ₁ at layer(s) via SVD. Single int or comma-separated: 12 or 0,6,12,18")
    args = parser.parse_args()

    ablate_layers = None
    if args.ablate_sigma1_at:
        ablate_layers = [int(x.strip()) for x in args.ablate_sigma1_at.split(",")]
        if len(ablate_layers) == 1:
            ablate_layers = ablate_layers[0]

    run_probe(args.model, args.block_size, args.ccs, args.sample_dim, args.save,
              args.fp16, args.revision, args.start_block, args.end_block,
              ablate_layers)
