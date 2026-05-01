#!/usr/bin/env python3
"""
Super Weight-style knockout: zero out the hero parameter(s) and see
if val_acc collapses.

If concentration is real and load-bearing:
  - Zeroing the top-k highest-sensitivity scalars should tank val_acc
  - Zeroing random scalars should barely move it
  - The asymmetry is the claim.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer, build_dataset, P


RUN_DIR = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
SNAP_DIR = RUN_DIR / "snapshots"


def load_model(step, device):
    m = GrokTransformer().to(device)
    sd = torch.load(SNAP_DIR / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    m.load_state_dict(sd)
    return m


def eval_model(model, data, device, batch=2048):
    a_all, b_all, y_all = data
    correct, n = 0, 0
    model.eval()
    with torch.no_grad():
        for i in range(0, len(a_all), batch):
            a = a_all[i:i+batch].to(device)
            b = b_all[i:i+batch].to(device)
            y = y_all[i:i+batch].to(device)
            logits = model(a, b)
            correct += (logits.argmax(-1) == y).sum().item()
            n += len(a)
    return correct / n


def rank_params_by_grad(model, probe, device):
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    loss = F.cross_entropy(model(a, b), y)
    loss.backward()
    ranked = []
    for name, p in model.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs().flatten().cpu().numpy()
        for i, v in enumerate(g):
            ranked.append((float(v), name, i))
    ranked.sort(reverse=True)
    return ranked


def zero_params(model, targets):
    """targets: list of (tensor_name, flat_idx)"""
    state = {name: p for name, p in model.named_parameters()}
    with torch.no_grad():
        for name, idx in targets:
            t = state[name]
            flat = t.view(-1)
            flat[idx] = 0.0


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")

    _, val = build_dataset(P)
    probe = torch.load(SNAP_DIR / "probe_inputs.pt", map_location=device, weights_only=True)

    step = 50000  # far post-grok, max concentration
    print(f"checkpoint: step {step}\n")

    # Baseline
    m = load_model(step, device)
    base_acc = eval_model(m, val, device)
    print(f"Baseline val_acc: {base_acc:.4f}\n")

    # Rank parameters
    ranked = rank_params_by_grad(m, probe, device)
    print(f"Total params ranked: {len(ranked)}")
    print(f"Top 10 by |grad|:")
    for v, name, idx in ranked[:10]:
        print(f"  {name}[{idx}] |grad|={v:.4e}")
    print()

    # Ablation sweep: zero top-k, random-k; measure val_acc
    ks = [1, 3, 10, 30, 100, 300]
    rng = np.random.default_rng(42)

    print(f"{'k':>5} {'top-k_val_acc':>15} {'random-k_val_acc':>18}")
    for k in ks:
        # Top-k
        m_top = load_model(step, device)
        top_targets = [(name, idx) for _, name, idx in ranked[:k]]
        zero_params(m_top, top_targets)
        top_acc = eval_model(m_top, val, device)

        # Random-k (same total param pool)
        rand_indices = rng.choice(len(ranked), size=k, replace=False)
        rand_targets = [(ranked[i][1], ranked[i][2]) for i in rand_indices]
        m_rand = load_model(step, device)
        zero_params(m_rand, rand_targets)
        rand_acc = eval_model(m_rand, val, device)

        print(f"{k:>5} {top_acc:>15.4f} {rand_acc:>18.4f}")


if __name__ == "__main__":
    main()
