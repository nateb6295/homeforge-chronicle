#!/usr/bin/env python3
"""
Concentration-of-function-sensitivity measure.

Test: at the memorization checkpoint (train=1.0, val=~random), is per-parameter
sensitivity concentrated or flat? At the post-grok checkpoint (train=1.0, val=1.0)?

If the claim holds:
  memorization → flat sensitivity distribution (high entropy, low gini)
  generalization → concentrated distribution (low entropy, high gini)

Measures per-parameter |gradient| on a fixed probe batch and reports:
  - Gini coefficient of |grad| distribution
  - Fraction of L1 energy in top 1% / top 0.1% of parameters
  - Max/mean ratio
  - Entropy
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
CHECKPOINTS = [
    (200, "early / untrained-ish"),
    (800, "memorization onset (train=0.994, val=0.057)"),
    (1500, "pure memorization (train=1.000, val=0.105)"),
    (3000, "transition (train=1.000, val=0.205)"),
    (5000, "just-grokked (train=1.000, val=1.000)"),
    (10000, "post-grok stable (train=1.000, val=1.000)"),
    (50000, "far post-grok (train=1.000, val=1.000)"),
]


def gini(x):
    x = np.sort(np.asarray(x, dtype=np.float64))
    n = len(x)
    if n == 0 or x.sum() == 0:
        return 0.0
    cum = np.cumsum(x)
    return (2 * np.sum((np.arange(1, n + 1)) * x) / (n * cum[-1])) - (n + 1) / n


def entropy(p):
    p = p / p.sum()
    p = p[p > 0]
    return float(-(p * np.log(p)).sum())


def top_frac(x, frac):
    x = np.sort(x)[::-1]
    k = max(1, int(len(x) * frac))
    return float(x[:k].sum() / x.sum())


def analyze_checkpoint(step, device):
    model = GrokTransformer().to(device)
    sd = torch.load(SNAP_DIR / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()

    # Use the fixed probe batch (the val batch used during training)
    probe_data = torch.load(SNAP_DIR / "probe_inputs.pt", map_location=device, weights_only=True)
    a = probe_data["a"].to(device)
    b = probe_data["b"].to(device)
    y = probe_data["y"].to(device)

    # Compute per-parameter gradient magnitude on this batch
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    logits = model(a, b)
    loss = F.cross_entropy(logits, y)
    loss.backward()

    grads = []
    for name, p in model.named_parameters():
        if p.grad is not None:
            grads.append(p.grad.detach().abs().flatten().cpu().numpy())
    all_grads = np.concatenate(grads)

    # Also compute per-parameter L1 of the weights themselves, for comparison
    weights = []
    for name, p in model.named_parameters():
        weights.append(p.detach().abs().flatten().cpu().numpy())
    all_weights = np.concatenate(weights)

    return {
        "step": step,
        "n_params": len(all_grads),
        "loss": float(loss.item()),
        "grad_gini": gini(all_grads),
        "grad_entropy": entropy(all_grads),
        "grad_top1pct": top_frac(all_grads, 0.01),
        "grad_top01pct": top_frac(all_grads, 0.001),
        "grad_max": float(all_grads.max()),
        "grad_mean": float(all_grads.mean()),
        "grad_max_over_mean": float(all_grads.max() / all_grads.mean()),
        "weight_gini": gini(all_weights),
        "weight_entropy": entropy(all_weights),
    }


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")
    print(f"{'step':>7} {'label':50} {'loss':>8} {'g_gini':>7} {'g_top1%':>8} {'g_top0.1%':>10} {'g_max/μ':>9} {'w_gini':>7}")
    results = []
    for step, label in CHECKPOINTS:
        ckpt = SNAP_DIR / f"step_{step:06d}.pt"
        if not ckpt.exists():
            print(f"  [missing] step_{step:06d}.pt")
            continue
        r = analyze_checkpoint(step, device)
        results.append((label, r))
        print(f"{r['step']:>7} {label:50} {r['loss']:>8.4f} {r['grad_gini']:>7.3f} "
              f"{r['grad_top1pct']:>8.3f} {r['grad_top01pct']:>10.4f} "
              f"{r['grad_max_over_mean']:>9.1f} {r['weight_gini']:>7.3f}")

    # Summary: memorization (1500) vs post-grok (10000)
    by_step = {r[1]["step"]: r[1] for r in results}
    if 1500 in by_step and 10000 in by_step:
        m, g = by_step[1500], by_step[10000]
        print()
        print("Memorization (step 1500) vs Post-grok (step 10000):")
        print(f"  grad gini:       {m['grad_gini']:.3f} → {g['grad_gini']:.3f} "
              f"(Δ={g['grad_gini']-m['grad_gini']:+.3f})")
        print(f"  grad top-1%:     {m['grad_top1pct']:.3f} → {g['grad_top1pct']:.3f} "
              f"(Δ={g['grad_top1pct']-m['grad_top1pct']:+.3f})")
        print(f"  grad top-0.1%:   {m['grad_top01pct']:.4f} → {g['grad_top01pct']:.4f} "
              f"(Δ={g['grad_top01pct']-m['grad_top01pct']:+.4f})")
        print(f"  grad max/mean:   {m['grad_max_over_mean']:.1f} → {g['grad_max_over_mean']:.1f}")
        if g['grad_gini'] > m['grad_gini']:
            print(f"  → CONCENTRATION INCREASES from memorization to generalization")
        else:
            print(f"  → CONCENTRATION DOES NOT INCREASE (claim fails)")


if __name__ == "__main__":
    main()
