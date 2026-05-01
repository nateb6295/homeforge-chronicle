#!/usr/bin/env python3
"""Verify the FFN claim: is MLP really absent from gradient energy,
or only absent from top-0.1% concentration?"""
import sys
from pathlib import Path
import numpy as np
import torch
import torch.nn.functional as F

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

RUNS = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
STEP = 50000

TASKS = {
    "add s0": RUNS / "v2",
    "sub s0": RUNS / "v2_sub_seed0",
    "mul s0": RUNS / "v2_mul_seed0",
    "mul s1": RUNS / "v2_mul_seed1",
    "mul s2": RUNS / "v2_mul_seed2",
}


def analyze(path):
    m = GrokTransformer()
    sd = torch.load(path / "snapshots" / f"step_{STEP:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    probe = torch.load(path / "snapshots" / "probe_inputs.pt",
                       map_location="cpu", weights_only=True)
    for p in m.parameters():
        p.requires_grad_(True)
    m.zero_grad()
    loss = F.cross_entropy(m(probe["a"], probe["b"]), probe["y"])
    loss.backward()

    mlp_l1 = 0.0
    mlp_n = 0
    total_l1 = 0.0
    total_n = 0
    mlp_grads = []
    non_mlp_grads = []
    for name, p in m.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs().flatten().numpy()
        total_l1 += g.sum()
        total_n += g.size
        if "mlp" in name:
            mlp_l1 += g.sum()
            mlp_n += g.size
            mlp_grads.append(g)
        else:
            non_mlp_grads.append(g)

    mlp_all = np.concatenate(mlp_grads)
    non_mlp_all = np.concatenate(non_mlp_grads)

    return {
        "mlp_share_l1":  mlp_l1 / total_l1,
        "mlp_share_params": mlp_n / total_n,
        "mlp_mean_grad":  float(mlp_all.mean()),
        "nonmlp_mean_grad": float(non_mlp_all.mean()),
        "mlp_max":  float(mlp_all.max()),
        "nonmlp_max": float(non_mlp_all.max()),
    }


print(f"{'run':10s} {'mlp L1 share':>13s} {'mlp params share':>17s} "
      f"{'mlp mean':>10s} {'non-mlp mean':>13s} {'ratio':>8s}")
for label, path in TASKS.items():
    r = analyze(path)
    ratio = r["nonmlp_mean_grad"] / r["mlp_mean_grad"]
    print(f"{label:10s} {r['mlp_share_l1']:>13.3f} {r['mlp_share_params']:>17.3f} "
          f"{r['mlp_mean_grad']:>10.2e} {r['nonmlp_mean_grad']:>13.2e} {ratio:>7.1f}x")
