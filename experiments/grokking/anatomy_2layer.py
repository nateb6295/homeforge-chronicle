#!/usr/bin/env python3
"""MLP share + concentration for the 2-layer run, to compare against
1-layer grokked runs. Same probe protocol as anatomy_check.py."""
import sys
from pathlib import Path
import numpy as np
import torch
import torch.nn.functional as F

sys.path.insert(0, str(Path(__file__).parent))
from grok_2layer import TwoLayer

RUN = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_2layer_seed0")
STEP = 50000

m = TwoLayer()
sd = torch.load(RUN / "snapshots" / f"step_{STEP:06d}.pt",
                map_location="cpu", weights_only=True)
m.load_state_dict(sd)
probe = torch.load(RUN / "snapshots" / "probe_inputs.pt",
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
all_abs = []
per_tensor = {}
for name, p in m.named_parameters():
    if p.grad is None:
        continue
    g = p.grad.detach().abs().flatten().numpy()
    all_abs.append(g)
    per_tensor[name] = g.sum()
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
flat = np.concatenate(all_abs)
flat_sorted = np.sort(flat)[::-1]
cum = flat_sorted.cumsum() / flat_sorted.sum()
top01_share = flat_sorted[: max(1, flat.size // 1000)].sum() / flat_sorted.sum()

print(f"2-layer add seed 0 @ step {STEP}")
print(f"  mlp L1 share:         {mlp_l1/total_l1:.3f}")
print(f"  mlp param share:      {mlp_n/total_n:.3f}")
print(f"  mlp mean grad:        {mlp_all.mean():.2e}")
print(f"  non-mlp mean grad:    {non_mlp_all.mean():.2e}")
print(f"  non-mlp / mlp ratio:  {non_mlp_all.mean()/mlp_all.mean():.1f}x")
print(f"  top-0.1% share:       {top01_share:.3f}")
print(f"  max/mean:             {flat.max()/flat.mean():.0f}")

print("\nTop-5 tensors by total L1 share:")
for n, s in sorted(per_tensor.items(), key=lambda kv: -kv[1])[:5]:
    print(f"  {s/total_l1:.3f}  {n}")
