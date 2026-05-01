#!/usr/bin/env python3
"""Check grok-transition-in-concentration on mul seed 0."""
import sys, pathlib
import numpy as np
import torch
import torch.nn.functional as F

sys.path.insert(0, "/home/nate-agx/chronicle/experiments/grokking")
from grok_v2 import GrokTransformer

RUN = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0")
probe = torch.load(RUN / "snapshots" / "probe_inputs.pt",
                   map_location="cpu", weights_only=True)
m = GrokTransformer()
print(f"{'step':>6} {'top-0.1%':>10} {'mlp share':>10}")
for step in [100, 1000, 2000, 3000, 4000, 5000, 10000, 50000]:
    snap = RUN / "snapshots" / f"step_{step:06d}.pt"
    if not snap.exists():
        print(f"{step:>6} (missing)")
        continue
    sd = torch.load(snap, map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    for p in m.parameters():
        p.requires_grad_(True)
    m.zero_grad()
    loss = F.cross_entropy(m(probe["a"], probe["b"]), probe["y"])
    loss.backward()
    all_g, mlp_l1, tot_l1 = [], 0.0, 0.0
    for name, p in m.named_parameters():
        if p.grad is None: continue
        g = p.grad.detach().abs().flatten().numpy()
        all_g.append(g)
        tot_l1 += g.sum()
        if "mlp" in name: mlp_l1 += g.sum()
    flat = np.concatenate(all_g)
    fs = np.sort(flat)[::-1]
    t01 = fs[: max(1, flat.size // 1000)].sum() / fs.sum()
    print(f"{step:>6} {t01:>10.3f} {mlp_l1/tot_l1:>10.3f}")
