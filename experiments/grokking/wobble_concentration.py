#!/usr/bin/env python3
"""Does the sub-seed-2 wobble show up in concentration?
Check at grok / trough / recovery steps."""
import sys, pathlib
import numpy as np
import torch
import torch.nn.functional as F

sys.path.insert(0, "/home/nate-agx/chronicle/experiments/grokking")
from grok_v2 import GrokTransformer

RUN = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed2")
probe = torch.load(RUN / "snapshots" / "probe_inputs.pt",
                   map_location="cpu", weights_only=True)

steps = [11000, 15000, 20000, 26000, 27000, 28000, 32000, 36000, 40000, 50000]
m = GrokTransformer()
print(f"{'step':>6} {'top-0.1%':>10} {'mlp share':>10} {'max/mean':>10}")
for step in steps:
    snap = RUN / "snapshots" / f"step_{step:06d}.pt"
    if not snap.exists():
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
    print(f"{step:>6} {t01:>10.3f} {mlp_l1/tot_l1:>10.3f} {flat.max()/flat.mean():>10.0f}")
