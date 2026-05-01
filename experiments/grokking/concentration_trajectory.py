#!/usr/bin/env python3
"""Concentration trajectory: top-0.1% and MLP L1 share vs step,
alongside train/val accuracy, for add seed 0."""
import json, pathlib, sys
import numpy as np
import torch
import torch.nn.functional as F
import matplotlib.pyplot as plt

sys.path.insert(0, "/home/nate-agx/chronicle/experiments/grokking")
from grok_v2 import GrokTransformer

RUN = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
snaps = sorted((RUN / "snapshots").glob("step_*.pt"))
# coarse sample: every 500 steps → 100 points
snaps = snaps[::5]

probe = torch.load(RUN / "snapshots" / "probe_inputs.pt",
                   map_location="cpu", weights_only=True)

steps, top01, mlp_share = [], [], []
m = GrokTransformer()
for snap in snaps:
    step = int(snap.stem.split("_")[1])
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
    steps.append(step); top01.append(float(t01))
    mlp_share.append(float(mlp_l1 / tot_l1))

# load val/train curves
curve_steps, train, val = [], [], []
for line in open(RUN / "metrics.jsonl"):
    r = json.loads(line)
    curve_steps.append(r["step"]); train.append(r["train_acc"]); val.append(r["val_acc"])

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.5, 6), sharex=True)
ax1.plot(curve_steps, train, color="#1f77b4", linewidth=1.0, label="train")
ax1.plot(curve_steps, val, color="#d62728", linewidth=1.3, label="val")
ax1.set_ylabel("accuracy"); ax1.set_ylim(0, 1.05)
ax1.legend(loc="center right"); ax1.grid(alpha=0.3)
ax1.set_title("add seed 0 (p=97): accuracy and gradient concentration vs step")

ax2.plot(steps, top01, color="#2ca02c", linewidth=1.3, label="top-0.1% share")
ax2.plot(steps, mlp_share, color="#ff7f0e", linewidth=1.3, label="MLP L1 share")
ax2.axhline(0.001, color="grey", linewidth=0.5, linestyle=":", label="uniform top-0.1%")
ax2.set_xlabel("step"); ax2.set_ylabel("share of total |∇| L1")
ax2.legend(loc="center right"); ax2.grid(alpha=0.3)
plt.tight_layout()

out = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig6_concentration_trajectory.png")
plt.savefig(out, dpi=140)
print("wrote", out)
print(f"at step 100:   top01={top01[0]:.3f}  mlp_share={mlp_share[0]:.3f}")
print(f"at step ~2000: top01={top01[3]:.3f}  mlp_share={mlp_share[3]:.3f}")
print(f"at step ~5000: top01={top01[9]:.3f}  mlp_share={mlp_share[9]:.3f}")
print(f"at step 50000: top01={top01[-1]:.3f}  mlp_share={mlp_share[-1]:.3f}")
