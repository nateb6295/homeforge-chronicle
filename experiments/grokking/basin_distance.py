#!/usr/bin/env python3
"""Is the recovered post-wobble state the same basin or a neighbor?
Compare weight-space distance from 11k to later steps on sub seed 2
(wobble) vs sub seed 1 (stable) as baseline."""
import pathlib
import torch
import torch.nn.functional as F

RUNS = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/runs")

def weights(run, step):
    return torch.load(RUNS / run / "snapshots" / f"step_{step:06d}.pt",
                      map_location="cpu", weights_only=True)

def diff(sd1, sd2):
    # L2 distance and cosine sim across all params, and per-tensor for tok_emb
    total_sq = 0.0
    total_norm1 = 0.0
    total_norm2 = 0.0
    dot = 0.0
    for k in sd1:
        v1 = sd1[k].flatten().float()
        v2 = sd2[k].flatten().float()
        d = v1 - v2
        total_sq += float((d * d).sum())
        total_norm1 += float((v1 * v1).sum())
        total_norm2 += float((v2 * v2).sum())
        dot += float((v1 * v2).sum())
    l2 = total_sq ** 0.5
    cos = dot / (total_norm1 ** 0.5 * total_norm2 ** 0.5)
    # tok_emb specifically
    e1 = sd1["tok_emb.weight"].flatten().float()
    e2 = sd2["tok_emb.weight"].flatten().float()
    emb_cos = float((e1 * e2).sum() / (e1.norm() * e2.norm()))
    return l2, cos, emb_cos

print("run         | step range | full L2 | full cos | tok_emb cos")
for run in ["v2_sub_seed1", "v2_sub_seed2"]:
    w11 = weights(run, 11000)
    for step in [15000, 20000, 26000, 36000, 50000]:
        try:
            wN = weights(run, step)
        except FileNotFoundError:
            continue
        l2, cos, emb = diff(w11, wN)
        print(f"{run:12s} | 11k → {step//1000:>2d}k | {l2:7.2f} | {cos:.4f}  | {emb:.4f}")
