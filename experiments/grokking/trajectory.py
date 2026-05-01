#!/usr/bin/env python3
"""
Concentration trajectory through full training.

Measure per-channel gradient energy at every checkpoint. Track:
  - Top-8-channel fraction of total (concentration over time)
  - Jaccard similarity of top-8 channel set between adjacent checkpoints
    (channel stability)
  - Which specific channels are in the top-8 at key phases

If concentration emerges early and the top-channel SET stays stable,
grokking tightens an existing basin. If the top-channel set changes
mid-training, the model moves to a different basin.
"""
import sys
import re
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

D_MODEL = 128
RUN_DIR = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
SNAP_DIR = RUN_DIR / "snapshots"


def channel_energy(step, probe, device):
    model = GrokTransformer().to(device)
    sd = torch.load(SNAP_DIR / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    F.cross_entropy(model(a, b), y).backward()
    ce = np.zeros(D_MODEL)
    for name, p in model.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs()
        if g.dim() == 1 and g.shape[0] == D_MODEL:
            ce += g.cpu().numpy()
        elif g.dim() >= 2 and g.shape[-1] == D_MODEL:
            ce += g.view(-1, D_MODEL).sum(dim=0).cpu().numpy()
    return ce


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")
    probe = torch.load(SNAP_DIR / "probe_inputs.pt", map_location=device, weights_only=True)

    # Enumerate step checkpoints
    step_re = re.compile(r"step_(\d+)\.pt")
    steps = sorted(int(step_re.match(p.name).group(1))
                   for p in SNAP_DIR.glob("step_*.pt"))
    # Sample coarsely to make it tractable: every 500 steps plus all of 0-5000
    sampled = [s for s in steps if s <= 5000 or s % 500 == 0]
    print(f"sampling {len(sampled)} checkpoints")

    import json
    metrics = {}
    with open(RUN_DIR / "metrics.jsonl") as f:
        for line in f:
            r = json.loads(line)
            metrics[r["step"]] = r

    results = []
    prev_top = None
    for s in sampled:
        ce = channel_energy(s, probe, device)
        ranked = np.argsort(-ce)
        top8 = set(ranked[:8].tolist())
        top8_frac = float(ce[ranked[:8]].sum() / ce.sum())
        top16_frac = float(ce[ranked[:16]].sum() / ce.sum())
        jac = len(top8 & prev_top) / 8 if prev_top is not None else 1.0
        val_acc = metrics.get(s, {}).get("val_acc", float('nan'))
        train_acc = metrics.get(s, {}).get("train_acc", float('nan'))
        results.append({
            "step": s, "val_acc": val_acc, "train_acc": train_acc,
            "top8_frac": top8_frac, "top16_frac": top16_frac,
            "jaccard_vs_prev": jac, "top8": sorted(top8),
        })
        prev_top = top8

    # Print summary table
    print(f"\n{'step':>6} {'train':>6} {'val':>6} {'top8%':>7} {'top16%':>8} {'jaccard':>9}")
    for r in results:
        print(f"{r['step']:>6} {r['train_acc']:>6.3f} {r['val_acc']:>6.3f} "
              f"{100*r['top8_frac']:>6.1f}% {100*r['top16_frac']:>7.1f}% "
              f"{r['jaccard_vs_prev']:>9.2f}")

    # Save full data for later
    import json
    with open(RUN_DIR / "trajectory.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {RUN_DIR / 'trajectory.json'}")


if __name__ == "__main__":
    main()
