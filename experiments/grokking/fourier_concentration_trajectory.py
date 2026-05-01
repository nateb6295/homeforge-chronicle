"""
When does the MLP-neuron Fourier concentration emerge during training?

At step 50k we saw sub/add at mean-conc 0.36 and mul at 0.085. Is
the concentration a pre-grok property, a grok-crossing property, or
a slow post-grok emergence? If it crosses with val_acc, it's a
progress measure.

Samples: sparse steps through training, tracks mean-concentration
and val-loss/val-acc from training log if available.
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

RUNS = {
    "sub s1": "v2_sub_seed1",
    "add s0": "v2",
    "mul s0": "v2_mul_seed0",
}
# coarse sweep — every 2000 steps
STEPS = [1000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 40000, 50000]
BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


def mean_conc(run, step):
    model = GrokTransformer()
    path = BASE / run / "snapshots" / f"step_{step:06d}.pt"
    if not path.exists():
        return None
    sd = torch.load(path, map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    W = model.mlp[0].weight.data.cpu().numpy()
    resp = W @ E[:97].T
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    conc = spec.max(axis=1) / spec.sum(axis=1)
    return float(conc.mean()), float(conc.std())


def main():
    print(f"{'step':<8} " + " ".join(f"{l:<14}" for l in RUNS))
    print("-" * 60)
    for step in STEPS:
        row = [f"{step:<8}"]
        for label, run in RUNS.items():
            r = mean_conc(run, step)
            row.append(f"{r[0]:.3f}±{r[1]:.3f}" if r else "n/a           ")
        print(" ".join(row))


if __name__ == "__main__":
    main()
