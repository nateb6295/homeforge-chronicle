#!/usr/bin/env python3
"""FFT of tok_emb row 97 across all 3 tasks at post-grok checkpoint.

Hypothesis from the walkback: mul has distributed causal locus because
its cyclic group structure yields a cleaner Fourier basis. If true,
mul's row-97 embedding should look more Fourier-sparse than add/sub's.
(Or possibly the opposite — we'll see.)

Report:
- Top-k frequency magnitudes per task
- Fraction of energy in top-1, top-3, top-8 frequencies
- Plot (optional) saved as fourier_row97.png
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

TASKS = {
    "add": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2"),
    "sub": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed0"),
    "mul": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0"),
}
STEP = 50000
ROW = 97


def load_row(run_dir, step, row):
    snap = run_dir / "snapshots"
    model = GrokTransformer()
    sd = torch.load(snap / f"step_{step:06d}.pt", map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    return model.tok_emb.weight.data[row].cpu().numpy()


def main():
    print(f"FFT of tok_emb[row {ROW}] at step {STEP}\n")
    results = {}
    for tag, run_dir in TASKS.items():
        v = load_row(run_dir, STEP, ROW)
        spec = np.abs(np.fft.rfft(v))
        spec_sq = spec ** 2
        total = spec_sq.sum()
        order = np.argsort(-spec_sq)
        top1 = spec_sq[order[0]] / total
        top3 = spec_sq[order[:3]].sum() / total
        top8 = spec_sq[order[:8]].sum() / total
        gini = compute_gini(spec_sq)
        results[tag] = dict(spec=spec, top1=top1, top3=top3, top8=top8, gini=gini,
                            top_freqs=order[:8].tolist())
        print(f"{tag}: L2={np.linalg.norm(v):.3f}  gini(|FFT|^2)={gini:.3f}  "
              f"top1={top1:.3f}  top3={top3:.3f}  top8={top8:.3f}  "
              f"top_freqs={order[:5].tolist()}")

    # Save numpy for downstream plotting
    np.savez("/home/nate-agx/chronicle/experiments/grokking/fourier_row97.npz",
             **{k: v["spec"] for k, v in results.items()})
    print(f"\nSaved spectra to fourier_row97.npz")


def compute_gini(arr):
    arr = np.sort(np.asarray(arr, dtype=np.float64))
    n = len(arr)
    if arr.sum() == 0:
        return 0.0
    idx = np.arange(1, n + 1)
    return float((2 * (idx * arr).sum() - (n + 1) * arr.sum()) / (n * arr.sum()))


if __name__ == "__main__":
    main()
