"""
Fourier-per-seed universality check.

At step 50000 (post-grok, final state), compute FFT of tok_emb row 97
across all 6 networks in our dense-dynamics sweep. Does the set of
top-k Fourier frequencies agree across seeds within a task? Across tasks?

Question this is trying to answer: Chughtai weak universality at the
representation level predicts same Fourier frequencies across seeds.
Earlier in this paper we show the functional organization (which
tensors are load-bearing under ablation) is seed-dependent. Does the
frequency set track the kinematic similarity or the functional
heterogeneity?
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

RUNS = {
    "sub s1": "v2_sub_seed1",
    "sub s2": "v2_sub_seed2",
    "add s0": "v2",
    "mul s0": "v2_mul_seed0",
    "mul s1": "v2_mul_seed1",
    "mul s2": "v2_mul_seed2",
}
STEP = 50000
ROW = 97
BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


def load_row(run_name, step, row):
    model = GrokTransformer()
    sd = torch.load(BASE / run_name / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    return model.tok_emb.weight.data[row].cpu().numpy()


def gini(arr):
    arr = np.sort(np.asarray(arr, dtype=np.float64))
    n = len(arr)
    if arr.sum() == 0:
        return 0.0
    idx = np.arange(1, n + 1)
    return float((2 * (idx * arr).sum() - (n + 1) * arr.sum()) / (n * arr.sum()))


def main():
    print(f"FFT of tok_emb[row {ROW}] at step {STEP}\n")
    print(f"{'network':<12} {'L2':<7} {'gini':<6} {'top1':<6} {'top3':<6} {'top8':<6} top_freqs")
    print("-" * 72)

    results = {}
    for label, run_name in RUNS.items():
        v = load_row(run_name, STEP, ROW)
        spec = np.abs(np.fft.rfft(v))
        spec_sq = spec ** 2
        total = spec_sq.sum()
        order = np.argsort(-spec_sq)
        top1 = spec_sq[order[0]] / total
        top3 = spec_sq[order[:3]].sum() / total
        top8 = spec_sq[order[:8]].sum() / total
        g = gini(spec_sq)
        results[label] = dict(spec=spec, top_freqs=order[:8].tolist(),
                              top1=top1, top3=top3, top8=top8, gini=g)
        print(f"{label:<12} {np.linalg.norm(v):<7.3f} {g:<6.3f} "
              f"{top1:<6.3f} {top3:<6.3f} {top8:<6.3f} {order[:5].tolist()}")

    print("\nTop-8 frequencies by network:")
    for label, r in results.items():
        print(f"  {label}: {r['top_freqs']}")

    print("\nAgreement analysis:")
    print("  Jaccard overlap of top-8 frequency sets (pairwise):")
    labels = list(results.keys())
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            sa = set(results[a]["top_freqs"])
            sb = set(results[b]["top_freqs"])
            inter = len(sa & sb)
            union = len(sa | sb)
            print(f"    {a} vs {b}: {inter}/{union} = {inter/union:.2f}")


if __name__ == "__main__":
    main()
