"""
Aggregated Fourier universality check.

Per-row FFT showed no cross-seed agreement. But Chughtai weak universality
is a population-level claim: the *full* embedding matrix's Fourier
structure should share dominant frequencies across seeds.

Two aggregation strategies:
  A) power-sum: sum |FFT(row_i)|^2 over all rows, then rank
  B) row-argmax vote: for each row, its top freq; count frequencies

If weak universality holds at the population level, A and B should agree
on a small set of "key frequencies" that recur across seeds, even when
individual rows don't.
"""
import sys
from pathlib import Path
from collections import Counter
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
BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


def load_emb(run_name, step):
    model = GrokTransformer()
    sd = torch.load(BASE / run_name / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    return model.tok_emb.weight.data.cpu().numpy()


def main():
    print(f"Population-level FFT at step {STEP}\n")

    agg = {}
    for label, run_name in RUNS.items():
        E = load_emb(run_name, STEP)  # (vocab, d_model)
        spec = np.abs(np.fft.rfft(E, axis=1)) ** 2  # (vocab, freq)

        power_sum = spec.sum(axis=0)
        # drop DC for argmax-voting (freq 0 dominates trivially in some seeds)
        row_top = np.argmax(spec[:, 1:], axis=1) + 1
        vote = Counter(row_top.tolist())

        top_by_power = np.argsort(-power_sum)[:8].tolist()
        top_by_vote = [f for f, _ in vote.most_common(8)]

        agg[label] = dict(power=top_by_power, vote=top_by_vote,
                          power_arr=power_sum)
        print(f"{label:<8} power-sum top-8: {top_by_power}")
        print(f"{'':<8} row-vote top-8: {top_by_vote}")
        print()

    print("Jaccard overlap of top-8 power-sum frequencies:")
    labels = list(agg.keys())
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            sa, sb = set(agg[a]["power"]), set(agg[b]["power"])
            print(f"  {a} vs {b}: {len(sa & sb)}/{len(sa | sb)} = {len(sa & sb)/len(sa | sb):.2f}")

    print("\nJaccard overlap of top-8 row-vote frequencies:")
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            sa, sb = set(agg[a]["vote"]), set(agg[b]["vote"])
            print(f"  {a} vs {b}: {len(sa & sb)}/{len(sa | sb)} = {len(sa & sb)/len(sa | sb):.2f}")

    # cosine similarity of full power spectra — most sensitive test
    print("\nCosine similarity of full power spectra (pairwise):")
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            pa, pb = agg[a]["power_arr"], agg[b]["power_arr"]
            cos = float(pa @ pb / (np.linalg.norm(pa) * np.linalg.norm(pb)))
            print(f"  {a} vs {b}: {cos:.3f}")


if __name__ == "__main__":
    main()
