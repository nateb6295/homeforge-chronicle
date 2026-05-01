"""
MLP-neuron preferred-frequency check.

Nanda's circuit: each of the 512 MLP hidden neurons reads a linear
combination of tok_emb rows; for grokked models, each neuron's input
weights are Fourier-structured — the neuron "prefers" a specific
frequency.

Test: for each seed, what is the distribution of preferred frequencies
across the 512 neurons? And do different seeds agree on the set of
preferred frequencies, even if individual neurons don't?

This is the lens where Chughtai/Nanda universality should be cleanest.
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


def neuron_preferred_freqs(run, step):
    """For each MLP-hidden neuron, find its preferred tok_emb-row frequency.

    W_in: (512, 128) — each row reads from residual stream.
    We need each neuron's preference over INPUT-TOKEN IDENTITY, which
    means we look at how it responds across tok_emb rows: compute
    W_in @ tok_emb.T → (512, 98), then FFT each row over vocab.
    """
    model = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()      # (98, 128)
    W = model.mlp[0].weight.data.cpu().numpy()       # (512, 128)
    # neuron response across vocab = W @ E^T : (512, 98)
    # restrict to numeric tokens 0..96 (row 97 is the "=" sentinel)
    resp = W @ E[:97].T                              # (512, 97)
    # FFT each neuron's response curve over vocab position
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2    # (512, 49)
    # drop DC (freq 0) since neurons often have a bias-like offset
    preferred = np.argmax(spec[:, 1:], axis=1) + 1   # (512,)
    # also measure concentration: top freq / total power per neuron
    conc = spec.max(axis=1) / spec.sum(axis=1)
    return preferred, conc, spec


def main():
    print(f"MLP-neuron preferred frequencies over vocab at step {STEP}\n")

    agg = {}
    for label, run in RUNS.items():
        pref, conc, spec = neuron_preferred_freqs(run, STEP)
        votes = Counter(pref.tolist())
        top10 = votes.most_common(10)
        # fraction of neurons whose preferred freq is in the top-8-voted set
        top8_freqs = {f for f, _ in votes.most_common(8)}
        coverage = sum(c for f, c in votes.items() if f in top8_freqs) / 512
        agg[label] = dict(pref=pref, votes=votes, top10=top10,
                          coverage=coverage, conc_mean=conc.mean())
        top8_str = ", ".join(f"{f}×{c}" for f, c in top10[:8])
        print(f"{label:<8} mean-conc={conc.mean():.3f}  top8={coverage:.2f}  top: {top8_str}")

    print("\nJaccard overlap of top-8 preferred frequencies (by neuron vote):")
    labels = list(agg.keys())
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            sa = {f for f, _ in agg[a]["top10"][:8]}
            sb = {f for f, _ in agg[b]["top10"][:8]}
            inter = len(sa & sb)
            union = len(sa | sb)
            print(f"  {a} vs {b}: {inter}/{union} = {inter/union:.2f}  [{sorted(sa)} ∩ {sorted(sb)}]")


if __name__ == "__main__":
    main()
