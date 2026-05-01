#!/usr/bin/env python3
"""
Cross-seed channel concentration: do all three seeds show the same
STRUCTURAL concentration signature, but at DIFFERENT channel indices?

If yes: process-invariant (concentration emerges) but content-invariant
(which channels) varies by seed — the Gorard distinction in one dataset.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

D_MODEL = 128
BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
SEEDS = [("v2", "seed=0"), ("v2_seed1", "seed=1"), ("v2_seed2", "seed=2")]
STEP = 50000


def channel_energy(snap_dir, step, device):
    model = GrokTransformer().to(device)
    sd = torch.load(snap_dir / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    probe = torch.load(snap_dir / "probe_inputs.pt", map_location=device, weights_only=True)
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


def top_frac(x, frac):
    x = np.sort(x)[::-1]
    k = max(1, int(len(x) * frac))
    return float(x[:k].sum() / x.sum())


def gini(x):
    x = np.sort(np.asarray(x, dtype=np.float64))
    n = len(x)
    if n == 0 or x.sum() == 0:
        return 0.0
    cum = np.cumsum(x)
    return (2 * np.sum((np.arange(1, n + 1)) * x) / (n * cum[-1])) - (n + 1) / n


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}\n")
    print(f"{'seed':12} {'gini':>6} {'top-8ch%':>9} {'top-16ch%':>10} top-8 channels")
    energies = {}
    tops = {}
    for tag, label in SEEDS:
        snap_dir = BASE / tag / "snapshots"
        ce = channel_energy(snap_dir, STEP, device)
        ranked = np.argsort(-ce)
        top8 = ranked[:8].tolist()
        energies[tag] = ce
        tops[tag] = top8
        g = gini(ce)
        t8 = 100 * ce[ranked[:8]].sum() / ce.sum()
        t16 = 100 * ce[ranked[:16]].sum() / ce.sum()
        print(f"{label:12} {g:>6.3f} {t8:>8.1f}% {t16:>9.1f}% {top8}")

    # Overlap between top-8 sets across seeds
    print("\nChannel-index overlap (top-8 across seeds):")
    sets = {tag: set(tops[tag]) for tag, _ in SEEDS}
    for i, (a, la) in enumerate(SEEDS):
        for b, lb in SEEDS[i+1:]:
            overlap = sets[a] & sets[b]
            print(f"  {la} ∩ {lb}: {len(overlap)}/8 shared — {sorted(overlap)}")

    # Rank correlation of full channel-energy vectors
    from itertools import combinations
    print("\nFull-channel Spearman rank correlation:")
    for (a, la), (b, lb) in combinations(SEEDS, 2):
        ra = np.argsort(np.argsort(-energies[a]))
        rb = np.argsort(np.argsort(-energies[b]))
        # Spearman via Pearson on ranks
        corr = np.corrcoef(ra, rb)[0, 1]
        print(f"  {la} vs {lb}: ρ = {corr:+.3f}")


if __name__ == "__main__":
    main()
