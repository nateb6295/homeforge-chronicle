"""
Figure: basis commitment timing.

Two-panel figure:
  (left) mean-rank trajectory for sub s1, add s0, mul s0 with grok steps marked
  (right) concentration trajectory (same configs) with grok steps marked

Both show the same inflection, aligning with grok.
"""
import json
import sys
from pathlib import Path
import numpy as np
import torch
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_basis_commit.png")
P, G = 97, 5
LOG_RI = np.array([pow(G, k, P) - 1 for k in range(P - 1)])

STEPS = [100, 500, 1000, 2000, 3000, 4000, 5000, 7000, 9000,
         11000, 15000, 20000, 30000, 50000]


def spec(run, step, re_index):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    E = m.tok_emb.weight.data.numpy()
    W = m.mlp[0].weight.data.numpy()
    resp = W @ E[1:97].T
    if re_index is not None:
        resp = resp[:, re_index]
    return np.abs(np.fft.rfft(resp, axis=1))[:, 1:] ** 2


def rank_trajectory(run, ri):
    sf = spec(run, 50000, ri)
    pref = np.argmax(sf, axis=1)
    ranks = []
    for s in STEPS:
        ss = spec(run, s, ri)
        order = np.argsort(-ss, axis=1)
        rs = np.zeros(ss.shape[0], dtype=int)
        for i in range(ss.shape[0]):
            rs[i] = int(np.where(order[i] == pref[i])[0][0])
        ranks.append(rs.mean())
    return ranks


def conc_trajectory(run, ri):
    concs = []
    for s in STEPS:
        ss = spec(run, s, ri)
        concs.append(float((ss.max(axis=1) / ss.sum(axis=1)).mean()))
    return concs


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        for l in f:
            r = json.loads(l)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


configs = [
    ("sub s1", "v2_sub_seed1", None, "#2a6"),
    ("add s0", "v2", None, "#e90"),
    ("mul s0", "v2_mul_seed0", LOG_RI, "#36b"),
]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

for label, run, ri, color in configs:
    gk = grok_step(run)
    ranks = rank_trajectory(run, ri)
    concs = conc_trajectory(run, ri)
    ax1.plot(STEPS, ranks, "o-", color=color, lw=1.8, ms=5, label=f"{label}  grok@{gk}")
    ax2.plot(STEPS, concs, "o-", color=color, lw=1.8, ms=5, label=f"{label}  grok@{gk}")
    if gk:
        ax1.axvline(gk, color=color, ls=":", alpha=0.4, lw=1)
        ax2.axvline(gk, color=color, ls=":", alpha=0.4, lw=1)

ax1.axhline(23.5, color="gray", ls="--", alpha=0.5, lw=0.8, label="random (23.5)")
ax1.set_xscale("log"); ax1.set_xlabel("step (log)")
ax1.set_ylabel("mean rank of final pref in step-s spectrum")
ax1.set_title("Basis commitment (rank collapse)")
ax1.legend(fontsize=8); ax1.grid(alpha=0.3, which="both")

ax2.set_xscale("log"); ax2.set_xlabel("step (log)")
ax2.set_ylabel("mean per-neuron concentration")
ax2.set_title("Fourier concentration (progress measure)")
ax2.legend(fontsize=8); ax2.grid(alpha=0.3, which="both")

fig.suptitle("Concentration rise and basis commitment are the same event (= grok)", fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
