"""
Figure: concentration trajectories in each task's natural group basis.

Shows five configurations rising from ~0.09 to 0.30-0.40 through grok,
with vertical markers at each config's grok step.
"""
import csv, json
import numpy as np
import torch
import sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_concentration_traj.png")
P, G = 97, 5
LOG_RI = np.array([pow(G, k, P) - 1 for k in range(P - 1)])

STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def conc(run, step, re_index=None):
    model = GrokTransformer()
    path = BASE / run / "snapshots" / f"step_{step:06d}.pt"
    if not path.exists():
        return None
    sd = torch.load(path, map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    W = model.mlp[0].weight.data.cpu().numpy()
    resp = W @ E[1:97].T
    if re_index is not None:
        resp = resp[:, re_index]
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    return float((spec.max(axis=1) / spec.sum(axis=1)).mean())


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        rows = [json.loads(l) for l in f]
    for r in rows:
        if r.get("val_acc", 0) >= 0.95:
            return r["step"]
    return None


configs = [
    ("sub s1", "v2_sub_seed1", None, "#2a6"),
    ("add s0", "v2", None, "#e90"),
    ("mul s0", "v2_mul_seed0", LOG_RI, "#36b"),
    ("mul s1", "v2_mul_seed1", LOG_RI, "#58c"),
    ("mul s2", "v2_mul_seed2", LOG_RI, "#7ad"),
]

fig, ax = plt.subplots(figsize=(9, 5))

for label, run, ri, color in configs:
    ys = [conc(run, s, ri) for s in STEPS]
    gs = grok_step(run)
    ax.plot(STEPS, ys, "o-", color=color, lw=1.8, markersize=5,
            label=f"{label}  grok@{gs}")
    if gs:
        ax.axvline(gs, color=color, ls=":", alpha=0.35, lw=1)

ax.set_xscale("log")
ax.set_xlabel("training step (log)")
ax.set_ylabel("mean MLP-neuron top-bin concentration (natural-group basis)")
ax.set_title("Fourier-concentration rises through grok across all tasks\n"
             "(sub/add in additive basis; mul in log-reindexed basis, g=5)")
ax.legend(fontsize=9)
ax.grid(alpha=0.3, which="both")
ax.set_ylim(0, 0.45)

fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
