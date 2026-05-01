"""
Figure: two-stage grokking — attention subspace collapse vs MLP rank collapse.

For each of three configs (sub s1, add s0, mul s0) plot effective rank
of attn.in_proj, attn.out_proj, mlp.0 through training with the grok
step marked. Attention collapses early, MLP collapses at grok.
"""
import json, sys
from pathlib import Path
import numpy as np
import torch
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_two_stage.png")
STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def eff_rank(W):
    s = np.linalg.svd(W, compute_uv=False)
    return float(s.sum() ** 2 / (s ** 2).sum())


def traj(run):
    a_in, a_out, m0 = [], [], []
    for s in STEPS:
        m = GrokTransformer()
        sd = torch.load(BASE / run / "snapshots" / f"step_{s:06d}.pt",
                        map_location="cpu", weights_only=True)
        m.load_state_dict(sd)
        a_in.append(eff_rank(m.attn.in_proj_weight.data.numpy()))
        a_out.append(eff_rank(m.attn.out_proj.weight.data.numpy()))
        m0.append(eff_rank(m.mlp[0].weight.data.numpy()))
    return a_in, a_out, m0


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        for l in f:
            r = json.loads(l)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


configs = [
    ("sub s1", "v2_sub_seed1", "#2a6"),
    ("add s0", "v2", "#e90"),
    ("mul s0", "v2_mul_seed0", "#36b"),
]

fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=False)

for ax, (label, run, color) in zip(axes, configs):
    a_in, a_out, m0 = traj(run)
    gk = grok_step(run)
    ax.plot(STEPS, a_in, "o-", color="#c33", lw=1.8, ms=4, label="attn.in_proj")
    ax.plot(STEPS, a_out, "o-", color="#e87", lw=1.8, ms=4, label="attn.out_proj")
    ax.plot(STEPS, m0, "s-", color="#36b", lw=1.8, ms=4, label="mlp.0")
    if gk:
        ax.axvline(gk, color="black", ls=":", alpha=0.5, lw=1.2, label=f"grok@{gk}")
    ax.set_xscale("log")
    ax.set_xlabel("step (log)")
    ax.set_ylabel("effective rank (participation ratio)")
    ax.set_title(label)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, which="both")

fig.suptitle("Two-stage grokking: attn.in_proj collapses in memorization; MLP collapses at grok",
             fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
