"""
Figure: Q/K/V rank decomposition through training.

Three panels (one per config) showing rank_Q, rank_K, rank_V trajectories.
Q crashes to ~1 during memorization. V holds high then drops at grok.
Q's norm grows as its rank shrinks — the smoking gun for "uniform attention
via rank-1 Q with high magnitude."
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
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_qkv_split.png")
STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def eff_rank(W):
    s = np.linalg.svd(W, compute_uv=False)
    if s.sum() == 0: return 0.0
    return float(s.sum() ** 2 / (s ** 2).sum())


def split(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()
    d = W.shape[1]
    Wq, Wk, Wv = W[:d], W[d:2*d], W[2*d:]
    return (eff_rank(Wq), eff_rank(Wk), eff_rank(Wv),
            float(np.linalg.norm(Wq)))


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        for l in f:
            r = json.loads(l)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


configs = [
    ("sub s1", "v2_sub_seed1"),
    ("add s0", "v2"),
    ("mul s0", "v2_mul_seed0"),
]

fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), sharey=False)

for ax, (label, run) in zip(axes, configs):
    rqs, rks, rvs, qnorms = [], [], [], []
    for s in STEPS:
        rq, rk, rv, qn = split(run, s)
        rqs.append(rq); rks.append(rk); rvs.append(rv); qnorms.append(qn)
    gk = grok_step(run)

    ax.plot(STEPS, rqs, "s-", color="#c33", lw=2, ms=5, label="rank Q")
    ax.plot(STEPS, rks, "o-", color="#e87", lw=2, ms=5, label="rank K")
    ax.plot(STEPS, rvs, "^-", color="#36b", lw=2, ms=5, label="rank V")
    if gk:
        ax.axvline(gk, color="black", ls=":", alpha=0.5, lw=1.2,
                   label=f"grok@{gk}")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("step (log)")
    ax.set_ylabel("effective rank (log)")
    ax.set_title(label)
    ax.legend(fontsize=9, loc="lower left")
    ax.grid(alpha=0.3, which="both")

    # secondary axis: Q norm
    ax2 = ax.twinx()
    ax2.plot(STEPS, qnorms, "x--", color="#933", alpha=0.6, lw=1.2, ms=6)
    ax2.set_ylabel("||Q||_F", color="#933", fontsize=9)
    ax2.tick_params(axis="y", labelcolor="#933", labelsize=8)

fig.suptitle("Q crashes to rank 1 (and grows in norm) during memorization;\n"
             "K crashes to rank ~3; only V retains capacity through grok.",
             fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
