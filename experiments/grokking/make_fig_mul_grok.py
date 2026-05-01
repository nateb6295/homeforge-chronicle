"""
Figure: mul's grok mechanism is cross-b spectral lock.

Two panels, side-by-side:
 (a) Sparsity trajectory: top-10 Fourier concentration vs step.
     add/sub rise to 1.00 at grok; mul stays at 0.52 throughout.
 (b) Cross-b spectral consistency vs step for mul s0.
     Locks at 0.97+ exactly at grok step 3300.
     val_acc overlaid for alignment.
"""
import sys, json
from pathlib import Path
import numpy as np
import torch
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer
from sparsity_trajectory import probe as sparsity_probe, primitive_root
from mul_grok_probe import probe as mul_probe, val_acc_at, grok_step

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_mul_grok.png")
STEPS = [500, 1000, 2000, 2500, 3000, 3300, 3500, 4000, 5000, 7000, 11000, 20000, 50000]

configs = [
    ("sub s1", "v2_sub_seed1", False, "#c33"),
    ("add s0", "v2", False, "#36b"),
    ("mul s0", "v2_mul_seed0", True, "#393"),
]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))

# Panel (a): top-10 sparsity through training
for label, run, is_mul, color in configs:
    vals = []
    for s in STEPS:
        try:
            vals.append(sparsity_probe(run, s, is_mul))
        except FileNotFoundError:
            vals.append(np.nan)
    gk = grok_step(run)
    ax1.plot(STEPS, vals, "o-", color=color, lw=2, ms=5, label=f"{label} (grok@{gk})")
    ax1.axvline(gk, color=color, ls=":", alpha=0.4, lw=1)
ax1.set_xscale("log")
ax1.set_xlabel("step (log)")
ax1.set_ylabel("top-10 Fourier concentration")
ax1.set_title("(a) sparsification at grok: add/sub rise to 1.00,\n"
              "mul stays at 0.52 throughout training")
ax1.set_ylim(0.4, 1.05)
ax1.legend(fontsize=9, loc="lower right")
ax1.grid(alpha=0.3, which="both")

# Panel (b): cross-b consistency for mul s0 + val_acc
mul_run = "v2_mul_seed0"
gk_mul = grok_step(mul_run)
xb = []
va = []
for s in STEPS:
    r = mul_probe(mul_run, s, True)
    xb.append(r["cross_b_cos_top10"])
    va.append(val_acc_at(mul_run, s))

ax2.plot(STEPS, xb, "o-", color="#393", lw=2.2, ms=6, label="cross-b spectral cos (top-10)")
ax2.plot(STEPS, va, "s--", color="#933", lw=1.6, ms=5, alpha=0.75, label="val_acc")
ax2.axvline(gk_mul, color="black", ls=":", alpha=0.5, lw=1.2, label=f"grok@{gk_mul}")
ax2.set_xscale("log")
ax2.set_xlabel("step (log)")
ax2.set_ylabel("metric value")
ax2.set_title("(b) mul s0: cross-b spectral lock at grok\n"
              "(neurons commit to b-invariant frequency profiles)")
ax2.set_ylim(0, 1.05)
ax2.legend(fontsize=9, loc="lower right")
ax2.grid(alpha=0.3, which="both")

fig.suptitle("MLP commitment at grok takes different forms for different tasks", fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
