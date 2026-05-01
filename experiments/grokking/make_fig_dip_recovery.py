"""
Figure: the dip-and-recovery mechanism of grokking.

Top panel: cross-b spectral consistency trajectory for add, sub, mul
  seeds, plus a horizontal band for the random-init trivial baseline.
  All three traces show the same dip-and-recovery pattern timed to
  their grok step.

Bottom panel: val_acc for each config — shows the accuracy jump
  aligns with the recovery leg of the dip-and-recovery.
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
from mul_grok_probe import probe, grok_step, val_acc_at

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_dip_recovery.png")
STEPS = [500, 1000, 2000, 2500, 3000, 3500, 4000, 5000, 7000, 11000, 20000, 50000]


def random_init_cross_b(seed):
    torch.manual_seed(seed); np.random.seed(seed)
    m = GrokTransformer()
    m.eval()
    post_per_b = []
    P = 97
    for b in [1, 5, 42, 71]:
        tok = torch.tensor([[a, b, 97] for a in range(P)])
        with torch.no_grad():
            x = m.tok_emb(tok) + m.pos_emb.weight[None, :, :]
            attn_out, _ = m.attn(x, x, x, need_weights=False)
            x = x + attn_out
            for layer in m.mlp:
                x = layer(x)
            post_per_b.append(x[:, 2, :].numpy())
    fs = [np.abs(np.fft.fft(p, axis=0)) ** 2 for p in post_per_b]
    for pw in fs: pw[0] = 0
    avg = np.mean([p.sum(axis=0) for p in fs], axis=0)
    top10 = np.argsort(avg)[::-1][:10]
    cs = []
    for n in top10:
        for i in range(len(fs)):
            for j in range(i+1, len(fs)):
                a_ = fs[i][:, n]; c_ = fs[j][:, n]
                cs.append(float(a_ @ c_ / (np.linalg.norm(a_) * np.linalg.norm(c_) + 1e-12)))
    return float(np.mean(cs))


rand_baseline = [random_init_cross_b(s) for s in range(10)]
rand_mean = np.mean(rand_baseline)
rand_std = np.std(rand_baseline)
print(f"random-init baseline: {rand_mean:.3f} ± {rand_std:.3f} (n=10)")

configs = [
    ("sub s1", "v2_sub_seed1", False, "#c33"),
    ("add s0", "v2", False, "#36b"),
    ("mul s0", "v2_mul_seed0", True, "#393"),
    ("mul s1", "v2_mul_seed1", True, "#6b6"),
    ("mul s2", "v2_mul_seed2", True, "#9c9"),
]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True,
                                gridspec_kw={"height_ratios": [3, 2]})

# Panel 1: cross-b trajectory
ax1.axhspan(rand_mean - 2*rand_std, rand_mean + 2*rand_std, color="gray",
            alpha=0.15, label=f"random-init band ({rand_mean:.2f}±2σ)")
ax1.axhline(rand_mean, color="gray", ls=":", lw=1.0, alpha=0.6)

for label, run, is_mul, color in configs:
    gk = grok_step(run)
    vals = []
    for s in STEPS:
        try:
            r = probe(run, s, is_mul)
            vals.append(r["cross_b_cos_top10"])
        except FileNotFoundError:
            vals.append(np.nan)
    ax1.plot(STEPS, vals, "o-", color=color, lw=1.8, ms=5,
             label=f"{label} (grok@{gk})")
    ax1.axvline(gk, color=color, ls=":", alpha=0.35, lw=1)

ax1.set_xscale("log")
ax1.set_ylabel("cross-b spectral consistency (top-10)")
ax1.set_ylim(0.55, 1.05)
ax1.set_title("cross-operand spectral lock: dip-and-recovery is the grokking signal")
ax1.legend(fontsize=8, loc="lower right", ncol=2)
ax1.grid(alpha=0.3, which="both")

ax1.annotate("trivially\nhigh\n(attention uniform)",
             xy=(500, rand_mean), xytext=(420, 0.75),
             fontsize=8, color="gray",
             arrowprops=dict(arrowstyle="->", color="gray", alpha=0.6))

# Panel 2: val_acc
for label, run, is_mul, color in configs:
    vals = []
    for s in STEPS:
        vals.append(val_acc_at(run, s))
    ax2.plot(STEPS, vals, "o-", color=color, lw=1.8, ms=5, label=label)
ax2.set_xlabel("step (log)")
ax2.set_ylabel("val_acc")
ax2.set_ylim(-0.05, 1.05)
ax2.grid(alpha=0.3, which="both")

fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
