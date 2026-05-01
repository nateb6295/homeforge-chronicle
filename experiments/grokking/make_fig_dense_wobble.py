"""
Figure — dense step-to-step L2 distance comparison, sub seed 1 vs seed 2.

Shows that both seeds exhibit heavy-tailed drift with similar shape;
wobble seed has modestly larger amplitude but no qualitative difference.
"""
import csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path("/home/nate-agx/chronicle/experiments/grokking")
FIG = BASE / "figures" / "fig_dense_wobble.png"

def load(path):
    rows = list(csv.DictReader(open(path)))
    return [int(r["step"]) for r in rows], [float(r["l2_delta"]) for r in rows]

s1_steps, s1_l2 = load(BASE / "dense_wobble_sub_seed1.csv")
s2_steps, s2_l2 = load(BASE / "dense_wobble_sub_seed2.csv")
m0_steps, m0_l2 = load(BASE / "dense_wobble_mul_seed0.csv")
m1_steps, m1_l2 = load(BASE / "dense_wobble_mul_seed1.csv")
m2_steps, m2_l2 = load(BASE / "dense_wobble_mul_seed2.csv")
a0_steps, a0_l2 = load(BASE / "dense_wobble_v2.csv")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

series = [
    (s1_steps, s1_l2, "#2a6", "sub s1 (stable)"),
    (s2_steps, s2_l2, "#d42", "sub s2 (wobble)"),
    (a0_steps, a0_l2, "#e90", "add s0"),
    (m0_steps, m0_l2, "#36b", "mul s0"),
    (m1_steps, m1_l2, "#58c", "mul s1"),
    (m2_steps, m2_l2, "#7ad", "mul s2"),
]

for steps, l2, color, label in series:
    mean = sum(l2) / len(l2)
    ax1.plot(steps, l2, color=color, lw=0.7, alpha=0.75, label=f"{label}  mean={mean:.2f}")
ax1.set_xlabel("step")
ax1.set_ylabel("step-to-step L2 distance")
ax1.set_title("weight-space trajectory (100-step resolution)")
ax1.legend(fontsize=8, loc="upper right", ncol=2)
ax1.grid(alpha=0.3)

for _, l2, color, label in series:
    ax2.hist(l2, bins=25, alpha=0.4, color=color, label=label, density=True)
ax2.set_xlabel("step-to-step L2 distance")
ax2.set_ylabel("density")
ax2.set_title("distribution of step-to-step motion")
ax2.legend(fontsize=8, loc="upper right", ncol=2)
ax2.grid(alpha=0.3)

fig.suptitle("Dense post-grok dynamics: drift-not-hopping across 6 networks (sub, add, mul)", fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
