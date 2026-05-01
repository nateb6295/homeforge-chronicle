"""
Figure: DLP-flip of MLP-neuron concentration.

Two panels:
  (left) concentration per task under additive-Fourier coords
  (right) concentration per task under multiplicative-Fourier coords (log re-indexed)

Shows the clean task-operation correspondence: sub/add sharp on left,
mul sharp on right.
"""
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
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_dlp_flip.png")
P = 97
G = 5


def log_reindex():
    return np.array([pow(G, k, P) - 1 for k in range(P - 1)])


def conc(run, step, re_index=None):
    model = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    W = model.mlp[0].weight.data.cpu().numpy()
    resp = W @ E[1:97].T
    if re_index is not None:
        resp = resp[:, re_index]
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    return spec.max(axis=1) / spec.sum(axis=1)


runs = [
    ("sub s1", "v2_sub_seed1", "#2a6"),
    ("sub s2", "v2_sub_seed2", "#4c8"),
    ("add s0", "v2", "#e90"),
    ("mul s0", "v2_mul_seed0", "#36b"),
    ("mul s1", "v2_mul_seed1", "#58c"),
    ("mul s2", "v2_mul_seed2", "#7ad"),
]

ri = log_reindex()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)

for label, run, color in runs:
    c_add = conc(run, 50000, None)
    c_mul = conc(run, 50000, ri)
    ax1.hist(c_add, bins=40, alpha=0.55, color=color, label=f"{label} (μ={c_add.mean():.2f})", density=True)
    ax2.hist(c_mul, bins=40, alpha=0.55, color=color, label=f"{label} (μ={c_mul.mean():.2f})", density=True)

ax1.set_title("Additive-Fourier basis (natural vocab)")
ax1.set_xlabel("per-neuron top-bin concentration")
ax1.set_ylabel("density")
ax1.legend(fontsize=8, loc="upper right")
ax1.grid(alpha=0.3)

ax2.set_title(f"Multiplicative-Fourier basis (log vocab, g={G})")
ax2.set_xlabel("per-neuron top-bin concentration")
ax2.legend(fontsize=8, loc="upper right")
ax2.grid(alpha=0.3)

fig.suptitle("MLP-neuron Fourier concentration flips under group change:\n"
             "sub/add concentrate additively, mul concentrates multiplicatively.",
             fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
