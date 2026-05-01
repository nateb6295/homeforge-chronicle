"""
Figure: Q/K direction alignment with positional/token embeddings.

Two panels:
 (a) |Q_v · pe_k| for k in {0,1,2} and |Q_v · E[97]| across all 6 configs.
     Shows Q universally points at the =/pos-2 merged marker.
 (b) 2D span of K's top-2 right-singular vectors, coverage of pe[0], pe[1], pe[2].
     Shows sub has K as operand-position detector; add/mul don't.
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
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_qk_directions.png")


def analyze(run, step=50000):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()
    d = W.shape[1]
    Wq, Wk = W[:d], W[d:2*d]
    _, _, VtQ = np.linalg.svd(Wq)
    _, _, VtK = np.linalg.svd(Wk)
    pe = m.pos_emb.weight.data.numpy()
    te = m.tok_emb.weight.data.numpy()
    def cosabs(a, b):
        return abs(float(a @ b)) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12)
    q_v = VtQ[0]
    k_B = np.stack([VtK[0], VtK[1]], axis=0)
    def span(v):
        v = v / (np.linalg.norm(v) + 1e-12)
        return float(np.linalg.norm(k_B.T @ (k_B @ v)))
    return {
        "Q_pe0": cosabs(q_v, pe[0]),
        "Q_pe1": cosabs(q_v, pe[1]),
        "Q_pe2": cosabs(q_v, pe[2]),
        "Q_E97": cosabs(q_v, te[97]),
        "K_pe0": span(pe[0]),
        "K_pe1": span(pe[1]),
        "K_pe2": span(pe[2]),
    }


configs = [
    ("sub s1", "v2_sub_seed1"),
    ("sub s2", "v2_sub_seed2"),
    ("add s0", "v2"),
    ("mul s0", "v2_mul_seed0"),
    ("mul s1", "v2_mul_seed1"),
    ("mul s2", "v2_mul_seed2"),
]

results = {lbl: analyze(run) for lbl, run in configs}
labels = [lbl for lbl, _ in configs]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))

# (a) Q direction alignment
x = np.arange(len(labels))
width = 0.2
ax1.bar(x - 1.5*width, [results[l]["Q_pe0"] for l in labels], width, label="pos[0]", color="#bbb")
ax1.bar(x - 0.5*width, [results[l]["Q_pe1"] for l in labels], width, label="pos[1]", color="#888")
ax1.bar(x + 0.5*width, [results[l]["Q_pe2"] for l in labels], width, label="pos[2]", color="#c33")
ax1.bar(x + 1.5*width, [results[l]["Q_E97"] for l in labels], width, label="E[97]", color="#e87", alpha=0.7)
ax1.set_xticks(x)
ax1.set_xticklabels(labels, rotation=30)
ax1.set_ylabel("|cos(Q's top v, target)|")
ax1.set_title("(a) Q's rank-1 direction: 5/6 align with =/pos-2 merged marker\n"
              "(pos_emb[2] and E[97] are perfectly collinear — cos=1.0000)",
              fontsize=10)
ax1.axhline(1.0, color="k", ls=":", alpha=0.3, lw=0.8)
ax1.set_ylim(0, 1.05)
ax1.legend(fontsize=9, loc="upper right", ncol=4)
ax1.grid(alpha=0.3, axis="y")

# (b) K 2D-span coverage of pe
ax2.bar(x - width, [results[l]["K_pe0"] for l in labels], width, label="pos[0]", color="#36b")
ax2.bar(x,         [results[l]["K_pe1"] for l in labels], width, label="pos[1]", color="#58d")
ax2.bar(x + width, [results[l]["K_pe2"] for l in labels], width, label="pos[2]", color="#aaa")
ax2.set_xticks(x)
ax2.set_xticklabels(labels, rotation=30)
ax2.set_ylabel("cos(pos[k], span{K's top 2})")
ax2.set_title("(b) K's 2D span: sub allocates it to operand-position detection;\n"
              "add/mul leave positions barely detectable (pe[0]≈pe[1] already at input)",
              fontsize=10)
ax2.axhline(1.0, color="k", ls=":", alpha=0.3, lw=0.8)
ax2.set_ylim(0, 1.05)
ax2.legend(fontsize=9, loc="upper right", ncol=3)
ax2.grid(alpha=0.3, axis="y")

# shade sub region
for ax in (ax1, ax2):
    ax.axvspan(-0.5, 1.5, alpha=0.08, color="red")
    ax.text(0.5, 1.01, "non-commutative", ha="center", fontsize=8,
            color="#933", alpha=0.8, transform=ax.get_xaxis_transform())
    ax.text(3.5, 1.01, "commutative", ha="center", fontsize=8,
            color="#336", alpha=0.8, transform=ax.get_xaxis_transform())

fig.suptitle("Attention circuit decomposition: Q universal, K task-specific", fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
