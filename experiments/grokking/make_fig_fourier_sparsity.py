"""
Figure: sparse vs dense Fourier codes at the MLP output.

Two panels:
 (a) Cumulative power-weighted concentration curves: fraction of
     power captured as we take the top K sorted frequencies, averaged
     (weighted by neuron power) across all MLP-output neurons.
     add/sub rise steeply; mul rises gradually.
 (b) Per-neuron top-frequency distribution: for each neuron, the
     single dominant frequency index. add/sub cluster on a few bins
     (sparse); mul spreads across the spectrum (dense).
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
FIG = Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig_fourier_sparsity.png")
P = 97


def primitive_root(p):
    phi = p - 1; n = phi; fs = []; d = 2
    while d*d <= n:
        if n % d == 0: fs.append(d)
        while n % d == 0: n //= d
        d += 1
    if n > 1: fs.append(n)
    for g in range(2, p):
        if all(pow(g, phi//f, p) != 1 for f in fs): return g


def hidden_spectrum(run, is_mul, b=42):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / "step_050000.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd); m.eval()
    tok = torch.tensor([[a, b, 97] for a in range(P)])
    with torch.no_grad():
        x = m.tok_emb(tok) + m.pos_emb.weight[None, :, :]
        attn_out, _ = m.attn(x, x, x, need_weights=False)
        x = x + attn_out
        for layer in m.mlp:
            x = layer(x)
        post = x[:, 2, :].numpy()
    if is_mul:
        g = primitive_root(P)
        ridx = np.array([pow(g, j, P) - 1 for j in range(P - 1)])
        post = post[ridx]
    pw = np.abs(np.fft.fft(post, axis=0)) ** 2
    pw[0] = 0
    return pw  # (freq, neuron)


configs = [
    ("sub s1", "v2_sub_seed1", False, "#c33"),
    ("sub s2", "v2_sub_seed2", False, "#e87"),
    ("add s0", "v2", False, "#36b"),
    ("mul s0", "v2_mul_seed0", True, "#393"),
    ("mul s1", "v2_mul_seed1", True, "#6b6"),
    ("mul s2", "v2_mul_seed2", True, "#9c9"),
]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))

for label, run, is_mul, color in configs:
    pw = hidden_spectrum(run, is_mul)
    nfreq = pw.shape[0] // 2  # half-spectrum (negative freqs redundant)
    # use half-spectrum for cleaner comparison
    pw_half = pw[:nfreq]
    per_neuron_total = pw_half.sum(axis=0)
    weights = per_neuron_total / (per_neuron_total.sum() + 1e-12)
    sorted_pw = np.sort(pw_half, axis=0)[::-1]
    cum = sorted_pw.cumsum(axis=0) / (pw_half.sum(axis=0) + 1e-12)
    # weighted cumulative
    weighted_cum = (cum * weights[None, :]).sum(axis=1)
    ax1.plot(np.arange(1, nfreq + 1), weighted_cum, color=color, lw=1.6, label=label)

    # panel b: histogram of dominant-frequency bin per neuron, weighted
    top_freq = np.argmax(pw_half, axis=0)
    # bin index, count-weighted by neuron power
    for fi, w in zip(top_freq, weights):
        pass  # use scatter plot
    if "mul" in label:
        offset = 0.1
    else:
        offset = -0.1
    pos = configs.index((label, run, is_mul, color))
    ax2.scatter(top_freq + np.random.randn(len(top_freq))*0.15,
                np.ones_like(top_freq) * pos + np.random.randn(len(top_freq))*0.12,
                s=np.clip(weights * 3000, 3, 80), color=color, alpha=0.55,
                edgecolors="none")

ax1.set_xlabel("top K frequency bins (sorted descending)")
ax1.set_ylabel("cumulative fraction of variance")
ax1.set_title("(a) sparse vs dense Fourier codes at MLP output\n"
              "(cumulative power curve — steep = sparse)")
ax1.axhline(0.9, color="k", ls=":", alpha=0.3, lw=0.8)
ax1.text(1, 0.91, "90%", fontsize=8, color="k", alpha=0.5)
ax1.set_xlim(1, pw.shape[0] // 2)
ax1.set_ylim(0, 1.05)
ax1.legend(fontsize=9, loc="lower right")
ax1.grid(alpha=0.3)

ax2.set_xlabel("dominant frequency bin for each neuron")
ax2.set_ylabel("config")
ax2.set_yticks(range(len(configs)))
ax2.set_yticklabels([l for l, _, _, _ in configs])
ax2.set_title("(b) per-neuron dominant frequency\n"
              "(marker size ∝ neuron power share; dense = spread wide)")
ax2.grid(alpha=0.3, axis="x")
ax2.set_xlim(-1, pw.shape[0] // 2 + 1)

fig.suptitle("add/sub commit to sparse Fourier codes; mul commits to a dense one", fontsize=11)
fig.tight_layout()
FIG.parent.mkdir(exist_ok=True, parents=True)
fig.savefig(FIG, dpi=140, bbox_inches="tight")
print(f"wrote {FIG}")
