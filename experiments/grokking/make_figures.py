#!/usr/bin/env python3
"""Generate paper figures from existing run snapshots. CPU-only to avoid
GPU contention with trainers still running."""
import sys
from pathlib import Path
import numpy as np
import torch
import torch.nn.functional as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

RUNS = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
FIGS = Path("/home/nate-agx/chronicle/experiments/grokking/figures")
FIGS.mkdir(exist_ok=True)

TASKS = {
    "add (seed 0)": RUNS / "v2",
    "sub (seed 0)": RUNS / "v2_sub_seed0",
    "mul (seed 0)": RUNS / "v2_mul_seed0",
    "mul (seed 1)": RUNS / "v2_mul_seed1",
    "mul (seed 2)": RUNS / "v2_mul_seed2",
}
STEP = 50000


def load(path):
    m = GrokTransformer()
    sd = torch.load(path / "snapshots" / f"step_{STEP:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    probe = torch.load(path / "snapshots" / "probe_inputs.pt",
                       map_location="cpu", weights_only=True)
    return m, probe


def grad_magnitudes(model, probe):
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    logits = model(probe["a"], probe["b"])
    loss = F.cross_entropy(logits, probe["y"])
    loss.backward()
    per_tensor = {}
    for name, p in model.named_parameters():
        if p.grad is not None:
            per_tensor[name] = p.grad.detach().abs().flatten().numpy()
    all_grads = np.concatenate(list(per_tensor.values()))
    return per_tensor, all_grads


def top_frac(x, frac):
    k = max(1, int(len(x) * frac))
    return float(np.sort(x)[::-1][:k].sum() / x.sum())


# ------------------------------------------------------------
# Figure 1: top-0.1% concentration bar chart across runs
# ------------------------------------------------------------
print("computing concentration...")
concentration = {}
anatomy = {}
for label, path in TASKS.items():
    m, probe = load(path)
    per_tensor, all_grads = grad_magnitudes(m, probe)
    concentration[label] = {
        "top0.1%": top_frac(all_grads, 0.001),
        "top1%":   top_frac(all_grads, 0.01),
        "max/mean": float(all_grads.max() / all_grads.mean()),
    }
    anatomy[label] = {k: v.sum() for k, v in per_tensor.items()}
    total = sum(anatomy[label].values())
    anatomy[label] = {k: v / total for k, v in anatomy[label].items()}
    print(f"  {label:18s} top0.1%={concentration[label]['top0.1%']:.3f} "
          f"max/μ={concentration[label]['max/mean']:.0f}")

fig, ax = plt.subplots(figsize=(8, 4))
labels = list(concentration.keys())
vals = [concentration[l]["top0.1%"] for l in labels]
bars = ax.bar(range(len(labels)), vals, color="#377eb8")
ax.axhline(0.001, color="gray", linestyle="--",
           label="uniform baseline (0.001)")
ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels, rotation=30, ha="right")
ax.set_ylabel("fraction of |∇| L1 energy in top 0.1%")
ax.set_title("Concentration exists across runs, degree is init-stochastic\n"
             "(post-grok, step 50k; uniform would be 0.001)")
ax.legend()
for i, v in enumerate(vals):
    ax.text(i, v + 0.01, f"{v:.2f}", ha="center", fontsize=8)
plt.tight_layout()
plt.savefig(FIGS / "fig1_concentration.png", dpi=150)
plt.close()
print(f"wrote {FIGS/'fig1_concentration.png'}")

# ------------------------------------------------------------
# Figure 2: anatomy breakdown (stacked bar by tensor family)
# ------------------------------------------------------------
families = {
    "tok_emb": ["tok_emb"],
    "pos_emb": ["pos_emb"],
    "attn.out_proj.bias": ["attn.out_proj.bias"],
    "attn other": ["attn.in_proj", "attn.out_proj.weight"],
    "ln": ["ln1", "ln2"],
    "mlp": ["mlp"],
    "out": ["out"],
}


def family_of(param_name):
    for fam, prefixes in families.items():
        if any(p in param_name for p in prefixes):
            return fam
    return "other"


fam_shares = {label: {f: 0.0 for f in families} for label in labels}
for label in labels:
    for pname, share in anatomy[label].items():
        fam_shares[label][family_of(pname)] += share

fig, ax = plt.subplots(figsize=(8, 4.5))
bottom = np.zeros(len(labels))
colors = plt.cm.tab10.colors
for i, fam in enumerate(families):
    vals = [fam_shares[l][fam] for l in labels]
    ax.bar(range(len(labels)), vals, bottom=bottom, label=fam, color=colors[i])
    bottom += np.array(vals)
ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels, rotation=30, ha="right")
ax.set_ylabel("share of |∇| L1 energy")
ax.set_title("Anatomy: where the gradient lives (post-grok, step 50k)")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.savefig(FIGS / "fig2_anatomy.png", dpi=150)
plt.close()
print(f"wrote {FIGS/'fig2_anatomy.png'}")

# ------------------------------------------------------------
# Figure 3: ablation grid heatmap (val_acc after knockout)
# hand-entered from overnight results
# ------------------------------------------------------------
abl = np.array([
    # row 97(=), attn.out_proj.bias
    [0.72, 0.44],  # add seed 0
    [0.51, 0.51],  # sub seed 0
    [0.95, 0.93],  # mul seed 0
    [1.00, 1.00],  # mul seed 1
    [0.22, 0.08],  # mul seed 2
    [1.00, 1.00],  # add p=113 seed 0 (row 113)
])
abl_labels = ["add s0 (p97)", "sub s0 (p97)", "mul s0 (p97)",
              "mul s1 (p97)", "mul s2 (p97)", "add s0 (p113)"]
fig, ax = plt.subplots(figsize=(6, 5))
im = ax.imshow(abl, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")
ax.set_xticks([0, 1])
ax.set_xticklabels(["zero row of\nequals token", "zero\nattn.out_proj.bias"])
ax.set_yticks(range(len(abl_labels)))
ax.set_yticklabels(abl_labels)
for i in range(abl.shape[0]):
    for j in range(abl.shape[1]):
        ax.text(j, i, f"{abl[i,j]:.2f}", ha="center", va="center",
                color="black", fontsize=10)
ax.set_title("Causal-ablation val_acc (baseline 1.00) —\n"
             "locus is initialization-stochastic")
plt.colorbar(im, ax=ax, label="val_acc after knockout")
plt.tight_layout()
plt.savefig(FIGS / "fig3_ablation_grid.png", dpi=150)
plt.close()
print(f"wrote {FIGS/'fig3_ablation_grid.png'}")

# ------------------------------------------------------------
# Figure 4: Fourier of row 97 across tasks
# ------------------------------------------------------------
fig, axes = plt.subplots(3, 1, figsize=(8, 6), sharex=True)
task_rows = {
    "add": RUNS / "v2",
    "sub": RUNS / "v2_sub_seed0",
    "mul": RUNS / "v2_mul_seed0",
}
for ax, (t, path) in zip(axes, task_rows.items()):
    sd = torch.load(path / "snapshots" / f"step_{STEP:06d}.pt",
                    map_location="cpu", weights_only=True)
    row = sd["tok_emb.weight"][97].numpy()
    spec = np.abs(np.fft.fft(row))[: len(row) // 2]
    ax.plot(spec, color="#377eb8")
    ax.set_ylabel(f"{t}\n|FFT|")
    ax.set_yscale("log")
axes[-1].set_xlabel("frequency index")
axes[0].set_title("Row-97 (equals-token) Fourier spectrum — "
                  "similar across tasks")
plt.tight_layout()
plt.savefig(FIGS / "fig4_fourier_row97.png", dpi=150)
plt.close()
print(f"wrote {FIGS/'fig4_fourier_row97.png'}")

print("done.")
