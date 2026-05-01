#!/usr/bin/env python3
"""fig5: sub seed 2 wobble trajectory vs sub seed 1 stable."""
import json, pathlib
import matplotlib.pyplot as plt

ROOT = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/runs")

def load(run):
    xs, train, val = [], [], []
    with (ROOT / run / "metrics.jsonl").open() as f:
        for line in f:
            r = json.loads(line)
            xs.append(r["step"])
            train.append(r["train_acc"])
            val.append(r["val_acc"])
    return xs, train, val

fig, ax = plt.subplots(figsize=(7.5, 4.2))
for run, color, label in [
    ("v2_sub_seed1", "#1f77b4", "sub seed 1 (stable)"),
    ("v2_sub_seed2", "#d62728", "sub seed 2 (wobble)"),
]:
    xs, train, val = load(run)
    ax.plot(xs, val, color=color, label=f"{label} — val", linewidth=1.6)
    ax.plot(xs, train, color=color, linestyle="--", alpha=0.5, linewidth=1.0,
            label=f"{label} — train")

ax.axhline(1.0, color="grey", linewidth=0.5, alpha=0.5)
ax.set_xlabel("training step")
ax.set_ylabel("accuracy")
ax.set_title("Post-grok wobble — sub seed 2 vs seed 1 (same hyperparams)")
ax.set_ylim(0, 1.05)
ax.legend(loc="lower right", fontsize=8)
ax.grid(alpha=0.3)
plt.tight_layout()
out = pathlib.Path("/home/nate-agx/chronicle/experiments/grokking/figures/fig5_wobble.png")
plt.savefig(out, dpi=140)
print("wrote", out)
