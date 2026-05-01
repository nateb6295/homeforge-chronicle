"""
Does attention have its own grok-aligned structural event?

Two measures per training step:
  - effective rank of attn.out_proj via participation ratio of SVs
    (tr(Sigma)^2 / tr(Sigma^2) — small = low-rank, d_model = full-rank)
  - effective rank of attn.in_proj (Q|K|V stack)

If these collapse toward low rank at grok, the attn is specializing
its read/write subspaces to a few directions — another structural
signature of grok beyond the MLP Fourier story.
"""
import json, sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def eff_rank(W):
    s = np.linalg.svd(W, compute_uv=False)
    return float(s.sum() ** 2 / (s ** 2).sum())


def ranks(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W_out = m.attn.out_proj.weight.data.numpy()
    W_in = m.attn.in_proj_weight.data.numpy()
    W_mlp0 = m.mlp[0].weight.data.numpy()
    W_mlp2 = m.mlp[2].weight.data.numpy()
    return eff_rank(W_out), eff_rank(W_in), eff_rank(W_mlp0), eff_rank(W_mlp2)


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        for l in f:
            r = json.loads(l)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


runs = [("sub s1", "v2_sub_seed1"), ("add s0", "v2"),
        ("mul s0", "v2_mul_seed0"), ("mul s1", "v2_mul_seed1")]

print("Effective rank (participation ratio) through training\n")
print(f"Each cell: attn_out | attn_in | mlp_0 | mlp_2\n")

for label, run in runs:
    gk = grok_step(run)
    print(f"{label} (grok@{gk}):")
    for s in STEPS:
        o, i, m0, m2 = ranks(run, s)
        marker = "  ←grok" if gk and abs(s - gk) < 1500 else ""
        print(f"  step {s:<6}  {o:5.1f} | {i:5.1f} | {m0:5.1f} | {m2:5.1f}{marker}")
    print()
