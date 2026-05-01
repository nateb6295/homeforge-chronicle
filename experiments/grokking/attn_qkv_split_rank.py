"""
Split attn.in_proj into Q, K, V and measure each's effective rank
through training. If post-grok attention is content-uniform (as we
just observed in attention patterns), Q and K projections should be
near-trivial (low rank, possibly near-zero norm) while V carries
the operand-aggregation signal.
"""
import json, sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 50000]


def eff_rank(W):
    s = np.linalg.svd(W, compute_uv=False)
    if s.sum() == 0:
        return 0.0
    return float(s.sum() ** 2 / (s ** 2).sum())


def split_qkv(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()  # (3*d, d), Q|K|V stacked
    d = W.shape[1]
    Wq, Wk, Wv = W[:d], W[d:2*d], W[2*d:]
    return (eff_rank(Wq), eff_rank(Wk), eff_rank(Wv),
            float(np.linalg.norm(Wq)), float(np.linalg.norm(Wk)), float(np.linalg.norm(Wv)))


def grok_step(run):
    with open(BASE / run / "metrics.jsonl") as f:
        for l in f:
            r = json.loads(l)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


runs = [("sub s1", "v2_sub_seed1"), ("add s0", "v2"), ("mul s0", "v2_mul_seed0")]

for label, run in runs:
    gk = grok_step(run)
    print(f"\n{label} (grok@{gk})")
    print(f"  step    rank_Q  rank_K  rank_V  ||Q||   ||K||   ||V||")
    for s in STEPS:
        rq, rk, rv, nq, nk, nv = split_qkv(run, s)
        marker = " ←grok" if gk and abs(s - gk) < 1500 else ""
        print(f"  {s:<6}  {rq:6.2f}  {rk:6.2f}  {rv:6.2f}  {nq:5.2f}   {nk:5.2f}   {nv:5.2f}{marker}")
