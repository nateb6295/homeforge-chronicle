"""
Does mul's Fourier code start dense or become dense at grok?

For each snapshot through training, compute the power-weighted top-10
cumulative concentration at the MLP output (across operand a at fixed b,
DLP-reindexed for mul). Low concentration = dense code, high = sparse.
"""
import sys, json
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97
STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def primitive_root(p):
    phi = p - 1; n = phi; fs = []; d = 2
    while d*d <= n:
        if n%d==0: fs.append(d)
        while n%d==0: n//=d
        d += 1
    if n>1: fs.append(n)
    for g in range(2,p):
        if all(pow(g,phi//f,p)!=1 for f in fs): return g


def weighted_top_k(pw, k):
    """power-weighted top-K cumulative concentration."""
    sorted_pw = np.sort(pw, axis=0)[::-1]
    cum = sorted_pw.cumsum(axis=0) / (pw.sum(axis=0) + 1e-12)
    per_neuron_total = pw.sum(axis=0)
    weights = per_neuron_total / (per_neuron_total.sum() + 1e-12)
    return float((cum[k-1] * weights).sum())


def probe(run, step, is_mul, b=42):
    m = GrokTransformer()
    sd = torch.load(BASE/run/"snapshots"/f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd); m.eval()
    tok = torch.tensor([[a, b, 97] for a in range(P)])
    with torch.no_grad():
        x = m.tok_emb(tok) + m.pos_emb.weight[None,:,:]
        attn_out, _ = m.attn(x, x, x, need_weights=False)
        x = x + attn_out
        for layer in m.mlp: x = layer(x)
        post = x[:,2,:].numpy()
    if is_mul:
        g = primitive_root(P)
        ridx = np.array([pow(g,j,P)-1 for j in range(P-1)])
        post = post[ridx]
    pw = np.abs(np.fft.fft(post, axis=0))**2
    pw[0] = 0
    half = pw.shape[0] // 2
    pw_half = pw[:half]
    return weighted_top_k(pw_half, 10)


def grok_step(run):
    with open(BASE/run/"metrics.jsonl") as f:
        for line in f:
            r = json.loads(line)
            if r.get("val_acc", 0) >= 0.95:
                return r["step"]
    return None


configs = [
    ("sub s1", "v2_sub_seed1", False),
    ("add s0", "v2", False),
    ("mul s0", "v2_mul_seed0", True),
    ("mul s1", "v2_mul_seed1", True),
    ("mul s2", "v2_mul_seed2", True),
]

print("Top-10 power-weighted cumulative concentration through training")
print("(1.0 = all power in 10 bins = sparse; 0.2 = dense)")
print()
print(f"{'run':<10} {'grok':<6} " + " ".join(f"{s:<6}" for s in STEPS))
print("-" * (18 + len(STEPS) * 7))
for label, run, is_mul in configs:
    gk = grok_step(run)
    row = []
    for s in STEPS:
        try:
            c = probe(run, s, is_mul)
            row.append(f"{c:<6.2f}")
        except FileNotFoundError:
            row.append(f"{'—':<6}")
    print(f"{label:<10} {gk:<6} " + " ".join(row))
