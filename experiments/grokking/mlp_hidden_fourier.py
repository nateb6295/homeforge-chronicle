"""
Where exactly does mul's DLP-Fourier structure appear in the forward pass?

V carries Fourier in add/sub but not mul (0.15 concentration even after
DLP reindex). W_mlp0 @ tok_emb shows 0.37 post-DLP. But the MLP sees
W_mlp0 @ (attention_output), not raw tokens.

Test: run the model on mul prompts spanning all 97 operand values for
position 0 (fixing b), collect the MLP's pre-activation hidden state at
the = position. FFT across a-values (DLP-reindexed).

If concentration is high, the Fourier structure materializes at the
MLP input stage.
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def primitive_root(p):
    phi = p - 1; n = phi; factors = []; d = 2
    while d*d <= n:
        if n % d == 0:
            factors.append(d)
            while n % d == 0: n //= d
        d += 1
    if n > 1: factors.append(n)
    for g in range(2, p):
        if all(pow(g, phi//f, p) != 1 for f in factors): return g


def concentration(vals):
    f = np.fft.fft(vals)
    pw = np.abs(f) ** 2
    pw[0] = 0
    s = pw.sum()
    if s == 0: return 0.0
    top = np.sort(pw)[::-1][:3].sum()
    return float(top / s)


def probe(run, step, b_fixed, is_mul):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    m.eval()
    tok = torch.tensor([[a, b_fixed, 97] for a in range(P)])  # (P, 3)
    with torch.no_grad():
        x = m.tok_emb(tok) + m.pos_emb.weight[None, :, :]  # (P, 3, d)
        attn_out, _ = m.attn(x, x, x, need_weights=False)
        x = x + attn_out
        x = m.ln1(x) if hasattr(m, "ln1") else x
        # MLP first projection (pre-activation)
        # GrokTransformer has m.mlp with sequential; access weights directly
        W0 = None
        for layer in m.mlp:
            if isinstance(layer, torch.nn.Linear):
                W0 = layer
                break
        pre = W0(x)  # (P, 3, mlp_d)
        # read at the = position (index 2)
        pre_eq = pre[:, 2, :].numpy()  # (P, mlp_d)
    if is_mul:
        g = primitive_root(P)
        re_idx = np.array([pow(g, j, P) - 1 for j in range(P - 1)])
        reord = pre_eq[re_idx]
    else:
        reord = pre_eq
    concs = [concentration(reord[:, j]) for j in range(reord.shape[1])]
    concs_sorted = sorted(concs, reverse=True)
    return {
        "mean_all": float(np.mean(concs)),
        "mean_top10": float(np.mean(concs_sorted[:10])),
        "max": float(concs_sorted[0]),
        "frac_above_0.3": float(np.mean([c > 0.3 for c in concs])),
    }


runs = [
    ("sub s1", "v2_sub_seed1", False),
    ("add s0", "v2", False),
    ("mul s0", "v2_mul_seed0", True),
    ("mul s1", "v2_mul_seed1", True),
    ("mul s2", "v2_mul_seed2", True),
]

print("MLP hidden (pre-activation) Fourier concentration at = position")
print("vary operand-a in [0..96], fix b=42; FFT across a")
print("(mul configs DLP-reindexed)")
print()
print(f"{'run':<10} {'mean_all':<10} {'mean_top10':<11} {'max':<6} {'frac>0.3':<9}")
print("-" * 55)
for label, run, is_mul in runs:
    r = probe(run, 50000, 42, is_mul)
    print(f"{label:<10} {r['mean_all']:<10.3f} "
          f"{r['mean_top10']:<11.3f} {r['max']:<6.3f} {r['frac_above_0.3']:<9.2f}")
