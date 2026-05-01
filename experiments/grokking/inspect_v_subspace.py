"""
What does V's rank-26 subspace compute?

After grok, V retains ~22-29 effective rank while Q/K are rank 1-4.
V's job is to decide *what value* each position contributes to the
output (the mix is fixed by uniform attention). So the action happens
in the image of V applied to each position's residual stream.

For the `=` (readout) position, V's input is essentially pos_emb[2] +
tok_emb[97] — a single direction (they're collinear). So V's output at
pos 2 is a single fixed vector, uninformative.

For operand positions 0 and 1, V's input is pos_emb[k] + tok_emb[a_k].
The variation across a_k ∈ {0..96} is what V can use to encode operand
identity. Question: what is the Fourier structure of V @ (pos_emb[k] +
tok_emb[a])? Is it in the model's operation-appropriate basis (natural
for add/sub, DLP-reindexed for mul)?

Compute: for each position k ∈ {0,1}, form the P×d matrix
M_k[a] = V @ (pos_emb[k] + tok_emb[a]).
FFT each column of M_k across the 97 operand values (DLP-reindex for
mul). Measure spectral concentration.

If the V output carries the Fourier-structured operand identity, that's
the mechanism by which the MLP can implement the trig circuit: the
attention layer hands it a uniform mix of two Fourier-indexed operand
vectors at the read position, and the MLP applies the nonlinear combination.
"""
import sys, json
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def primitive_root(p):
    phi = p - 1
    n = phi
    factors = []
    d = 2
    while d * d <= n:
        if n % d == 0:
            factors.append(d)
            while n % d == 0:
                n //= d
        d += 1
    if n > 1:
        factors.append(n)
    for g in range(2, p):
        if all(pow(g, phi // f, p) != 1 for f in factors):
            return g
    return None


def load(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()
    d = W.shape[1]
    Wv = W[2 * d:]
    pe = m.pos_emb.weight.data.numpy()
    te = m.tok_emb.weight.data.numpy()
    return Wv, pe, te


def concentration(vals):
    """Fourier concentration (top-3 / total) for a 1D array of length P."""
    f = np.fft.fft(vals)
    pw = np.abs(f) ** 2
    pw[0] = 0
    s = pw.sum()
    if s == 0:
        return 0.0
    top = np.sort(pw)[::-1][:3].sum()
    return float(top / s)


def run_config(label, run, step, is_mul):
    Wv, pe, te = load(run, step)
    # V output for each operand value a at position k
    # input at pos k with token a: pe[k] + te[a]  (d-dim)
    # V output: Wv @ input, shape (d,)
    results = {}
    for k in [0, 1]:
        inputs = np.stack([pe[k] + te[a] for a in range(P)], axis=0)  # (P, d)
        vouts = inputs @ Wv.T  # (P, d)
        if is_mul:
            g = primitive_root(P)
            re_idx = np.array([pow(g, j, P) - 1 for j in range(P - 1)])
            reord = vouts[re_idx]  # (P-1, d) in DLP basis
            concs = [concentration(reord[:, j]) for j in range(vouts.shape[1])]
        else:
            concs = [concentration(vouts[:, j]) for j in range(vouts.shape[1])]
        # summarize: mean over top-10 most-concentrated neurons
        concs_sorted = sorted(concs, reverse=True)
        results[k] = {
            "mean_all": float(np.mean(concs)),
            "mean_top10": float(np.mean(concs_sorted[:10])),
            "max": float(concs_sorted[0]),
        }
    return results


runs = [
    ("sub s1", "v2_sub_seed1", False),
    ("sub s2", "v2_sub_seed2", False),
    ("add s0", "v2", False),
    ("mul s0", "v2_mul_seed0", True),
    ("mul s1", "v2_mul_seed1", True),
    ("mul s2", "v2_mul_seed2", True),
]

print("V-output Fourier concentration across operand values at step 50000")
print("(mul configs DLP-reindexed via primitive root g=5)")
print()
print(f"{'run':<10} {'pos':<4} {'mean_all':<10} {'mean_top10':<11} {'max':<6}")
print("-" * 50)
for label, run, is_mul in runs:
    r = run_config(label, run, 50000, is_mul)
    for k in [0, 1]:
        d = r[k]
        print(f"{label:<10} {k:<4} {d['mean_all']:<10.3f} "
              f"{d['mean_top10']:<11.3f} {d['max']:<6.3f}")

print()
print("Baseline concentration (uniform random over P values): ~0.09")
print("(top-3 / (P-1) = 3/96 = 0.031 for uniform, but power spectrum")
print(" of gaussian noise has slight concentration)")
