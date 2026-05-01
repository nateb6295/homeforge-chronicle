"""
K is rank 2-4 post-grok. What are its dominant directions?

Hypothesis: K detects operand positions (pos 0 and pos 1) and/or the
= position. If K's top-2 right-singular vectors align with pos_emb[0]
and pos_emb[1] (or their sum/difference), that completes the attention
story: Q asks "am I the = marker?", K reports "here is an operand
position", softmax combines to give each position its self-detecting
score.
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def cos(a, b):
    return abs(float(a @ b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))


def decomp(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()
    d = W.shape[1]
    Wk = W[d:2*d]
    U, S, Vt = np.linalg.svd(Wk)
    pe = m.pos_emb.weight.data.numpy()
    te = m.tok_emb.weight.data.numpy()
    return U, S, Vt, pe, te


runs = [
    ("sub s1", "v2_sub_seed1"),
    ("sub s2", "v2_sub_seed2"),
    ("add s0", "v2"),
    ("mul s0", "v2_mul_seed0"),
    ("mul s1", "v2_mul_seed1"),
    ("mul s2", "v2_mul_seed2"),
]

print("K projection top-3 singular directions at step 50000")
print("Testing alignment with pos_emb[0,1,2] and E[97]")
print()
print(f"{'run':<10} {'σ₀':<6} {'σ₁':<6} {'σ₂':<6}   "
      f"{'v₀·p0':<7} {'v₀·p1':<7} {'v₀·p2':<7}   "
      f"{'v₁·p0':<7} {'v₁·p1':<7} {'v₁·p2':<7}")
print("-" * 100)
for label, run in runs:
    U, S, Vt, pe, te = decomp(run, 50000)
    v0, v1, v2 = Vt[0], Vt[1], Vt[2]
    print(f"{label:<10} {S[0]:<6.2f} {S[1]:<6.2f} {S[2]:<6.2f}   "
          f"{cos(v0, pe[0]):<7.3f} {cos(v0, pe[1]):<7.3f} {cos(v0, pe[2]):<7.3f}   "
          f"{cos(v1, pe[0]):<7.3f} {cos(v1, pe[1]):<7.3f} {cos(v1, pe[2]):<7.3f}")

print()
print("Testing 2D span {v₀, v₁} coverage of {pe[0], pe[1], pe[2]}")
print("(cos with best unit vector in span)")
print(f"{'run':<10} {'p0_in_span':<11} {'p1_in_span':<11} {'p2_in_span':<11}")
print("-" * 55)
for label, run in runs:
    U, S, Vt, pe, te = decomp(run, 50000)
    B = np.stack([Vt[0], Vt[1]], axis=0)  # (2, d)
    # project each pe onto span(B)
    for k in [0, 1, 2]:
        pass
    def span_cos(v, B):
        v = v / (np.linalg.norm(v) + 1e-12)
        # projection: B.T @ inv(B @ B.T) @ B @ v ; rows of B are orthonormal after SVD of Wk
        proj = B.T @ (B @ v)
        return float(np.linalg.norm(proj))  # since v is unit, this is cos with nearest in-span
    print(f"{label:<10} "
          f"{span_cos(pe[0], B):<11.3f} "
          f"{span_cos(pe[1], B):<11.3f} "
          f"{span_cos(pe[2], B):<11.3f}")
