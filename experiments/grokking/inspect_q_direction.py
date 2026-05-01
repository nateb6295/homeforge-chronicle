"""
What is the single dominant Q direction?

Post-grok, Q has effective rank ~1. Its top singular vector points to
a single 128D direction. What does that direction correspond to?

Tests:
  (1) Does the Q-direction align with the positional embedding for
      position 2 (the = position, which is where we read from)?
  (2) Does it align with the tok_emb of the = token (id 97)?
  (3) What does it look like when we compare it across seeds? If
      all seeds land on the "same" direction (up to sign/norm), that's
      a universal geometric feature.

The Q matrix shape is (d_model, d_model) = (128, 128). Q acts as
query = Q @ residual_stream_column. Since rank(Q) = 1, we can write
Q ≈ σ u v^T where u, v are unit vectors. The "question it asks" is
v (the direction in the input that matters) and u is the direction
it gets projected to in query-space.
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def q_decomp(run, step):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    W = m.attn.in_proj_weight.data.numpy()
    d = W.shape[1]
    Wq = W[:d]
    U, S, Vt = np.linalg.svd(Wq)
    # top singular vectors: u = U[:,0], v = Vt[0]
    return U[:, 0], Vt[0], S, m.pos_emb.weight.data.numpy(), m.tok_emb.weight.data.numpy()


runs = [
    ("sub s1", "v2_sub_seed1"),
    ("sub s2", "v2_sub_seed2"),
    ("add s0", "v2"),
    ("mul s0", "v2_mul_seed0"),
    ("mul s1", "v2_mul_seed1"),
    ("mul s2", "v2_mul_seed2"),
]

print("Dominant Q direction analysis at step 50000\n")
print(f"{'run':<10} {'σ₀/σ₁':<7} {'|v·pos₀|':<9} {'|v·pos₁|':<9} {'|v·pos₂|':<9} "
      f"{'|v·E[97]|':<10}")
print("-" * 75)

v_list = []
for label, run in runs:
    u, v, S, pos_emb, tok_emb = q_decomp(run, 50000)
    sigma_ratio = S[0] / S[1]
    # normalize pos rows
    def align(a, b):
        return abs(float(a @ b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    p0 = align(v, pos_emb[0])
    p1 = align(v, pos_emb[1])
    p2 = align(v, pos_emb[2])
    eq = align(v, tok_emb[97])
    v_list.append((label, v))
    print(f"{label:<10} {sigma_ratio:<7.1f} {p0:<9.3f} {p1:<9.3f} {p2:<9.3f} {eq:<10.3f}")

print("\nCross-seed cosine similarity of v-directions (absolute value):")
labels = [l for l, _ in v_list]
for i, (la, va) in enumerate(v_list):
    for (lb, vb) in v_list[i+1:]:
        c = abs(float(va @ vb) / (np.linalg.norm(va) * np.linalg.norm(vb)))
        print(f"  {la} vs {lb}: {c:.3f}")
