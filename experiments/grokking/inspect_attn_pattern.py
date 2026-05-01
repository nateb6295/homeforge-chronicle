"""
What does the collapsed 6D attn subspace compute?

Take a trained model, run a few arithmetic prompts through, extract
the attention weights. Sequence is [a, b, 97] (97 is the = token).
Attention pattern = softmax(QK^T / sqrt(d))  — shape (1, n_heads, 3, 3).

We read FROM the final position (=) and attend TO positions (a, b, =).
If the model uses attn to gather operands, we should see high attention
weight from position 2 onto positions 0 and 1.

Also compare: attn pattern at step 500 (pre-collapse) vs step 50k (post).
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def attn_pattern(run, step, a, b):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    m.eval()

    x = torch.tensor([[a, b, P]])
    # batch_first=True so (batch, seq, dim) is correct
    with torch.no_grad():
        tok = m.tok_emb(x) + m.pos_emb(torch.arange(3).unsqueeze(0))
        out, attn = m.attn(tok, tok, tok, need_weights=True,
                           average_attn_weights=False)
    return attn.squeeze(0).numpy()  # (n_heads, 3, 3)


runs = [
    ("sub s1", "v2_sub_seed1"),
    ("add s0", "v2"),
    ("mul s0", "v2_mul_seed0"),
]
steps = [500, 2000, 5000, 50000]
probes = [(3, 7), (42, 55), (96, 1)]

for label, run in runs:
    print(f"\n=== {label} ===")
    for step in steps:
        print(f"\n  step {step}:")
        for a, b in probes:
            A = attn_pattern(run, step, a, b)
            # average over heads if multi-head
            if A.ndim == 3:
                A = A.mean(axis=0)
            # show attention FROM final position onto [a, b, =]
            last_row = np.asarray(A[-1]).flatten().astype(float)
            v0, v1, v2 = float(last_row[0]), float(last_row[1]), float(last_row[2])
            print(f"    prompt ({a},{b},=):  attn_from_last → [a={v0:.3f}, b={v1:.3f}, ={v2:.3f}]")
