"""
Concentration trajectory in the OPERATION-MATCHED basis.

For each task, measure mean MLP-neuron concentration in its natural
group basis (additive for sub/add, multiplicative via log for mul)
across training steps. If the Fourier-circuit formation is universal,
this curve should rise through grok for all three tasks — unlike
the mixed-basis version which was flat for mul.
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P, G = 97, 5
LOG_RI = np.array([pow(G, k, P) - 1 for k in range(P - 1)])

STEPS = [500, 1000, 2000, 3000, 5000, 7000, 9000, 11000, 15000, 20000, 30000, 50000]


def conc(run, step, re_index=None):
    model = GrokTransformer()
    path = BASE / run / "snapshots" / f"step_{step:06d}.pt"
    if not path.exists():
        return None
    sd = torch.load(path, map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    W = model.mlp[0].weight.data.cpu().numpy()
    resp = W @ E[1:97].T
    if re_index is not None:
        resp = resp[:, re_index]
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    return float((spec.max(axis=1) / spec.sum(axis=1)).mean())


configs = [
    ("sub s1", "v2_sub_seed1", None, 7600),
    ("add s0", "v2", None, 3900),
    ("mul s0", "v2_mul_seed0", LOG_RI, 3300),
    ("mul s1", "v2_mul_seed1", LOG_RI, None),
    ("mul s2", "v2_mul_seed2", LOG_RI, None),
]

print(f"{'step':<8}" + "".join(f"{l:<14}" for l, _, _, _ in configs))
print("-" * 80)
for step in STEPS:
    row = f"{step:<8}"
    for label, run, ri, _ in configs:
        c = conc(run, step, ri)
        row += f"{c:.3f}         "[:14] if c is not None else "n/a           "
    print(row)

print("\nGrok steps (val_acc >= 0.95):")
for label, _, _, g in configs:
    print(f"  {label}: step {g}" if g else f"  {label}: (not looked up)")
