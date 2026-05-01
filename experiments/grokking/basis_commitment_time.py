"""
When does basis choice get committed?

For each intermediate step s, compute mean rank of step-50k preferred
frequency within step-s spectrum. If basis is random at step 500 but
committed by some step s*, mean rank will drop from ~23.5 to near 0
somewhere in (500, 50000).

Low rank = that neuron's final preference was already the top-ranked
frequency in its intermediate spectrum. Rank 0 = perfectly committed.
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


def spec(run, step, re_index=None):
    m = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    m.load_state_dict(sd)
    E = m.tok_emb.weight.data.numpy()
    W = m.mlp[0].weight.data.numpy()
    resp = W @ E[1:97].T
    if re_index is not None:
        resp = resp[:, re_index]
    return np.abs(np.fft.rfft(resp, axis=1))[:, 1:] ** 2


def mean_rank(s_at, final_pref):
    order = np.argsort(-s_at, axis=1)
    ranks = np.zeros(s_at.shape[0], dtype=int)
    for i in range(s_at.shape[0]):
        ranks[i] = int(np.where(order[i] == final_pref[i])[0][0])
    return ranks.mean()


configs = [
    ("sub s1", "v2_sub_seed1", None, 7600),
    ("add s0", "v2", None, 3900),
    ("mul s0", "v2_mul_seed0", LOG_RI, 3300),
]
probe_steps = [100, 500, 1000, 2000, 3000, 4000, 5000, 7000, 9000,
               11000, 15000, 20000, 30000, 50000]

print("Mean rank of step-50k preferred freq within step-s spectrum (random ≈ 23.5)\n")
print(f"{'step':<8} {'sub s1 (gk7600)':<16} {'add s0 (gk3900)':<16} {'mul s0 (gk3300)':<16}")
print("-" * 60)

tables = {label: [] for label, _, _, _ in configs}
for s in probe_steps:
    row = f"{s:<8}"
    for label, run, ri, gk in configs:
        sf = spec(run, 50000, ri)
        ss = spec(run, s, ri)
        pref = np.argmax(sf, axis=1)
        mr = mean_rank(ss, pref)
        tables[label].append((s, mr))
        row += f"{mr:<16.2f}"
    print(row)

print()
for label, rows in tables.items():
    # find step where mean rank first drops below 5 (strong commitment)
    first_commit = next((s for s, r in rows if r < 5), None)
    # rank at grok
    gk = dict(configs)[label.split()[0] + " s" + label.split()[1][1:]] if False else None
    print(f"{label}: first-step-with-rank<5 = {first_commit}")
