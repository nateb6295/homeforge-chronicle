"""
Does initialization predict basis choice?

Hypothesis: the final preferred frequency of each neuron was already
weakly "seeded" at init — i.e., neurons that end up preferring freq f
at step 50k had above-random freq-f power at step 500.

Test: for each neuron, take its preferred frequency at step 50k. Compute
its rank (0-based) of that same frequency in its own step-500 spectrum.
Average rank across neurons tells us whether final-pref has any early
signal. If random, mean rank = (n_freqs-1)/2 ≈ 24.
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
    s = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    return s[:, 1:]  # drop DC


configs = [
    ("sub s1", "v2_sub_seed1", None),
    ("add s0", "v2", None),
    ("mul s0", "v2_mul_seed0", LOG_RI),
    ("mul s1", "v2_mul_seed1", LOG_RI),
    ("mul s2", "v2_mul_seed2", LOG_RI),
]

print("Does init predict the final preferred frequency?\n")
print(f"{'run':<10} {'n_freq':<7} {'rand_rank':<10} {'mean_rank@init':<15} {'p-val':<8}")
print("-" * 55)

for label, run, ri in configs:
    s_final = spec(run, 50000, ri)
    s_init = spec(run, 500, ri)
    n_freqs = s_final.shape[1]
    random_mean_rank = (n_freqs - 1) / 2

    # for each neuron: rank of its FINAL preferred freq within its INIT spectrum
    final_pref = np.argmax(s_final, axis=1)  # (n_neurons,)
    # argsort init spectrum descending; rank of freq f = where f sits
    order = np.argsort(-s_init, axis=1)  # (n_neurons, n_freqs)
    # find rank of final_pref[i] in order[i]
    ranks = np.zeros(s_init.shape[0], dtype=int)
    for i in range(s_init.shape[0]):
        ranks[i] = int(np.where(order[i] == final_pref[i])[0][0])
    mean_rank = ranks.mean()

    # permutation test: shuffle which final_pref is paired with which neuron
    rng = np.random.default_rng(0)
    nulls = []
    for _ in range(200):
        shuf = rng.permutation(final_pref)
        r = np.zeros_like(ranks)
        for i in range(s_init.shape[0]):
            r[i] = int(np.where(order[i] == shuf[i])[0][0])
        nulls.append(r.mean())
    nulls = np.array(nulls)
    p = float((nulls <= mean_rank).mean())

    print(f"{label:<10} {n_freqs:<7} {random_mean_rank:<10.2f} "
          f"{mean_rank:<15.2f} {p:<8.3f}")
