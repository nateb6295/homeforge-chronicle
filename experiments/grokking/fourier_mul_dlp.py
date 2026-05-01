"""
DLP test: do mul neurons implement multiplication via log-add-exp?

If yes, re-indexing the vocab via a primitive root g of Z/97Z* should
turn mul into add. Under that indexing, token i is mapped to position
k such that g^k = i (mod 97), i.e., k = log_g(i). Then
  i * j mod 97 = g^(k_i + k_j) mod 97
which is additive in the log index.

If mul neurons are DLP-structured, their response curves over the
re-indexed vocab should show SHARP Fourier peaks (matching the add
regime). Under the original vocab indexing, the multiplicative
Fourier structure looks like noise / diffuse bins — which is exactly
what we observe.

Token 0 is problematic (0 has no log in Z/97Z*); we exclude it.
"""
import sys
from pathlib import Path
from collections import Counter
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")
P = 97


def primitive_root(p):
    """Find smallest primitive root of Z/pZ*."""
    factors = []
    phi = p - 1
    n = phi
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
    raise RuntimeError("no primitive root")


def log_table(p, g):
    """log_table[i] = k where g^k = i mod p, for i in 1..p-1."""
    logt = [None] * p
    x = 1
    for k in range(p - 1):
        logt[x] = k
        x = (x * g) % p
    return logt


def analyze(run_name, step, re_index=None):
    model = GrokTransformer()
    sd = torch.load(BASE / run_name / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    W = model.mlp[0].weight.data.cpu().numpy()
    # response over vocab, tokens 1..96 (drop 0 because no log)
    resp = W @ E[1:97].T  # (512, 96)
    if re_index is not None:
        # re_index[k] = token-id at position k in log order
        resp = resp[:, re_index]
    spec = np.abs(np.fft.rfft(resp, axis=1)) ** 2
    # drop DC
    preferred = np.argmax(spec[:, 1:], axis=1) + 1
    conc = spec.max(axis=1) / spec.sum(axis=1)
    return preferred, conc


def main():
    g = primitive_root(P)
    print(f"Primitive root of Z/{P}Z*: g = {g}\n")
    logt = log_table(P, g)
    # For re-indexing: position k (k=0..95) should hold token with log_g = k
    # i.e., token i = g^k mod P
    by_log = [pow(g, k, P) for k in range(P - 1)]
    # We want an array that when applied to resp[:, 1:97] (0-indexed tokens 1..96)
    # reorders columns so column k is the token whose log equals k.
    # Column index in resp[:, 1:97] for token t is t-1.
    re_index = np.array([t - 1 for t in by_log])

    runs = [
        ("sub s1", "v2_sub_seed1"),
        ("add s0", "v2"),
        ("mul s0", "v2_mul_seed0"),
        ("mul s1", "v2_mul_seed1"),
        ("mul s2", "v2_mul_seed2"),
    ]
    print(f"{'network':<10} {'orig-conc':<12} {'dlp-conc':<12}  top8-orig → top8-dlp")
    print("-" * 80)
    for label, run in runs:
        p_orig, c_orig = analyze(run, 50000, re_index=None)
        p_dlp, c_dlp = analyze(run, 50000, re_index=re_index)
        v_orig = Counter(p_orig.tolist()).most_common(8)
        v_dlp = Counter(p_dlp.tolist()).most_common(8)
        v_orig_s = [f for f, _ in v_orig]
        v_dlp_s = [f for f, _ in v_dlp]
        print(f"{label:<10} {c_orig.mean():<12.3f} {c_dlp.mean():<12.3f}  {v_orig_s} → {v_dlp_s}")


if __name__ == "__main__":
    main()
