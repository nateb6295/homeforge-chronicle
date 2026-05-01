"""
Sanity: is cross-seed power-spectrum cosine of 0.91-0.94 actually high,
or is it what you'd get from any two non-negative dense vectors?

Baselines to compare against the real cross-seed numbers:
  (1) Shuffle one spectrum — does the envelope hold after permutation?
  (2) Compare to a random-init model (never trained)
  (3) Compare to early-training (pre-grok) checkpoint
"""
import sys
from pathlib import Path
import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

BASE = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


def spec_from(run, step):
    model = GrokTransformer()
    sd = torch.load(BASE / run / "snapshots" / f"step_{step:06d}.pt",
                    map_location="cpu", weights_only=True)
    model.load_state_dict(sd)
    E = model.tok_emb.weight.data.cpu().numpy()
    return (np.abs(np.fft.rfft(E, axis=1)) ** 2).sum(axis=0)


def random_init_spec(seed):
    torch.manual_seed(seed)
    model = GrokTransformer()
    E = model.tok_emb.weight.data.cpu().numpy()
    return (np.abs(np.fft.rfft(E, axis=1)) ** 2).sum(axis=0)


def cos(a, b):
    return float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b)))


def main():
    s1 = spec_from("v2_sub_seed1", 50000)
    s2 = spec_from("v2_sub_seed2", 50000)
    a0 = spec_from("v2", 50000)

    # real cross-seed
    print(f"REAL cross-seed: sub s1 vs sub s2 = {cos(s1, s2):.3f}")
    print(f"REAL cross-task: sub s1 vs add s0 = {cos(s1, a0):.3f}")

    # shuffle baseline — permute one spectrum's frequency bins
    rng = np.random.default_rng(0)
    shuffles = []
    for _ in range(50):
        s2_shuf = rng.permutation(s2)
        shuffles.append(cos(s1, s2_shuf))
    print(f"\nSHUFFLED (50 trials): sub s1 vs shuffle(sub s2)")
    print(f"  mean = {np.mean(shuffles):.3f}  std = {np.std(shuffles):.3f}  max = {max(shuffles):.3f}")

    # random-init baseline
    r0 = random_init_spec(0)
    r1 = random_init_spec(1)
    print(f"\nRANDOM-INIT: two independent inits = {cos(r0, r1):.3f}")
    print(f"RANDOM-INIT vs trained (sub s1): {cos(r0, s1):.3f}")

    # pre-grok snapshot (same seed, early step)
    # step 1000 is well before grok on sub
    s1_early = spec_from("v2_sub_seed1", 1000)
    print(f"\nPRE-GROK: sub s1 @ step 1000 vs @ step 50000 = {cos(s1_early, s1):.3f}")
    print(f"PRE-GROK: sub s1 @ 1000 vs sub s2 @ 50000 = {cos(s1_early, s2):.3f}")


if __name__ == "__main__":
    main()
