#!/usr/bin/env python3
"""
Where do the high-sensitivity parameters LIVE?

Super Weight predicts: FFN down-projection, early layer. In my 1-layer
GrokTransformer, the down-projection is mlp.2 (the 4d_model → d_model Linear).

For each checkpoint, rank parameters by |gradient| and report which tensor
each of the top-K resides in. Also per-tensor: what fraction of that
tensor's gradient energy is in its top-0.1%.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer


RUN_DIR = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
SNAP_DIR = RUN_DIR / "snapshots"

CHECKPOINTS = [1500, 10000, 50000]


def compute_grads(step, device):
    model = GrokTransformer().to(device)
    sd = torch.load(SNAP_DIR / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()

    probe = torch.load(SNAP_DIR / "probe_inputs.pt", map_location=device, weights_only=True)
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)

    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    loss = F.cross_entropy(model(a, b), y)
    loss.backward()

    named = {}
    for name, p in model.named_parameters():
        if p.grad is not None:
            named[name] = p.grad.detach().abs().flatten().cpu().numpy()
    return named


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}\n")

    for step in CHECKPOINTS:
        grads = compute_grads(step, device)
        # flat array with (tensor_name, local_idx, value)
        entries = []
        for name, arr in grads.items():
            for i, v in enumerate(arr):
                entries.append((name, i, float(v)))
        entries.sort(key=lambda e: -e[2])

        total = sum(v for _, _, v in entries)
        n = len(entries)
        top_k = max(1, int(0.001 * n))  # top 0.1%

        print(f"=== step {step} ({n} params, top-0.1% = {top_k} params) ===")

        # Which tensors contain the top-0.1% and how many each
        tensor_counts = {}
        tensor_energy = {}
        for name, _, v in entries[:top_k]:
            tensor_counts[name] = tensor_counts.get(name, 0) + 1
            tensor_energy[name] = tensor_energy.get(name, 0.0) + v
        top_total_energy = sum(tensor_energy.values())

        # Also tensor sizes for reference
        tensor_sizes = {name: len(arr) for name, arr in grads.items()}
        tensor_total_energy = {name: float(arr.sum()) for name, arr in grads.items()}

        print(f"{'tensor':30} {'size':>8} {'top-0.1%_count':>14} {'%_of_top':>9} "
              f"{'%_of_tensor_params':>20}")
        for name in sorted(tensor_counts, key=lambda n: -tensor_counts[n]):
            cnt = tensor_counts[name]
            sz = tensor_sizes[name]
            pct_top = 100 * tensor_energy[name] / top_total_energy
            pct_params = 100 * cnt / sz
            print(f"{name:30} {sz:>8} {cnt:>14} {pct_top:>8.1f}% "
                  f"{pct_params:>19.2f}%")

        # Hero scalar: single biggest gradient
        hero = entries[0]
        hero_pct = 100 * hero[2] / total
        print(f"  hero param: {hero[0]}[{hero[1]}] |grad|={hero[2]:.4e} "
              f"({hero_pct:.2f}% of total gradient energy)")
        print()


if __name__ == "__main__":
    main()
