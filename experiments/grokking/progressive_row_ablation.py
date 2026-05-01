#!/usr/bin/env python3
"""Progressive row ablation: how many rows does each task depend on?

For each task, zero rows of tok_emb in order of |row L2|, descending.
Measure val_acc at each step. This localizes how distributed the
tok_emb dependence is.

Also: zero rows in RANDOM order as a control. If the model depends on
any k rows out of 128, random ablation of k rows should be much less
damaging than top-|L2| k ablation (which hits the rows the model
weighted most heavily).
"""
import sys
from pathlib import Path
import torch
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

TASKS = {
    "add": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2"),
    "sub": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed0"),
    "mul": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0"),
}
STEP = 50000
VOCAB = 98  # 0..96 digits, 97 "="


def load_model(run_dir, step, device):
    snap = run_dir / "snapshots"
    model = GrokTransformer().to(device)
    sd = torch.load(snap / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()
    probe = torch.load(snap / "probe_inputs.pt", map_location=device, weights_only=True)
    return model, probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)


def acc_with_rows_zeroed(model, rows, a, b, y):
    tok = model.tok_emb.weight
    orig = tok.data.clone()
    tok.data[rows] = 0.0
    with torch.no_grad():
        pred = model(a, b).argmax(-1)
    tok.data.copy_(orig)
    return (pred == y).float().mean().item()


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}\n")

    for tag, run_dir in TASKS.items():
        print(f"=== {tag} ===")
        model, a, b, y = load_model(run_dir, STEP, device)

        # Rank rows by L2 norm
        tok = model.tok_emb.weight
        row_l2 = tok.data.pow(2).sum(dim=1).sqrt().cpu().numpy()
        ranked = np.argsort(-row_l2)  # largest first

        base = acc_with_rows_zeroed(model, [], a, b, y)
        print(f"baseline: {base:.4f}")

        ks = [1, 2, 4, 8, 16, 32]
        print(f"{'k':>3}  {'top-|L2| rows':<50}  {'val_acc':>8}  {'random val_acc (n=5)':>25}")
        for k in ks:
            top_rows = ranked[:k].tolist()
            top_acc = acc_with_rows_zeroed(model, top_rows, a, b, y)
            # Random control (5 samples of k random rows)
            rng = np.random.RandomState(0)
            rand_accs = []
            for _ in range(5):
                rand_rows = rng.choice(VOCAB, size=k, replace=False).tolist()
                rand_accs.append(acc_with_rows_zeroed(model, rand_rows, a, b, y))
            ra = np.array(rand_accs)
            # Is row 97 in the top-k?
            has_eq = " (contains =)" if 97 in top_rows else ""
            print(f"{k:>3}  {str(top_rows):<50}  {top_acc:>8.4f}  "
                  f"mean={ra.mean():.4f} min={ra.min():.4f}{has_eq}")
        print()


if __name__ == "__main__":
    main()
