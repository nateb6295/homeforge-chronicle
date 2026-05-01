#!/usr/bin/env python3
"""Row ablation: zero out entire rows of tok_emb.

Null result from single-scalar ablation: model robust to any 1-scalar zeroing.
So the claim "hero scalar carries the computation" is wrong.

The next-level claim to test: is the *row 97 embedding* (equals-token)
load-bearing? That's 128 scalars = the full embedding of one token.

Test:
  1. Baseline val_acc
  2. Zero entire row 97 of tok_emb (the "=" token embedding)
  3. Zero a random other row of tok_emb (e.g., a digit row)
  4. Zero the bias of attn.out_proj entirely
  5. Zero random rows of tok_emb averaged over 20 trials

If row-97 knockout is dramatically worse than random-row knockout,
the equals-token row IS the structural locus, just distributed within.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer


TASKS = {
    "add": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2"),
    "sub": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed0"),
    "mul": Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0"),
}
STEP = 50000
EQ_ROW = 97  # "=" token
DIGIT_ROWS = list(range(0, 97))


def load_model_and_probe(run_dir, step, device):
    snap = run_dir / "snapshots"
    model = GrokTransformer().to(device)
    sd = torch.load(snap / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()
    probe = torch.load(snap / "probe_inputs.pt", map_location=device, weights_only=True)
    return model, probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)


def acc(model, a, b, y):
    with torch.no_grad():
        pred = model(a, b).argmax(-1)
    return (pred == y).float().mean().item()


def zero_tok_emb_row(model, row):
    for name, p in model.named_parameters():
        if name == "tok_emb.weight":
            orig = p.data[row].clone()
            p.data[row] = 0.0
            return lambda _p=p, _r=row, _o=orig: _p.data.__setitem__(_r, _o)


def zero_attn_out_bias(model):
    for name, p in model.named_parameters():
        if name == "attn.out_proj.bias":
            orig = p.data.clone()
            p.data.zero_()
            return lambda _p=p, _o=orig: _p.data.copy_(_o)


def run_task(tag, run_dir, device):
    print(f"\n=== {tag}  step {STEP} ===")
    model, a, b, y = load_model_and_probe(run_dir, STEP, device)
    base = acc(model, a, b, y)
    print(f"baseline: {base:.4f}")

    # 1. Zero row 97 (equals token)
    restore = zero_tok_emb_row(model, EQ_ROW)
    eq_acc = acc(model, a, b, y)
    restore()
    print(f"zero tok_emb[row 97 (=)]:  {eq_acc:.4f}  (Δ = {eq_acc - base:+.4f})")

    # 2. Zero random digit rows (n=20 trials)
    rng = np.random.RandomState(0)
    digit_accs = []
    for _ in range(20):
        r = int(rng.choice(DIGIT_ROWS))
        restore = zero_tok_emb_row(model, r)
        digit_accs.append(acc(model, a, b, y))
        restore()
    da = np.array(digit_accs)
    print(f"zero tok_emb[random digit row] (n=20):  "
          f"mean={da.mean():.4f}  min={da.min():.4f}  max={da.max():.4f}")

    # 3. Zero attn.out_proj.bias entirely
    restore = zero_attn_out_bias(model)
    ab_acc = acc(model, a, b, y)
    restore()
    print(f"zero attn.out_proj.bias (all 128): {ab_acc:.4f}  (Δ = {ab_acc - base:+.4f})")


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")
    for tag, run_dir in TASKS.items():
        run_task(tag, run_dir, device)


if __name__ == "__main__":
    main()
