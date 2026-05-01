#!/usr/bin/env python3
"""Single-scalar ablation: causally test the hero scalar.

For each task's post-grok checkpoint:
  1. Load model. Measure val_acc baseline.
  2. Re-run anatomy to find the current hero scalar.
  3. Zero out ONLY that scalar. Measure val_acc.
  4. As control, zero out a random scalar of similar magnitude.
  5. As further control, zero out 10 random scalars.
  6. Report delta.

If zeroing one scalar collapses accuracy, the hero framing is causal.
If it doesn't, the hero is correlational and the distributed concentration
(top-0.1%) is what matters.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer


TASKS = {
    "add":  Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2"),
    "sub":  Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed0"),
    "mul":  Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed0"),
}
STEP = 50000  # add+mul have it; sub has 50000 too


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
        logits = model(a, b)
        pred = logits.argmax(-1)
    return (pred == y).float().mean().item()


def find_hero(model, a, b, y):
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    loss = F.cross_entropy(model(a, b), y)
    loss.backward()
    best_name, best_idx, best_val = None, None, -1.0
    for name, p in model.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs().flatten()
        v, i = g.max(dim=0)
        if v.item() > best_val:
            best_val = v.item()
            best_name = name
            best_idx = int(i.item())
    return best_name, best_idx, best_val


def zero_param(model, name, flat_idx):
    """Return a function that restores the original value."""
    for pname, p in model.named_parameters():
        if pname == name:
            orig_shape = p.shape
            flat = p.data.view(-1)
            orig_val = flat[flat_idx].item()
            flat[flat_idx] = 0.0
            def restore(_flat=flat, _idx=flat_idx, _val=orig_val):
                _flat[_idx] = _val
            return restore, orig_val
    raise KeyError(name)


def random_scalar_ablation(model, a, b, y, rng, exclude=None):
    """Zero out a random scalar (not in exclude set) and return acc."""
    names_params = [(n, p) for n, p in model.named_parameters()]
    name, param = names_params[rng.randint(0, len(names_params)-1)]
    flat = param.data.view(-1)
    n = flat.shape[0]
    while True:
        idx = rng.randint(0, n-1)
        if exclude is None or (name, idx) not in exclude:
            break
    restore, orig = zero_param(model, name, idx)
    a_v = acc(model, a, b, y)
    restore()
    return (name, idx, orig), a_v


def run_task(tag, run_dir, device):
    print(f"\n=== task: {tag}  run: {run_dir.name}  step: {STEP} ===")
    model, a, b, y = load_model_and_probe(run_dir, STEP, device)

    base = acc(model, a, b, y)
    print(f"baseline val_acc:  {base:.4f}")

    hero_name, hero_idx, hero_grad = find_hero(model, a, b, y)
    print(f"hero scalar:       {hero_name}[{hero_idx}]  |grad|={hero_grad:.4e}")

    # get hero value and magnitude for reporting
    for pname, p in model.named_parameters():
        if pname == hero_name:
            hero_val = p.data.view(-1)[hero_idx].item()
            break
    print(f"hero value:        {hero_val:+.4f}")

    # 1) Zero the hero
    restore, _ = zero_param(model, hero_name, hero_idx)
    hero_acc = acc(model, a, b, y)
    restore()
    print(f"zero hero scalar:  val_acc = {hero_acc:.4f}  (Δ = {hero_acc - base:+.4f})")

    # 2) Random scalar controls (20 samples)
    rng = np.random.RandomState(0)
    rand_accs = []
    for _ in range(20):
        _, racc = random_scalar_ablation(model, a, b, y, rng, exclude={(hero_name, hero_idx)})
        rand_accs.append(racc)
    rand_mean = float(np.mean(rand_accs))
    rand_min = float(np.min(rand_accs))
    rand_max = float(np.max(rand_accs))
    print(f"random-1 (n=20):   mean={rand_mean:.4f}  min={rand_min:.4f}  max={rand_max:.4f}")

    # 3) Same-magnitude random control: pick a non-hero scalar whose |value|
    #    is closest to |hero_val|, zero it, measure
    closest_name, closest_idx, closest_dist = None, None, float("inf")
    hero_abs = abs(hero_val)
    for pname, p in model.named_parameters():
        flat = p.data.view(-1)
        dists = (flat.abs() - hero_abs).abs()
        if pname == hero_name:
            dists[hero_idx] = float("inf")  # exclude hero itself
        v, i = dists.min(dim=0)
        if v.item() < closest_dist:
            closest_dist = v.item()
            closest_name = pname
            closest_idx = int(i.item())
    restore, closest_val = zero_param(model, closest_name, closest_idx)
    closest_acc = acc(model, a, b, y)
    restore()
    print(f"same-|val| ctrl:   {closest_name}[{closest_idx}] val={closest_val:+.4f} "
          f"val_acc = {closest_acc:.4f}  (Δ = {closest_acc - base:+.4f})")


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")
    for tag, run_dir in TASKS.items():
        run_task(tag, run_dir, device)


if __name__ == "__main__":
    main()
