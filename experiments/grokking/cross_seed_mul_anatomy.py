#!/usr/bin/env python3
"""Cross-seed mul: does row 97 hero pattern reproduce across seeds?

Given seed 0 showed hero on tok_emb[97, 23] at step 20000,
seed 0 also showed row-97 knockout gave only -0.05 drop,
what do seeds 1 and 2 show?

For each seed: hero scalar at step 50000, row-97 ablation result.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer

SEEDS = [0, 1, 2]
STEP = 50000
EQ_ROW = 97


def analyze(run_dir, device):
    snap = run_dir / "snapshots"
    model = GrokTransformer().to(device)
    sd = torch.load(snap / f"step_{STEP:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()
    probe = torch.load(snap / "probe_inputs.pt", map_location=device, weights_only=True)
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)

    with torch.no_grad():
        base = (model(a, b).argmax(-1) == y).float().mean().item()

    # Hero scalar
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    loss = F.cross_entropy(model(a, b), y)
    loss.backward()
    hero_name, hero_flat_idx, hero_val = None, None, -1.0
    for name, p in model.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs().flatten()
        v, i = g.max(dim=0)
        if v.item() > hero_val:
            hero_val = v.item()
            hero_name = name
            hero_flat_idx = int(i.item())

    # Interpret hero flat index if it's tok_emb
    hero_loc = f"{hero_name}[{hero_flat_idx}]"
    if hero_name == "tok_emb.weight":
        row = hero_flat_idx // 128
        col = hero_flat_idx % 128
        hero_loc = f"tok_emb[{row},{col}]"

    # Row 97 ablation
    tok = model.tok_emb.weight
    orig = tok.data.clone()
    tok.data[EQ_ROW] = 0.0
    with torch.no_grad():
        eq_acc = (model(a, b).argmax(-1) == y).float().mean().item()
    tok.data.copy_(orig)

    # attn.out_proj.bias ablation
    for name, p in model.named_parameters():
        if name == "attn.out_proj.bias":
            ob_orig = p.data.clone()
            p.data.zero_()
            break
    with torch.no_grad():
        ob_acc = (model(a, b).argmax(-1) == y).float().mean().item()
    for name, p in model.named_parameters():
        if name == "attn.out_proj.bias":
            p.data.copy_(ob_orig)

    return dict(base=base, hero=hero_loc, hero_grad=hero_val,
                eq_ablation=eq_acc, attnb_ablation=ob_acc)


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"cross-seed mul analysis (step {STEP})\n")
    print(f"{'seed':>4}  {'base':>6}  {'hero':<30}  {'|grad|':>10}  "
          f"{'eq_abl':>7}  {'attnb_abl':>9}")
    for seed in SEEDS:
        run_dir = Path(f"/home/nate-agx/chronicle/experiments/grokking/runs/v2_mul_seed{seed}")
        if not (run_dir / "snapshots" / f"step_{STEP:06d}.pt").exists():
            print(f"{seed:>4}  [not ready]")
            continue
        r = analyze(run_dir, device)
        print(f"{seed:>4}  {r['base']:>6.4f}  {r['hero']:<30}  "
              f"{r['hero_grad']:>10.3e}  {r['eq_ablation']:>7.4f}  {r['attnb_ablation']:>9.4f}")


if __name__ == "__main__":
    main()
