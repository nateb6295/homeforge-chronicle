#!/usr/bin/env python3
"""
Channel ablation test.

The d_model=128 hidden dimension is the axis shared by tok_emb (98,128),
pos_emb (3,128), attn.out_proj.bias (128,), attn.out_proj.weight (128,128),
mlp input/output, etc. Super Weight showed that function-carrying
scalars are often slice of a coordinated channel.

Test:
  1. For each channel c in 0..127, compute total |grad| across ALL
     parameters whose last-axis index is c (plus the bias[c]).
  2. Rank channels by that total.
  3. Ablate top-k channels (zero out all params sharing that column index).
  4. Compare to random-k channels.

If the top channels are the load-bearing ones, top-k ablation should
collapse val_acc while random-k barely moves it.
"""
import sys
from pathlib import Path
import torch
import torch.nn.functional as F
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer, build_dataset, P


RUN_DIR = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2")
SNAP_DIR = RUN_DIR / "snapshots"
D_MODEL = 128


def load_model(step, device):
    m = GrokTransformer().to(device)
    sd = torch.load(SNAP_DIR / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    m.load_state_dict(sd)
    return m


def eval_model(model, data, device, batch=2048):
    a_all, b_all, y_all = data
    correct, n = 0, 0
    model.eval()
    with torch.no_grad():
        for i in range(0, len(a_all), batch):
            a = a_all[i:i+batch].to(device)
            b = b_all[i:i+batch].to(device)
            y = y_all[i:i+batch].to(device)
            logits = model(a, b)
            correct += (logits.argmax(-1) == y).sum().item()
            n += len(a)
    return correct / n


def channel_gradient_energy(model, probe, device):
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)
    for p in model.parameters():
        p.requires_grad_(True)
    model.zero_grad()
    loss = F.cross_entropy(model(a, b), y)
    loss.backward()

    # Aggregate |grad| per d_model channel. For tensors with last dim = D_MODEL,
    # sum over all other dims. For bias vectors of size D_MODEL, use directly.
    channel_energy = np.zeros(D_MODEL)
    for name, p in model.named_parameters():
        if p.grad is None:
            continue
        g = p.grad.detach().abs()
        if g.dim() == 1 and g.shape[0] == D_MODEL:
            channel_energy += g.cpu().numpy()
        elif g.dim() >= 2 and g.shape[-1] == D_MODEL:
            channel_energy += g.view(-1, D_MODEL).sum(dim=0).cpu().numpy()
        # Skip tensors that don't have a D_MODEL axis (e.g. out.weight is (p, d_model) — has it)
    return channel_energy


def zero_channels(model, channels):
    """Zero out all parameters that share these d_model column indices."""
    chan_set = set(channels)
    with torch.no_grad():
        for name, p in model.named_parameters():
            if p.dim() == 1 and p.shape[0] == D_MODEL:
                for c in chan_set:
                    p[c] = 0.0
            elif p.dim() >= 2 and p.shape[-1] == D_MODEL:
                v = p.view(-1, D_MODEL)
                for c in chan_set:
                    v[:, c] = 0.0
            elif p.dim() >= 2 and p.shape[0] == D_MODEL:
                # e.g. attn.in_proj matrices where d_model is first dim of some slice
                # be conservative: only zero if last-dim matches (handled above)
                pass


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")
    _, val = build_dataset(P)
    probe = torch.load(SNAP_DIR / "probe_inputs.pt", map_location=device, weights_only=True)

    for step in [1500, 10000, 50000]:
        print(f"\n=== step {step} ===")
        m = load_model(step, device)
        base_acc = eval_model(m, val, device)
        print(f"Baseline val_acc: {base_acc:.4f}")

        energy = channel_gradient_energy(m, probe, device)
        ranked = np.argsort(-energy)
        print(f"Top 10 channels by |grad| energy:")
        for c in ranked[:10]:
            print(f"  ch{c}: energy={energy[c]:.4e}")

        rng = np.random.default_rng(42)
        print(f"\n{'k':>3} {'top-k_val':>10} {'random-k_val':>14}")
        for k in [1, 2, 4, 8, 16, 32]:
            m_top = load_model(step, device)
            zero_channels(m_top, ranked[:k].tolist())
            top_acc = eval_model(m_top, val, device)

            rand_chans = rng.choice(D_MODEL, size=k, replace=False).tolist()
            m_rand = load_model(step, device)
            zero_channels(m_rand, rand_chans)
            rand_acc = eval_model(m_rand, val, device)

            print(f"{k:>3} {top_acc:>10.4f} {rand_acc:>14.4f}")


if __name__ == "__main__":
    main()
