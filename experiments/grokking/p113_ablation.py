#!/usr/bin/env python3
"""After p=113 training finishes, test whether row 113 (= token at p=113)
is load-bearing the way row 97 was at p=97.

Key prediction: if the "equals-token-row is load-bearing" finding is about
the equals token specifically, row 113 should drop val_acc at p=113 the
way row 97 dropped val_acc at p=97. If it was a p=97 artifact, row 113
won't be special.
"""
import sys
from pathlib import Path
import torch
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from grok_p113 import GrokTransformer, P

RUN_DIR = Path("/home/nate-agx/chronicle/experiments/grokking/runs/v2_p113_seed0")
STEP_CANDIDATES = [60000, 50000, 40000, 30000]


def pick_step(snap_dir):
    for s in STEP_CANDIDATES:
        if (snap_dir / f"step_{s:06d}.pt").exists():
            return s
    raise FileNotFoundError(f"no snapshot in {STEP_CANDIDATES}")


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    snap = RUN_DIR / "snapshots"
    step = pick_step(snap)
    print(f"p=113 analysis  step={step}  device={device}\n")

    model = GrokTransformer().to(device)
    sd = torch.load(snap / f"step_{step:06d}.pt", map_location=device, weights_only=True)
    model.load_state_dict(sd)
    model.eval()
    probe = torch.load(snap / "probe_inputs.pt", map_location=device, weights_only=True)
    a, b, y = probe["a"].to(device), probe["b"].to(device), probe["y"].to(device)

    with torch.no_grad():
        base = (model(a, b).argmax(-1) == y).float().mean().item()
    print(f"baseline val_acc: {base:.4f}")

    tok = model.tok_emb.weight
    VOCAB = tok.shape[0]  # should be 114 (P+1)
    EQ_ROW = P  # 113

    # Row 113 (= token)
    orig = tok.data.clone()
    tok.data[EQ_ROW] = 0.0
    with torch.no_grad():
        eq_acc = (model(a, b).argmax(-1) == y).float().mean().item()
    tok.data.copy_(orig)
    print(f"zero tok_emb[row {EQ_ROW} (=)]:  {eq_acc:.4f}  (Δ = {eq_acc - base:+.4f})")

    # Random digit rows
    rng = np.random.RandomState(0)
    accs = []
    for _ in range(20):
        r = int(rng.randint(0, P))
        tok.data[r] = 0.0
        with torch.no_grad():
            accs.append((model(a, b).argmax(-1) == y).float().mean().item())
        tok.data.copy_(orig)
    ra = np.array(accs)
    print(f"zero tok_emb[random digit] (n=20):  "
          f"mean={ra.mean():.4f}  min={ra.min():.4f}  max={ra.max():.4f}")

    # attn.out_proj.bias
    for name, p in model.named_parameters():
        if name == "attn.out_proj.bias":
            ob_orig = p.data.clone()
            p.data.zero_()
            with torch.no_grad():
                ob_acc = (model(a, b).argmax(-1) == y).float().mean().item()
            p.data.copy_(ob_orig)
            break
    print(f"zero attn.out_proj.bias:  {ob_acc:.4f}  (Δ = {ob_acc - base:+.4f})")


if __name__ == "__main__":
    main()
