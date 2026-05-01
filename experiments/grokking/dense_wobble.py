"""
Dense wobble analysis — sub seed 2, steps 11000-30000 at 100-step resolution.

Question: is post-grok wobble smooth drift (uniform small steps in weight space)
or discrete hopping (spikes in step-to-step distance)?

Writes dense_wobble_sub_seed2.csv and prints summary.
"""
import torch, glob, re
from pathlib import Path

import sys
SEED = sys.argv[1] if len(sys.argv) > 1 else "2"
RUN = Path(f"/home/nate-agx/chronicle/experiments/grokking/runs/v2_sub_seed{SEED}/snapshots")
OUT = Path(f"/home/nate-agx/chronicle/experiments/grokking/dense_wobble_sub_seed{SEED}.csv")

steps = sorted(
    int(re.search(r"step_(\d+)", p.name).group(1))
    for p in RUN.glob("step_*.pt")
)
window = [s for s in steps if 11000 <= s <= 30000]
print(f"analyzing {len(window)} snapshots from step {window[0]} to {window[-1]}")

def load_flat(step):
    sd = torch.load(RUN / f"step_{step:06d}.pt", map_location="cpu", weights_only=True)
    parts = [v.flatten().float() for k, v in sd.items() if v.dtype in (torch.float32, torch.float16, torch.bfloat16)]
    return torch.cat(parts)

prev_vec = load_flat(window[0])
rows = []
for step in window[1:]:
    vec = load_flat(step)
    delta = vec - prev_vec
    l2 = delta.norm().item()
    cos = torch.nn.functional.cosine_similarity(vec.unsqueeze(0), prev_vec.unsqueeze(0)).item()
    rows.append((step, l2, cos))
    prev_vec = vec

with OUT.open("w") as f:
    f.write("step,l2_delta,cos_to_prev\n")
    for step, l2, cos in rows:
        f.write(f"{step},{l2:.6f},{cos:.8f}\n")

l2s = torch.tensor([r[1] for r in rows])
mean = l2s.mean().item()
std = l2s.std().item()
mx = l2s.max().item()
mn = l2s.min().item()
print(f"\nstep-to-step L2 delta over steps {window[0]}-{window[-1]}:")
print(f"  mean = {mean:.4f}")
print(f"  std  = {std:.4f}")
print(f"  max  = {mx:.4f}  (at step {rows[int(l2s.argmax())][0]})")
print(f"  min  = {mn:.4f}  (at step {rows[int(l2s.argmin())][0]})")
print(f"  max/mean ratio = {mx/mean:.2f}")

top5_idx = l2s.topk(5).indices.tolist()
print(f"\ntop-5 step-to-step jumps:")
for i in top5_idx:
    s, l2, cos = rows[i]
    print(f"  step {s}: L2={l2:.4f}, cos={cos:.6f}")

print(f"\nwrote {OUT}")
