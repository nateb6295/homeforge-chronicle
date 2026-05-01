"""
Generalized dense step-to-step analysis.
Usage: python3 dense_wobble_any.py <run-name>
e.g.   python3 dense_wobble_any.py v2        (add seed 0)
       python3 dense_wobble_any.py v2_sub_seed1
"""
import sys, torch, re
from pathlib import Path

RUN_NAME = sys.argv[1]
BASE = Path("/home/nate-agx/chronicle/experiments/grokking")
RUN = BASE / "runs" / RUN_NAME / "snapshots"
OUT = BASE / f"dense_wobble_{RUN_NAME}.csv"

steps = sorted(
    int(re.search(r"step_(\d+)", p.name).group(1))
    for p in RUN.glob("step_*.pt")
)
window = [s for s in steps if 11000 <= s <= 30000]
print(f"{RUN_NAME}: analyzing {len(window)} snapshots from step {window[0]} to {window[-1]}")

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
coss = torch.tensor([r[2] for r in rows])
print(f"\n{RUN_NAME} — step-to-step L2 delta:")
print(f"  mean = {l2s.mean().item():.4f}")
print(f"  std  = {l2s.std().item():.4f}")
print(f"  max  = {l2s.max().item():.4f}  (at step {rows[int(l2s.argmax())][0]})")
print(f"  max/mean ratio = {l2s.max().item()/l2s.mean().item():.2f}")
print(f"  adjacent-step cosine: mean {coss.mean().item():.6f}, min {coss.min().item():.6f}")
print(f"\nwrote {OUT}")
