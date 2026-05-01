#!/bin/bash
# Second overnight chain — kicks off after sub seeds 1 & 2 and sort seed 0 finish.
# Adds: 2-layer addition, cross-seed sub anatomy, sort anatomy
set -u
cd /home/nate-agx/chronicle/experiments/grokking
mkdir -p overnight

WAIT_FOR=(152950 152951 153203)  # sub s1, sub s2, sort s0

echo "=== chain2 start $(date) ===" > overnight/chain2.log
for pid in "${WAIT_FOR[@]}"; do
  while kill -0 "$pid" 2>/dev/null; do sleep 30; done
  echo "pid $pid finished $(date)" >> overnight/chain2.log
done

echo "=== 2-layer addition training $(date) ===" >> overnight/chain2.log
python3 grok_2layer.py --tag v2_2layer_seed0 --seed 0 >> overnight/chain2.log 2>&1

echo "=== cross-seed sub anatomy $(date) ===" >> overnight/chain2.log
python3 anatomy_sub.py >> overnight/chain2.log 2>&1 || true
# Re-run make_figures with sub s1, s2 in the mix
python3 make_figures.py >> overnight/chain2.log 2>&1

echo "=== sort anatomy (non-modular) $(date) ===" >> overnight/chain2.log
# Custom one-shot
python3 -c "
import sys
sys.path.insert(0, '/home/nate-agx/chronicle/experiments/grokking')
from pathlib import Path
import numpy as np, torch, torch.nn.functional as F
from grok_sort import GrokTransformer
RUN = Path('/home/nate-agx/chronicle/experiments/grokking/runs/v2_sort_seed0')
m = GrokTransformer()
m.load_state_dict(torch.load(RUN/'snapshots'/'step_050000.pt', map_location='cpu', weights_only=True))
probe = torch.load(RUN/'snapshots'/'probe_inputs.pt', map_location='cpu', weights_only=True)
for p in m.parameters(): p.requires_grad_(True)
m.zero_grad()
loss = F.cross_entropy(m(probe['a'], probe['b']), probe['y'])
loss.backward()
per_tensor = {}
for name, p in m.named_parameters():
    if p.grad is not None: per_tensor[name] = p.grad.detach().abs().flatten().numpy()
all_g = np.concatenate(list(per_tensor.values()))
print('=== sort (max(a,b)) anatomy, step 50k ===')
print(f'loss={loss.item():.4f}')
k = max(1, int(len(all_g)*0.001))
print(f'top-0.1% share of L1: {float(np.sort(all_g)[::-1][:k].sum()/all_g.sum()):.3f}')
print(f'max/mean: {float(all_g.max()/all_g.mean()):.0f}')
mlp_l1 = sum(g.sum() for n,g in per_tensor.items() if 'mlp' in n)
total_l1 = sum(g.sum() for g in per_tensor.values())
print(f'MLP L1 share: {mlp_l1/total_l1:.3f}')
for name in sorted(per_tensor, key=lambda n: -per_tensor[n].sum())[:8]:
    g = per_tensor[name]
    print(f'  {name:40s} L1_share={g.sum()/total_l1:.3f}  max={g.max():.2e}')
" >> overnight/chain2.log 2>&1

touch overnight/CHAIN2_COMPLETE
echo "=== chain2 complete $(date) ===" >> overnight/chain2.log
