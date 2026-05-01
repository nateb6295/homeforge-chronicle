#!/usr/bin/env python3
"""Non-modular grokking probe: "return the larger of (a, b)".

Still a 2-input task with integer output, so same architecture works.
But: not a modular arithmetic operation. Output is just max(a, b)
mapped to the same range 0..p-1.

If the distributional signature still shows up (top-0.1% holds ~50% of
grad energy, anatomy in attn.out_proj.bias + embeddings, no FFN), that
extends the claim beyond modular arithmetic. If it doesn't, the
grokking signature we've been describing is modular-arithmetic-specific.

Grokking may not happen at all for this task; that's an informative
negative result too.
"""
import argparse
import json
import time
from pathlib import Path
import torch
import torch.nn as nn
import torch.nn.functional as F

P = 97
TRAIN_FRAC = 0.30
BATCH = 512
LR = 1e-3
WD = 1.0
STEPS = 50_000
LOG_EVERY = 200
SEED = 0
OUT = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


class GrokTransformer(nn.Module):
    def __init__(self, p=P, d_model=128, n_heads=4):
        super().__init__()
        self.p = p
        self.tok_emb = nn.Embedding(p + 1, d_model)
        self.pos_emb = nn.Embedding(3, d_model)
        self.attn = nn.MultiheadAttention(d_model, n_heads, batch_first=True)
        self.ln1 = nn.LayerNorm(d_model)
        self.mlp = nn.Sequential(
            nn.Linear(d_model, 4 * d_model),
            nn.GELU(),
            nn.Linear(4 * d_model, d_model),
        )
        self.ln2 = nn.LayerNorm(d_model)
        self.out = nn.Linear(d_model, p)

    def forward(self, a, b):
        eq = torch.full_like(a, self.p)
        x = torch.stack([a, b, eq], dim=1)
        pos = torch.arange(3, device=x.device).unsqueeze(0).expand_as(x)
        h = self.tok_emb(x) + self.pos_emb(pos)
        attn_out, _ = self.attn(h, h, h)
        h = self.ln1(h + attn_out)
        h = self.ln2(h + self.mlp(h))
        return self.out(h[:, -1, :])


def build(p, seed):
    a = torch.arange(p).repeat_interleave(p)
    b = torch.arange(p).repeat(p)
    y = torch.maximum(a, b)  # non-modular, order-dependent, trivial-looking
    idx = torch.randperm(len(a), generator=torch.Generator().manual_seed(seed))
    n_train = int(TRAIN_FRAC * len(a))
    return ((a[idx[:n_train]], b[idx[:n_train]], y[idx[:n_train]]),
            (a[idx[n_train:]], b[idx[n_train:]], y[idx[n_train:]]))


def evaluate(model, data, device, batch=2048):
    a_all, b_all, y_all = data
    total_loss, total_acc, n = 0.0, 0.0, 0
    model.eval()
    with torch.no_grad():
        for i in range(0, len(a_all), batch):
            a = a_all[i:i+batch].to(device); b = b_all[i:i+batch].to(device); y = y_all[i:i+batch].to(device)
            logits = model(a, b)
            total_loss += F.cross_entropy(logits, y).item() * len(a)
            total_acc += (logits.argmax(-1) == y).float().sum().item()
            n += len(a)
    model.train()
    return total_loss / n, total_acc / n


def run(tag, steps, seed):
    torch.manual_seed(seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}  task: max(a,b), p={P}")
    train, val = build(P, seed)
    model = GrokTransformer().to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WD, betas=(0.9, 0.98))

    run_dir = OUT / tag
    run_dir.mkdir(parents=True, exist_ok=True)
    snap_dir = run_dir / "snapshots"
    snap_dir.mkdir(exist_ok=True)
    log = open(run_dir / "metrics.jsonl", "w")
    SAVE_STEPS = {500, 2000, 5000, 10000, 20000, 30000, 50000}

    probe_idx = torch.arange(min(256, len(val[0])))
    pa, pb, py = val[0][probe_idx].to(device), val[1][probe_idx].to(device), val[2][probe_idx].cpu()
    torch.save({"a": pa.cpu(), "b": pb.cpu(), "y": py}, snap_dir / "probe_inputs.pt")

    a_tr, b_tr, y_tr = train
    n_train = len(a_tr)
    start = time.time()
    for step in range(1, steps + 1):
        idx = torch.randint(0, n_train, (BATCH,))
        a, b, y = a_tr[idx].to(device), b_tr[idx].to(device), y_tr[idx].to(device)
        loss = F.cross_entropy(model(a, b), y)
        opt.zero_grad(); loss.backward(); opt.step()
        if step % LOG_EVERY == 0:
            tr_loss, tr_acc = evaluate(model, train, device)
            val_loss, val_acc = evaluate(model, val, device)
            rec = {"step": step, "train_loss": tr_loss, "train_acc": tr_acc,
                   "val_loss": val_loss, "val_acc": val_acc,
                   "elapsed_s": round(time.time() - start, 1)}
            log.write(json.dumps(rec) + "\n"); log.flush()
            if step in SAVE_STEPS:
                torch.save(model.state_dict(), snap_dir / f"step_{step:06d}.pt")
            if step % 2000 == 0:
                print(f"[{step:6d}] train={tr_acc:.3f} val={val_acc:.3f}  t={rec['elapsed_s']}s")
    log.close()
    print(f"done. {run_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default=f"v2_max_seed{SEED}")
    parser.add_argument("--steps", type=int, default=STEPS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    run(args.tag, args.steps, args.seed)
