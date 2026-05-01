#!/usr/bin/env python3
"""Grok addition at p=113 (next prime after 97).

Tests whether the "row 97 = equals token" finding generalizes:
at p=113, the equals token will be row 113. If the structural law
is real, zeroing row 113 should collapse val_acc the way zeroing
row 97 did at p=97. If the pattern was a p=97-specific artifact,
we'll see something different.

Same arch/hparams. Only P changes.
"""
import argparse
import json
import time
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F

P = 113
TRAIN_FRAC = 0.30
BATCH = 512
LR = 1e-3
WD = 1.0
STEPS = 60_000  # a touch longer since task is larger
LOG_EVERY = 200  # fewer snapshots to save disk
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
    y = (a + b) % p
    idx = torch.randperm(len(a), generator=torch.Generator().manual_seed(seed))
    n_train = int(TRAIN_FRAC * len(a))
    train = (a[idx[:n_train]], b[idx[:n_train]], y[idx[:n_train]])
    val = (a[idx[n_train:]], b[idx[n_train:]], y[idx[n_train:]])
    return train, val


def evaluate(model, data, device, batch=2048):
    a_all, b_all, y_all = data
    total_loss, total_acc, n = 0.0, 0.0, 0
    model.eval()
    with torch.no_grad():
        for i in range(0, len(a_all), batch):
            a = a_all[i:i+batch].to(device)
            b = b_all[i:i+batch].to(device)
            y = y_all[i:i+batch].to(device)
            logits = model(a, b)
            loss = F.cross_entropy(logits, y)
            total_loss += loss.item() * len(a)
            total_acc += (logits.argmax(-1) == y).float().sum().item()
            n += len(a)
    model.train()
    return total_loss / n, total_acc / n


def run(tag, steps, seed):
    torch.manual_seed(seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}  task: (a+b) mod {P}")
    train, val = build(P, seed)
    model = GrokTransformer().to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WD, betas=(0.9, 0.98))

    run_dir = OUT / tag
    run_dir.mkdir(parents=True, exist_ok=True)
    snap_dir = run_dir / "snapshots"
    snap_dir.mkdir(exist_ok=True)
    log_path = run_dir / "metrics.jsonl"
    log_file = open(log_path, "w")

    fixed_val_idx = torch.arange(min(256, len(val[0])))
    fixed_val_a = val[0][fixed_val_idx].to(device)
    fixed_val_b = val[1][fixed_val_idx].to(device)
    torch.save({"a": fixed_val_a.cpu(), "b": fixed_val_b.cpu(),
                "y": val[2][fixed_val_idx].cpu()},
               snap_dir / "probe_inputs.pt")

    a_tr, b_tr, y_tr = train
    n_train = len(a_tr)
    start = time.time()

    # Keep only a handful of snapshots to save disk
    SAVE_STEPS = {500, 2000, 5000, 10000, 20000, 30000, 40000, 50000, 60000}

    for step in range(1, steps + 1):
        idx = torch.randint(0, n_train, (BATCH,))
        a = a_tr[idx].to(device)
        b = b_tr[idx].to(device)
        y = y_tr[idx].to(device)
        logits = model(a, b)
        loss = F.cross_entropy(logits, y)
        opt.zero_grad()
        loss.backward()
        opt.step()

        if step % LOG_EVERY == 0:
            tr_loss, tr_acc = evaluate(model, train, device)
            val_loss, val_acc = evaluate(model, val, device)
            rec = {"step": step, "train_loss": tr_loss, "train_acc": tr_acc,
                   "val_loss": val_loss, "val_acc": val_acc,
                   "elapsed_s": round(time.time() - start, 1)}
            log_file.write(json.dumps(rec) + "\n")
            log_file.flush()
            if step in SAVE_STEPS:
                torch.save(model.state_dict(), snap_dir / f"step_{step:06d}.pt")
            if step % 2000 == 0:
                print(f"[{step:6d}] train={tr_acc:.3f} val={val_acc:.3f} "
                      f"tr_loss={tr_loss:.4f} val_loss={val_loss:.4f} "
                      f"t={rec['elapsed_s']}s")

    log_file.close()
    print(f"done. metrics at {log_path}, snapshots at {snap_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default=f"v2_p113_seed{SEED}")
    parser.add_argument("--steps", type=int, default=STEPS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    run(args.tag, args.steps, args.seed)
