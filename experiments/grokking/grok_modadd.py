#!/usr/bin/env python3
"""
Grokking on modular addition — minimal implementation.

Reproducing the Power-Nanda-et-al-style phase transition: a tiny transformer
learns a mod-p addition table. Training loss saturates quickly; validation
loss stays high; then — suddenly, tens of thousands of steps later — the model
grokks and generalization kicks in.

This experiment exists to put empirical teeth under Post #161 "Evals Without
Substrate." The claim there: phase transitions can happen silently under the
metrics you think you're watching. This script watches for the phase transition
by logging train/val loss every N steps.

Design:
  - Task: a + b mod p,  a, b in [0, p), p = 97
  - 1-layer transformer with learned embeddings, concat (a, b) → logits over p
  - AdamW, weight_decay = 1.0 (crucial — grokking depends on wd)
  - Train fraction: 0.30 (memorizes fast, generalizes slow)
  - Budget: up to 50k steps, checkpoint every 500, log every 100

Hardware: CUDA if available, CPU fallback. AGX should run this easily.
"""

import argparse
import json
import math
import os
import time
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F


P = 97           # prime modulus
TRAIN_FRAC = 0.30
BATCH = 512
LR = 1e-3
WD = 1.0
STEPS = 50_000
LOG_EVERY = 100
CKPT_EVERY = 500
SEED = 0

OUT = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


class GrokTransformer(nn.Module):
    def __init__(self, p=P, d_model=128, n_heads=4):
        super().__init__()
        self.p = p
        self.tok_emb = nn.Embedding(p + 1, d_model)  # p + 1 for the "=" token
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
        # Sequence: [a, b, =]
        eq = torch.full_like(a, self.p)  # "=" token
        x = torch.stack([a, b, eq], dim=1)
        pos = torch.arange(3, device=x.device).unsqueeze(0).expand_as(x)
        h = self.tok_emb(x) + self.pos_emb(pos)
        # Self-attention
        attn_out, _ = self.attn(h, h, h)
        h = self.ln1(h + attn_out)
        h = self.ln2(h + self.mlp(h))
        return self.out(h[:, -1, :])  # predict at the "=" position


def build_dataset(p):
    """All (a, b) pairs and their a+b mod p targets."""
    a = torch.arange(p).repeat_interleave(p)
    b = torch.arange(p).repeat(p)
    y = (a + b) % p
    idx = torch.randperm(len(a), generator=torch.Generator().manual_seed(SEED))
    n_train = int(TRAIN_FRAC * len(a))
    train = (a[idx[:n_train]], b[idx[:n_train]], y[idx[:n_train]])
    val = (a[idx[n_train:]], b[idx[n_train:]], y[idx[n_train:]])
    return train, val


def accuracy(logits, y):
    return (logits.argmax(-1) == y).float().mean().item()


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
            total_acc += accuracy(logits, y) * len(a)
            n += len(a)
    model.train()
    return total_loss / n, total_acc / n


def run(tag: str, steps: int):
    torch.manual_seed(SEED)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}")

    train, val = build_dataset(P)
    model = GrokTransformer().to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WD, betas=(0.9, 0.98))

    run_dir = OUT / tag
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "metrics.jsonl"
    log_file = open(log_path, "w")

    a_tr, b_tr, y_tr = train
    n_train = len(a_tr)
    start = time.time()

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
            rec = {
                "step": step,
                "train_loss": tr_loss,
                "train_acc": tr_acc,
                "val_loss": val_loss,
                "val_acc": val_acc,
                "elapsed_s": round(time.time() - start, 1),
            }
            log_file.write(json.dumps(rec) + "\n")
            log_file.flush()
            if step % 1000 == 0:
                print(f"[{step:6d}] train={tr_acc:.3f} val={val_acc:.3f} "
                      f"tr_loss={tr_loss:.4f} val_loss={val_loss:.4f} "
                      f"t={rec['elapsed_s']}s")

    log_file.close()
    print(f"done. metrics at {log_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default=f"run_{int(time.time())}")
    parser.add_argument("--steps", type=int, default=STEPS)
    args = parser.parse_args()
    run(args.tag, args.steps)
