#!/usr/bin/env python3
"""
Modular SUBTRACTION variant of grok_v2.

Same architecture, same hyperparams, target = (a - b) mod p instead of
(a + b) mod p. Non-commutative — tests whether concentration is
task-specific or general.
"""
import argparse
import json
import os
import time
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F

import sys
sys.path.insert(0, str(Path(__file__).parent))
from grok_v2 import GrokTransformer, evaluate, accuracy

P = 97
TRAIN_FRAC = 0.30
BATCH = 512
LR = 1e-3
WD = 1.0
STEPS = 50_000
LOG_EVERY = 100
SEED = 0

OUT = Path("/home/nate-agx/chronicle/experiments/grokking/runs")


def build_sub_dataset(p, seed=SEED):
    a = torch.arange(p).repeat_interleave(p)
    b = torch.arange(p).repeat(p)
    y = (a - b) % p
    idx = torch.randperm(len(a), generator=torch.Generator().manual_seed(seed))
    n_train = int(TRAIN_FRAC * len(a))
    train = (a[idx[:n_train]], b[idx[:n_train]], y[idx[:n_train]])
    val = (a[idx[n_train:]], b[idx[n_train:]], y[idx[n_train:]])
    return train, val


def run(tag, steps, seed):
    torch.manual_seed(seed)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"device: {device}  task: (a-b) mod {P}")

    train, val = build_sub_dataset(P, seed)
    model = GrokTransformer().to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WD, betas=(0.9, 0.98))

    run_dir = OUT / tag
    run_dir.mkdir(parents=True, exist_ok=True)
    snap_dir = run_dir / "snapshots"
    snap_dir.mkdir(exist_ok=True)
    log_path = run_dir / "metrics.jsonl"
    log_file = open(log_path, "w")

    fixed_val_idx = torch.arange(256)
    fixed_val_a = val[0][fixed_val_idx].to(device)
    fixed_val_b = val[1][fixed_val_idx].to(device)
    torch.save({"a": fixed_val_a.cpu(), "b": fixed_val_b.cpu(),
                "y": val[2][fixed_val_idx].cpu()},
               snap_dir / "probe_inputs.pt")

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
            rec = {"step": step, "train_loss": tr_loss, "train_acc": tr_acc,
                   "val_loss": val_loss, "val_acc": val_acc,
                   "elapsed_s": round(time.time() - start, 1)}
            log_file.write(json.dumps(rec) + "\n")
            log_file.flush()
            torch.save(model.state_dict(), snap_dir / f"step_{step:06d}.pt")
            model.eval()
            with torch.no_grad():
                probe_logits = model(fixed_val_a, fixed_val_b).cpu()
            model.train()
            torch.save(probe_logits, snap_dir / f"probe_{step:06d}.pt")
            if step % 1000 == 0:
                print(f"[{step:6d}] train={tr_acc:.3f} val={val_acc:.3f} "
                      f"tr_loss={tr_loss:.4f} val_loss={val_loss:.4f} "
                      f"t={rec['elapsed_s']}s")

    log_file.close()
    print(f"done. metrics at {log_path}, snapshots at {snap_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default=f"v2_sub_seed{SEED}")
    parser.add_argument("--steps", type=int, default=STEPS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    run(args.tag, args.steps, args.seed)
