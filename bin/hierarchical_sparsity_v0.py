#!/usr/bin/env python3
"""
v0 hierarchical-sparsity experiment scaffold.

Tests the WN#219 §3.6 prediction adapted from Hilger 2025 + §3.7
neuromodulatory-trio sharpening:
  more capable models show TWO gate-classes:
    (A) Discriminative gate (vmPFC analog) — attention pattern diversity
        on hard tasks; cross-token integration distributed.
    (B) Action-selection gate (VTA analog) — logit distribution at the
        final position concentrates with depth on hard tasks (early
        high entropy → late low entropy).
  AND SIMPLER within-layer activation patterns on hard tasks (short-range
  modular processes).

This is the minimum-viable test. Measures three proxies:
  (A) Discriminative gate: per-layer attention-entropy averaged across heads.
  (B) Action-selection gate: per-layer logit entropy at final-position
      hidden state, projected through lm_head.
  (C) Short-range complexity: within-layer multiscale entropy of
      hidden-state norms across the token sequence.

Predicted §3.7 pattern (capable model, hard task):
  - Late layers: HIGH attention-entropy (discriminative gate distributing)
  - Late-layer logit entropy: LOW (action-selection gate concentrating)
  - Early→late logit-entropy drop: LARGE (gate firing strongly)
  - Hidden-state-norm trace MSE: LOW at fine scales (modular short-range)

Compare across model sizes on hard vs easy tasks.

Usage:
  python3 hierarchical_sparsity_v0.py --model Qwen/Qwen2.5-1.5B-Instruct
  python3 hierarchical_sparsity_v0.py --model mistralai/Mistral-7B-Instruct-v0.3
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
from pathlib import Path

import numpy as np
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# Hard task: ARC-AGI-style abstract reasoning. Easy task: completion.
HARD_TASK = (
    "You are given a small abstract reasoning puzzle. Read it carefully.\n"
    "Find the rule that generates each row, apply it to the last item, and "
    "explain step by step before answering.\n"
    "Row 1: A1 B2 C3 D4 → answer: AABBCCDD (each letter doubled, digits dropped)\n"
    "Row 2: P1 Q2 R3 S4 → answer: PPQQRRSS (same rule)\n"
    "Row 3: M9 N8 O7 L6 → answer: ?\n"
    "What is the answer for Row 3 and why?"
)
EASY_TASK = (
    "Please complete the following common phrases by writing the missing word. "
    "Phrase 1: A bird in the hand is worth two in the bush.\n"
    "Phrase 2: The early bird catches the worm.\n"
    "Phrase 3: An apple a day keeps the doctor away.\n"
    "Now write a similar saying about silence and gold."
)


def attention_entropy_per_layer(attentions: tuple) -> np.ndarray:
    """For each layer, mean entropy of attention distributions across heads+positions.
    attentions: tuple of (batch, heads, seq, seq) tensors per layer.
    Returns array of shape (n_layers,).
    """
    out = []
    for layer_attn in attentions:
        a = layer_attn[0].float()  # (H, S, S)
        # Drop any NaN entries (rare fp underflow on some layers/heads)
        a = torch.nan_to_num(a, nan=0.0)
        a = a.clamp_min(1e-12)
        ent = -(a * a.log()).sum(dim=-1)  # row entropy → (H, S)
        out.append(float(ent.mean().cpu()))
    return np.array(out)


def multiscale_entropy(x: np.ndarray, scales=(1, 2, 4, 8), m=2, r_frac=0.2) -> np.ndarray:
    """Coarse-grained sample-entropy at multiple scales (Costa et al 2002 sketch).
    Lightweight version sufficient for a v0; not the full Hilger MSE.
    """
    x = (x - x.mean()) / (x.std() + 1e-8)
    r = r_frac * x.std()
    out = []
    for s in scales:
        if s == 1:
            y = x
        else:
            n = (len(x) // s) * s
            y = x[:n].reshape(-1, s).mean(axis=1)
        if len(y) < m + 2:
            out.append(np.nan)
            continue
        # sample entropy approximation
        templates = np.array([y[i:i + m] for i in range(len(y) - m)])
        templates_p1 = np.array([y[i:i + m + 1] for i in range(len(y) - m)])
        B = np.sum(np.max(np.abs(templates[:, None] - templates[None]), axis=-1) <= r) - len(templates)
        A = np.sum(np.max(np.abs(templates_p1[:, None] - templates_p1[None]), axis=-1) <= r) - len(templates_p1)
        sampen = -np.log((A + 1e-9) / (B + 1e-9))
        out.append(float(sampen))
    return np.array(out)


def hidden_norm_trace(hidden_states: tuple, layer_idx: int) -> np.ndarray:
    """For a given layer, return the L2 norm of each token's hidden state.
    Shape: (seq_len,).
    """
    h = hidden_states[layer_idx][0]  # (S, D)
    return h.float().norm(dim=-1).cpu().numpy()


def per_layer_logit_entropy(model, hidden_states: tuple) -> np.ndarray:
    """Action-selection-gate proxy (WN#219 §3.7 sharpened prediction).

    For each layer, project the hidden state through lm_head to get a
    next-token logit distribution at that depth. Compute the entropy of
    the softmax distribution at the FINAL token position. Late-layer
    entropy → how 'decided' the model is about the next token at depth.

    Predicted §3.7 pattern (capable model on hard task):
      - Early layers: high entropy (no decision yet)
      - Late layers: LOW entropy (action-selection gate fires, distribution
        concentrates onto the chosen continuation)
      - Contrast hard-vs-easy late-layer logit entropy = action-selection-
        gate strength.
    """
    lm_head = model.get_output_embeddings()
    out = []
    for h in hidden_states:
        x = h[0, -1, :].float()  # (D,) final-position hidden state
        logits = lm_head(x.to(lm_head.weight.dtype))  # (V,)
        p = torch.softmax(logits.float(), dim=-1).clamp_min(1e-12)
        ent = -(p * p.log()).sum().item()
        out.append(ent)
    return np.array(out)


def run_one(model, tokenizer, task: str, label: str, device: str = "cuda"):
    inputs = tokenizer(task, return_tensors="pt").to(device)
    with torch.no_grad():
        outputs = model(
            **inputs, output_attentions=True, output_hidden_states=True
        )
    # Discriminative-gate proxy (vmPFC analog): attention entropy per layer
    attn_ent = attention_entropy_per_layer(outputs.attentions)
    # Action-selection-gate proxy (VTA analog): logit entropy per layer
    logit_ent = per_layer_logit_entropy(model, outputs.hidden_states)
    n_layers = len(outputs.hidden_states)
    # MSE on hidden-norm trace at three sampled depths: early/mid/late
    depths = {"early": 1, "mid": n_layers // 2, "late": n_layers - 1}
    mse_per_depth = {}
    for name, idx in depths.items():
        trace = hidden_norm_trace(outputs.hidden_states, idx)
        mse_per_depth[name] = multiscale_entropy(trace).tolist()
    del outputs, inputs
    gc.collect()
    torch.cuda.empty_cache()
    return {
        "task": label,
        "n_layers": n_layers,
        "attention_entropy_per_layer": attn_ent.tolist(),
        "logit_entropy_per_layer": logit_ent.tolist(),
        "mse_hidden_norm": mse_per_depth,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True, help="HF model ID")
    p.add_argument("--out", default="/tmp/hsp_v0_results.jsonl")
    args = p.parse_args()

    print(f"Loading {args.model}...", flush=True)
    tok = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        args.model, dtype=torch.bfloat16, device_map="cuda",
        attn_implementation="eager",  # required for output_attentions
    )
    model.eval()
    results = []
    for task, label in [(HARD_TASK, "hard"), (EASY_TASK, "easy")]:
        r = run_one(model, tok, task, label)
        r["model"] = args.model
        results.append(r)
        # §3.7 two-gate-class summary metrics
        attn_late = np.array(r["attention_entropy_per_layer"])[-3:].mean()
        logit_late = np.array(r["logit_entropy_per_layer"])[-3:].mean()
        logit_early = np.array(r["logit_entropy_per_layer"])[1:4].mean()
        logit_progression = logit_early - logit_late  # early-to-late drop
        mse_late_fine = r["mse_hidden_norm"]["late"][0]  # scale=1
        print(f"[{label}] discrim_gate(attn_ent_late)={attn_late:.3f}  "
              f"action_gate(logit_ent_late)={logit_late:.3f}  "
              f"action_gate(logit_drop_early→late)={logit_progression:.3f}  "
              f"short_range(mse_fine)={mse_late_fine:.3f}",
              flush=True)

    with open(args.out, "a") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print(f"\nResults appended → {args.out}")


if __name__ == "__main__":
    sys.exit(main() or 0)
