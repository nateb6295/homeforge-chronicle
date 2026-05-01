#!/usr/bin/env python3
"""runpod_lora_finetune — RunPod-side LoRA fine-tune script.

Reads a JSONL training file with {prompt, completion} examples, fine-tunes
a base model with LoRA, saves adapter to output dir.

Usage on RunPod:
    python3 /workspace/runpod_lora_finetune.py \
        --base /workspace/qwen2.5-3b-instruct \
        --train /workspace/condition_x_train.jsonl \
        --out /workspace/qwen-x-lora \
        --epochs 3 --rank 16 --alpha 32 --batch-size 4 --lr 2e-4
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, TrainingArguments
from peft import LoraConfig, get_peft_model, TaskType
from datasets import Dataset
from trl import SFTTrainer, SFTConfig


def load_data(path: str) -> Dataset:
    with open(path) as f:
        rows = [json.loads(l) for l in f if l.strip()]
    return Dataset.from_list(rows)


def format_for_sft(example, tokenizer):
    """Format prompt+completion as a single chat-template-formatted string."""
    text = tokenizer.apply_chat_template(
        [
            {"role": "user", "content": example["prompt"]},
            {"role": "assistant", "content": example["completion"]},
        ],
        tokenize=False,
        add_generation_prompt=False,
    )
    return {"text": text}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="base model path")
    ap.add_argument("--train", required=True, help="training JSONL")
    ap.add_argument("--out", required=True, help="output dir for adapter")
    ap.add_argument("--epochs", type=int, default=3)
    ap.add_argument("--rank", type=int, default=16)
    ap.add_argument("--alpha", type=int, default=32)
    ap.add_argument("--batch-size", type=int, default=4)
    ap.add_argument("--lr", type=float, default=2e-4)
    ap.add_argument("--max-seq", type=int, default=1024)
    args = ap.parse_args()

    print(f"[finetune] base={args.base}, train={args.train}, out={args.out}")
    print(f"[finetune] epochs={args.epochs}, rank={args.rank}, alpha={args.alpha}, batch={args.batch_size}, lr={args.lr}")

    tok = AutoTokenizer.from_pretrained(args.base)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token

    model = AutoModelForCausalLM.from_pretrained(args.base, dtype=torch.bfloat16, device_map="cuda")
    print(f"[finetune] model loaded, {sum(p.numel() for p in model.parameters())/1e9:.2f}B params")

    lora_cfg = LoraConfig(
        task_type=TaskType.CAUSAL_LM,
        r=args.rank,
        lora_alpha=args.alpha,
        lora_dropout=0.05,
        bias="none",
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
    )
    model = get_peft_model(model, lora_cfg)
    model.print_trainable_parameters()

    raw_dataset = load_data(args.train)
    dataset = raw_dataset.map(lambda ex: format_for_sft(ex, tok), remove_columns=raw_dataset.column_names)
    print(f"[finetune] dataset: {len(dataset)} examples")

    cfg = SFTConfig(
        output_dir=args.out,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        gradient_accumulation_steps=2,
        learning_rate=args.lr,
        bf16=True,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=1,
        max_length=args.max_seq,
        report_to="none",
        warmup_ratio=0.05,
        lr_scheduler_type="cosine",
    )

    trainer = SFTTrainer(
        model=model,
        args=cfg,
        train_dataset=dataset,
        processing_class=tok,
    )

    trainer.train()
    trainer.save_model(args.out)
    tok.save_pretrained(args.out)
    print(f"[finetune] done. adapter saved to {args.out}")


if __name__ == "__main__":
    main()
