#!/usr/bin/env python3
"""DPO training for compression quality on Qwen 2.5 7B.

Uses pairs where chosen = high-structural-quality CCS, rejected = flat/operational CCS.
Teaches the model to preserve relational structure during compression.

Designed for RunPod A6000 (48GB). Based on phase4_5_sft_train.py architecture.

Usage:
    python3 compression_dpo_train.py --data compression_dpo_pairs.jsonl --output ./adapters/compression_dpo
"""
import argparse
import json
import os

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Path to DPO pairs JSONL")
    parser.add_argument("--output", default="./compression_dpo_adapter", help="Output dir")
    parser.add_argument("--model", default="Qwen/Qwen2.5-7B-Instruct", help="Base model")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--lr", type=float, default=5e-5)
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--beta", type=float, default=0.1, help="DPO beta parameter")
    args = parser.parse_args()

    print(f"Loading DPO pairs from {args.data}")
    pairs = []
    with open(args.data) as f:
        for line in f:
            pairs.append(json.loads(line))
    print(f"Loaded {len(pairs)} pairs")

    from datasets import Dataset
    from peft import LoraConfig
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from trl import DPOConfig, DPOTrainer

    print(f"Loading model: {args.model}")
    tokenizer = AutoTokenizer.from_pretrained(args.model)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        args.model, torch_dtype="auto", device_map="auto"
    )

    lora_config = LoraConfig(
        r=args.lora_r,
        lora_alpha=args.lora_r * 2,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
        lora_dropout=0.05,
        task_type="CAUSAL_LM",
    )

    ds_records = []
    for p in pairs:
        ds_records.append({
            "prompt": p["prompt"],
            "chosen": p["chosen"],
            "rejected": p["rejected"],
        })
    dataset = Dataset.from_list(ds_records)

    training_args = DPOConfig(
        output_dir=args.output,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        gradient_accumulation_steps=4,
        learning_rate=args.lr,
        beta=args.beta,
        logging_steps=5,
        save_strategy="epoch",
        bf16=True,
        remove_unused_columns=False,
        max_length=2048,
        max_prompt_length=256,
    )

    trainer = DPOTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        processing_class=tokenizer,
        peft_config=lora_config,
    )

    print("Starting DPO training...")
    trainer.train()

    print(f"Saving adapter to {args.output}/final")
    trainer.save_model(f"{args.output}/final")
    tokenizer.save_pretrained(f"{args.output}/final")
    print("Done.")


if __name__ == "__main__":
    main()
