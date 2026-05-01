#!/usr/bin/env python3
"""DPO training for care-template suppression on Qwen 2.5 7B Instruct.

Per Phase 1 plan from 2026-04-30:
- chosen = decisive content with care-template wrapper stripped (R1-rewritten)
- rejected = original Hermes-4 70B / Llama 3.3 70B response with wrapper

Output: LoRA adapter at /workspace/care-suppression-qwen7b-r16/
"""
import json
import os
import sys
from pathlib import Path

import torch
from datasets import Dataset
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM, AutoTokenizer
from trl import DPOConfig, DPOTrainer

PAIRS_FILE = "/workspace/care_template_dpo_run/pairs.jsonl"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
OUTPUT_DIR = "/workspace/care-suppression-qwen7b-r16"


def load_pairs(path):
    pairs = []
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            pairs.append({
                "prompt": r["prompt"],
                "chosen": r["chosen"],
                "rejected": r["rejected"],
            })
    return pairs


def format_pair(example, tokenizer):
    """Apply Qwen2.5 chat template. DPOTrainer expects prompt/chosen/rejected
    as plain strings, so we wrap each in the chat format."""
    messages_prompt = [
        {"role": "system", "content": "You are a helpful assistant. Answer the user's question."},
        {"role": "user", "content": example["prompt"]},
    ]
    prompt_str = tokenizer.apply_chat_template(messages_prompt, tokenize=False, add_generation_prompt=True)
    return {
        "prompt": prompt_str,
        "chosen": example["chosen"],
        "rejected": example["rejected"],
    }


def main():
    print(f"Loading pairs from {PAIRS_FILE}...")
    pairs = load_pairs(PAIRS_FILE)
    print(f"  {len(pairs)} pairs loaded")

    print(f"Loading tokenizer + model: {MODEL_NAME}")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        attn_implementation="flash_attention_2",
    )
    model.config.use_cache = False

    print("Wrapping with LoRA (r=16, target=q,k,v,o,gate,up,down)...")
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    print("Building dataset...")
    formatted = [format_pair(p, tokenizer) for p in pairs]
    dataset = Dataset.from_list(formatted)
    print(f"  dataset size: {len(dataset)}")
    print(f"  example prompt[:200]: {formatted[0]['prompt'][:200]}")
    print(f"  example chosen[:150]: {formatted[0]['chosen'][:150]}")

    training_args = DPOConfig(
        output_dir=OUTPUT_DIR,
        num_train_epochs=3,
        per_device_train_batch_size=2,
        gradient_accumulation_steps=4,
        learning_rate=5e-6,
        beta=0.1,
        max_length=2048,
        max_prompt_length=512,
        logging_steps=2,
        save_steps=20,
        save_total_limit=2,
        bf16=True,
        gradient_checkpointing=True,
        gradient_checkpointing_kwargs={"use_reentrant": False},
        report_to="none",
        warmup_ratio=0.1,
        lr_scheduler_type="cosine",
        remove_unused_columns=False,
    )

    trainer = DPOTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        tokenizer=tokenizer,
    )

    print("Starting training...")
    trainer.train()

    print(f"Saving final adapter to {OUTPUT_DIR}/final")
    trainer.save_model(f"{OUTPUT_DIR}/final")
    print("Done.")


if __name__ == "__main__":
    main()
