#!/usr/bin/env python3
"""
Evolutionary Model Forge — Domain Knowledge LoRA Training.

Trains a LoRA adapter focused on homeforge/ICP/XRPL domain knowledge.
Near-copy of train_mind_lora.py with domain-specific defaults:
  - Input: ~/domain_training.jsonl
  - Output: ~/domain-lora-adapter/
  - Checkpoints: /mnt/hdd/forge/checkpoints-domain/

Runs on AGX Orin 64GB in fp16.

Usage:
    python3 /tmp/train_domain_lora.py --pilot     # 100 samples, 1 epoch (~30m)
    python3 /tmp/train_domain_lora.py              # full run (~1-2h)
    python3 /tmp/train_domain_lora.py --resume     # resume from checkpoint
"""

import argparse
import os
import sys

# Ensure cuSPARSELt is findable
_cusparselt = os.path.expanduser("~/.local/lib/python3.10/site-packages/nvidia/cusparselt/lib")
if os.path.isdir(_cusparselt):
    os.environ.setdefault("LD_LIBRARY_PATH", "")
    if _cusparselt not in os.environ["LD_LIBRARY_PATH"]:
        os.environ["LD_LIBRARY_PATH"] = _cusparselt + ":" + os.environ["LD_LIBRARY_PATH"]

# Redirect HF cache to SSD if available
if os.path.isdir("/mnt/ssd/hf-cache"):
    os.environ.setdefault("HF_HOME", "/mnt/ssd/hf-cache")


def main():
    parser = argparse.ArgumentParser(description="Domain knowledge LoRA training for Chronicle Mind")
    parser.add_argument("--pilot", action="store_true", help="Use first 100 samples only")
    parser.add_argument("--epochs", type=int, default=1, help="Number of training epochs")
    parser.add_argument("--batch-size", type=int, default=2, help="Per-device batch size")
    parser.add_argument("--grad-accum", type=int, default=4, help="Gradient accumulation steps")
    parser.add_argument("--lr", type=float, default=2e-4, help="Learning rate")
    parser.add_argument("--rank", type=int, default=16, help="LoRA rank")
    parser.add_argument("--max-seq-len", type=int, default=512, help="Max sequence length")
    parser.add_argument("--resume", action="store_true", help="Resume from latest checkpoint")
    parser.add_argument("--input", type=str, default=os.path.expanduser("~/domain_training.jsonl"),
                        help="Training data JSONL path")
    parser.add_argument("--output", type=str, default=os.path.expanduser("~/domain-lora-adapter"),
                        help="Output adapter directory")
    parser.add_argument("--base-model", type=str, default="NousResearch/Hermes-3-Llama-3.1-8B",
                        help="Base model name or path")
    args = parser.parse_args()

    # ── Imports (slow, do after arg parse) ─────────────────────
    print("Loading libraries...")
    import torch
    import json

    if not torch.cuda.is_available():
        print("ERROR: CUDA not available. Cannot train on CPU.")
        sys.exit(1)

    device_name = torch.cuda.get_device_name(0)
    props = torch.cuda.get_device_properties(0)
    mem_gb = getattr(props, 'total_memory', getattr(props, 'total_mem', 0)) / (1024**3)
    print(f"GPU: {device_name} ({mem_gb:.1f} GB)")

    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        TrainingArguments,
    )
    from peft import LoraConfig, get_peft_model
    from trl import SFTTrainer
    try:
        from trl import SFTConfig
    except ImportError:
        SFTConfig = None
    from datasets import Dataset

    # ── Load training data ─────────────────────────────────────
    print(f"\nLoading training data from {args.input}...")
    if not os.path.isfile(args.input):
        print(f"ERROR: Training data not found: {args.input}")
        print("Run format_domain_data.py first.")
        sys.exit(1)

    examples = []
    with open(args.input, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                examples.append(json.loads(line))

    if args.pilot:
        examples = examples[:100]
        print(f"Pilot mode: using {len(examples)} samples")
    else:
        print(f"Full mode: using {len(examples)} samples")

    dataset = Dataset.from_list(examples)
    print(f"Dataset: {len(dataset)} examples")

    def format_chat(example):
        """Format messages as ChatML text for SFTTrainer."""
        text = ""
        for msg in example["messages"]:
            role = msg["role"]
            content = msg["content"]
            text += f"<|im_start|>{role}\n{content}<|im_end|>\n"
        return {"text": text}

    dataset = dataset.map(format_chat)

    # ── Load base model (fp16 — AGX has 61GB unified RAM) ─────
    print(f"\nLoading base model in fp16: {args.base_model}")

    model = AutoModelForCausalLM.from_pretrained(
        args.base_model,
        device_map="auto",
        trust_remote_code=True,
        torch_dtype=torch.float16,
    )

    tokenizer = AutoTokenizer.from_pretrained(
        args.base_model,
        trust_remote_code=True,
    )

    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right"

    # ── Prepare for LoRA ──────────────────────────────────────
    model.enable_input_require_grads()

    lora_config = LoraConfig(
        r=args.rank,
        lora_alpha=args.rank * 2,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        target_modules=[
            "q_proj", "k_proj", "v_proj", "o_proj",
            "gate_proj", "up_proj", "down_proj",
        ],
    )

    model = get_peft_model(model, lora_config)

    trainable, total = model.get_nb_trainable_parameters()
    print(f"\nModel loaded:")
    print(f"  Total params:     {total:,}")
    print(f"  Trainable params: {trainable:,} ({100 * trainable / total:.2f}%)")

    # ── Training arguments ─────────────────────────────────────
    checkpoint_dir = "/mnt/hdd/forge/checkpoints-domain"
    if not os.path.isdir("/mnt/hdd/forge"):
        checkpoint_dir = os.path.join(args.output, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)

    effective_batch = args.batch_size * args.grad_accum
    total_steps = (len(dataset) * args.epochs) // effective_batch
    warmup_steps = int(total_steps * 0.03)
    save_steps = max(total_steps // 5, 50)

    print(f"\nTraining config:")
    print(f"  Epochs:          {args.epochs}")
    print(f"  Batch size:      {args.batch_size} x {args.grad_accum} = {effective_batch} effective")
    print(f"  Learning rate:   {args.lr}")
    print(f"  LoRA rank:       {args.rank}")
    print(f"  Max seq length:  {args.max_seq_len}")
    print(f"  Total steps:     ~{total_steps}")
    print(f"  Warmup steps:    {warmup_steps}")
    print(f"  Save every:      {save_steps} steps")
    print(f"  Checkpoints:     {checkpoint_dir}")
    print(f"  Output:          {args.output}")

    if SFTConfig is not None:
        training_args = SFTConfig(
            output_dir=checkpoint_dir,
            num_train_epochs=args.epochs,
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=args.grad_accum,
            learning_rate=args.lr,
            warmup_steps=warmup_steps,
            optim="adamw_torch",
            fp16=True,
            bf16=False,
            logging_steps=10,
            save_steps=save_steps,
            save_total_limit=3,
            gradient_checkpointing=True,
            gradient_checkpointing_kwargs={"use_reentrant": False},
            max_seq_length=args.max_seq_len,
            report_to="none",
            seed=42,
            dataset_text_field=None,
        )
    else:
        training_args = TrainingArguments(
            output_dir=checkpoint_dir,
            num_train_epochs=args.epochs,
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=args.grad_accum,
            learning_rate=args.lr,
            warmup_steps=warmup_steps,
            optim="adamw_torch",
            fp16=True,
            bf16=False,
            logging_steps=10,
            save_steps=save_steps,
            save_total_limit=3,
            gradient_checkpointing=True,
            gradient_checkpointing_kwargs={"use_reentrant": False},
            report_to="none",
            seed=42,
        )

    # ── Create trainer ─────────────────────────────────────────
    trainer_kwargs = dict(
        model=model,
        args=training_args,
        train_dataset=dataset,
        max_seq_length=args.max_seq_len,
        dataset_text_field="text",
    )
    import inspect
    sft_params = inspect.signature(SFTTrainer.__init__).parameters
    if "processing_class" in sft_params:
        trainer_kwargs["processing_class"] = tokenizer
    else:
        trainer_kwargs["tokenizer"] = tokenizer

    trainer = SFTTrainer(**trainer_kwargs)

    # ── Train ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Starting DOMAIN LoRA {'pilot' if args.pilot else 'full'} training...")
    print(f"{'='*60}\n")

    if args.resume:
        checkpoints = [d for d in os.listdir(checkpoint_dir)
                      if d.startswith("checkpoint-")] if os.path.exists(checkpoint_dir) else []
        if checkpoints:
            latest = sorted(checkpoints, key=lambda x: int(x.split("-")[1]))[-1]
            resume_path = os.path.join(checkpoint_dir, latest)
            print(f"Resuming from {resume_path}")
            trainer.train(resume_from_checkpoint=resume_path)
        else:
            print("No checkpoints found, starting fresh")
            trainer.train()
    else:
        trainer.train()

    # ── Save adapter ───────────────────────────────────────────
    print(f"\nSaving adapter to {args.output}...")
    model.save_pretrained(args.output)
    tokenizer.save_pretrained(args.output)

    print(f"\n{'='*60}")
    print(f"Domain LoRA training complete!")
    print(f"Adapter saved to: {args.output}")
    print(f"{'='*60}")
    print(f"\nNext: Run merge_loras_to_base.py to create full merged model")


if __name__ == "__main__":
    main()
