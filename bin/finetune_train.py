#!/usr/bin/env python3
"""
Chronicle Fine-Tuning Training — Objective #7

Trains a LoRA adapter on Chronicle's extracted knowledge using QLoRA.
Designed for Jetson AGX Orin (65.9 GB unified memory, compute 8.7).

Base model: Qwen2.5-3B-Instruct (configurable)
Method: QLoRA (4-bit quantized base + LoRA adapters)
Data: Output from finetune_data.py

Usage:
  python3 finetune_train.py train [--base-model MODEL] [--dataset DIR] [--output DIR]
  python3 finetune_train.py merge [--adapter DIR] [--output DIR]
  python3 finetune_train.py test [--adapter DIR] [--prompt TEXT]

Environment:
  HF_HOME=/mnt/hdd/huggingface  (auto-set, keeps eMMC clear)
"""

import json
import os
import sys
from pathlib import Path

# Redirect HuggingFace cache to HDD
os.environ["HF_HOME"] = "/mnt/hdd/huggingface"

DEFAULT_BASE_MODEL = "Qwen/Qwen2.5-3B-Instruct"
DEFAULT_DATASET = "/mnt/hdd/chronicle-finetune"
DEFAULT_OUTPUT = "/mnt/hdd/chronicle-finetune/adapters"

# QLoRA config tuned for Jetson AGX Orin
LORA_R = 16
LORA_ALPHA = 32
LORA_DROPOUT = 0.05
LORA_TARGET_MODULES = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]

# Training hyperparameters
BATCH_SIZE = 1
GRADIENT_ACCUMULATION = 16  # effective batch = 16
LEARNING_RATE = 2e-4
NUM_EPOCHS = 1  # Start with 1 epoch as proof-of-concept; increase for production
MAX_SEQ_LENGTH = 1024  # Reduced from 2048 — Gemma 27B fp16 OOMs on A100 80GB at 2048
WARMUP_RATIO = 0.03
WEIGHT_DECAY = 0.01
LOGGING_STEPS = 10
SAVE_STEPS = 25
RESUME_FROM_CHECKPOINT = True  # Auto-resume from last checkpoint if available


def load_dataset_from_jsonl(dataset_dir):
    """Load JSONL training data into HuggingFace Dataset format."""
    from datasets import Dataset

    combined_path = Path(dataset_dir) / "combined.jsonl"
    if not combined_path.exists():
        print(f"Error: {combined_path} not found. Run finetune_data.py extract first.")
        sys.exit(1)

    examples = []
    with open(combined_path) as f:
        for line in f:
            ex = json.loads(line)
            examples.append(ex)

    # Filter examples that are too long (rough token estimate)
    filtered = []
    skipped = 0
    for ex in examples:
        total_chars = sum(len(m["content"]) for m in ex["messages"])
        if total_chars // 4 > MAX_SEQ_LENGTH * 1.5:
            skipped += 1
            continue
        filtered.append(ex)

    if skipped:
        print(f"  Filtered {skipped} examples exceeding {MAX_SEQ_LENGTH} tokens")

    # Convert to Dataset format
    dataset = Dataset.from_list([
        {"messages": ex["messages"]} for ex in filtered
    ])

    return dataset


def format_messages_for_training(example, tokenizer):
    """Format ChatML messages into a single training string."""
    text = tokenizer.apply_chat_template(
        example["messages"],
        tokenize=False,
        add_generation_prompt=False,
    )
    return {"text": text}


def train(base_model=DEFAULT_BASE_MODEL, dataset_dir=DEFAULT_DATASET, output_dir=DEFAULT_OUTPUT):
    """Run LoRA fine-tuning (fp16 on Jetson — bitsandbytes 4bit not compatible with sm_87)."""
    import torch
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
    )
    from peft import LoraConfig, get_peft_model
    try:
        from trl import SFTTrainer, SFTConfig
        USE_SFT_CONFIG = True
    except ImportError:
        from trl import SFTTrainer
        from transformers import TrainingArguments
        USE_SFT_CONFIG = False

    print(f"Chronicle Fine-Tuning")
    print(f"  Base model: {base_model}")
    print(f"  Dataset: {dataset_dir}")
    print(f"  Output: {output_dir}")
    print(f"  Method: LoRA fp16 (r={LORA_R}, alpha={LORA_ALPHA})")
    print(f"  Max seq length: {MAX_SEQ_LENGTH}")
    print(f"  Note: Using fp16 instead of 4-bit (bitsandbytes incompatible with Jetson sm_87)")
    print()

    # Load dataset
    print("Loading dataset...")
    dataset = load_dataset_from_jsonl(dataset_dir)
    print(f"  {len(dataset)} training examples")

    # Split: 95% train, 5% eval
    split = dataset.train_test_split(test_size=0.05, seed=42)
    train_dataset = split["train"]
    eval_dataset = split["test"]
    print(f"  Train: {len(train_dataset)}, Eval: {len(eval_dataset)}")

    # Load tokenizer
    print(f"\nLoading tokenizer from {base_model}...")
    tokenizer = AutoTokenizer.from_pretrained(
        base_model,
        trust_remote_code=True,
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right"

    # Format dataset
    print("Formatting dataset...")
    train_dataset = train_dataset.map(
        lambda ex: format_messages_for_training(ex, tokenizer),
        remove_columns=train_dataset.column_names,
    )
    eval_dataset = eval_dataset.map(
        lambda ex: format_messages_for_training(ex, tokenizer),
        remove_columns=eval_dataset.column_names,
    )

    # Load model in fp16 (Jetson has 65.9 GB unified — 3B model needs ~6 GB in fp16)
    # Force CUDA placement — device_map="auto" sometimes puts on CPU on Jetson
    print(f"Loading model {base_model} (fp16)...")
    torch.cuda.empty_cache()
    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        device_map={"": "cuda:0"},
        trust_remote_code=True,
        torch_dtype=torch.float16,
    )
    model.config.use_cache = False
    model.enable_input_require_grads()

    # LoRA config
    lora_config = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        lora_dropout=LORA_DROPOUT,
        target_modules=LORA_TARGET_MODULES,
        bias="none",
        task_type="CAUSAL_LM",
    )

    model = get_peft_model(model, lora_config)
    trainable, total = model.get_nb_trainable_parameters()
    print(f"  Trainable parameters: {trainable:,} / {total:,} ({100 * trainable / total:.2f}%)")
    print(f"  GPU memory: {torch.cuda.memory_allocated() / 1e9:.2f} GB")

    # Training arguments
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Training config — compatible with both trl<1.0 and trl>=1.0
    common_args = dict(
        output_dir=str(output_path),
        num_train_epochs=NUM_EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=GRADIENT_ACCUMULATION,
        learning_rate=LEARNING_RATE,
        warmup_steps=max(1, int(0.03 * len(train_dataset) // (BATCH_SIZE * GRADIENT_ACCUMULATION))),
        weight_decay=WEIGHT_DECAY,
        logging_steps=LOGGING_STEPS,
        save_steps=SAVE_STEPS,
        save_total_limit=3,
        eval_strategy="steps",
        eval_steps=SAVE_STEPS,
        fp16=True,
        optim="adamw_torch",
        gradient_checkpointing=True,
        max_grad_norm=0.3,
        report_to="none",
        dataloader_pin_memory=False,
    )

    # Gemma 4 requires token_type_ids (multimodal arch, 0=text).
    # Wrap the default collator to inject them.
    from transformers import DataCollatorForLanguageModeling
    base_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False)

    class Gemma4Collator:
        """Adds token_type_ids=0 for text-only training on Gemma 4."""
        def __init__(self, base):
            self.base = base
        def __call__(self, features):
            batch = self.base(features)
            if "token_type_ids" not in batch:
                batch["token_type_ids"] = torch.zeros_like(batch["input_ids"])
            return batch

    collator = Gemma4Collator(base_collator) if "gemma4" in base_model or "gemma-4" in base_model else None

    if USE_SFT_CONFIG:
        # trl >= 1.0: use SFTConfig (includes max_length and dataset_text_field)
        training_args = SFTConfig(
            **common_args,
            max_length=MAX_SEQ_LENGTH,
            dataset_text_field="text",
        )
        trainer = SFTTrainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            processing_class=tokenizer,
            data_collator=collator,
        )
    else:
        # trl < 1.0: use TrainingArguments + SFTTrainer kwargs
        training_args = TrainingArguments(**common_args)
        trainer = SFTTrainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            processing_class=tokenizer,
            max_seq_length=MAX_SEQ_LENGTH,
            dataset_text_field="text",
            data_collator=collator,
        )

    # Train
    print("\nStarting training...")
    print(f"  Effective batch size: {BATCH_SIZE * GRADIENT_ACCUMULATION}")
    print(f"  Steps per epoch: ~{len(train_dataset) // (BATCH_SIZE * GRADIENT_ACCUMULATION)}")
    print(f"  Total steps: ~{NUM_EPOCHS * len(train_dataset) // (BATCH_SIZE * GRADIENT_ACCUMULATION)}")
    print()

    # Check for checkpoint to resume from
    resume_checkpoint = None
    if RESUME_FROM_CHECKPOINT:
        checkpoints = sorted(output_path.glob("checkpoint-*"), key=lambda p: int(p.name.split("-")[1]))
        if checkpoints:
            resume_checkpoint = str(checkpoints[-1])
            print(f"  Resuming from checkpoint: {resume_checkpoint}")

    trainer.train(resume_from_checkpoint=resume_checkpoint)

    # Save final adapter
    final_path = output_path / "chronicle-specialist"
    trainer.save_model(str(final_path))
    tokenizer.save_pretrained(str(final_path))

    print(f"\nTraining complete!")
    print(f"  Adapter saved to: {final_path}")
    print(f"  Base model: {base_model}")
    print(f"  To test: python3 finetune_train.py test --adapter {final_path}")


def merge_adapter(adapter_dir=None, output_dir=None):
    """Merge LoRA adapter with base model for standalone deployment."""
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from peft import PeftModel

    adapter_path = Path(adapter_dir or f"{DEFAULT_OUTPUT}/chronicle-specialist")
    output_path = Path(output_dir or "/mnt/hdd/chronicle-finetune/merged")

    if not adapter_path.exists():
        print(f"Error: adapter not found at {adapter_path}")
        sys.exit(1)

    # Load adapter config to find base model
    config_path = adapter_path / "adapter_config.json"
    with open(config_path) as f:
        config = json.load(f)
    base_model = config.get("base_model_name_or_path", DEFAULT_BASE_MODEL)

    print(f"Merging adapter into base model...")
    print(f"  Base: {base_model}")
    print(f"  Adapter: {adapter_path}")
    print(f"  Output: {output_path}")

    tokenizer = AutoTokenizer.from_pretrained(str(adapter_path))
    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        trust_remote_code=True,
    )
    model = PeftModel.from_pretrained(model, str(adapter_path))
    model = model.merge_and_unload()

    output_path.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(output_path))
    tokenizer.save_pretrained(str(output_path))

    print(f"\nMerged model saved to: {output_path}")
    print(f"  Convert to GGUF for Ollama: python3 llama.cpp/convert.py {output_path}")


def test_adapter(adapter_dir=None, prompt=None):
    """Test the fine-tuned model with a prompt."""
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from peft import PeftModel

    adapter_path = Path(adapter_dir or f"{DEFAULT_OUTPUT}/chronicle-specialist")

    if not adapter_path.exists():
        print(f"Error: adapter not found at {adapter_path}")
        sys.exit(1)

    config_path = adapter_path / "adapter_config.json"
    with open(config_path) as f:
        config = json.load(f)
    base_model = config.get("base_model_name_or_path", DEFAULT_BASE_MODEL)

    print(f"Loading model + adapter (fp16)...")
    tokenizer = AutoTokenizer.from_pretrained(str(adapter_path))
    model = AutoModelForCausalLM.from_pretrained(
        base_model,
        device_map="auto",
        trust_remote_code=True,
        torch_dtype=torch.float16,
    )
    model = PeftModel.from_pretrained(model, str(adapter_path))
    model.eval()

    if not prompt:
        prompt = (
            "Find the structural connection between these two observations: "
            "(1) HAL logged 48 false arrival events per day while having baselines "
            "that could prevent them. (2) Chronicle ran 224 threads while build "
            "objectives went unaddressed."
        )

    messages = [
        {"role": "system", "content": "You are a Chronicle analyst — a reasoning system that finds non-obvious connections, identifies structural patterns, and builds understanding through iterative investigation."},
        {"role": "user", "content": prompt},
    ]

    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer(text, return_tensors="pt").to(model.device)

    print(f"\nPrompt: {prompt}\n")
    print("Response:")
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=512,
            temperature=0.7,
            top_p=0.9,
            do_sample=True,
        )
    response = tokenizer.decode(outputs[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True)
    print(response)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "train":
        base_model = DEFAULT_BASE_MODEL
        dataset_dir = DEFAULT_DATASET
        output_dir = DEFAULT_OUTPUT

        if "--base-model" in sys.argv:
            idx = sys.argv.index("--base-model")
            base_model = sys.argv[idx + 1]
        if "--dataset" in sys.argv:
            idx = sys.argv.index("--dataset")
            dataset_dir = sys.argv[idx + 1]
        if "--output" in sys.argv:
            idx = sys.argv.index("--output")
            output_dir = sys.argv[idx + 1]

        train(base_model, dataset_dir, output_dir)

    elif cmd == "merge":
        adapter_dir = None
        output_dir = None
        if "--adapter" in sys.argv:
            idx = sys.argv.index("--adapter")
            adapter_dir = sys.argv[idx + 1]
        if "--output" in sys.argv:
            idx = sys.argv.index("--output")
            output_dir = sys.argv[idx + 1]
        merge_adapter(adapter_dir, output_dir)

    elif cmd == "test":
        adapter_dir = None
        prompt = None
        if "--adapter" in sys.argv:
            idx = sys.argv.index("--adapter")
            adapter_dir = sys.argv[idx + 1]
        if "--prompt" in sys.argv:
            idx = sys.argv.index("--prompt")
            prompt = sys.argv[idx + 1]
        test_adapter(adapter_dir, prompt)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
