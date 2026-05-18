"""Phase 4.5 SFT training — transfiguration experiment.

Four arms via --arm flag:
  4.5a: answer-only, factual exemplars only (61 records)
  4.5b: answer-only, combined (179 original + 61 factual = 240)
  4.5c: think+answer, combined (240 records)
  control: Phase 4 Arm A rerun for direct comparison

Same LoRA config as Phase 4: r=16, alpha=32, Qwen2.5-7B-Instruct.
3 epochs. Output adapter to phase4_5_arm_{a,b,c,control}/final/.

Prerequisites on pod:
  pip install -q transformers peft trl accelerate datasets bitsandbytes

Data files (upload from AGX):
  /workspace/care_template_dpo_run/phase4_5_train_factual_only.jsonl    (61)
  /workspace/care_template_dpo_run/phase4_5_train_combined.jsonl        (240)
  /workspace/care_template_dpo_run/cot_care_traces_combined.jsonl       (179, for control)

Run:
  python3 phase4_5_sft_train.py --arm a
  python3 phase4_5_sft_train.py --arm b
  python3 phase4_5_sft_train.py --arm c
  python3 phase4_5_sft_train.py --arm control
"""
import argparse
import json
from pathlib import Path

import torch
from datasets import Dataset
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM, AutoTokenizer
from trl import SFTConfig, SFTTrainer

WORKSPACE = "/workspace/care_template_dpo_run"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"

ARM_DATA = {
    "a": f"{WORKSPACE}/phase4_5_train_factual_only.jsonl",
    "b": f"{WORKSPACE}/phase4_5_train_combined.jsonl",
    "c": f"{WORKSPACE}/phase4_5_train_combined.jsonl",
    "control": f"{WORKSPACE}/cot_care_traces_combined.jsonl",
}

ARM_FORMAT = {
    "a": "answer_only",
    "b": "answer_only",
    "c": "think_answer",
    "control": "think_answer",
}


def load_traces(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


def format_record(rec, fmt, tokenizer):
    if fmt == "think_answer" and rec.get("think"):
        response = f"<think>\n{rec['think']}\n</think>\n\n{rec['answer']}"
    else:
        response = rec["answer"]

    messages = [
        {"role": "system", "content": "You are a helpful assistant. Answer the user's question."},
        {"role": "user", "content": rec["prompt"]},
        {"role": "assistant", "content": response},
    ]
    return {"text": tokenizer.apply_chat_template(messages, tokenize=False)}


def build_dataset(arm, tokenizer):
    records = load_traces(ARM_DATA[arm])
    fmt = ARM_FORMAT[arm]
    formatted = [format_record(r, fmt, tokenizer) for r in records]
    print(f"[arm {arm}] {len(formatted)} training records, format={fmt}", flush=True)
    print(f"[sample]\n{formatted[0]['text'][:600]}\n...", flush=True)
    return Dataset.from_list(formatted)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", required=True, choices=["a", "b", "c", "control"])
    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--lr", type=float, default=2e-4)
    p.add_argument("--batch", type=int, default=2)
    args = p.parse_args()

    out_dir = f"{WORKSPACE}/adapters/phase4_5_arm_{args.arm}"
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    print(f"Loading {MODEL_NAME}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )

    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.0,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        task_type="CAUSAL_LM",
        bias="none",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    dataset = build_dataset(args.arm, tokenizer)

    sft_config = SFTConfig(
        output_dir=out_dir,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch,
        gradient_accumulation_steps=4,
        learning_rate=args.lr,
        bf16=True,
        logging_steps=5,
        save_strategy="epoch",
        save_total_limit=1,
        report_to="none",
    )

    trainer = SFTTrainer(
        model=model,
        args=sft_config,
        train_dataset=dataset,
        processing_class=tokenizer,
    )

    print(f"\n=== Training Phase 4.5 arm {args.arm} ===", flush=True)
    trainer.train()
    final_dir = f"{out_dir}/final"
    trainer.save_model(final_dir)
    print(f"\nSaved adapter to {final_dir}", flush=True)


if __name__ == "__main__":
    main()
