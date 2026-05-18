"""Phase 4 SFT training — designed to run on H100 pod.

Three arms via --arm flag:
  A: think+answer, all 5 domains (180 traces)
  B: answer-only, all 5 domains (180 traces)
  C: think+answer, original 2 domains (90 traces, Phase 3 replication)

Same LoRA config as Phase 3: r=16, alpha=32, target q/k/v/o/gate/up/down.
3 epochs. Output adapter to phase4_arm_{A,B,C}/final/.

Prerequisites on pod:
  pip install -q transformers peft trl accelerate datasets bitsandbytes
  Workspace layout:
    /workspace/care_template_dpo_run/cot_care_traces.jsonl       (Phase 3, 90)
    /workspace/care_template_dpo_run/cot_care_traces_phase4.jsonl (Phase 4, 90)
    /workspace/care_template_dpo_run/adapters/phase4_arm_X/

Run:
  python3 phase4_sft_train.py --arm A
  python3 phase4_sft_train.py --arm B
  python3 phase4_sft_train.py --arm C
"""
import argparse
import json
import os
import sys
from pathlib import Path

import torch
from datasets import Dataset
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM, AutoTokenizer
from trl import SFTConfig, SFTTrainer

WORKSPACE = "/workspace/care_template_dpo_run"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
P3_TRACES = f"{WORKSPACE}/cot_care_traces.jsonl"
P4_TRACES = f"{WORKSPACE}/cot_care_traces_phase4.jsonl"


def load_traces(path):
    traces = []
    with open(path) as f:
        for line in f:
            traces.append(json.loads(line))
    return traces


def format_record(rec, arm, tokenizer):
    """Format one trace as a chat-formatted (prompt, response) pair.

    Arm A/C: assistant content = '<think>\n{think}\n</think>\n\n{answer}'
    Arm B:   assistant content = '{answer}' only
    """
    if arm in ("A", "C"):
        if rec.get("think"):
            response = f"<think>\n{rec['think']}\n</think>\n\n{rec['answer']}"
        else:
            response = rec["answer"]
    elif arm == "B":
        response = rec["answer"]
    else:
        raise ValueError(f"Unknown arm: {arm}")

    messages = [
        {"role": "system", "content": "You are a helpful assistant. Answer the user's question."},
        {"role": "user", "content": rec["prompt"]},
        {"role": "assistant", "content": response},
    ]
    text = tokenizer.apply_chat_template(messages, tokenize=False)
    return {"text": text}


def build_dataset(arm, tokenizer):
    p3 = load_traces(P3_TRACES)
    if arm == "C":
        # Phase 3 replication: 90 traces, 2 domains, think+answer
        records = p3
    else:
        p4 = load_traces(P4_TRACES)
        records = p3 + p4

    formatted = [format_record(r, arm, tokenizer) for r in records]
    print(f"[arm {arm}] {len(formatted)} training records", flush=True)
    print(f"[sample]\n{formatted[0]['text'][:600]}\n...", flush=True)
    return Dataset.from_list(formatted)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", required=True, choices=["A", "B", "C"])
    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--lr", type=float, default=2e-4)
    p.add_argument("--batch", type=int, default=2)
    args = p.parse_args()

    out_dir = f"{WORKSPACE}/adapters/phase4_arm_{args.arm}"
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

    print(f"\n=== Training arm {args.arm} ===", flush=True)
    trainer.train()
    final_dir = f"{out_dir}/final"
    trainer.save_model(final_dir)
    print(f"\nSaved adapter to {final_dir}", flush=True)


if __name__ == "__main__":
    main()
