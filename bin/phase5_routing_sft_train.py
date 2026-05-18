"""Phase 5: LoRA SFT training for sovereign routing on Gemma 4 26B.

Trains Gemma to do observation routing (ignore/think/deep) using R1-generated
reasoning traces. Same LoRA approach as Phase 4 but targeting Gemma 4 26B.

Run on RunPod H100: python3 phase5_routing_sft_train.py

Input: phase5_routing_traces.jsonl (R1-generated reasoning traces)
Output: LoRA adapter at /workspace/phase5_routing/adapters/final/
"""
import json
import os
import torch
from datasets import Dataset
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM, AutoTokenizer
from trl import SFTConfig, SFTTrainer

WORKSPACE = "/workspace/phase5_routing"
MODEL_NAME = "google/gemma-4-26B-A4B-it"
TRACE_FILE = f"{WORKSPACE}/phase5_routing_traces.jsonl"

ROUTE_SYSTEM = """You route observations for a cognitive system. Reason step by step about what the observation is, whether it connects to core interests, and how urgent it is. Then output a single route number.

Routes:
1 = noise (generic news, system metrics, routine updates)
2 = signal (XRP/ICP/Flare, AI cognition, BCI, sovereignty, home security)
3 = alarm (major regulatory shift, BCI breakthrough, nighttime person on camera, family threat)"""


def load_traces():
    records = []
    for line in open(TRACE_FILE):
        try:
            records.append(json.loads(line))
        except:
            pass
    return records


def format_for_sft(records):
    """Format traces as chat completions for SFT."""
    formatted = []
    for r in records:
        user_msg = f"Source: {r['source']}\nObservation: {r['content']}"
        assistant_msg = r['trace']

        text = f"<start_of_turn>system\n{ROUTE_SYSTEM}<end_of_turn>\n"
        text += f"<start_of_turn>user\n{user_msg}<end_of_turn>\n"
        text += f"<start_of_turn>model\n{assistant_msg}<end_of_turn>"

        formatted.append({"text": text})
    return formatted


def main():
    os.makedirs(f"{WORKSPACE}/adapters", exist_ok=True)

    print(f"Loading traces from {TRACE_FILE}...", flush=True)
    records = load_traces()
    print(f"  {len(records)} traces loaded", flush=True)

    formatted = format_for_sft(records)
    dataset = Dataset.from_list(formatted)
    print(f"  Dataset: {len(dataset)} examples", flush=True)

    print(f"Loading model {MODEL_NAME}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        attn_implementation="flash_attention_2",
    )

    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    output_dir = f"{WORKSPACE}/adapters/routing"

    training_args = SFTConfig(
        output_dir=output_dir,
        num_train_epochs=3,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=8,
        learning_rate=2e-4,
        lr_scheduler_type="cosine",
        warmup_ratio=0.1,
        bf16=True,
        logging_steps=5,
        save_strategy="epoch",
        report_to="none",
    )

    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        processing_class=tokenizer,
    )

    print("Starting training...", flush=True)
    trainer.train()

    final_path = f"{output_dir}/final"
    model.save_pretrained(final_path)
    tokenizer.save_pretrained(final_path)
    print(f"\nAdapter saved to {final_path}", flush=True)

    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
