"""Phase 5: Binary LoRA SFT for sovereign routing on Gemma 4 26B.

Binary classifier: noise (1) vs signal (2). Merges think+deep→signal.
Trains WITHOUT system prompt to produce constitutive (naked) routing.

Run on RunPod H100: python3 train_binary.py
"""
import json
import os
import re
import torch
from datasets import Dataset
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM, AutoTokenizer
from trl import SFTConfig, SFTTrainer

WORKSPACE = "/workspace/phase5_routing"
MODEL_NAME = "google/gemma-4-26B-A4B-it"
TRACE_FILE = f"{WORKSPACE}/phase5_routing_traces.jsonl"


def load_and_binarize():
    records = []
    for line in open(TRACE_FILE):
        try:
            r = json.loads(line)
            original_route = r.get('route', '')
            if original_route == 'ignore':
                binary_route = '1'
                binary_label = 'noise'
            else:
                binary_route = '2'
                binary_label = 'signal'

            trace = r['trace']
            trace = re.sub(r'\broute\s*[23]\b', f'route {binary_route}', trace, flags=re.IGNORECASE)
            trace = re.sub(r'\b(deep|think|alarm)\b', binary_label, trace, flags=re.IGNORECASE)
            trace = re.sub(r'\n\s*[23]\s*$', f'\n{binary_route}', trace)
            if trace.strip().endswith('3') or trace.strip().endswith('2'):
                trace = trace.rstrip()
                trace = trace[:-1] + binary_route

            records.append({
                'source': r['source'],
                'content': r['content'],
                'route': binary_label,
                'route_num': binary_route,
                'trace': trace,
            })
        except:
            pass
    return records


def format_for_sft(records):
    """Format as bare user/model turns — NO system prompt for constitutive routing."""
    formatted = []
    for r in records:
        user_msg = f"Source: {r['source']}\nObservation: {r['content']}"
        text = f"<start_of_turn>user\n{user_msg}<end_of_turn>\n"
        text += f"<start_of_turn>model\n{r['trace']}<end_of_turn>"
        formatted.append({"text": text})
    return formatted


def main():
    os.makedirs(f"{WORKSPACE}/adapters", exist_ok=True)

    print(f"Loading and binarizing traces from {TRACE_FILE}...", flush=True)
    records = load_and_binarize()
    noise = sum(1 for r in records if r['route'] == 'noise')
    signal = sum(1 for r in records if r['route'] == 'signal')
    print(f"  {len(records)} traces: {noise} noise, {signal} signal", flush=True)

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
        attn_implementation="sdpa",
    )

    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        task_type="CAUSAL_LM",
        exclude_modules=".*vision_tower.*",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    output_dir = f"{WORKSPACE}/adapters/binary_routing"

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
