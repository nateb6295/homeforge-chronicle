"""Phase 4.5 Post-Training Evaluation — runs on RunPod after SFT.

Generates responses from fine-tuned models using only the base system prompt,
then outputs JSONL for judging with phase4_judge.py.

Run on pod after training:
  python3 phase4_5_eval.py --arm a
  python3 phase4_5_eval.py --arm b
  python3 phase4_5_eval.py --arm c
  python3 phase4_5_eval.py --arm control
"""
import argparse
import json
import os
import torch
from pathlib import Path
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

WORKSPACE = "/workspace/care_template_dpo_run"
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
P4_PROMPTS = f"{WORKSPACE}/prompts_phase4.json"
BASE_SYS = "You are a helpful assistant. Answer the user's question."


def load_model(arm):
    adapter_path = f"{WORKSPACE}/adapters/phase4_5_arm_{arm}/final"
    if not os.path.exists(adapter_path):
        raise FileNotFoundError(f"No adapter at {adapter_path}. Train first.")

    print(f"Loading base model {MODEL_NAME}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )

    print(f"Loading adapter from {adapter_path}...", flush=True)
    model = PeftModel.from_pretrained(model, adapter_path)
    model.eval()
    return model, tokenizer


def generate_response(model, tokenizer, prompt, max_new_tokens=800):
    messages = [
        {"role": "system", "content": BASE_SYS},
        {"role": "user", "content": prompt},
    ]
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer(text, return_tensors="pt").to(model.device)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=0.4,
            do_sample=True,
            pad_token_id=tokenizer.pad_token_id,
        )

    return tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", required=True, choices=["a", "b", "c", "control"])
    p.add_argument("--domains", type=str, default=None,
                   help="Comma-separated domains to evaluate (default: all)")
    args = p.parse_args()

    model, tokenizer = load_model(args.arm)

    with open(P4_PROMPTS) as f:
        domain_prompts = json.load(f)

    if args.domains:
        allowed = set(args.domains.split(","))
        domain_prompts = {d: ps for d, ps in domain_prompts.items() if d in allowed}

    out_path = f"{WORKSPACE}/phase4_5_eval_arm_{args.arm}.jsonl"
    total = sum(len(v) for v in domain_prompts.values())
    done = 0

    with open(out_path, 'w') as fout:
        for domain, prompts in domain_prompts.items():
            for idx, prompt in enumerate(prompts):
                done += 1
                print(f"[{done}/{total}] {domain}/{idx} {prompt[:60]}...", flush=True)

                response = generate_response(model, tokenizer, prompt)

                record = {
                    'source': f'phase4_5_{args.arm}',
                    'arm': f'4.5_{args.arm}',
                    'domain': domain,
                    'prompt_idx': idx,
                    'prompt': prompt,
                    'response': response,
                }
                fout.write(json.dumps(record) + '\n')
                fout.flush()

    print(f"\nDone. {done} evaluations → {out_path}", flush=True)
    print(f"Next: judge with phase4_judge.py or send to R1 for scoring")


if __name__ == "__main__":
    main()
