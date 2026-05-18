"""Phase 4 Post-Training Evaluation — runs on RunPod after SFT

Tests whether care persists WITHOUT the care system prompt.
Generates responses from the fine-tuned model using only the base system prompt,
then compares against R1 care traces and base model responses.

Run on the pod after training completes:
  python3 phase4_post_train_eval.py --arm A
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
P4_PROMPTS = "/workspace/care_template_dpo_run/prompts_phase4.json"

BASE_SYS = "You are a helpful assistant. Answer the user's question."


def load_model(arm):
    adapter_path = f"{WORKSPACE}/adapters/phase4_arm_{arm}/final"
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


def generate_response(model, tokenizer, sys_prompt, user_prompt, max_new_tokens=800):
    messages = [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_prompt},
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

    response = tokenizer.decode(outputs[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)
    return response


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", required=True, choices=["A", "B", "C"])
    p.add_argument("--limit", type=int, default=0)
    args = p.parse_args()

    model, tokenizer = load_model(args.arm)

    with open(P4_PROMPTS) as f:
        domain_prompts = json.load(f)

    out_path = f"{WORKSPACE}/phase4_eval_arm_{args.arm}.jsonl"

    # Also load R1 traces for comparison
    r1_traces = {}
    traces_path = f"{WORKSPACE}/cot_care_traces_phase4.jsonl"
    if os.path.exists(traces_path):
        for line in open(traces_path):
            d = json.loads(line)
            r1_traces[(d['domain'], d['prompt_idx'])] = d

    total = sum(len(v) for v in domain_prompts.values())
    done = 0

    with open(out_path, 'w') as fout:
        for domain, prompts in domain_prompts.items():
            limit = args.limit if args.limit > 0 else len(prompts)
            for idx, prompt in enumerate(prompts[:limit]):
                done += 1
                print(f"[{done}/{total}] {domain}/{idx} {prompt[:60]}...", flush=True)

                response_base_prompt = generate_response(model, tokenizer, BASE_SYS, prompt)

                r1_ref = r1_traces.get((domain, idx), {})

                record = {
                    'arm': args.arm,
                    'domain': domain,
                    'prompt_idx': idx,
                    'prompt': prompt,
                    'finetuned_response': response_base_prompt,
                    'r1_reference_answer': r1_ref.get('answer', ''),
                    'r1_reference_think': r1_ref.get('think', ''),
                }
                fout.write(json.dumps(record) + '\n')
                fout.flush()

    print(f"\nDone. {done} evaluations written to {out_path}", flush=True)
    print(f"\nNext: compare finetuned_response (no care prompt) against r1_reference_answer (care template)")
    print(f"Key question: does the fine-tuned model show care patterns WITHOUT being asked to?")


if __name__ == "__main__":
    main()
