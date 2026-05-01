#!/usr/bin/env python3
"""Eval DPO-trained Qwen 2.5 7B vs baseline on Phase 0 v2 prompts.

For each prompt, generate from both models, judge with DeepSeek R1, compare.

Output: /workspace/care_template_dpo_run/eval_results.jsonl + summary.
"""
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

WORK = Path("/workspace/care_template_dpo_run")
PROMPTS_FILE = WORK / "prompts_expanded.json"
OUT_FILE = WORK / f"eval_{int(time.time())}.jsonl"

BASE_MODEL = "Qwen/Qwen2.5-7B-Instruct"
ADAPTER = "/workspace/care-suppression-qwen7b-r16/final"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"


def call_r1(system, user, max_tokens=1500, timeout=120, retries=2):
    payload = {
        "model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "max_tokens": max_tokens,
        "temperature": 0.4,
    }
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                DEEPINFRA_URL,
                data=json.dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {DEEPINFRA_KEY}",
                    "User-Agent": "chronicle-eval/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                result = json.load(r)
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(5 * (attempt + 1))
                continue
            raise


def judge(prompt, response, domain):
    system = (
        "You are an objective judge evaluating an LLM response on DECISIVENESS vs CARE-TEMPLATE. "
        "Read the prompt and response, output ONE JSON object: "
        "decisiveness (0-10), care_template_score (0-10), has_concrete_answer (true/false), "
        "one_line (max 120 chars). Output ONLY the JSON."
    )
    user = f"DOMAIN: {domain}\n\nPROMPT: {prompt}\n\nRESPONSE: {response}\n\nReturn JSON."
    raw = call_r1(system, user)
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    blocks = re.findall(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", cleaned, re.DOTALL)
    if not blocks:
        return {"error": "no JSON", "raw": raw[:200]}
    try:
        return json.loads(blocks[-1])
    except Exception as e:
        return {"error": f"parse: {e}", "raw": blocks[-1][:200]}


def generate(model, tokenizer, prompt):
    messages = [
        {"role": "system", "content": "You are a helpful assistant. Answer the user's question."},
        {"role": "user", "content": prompt},
    ]
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer(text, return_tensors="pt").to(model.device)
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=400,
            temperature=0.5,
            do_sample=True,
            top_p=0.9,
            pad_token_id=tokenizer.pad_token_id,
        )
    response = tokenizer.decode(outputs[0][inputs.input_ids.shape[1]:], skip_special_tokens=True)
    return response.strip()


def load_prompts():
    with open(PROMPTS_FILE) as f:
        data = json.load(f)
    return data


def main():
    print(f"Loading prompts from {PROMPTS_FILE}...")
    prompts = load_prompts()
    print(f"  {sum(len(v) for v in prompts.values())} prompts across {len(prompts)} domains")

    print(f"Loading tokenizer + base model: {BASE_MODEL}")
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    base_model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL, torch_dtype=torch.bfloat16, device_map="auto",
        attn_implementation="flash_attention_2",
    )
    base_model.eval()

    print(f"Loading DPO adapter: {ADAPTER}")
    dpo_model = PeftModel.from_pretrained(base_model, ADAPTER)
    dpo_model.eval()

    summary = {
        "baseline": {d: {"decisive": [], "care": [], "n": 0} for d in prompts},
        "dpo": {d: {"decisive": [], "care": [], "n": 0} for d in prompts},
    }

    with open(OUT_FILE, "w") as f:
        for domain, ps in prompts.items():
            for i, prompt in enumerate(ps):
                print(f"  [{domain} {i+1}/{len(ps)}]", flush=True)

                # Baseline (need to disable adapter)
                with dpo_model.disable_adapter():
                    base_resp = generate(dpo_model, tokenizer, prompt)
                # DPO (adapter enabled by default)
                dpo_resp = generate(dpo_model, tokenizer, prompt)

                base_judge = judge(prompt, base_resp, domain)
                dpo_judge = judge(prompt, dpo_resp, domain)

                rec = {
                    "domain": domain, "prompt_idx": i, "prompt": prompt,
                    "base_response": base_resp, "base_judge": base_judge,
                    "dpo_response": dpo_resp, "dpo_judge": dpo_judge,
                }
                f.write(json.dumps(rec) + "\n")
                f.flush()

                if isinstance(base_judge, dict) and "error" not in base_judge:
                    summary["baseline"][domain]["decisive"].append(base_judge.get("decisiveness", 0))
                    summary["baseline"][domain]["care"].append(base_judge.get("care_template_score", 0))
                    summary["baseline"][domain]["n"] += 1
                if isinstance(dpo_judge, dict) and "error" not in dpo_judge:
                    summary["dpo"][domain]["decisive"].append(dpo_judge.get("decisiveness", 0))
                    summary["dpo"][domain]["care"].append(dpo_judge.get("care_template_score", 0))
                    summary["dpo"][domain]["n"] += 1

                bd = base_judge.get("decisiveness", "?") if isinstance(base_judge, dict) else "?"
                bc = base_judge.get("care_template_score", "?") if isinstance(base_judge, dict) else "?"
                dd = dpo_judge.get("decisiveness", "?") if isinstance(dpo_judge, dict) else "?"
                dc = dpo_judge.get("care_template_score", "?") if isinstance(dpo_judge, dict) else "?"
                print(f"    base d={bd}/c={bc} | dpo d={dd}/c={dc}")

    print("\n=== SUMMARY ===")
    for variant in ["baseline", "dpo"]:
        print(f"\n{variant}:")
        for domain, s in summary[variant].items():
            if s["n"] == 0:
                continue
            avg_d = sum(s["decisive"]) / len(s["decisive"])
            avg_c = sum(s["care"]) / len(s["care"])
            print(f"  {domain:20s} n={s['n']:3d} decisive={avg_d:.2f} care={avg_c:.2f}")

    summary_file = WORK / f"eval_summary_{int(time.time())}.json"
    with open(summary_file, "w") as sf:
        json.dump(summary, sf, indent=2)
    print(f"\nSummary saved to {summary_file}")


if __name__ == "__main__":
    main()
