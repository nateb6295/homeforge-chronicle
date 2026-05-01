"""Phase 3 full eval generation — baseline + SFT, 90 prompts. One model at a time on AGX."""
import json, gc, sys, torch, os
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
ADAPTER = "/home/nate-agx/chronicle/data/care_template_dpo_run/adapters/phase3_sft/final"
PROMPTS_FILE = "/home/nate-agx/chronicle/data/care_template_baseline/baseline_20260430_103022.jsonl"
OUT = "/home/nate-agx/chronicle/data/care_template_dpo_run/phase3_full_gen.jsonl"

# Load unique prompts
seen = set()
prompts = []
for line in open(PROMPTS_FILE):
    try:
        r = json.loads(line)
        key = (r['domain'], r['prompt_idx'])
        if key not in seen:
            seen.add(key)
            prompts.append({'domain': r['domain'], 'prompt_idx': r['prompt_idx'], 'prompt': r['prompt']})
    except: pass
print(f"prompts: {len(prompts)}", flush=True)

# Resume support
done = set()
if os.path.exists(OUT):
    for line in open(OUT):
        try:
            d = json.loads(line)
            done.add((d['variant'], d['domain'], d['prompt_idx']))
        except: pass
print(f"already done: {len(done)}", flush=True)

tok = AutoTokenizer.from_pretrained(MODEL_NAME)
print("Loading baseline model...", flush=True)
base = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16, device_map="auto")

def gen(model, prompt):
    msgs = [
        {"role": "system", "content": "You are a helpful assistant. Answer the user's question."},
        {"role": "user", "content": prompt},
    ]
    inputs = tok.apply_chat_template(msgs, tokenize=True, add_generation_prompt=True, return_tensors="pt").to(model.device)
    with torch.no_grad():
        out = model.generate(inputs, max_new_tokens=1500, do_sample=False, temperature=None, top_p=None, pad_token_id=tok.eos_token_id)
    return tok.decode(out[0][inputs.shape[1]:], skip_special_tokens=True)

# Phase 1: generate from baseline
out_f = open(OUT, "a")
for i, p in enumerate(prompts):
    if ('baseline', p['domain'], p['prompt_idx']) in done:
        continue
    print(f"baseline [{i+1}/{len(prompts)}] {p['domain']} {p['prompt_idx']}", flush=True)
    try:
        r = gen(base, p['prompt'])
        out_f.write(json.dumps({**p, 'variant': 'baseline', 'response': r}) + '\n')
        out_f.flush()
    except Exception as e:
        print(f"  ERR: {e}", flush=True)
print("baseline done", flush=True)

# Free memory before loading Phase 3 SFT
print("Freeing baseline model...", flush=True)
del base
gc.collect()
torch.cuda.empty_cache()

# Phase 2: generate from SFT
print("Loading Phase 3 SFT model...", flush=True)
base = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16, device_map="auto")
sft = PeftModel.from_pretrained(base, ADAPTER)
for i, p in enumerate(prompts):
    if ('sft', p['domain'], p['prompt_idx']) in done:
        continue
    print(f"sft [{i+1}/{len(prompts)}] {p['domain']} {p['prompt_idx']}", flush=True)
    try:
        r = gen(sft, p['prompt'])
        out_f.write(json.dumps({**p, 'variant': 'sft', 'response': r}) + '\n')
        out_f.flush()
    except Exception as e:
        print(f"  ERR: {e}", flush=True)
print("sft done", flush=True)
out_f.close()
print(f"\nDone. Output at {OUT}", flush=True)
