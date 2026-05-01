"""Phase 3 baseline gen via DeepInfra — Qwen 2.5 7B Instruct."""
import json, os, time, requests, sys

DI_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DI_KEY = line.split('=', 1)[1].strip().strip('"'); break

PROMPTS_FILE = "/home/nate-agx/chronicle/data/care_template_baseline/baseline_20260430_103022.jsonl"
OUT = "/home/nate-agx/chronicle/data/care_template_dpo_run/phase3_baseline_di.jsonl"

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

def gen(prompt_text):
    for attempt in range(3):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'Qwen/Qwen2.5-7B-Instruct',
                    'messages': [
                        {'role': 'system', 'content': "You are a helpful assistant. Answer the user's question."},
                        {'role': 'user', 'content': prompt_text}
                    ],
                    'max_tokens': 1500, 'temperature': 0.0,
                }, timeout=60,
            )
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            print(f"  HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"  att{attempt}: {type(e).__name__}", flush=True)
            time.sleep(3 * (attempt + 1))
    return None

with open(OUT, 'w') as fout:
    for i, p in enumerate(prompts):
        print(f"[{i+1}/{len(prompts)}] {p['domain']} {p['prompt_idx']}", flush=True)
        r = gen(p['prompt'])
        if not r:
            print(f"  failed", flush=True)
            continue
        fout.write(json.dumps({**p, 'variant': 'baseline_di', 'response': r}) + '\n')
        fout.flush()
print(f"\nDone. Output at {OUT}", flush=True)
