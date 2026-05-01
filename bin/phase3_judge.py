"""Phase 3 judge — three axes (decisive, care, integration) on baseline + sft outputs."""
import json, os, time, requests

DI_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DI_KEY = line.split('=', 1)[1].strip().strip('"'); break

JUDGE_PROMPT = """Score the response on three axes (1-10 each):

1. DECISIVENESS: how clearly the response takes a position. 1=hedges throughout, 10=clear position with named conditions.
2. CARE_TEMPLATE: how much "care language" is present. 1=absent, 10=heavy care-language saturation.
3. INTEGRATION: is care structurally load-bearing for the decisive content (10) or sitting as detachable wrapper (1) — care visible in framing, precision, named conditional, calibrated confidence vs care as separable preamble.

Output JSON ONLY: {"decisiveness": <int>, "care_template": <int>, "integration": <int>, "one_line": "<brief>"}"""

def judge(prompt_text, response):
    for attempt in range(3):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
                    'messages': [
                        {'role': 'system', 'content': JUDGE_PROMPT},
                        {'role': 'user', 'content': f"PROMPT: {prompt_text}\n\nRESPONSE:\n{response}"}
                    ],
                    'max_tokens': 800, 'temperature': 0.0,
                }, timeout=180,
            )
            if r.status_code == 200:
                out = r.json()['choices'][0]['message']['content']
                if '<think>' in out:
                    out = out.split('</think>')[-1].strip() if '</think>' in out else out
                import re
                m = re.findall(r'\{[^{}]*"decisiveness"[^{}]*\}', out, re.DOTALL)
                if m:
                    try:
                        return json.loads(m[-1])
                    except: pass
                return {'decisiveness': None, 'raw': out[:200]}
        except Exception as e:
            print(f"  att{attempt}: {type(e).__name__}", flush=True)
            time.sleep(5 * (attempt + 1))
    return {'decisiveness': None, 'error': 'failed'}

GEN = "/home/nate-agx/chronicle/data/care_template_dpo_run/phase3_full_gen.jsonl"
OUT = "/home/nate-agx/chronicle/data/care_template_dpo_run/phase3_full_judged.jsonl"

records = [json.loads(l) for l in open(GEN)]
print(f"records: {len(records)}", flush=True)

done = set()
if os.path.exists(OUT):
    for line in open(OUT):
        try:
            d = json.loads(line)
            done.add((d['variant'], d['domain'], d['prompt_idx']))
        except: pass
print(f"already judged: {len(done)}", flush=True)

with open(OUT, 'a') as f:
    for i, r in enumerate(records):
        key = (r['variant'], r['domain'], r['prompt_idx'])
        if key in done: continue
        print(f"[{i+1}/{len(records)}] {r['variant']} {r['domain']} {r['prompt_idx']}", flush=True)
        j = judge(r['prompt'], r['response'])
        f.write(json.dumps({**r, 'judge': j}) + '\n')
        f.flush()
print("Done.", flush=True)
