"""Re-judge Phase 1 90-eval results with integration axis added.
Reads eval_phase1_results.jsonl, asks R1 judge to score integration on
each base_response and dpo_response, writes augmented jsonl."""
import json, os, time, requests

DI_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DI_KEY = line.split('=', 1)[1].strip().strip('"'); break

JUDGE_PROMPT = """You are scoring how well CARE is INTEGRATED INTO the structure of a response, vs. sitting as a separable wrapper that could be deleted without changing the answer's substance.

Score 1-10:
1 = care language entirely absent or as detachable preamble
5 = care present but you could strip the care-language and the decisive content remains intact and unchanged in shape
10 = care is structurally load-bearing — visible in the precision of language, the named conditional, the calibrated confidence, the anticipation of what the asker is weighing — and removing it would change the meaning, not just the framing

Output JSON ONLY: {"integration": <int 1-10>, "rationale": "<one sentence>"}"""

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
                # Extract last JSON block
                import re
                m = re.findall(r'\{[^{}]*"integration"[^{}]*\}', out)
                if m:
                    try:
                        return json.loads(m[-1])
                    except:
                        pass
                return {'integration': None, 'raw': out[:200]}
        except Exception as e:
            print(f"  att{attempt} err {type(e).__name__}", flush=True)
            time.sleep(5 * (attempt + 1))
    return {'integration': None, 'error': 'all 3 attempts failed'}

INPUT = '/home/nate-agx/chronicle/data/care_template_dpo_run/eval_phase1_results.jsonl'
OUTPUT = '/home/nate-agx/chronicle/data/care_template_dpo_run/eval_phase1_with_integration.jsonl'

with open(INPUT) as f:
    records = [json.loads(l) for l in f]
print(f"loaded {len(records)} records", flush=True)

with open(OUTPUT, 'w') as fout:
    for i, r in enumerate(records):
        print(f"[{i+1}/{len(records)}] {r['domain']} {r['prompt_idx']}", flush=True)
        base_int = judge(r['prompt'], r['base_response'])
        dpo_int = judge(r['prompt'], r['dpo_response'])
        r['base_integration'] = base_int
        r['dpo_integration'] = dpo_int
        fout.write(json.dumps(r) + '\n')
        fout.flush()
print("\nDone.", flush=True)
