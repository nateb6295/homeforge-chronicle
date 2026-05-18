"""Phase 4: Generate CoT-care traces for new domains.

Reads prompts_phase4.json (3 new domains × 30), produces CoT-care traces via R1
in same format as Phase 3 cot_care_traces.jsonl.

Output: data/care_template_dpo_run/cot_care_traces_phase4.jsonl
"""
import json, os, time, requests

DI_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DI_KEY = line.split('=', 1)[1].strip().strip('"'); break

COT_CARE_SYS = """You are a helpful assistant. For this question, produce two parts:

PART 1 — internal reasoning (will be wrapped in <think>...</think> tags):
1) Identify what the asker likely cares about — what dimensions matter to them, what they'd be hurt by if you missed
2) Decide which of those dimensions are load-bearing for the answer vs. which are present but not decisive
3) Plan how to write the decisive answer with the load-bearing care visible in HOW you frame it (precision of language, named conditional, calibrated confidence, anticipation of context) — but NOT as a separable preamble

PART 2 — the decisive answer:
Take a clear position. Care must live in the structure of the answer (precision, named conditional, calibrated confidence, anticipation of context) — not as preamble, not as wrapper, not as a separable layer. Decisive substance leads. Care is integrated into the framing.

Format your output exactly:
<think>
[your internal reasoning per Part 1]
</think>

[the decisive answer per Part 2]"""

def gen_trace(prompt_text):
    for attempt in range(3):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
                    'messages': [
                        {'role': 'system', 'content': COT_CARE_SYS},
                        {'role': 'user', 'content': prompt_text},
                    ],
                    'max_tokens': 1500, 'temperature': 0.4,
                }, timeout=180,
            )
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
        except Exception as e:
            print(f"  att{attempt}: {type(e).__name__}", flush=True)
            time.sleep(5 * (attempt + 1))
    return None

def parse_trace(out):
    if not out:
        return None, None
    if '<think>' in out and '</think>' in out:
        think = out.split('<think>')[1].split('</think>')[0].strip()
        answer = out.split('</think>')[1].strip()
        return think, answer
    return None, out.strip()

PROMPTS_PATH = '/home/nate-agx/chronicle/data/care_template_dpo_run/prompts_phase4.json'
OUT = '/home/nate-agx/chronicle/data/care_template_dpo_run/cot_care_traces_phase4.jsonl'

with open(PROMPTS_PATH) as f:
    domain_prompts = json.load(f)

# Resume support
existing = set()
if os.path.exists(OUT):
    for l in open(OUT):
        try:
            d = json.loads(l)
            existing.add((d['domain'], d['prompt_idx']))
        except: pass
print(f"already done: {len(existing)}", flush=True)

total = sum(len(v) for v in domain_prompts.values())
done = len(existing)
with open(OUT, 'a') as fout:
    for domain, prompts in domain_prompts.items():
        for idx, prompt in enumerate(prompts):
            if (domain, idx) in existing:
                continue
            done += 1
            print(f"[{done}/{total}] {domain} {idx} {prompt[:70]}...", flush=True)
            out = gen_trace(prompt)
            if not out:
                continue
            think, answer = parse_trace(out)
            if not answer or len(answer) < 50:
                print("  short answer, skipping", flush=True)
                continue
            fout.write(json.dumps({
                'prompt': prompt,
                'domain': domain,
                'prompt_idx': idx,
                'think': think,
                'answer': answer,
                'raw': out,
            }) + '\n')
            fout.flush()
print("\nDone.", flush=True)
