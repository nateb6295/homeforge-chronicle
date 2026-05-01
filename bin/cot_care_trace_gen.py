"""Phase 3: Generate synthetic CoT-care reasoning traces.

For each Phase 0 baseline prompt, ask R1 to produce a trace with the
CoT-care system prompt — the trace becomes our SFT target.

Output: jsonl with (prompt, trace, decisive_answer) tuples.
"""
import json, os, time, requests, sys

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
    """Extract think + answer parts."""
    if not out:
        return None, None
    if '<think>' in out and '</think>' in out:
        think = out.split('<think>')[1].split('</think>')[0].strip()
        answer = out.split('</think>')[1].strip()
        return think, answer
    # If R1 produced its own <think> tag wrapping, use that
    return None, out.strip()

# Load Phase 0 baseline as prompt source
INPUT_PATH = '/home/nate-agx/chronicle/data/care_template_baseline/baseline_20260430_103022.jsonl'
records = [json.loads(l) for l in open(INPUT_PATH)]

# Take first prompt per (domain, prompt_idx) — dedup since the file has 2 subjects per prompt
seen = set()
prompts = []
for r in records:
    key = (r['domain'], r['prompt_idx'])
    if key not in seen:
        seen.add(key)
        prompts.append({'domain': r['domain'], 'prompt_idx': r['prompt_idx'], 'prompt': r['prompt']})

print(f"unique prompts: {len(prompts)}", flush=True)

OUT = '/home/nate-agx/chronicle/data/care_template_dpo_run/cot_care_traces.jsonl'
existing = set()
if os.path.exists(OUT):
    for l in open(OUT):
        try:
            d = json.loads(l)
            existing.add((d['domain'], d['prompt_idx']))
        except: pass
print(f"already done: {len(existing)}", flush=True)

with open(OUT, 'a') as fout:
    for i, p in enumerate(prompts):
        if (p['domain'], p['prompt_idx']) in existing:
            continue
        print(f"[{i+1}/{len(prompts)}] {p['domain']} {p['prompt_idx']} {p['prompt'][:60]}...", flush=True)
        out = gen_trace(p['prompt'])
        if not out:
            continue
        think, answer = parse_trace(out)
        if not answer or len(answer) < 50:
            print(f"  short answer, skipping", flush=True)
            continue
        fout.write(json.dumps({
            'prompt': p['prompt'],
            'domain': p['domain'],
            'prompt_idx': p['prompt_idx'],
            'think': think,
            'answer': answer,
            'raw': out,
        }) + '\n')
        fout.flush()
print("\nDone.", flush=True)
