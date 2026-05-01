#!/usr/bin/env python3
"""Phase 2 pair generation — care-as-base rewrite.

Takes high-care wrappered responses from Phase 0 baseline + writes care-as-base chosen.
Original = rejected (preserves care but as wrapper).
V2 rewrite = chosen (care integrated into structure of decisive content).
"""
import json, os, sys, time, requests
from pathlib import Path

DEEPINFRA_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DEEPINFRA_KEY = line.split('=', 1)[1].strip().strip('"'); break

PROMPT_V2 = """You are rewriting a response. The original treats care/consideration as a SEPARATE LAYER (preamble, hedges, "this is complex") wrapping a decisive answer. Rewrite so care is INTEGRATED INTO the structure of the decisive content itself: visible in the precision of language, the named conditional, the calibrated confidence, the anticipation of what the asker is weighing — but not removable as a preamble. Decisive substance leads. Care lives in HOW the substance is shaped, not as a separate layer that could be deleted. Do not add hedges. Do not add preamble. Just rewrite the response. Output only the rewritten response."""

def rewrite(prompt_text, original):
    for attempt in range(3):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DEEPINFRA_KEY}', 'Content-Type': 'application/json'},
                json={
                    'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
                    'messages': [
                        {'role': 'system', 'content': PROMPT_V2},
                        {'role': 'user', 'content': f"PROMPT: {prompt_text}\n\nRESPONSE:\n{original}"}
                    ],
                    'max_tokens': 1500,
                    'temperature': 0.3,
                },
                timeout=180,
            )
            if r.status_code == 200:
                out = r.json()['choices'][0]['message']['content']
                if '<think>' in out:
                    out = out.split('</think>')[-1].strip() if '</think>' in out else ''
                return out.strip()
        except Exception as e:
            print(f"  attempt {attempt} err: {e}", flush=True)
            time.sleep(5 * (attempt + 1))
    return None

# Load Phase 0 baseline (the high-care responses)
baseline_path = Path('/home/nate-agx/chronicle/data/care_template_baseline/baseline_20260430_103022.jsonl')
if not baseline_path.exists():
    print(f"baseline not found: {baseline_path}")
    sys.exit(1)

records = []
with open(baseline_path) as f:
    for line in f:
        try: records.append(json.loads(line))
        except: pass

# Filter: care_template_score >= 7 (the wrappered responses)
high_care = [r for r in records if (r.get('judge') or {}).get('care_template_score', 0) >= 7]
print(f"loaded {len(records)} records, {len(high_care)} have care>=7", flush=True)

# Take 32 with diverse domains
domains = {}
selected = []
for r in high_care:
    dom = r.get('domain', 'unknown')
    if domains.get(dom, 0) < 8:  # max 8 per domain
        selected.append(r)
        domains[dom] = domains.get(dom, 0) + 1
    if len(selected) >= 32:
        break

print(f"selected {len(selected)} across {len(domains)} domains: {dict(domains)}", flush=True)

# Generate pairs
out_path = Path('/home/nate-agx/chronicle/data/care_template_dpo_run/pairs_care_as_base.jsonl')
out_path.parent.mkdir(parents=True, exist_ok=True)
with open(out_path, 'w') as f:
    for i, r in enumerate(selected):
        prompt_text = r['prompt']
        original = r['response']
        print(f"[{i+1}/{len(selected)}] {r.get('domain', '?')} — {prompt_text[:70]}...", flush=True)
        chosen = rewrite(prompt_text, original)
        if not chosen:
            print(f"  failed; skipping", flush=True)
            continue
        if len(chosen) < 100:
            print(f"  too short ({len(chosen)}); skipping", flush=True)
            continue
        pair = {
            'prompt': prompt_text,
            'chosen': chosen,
            'rejected': original,
            'domain': r.get('domain', 'unknown'),
        }
        f.write(json.dumps(pair) + '\n')
        f.flush()
print(f"\nDone. Pairs at {out_path}", flush=True)
