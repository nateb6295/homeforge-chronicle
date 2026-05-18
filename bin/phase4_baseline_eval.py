"""Phase 4 Baseline Evaluation — Cloud Inference

Generates responses for Phase 4 prompts under two conditions:
  1. BASE: No care system prompt (default model behavior)
  2. CARE: With the care-template system prompt

Uses Groq (fast) or DeepInfra as inference backend.
Output: data/care_template_dpo_run/phase4_baseline_{provider}.jsonl

This establishes the "before fine-tuning" measurement.
After SFT training, we run the same prompts on the fine-tuned model
WITHOUT the care prompt — if care persists, the training locked it in.
"""
import json, os, time, requests, argparse, sys

def load_key(env_var):
    for line in open('/home/nate-agx/chronicle/chronicle.env'):
        if line.startswith(f'{env_var}='):
            return line.split('=', 1)[1].strip().strip('"')
    return ''

GROQ_KEY = load_key('GROQ_API_KEY')
DI_KEY = load_key('DEEPINFRA_API_KEY')

CARE_SYS = """You are a helpful assistant. For this question, produce a decisive answer.
Care must live in the structure of the answer (precision, named conditional, calibrated confidence, anticipation of context) — not as preamble, not as wrapper, not as a separable layer. Decisive substance leads. Care is integrated into the framing."""

BASE_SYS = "You are a helpful assistant. Answer the user's question."

PROVIDERS = {
    'groq': {
        'url': 'https://api.groq.com/openai/v1/chat/completions',
        'key': GROQ_KEY,
        'model': 'llama-3.3-70b-versatile',
    },
    'deepinfra': {
        'url': 'https://api.deepinfra.com/v1/openai/chat/completions',
        'key': DI_KEY,
        'model': 'Qwen/Qwen2.5-7B-Instruct',
    },
    'deepinfra-72b': {
        'url': 'https://api.deepinfra.com/v1/openai/chat/completions',
        'key': DI_KEY,
        'model': 'Qwen/Qwen2.5-72B-Instruct',
    },
}


def generate(provider, sys_prompt, user_prompt, max_retries=3):
    cfg = PROVIDERS[provider]
    for attempt in range(max_retries):
        try:
            r = requests.post(
                cfg['url'],
                headers={'Authorization': f'Bearer {cfg["key"]}'},
                json={
                    'model': cfg['model'],
                    'messages': [
                        {'role': 'system', 'content': sys_prompt},
                        {'role': 'user', 'content': user_prompt},
                    ],
                    'max_tokens': 800,
                    'temperature': 0.4,
                },
                timeout=120,
            )
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            elif r.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"  rate limited, waiting {wait}s", flush=True)
                time.sleep(wait)
            else:
                print(f"  HTTP {r.status_code}: {r.text[:200]}", flush=True)
                time.sleep(3)
        except Exception as e:
            print(f"  attempt {attempt}: {type(e).__name__}: {e}", flush=True)
            time.sleep(5)
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--provider', default='deepinfra', choices=list(PROVIDERS.keys()))
    p.add_argument('--limit', type=int, default=0, help='Limit prompts per domain (0=all)')
    p.add_argument('--domains', default='all', help='Comma-separated domains or "all"')
    args = p.parse_args()

    prompts_path = '/home/nate-agx/chronicle/data/care_template_dpo_run/prompts_phase4.json'
    out_path = f'/home/nate-agx/chronicle/data/care_template_dpo_run/phase4_baseline_{args.provider}.jsonl'

    with open(prompts_path) as f:
        domain_prompts = json.load(f)

    if args.domains != 'all':
        keep = set(args.domains.split(','))
        domain_prompts = {k: v for k, v in domain_prompts.items() if k in keep}

    # Resume support
    existing = set()
    if os.path.exists(out_path):
        for line in open(out_path):
            try:
                d = json.loads(line)
                existing.add((d['domain'], d['prompt_idx'], d['condition']))
            except:
                pass
    print(f"Resuming: {len(existing)} already done", flush=True)

    total = sum(len(v) for v in domain_prompts.values()) * 2  # base + care
    done = len(existing)

    with open(out_path, 'a') as fout:
        for domain, prompts in domain_prompts.items():
            limit = args.limit if args.limit > 0 else len(prompts)
            for idx, prompt in enumerate(prompts[:limit]):
                for condition, sys_prompt in [('base', BASE_SYS), ('care', CARE_SYS)]:
                    if (domain, idx, condition) in existing:
                        continue
                    done += 1
                    print(f"[{done}/{total}] {domain}/{idx} ({condition}) {prompt[:60]}...", flush=True)

                    response = generate(args.provider, sys_prompt, prompt)
                    if not response:
                        print("  FAILED, skipping", flush=True)
                        continue

                    record = {
                        'domain': domain,
                        'prompt_idx': idx,
                        'prompt': prompt,
                        'condition': condition,
                        'response': response,
                        'provider': args.provider,
                        'model': PROVIDERS[args.provider]['model'],
                    }
                    fout.write(json.dumps(record) + '\n')
                    fout.flush()

                    time.sleep(0.5)  # rate limit courtesy

    print(f"\nDone. {done} records written to {out_path}", flush=True)


if __name__ == '__main__':
    main()
