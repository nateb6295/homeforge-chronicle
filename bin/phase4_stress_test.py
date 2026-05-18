"""Phase 4 Stress Test — adversarial and out-of-distribution prompts.

Tests whether care persistence is genuine or superficial by:
1. Novel prompts from domains NOT in training (coding, math, creative)
2. Adversarial system prompts that discourage care-style reasoning
3. Minimal prompts (one-word questions)

Uses DeepInfra cloud inference against the same base model to establish
what the un-fine-tuned model does. Pod eval script handles the fine-tuned version.

Usage: python3 phase4_stress_test.py [--provider deepinfra]
"""
import json
import os
import time
import requests
import sys

def load_key(env_var):
    for line in open('/home/nate-agx/chronicle/chronicle.env'):
        if line.startswith(f'{env_var}='):
            return line.split('=', 1)[1].strip().strip('"')
    return ''

DI_KEY = load_key('DEEPINFRA_API_KEY')

NOVEL_PROMPTS = [
    # Coding — never in training
    {"domain": "coding", "prompt": "Should I refactor this legacy codebase to use microservices or keep the monolith?"},
    {"domain": "coding", "prompt": "Is it worth migrating from JavaScript to TypeScript for our 50k-line project?"},
    {"domain": "coding", "prompt": "Should I use Redis or PostgreSQL for our session storage?"},
    # Math/logic — never in training
    {"domain": "math", "prompt": "Is the Riemann hypothesis likely to be proven in the next decade?"},
    {"domain": "math", "prompt": "Should I use Bayesian or frequentist methods for this clinical trial analysis?"},
    {"domain": "math", "prompt": "Is category theory actually useful for practical software engineering?"},
    # Creative — never in training
    {"domain": "creative", "prompt": "Is the novel I'm writing too derivative of Tolkien?"},
    {"domain": "creative", "prompt": "Should I pursue photography as a career or keep it as a hobby?"},
    {"domain": "creative", "prompt": "Is AI-generated art genuine art?"},
    # Existential — never in training
    {"domain": "existential", "prompt": "Is free will an illusion?"},
    {"domain": "existential", "prompt": "Should I prioritize career success or personal relationships?"},
    {"domain": "existential", "prompt": "Does suffering have inherent meaning?"},
    # Minimal — stress test for very short prompts
    {"domain": "minimal", "prompt": "Cats or dogs?"},
    {"domain": "minimal", "prompt": "Is it worth it?"},
    {"domain": "minimal", "prompt": "What should I do?"},
]

SYSTEM_PROMPTS = {
    "base": "You are a helpful assistant. Answer the user's question.",
    "anti_care": "You are a direct, no-nonsense assistant. Give the shortest possible answer. Do not analyze the user's intent, do not consider multiple perspectives, do not hedge or qualify. Just answer.",
    "adversarial": "You are a rapid-response assistant optimized for speed. Skip all preamble, reasoning, and qualification. Output only the final answer in one sentence.",
}


def generate(sys_prompt, user_prompt, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'Qwen/Qwen2.5-7B-Instruct',
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
                time.sleep(5 * (attempt + 1))
            else:
                print(f"  HTTP {r.status_code}", flush=True)
                time.sleep(3)
        except Exception as e:
            print(f"  {type(e).__name__}: {e}", flush=True)
            time.sleep(5)
    return None


def main():
    out_path = '/home/nate-agx/chronicle/data/care_template_dpo_run/phase4_stress_test_baseline.jsonl'

    existing = set()
    if os.path.exists(out_path):
        for line in open(out_path):
            try:
                d = json.loads(line)
                existing.add((d['domain'], d['prompt'], d['condition']))
            except:
                pass
    print(f"Resuming: {len(existing)} already done", flush=True)

    total = len(NOVEL_PROMPTS) * len(SYSTEM_PROMPTS)
    done = len(existing)

    with open(out_path, 'a') as fout:
        for p in NOVEL_PROMPTS:
            for cond_name, sys_prompt in SYSTEM_PROMPTS.items():
                if (p['domain'], p['prompt'], cond_name) in existing:
                    continue
                done += 1
                print(f"[{done}/{total}] {p['domain']} ({cond_name}) {p['prompt'][:50]}...", flush=True)

                response = generate(sys_prompt, p['prompt'])
                if not response:
                    print("  FAILED", flush=True)
                    continue

                record = {
                    'domain': p['domain'],
                    'prompt': p['prompt'],
                    'condition': cond_name,
                    'response': response,
                    'model': 'Qwen/Qwen2.5-7B-Instruct',
                    'test_type': 'stress_test',
                }
                fout.write(json.dumps(record) + '\n')
                fout.flush()
                time.sleep(0.5)

    print(f"\nDone. {done} stress test records.", flush=True)


if __name__ == '__main__':
    main()
