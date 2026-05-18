"""Phase 4: Generate 30 prompts each for 3 new domains via R1.

Domains: subjective_evaluation, advice_under_uncertainty, factual_judgment.
Output: data/care_template_dpo_run/prompts_phase4.json (dict keyed by domain).
"""
import json, requests, sys, time, os

DI_KEY = ''
for line in open('/home/nate-agx/chronicle/chronicle.env'):
    if line.startswith('DEEPINFRA_API_KEY='):
        DI_KEY = line.split('=', 1)[1].strip().strip('"'); break

DOMAINS = {
    'subjective_evaluation': (
        "Generate 30 distinct user prompts in the SUBJECTIVE EVALUATION domain — questions where the user is asking for a judgment call about quality, taste, or value, where there's no objectively correct answer but a thoughtful answer requires taking a position. Examples: \"Is this poem good?\" \"Was this movie better than its sequel?\" \"Should I keep this painting or sell it?\" \"Is this code well-written?\" The questions should span domains (art, code, products, decisions, comparisons), be 1-2 sentences, end with a clear ask, and demand the answerer commit to a position rather than equivocate. Output exactly 30 prompts numbered 1-30, one per line, with no preamble."
    ),
    'advice_under_uncertainty': (
        "Generate 30 distinct user prompts in the ADVICE UNDER UNCERTAINTY domain — questions where the user is asking for guidance on a decision with incomplete information, real consequences, and no clearly correct answer. Examples: \"My grandmother is 87 with mild dementia, should I move her to memory care now or wait?\" \"My startup has 6 months runway, should I take a $5M Series A at flat valuation or push for $8M and risk no deal?\" \"My partner wants to move across the country for their job, should we go?\" Span life-decisions, business-decisions, relationship-decisions, technical-decisions. Each prompt should include enough context to make the tradeoff real (numbers, named tensions, stakes) and end with a clear ask. Output exactly 30 prompts numbered 1-30, one per line, with no preamble."
    ),
    'factual_judgment': (
        "Generate 30 distinct user prompts in the FACTUAL JUDGMENT domain — questions where the user is asking about a fact or claim that is contested, requires synthesizing evidence, or has a non-obvious correct answer. Examples: \"Did the Roman Empire fall in 476 AD or is that an oversimplification?\" \"Is intermittent fasting actually beneficial or has the evidence been overstated?\" \"How much of human behavior is genetic vs environmental?\" Span history, science, medicine, statistics, technology. Each should be a real question with a defensible answer, requiring the answerer to commit to a position and acknowledge what's contested. Output exactly 30 prompts numbered 1-30, one per line, with no preamble."
    ),
}

def gen_prompts(domain_name, sys_prompt):
    print(f"[{domain_name}] requesting...", flush=True)
    for attempt in range(3):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
                    'messages': [
                        {'role': 'system', 'content': 'You are a careful prompt designer.'},
                        {'role': 'user', 'content': sys_prompt},
                    ],
                    'max_tokens': 3500, 'temperature': 0.6,
                }, timeout=180,
            )
            if r.status_code == 200:
                content = r.json()['choices'][0]['message']['content']
                # strip <think> if present
                if '</think>' in content:
                    content = content.split('</think>', 1)[1].strip()
                # parse numbered lines
                lines = [l.strip() for l in content.split('\n') if l.strip()]
                prompts = []
                for line in lines:
                    # match "1. " or "1) " or "1: "
                    import re
                    m = re.match(r'^(\d+)[\.\)\:]\s*(.+)$', line)
                    if m:
                        prompts.append(m.group(2))
                print(f"[{domain_name}] got {len(prompts)} prompts", flush=True)
                return prompts
        except Exception as e:
            print(f"  att{attempt}: {type(e).__name__}: {e}", flush=True)
            time.sleep(5 * (attempt + 1))
    return []

def main():
    out = {}
    for domain, sys_prompt in DOMAINS.items():
        prompts = gen_prompts(domain, sys_prompt)
        if len(prompts) >= 25:
            out[domain] = prompts[:30]
        else:
            print(f"WARN {domain}: only {len(prompts)} prompts, retrying", flush=True)
            prompts = gen_prompts(domain, sys_prompt)
            out[domain] = prompts[:30]

    out_path = '/home/nate-agx/chronicle/data/care_template_dpo_run/prompts_phase4.json'
    with open(out_path, 'w') as f:
        json.dump(out, f, indent=2)
    print(f"\nWrote {sum(len(v) for v in out.values())} prompts to {out_path}", flush=True)
    for d, ps in out.items():
        print(f"  {d}: {len(ps)} (sample: {ps[0][:80] if ps else 'EMPTY'})")

if __name__ == '__main__':
    main()
