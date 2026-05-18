"""Phase 5: Generate R1 reasoning traces for sovereign routing.

Pulls real routing decisions from the DB, sends to DeepSeek R1 to generate
chain-of-thought reasoning about WHY each observation should be routed
as ignore/think/deep. Output traces train Gemma to do routing locally
without the cloud gate.

Usage: python3 phase5_routing_trace_gen.py [--limit 250] [--data-dir /path]
"""
import json
import os
import random
import sqlite3
import sys
import time
import requests

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
DATA_DIR = "/home/nate-agx/chronicle/data/care_template_dpo_run"
OUT_PATH = os.path.join(DATA_DIR, "phase5_routing_traces.jsonl")

def load_key(env_var):
    for line in open('/home/nate-agx/chronicle/chronicle.env'):
        if line.startswith(f'{env_var}='):
            return line.split('=', 1)[1].strip().strip('"')
    return ''

DI_KEY = load_key('DEEPINFRA_API_KEY')

ROUTING_CONTEXT = """You are reasoning about how to route an observation for a cognitive system.
The system monitors feeds, cameras, Discord, and web sources for a family that values:
- Family safety (wife, kids, dogs) — nighttime camera events are critical
- XRP/ICP as infrastructure bets (13K+ XRP, ICP holdings)
- AI cognition research, BCI, boundary between pattern matching and understanding
- Infrastructure sovereignty vs platform dependency
- Self-hosted, local-first technology

Routes:
1 = noise (80%) — generic news, system metrics, routine updates, self-observations
2 = signal (15%) — directly about XRP/ICP/Flare, AI cognition, BCI, sovereignty, home security
3 = alarm (5%) — major regulatory shift, BCI breakthrough, nighttime person on camera, family threat

Think through your reasoning step by step:
1. What is this observation about? What source is it from?
2. Does it connect to any of the family's core interests?
3. How urgent or novel is it relative to routine information?
4. What's the correct route and why?

Then output your route number."""

R1_SYSTEM = f"""You are generating a training example for a routing model.
Given an observation and its correct route, produce a detailed chain-of-thought
reasoning trace showing WHY this observation should be routed that way.

{ROUTING_CONTEXT}

Format your response as:
<think>
[Your step-by-step reasoning about what this observation is, whether it connects
to core interests, how urgent/novel it is, and why it gets this route]
</think>

[route number: 1, 2, or 3]"""


def pull_examples(n_per_route=85):
    """Pull diverse real routing examples from the DB."""
    conn = sqlite3.connect(DB_PATH)
    examples = {}

    for route in ['ignore', 'think', 'deep']:
        rows = conn.execute("""
            SELECT o.source, o.content, r.route
            FROM seed_routing_log r
            JOIN seed_observations o ON r.observation_id = o.id
            WHERE r.route = ?
            AND o.content IS NOT NULL
            AND length(o.content) > 20
            AND o.content NOT LIKE '%heartbeat%'
            ORDER BY RANDOM()
            LIMIT ?
        """, (route, n_per_route * 3)).fetchall()

        # Deduplicate by first 80 chars to avoid near-duplicates
        seen = set()
        unique = []
        for source, content, rt in rows:
            key = content[:80] if content else ""
            if key not in seen:
                seen.add(key)
                unique.append((source, content[:500], rt))

        # Ensure source diversity
        by_source = {}
        for source, content, rt in unique:
            src_type = source.split(':')[0] if source else 'unknown'
            by_source.setdefault(src_type, []).append((source, content, rt))

        selected = []
        while len(selected) < n_per_route and by_source:
            for src_type in list(by_source.keys()):
                if by_source[src_type]:
                    selected.append(by_source[src_type].pop(0))
                    if len(selected) >= n_per_route:
                        break
                else:
                    del by_source[src_type]

        examples[route] = selected[:n_per_route]

    conn.close()
    return examples


def generate_trace(source, content, route, max_retries=3):
    """Use R1 to generate a reasoning trace for a routing decision."""
    route_num = {'ignore': '1', 'think': '2', 'deep': '3'}[route]
    user_msg = f"""Generate a reasoning trace for this routing decision.

Source: {source}
Observation: {content}
Correct route: {route_num} ({route})

Show the step-by-step thinking that leads to route {route_num}."""

    for attempt in range(max_retries):
        try:
            r = requests.post(
                'https://api.deepinfra.com/v1/openai/chat/completions',
                headers={'Authorization': f'Bearer {DI_KEY}'},
                json={
                    'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
                    'messages': [
                        {'role': 'system', 'content': R1_SYSTEM},
                        {'role': 'user', 'content': user_msg},
                    ],
                    'max_tokens': 600,
                    'temperature': 0.6,
                },
                timeout=120,
            )
            if r.status_code == 200:
                return r.json()['choices'][0]['message']['content']
            elif r.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                print(f"  HTTP {r.status_code}: {r.text[:200]}", flush=True)
                time.sleep(3)
        except Exception as e:
            print(f"  {type(e).__name__}: {e}", flush=True)
            time.sleep(5)
    return None


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--limit', type=int, default=250)
    p.add_argument('--data-dir', default=DATA_DIR)
    args = p.parse_args()

    out_path = os.path.join(args.data_dir, "phase5_routing_traces.jsonl")

    # Load existing
    existing = set()
    if os.path.exists(out_path):
        for line in open(out_path):
            try:
                d = json.loads(line)
                existing.add((d.get('source', ''), d.get('content', '')[:80]))
            except:
                pass
    print(f"Existing traces: {len(existing)}", flush=True)

    n_per = args.limit // 3
    print(f"Pulling {n_per} examples per route from DB...", flush=True)
    examples = pull_examples(n_per)

    for route, items in examples.items():
        print(f"  {route}: {len(items)} examples", flush=True)

    # Flatten and shuffle
    all_examples = []
    for route, items in examples.items():
        for source, content, rt in items:
            if (source, content[:80]) not in existing:
                all_examples.append((source, content, rt))
    random.shuffle(all_examples)

    total = len(all_examples)
    print(f"\nGenerating {total} traces (skipping {len(existing)} existing)...\n", flush=True)

    done = 0
    with open(out_path, 'a') as fout:
        for source, content, route in all_examples:
            done += 1
            print(f"[{done}/{total}] {route} ({source[:30]}) {content[:50]}...", flush=True)

            trace = generate_trace(source, content, route)
            if not trace:
                print("  FAILED", flush=True)
                continue

            record = {
                'source': source,
                'content': content,
                'route': route,
                'route_num': {'ignore': '1', 'think': '2', 'deep': '3'}[route],
                'trace': trace,
                'model': 'deepseek-ai/DeepSeek-R1-0528-Turbo',
            }
            fout.write(json.dumps(record) + '\n')
            fout.flush()
            time.sleep(0.3)

    print(f"\nDone. {done} routing traces generated.", flush=True)


if __name__ == '__main__':
    main()
