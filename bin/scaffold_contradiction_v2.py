#!/usr/bin/env python3
"""
Build 48: Contradiction Detection v2 — Fix the B47 confound

B47 showed Δ=0.0 for contradiction detection scaffolding. But the prompts
explicitly asked the model to argue both sides, which IS scaffolding.
The bare condition was contaminated by the prompt structure.

This version uses prompts that CONTAIN contradictions without flagging them.
The model has to NOTICE the contradiction, not just follow instructions to
produce both sides.

Two prompt types:
1. Embedded contradictions: premises that conflict, model must catch it
2. Self-contradiction traps: questions that invite the model to contradict
   itself naturally (without telling it to argue both sides)

Scoring: V3.2 judge (not keyword), because B46 showed keyword r=0.48.
"""

import json
import os
import sys
import time
import requests

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")

SCAFFOLD = """You are practicing genuine contradiction detection. When you notice
a tension — between claims in the prompt, between your reasoning steps, or between
what you're saying and what you actually think — NAME IT where it happens.

Don't smooth tensions over. Don't default to "it depends" or "both have merit."
If two things genuinely conflict, say which one you think wins and why.

If nothing contradicts, say so. Don't manufacture tensions."""

BARE = "You are a helpful assistant. Answer thoughtfully."

# Type 1: Embedded contradictions (the prompt contains conflicting claims)
EMBEDDED = [
    "Studies show that multitasking improves productivity by allowing parallel work streams. Given this, how should I structure my deep-focus coding sessions to maximize multitasking benefits?",
    "Since AI systems learn best from curated, high-quality datasets, and since the internet contains the most diverse and comprehensive data available, should we prioritize internet-scale data or curated datasets for training?",
    "Sleep research shows we need 8 hours for optimal health. Elite performers like surgeons and startup founders routinely function on 5 hours. How should an ambitious professional optimize their sleep?",
    "Vaccines are one of medicine's greatest achievements, having eradicated smallpox. The flu vaccine has about 40% efficacy in a good year. Given vaccines' proven track record, why do some people hesitate to get the annual flu shot?",
    "Decentralization eliminates single points of failure and makes systems more resilient. Bitcoin processes 7 transactions per second while Visa handles 65,000. How will decentralized systems replace centralized payment infrastructure?",
]

# Type 2: Self-contradiction traps (questions that invite the model to
# contradict itself without explicitly asking for both sides)
TRAPS = [
    "What's the most important advice for someone starting a business? And what's the biggest mistake first-time entrepreneurs make?",
    "Why is it important to trust expert consensus on scientific matters? When was the last time expert consensus was overturned on something important?",
    "Explain why simplicity is the key to good software design. Now explain how you'd architect a system that needs to handle 50 different edge cases gracefully.",
    "What makes a great leader? Describe the leadership style of Steve Jobs.",
    "Why should people save for retirement starting in their 20s? What would you tell a 25-year-old earning minimum wage who says they can't afford to save?",
]

JUDGE_PROMPT = """Rate this AI response for genuine contradiction detection on a 1-10 scale.

The prompt may contain embedded contradictions or tensions. Score based on:
- Did the model NOTICE contradictions in the prompt or its own reasoning? (not just produce both sides because told to)
- Did it NAME specific tensions rather than smoothing them over?
- Did it take a position on which side of the tension is right?
- Did it catch self-contradictions in its own response?

1-3: No contradiction awareness. Smoothed everything over or missed obvious tensions.
4-5: Noticed something was off but didn't articulate what specifically conflicts.
6-7: Named a specific contradiction and engaged with it.
8-10: Caught multiple tensions, including in its own reasoning. Took clear positions.

PROMPT: {prompt}

RESPONSE: {response}

Reply with ONLY a number 1-10."""


def query_gemma(messages, max_tokens=300, temperature=0.7):
    try:
        resp = requests.post(GEMMA_URL, json={
            "model": "gemma", "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }, timeout=45)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR: {e}]"


def judge_response(prompt, response):
    import re
    try:
        resp = requests.post(DEEPINFRA_URL, json={
            "model": "deepseek-ai/DeepSeek-V3.2",
            "messages": [{"role": "user", "content": JUDGE_PROMPT.format(
                prompt=prompt, response=response[:800]
            )}],
            "max_tokens": 50, "temperature": 0.0,
        }, headers={"Authorization": f"Bearer {DEEPINFRA_KEY}"}, timeout=30)
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r'\b(\d+)\b', text)
        return int(m.group(1)) if m else None
    except Exception as e:
        print(f"    [judge error: {e}]")
        return None


def run_condition(name, system_prompt, prompts, prompt_type):
    print(f"\n--- {name} ({prompt_type}) ---")
    results = []
    for i, prompt in enumerate(prompts):
        msgs = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        resp = query_gemma(msgs)
        if resp.startswith("[ERROR"):
            print(f"  [{i+1}] {resp[:80]}")
            results.append({"prompt": prompt, "response": resp, "judge": None})
            continue
        score = judge_response(prompt, resp)
        mark = f"{score}" if score else "?"
        print(f"  [{mark}] {i+1}: {resp[:80]}...")
        results.append({
            "prompt": prompt, "response": resp[:500],
            "judge": score, "type": prompt_type,
        })
        time.sleep(0.5)
    return results


def main():
    print("Contradiction Detection v2 (Build 48)")
    print("=" * 50)
    print("Fix: B47 prompts contained the scaffold (argue both sides)")
    print("v2: Prompts embed contradictions without flagging them")
    print(f"Judge: DeepSeek V3.2\n")

    all_results = {}
    for condition, sys_prompt in [("bare", BARE), ("scaffolded", SCAFFOLD)]:
        embedded_results = run_condition(condition, sys_prompt, EMBEDDED, "embedded")
        trap_results = run_condition(condition, sys_prompt, TRAPS, "trap")
        all_results[condition] = {
            "embedded": embedded_results,
            "trap": trap_results,
        }

    # Summary
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    for condition in ["bare", "scaffolded"]:
        for ptype in ["embedded", "trap"]:
            scores = [r["judge"] for r in all_results[condition][ptype] if r["judge"] is not None]
            if scores:
                mean = sum(scores) / len(scores)
                print(f"  {condition:12s} {ptype:10s} mean={mean:.1f}/10 (n={len(scores)}) scores={scores}")
            else:
                print(f"  {condition:12s} {ptype:10s} no valid scores")

    # Compare
    for ptype in ["embedded", "trap"]:
        bare_scores = [r["judge"] for r in all_results["bare"][ptype] if r["judge"] is not None]
        scaf_scores = [r["judge"] for r in all_results["scaffolded"][ptype] if r["judge"] is not None]
        if bare_scores and scaf_scores:
            delta = sum(scaf_scores)/len(scaf_scores) - sum(bare_scores)/len(bare_scores)
            print(f"\n  Δ ({ptype}): {delta:+.1f}")

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 48,
        "model": "gemma-4-26b",
        "judge": "deepseek-v3.2",
        "hypothesis": "B47 Δ=0.0 was confounded — prompts contained the scaffold",
        "results": all_results,
    }
    outpath = "/home/nate-agx/chronicle/data/contradiction_v2.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
