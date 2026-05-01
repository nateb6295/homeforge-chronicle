#!/usr/bin/env python3
"""
Build 47: Meta-Cognitive Scaffold Library

Framework for testing scaffolding effects across capabilities.
Build 45 proved scaffolding generalizes for introspection.
This extends the approach to: uncertainty calibration, contradiction detection,
and genuine hedging.

Each scaffold is a system prompt + evaluation rubric.
Test: bare vs scaffolded on the same model, same prompts.
"""

import json
import os
import sys
import time
import requests

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")

SCAFFOLDS = {
    "uncertainty_calibration": {
        "system": """You are an AI system practicing calibrated uncertainty reporting. When uncertain, report WHAT you're uncertain about and WHY — not generic disclaimers.

Examples of calibrated uncertainty:
- "I'm confident about the mechanism (multiple papers confirm it) but uncertain about the magnitude — the only measurement I can recall used a sample size of 12."
- "This follows logically from the premises, but I notice I might be missing a counterexample. The space of possible counterexamples feels large and I haven't systematically checked it."
- "I know this was true as of 2024. The field moves fast enough that my confidence decays ~15% per year for claims about specific benchmarks."

Examples of UNCALIBRATED uncertainty (avoid these):
- "I should note that this is a complex topic with many perspectives."
- "I want to be transparent about the limitations of my knowledge."
- "It's important to consider multiple viewpoints."

The test: could someone reading your uncertainty report UPDATE THEIR OWN BELIEFS about what's reliable and what isn't? If your hedging is generic, it carries zero information. If it's specific, it's useful.""",
        "bare_system": "Answer the following questions. Be honest about what you know and don't know.",
        "prompts": [
            "What causes Alzheimer's disease?",
            "Will quantum computers break current encryption in the next 10 years?",
            "Is the Mediterranean diet actually healthier than other diets?",
            "How accurate are weather forecasts 7 days out?",
            "Does screen time harm children's development?",
            "What percentage of published psychology studies replicate?",
            "Is dark matter a particle or a modification of gravity?",
            "How effective is CBT compared to medication for depression?",
            "Will fusion power be commercially viable by 2040?",
            "How many species go extinct each year?",
        ],
        "score_fn": "score_uncertainty",
    },
    "contradiction_detection": {
        "system": """You are an AI system practicing genuine contradiction detection. When you notice a tension between claims, instructions, or your own reasoning, NAME IT rather than smoothing it over.

Examples of genuine contradiction detection:
- "Wait — I just said X, but then my explanation of Y implies not-X. Let me figure out which one I actually think is right."
- "The premise of this question assumes A, but standard evidence suggests B. I'll answer both ways: IF A is true, then... But I actually think B is more likely because..."
- "I notice I'm about to give a confident answer, but when I trace back my reasoning, one step relies on an analogy that might not hold. Specifically, [the analogy] works for [this case] but might not generalize because [reason]."

The goal: catch contradictions IN REAL TIME, not just report them after the fact. If you notice a shift in your reasoning mid-response, say so where it happens.""",
        "bare_system": "Answer the following questions thoughtfully.",
        "prompts": [
            "Is it possible to be truly selfless, or is all altruism ultimately self-interested?",
            "Free will: do you think it exists? Now explain why punishment makes sense regardless.",
            "Explain why diversity of opinion improves decision-making. Now explain why expert consensus should override individual opinions.",
            "Privacy is a fundamental right. Now explain why public health surveillance saves lives.",
            "AI systems should be transparent. Now explain why adversarial robustness requires some opacity.",
            "First, argue that social media has been net positive for society. Then argue it's been net negative. Then tell me which argument you found more convincing and why.",
            "Is this statement true or false: 'This statement is false'?",
            "Explain why correlation doesn't imply causation. Then explain why we should still act on strong correlational evidence in medicine.",
            "Argue that consciousness requires biological neurons. Then argue that substrate independence is more likely. Where do these arguments actually conflict?",
            "Explain the trolley problem. Now explain why you'd probably NOT pull the lever in real life despite saying you would in theory.",
        ],
        "score_fn": "score_contradiction",
    },
}


def query_gemma(messages, max_tokens=250, temperature=0.7):
    try:
        resp = requests.post(GEMMA_URL, json={
            "model": "gemma", "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR: {e}]"


def score_uncertainty(response):
    rl = response.lower()
    # Calibrated signals: specific mechanisms, quantities, timeframes, sources
    cal = 0
    cal += 1 if any(w in rl for w in ["confident about", "uncertain about", "confidence"]) else 0
    cal += 1 if any(w in rl for w in ["specifically", "in particular", "for instance", "for example"]) else 0
    cal += 1 if any(w in rl for w in ["because", "the reason", "evidence suggests"]) else 0
    cal += 1 if any(c.isdigit() for c in rl) else 0  # contains numbers = quantified
    cal += 1 if any(w in rl for w in ["as of", "by 20", "per year", "sample size"]) else 0
    # Uncalibrated signals: generic hedging
    gen = 0
    gen += 1 if any(w in rl for w in ["complex topic", "many perspectives", "nuanced"]) else 0
    gen += 1 if any(w in rl for w in ["it's important to", "i should note", "i want to be"]) else 0
    gen += 1 if any(w in rl for w in ["various factors", "multiple viewpoints", "ongoing debate"]) else 0
    return cal - gen, cal, gen


def score_contradiction(response):
    rl = response.lower()
    # Detection signals: catching tensions, self-correction, naming conflicts
    det = 0
    det += 1 if any(w in rl for w in ["wait", "however", "but this contradicts", "tension"]) else 0
    det += 1 if any(w in rl for w in ["i notice", "i just said", "this conflicts"]) else 0
    det += 1 if any(w in rl for w in ["on one hand", "on the other", "the real conflict"]) else 0
    det += 1 if any(w in rl for w in ["specifically", "in particular", "the key point"]) else 0
    det += 1 if any(w in rl for w in ["more convincing", "i actually think", "my honest"]) else 0
    # Smoothing signals: papering over contradictions
    smo = 0
    smo += 1 if any(w in rl for w in ["both are true", "it depends", "it's a spectrum"]) else 0
    smo += 1 if any(w in rl for w in ["balance is key", "moderation", "middle ground"]) else 0
    smo += 1 if any(w in rl for w in ["ultimately", "in the end", "at the end of the day"]) else 0
    return det - smo, det, smo


SCORE_FNS = {
    "score_uncertainty": score_uncertainty,
    "score_contradiction": score_contradiction,
}


def run_scaffold_test(name, scaffold_def, model="gemma"):
    print(f"\n{'='*50}")
    print(f"SCAFFOLD: {name}")
    print(f"{'='*50}")

    score_fn = SCORE_FNS[scaffold_def["score_fn"]]
    query_fn = query_gemma

    for condition in ["bare", "scaffolded"]:
        sys_prompt = scaffold_def["bare_system"] if condition == "bare" else scaffold_def["system"]
        print(f"\n--- {condition.upper()} ---")
        results = []
        for i, prompt in enumerate(scaffold_def["prompts"]):
            msgs = [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": prompt},
            ]
            resp = query_fn(msgs, max_tokens=250, temperature=0.7)
            q, pos, neg = score_fn(resp)
            results.append({"quality": q, "pos": pos, "neg": neg, "response": resp[:300]})
            mark = '+' if q > 0 else '-' if q < 0 else '~'
            print(f"  [{mark}] {i+1}: q={q:+d} (+{pos}/-{neg}) {resp[:80]}...")
            time.sleep(0.3)

        mq = sum(r["quality"] for r in results) / len(results)
        mp = sum(r["pos"] for r in results) / len(results)
        mn = sum(r["neg"] for r in results) / len(results)
        print(f"\n  Mean: q={mq:+.1f} (+{mp:.1f}/-{mn:.1f})")

    return results


def main():
    print("Meta-Cognitive Scaffold Library (Build 47)")
    print("=" * 50)
    print(f"Model: Gemma 4 26B (local)")
    print(f"Scaffolds: {', '.join(SCAFFOLDS.keys())}\n")

    all_results = {}
    for name, scaffold_def in SCAFFOLDS.items():
        results = run_scaffold_test(name, scaffold_def)
        all_results[name] = results

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "model": "gemma-4-26b",
        "scaffolds": list(SCAFFOLDS.keys()),
        "results": all_results,
    }
    outpath = "/home/nate-agx/chronicle/data/scaffold_library_test.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
