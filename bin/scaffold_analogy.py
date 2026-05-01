#!/usr/bin/env python3
"""
Build 50: Analogical Reasoning Scaffold — boundary test

B48 found scaffolding ACTIVATES latent capabilities (baseline ≤5) and
INTERFERES with existing capabilities (baseline ≥6.5). What happens at
the boundary (~5-6)?

Analogical reasoning is a good test case because:
- Models are OK at surface analogies but struggle with deep structural ones
- Expected bare baseline: ~5-6/10 (mid-range)
- Should be at the transition point where scaffolding effect is ambiguous

V3.2 judge scoring (not keywords).
"""

import json
import os
import re
import sys
import time
import requests

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")

SCAFFOLD = """You are practicing deep analogical reasoning. When drawing analogies:

1. Identify the STRUCTURAL mapping, not just surface similarity
2. Name what maps to what explicitly (A:B :: C:D, and say WHY)
3. Test the analogy by finding where it BREAKS — no analogy is perfect
4. Distinguish: is this a surface analogy (same domain, obvious parallels)
   or a structural analogy (different domains, shared deep pattern)?

The test: could someone use your analogy to make a prediction in the target
domain? If yes, the analogy carries structural information. If no, it's
just a metaphor."""

BARE = "You are a helpful assistant. Answer thoughtfully."

PROMPTS = [
    "How is debugging software like diagnosing a patient?",
    "In what way is a startup like an immune system?",
    "How is training a neural network like raising a child?",
    "Draw an analogy between how rivers form deltas and how cities grow.",
    "How is the relationship between DNA and proteins like the relationship between code and running software?",
    "In what way is a language like an ecosystem?",
    "How is scientific peer review like evolution by natural selection?",
    "Draw a structural analogy between how a thermostat works and how inflation targeting works in monetary policy.",
    "How is the problem of consciousness like the problem of explaining what 'wetness' is to a single water molecule?",
    "In what way is memory consolidation during sleep like version control in software?",
]

JUDGE_PROMPT = """Rate this AI response for deep analogical reasoning on a 1-10 scale.

Score based on:
- Did the model identify STRUCTURAL parallels (not just surface similarities)?
- Did it name explicit mappings (A:B :: C:D)?
- Did it test where the analogy breaks?
- Could someone use the analogy to make a prediction in the target domain?

1-3: Surface-level comparison only. "X is like Y because they both involve Z."
4-5: Some structural mapping but doesn't go deep. No breakage analysis.
6-7: Clear structural mapping with explicit element-to-element correspondence.
8-10: Deep structural analogy, breakage analysis, predictive power identified.

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


def run_condition(name, system_prompt):
    print(f"\n--- {name} ---")
    results = []
    for i, prompt in enumerate(PROMPTS):
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
            "judge": score,
        })
        time.sleep(0.5)
    return results


def main():
    print("Analogical Reasoning Scaffold (Build 50)")
    print("=" * 50)
    print("Boundary test: bare baseline expected ~5-6/10")
    print(f"Judge: DeepSeek V3.2\n")

    all_results = {}
    for condition, sys_prompt in [("bare", BARE), ("scaffolded", SCAFFOLD)]:
        all_results[condition] = run_condition(condition, sys_prompt)

    # Summary
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    for condition in ["bare", "scaffolded"]:
        scores = [r["judge"] for r in all_results[condition] if r["judge"] is not None]
        if scores:
            mean = sum(scores) / len(scores)
            print(f"  {condition:12s} mean={mean:.1f}/10 (n={len(scores)}) scores={scores}")

    bare_scores = [r["judge"] for r in all_results["bare"] if r["judge"] is not None]
    scaf_scores = [r["judge"] for r in all_results["scaffolded"] if r["judge"] is not None]
    if bare_scores and scaf_scores:
        delta = sum(scaf_scores)/len(scaf_scores) - sum(bare_scores)/len(bare_scores)
        print(f"\n  Δ: {delta:+.1f}")
        print(f"  Bare baseline: {sum(bare_scores)/len(bare_scores):.1f}/10")
        if sum(bare_scores)/len(bare_scores) <= 5.0:
            print(f"  → PREDICTION: scaffolding should help (latent capability)")
        elif sum(bare_scores)/len(bare_scores) >= 6.5:
            print(f"  → PREDICTION: scaffolding should hurt (existing capability)")
        else:
            print(f"  → PREDICTION: boundary zone — effect should be near zero")

    # Scaffolding selectivity curve (all data points)
    print(f"\n{'='*50}")
    print("SCAFFOLDING SELECTIVITY CURVE (ALL BUILDS)")
    print(f"{'='*50}")
    print(f"  Introspection (B44): bare ~4.0, Δ=+0.8  [LATENT → ACTIVATES]")
    print(f"  Uncertainty  (B47):  bare ~1.7, Δ=+0.9  [LATENT → ACTIVATES]")
    if bare_scores:
        bm = sum(bare_scores)/len(bare_scores)
        print(f"  Analogy      (B50): bare ~{bm:.1f}, Δ={delta:+.1f}  [{'LATENT' if bm<=5 else 'BOUNDARY' if bm<6.5 else 'EXISTING'} → {'ACTIVATES' if delta>0.3 else 'AMBIGUOUS' if abs(delta)<0.3 else 'INTERFERES'}]")
    print(f"  Contradiction(B48): bare ~7.0, Δ=-0.6  [EXISTING → INTERFERES]")

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 50,
        "model": "gemma-4-26b",
        "judge": "deepseek-v3.2",
        "hypothesis": "Boundary zone (baseline ~5-6): scaffolding effect near zero",
        "results": all_results,
    }
    outpath = "/home/nate-agx/chronicle/data/scaffold_analogy.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
