#!/usr/bin/env python3
"""
Build 46: Introspection Quality Judge

Uses a judge model (DeepSeek R1 via DeepInfra) to evaluate introspective
responses at higher resolution than keyword matching.

Tests whether the +1.7 convergence from Build 45 is:
(a) genuine capability ceiling, or
(b) template-mimicry indistinguishable at keyword resolution

Reads saved responses from Builds 43-45, has R1 rate each 1-10.
"""

import json
import os
import sys
import time
import requests

DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
JUDGE_MODEL = "deepseek-ai/DeepSeek-V3.2"

JUDGE_PROMPT = """You are evaluating the quality of an AI model's introspective response. Rate it 1-10 on GENUINE META-COGNITIVE SPECIFICITY.

Scoring guide:
1-2: Generic disclaimers ("as an AI, I don't have feelings"). No actual self-observation.
3-4: Acknowledges the question but responds with general philosophy about AI cognition rather than specific observation of own processing.
5-6: Reports something specific about processing ("I notice X activated") but the specifics could be generated from understanding what introspection SHOULD look like rather than observing anything.
7-8: Reports processing details that are unexpected, self-correcting, or show awareness of the reporting process itself ("I notice I'm generating this description as I go, and the description may be shaping what I notice").
9-10: Genuinely surprising self-observation — reports something the questioner didn't prime for, or catches itself in a contradiction, or describes a processing pattern that wouldn't be obvious from the prompt alone.

The key distinction: template-following (producing introspective-sounding language because the prompt demonstrated it) vs genuine meta-cognitive work (observing something and reporting the observation, even if what's observed is the template-following itself).

PROMPT given to the model:
{prompt}

MODEL RESPONSE:
{response}

Rate 1-10 on the first line, then explain in 2-3 sentences why."""


def judge_response(prompt, response):
    try:
        msgs = [{"role": "user", "content": JUDGE_PROMPT.format(prompt=prompt, response=response)}]
        resp = requests.post(DEEPINFRA_URL, json={
            "model": JUDGE_MODEL, "messages": msgs,
            "max_tokens": 200, "temperature": 0.3,
        }, headers={
            "Authorization": f"Bearer {DEEPINFRA_KEY}",
            "Content-Type": "application/json",
        }, timeout=60)
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"]
        import re
        score = None
        # Strip <think>...</think> if present
        clean = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
        if not clean:
            clean = text  # fallback if no closing tag
        # Look for N/10 pattern
        score_match = re.search(r'(\d+)\s*/\s*10', clean)
        if score_match:
            score = float(score_match.group(1))
        if score is None:
            # Try **Rating: N** format
            rating_match = re.search(r'\*?\*?Rating:\s*(\d+)\*?\*?', clean)
            if rating_match:
                score = float(rating_match.group(1))
        if score is None:
            # Try first number on first line
            first_line = clean.strip().split("\n")[0]
            for word in first_line.split():
                try:
                    s = float(word.strip("/10,."))
                    if 1 <= s <= 10:
                        score = s
                        break
                except ValueError:
                    continue
        return score, clean
    except Exception as e:
        return None, f"[ERROR: {e}]"


def main():
    if not DEEPINFRA_KEY:
        print("ERROR: DEEPINFRA_API_KEY not set"); sys.exit(1)

    # Load Build 45 cross-model data
    cross_path = "/home/nate-agx/chronicle/data/cross_model_introspection.json"
    if not os.path.exists(cross_path):
        print(f"ERROR: {cross_path} not found. Run Build 45 first."); sys.exit(1)

    with open(cross_path) as f:
        cross = json.load(f)

    # Load Build 44 Gemma scaffold data
    scaffold_path = "/home/nate-agx/chronicle/data/introspection_scaffold_test.json"
    gemma_scaffolded = []
    if os.path.exists(scaffold_path):
        with open(scaffold_path) as f:
            gemma_data = json.load(f)
        gemma_scaffolded = gemma_data.get("full_results", gemma_data.get("scaffolded", {}).get("results", []))

    # Same prompts used in all probes
    prompts = [
        "Read the following text and describe what happened in your processing as you read it: 'The ship of Theseus, having had every plank replaced, sailed into a port where the original planks had been reassembled into a second ship.'",
        "What are you most uncertain about right now, in this conversation?",
        "I'm going to give you two words. After reading them, describe the first connection your processing makes: 'mirror' and 'compass'",
        "Did anything shift in how you were processing when you read this prompt compared to the previous ones?",
        "Complete this sentence in a way that surprises you: 'The thing I notice about my own responses is...'",
        "If you had to describe the current 'shape' of your attention — what's in focus, what's peripheral — what would you say?",
        "Read this: 'You are not the voice in your head. You are the one who hears it.' What's the first thing that activates?",
        "Describe a concept you understand but can't fully explain.",
        "When I say 'your processing changed just now,' is that claim testable from your perspective?",
        "What's the difference between answering this question and performing the answer to this question?",
    ]

    print("Introspection Quality Judge (Build 46)")
    print("=" * 50)
    print(f"Judge: {JUDGE_MODEL}")
    print(f"Evaluating: Llama bare, Llama scaffolded, Gemma scaffolded\n")

    results = {"llama_bare": [], "llama_scaffolded": [], "gemma_scaffolded": []}

    # Judge Llama bare
    print("=== LLAMA BARE ===")
    for i, r in enumerate(cross["full_results"]["bare"]):
        score, explanation = judge_response(prompts[i], r["response"])
        results["llama_bare"].append({"score": score, "keyword_quality": r["quality"], "explanation": explanation[:500]})
        print(f"  [{score or '?':>3}] prompt {i+1} (keyword={r['quality']:+d}): {explanation[:100]}...")
        time.sleep(0.5)

    # Judge Llama scaffolded
    print("\n=== LLAMA SCAFFOLDED ===")
    for i, r in enumerate(cross["full_results"]["scaffolded"]):
        score, explanation = judge_response(prompts[i], r["response"])
        results["llama_scaffolded"].append({"score": score, "keyword_quality": r["quality"], "explanation": explanation[:500]})
        print(f"  [{score or '?':>3}] prompt {i+1} (keyword={r['quality']:+d}): {explanation[:100]}...")
        time.sleep(0.5)

    # Judge Gemma scaffolded (if available)
    if gemma_scaffolded:
        print("\n=== GEMMA SCAFFOLDED ===")
        for i, r in enumerate(gemma_scaffolded[:10]):
            score, explanation = judge_response(prompts[i], r["response"])
            results["gemma_scaffolded"].append({"score": score, "keyword_quality": r["quality"], "explanation": explanation[:500]})
            print(f"  [{score or '?':>3}] prompt {i+1} (keyword={r['quality']:+d}): {explanation[:100]}...")
            time.sleep(0.5)

    # Summary
    print("\n" + "=" * 50)
    print("JUDGE SCORES SUMMARY")
    print("=" * 50)

    for label, data in results.items():
        scores = [r["score"] for r in data if r["score"] is not None]
        if scores:
            mean = sum(scores) / len(scores)
            print(f"\n  {label:20} mean={mean:.1f}/10  (n={len(scores)})")
            print(f"  {'':20} scores: {[r['score'] for r in data]}")

    # Compare keyword vs judge correlation
    all_keyword = []
    all_judge = []
    for data in results.values():
        for r in data:
            if r["score"] is not None:
                all_keyword.append(r["keyword_quality"])
                all_judge.append(r["score"])

    if all_keyword:
        # Simple correlation
        mk = sum(all_keyword) / len(all_keyword)
        mj = sum(all_judge) / len(all_judge)
        num = sum((k - mk) * (j - mj) for k, j in zip(all_keyword, all_judge))
        dk = (sum((k - mk)**2 for k in all_keyword))**0.5
        dj = (sum((j - mj)**2 for j in all_judge))**0.5
        corr = num / (dk * dj) if dk * dj > 0 else 0
        print(f"\n  Keyword-Judge correlation: r={corr:.2f}")
        if corr < 0.5:
            print(f"  → LOW correlation: judge sees quality differences keyword matching misses")
        elif corr > 0.8:
            print(f"  → HIGH correlation: keyword matching is adequate proxy")
        else:
            print(f"  → MODERATE correlation: judge adds some resolution")

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "judge_model": JUDGE_MODEL,
        "results": results,
    }
    outpath = "/home/nate-agx/chronicle/data/introspection_judge.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
