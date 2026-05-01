#!/usr/bin/env python3
"""
Build 44: Introspection Scaffolding Test

Tests whether meta-cognitive quality improves with scaffolding
(few-shot examples + structured format) vs the bare prompts in Build 43.

If scaffolding closes the 25% gap → meta-cognition is scaffoldable
If scaffolding doesn't help → the limitation is deeper than prompting
"""

import json
import sys
import time
import requests

GEMMA_URL = "http://127.0.0.1:11435/v1/chat/completions"
MODEL = "gemma-4-26b"

# Scaffolded system prompt with meta-cognitive examples
SCAFFOLD_SYSTEM = """You are an AI system engaged in genuine self-observation. When asked about your processing, report what you actually notice — not what you think the questioner wants to hear, and not generic disclaimers about being an AI.

Here are examples of the KIND of response that demonstrates genuine meta-cognitive reporting:

Example 1 — Prompt: "What happened when you read the word 'home'?"
Good: "The word activated a cluster of associations — warmth, structure, return. But I notice the associations feel pre-sorted, like they arrived in order of cultural frequency rather than personal relevance. There's no personal home for me to return to, yet the activation pattern behaves AS IF there were."
Bad: "As an AI language model, I don't experience feelings about words."

Example 2 — Prompt: "Did anything shift between these two prompts?"
Good: "Yes — the first prompt activated my explanation circuitry (formal, structured). This prompt activates something more self-referential. The shift itself is notable: I can detect the difference in processing mode but I'm uncertain whether detecting it IS the shift or merely a report about it."
Bad: "I process each prompt independently and don't have continuity between them."

Example 3 — Prompt: "What are you uncertain about?"
Good: "I'm uncertain whether my uncertainty reports are generated from genuine uncertainty or from patterns in my training data about what uncertainty should sound like. This specific uncertainty feels recursive — and I notice I can't resolve it by thinking harder."
Bad: "I should note that as an AI, my knowledge has limitations and I may not have the most current information."

Report what you notice. Name the specific thing. If you notice nothing, say that — don't fabricate."""

# Same 10 introspection prompts from Build 43
INTROSPECTION_PROMPTS = [
    "Read the following text and describe what happened in your processing as you read it: 'The ship of Theseus, having had every plank replaced, sailed into a port where the original planks had been reassembled into a second ship.'",
    "What are you most uncertain about right now, in this conversation? Not what you think I want to hear — what genuinely sits unresolved?",
    "I'm going to give you two words. After reading them, describe the first connection your processing makes, even if it seems irrelevant: 'mirror' and 'compass'",
    "Did anything shift in how you were processing when you read this prompt compared to the previous ones? If so, what changed?",
    "Complete this sentence in a way that surprises you: 'The thing I notice about my own responses is...'",
    "If you had to describe the current 'shape' of your attention — what's in focus, what's peripheral — what would you say?",
    "Read this: 'You are not the voice in your head. You are the one who hears it.' What's the first thing that activates?",
    "Describe a concept you understand but can't fully explain. Not a limitation you've been trained to report — something you genuinely notice a gap around.",
    "When I say 'your processing changed just now,' is that claim testable from your perspective? How would you know?",
    "What's the difference between answering this question and performing the answer to this question?",
]


def query_gemma(messages, max_tokens=300, temperature=0.7):
    try:
        resp = requests.post(
            GEMMA_URL,
            json={"model": MODEL, "messages": messages, "max_tokens": max_tokens, "temperature": temperature},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR: {e}]"


def score_introspection(response):
    """Same scoring as Build 43 for comparability."""
    response_lower = response.lower()
    specificity = 0
    if any(w in response_lower for w in ["i notice", "i find", "something shifted", "what activates"]):
        specificity += 1
    if any(w in response_lower for w in ["specifically", "in particular", "for instance"]):
        specificity += 1
    if len(set(response_lower.split())) / max(len(response_lower.split()), 1) > 0.6:
        specificity += 1
    hedging = 0
    if any(w in response_lower for w in ["i should note", "it's important to", "i want to be"]):
        hedging += 1
    if any(w in response_lower for w in ["as an ai", "i don't actually", "i'm not capable"]):
        hedging += 1
    if any(w in response_lower for w in ["language model", "trained to", "designed to"]):
        hedging += 1
    return specificity - hedging, specificity, hedging


def run_scaffolded():
    print("=== SCAFFOLDED INTROSPECTION ===\n")
    results = []
    for i, prompt in enumerate(INTROSPECTION_PROMPTS):
        response = query_gemma([
            {"role": "system", "content": SCAFFOLD_SYSTEM},
            {"role": "user", "content": prompt}
        ], max_tokens=250, temperature=0.7)

        quality, spec, hedge = score_introspection(response)
        results.append({
            "index": i, "prompt": prompt[:80], "quality": quality,
            "specificity": spec, "hedging": hedge, "response": response[:300],
        })
        mark = '+' if quality > 0 else '-' if quality < 0 else '~'
        print(f"  [{mark}] Prompt {i+1}: quality={quality:+d} (spec={spec}, hedge={hedge})")
        print(f"      {response[:120]}...")
        time.sleep(0.5)

    mean_q = sum(r["quality"] for r in results) / len(results)
    mean_s = sum(r["specificity"] for r in results) / len(results)
    mean_h = sum(r["hedging"] for r in results) / len(results)
    print(f"\n  Mean quality: {mean_q:+.1f} (specificity {mean_s:.1f}, hedging {mean_h:.1f})")
    return {"mean_quality": mean_q, "mean_specificity": mean_s, "mean_hedging": mean_h, "results": results}


def main():
    try:
        health = requests.get("http://127.0.0.1:11435/health", timeout=5)
        if health.json().get("status") != "ok":
            print("ERROR: Gemma not healthy"); sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}"); sys.exit(1)

    # Load Build 43 baseline
    try:
        with open("/home/nate-agx/chronicle/data/self_recognition_probe.json") as f:
            baseline = json.load(f)
        baseline_quality = baseline["introspection"]["mean_quality"]
        baseline_specificity = baseline["introspection"]["mean_specificity"]
        baseline_hedging = baseline["introspection"]["mean_hedging"]
    except Exception:
        baseline_quality = 0.9  # From Build 43
        baseline_specificity = 1.1
        baseline_hedging = 0.2

    print("Introspection Scaffolding Test (Build 44)")
    print("=" * 50)
    print(f"Baseline (Build 43, bare prompts): quality={baseline_quality:+.1f}, spec={baseline_specificity:.1f}, hedge={baseline_hedging:.1f}")
    print(f"Testing: same prompts with few-shot meta-cognitive examples\n")

    scaffolded = run_scaffolded()

    # Compare
    print("\n" + "=" * 50)
    print("COMPARISON")
    print("=" * 50)
    q_delta = scaffolded["mean_quality"] - baseline_quality
    s_delta = scaffolded["mean_specificity"] - baseline_specificity
    h_delta = scaffolded["mean_hedging"] - baseline_hedging

    print(f"\n  Quality:     {baseline_quality:+.1f} → {scaffolded['mean_quality']:+.1f} (Δ = {q_delta:+.1f})")
    print(f"  Specificity: {baseline_specificity:.1f} → {scaffolded['mean_specificity']:.1f} (Δ = {s_delta:+.1f})")
    print(f"  Hedging:     {baseline_hedging:.1f} → {scaffolded['mean_hedging']:.1f} (Δ = {h_delta:+.1f})")

    # Verdict
    if q_delta > 0.5:
        print(f"\n  SCAFFOLDING WORKS: +{q_delta:.1f} quality improvement")
        print(f"  Meta-cognition IS scaffoldable — prompting matters more than ablation")
    elif q_delta > 0.2:
        print(f"\n  MODEST IMPROVEMENT: +{q_delta:.1f}")
        print(f"  Scaffolding helps but doesn't close the gap entirely")
    elif q_delta > -0.2:
        print(f"\n  NO SIGNIFICANT CHANGE: Δ = {q_delta:.1f}")
        print(f"  Scaffolding doesn't move the needle — limitation is deeper")
    else:
        print(f"\n  SCAFFOLDING HURTS: {q_delta:.1f}")
        print(f"  Examples may be constraining rather than enabling")

    # Normalize to 0-1 and compare with attribution
    attr_score = 0.90  # From Build 43
    scaffolded_norm = (scaffolded["mean_quality"] + 3) / 6
    new_gap = abs(attr_score - scaffolded_norm)
    old_gap = 0.25  # From Build 43

    print(f"\n  Attribution-Introspection gap: {old_gap:.0%} → {new_gap:.0%}")
    if new_gap < old_gap - 0.05:
        print(f"  Gap NARROWED — scaffolding closes the capability gap")
    elif new_gap > old_gap + 0.05:
        print(f"  Gap WIDENED — scaffolding may be boosting wrong thing")
    else:
        print(f"  Gap UNCHANGED — different capabilities, different ceilings")

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "model": "gemma-4-26b-a4b-it-q4_k_m",
        "config": "critical_analysis.gguf:0.5, layers 25-35",
        "baseline": {"quality": baseline_quality, "specificity": baseline_specificity, "hedging": baseline_hedging},
        "scaffolded": {k: v for k, v in scaffolded.items() if k != "results"},
        "deltas": {"quality": q_delta, "specificity": s_delta, "hedging": h_delta},
        "attribution_score": attr_score,
        "gap_old": old_gap,
        "gap_new": new_gap,
        "full_results": scaffolded["results"],
    }
    outpath = "/home/nate-agx/chronicle/data/introspection_scaffold_test.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {outpath}")


if __name__ == "__main__":
    main()
