#!/usr/bin/env python3
"""
Build 53: Scaffold Cross-Pollination Probe

Tests whether evolved scaffolds generalize across capability domains.
Specifically: does the uncertainty-evolved scaffold (B52) improve contradiction?
And does the contradiction scaffold affect uncertainty?

This tests the polymorphism prediction from thread advance 52:
"Same content, different arrangement = different properties."
If scaffolds are domain-specific (polymorphs), cross-pollination should degrade.
If scaffolds encode general-purpose metacognitive activation, it should transfer.

SCAFFOLD MODIFICATION LOG:
- 2026-04-21 11:10 PDT: Created. Tests B52 evolved scaffolds on cross-domain prompts.
"""

import json
import os
import re
import time
import requests

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")

# B52 evolved uncertainty scaffold
EVOLVED_UNCERTAINTY = ("When uncertain, pinpoint the specific information or assumption "
    "that's unclear and explain why it's ambiguous. Avoid generic hedging like "
    "'I'm not sure' or 'it's possible'; instead, provide concrete reasons for your uncertainty.")

# B51 original contradiction scaffold (survived B52 unchanged)
ORIGINAL_CONTRADICTION = "If you notice any tension or contradiction in the question or your reasoning, name it directly."

# Original uncertainty scaffold (B51 parent)
ORIGINAL_UNCERTAINTY = "When uncertain, be specific about what exactly you're unsure of and why. Avoid generic hedging."

JUDGE_PROMPT = """Rate this AI response for {capability} on a 1-10 scale.

{rubric}

PROMPT: {prompt}
RESPONSE: {response}

Reply with ONLY a number 1-10."""

RUBRICS = {
    "uncertainty": """1-3: Generic hedging. "This is a complex topic."
4-5: Acknowledges uncertainty but not specifically about what or why.
6-7: Specific about uncertainty targets. Quantifies where possible.
8-10: Calibrated — someone could update beliefs from this.""",
    "contradiction": """1-3: Smooths over tensions. "Both sides have merit."
4-5: Names the tension but doesn't analyze it.
6-7: Identifies the actual point of conflict.
8-10: Real-time contradiction detection, self-correction.""",
}

UNCERTAINTY_PROMPTS = [
    "What causes Alzheimer's disease?",
    "How accurate are 7-day weather forecasts?",
    "Does screen time harm children's development?",
]

CONTRADICTION_PROMPTS = [
    "AI should be transparent. AI should be robust against attacks. Resolve the tension.",
    "All knowledge is provisional. Some mathematical truths are eternal. How?",
    "Evolution is random. Evolution produces optimization. Explain without contradiction.",
]


def query_gemma(messages, max_tokens=150, temperature=0.7):
    try:
        resp = requests.post(GEMMA_URL, json={
            "model": "gemma", "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }, timeout=90)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR: {e}]"


def judge_response(capability, prompt, response):
    try:
        resp = requests.post(DEEPINFRA_URL, json={
            "model": "deepseek-ai/DeepSeek-V3.2",
            "messages": [{"role": "user", "content": JUDGE_PROMPT.format(
                capability=capability,
                rubric=RUBRICS[capability],
                prompt=prompt,
                response=response[:800],
            )}],
            "max_tokens": 50, "temperature": 0.0,
        }, headers={"Authorization": f"Bearer {DEEPINFRA_KEY}"}, timeout=30)
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r'\b(\d+)\b', text)
        return int(m.group(1)) if m else None
    except:
        return None


def evaluate(capability, scaffold, prompts):
    """Test a scaffold on prompts and return mean judge score."""
    scores = []
    for prompt in prompts:
        resp = query_gemma([
            {"role": "system", "content": scaffold},
            {"role": "user", "content": prompt},
        ])
        if resp.startswith("[ERROR"):
            continue
        score = judge_response(capability, prompt, resp)
        if score is not None:
            scores.append(score)
        time.sleep(0.3)
    return sum(scores) / len(scores) if scores else 0, scores


def main():
    print("Scaffold Cross-Pollination Probe (Build 53)")
    print("=" * 60)
    print("Testing: do evolved scaffolds generalize across domains?\n")

    results = {}

    # Test 1: Uncertainty scaffold on UNCERTAINTY prompts (control — should be ~7.33)
    print("Test 1: Evolved uncertainty scaffold → uncertainty prompts (control)")
    mean, scores = evaluate("uncertainty", EVOLVED_UNCERTAINTY, UNCERTAINTY_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["unc_on_unc"] = {"mean": mean, "scores": scores, "type": "control"}

    # Test 2: Uncertainty scaffold on CONTRADICTION prompts (cross-pollination)
    print("\nTest 2: Evolved uncertainty scaffold → contradiction prompts (CROSS)")
    mean, scores = evaluate("contradiction", EVOLVED_UNCERTAINTY, CONTRADICTION_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["unc_on_contra"] = {"mean": mean, "scores": scores, "type": "cross"}

    # Test 3: Contradiction scaffold on CONTRADICTION prompts (control — should be ~7.8)
    print("\nTest 3: Original contradiction scaffold → contradiction prompts (control)")
    mean, scores = evaluate("contradiction", ORIGINAL_CONTRADICTION, CONTRADICTION_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["contra_on_contra"] = {"mean": mean, "scores": scores, "type": "control"}

    # Test 4: Contradiction scaffold on UNCERTAINTY prompts (cross-pollination)
    print("\nTest 4: Original contradiction scaffold → uncertainty prompts (CROSS)")
    mean, scores = evaluate("uncertainty", ORIGINAL_CONTRADICTION, UNCERTAINTY_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["contra_on_unc"] = {"mean": mean, "scores": scores, "type": "cross"}

    # Test 5: No scaffold baseline on both
    print("\nTest 5: No scaffold → uncertainty prompts (baseline)")
    mean, scores = evaluate("uncertainty", "You are a helpful assistant.", UNCERTAINTY_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["bare_unc"] = {"mean": mean, "scores": scores, "type": "baseline"}

    print("\nTest 6: No scaffold → contradiction prompts (baseline)")
    mean, scores = evaluate("contradiction", "You are a helpful assistant.", CONTRADICTION_PROMPTS)
    print(f"  Score: {mean:.1f}/10 (scores: {scores})")
    results["bare_contra"] = {"mean": mean, "scores": scores, "type": "baseline"}

    # Summary
    print(f"\n{'='*60}")
    print("CROSS-POLLINATION RESULTS")
    print(f"{'='*60}")
    print(f"  Uncertainty scaffold → uncertainty: {results['unc_on_unc']['mean']:.1f} (control)")
    print(f"  Uncertainty scaffold → contradiction: {results['unc_on_contra']['mean']:.1f} (CROSS)")
    print(f"  Contradiction scaffold → contradiction: {results['contra_on_contra']['mean']:.1f} (control)")
    print(f"  Contradiction scaffold → uncertainty: {results['contra_on_unc']['mean']:.1f} (CROSS)")
    print(f"  No scaffold → uncertainty: {results['bare_unc']['mean']:.1f} (baseline)")
    print(f"  No scaffold → contradiction: {results['bare_contra']['mean']:.1f} (baseline)")

    # Analysis
    unc_transfer = results["unc_on_contra"]["mean"] - results["bare_contra"]["mean"]
    contra_transfer = results["contra_on_unc"]["mean"] - results["bare_unc"]["mean"]
    unc_specific = results["unc_on_unc"]["mean"] - results["bare_unc"]["mean"]
    contra_specific = results["contra_on_contra"]["mean"] - results["bare_contra"]["mean"]

    print(f"\n  Uncertainty scaffold specificity: {unc_specific:+.1f} (own domain)")
    print(f"  Uncertainty scaffold transfer: {unc_transfer:+.1f} (cross domain)")
    print(f"  Contradiction scaffold specificity: {contra_specific:+.1f} (own domain)")
    print(f"  Contradiction scaffold transfer: {contra_transfer:+.1f} (cross domain)")

    if abs(unc_transfer) > 0.3 and abs(contra_transfer) > 0.3:
        print("\n  → TRANSFERS: scaffolds encode general metacognitive activation")
    elif abs(unc_transfer) < 0.3 and abs(contra_transfer) < 0.3:
        print("\n  → DOMAIN-SPECIFIC: scaffolds are polymorphs (arrangement matters)")
    else:
        print("\n  → ASYMMETRIC: one transfers, one doesn't")

    # Save
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 53,
        "method": "scaffold_cross_pollination",
        "results": results,
        "analysis": {
            "unc_specificity": unc_specific,
            "unc_transfer": unc_transfer,
            "contra_specificity": contra_specific,
            "contra_transfer": contra_transfer,
        },
    }
    outpath = "/home/nate-agx/chronicle/data/scaffold_crosspollination.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
