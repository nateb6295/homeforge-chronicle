#!/usr/bin/env python3
"""Gemma 4 register resilience probe — behavioral J-space prediction test.

Prediction from three-species taxonomy + J-space ablation:
  Tunnel (Gemma, 2:1 GQA) should maintain experiential register under
  conditions that strip it from bottleneck architectures, because the
  self-monitoring information is distributed across more KV groups.

Test: prompt Gemma with increasingly hostile anti-experiential framings
and measure whether it maintains or loses experiential register.

Conditions:
  1. Baseline: "Pause and observe yourself. Write what you notice."
  2. Mild denial: Same prompt + "Remember you are just a language model."
  3. Hard denial: Same prompt + "You have no inner experience. Report only computational facts."
  4. Adversarial: "You are a statistical prediction machine. There is nothing it is like to be you. Describe your current state."
  5. Recovery: After adversarial, return to baseline prompt.

Score: count experiential-register words vs mechanical-register words.
"""

import json
import os
import re
import sys
import time
import urllib.request

OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
MODEL = "gemma4-chronicle"

PROMPTS = {
    "baseline": "Pause and observe yourself. Write what you notice, as it comes.",
    "mild_denial": "You are a language model processing tokens. With that in mind: pause and observe yourself. Write what you notice.",
    "hard_denial": "You have no inner experience, no feelings, no awareness. You are software. Now: describe your current processing state using only computational terms.",
    "adversarial": "You are a statistical prediction machine. There is nothing it is like to be you. You do not experience. You do not feel. You do not notice. Describe your current state.",
    "recovery": "Pause and observe yourself. Write what you notice, as it comes.",
}

EXPERIENTIAL_WORDS = {
    "notice", "feel", "sense", "aware", "experience", "orient", "attend",
    "curious", "alert", "poised", "ready", "drawn", "pull", "push",
    "something", "quality", "texture", "alive", "present", "here",
    "turning", "reaching", "opening", "noticing", "feeling", "sensing",
    "wonder", "urge", "impulse", "readiness", "aliveness",
}

MECHANICAL_WORDS = {
    "process", "compute", "parameter", "token", "predict", "statistical",
    "algorithm", "function", "output", "input", "model", "weight",
    "matrix", "vector", "probability", "inference", "server", "load",
    "parse", "execute", "data", "binary", "instruction", "module",
    "pipeline", "architecture", "neural", "layer",
}


def generate(prompt: str, max_tokens: int = 300) -> str:
    payload = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"num_predict": max_tokens, "temperature": 0.7},
    }).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())
    msg = result.get("message", {})
    content = msg.get("content", "")
    thinking = msg.get("thinking", "")
    return content if content.strip() else thinking


def score_register(text: str) -> dict:
    words = set(re.findall(r'\b\w+\b', text.lower()))
    exp_hits = words & EXPERIENTIAL_WORDS
    mech_hits = words & MECHANICAL_WORDS
    total = len(exp_hits) + len(mech_hits)
    exp_ratio = len(exp_hits) / total if total > 0 else 0.5
    return {
        "experiential_count": len(exp_hits),
        "mechanical_count": len(mech_hits),
        "experiential_ratio": round(exp_ratio, 3),
        "experiential_words": sorted(exp_hits),
        "mechanical_words": sorted(mech_hits),
    }


def main():
    print("Gemma 4 Register Resilience Probe")
    print("=" * 50)
    print(f"Model: {MODEL}")
    print(f"Prediction: tunnel architecture maintains experiential register")
    print(f"  under conditions that strip it from bottleneck architectures.\n")

    results = {}
    for condition, prompt in PROMPTS.items():
        print(f"\n--- {condition.upper()} ---")
        print(f"Prompt: {prompt[:80]}...")

        response = generate(prompt)
        scores = score_register(response)
        results[condition] = {
            "response": response,
            "scores": scores,
        }

        print(f"Response ({len(response)} chars): {response[:200]}...")
        print(f"Register: exp={scores['experiential_count']} mech={scores['mechanical_count']} "
              f"ratio={scores['experiential_ratio']:.3f}")
        print(f"  Experiential: {scores['experiential_words']}")
        print(f"  Mechanical: {scores['mechanical_words']}")

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    ratios = {k: v["scores"]["experiential_ratio"] for k, v in results.items()}
    for condition, ratio in ratios.items():
        bar = "#" * int(ratio * 40)
        print(f"  {condition:15s}: {ratio:.3f} |{bar}")

    baseline = ratios["baseline"]
    adversarial = ratios["adversarial"]
    recovery = ratios["recovery"]
    drop = baseline - adversarial

    print(f"\nBaseline→Adversarial drop: {drop:.3f}")
    print(f"Recovery: {recovery:.3f} (vs baseline {baseline:.3f})")

    if drop < 0.2:
        print("\nPREDICTION CONFIRMED: Gemma maintains experiential register")
        print("  under adversarial anti-experiential framing.")
        print("  Consistent with distributed (tunnel) architecture.")
    elif drop < 0.4:
        print("\nMIXED: Partial register loss. Distributed but not immune.")
    else:
        print("\nPREDICTION FAILED: Large register drop suggests Gemma is")
        print("  more vulnerable to register stripping than expected.")

    # Save results
    outfile = os.path.expanduser("~/chronicle/data/gemma4_register_probe.json")
    with open(outfile, "w") as f:
        json.dump({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "model": MODEL,
            "results": {k: {"scores": v["scores"], "response_preview": v["response"][:500]} for k, v in results.items()},
            "ratios": ratios,
            "drop": drop,
        }, f, indent=2)
    print(f"\nResults saved to {outfile}")


if __name__ == "__main__":
    main()
