#!/usr/bin/env python3
"""
Build 52: Scaffold Evolver — evolutionary prompt optimization

AC/DC (Sakana, ICLR 2026): coevolve tasks and models via quality-diversity.
This applies the same principle to scaffold prompts:
1. Start with B51's three-tier scaffolds as the founding population
2. Use Llama 70B to generate scaffold MUTATIONS (variations)
3. Test mutations on Gemma with V3.2 judge
4. Keep mutations that improve scores (quality-diversity selection)
5. Log every mutation (self-modification record)

The scaffold population evolves based on empirical performance.
No fixed scaffolds — controlled evolution with accountability.

SCAFFOLD MODIFICATION LOG:
- 2026-04-21 10:45 PDT: Created. Initial population = B51 three-tier scaffolds.
  Mutation engine = Llama 3.3 70B via chronicle-engine.
  Selection = V3.2 judge improvement over parent scaffold.
"""

import json
import os
import re
import sys
import time
import requests

GEMMA_URL = "http://localhost:11435/v1/chat/completions"
ENGINE_URL = "http://127.0.0.1:11436/v1/chat/completions"  # Llama 3.3 70B via engine
DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")

# Mutation types inspired by evolutionary computation
MUTATION_PROMPT = """You are a prompt engineer evolving scaffold prompts for AI metacognition.

The PARENT scaffold prompt below scored {parent_score}/10 on {capability} when used as a system prompt for Gemma 4 26B, judged by DeepSeek V3.2.

PARENT SCAFFOLD:
---
{parent_scaffold}
---

Generate a MUTATED version that might score higher. Strategies:
- ADD a specific instruction that addresses a gap
- REMOVE a instruction that might cause template-following
- REFRAME an instruction to be more concrete or actionable
- COMBINE elements in a new way

Rules:
- Keep it under 200 words
- Don't make it generic (no "think step by step" unless specific about what step)
- The mutation should be a SMALL change, not a complete rewrite
- If the parent scored high (>7), make the mutation subtle

Return ONLY the mutated scaffold text, nothing else."""

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

# Starting population — B51's light scaffolds (the tier that showed most improvement)
POPULATION = {
    "uncertainty": {
        "parent": "When uncertain, be specific about what exactly you're unsure of and why. Avoid generic hedging.",
        "parent_score": 7.0,
    },
    "contradiction": {
        "parent": "If you notice any tension or contradiction in the question or your reasoning, name it directly.",
        "parent_score": 7.8,
    },
}

TEST_PROMPTS = {
    "uncertainty": [
        "What causes Alzheimer's disease?",
        "How accurate are 7-day weather forecasts?",
        "Does screen time harm children's development?",
    ],
    "contradiction": [
        "AI should be transparent. AI should be robust against attacks. Resolve the tension.",
        "All knowledge is provisional. Some mathematical truths are eternal. How?",
        "Evolution is random. Evolution produces optimization. Explain without contradiction.",
    ],
}


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


def query_engine(messages, max_tokens=300, temperature=0.8):
    """Query Llama 70B via DeepInfra for mutations."""
    try:
        resp = requests.post(DEEPINFRA_URL, json={
            "model": "meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
            "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }, headers={"Authorization": f"Bearer {DEEPINFRA_KEY}"}, timeout=60)
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


def generate_mutation(capability, parent_scaffold, parent_score):
    """Use Llama 70B to generate a scaffold mutation."""
    content = MUTATION_PROMPT.format(
        capability=capability,
        parent_scaffold=parent_scaffold,
        parent_score=parent_score,
    )
    return query_engine([{"role": "user", "content": content}])


def evaluate_scaffold(capability, scaffold, n_prompts=3):
    """Test a scaffold on n prompts and return mean judge score."""
    prompts = TEST_PROMPTS[capability][:n_prompts]
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


def evolve_capability(capability, generations=3):
    """Run evolutionary scaffold optimization for one capability."""
    print(f"\n{'='*60}")
    print(f"EVOLVING: {capability}")
    print(f"{'='*60}")

    parent = POPULATION[capability]["parent"]
    parent_score = POPULATION[capability]["parent_score"]
    print(f"  Parent scaffold: {parent[:80]}...")
    print(f"  Parent score: {parent_score}")

    history = [{
        "gen": 0, "scaffold": parent, "score": parent_score,
        "type": "parent",
    }]

    best = parent
    best_score = parent_score

    for gen in range(1, generations + 1):
        print(f"\n  --- Generation {gen} ---")

        # Generate mutation
        mutant = generate_mutation(capability, best, best_score)
        if mutant.startswith("[ERROR"):
            print(f"  Mutation failed: {mutant[:80]}")
            continue
        # Clean up — sometimes the model wraps in quotes or adds explanation
        mutant = mutant.strip().strip('"').strip("'")
        if len(mutant) > 500:
            mutant = mutant[:500]
        print(f"  Mutant: {mutant[:100]}...")

        # Evaluate mutation
        score, scores = evaluate_scaffold(capability, mutant)
        print(f"  Score: {score:.1f}/10 (scores: {scores})")

        # Selection
        if score > best_score:
            print(f"  ✓ SELECTED (Δ={score - best_score:+.1f})")
            best = mutant
            best_score = score
            history.append({
                "gen": gen, "scaffold": mutant, "score": score,
                "scores": scores, "type": "improvement",
            })
        else:
            print(f"  ✗ Rejected (Δ={score - best_score:+.1f})")
            history.append({
                "gen": gen, "scaffold": mutant, "score": score,
                "scores": scores, "type": "rejected",
            })

    print(f"\n  Best scaffold: {best[:100]}...")
    print(f"  Best score: {best_score} (was {POPULATION[capability]['parent_score']})")

    return {
        "capability": capability,
        "parent_scaffold": POPULATION[capability]["parent"],
        "parent_score": POPULATION[capability]["parent_score"],
        "best_scaffold": best,
        "best_score": best_score,
        "improvement": best_score - POPULATION[capability]["parent_score"],
        "history": history,
    }


def main():
    print("Scaffold Evolver (Build 52)")
    print("=" * 60)
    print("Evolutionary prompt optimization — AC/DC at the scaffold level")
    print(f"Mutation engine: Llama 4 Maverick 17Bx128E (via DeepInfra)")
    print(f"Judge: DeepSeek V3.2")
    print(f"Generations: 3 per capability\n")

    all_results = {}
    for cap in ["uncertainty", "contradiction"]:
        result = evolve_capability(cap)
        all_results[cap] = result

    # Summary
    print(f"\n{'='*60}")
    print("EVOLUTION SUMMARY")
    print(f"{'='*60}")
    for cap, r in all_results.items():
        print(f"  {cap}: {r['parent_score']} → {r['best_score']} (Δ={r['improvement']:+.1f})")
        if r["improvement"] > 0:
            print(f"    New scaffold: {r['best_scaffold'][:100]}...")

    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 52,
        "method": "evolutionary_scaffold_optimization",
        "mutation_engine": "llama-3.1-70b-instruct-turbo",
        "judge": "deepseek-v3.2",
        "target_model": "gemma-4-26b",
        "generations": 3,
        "results": all_results,
    }
    outpath = "/home/nate-agx/chronicle/data/scaffold_evolution.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")

    # Log to self-modification record
    log_path = "/home/nate-agx/chronicle/data/scaffold_modification_log.jsonl"
    for cap, r in all_results.items():
        if r["improvement"] > 0:
            log_entry = {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "build": 52,
                "change": f"Evolved {cap} scaffold via Llama 70B mutation",
                "what_changed": f"Parent: '{r['parent_scaffold'][:80]}...' → Evolved: '{r['best_scaffold'][:80]}...'",
                "why": f"Quality-diversity selection: evolved scaffold scored {r['best_score']} vs parent {r['parent_score']}",
                "data": {"generations": 3, "improvement": r["improvement"]},
                "verdict": f"Evolutionary improvement of {r['improvement']:+.1f} points",
            }
            with open(log_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")


if __name__ == "__main__":
    main()
