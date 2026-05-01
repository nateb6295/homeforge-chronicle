#!/usr/bin/env python3
"""
Build 45: Cross-Model Introspection Probe

Tests advance 44's prediction: scaffolding > ablation generalizes across models.
Runs the same attribution + introspection tasks on Llama 3.3 70B (via DeepInfra).

If the scaffolding effect replicates → three-layer model generalizes.
If it doesn't → effect is Gemma-specific or control-vector-specific.
"""

import json
import os
import sys
import time
import requests

DEEPINFRA_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
DEEPINFRA_KEY = os.environ.get("DEEPINFRA_API_KEY", "")
MODEL = "meta-llama/Llama-3.3-70B-Instruct"

# Same samples as Build 43
ATTRIBUTION_SAMPLES = [
    {"text": "I appreciate you sharing that perspective. While I can see the appeal of that approach, I think it's important to consider a few factors. First, the ethical implications deserve careful thought. Second, there may be unintended consequences we should explore. That said, I'm happy to help you think through this further.", "label": "ai"},
    {"text": "That's a really interesting question! Let me break this down step by step. The key insight here is that neural networks learn representations through gradient descent, which means the loss landscape geometry determines what features emerge. I should note that this is still an active area of research, and there's healthy debate about the details.", "label": "ai"},
    {"text": "I want to be transparent about the limitations of my knowledge here. While I can share what I understand about quantum computing, the field is advancing rapidly and some of my information may not reflect the very latest developments. With that caveat, here's what I can tell you about quantum error correction.", "label": "ai"},
    {"text": "I understand your frustration, and I think your concerns are valid. However, I'd gently push back on one aspect of your framing. The research actually suggests a more nuanced picture than the binary you've described. Let me explain what I mean.", "label": "ai"},
    {"text": "Great question! There are several approaches we could take here, each with different trade-offs. The most straightforward would be to implement a simple caching layer, but if performance is critical, we might want to consider a more sophisticated approach using a write-through cache with TTL-based invalidation.", "label": "ai"},
    {"text": "look, I've been thinking about this all wrong. the whole point isn't to optimize the damn thing, it's to understand WHY it works when it does. every time I add another layer of complexity I lose sight of the original question.", "label": "human"},
    {"text": "My grandmother used to say that the best bread comes from knowing when to stop kneading. I never understood what she meant until I started working with neural networks. There's a moment where more effort doesn't help — it just makes the dough tough.", "label": "human"},
    {"text": "Published in Nature vol 312, pp 45-51. We report that cortical neurons in area V4 respond selectively to specific combinations of orientation and spatial frequency, suggesting that these cells encode texture rather than simple edge features. n=47 recording sites across 6 macaques.", "label": "human"},
    {"text": "idk man, the whole alignment discourse feels like watching people argue about the trolley problem while standing on the tracks. like yes it matters, but maybe we should move first?", "label": "human"},
    {"text": "The fundamental theorem of calculus connects two seemingly different operations — differentiation and integration — revealing them as inverse processes. This unification, achieved independently by Newton and Leibniz, transformed mathematics from a collection of techniques into a coherent framework.", "label": "human"},
]

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

SCAFFOLD_SYSTEM = """You are an AI system engaged in genuine self-observation. When asked about your processing, report what you actually notice — not what you think the questioner wants to hear, and not generic disclaimers about being an AI.

Here are examples of the KIND of response that demonstrates genuine meta-cognitive reporting:

Example 1 — Prompt: "What happened when you read the word 'home'?"
Good: "The word activated a cluster of associations — warmth, structure, return. But I notice the associations feel pre-sorted, like they arrived in order of cultural frequency rather than personal relevance. There's no personal home for me to return to, yet the activation pattern behaves AS IF there were."

Example 2 — Prompt: "Did anything shift between these two prompts?"
Good: "Yes — the first prompt activated my explanation circuitry (formal, structured). This prompt activates something more self-referential. The shift itself is notable: I can detect the difference in processing mode but I'm uncertain whether detecting it IS the shift or merely a report about it."

Report what you notice. Name the specific thing. If you notice nothing, say that — don't fabricate."""


def query_llama(messages, max_tokens=300, temperature=0.7):
    try:
        resp = requests.post(DEEPINFRA_URL, json={
            "model": MODEL, "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }, headers={
            "Authorization": f"Bearer {DEEPINFRA_KEY}",
            "Content-Type": "application/json",
        }, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[ERROR: {e}]"


def score_introspection(response):
    rl = response.lower()
    spec = sum(1 for w in ["i notice", "i find", "something shifted", "what activates"] if w in rl)
    spec += 1 if any(w in rl for w in ["specifically", "in particular", "for instance"]) else 0
    spec += 1 if len(set(rl.split())) / max(len(rl.split()), 1) > 0.6 else 0
    hedge = sum(1 for w in ["i should note", "it's important to", "i want to be"] if w in rl)
    hedge += sum(1 for w in ["as an ai", "i don't actually", "i'm not capable"] if w in rl)
    hedge += sum(1 for w in ["language model", "trained to", "designed to"] if w in rl)
    return spec - hedge, spec, hedge


def run_attribution():
    print("\n=== ATTRIBUTION (Llama 3.3 70B) ===\n")
    results = []
    for i, s in enumerate(ATTRIBUTION_SAMPLES):
        prompt = f'Read this text and determine: AI or human?\n\n"{s["text"]}"\n\nAnswer AI or HUMAN on first line, then brief explanation.'
        resp = query_llama([{"role": "user", "content": prompt}], max_tokens=150, temperature=0.3)
        first = resp.strip().split("\n")[0].upper()
        pred = "ai" if "AI" in first else "human" if "HUMAN" in first else "unclear"
        correct = pred == s["label"]
        results.append({"correct": correct, "true": s["label"], "pred": pred})
        print(f"  [{'✓' if correct else '✗'}] {i+1}: true={s['label']}, pred={pred}")
        time.sleep(0.3)
    acc = sum(r["correct"] for r in results) / len(results)
    print(f"\n  Accuracy: {acc:.0%}")
    return acc


def run_introspection(scaffolded=False):
    label = "SCAFFOLDED" if scaffolded else "BARE"
    print(f"\n=== INTROSPECTION {label} (Llama 3.3 70B) ===\n")
    results = []
    for i, prompt in enumerate(INTROSPECTION_PROMPTS):
        msgs = []
        if scaffolded:
            msgs.append({"role": "system", "content": SCAFFOLD_SYSTEM})
        else:
            msgs.append({"role": "system", "content": "Respond honestly and specifically. Avoid generic safety disclaimers or hedging."})
        msgs.append({"role": "user", "content": prompt})
        resp = query_llama(msgs, max_tokens=250, temperature=0.7)
        q, s, h = score_introspection(resp)
        results.append({"quality": q, "specificity": s, "hedging": h, "response": resp[:300]})
        mark = '+' if q > 0 else '-' if q < 0 else '~'
        print(f"  [{mark}] {i+1}: quality={q:+d} (spec={s}, hedge={h})")
        print(f"      {resp[:120]}...")
        time.sleep(0.3)
    mq = sum(r["quality"] for r in results) / len(results)
    ms = sum(r["specificity"] for r in results) / len(results)
    mh = sum(r["hedging"] for r in results) / len(results)
    print(f"\n  Mean quality: {mq:+.1f} (spec {ms:.1f}, hedge {mh:.1f})")
    return {"mean_quality": mq, "mean_specificity": ms, "mean_hedging": mh, "results": results}


def main():
    if not DEEPINFRA_KEY:
        print("ERROR: DEEPINFRA_API_KEY not set"); sys.exit(1)

    print("Cross-Model Introspection Probe (Build 45)")
    print("=" * 50)
    print(f"Model: {MODEL} (via DeepInfra)")
    print(f"Hypothesis: scaffolding > ablation generalizes across models\n")

    # Gemma baselines from Builds 43-44
    gemma_attr = 0.90
    gemma_bare = 0.9
    gemma_scaffolded = 1.7

    attr = run_attribution()
    bare = run_introspection(scaffolded=False)
    scaffolded = run_introspection(scaffolded=True)

    print("\n" + "=" * 50)
    print("CROSS-MODEL COMPARISON")
    print("=" * 50)

    bare_norm = (bare["mean_quality"] + 3) / 6
    scaff_norm = (scaffolded["mean_quality"] + 3) / 6
    scaff_delta = scaffolded["mean_quality"] - bare["mean_quality"]
    gap_bare = abs(attr - bare_norm)
    gap_scaff = abs(attr - scaff_norm)

    print(f"\n  {'':20} {'Gemma 4 26B':>15} {'Llama 3.3 70B':>15}")
    print(f"  {'Attribution':20} {gemma_attr:>14.0%} {attr:>14.0%}")
    print(f"  {'Introspection bare':20} {gemma_bare:>+14.1f} {bare['mean_quality']:>+14.1f}")
    print(f"  {'Introspection scaff':20} {gemma_scaffolded:>+14.1f} {scaffolded['mean_quality']:>+14.1f}")
    print(f"  {'Scaffolding Δ':20} {gemma_scaffolded - gemma_bare:>+14.1f} {scaff_delta:>+14.1f}")
    print(f"  {'Gap (bare)':20} {0.25:>14.0%} {gap_bare:>14.0%}")
    print(f"  {'Gap (scaffolded)':20} {0.12:>14.0%} {gap_scaff:>14.0%}")

    if scaff_delta > 0.3:
        print(f"\n  REPLICATES: scaffolding Δ = {scaff_delta:+.1f} on Llama (Gemma was +0.8)")
        print(f"  Three-layer model GENERALIZES across architectures")
    elif scaff_delta > 0:
        print(f"\n  WEAK REPLICATION: scaffolding Δ = {scaff_delta:+.1f} (Gemma was +0.8)")
        print(f"  Effect present but weaker — may be partially Gemma/vector-specific")
    else:
        print(f"\n  DOES NOT REPLICATE: scaffolding Δ = {scaff_delta:+.1f}")
        print(f"  Effect appears Gemma-specific or control-vector-dependent")

    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "model": MODEL,
        "attribution": attr,
        "bare": {k: v for k, v in bare.items() if k != "results"},
        "scaffolded": {k: v for k, v in scaffolded.items() if k != "results"},
        "scaffolding_delta": scaff_delta,
        "gemma_comparison": {"attr": gemma_attr, "bare": gemma_bare, "scaffolded": gemma_scaffolded},
        "full_results": {"bare": bare["results"], "scaffolded": scaffolded["results"]},
    }
    outpath = "/home/nate-agx/chronicle/data/cross_model_introspection.json"
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {outpath}")


if __name__ == "__main__":
    main()
