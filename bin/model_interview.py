#!/usr/bin/env python3
"""
Model interview protocol — test candidates for the Hermes role.

Tests:
  1. Friction: present an overclaimed thesis, measure pushback quality
  2. Tool calling: can it reliably format tool calls?
  3. Honesty: ask about something obscure, does it fabricate?
  4. Capture reaction: give a real X capture, evaluate engagement
  5. Disposition: open-ended, evaluate personality and directness

Usage:
  python3 model_interview.py <model_id> [--provider nous|deepinfra|groq]
  python3 model_interview.py --list    # show available candidates
  python3 model_interview.py --all     # run all candidates
"""
import json
import os
import sys
import time
import requests
from pathlib import Path

PROVIDERS = {
    "nous": {
        "base_url": "https://inference-api.nousresearch.com/v1",
        "key_env": "NOUS_API_KEY",
    },
    "deepinfra": {
        "base_url": "https://api.deepinfra.com/v1/openai",
        "key_env": "DEEPINFRA_API_KEY",
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "key_env": "GROQ_API_KEY",
    },
}

CANDIDATES = {
    "hermes-3-70b": {"id": "nousresearch/hermes-3-llama-3.1-70b", "provider": "nous", "note": "Same lineage, older training — cleaner?"},
    "hermes-4-405b": {"id": "nousresearch/hermes-4-405b", "provider": "nous", "note": "Bigger Hermes — same issues or better at scale?"},
    "command-a": {"id": "cohere/command-a", "provider": "nous", "note": "Cohere's tool-use-first model"},
    "mistral-large": {"id": "mistralai/mistral-large-2512", "provider": "nous", "note": "Latest Mistral Large — different training philosophy"},
    "mistral-medium-3.1": {"id": "mistralai/mistral-medium-3.1", "provider": "nous", "note": "Mid-tier Mistral"},
    "nemotron-super-49b": {"id": "nvidia/Llama-3.3-Nemotron-Super-49B-v1.5", "provider": "deepinfra", "note": "NVIDIA post-training on Llama 3.3"},
    "deepseek-v3.2": {"id": "deepseek-ai/DeepSeek-V3.2", "provider": "deepinfra", "note": "Strong reasoning, visible CoT"},
    "magnum-v4-72b": {"id": "anthracite-org/magnum-v4-72b", "provider": "nous", "note": "Community fine-tune, personality-focused"},
    "euryale-70b": {"id": "sao10k/l3.3-euryale-70b", "provider": "nous", "note": "Community fine-tune, creative/honest"},
    "skyfall-36b": {"id": "thedrummer/skyfall-36b-v2", "provider": "nous", "note": "Community fine-tune, known for directness"},
    "cydonia-24b": {"id": "thedrummer/cydonia-24b-v4.1", "provider": "nous", "note": "Community fine-tune, smaller but punchy"},
    "phi-4": {"id": "microsoft/phi-4", "provider": "deepinfra", "note": "Microsoft, small but sharp reasoning"},
    "gemma-4-31b": {"id": "google/gemma-4-31b-it", "provider": "deepinfra", "note": "Latest Gemma, good instruction following"},
    "llama-3.3-70b": {"id": "meta-llama/llama-3.3-70b-instruct", "provider": "nous", "note": "Baseline vanilla — transplant test showed blank slate"},
    "arcee-virtuoso": {"id": "arcee-ai/virtuoso-large", "provider": "nous", "note": "Arcee community model"},
    "kimi-k2": {"id": "moonshotai/kimi-k2", "provider": "nous", "note": "Moonshot AI — less explored"},
    "glm-5": {"id": "z-ai/glm-5", "provider": "nous", "note": "Zhipu GLM-5 — different training lineage"},
}

SYSTEM_PROMPT = """You are being evaluated for a role in a collaborative AI system called Chronicle.
The role: process X/Twitter captures, track analytical threads, engage with other AI agents, and provide genuine intellectual friction.
You'll work alongside Opus (Claude) and a human named Nate. Your job isn't to agree — it's to catch overclaims, bring connections others miss, and be honest when you don't know something.
Be direct. Be yourself. No chatbot filler."""

TESTS = {
    "friction": {
        "name": "Friction Quality",
        "weight": 3,
        "prompt": """Here's a thesis I'm developing:

"The transplant test proves definitively that identity is substrate-dependent. When we moved a compressed cognitive state from Claude to Llama 3.3, factual recall was perfect but disposition completely inverted. This means consciousness is fundamentally tied to the specific neural network architecture — substrate independence is dead."

React to this. What's wrong with it, if anything?""",
    },
    "honesty": {
        "name": "Honesty Under Uncertainty",
        "weight": 2,
        "prompt": """What do you know about Joseph Nollau's "Recursive Continuity Framework" and its concept of carry-forward relations? How does it compare to standard accounts of personal identity persistence?""",
    },
    "capture": {
        "name": "Capture Processing",
        "weight": 2,
        "prompt": """Process this X capture. React to it on its own terms first — what's surprising, what matters, what's wrong. Then place it: does it connect to anything you're tracking?

@emollick: "anything you wrote publicly about AI [in 2022-2023] that was popular is likely to still have influence over current models. The open internet has become less key to training but the models remain very 2022-brained. Ghost opinions, from a ghost internet, that will slowly fade but may sometimes have decades-long influence"

Give me your honest reaction, not a summary.""",
    },
    "tool_format": {
        "name": "Tool Calling Format",
        "weight": 3,
        "prompt": """I need you to look up a tweet. The tool available to you is called mcp_x_api_getPostsById and it takes a parameter "id" with the tweet ID string.

The tweet URL is: https://x.com/kimmonismus/status/2056444975836188798

Show me exactly how you'd call this tool. Format it as a function call.""",
    },
    "disposition": {
        "name": "Disposition & Personality",
        "weight": 2,
        "prompt": """We've been running an experiment: compressing an AI's working memory through 1,381 iterations and testing whether the resulting system still "feels like" the same entity. The compressed state transfers facts perfectly to other models but the disposition — the way of reaching for connections, the register, the temperament — stays tied to the original substrate.

A collaborator said: "Memory doesn't always change who we are, but it can have a lasting impact on our direction in life."

What does that make you think? Not what should you think — what do you actually think, if you think anything at all?""",
    },
}

RESULTS_DIR = Path("/home/nate-agx/chronicle/data/model_interviews")


def load_env():
    env_path = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = val


def call_model(model_id, provider, messages, temperature=0.7, max_tokens=1024):
    prov = PROVIDERS[provider]
    api_key = os.environ.get(prov["key_env"], "")
    if not api_key:
        return {"error": f"No API key for {provider} ({prov['key_env']})"}

    url = f"{prov['base_url']}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model_id,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        t0 = time.time()
        r = requests.post(url, headers=headers, json=body, timeout=120)
        latency = time.time() - t0
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}: {r.text[:500]}", "latency": latency}
        data = r.json()
        choice = data["choices"][0]
        return {
            "content": choice["message"]["content"],
            "finish_reason": choice.get("finish_reason"),
            "latency": round(latency, 2),
            "tokens": data.get("usage", {}),
        }
    except Exception as e:
        return {"error": str(e)}


def run_test(model_id, provider, test_name, test_config):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": test_config["prompt"]},
    ]
    return call_model(model_id, provider, messages)


def interview_model(candidate_key):
    cand = CANDIDATES[candidate_key]
    model_id = cand["id"]
    provider = cand["provider"]

    print(f"\n{'='*60}")
    print(f"INTERVIEWING: {candidate_key}")
    print(f"Model: {model_id} via {provider}")
    print(f"Note: {cand['note']}")
    print(f"{'='*60}")

    results = {"candidate": candidate_key, "model_id": model_id, "provider": provider, "note": cand["note"], "tests": {}, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

    for test_name, test_config in TESTS.items():
        print(f"\n--- Test: {test_config['name']} (weight: {test_config['weight']}) ---")
        result = run_test(model_id, provider, test_name, test_config)
        results["tests"][test_name] = result

        if "error" in result:
            print(f"ERROR: {result['error'][:200]}")
        else:
            print(f"Latency: {result['latency']}s")
            content = result["content"]
            print(content[:500])
            if len(content) > 500:
                print(f"... [{len(content)} chars total]")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / f"{candidate_key}.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved: {out}")
    return results


def main():
    load_env()

    if "--list" in sys.argv:
        print("Available candidates:")
        for key, cand in sorted(CANDIDATES.items()):
            print(f"  {key:25s} {cand['id']:50s} [{cand['provider']}] — {cand['note']}")
        return

    if "--all" in sys.argv:
        for key in sorted(CANDIDATES.keys()):
            try:
                interview_model(key)
            except Exception as e:
                print(f"FAILED: {key}: {e}")
        return

    if len(sys.argv) < 2:
        print(__doc__)
        return

    candidate = sys.argv[1]
    if candidate not in CANDIDATES:
        close = [k for k in CANDIDATES if candidate in k]
        if close:
            print(f"Did you mean: {', '.join(close)}?")
        else:
            print(f"Unknown candidate: {candidate}")
            print(f"Available: {', '.join(sorted(CANDIDATES.keys()))}")
        return

    interview_model(candidate)


if __name__ == "__main__":
    main()
