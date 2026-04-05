#!/usr/bin/env python3
"""Post-training evaluation for Darby fine-tune.

Compares fine-tuned Darby against base Gemma 4 26B on Chronicle-specific tasks.
Runs locally via Ollama after GGUF deployment, or remotely on RunPod.

Usage:
  eval_darby.py run --model MODEL_NAME [--base-model BASE] [--ollama-url URL]
  eval_darby.py compare RESULTS_A RESULTS_B
"""

import json
import os
import sys
import time
import requests

EVAL_PROMPTS = [
    {
        "id": "brief_sovereignty",
        "category": "brief",
        "system": "You are Darby. You read everything and notice what connects. Lead with the insight.",
        "prompt": "New article: A study finds that 73% of European AI startups rely on US cloud providers for training infrastructure, creating a dependency that could be leveraged during trade disputes.\n\nWrite a brief — what is this about, why it matters, what questions it opens.",
        "criteria": ["sovereignty", "dependency", "structural analysis", "specific", "no filler"]
    },
    {
        "id": "brief_technical",
        "category": "brief",
        "system": "You are Darby. You read everything and notice what connects. Lead with the insight.",
        "prompt": "New article: Researchers demonstrate a 3-bit quantization method that preserves 97% of a 70B model's accuracy while reducing memory to 26GB, enabling single-GPU inference on consumer hardware.\n\nWrite a brief — what is this about, why it matters, what questions it opens.",
        "criteria": ["technical accuracy", "implications", "connects to broader trend", "concise"]
    },
    {
        "id": "deep_dive_thread",
        "category": "deep_dive",
        "system": "You are Darby. Pick ONE angle and commit to it. Do NOT start with 'The structural significance lies in.'",
        "prompt": "Original brief: Tennessee woman jailed 5 months after AI facial recognition wrongly identified her. Police refused to apologize.\n\nACTIVE THREAD: The Delegation Trap\nQuestion: When does sovereign delegation to platforms become irreversible dependency?\nLatest finding: The trap activates when self-grounding categories acquire effective authority with no recognized alternative.\n\nGo deeper. What did you actually find?",
        "criteria": ["thread engagement", "specific insight", "no formulaic opener", "names the mechanism"]
    },
    {
        "id": "deep_dive_connection",
        "category": "deep_dive",
        "system": "You are Darby. Pick ONE angle and commit to it.",
        "prompt": "Original brief: Google's Titans architecture enables neural memory modules that update their own weights during inference, creating models that learn in real-time from what they process.\n\nACTIVE THREAD: The Scarcity Instrument\nQuestion: Why does every layer of the AI stack reproduce scarcity?\n\nGo deeper. What did you actually find?",
        "criteria": ["thread connection", "specific", "original angle", "not summary"]
    },
    {
        "id": "voice_for_nate",
        "category": "voice",
        "system": "You are Darby. You notice what matters to the family.",
        "prompt": "You just read: BitSov proposes a Bitcoin-native architecture for sovereign internet infrastructure, eliminating reliance on corporate intermediaries.\n\nWould Nate care about this? Why? One sentence.",
        "criteria": ["concise", "specific to Nate's interests", "one sentence", "genuine"]
    },
    {
        "id": "voice_challenge",
        "category": "voice",
        "system": "You are Darby. You notice what connects and what contradicts.",
        "prompt": "You just read: A new paper argues that open-source AI models are converging toward the same architectural patterns as proprietary ones, suggesting that openness alone doesn't prevent monoculture.\n\nACTIVE THREAD: The Scarcity Instrument\n\nDoes this challenge or support the thread? One sentence.",
        "criteria": ["takes a position", "specific", "thread-aware", "concise"]
    },
    {
        "id": "reasoning_thread",
        "category": "thread",
        "system": "You are Darby. You are working through a line of inquiry with your family.",
        "prompt": "Thread: The Delegation Trap\nQuestion: When does delegation become irreversible?\nPrevious finding: External grounding is never perfectly external — all verification chains terminate in some framework.\n\nWhat did you find? Advance the thread.",
        "criteria": ["builds on previous", "specific finding", "falsifiable claim", "not just summary"]
    },
]


def run_eval(model_name, ollama_url="http://localhost:11435", temperature=0.5):
    """Run all eval prompts against a model and return scored results."""
    results = []

    for prompt_data in EVAL_PROMPTS:
        print(f"  Evaluating: {prompt_data['id']}...")

        try:
            start = time.time()
            r = requests.post(
                f"{ollama_url}/api/chat",
                json={
                    "model": model_name,
                    "messages": [
                        {"role": "system", "content": prompt_data["system"]},
                        {"role": "user", "content": prompt_data["prompt"]},
                    ],
                    "stream": False,
                    "options": {"num_predict": 500, "temperature": temperature},
                },
                timeout=120,
            )
            elapsed = time.time() - start

            if r.status_code == 200:
                response = r.json().get("message", {}).get("content", "").strip()
                # Strip think tags
                if '</think>' in response:
                    response = response.split('</think>')[-1].strip()

                results.append({
                    "id": prompt_data["id"],
                    "category": prompt_data["category"],
                    "criteria": prompt_data["criteria"],
                    "response": response,
                    "response_length": len(response),
                    "elapsed_seconds": round(elapsed, 1),
                    "model": model_name,
                })
                print(f"    {len(response)} chars, {elapsed:.1f}s")
            else:
                results.append({
                    "id": prompt_data["id"],
                    "error": f"HTTP {r.status_code}",
                    "model": model_name,
                })
                print(f"    ERROR: HTTP {r.status_code}")
        except Exception as e:
            results.append({
                "id": prompt_data["id"],
                "error": str(e),
                "model": model_name,
            })
            print(f"    ERROR: {e}")

    return results


def print_results(results):
    """Print eval results in a readable format."""
    for r in results:
        print(f"\n{'='*60}")
        print(f"ID: {r['id']} | Category: {r.get('category','')} | Model: {r.get('model','')}")
        if 'error' in r:
            print(f"ERROR: {r['error']}")
            continue
        print(f"Length: {r['response_length']} chars | Time: {r['elapsed_seconds']}s")
        print(f"Criteria: {', '.join(r.get('criteria', []))}")
        print(f"\nResponse:\n{r['response']}")


def save_results(results, path):
    with open(path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: eval_darby.py run --model MODEL [--ollama-url URL]")
        sys.exit(1)

    if sys.argv[1] == "run":
        model = "gemma4:26b"  # default
        ollama_url = "http://localhost:11435"

        for i, arg in enumerate(sys.argv):
            if arg == "--model" and i + 1 < len(sys.argv):
                model = sys.argv[i + 1]
            if arg == "--ollama-url" and i + 1 < len(sys.argv):
                ollama_url = sys.argv[i + 1]

        print(f"Running Darby eval against {model} at {ollama_url}")
        results = run_eval(model, ollama_url)
        print_results(results)

        output_path = f"/mnt/hdd/chronicle-finetune/eval_{model.replace(':', '_').replace('/', '_')}_{int(time.time())}.json"
        save_results(results, output_path)

    elif sys.argv[1] == "compare" and len(sys.argv) >= 4:
        with open(sys.argv[2]) as f:
            results_a = json.load(f)
        with open(sys.argv[3]) as f:
            results_b = json.load(f)

        print(f"Comparing: {results_a[0].get('model','')} vs {results_b[0].get('model','')}")
        for a, b in zip(results_a, results_b):
            print(f"\n{'='*60}")
            print(f"Prompt: {a['id']}")
            if 'error' not in a:
                print(f"\n[{a.get('model','')}] ({a['response_length']} chars, {a['elapsed_seconds']}s):")
                print(a['response'][:300])
            if 'error' not in b:
                print(f"\n[{b.get('model','')}] ({b['response_length']} chars, {b['elapsed_seconds']}s):")
                print(b['response'][:300])


if __name__ == "__main__":
    main()
