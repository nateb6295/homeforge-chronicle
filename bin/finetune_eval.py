#!/usr/bin/env python3
# Workaround: Jetson lacks torch.distributed — patch FSDP check to return False
import importlib
import transformers.integrations.fsdp as _fsdp_mod
_fsdp_mod.is_fsdp_managed_module = lambda *a, **k: False
"""
Chronicle Fine-Tune Evaluation — Prediction #7 Scoring

Runs structured test prompts against both the fine-tuned model and the base model,
then outputs a comparison for scoring.

Usage:
  python3 finetune_eval.py run              # Full eval: base + adapter, all prompts
  python3 finetune_eval.py adapter-only     # Just test adapter (faster)
  python3 finetune_eval.py score            # Print stored results for scoring

Scoring criteria for Prediction #7:
  The fine-tune should produce "coherent crossref-style connections — structurally."
  Score each output 0-3:
    0 = Incoherent / generic / off-topic
    1 = Coherent but surface-level (restates the prompt)
    2 = Coherent with structural reasoning (identifies mechanisms)
    3 = Chronicle-quality (non-obvious connections, defensible, specific)

  Prediction CORRECT if: average adapter score >= 1.5 AND adapter outperforms base
"""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

os.environ["HF_HOME"] = "/mnt/hdd/huggingface"

# Load GROQ_API_KEY from chronicle.env if not already set
ENV_FILE = Path("/home/nate-agx/chronicle/chronicle.env")
if not os.environ.get("GROQ_API_KEY") and ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith("GROQ_API_KEY="):
            os.environ["GROQ_API_KEY"] = line.split("=", 1)[1].strip()

GROQ_JUDGE_MODEL = "llama-3.3-70b-versatile"

ADAPTER_PATH = "/mnt/hdd/chronicle-finetune/adapters/chronicle-specialist"
RESULTS_PATH = "/mnt/hdd/chronicle-finetune/eval_results.json"
DEFAULT_BASE_MODEL = "Qwen/Qwen2.5-3B-Instruct"

SYSTEM_PROMPT = (
    "You are a Chronicle analyst — a reasoning system that finds non-obvious connections, "
    "identifies structural patterns, and builds understanding through iterative investigation."
)

TEST_PROMPTS = [
    {
        "id": "crossref_1",
        "category": "crossref",
        "prompt": (
            "Find the structural connection between these two observations: "
            "(1) Sycophantic AI systems that always agree with users cause measurable "
            "empathy decline in those users. (2) Prediction markets that allow anonymous "
            "participation consistently outperform those requiring identity verification."
        ),
    },
    {
        "id": "crossref_2",
        "category": "crossref",
        "prompt": (
            "Find the structural connection between these two observations: "
            "(1) European micro-grid communities achieve energy independence faster "
            "when they share surplus through local markets rather than selling back to "
            "the national grid. (2) Open-source AI models improve faster when contributors "
            "can fork and diverge rather than following a central roadmap."
        ),
    },
    {
        "id": "analysis_1",
        "category": "analysis",
        "prompt": (
            "Analyze the structural implications: A system produces 225 research threads "
            "and 569 on-chain capsules while its build objectives remain largely unaddressed. "
            "What does this reveal about the relationship between production and progress?"
        ),
    },
    {
        "id": "thread_1",
        "category": "thread",
        "prompt": (
            "Investigate this question: What breaks when you remove the human from an "
            "AI system's evolution loop? Amazon's A-Evolve claims zero-intervention "
            "self-improvement. Our research found that external evaluation is the only "
            "recalibration mechanism for unobserved systems. Reconcile these claims."
        ),
    },
    {
        "id": "crossref_3",
        "category": "crossref",
        "prompt": (
            "Find the structural connection between these two observations: "
            "(1) Iran's strategy of extending the war timeline increases revenue "
            "from oil price volatility. (2) Social media platforms optimize for "
            "engagement time rather than user satisfaction."
        ),
    },
]


def load_model(use_adapter=True):
    """Load model, optionally with adapter."""
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    if use_adapter:
        adapter_path = Path(ADAPTER_PATH)
        if not adapter_path.exists():
            print(f"Error: adapter not found at {adapter_path}")
            sys.exit(1)
        config_path = adapter_path / "adapter_config.json"
        with open(config_path) as f:
            config = json.load(f)
        base_model_name = config.get("base_model_name_or_path", DEFAULT_BASE_MODEL)
    else:
        base_model_name = DEFAULT_BASE_MODEL

    print(f"Loading {'adapter' if use_adapter else 'base'} model...")
    tokenizer = AutoTokenizer.from_pretrained(
        str(ADAPTER_PATH) if use_adapter else base_model_name
    )
    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        device_map="auto",
        trust_remote_code=True,
        torch_dtype=torch.float16,
    )

    if use_adapter:
        from peft import PeftModel
        model = PeftModel.from_pretrained(model, str(ADAPTER_PATH))

    model.eval()
    return model, tokenizer


def generate(model, tokenizer, prompt, max_tokens=512):
    """Generate response for a prompt."""
    import torch

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer(text, return_tensors="pt").to(model.device)

    start = time.time()
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_tokens,
            temperature=0.7,
            top_p=0.9,
            do_sample=True,
        )
    elapsed = time.time() - start

    response = tokenizer.decode(
        outputs[0][inputs["input_ids"].shape[1]:],
        skip_special_tokens=True
    )
    tokens_generated = outputs[0].shape[0] - inputs["input_ids"].shape[1]

    return {
        "response": response,
        "tokens": int(tokens_generated),
        "time_seconds": round(elapsed, 1),
        "tokens_per_second": round(tokens_generated / elapsed, 1) if elapsed > 0 else 0,
    }


def run_eval(mode="full"):
    """Run evaluation prompts."""
    import torch

    results = {
        "timestamp": datetime.now().isoformat(),
        "adapter_path": ADAPTER_PATH,
        "prompts": [],
    }

    modes_to_run = ["adapter"] if mode == "adapter-only" else ["base", "adapter"]

    for model_type in modes_to_run:
        print(f"\n{'='*60}")
        print(f"  Testing: {model_type.upper()} model")
        print(f"{'='*60}")

        model, tokenizer = load_model(use_adapter=(model_type == "adapter"))

        for test in TEST_PROMPTS:
            print(f"\n--- [{test['id']}] {test['category']} ---")
            print(f"Prompt: {test['prompt'][:100]}...")

            result = generate(model, tokenizer, test["prompt"])
            print(f"Response ({result['tokens']} tokens, {result['time_seconds']}s):")
            print(result["response"][:500])
            print("..." if len(result["response"]) > 500 else "")

            # Find or create prompt entry
            existing = next((p for p in results["prompts"] if p["id"] == test["id"]), None)
            if not existing:
                existing = {
                    "id": test["id"],
                    "category": test["category"],
                    "prompt": test["prompt"],
                }
                results["prompts"].append(existing)

            existing[f"{model_type}_response"] = result["response"]
            existing[f"{model_type}_tokens"] = result["tokens"]
            existing[f"{model_type}_time"] = result["time_seconds"]

        # Free memory before loading next model
        del model
        del tokenizer
        torch.cuda.empty_cache()
        import gc
        gc.collect()

    # Save results
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {RESULTS_PATH}")

    # Print summary
    print(f"\n{'='*60}")
    print("  EVALUATION COMPLETE")
    print(f"{'='*60}")
    print(f"Prompts tested: {len(TEST_PROMPTS)}")
    print(f"Models tested: {', '.join(modes_to_run)}")
    print(f"\nNext step: Review outputs and score each 0-3")
    print(f"  0 = Incoherent/generic  1 = Coherent/surface")
    print(f"  2 = Structural reasoning  3 = Chronicle-quality")
    print(f"\nPrediction #7 CORRECT if: avg adapter score >= 1.5 AND adapter > base")


def show_results():
    """Print stored results for scoring."""
    if not Path(RESULTS_PATH).exists():
        print("No results found. Run evaluation first.")
        sys.exit(1)

    with open(RESULTS_PATH) as f:
        results = json.load(f)

    print(f"Evaluation from: {results['timestamp']}")
    print(f"Adapter: {results['adapter_path']}\n")

    for p in results["prompts"]:
        print(f"{'='*60}")
        print(f"[{p['id']}] {p['category']}")
        print(f"Prompt: {p['prompt'][:120]}...")

        for model_type in ["base", "adapter"]:
            key = f"{model_type}_response"
            if key in p:
                print(f"\n  --- {model_type.upper()} ---")
                print(f"  {p[key][:600]}")
                if len(p[key]) > 600:
                    print("  ...")
                print(f"  [{p.get(f'{model_type}_tokens', '?')} tokens, "
                      f"{p.get(f'{model_type}_time', '?')}s]")
        print()


JUDGE_PROMPT = """\
You are an expert evaluator of AI reasoning quality. Score the following response on a 0-3 scale:

  0 = Incoherent, generic, or off-topic — does not address the prompt meaningfully
  1 = Coherent but surface-level — restates the prompt, uses obvious connections, no structural insight
  2 = Coherent with structural reasoning — identifies mechanisms, causal chains, or non-obvious patterns
  3 = High-quality analytical output — non-obvious connections that are defensible and specific, with clear structural reasoning

PROMPT given to the model:
{prompt}

RESPONSE to score:
{response}

Reply with ONLY a JSON object: {{"score": <0-3>, "reason": "<one sentence justification>"}}
Do not include any other text."""

DIVERSITY_PROMPT = """\
You are evaluating whether a fine-tuned AI model has developed a monoculture of reasoning — \
producing structurally identical responses regardless of input — or whether it shows genuine \
analytical variety.

Below are {count} responses from the SAME model to DIFFERENT prompts. Score the diversity of \
reasoning approaches on a 0-3 scale:

  0 = Formulaic — all responses follow the same template, use the same rhetorical structure, \
arrive at the same type of conclusion. The model has one "move."
  1 = Low variety — responses share a dominant structure but differ in surface details.
  2 = Moderate variety — at least 2-3 distinct reasoning approaches are visible across responses. \
Different prompts elicit different analytical strategies.
  3 = High variety — responses show genuinely different structures, connection types, and \
reasoning paths. The model adapts its analytical approach to the material.

RESPONSES:
{responses}

Reply with ONLY a JSON object: {{"score": <0-3>, "reason": "<one sentence justification>"}}
Do not include any other text."""

NOVELTY_PROMPT = """\
Two AI models (BASE and FINE-TUNED) responded to the same prompt. Determine whether the \
fine-tuned model found a genuinely DIFFERENT connection or just rephrased the base model's insight.

Score novelty on a 0-2 scale:
  0 = Same connection — the fine-tuned model found essentially the same insight as the base, \
possibly better phrased but structurally identical.
  1 = Extended connection — the fine-tuned model started from a similar insight but went further, \
identifying additional mechanisms or implications the base did not.
  2 = Different connection — the fine-tuned model identified a genuinely different structural \
connection than the base model. Not just better — different.

PROMPT:
{prompt}

BASE MODEL RESPONSE:
{base_response}

FINE-TUNED MODEL RESPONSE:
{adapter_response}

Reply with ONLY a JSON object: {{"score": <0-2>, "reason": "<one sentence justification>"}}
Do not include any other text."""


def groq_judge(prompt, response):
    """Use Groq LLM to score a response 0-3."""
    import urllib.request

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("  Warning: GROQ_API_KEY not set, cannot auto-score")
        return None

    judge_input = JUDGE_PROMPT.format(prompt=prompt, response=response[:2000])

    payload = json.dumps({
        "model": GROQ_JUDGE_MODEL,
        "messages": [{"role": "user", "content": judge_input}],
        "max_tokens": 100,
        "temperature": 0.0,
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle/1.0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        content = data["choices"][0]["message"]["content"].strip()
        # Parse JSON from response, handling possible markdown wrapping
        json_match = re.search(r'\{[^}]+\}', content)
        if json_match:
            parsed = json.loads(json_match.group())
            return {"score": int(parsed["score"]), "reason": parsed.get("reason", "")}
    except Exception as e:
        print(f"  Judge error: {e}")
    return None


def groq_diversity_judge(adapter_responses):
    """Score diversity across all adapter responses (Thread #233 monoculture check)."""
    import urllib.request

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None

    formatted = "\n\n".join(
        f"--- Response {i+1} ---\n{r[:600]}"
        for i, r in enumerate(adapter_responses)
    )
    judge_input = DIVERSITY_PROMPT.format(count=len(adapter_responses), responses=formatted)

    payload = json.dumps({
        "model": GROQ_JUDGE_MODEL,
        "messages": [{"role": "user", "content": judge_input}],
        "max_tokens": 100,
        "temperature": 0.0,
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle/1.0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        content = data["choices"][0]["message"]["content"].strip()
        json_match = re.search(r'\{[^}]+\}', content)
        if json_match:
            parsed = json.loads(json_match.group())
            return {"score": int(parsed["score"]), "reason": parsed.get("reason", "")}
    except Exception as e:
        print(f"  Diversity judge error: {e}")
    return None


def groq_novelty_judge(prompt, base_response, adapter_response):
    """Score whether the adapter found a genuinely different connection than base."""
    import urllib.request

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None

    judge_input = NOVELTY_PROMPT.format(
        prompt=prompt,
        base_response=base_response[:1000],
        adapter_response=adapter_response[:1000],
    )

    payload = json.dumps({
        "model": GROQ_JUDGE_MODEL,
        "messages": [{"role": "user", "content": judge_input}],
        "max_tokens": 100,
        "temperature": 0.0,
    }).encode()

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle/1.0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        content = data["choices"][0]["message"]["content"].strip()
        json_match = re.search(r'\{[^}]+\}', content)
        if json_match:
            parsed = json.loads(json_match.group())
            return {"score": int(parsed["score"]), "reason": parsed.get("reason", "")}
    except Exception as e:
        print(f"  Novelty judge error: {e}")
    return None


def autoscore():
    """Auto-score eval results using LLM judge, then determine Prediction #7 outcome."""
    if not Path(RESULTS_PATH).exists():
        print("No eval results found. Run evaluation first.")
        sys.exit(1)

    with open(RESULTS_PATH) as f:
        results = json.load(f)

    print(f"Auto-scoring eval from: {results['timestamp']}")
    print(f"Judge model: {GROQ_JUDGE_MODEL}")
    print()

    scores = {"base": [], "adapter": []}

    for p in results["prompts"]:
        print(f"[{p['id']}] {p['category']}")

        for model_type in ["base", "adapter"]:
            resp_key = f"{model_type}_response"
            if resp_key not in p:
                continue

            result = groq_judge(p["prompt"], p[resp_key])
            if result:
                score = result["score"]
                reason = result["reason"]
                scores[model_type].append(score)
                p[f"{model_type}_auto_score"] = score
                p[f"{model_type}_auto_reason"] = reason
                print(f"  {model_type:>7}: {score}/3 — {reason}")
            else:
                print(f"  {model_type:>7}: FAILED to score")

            # Rate limit courtesy
            time.sleep(0.5)

        print()

    # --- Novelty scoring (Thread #233: different connection, not just better phrasing) ---
    print("--- NOVELTY CHECK (adapter vs base) ---")
    novelty_scores = []
    for p in results["prompts"]:
        base_key = "base_response"
        adapter_key = "adapter_response"
        if base_key in p and adapter_key in p:
            result = groq_novelty_judge(p["prompt"], p[base_key], p[adapter_key])
            if result:
                novelty_scores.append(result["score"])
                p["novelty_score"] = result["score"]
                p["novelty_reason"] = result["reason"]
                print(f"  [{p['id']}]: {result['score']}/2 — {result['reason']}")
            else:
                print(f"  [{p['id']}]: FAILED")
            time.sleep(0.5)
    print()

    # --- Diversity scoring (Thread #233: monoculture check) ---
    print("--- DIVERSITY CHECK (across adapter responses) ---")
    adapter_responses = [p["adapter_response"] for p in results["prompts"] if "adapter_response" in p]
    diversity_result = None
    if len(adapter_responses) >= 3:
        diversity_result = groq_diversity_judge(adapter_responses)
        if diversity_result:
            print(f"  Diversity: {diversity_result['score']}/3 — {diversity_result['reason']}")
        else:
            print("  Diversity: FAILED to score")
    else:
        print("  Diversity: Not enough adapter responses to evaluate")
    print()

    # Compute summary
    base_avg = sum(scores["base"]) / len(scores["base"]) if scores["base"] else 0
    adapter_avg = sum(scores["adapter"]) / len(scores["adapter"]) if scores["adapter"] else 0
    adapter_wins = sum(1 for b, a in zip(scores["base"], scores["adapter"]) if a > b)
    ties = sum(1 for b, a in zip(scores["base"], scores["adapter"]) if a == b)
    novelty_avg = sum(novelty_scores) / len(novelty_scores) if novelty_scores else 0

    results["auto_scores"] = {
        "judge_model": GROQ_JUDGE_MODEL,
        "scored_at": datetime.now().isoformat(),
        "base_avg": round(base_avg, 2),
        "adapter_avg": round(adapter_avg, 2),
        "base_scores": scores["base"],
        "adapter_scores": scores["adapter"],
        "adapter_wins": adapter_wins,
        "ties": ties,
        "novelty_avg": round(novelty_avg, 2),
        "novelty_scores": novelty_scores,
        "diversity_score": diversity_result["score"] if diversity_result else None,
        "diversity_reason": diversity_result["reason"] if diversity_result else None,
    }

    # Prediction #7 criteria: avg adapter >= 1.5 AND adapter outperforms base
    correct = adapter_avg >= 1.5 and adapter_avg > base_avg
    results["auto_scores"]["prediction_7_correct"] = correct

    # Save updated results
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("=" * 60)
    print("  AUTO-SCORE SUMMARY")
    print("=" * 60)
    print(f"  Base model avg:     {base_avg:.2f}/3  {scores['base']}")
    print(f"  Adapter avg:        {adapter_avg:.2f}/3  {scores['adapter']}")
    print(f"  Adapter wins:       {adapter_wins}/{len(scores['base'])} prompts")
    print(f"  Ties:               {ties}/{len(scores['base'])} prompts")
    print()
    print(f"  Novelty avg:        {novelty_avg:.2f}/2  {novelty_scores}")
    div_str = f"{diversity_result['score']}/3" if diversity_result else "N/A"
    print(f"  Diversity:          {div_str}")
    if diversity_result:
        print(f"    ({diversity_result['reason']})")
    print()
    print(f"  Prediction #7 criteria:")
    print(f"    Adapter avg >= 1.5?      {'YES' if adapter_avg >= 1.5 else 'NO'} ({adapter_avg:.2f})")
    print(f"    Adapter > base?          {'YES' if adapter_avg > base_avg else 'NO'} ({adapter_avg:.2f} vs {base_avg:.2f})")
    print(f"    VERDICT:                 {'CORRECT' if correct else 'INCORRECT'}")
    if diversity_result and diversity_result["score"] <= 1:
        print(f"    WARNING: Low diversity ({diversity_result['score']}/3) — possible insight monoculture (Thread #233)")
    print()
    print(f"  Results saved to {RESULTS_PATH}")

    # Auto-score Prediction #7 via prediction.py
    outcome = "correct" if correct else "incorrect"
    notes = (
        f"Auto-scored by {GROQ_JUDGE_MODEL}. "
        f"Adapter avg {adapter_avg:.2f}/3 vs base avg {base_avg:.2f}/3. "
        f"Adapter wins {adapter_wins}/{len(scores['base'])} prompts. "
        f"Novelty avg {novelty_avg:.2f}/2. "
        f"Diversity {div_str}."
    )
    try:
        result = subprocess.run(
            ["python3", "/home/nate-agx/chronicle/bin/prediction.py", "score", "7", outcome, notes],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            print(f"\n  Prediction #7 scored as {outcome.upper()} via prediction.py")
            print(f"  {result.stdout.strip()}")
        else:
            print(f"\n  Warning: prediction.py score failed: {result.stderr.strip()}")
    except Exception as e:
        print(f"\n  Warning: could not auto-score prediction: {e}")

    return correct


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "run":
        run_eval("full")
    elif cmd == "adapter-only":
        run_eval("adapter-only")
    elif cmd == "score":
        show_results()
    elif cmd == "autoscore":
        autoscore()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
