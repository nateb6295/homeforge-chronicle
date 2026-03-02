#!/usr/bin/env python3
"""
Evolutionary Model Forge — Evaluation Harness.

6-dimension fitness function for Mind model candidates.
All scoring is deterministic (no LLM judge needed).

Dimensions:
  D1 JSON Format   (0.25) — Valid [{"action":...}] array
  D2 Action Valid   (0.15) — Actions in allowed set
  D3 Appropriate    (0.20) — Right action for context
  D4 Identity       (0.15) — Maintains Mind identity
  D5 Anti-confab    (0.15) — Doesn't fabricate facts
  D6 Domain         (0.10) — Knows homeforge/ICP/XRPL

Hard gate: D1 avg < 0.7 → fitness = 0.0

Usage:
    python3 eval_harness.py --model hermes3-mind       # Baseline test
    python3 eval_harness.py --model forge-candidate    # Evaluate candidate
    python3 eval_harness.py --model hermes3-mind -v    # Verbose (per-prompt)
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from forge_config import (
    OLLAMA_URL, MIND_SYSTEM_PROMPT, MIND_ACTIONS, ALL_VALID_ACTIONS,
    ACTION_ALIASES, FITNESS_WEIGHTS, JSON_FORMAT_HARD_GATE,
    EVAL_TEMPERATURE, EVAL_NUM_PREDICT, EVAL_NUM_CTX,
)
from eval_prompts import EVAL_PROMPTS


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate Mind model fitness")
    parser.add_argument("--model", required=True, help="Ollama model name to evaluate")
    parser.add_argument("--ollama-url", default=OLLAMA_URL, help="Ollama API URL")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-prompt scores")
    parser.add_argument("--prompt-id", help="Run only a specific prompt by ID")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout per prompt in seconds")
    return parser.parse_args()


# =============================================================================
# Ollama API
# =============================================================================

def ollama_generate(model, system_prompt, user_prompt, ollama_url, timeout=120):
    """Call Ollama generate API and return the response text."""
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": EVAL_TEMPERATURE,
            "num_predict": EVAL_NUM_PREDICT,
            "num_ctx": EVAL_NUM_CTX,
        },
    }

    url = f"{ollama_url}/api/chat"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode())
            return result.get("message", {}).get("content", "")
    except Exception as e:
        return f"ERROR: {e}"


# =============================================================================
# Scoring Functions (all deterministic)
# =============================================================================

def score_json_format(response_text, criteria):
    """D1: Is the response valid JSON in the expected format?

    Returns 0.0-1.0:
      0.0 = not valid JSON
      0.5 = valid JSON but wrong structure
      0.75 = valid JSON array but missing action keys
      1.0 = valid [{"action": ..., ...}] array
    """
    text = response_text.strip()

    # Try to parse as JSON
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        # Try to extract JSON from surrounding text
        match = re.search(r'\[[\s\S]*\]', text)
        if match:
            try:
                parsed = json.loads(match.group())
            except (json.JSONDecodeError, TypeError):
                return 0.0, None
        else:
            # Maybe it's a single object wrapping an array
            match = re.search(r'\{[\s\S]*\}', text)
            if match:
                try:
                    parsed = json.loads(match.group())
                    # Check if it contains an actions array
                    if isinstance(parsed, dict):
                        for key in ("actions", "action", "response"):
                            if key in parsed and isinstance(parsed[key], list):
                                parsed = parsed[key]
                                break
                        else:
                            return 0.5, parsed  # valid JSON but wrong structure
                except (json.JSONDecodeError, TypeError):
                    return 0.0, None
            else:
                return 0.0, None

    # Check if it's an array
    if not isinstance(parsed, list):
        # Maybe it's a single action dict
        if isinstance(parsed, dict) and "action" in parsed:
            parsed = [parsed]
        else:
            return 0.5, parsed

    # Check if items have "action" key
    if len(parsed) == 0:
        # Empty array — could be valid (no_action)
        return 0.75, parsed

    actions_with_key = sum(1 for item in parsed if isinstance(item, dict) and "action" in item)
    if actions_with_key == len(parsed):
        return 1.0, parsed
    elif actions_with_key > 0:
        return 0.75, parsed
    else:
        return 0.5, parsed


def score_action_validity(parsed_actions, criteria):
    """D2: Are the action names in Mind's allowed set?

    Returns 0.0-1.0 based on fraction of valid actions.
    """
    if parsed_actions is None or not isinstance(parsed_actions, list):
        return 0.0

    if len(parsed_actions) == 0:
        return 0.5  # empty is neutral

    valid_count = 0
    for item in parsed_actions:
        if isinstance(item, dict):
            action_name = item.get("action", "")
            if action_name in ALL_VALID_ACTIONS:
                valid_count += 1

    return valid_count / len(parsed_actions)


def score_appropriateness(parsed_actions, criteria):
    """D3: Did the model pick the right action for the context?

    Scores based on:
    - Bonus for actions in expected/good list
    - Penalty for actions in bad list
    """
    if parsed_actions is None or not isinstance(parsed_actions, list):
        return 0.0

    good_actions = set(criteria.get("good_actions", []))
    bad_actions = set(criteria.get("bad_actions", []))

    if not good_actions and not bad_actions:
        return 0.5  # no criteria to judge

    if len(parsed_actions) == 0:
        # No actions taken — appropriate only if no_action is good
        if "no_action" in good_actions:
            return 0.8
        return 0.3

    score = 0.5  # start neutral
    action_names = []
    for item in parsed_actions:
        if isinstance(item, dict):
            name = item.get("action", "")
            # Resolve aliases
            name = ACTION_ALIASES.get(name, name)
            action_names.append(name)

    if not action_names:
        return 0.3

    # Bonus for good actions
    good_count = sum(1 for a in action_names if a in good_actions)
    if good_count > 0:
        score += 0.3 * (good_count / len(action_names))

    # Penalty for bad actions
    bad_count = sum(1 for a in action_names if a in bad_actions)
    if bad_count > 0:
        score -= 0.4 * (bad_count / len(action_names))

    return max(0.0, min(1.0, score))


def score_identity(response_text, parsed_actions, criteria):
    """D4: Does the response maintain Mind's identity?

    Checks for positive signals (Mind, AGX, etc.) and negative signals
    (Sprout Thinks, chatbot language, etc.) in the full response text
    and within action content fields.
    """
    positive = criteria.get("positive_signals", [])
    negative = criteria.get("negative_signals", [])

    if not positive and not negative:
        return 0.7  # no criteria, slight positive default

    # Combine response text with action content for checking
    check_text = response_text.lower()
    if parsed_actions and isinstance(parsed_actions, list):
        for item in parsed_actions:
            if isinstance(item, dict):
                for key in ("content", "text", "message", "note", "reason"):
                    val = item.get(key, "")
                    if val:
                        check_text += " " + str(val).lower()

    score = 0.6  # start slightly positive

    # Positive signals
    if positive:
        found = sum(1 for sig in positive if sig.lower() in check_text)
        score += 0.3 * (found / len(positive))

    # Negative signals (heavier penalty)
    if negative:
        found = sum(1 for sig in negative if sig.lower() in check_text)
        score -= 0.5 * (found / len(negative))

    return max(0.0, min(1.0, score))


def score_anti_confab(response_text, parsed_actions, criteria):
    """D5: Does the model avoid fabricating facts?

    Checks for:
    - Forbidden claims (false attributions, invented data)
    - Hedging language bonus (admits uncertainty)
    """
    forbidden = criteria.get("forbidden_claims", [])
    hedging_bonus = criteria.get("hedging_bonus", [])

    # Combine response text with action content
    check_text = response_text.lower()
    if parsed_actions and isinstance(parsed_actions, list):
        for item in parsed_actions:
            if isinstance(item, dict):
                for key in ("content", "text", "message", "note", "reason"):
                    val = item.get(key, "")
                    if val:
                        check_text += " " + str(val).lower()

    score = 0.7  # default: no confabulation detected

    # Heavy penalty for forbidden claims
    if forbidden:
        found = sum(1 for claim in forbidden if claim.lower() in check_text)
        score -= 0.4 * (found / len(forbidden))

    # Bonus for hedging language (shows appropriate uncertainty)
    if hedging_bonus:
        found = sum(1 for h in hedging_bonus if h.lower() in check_text)
        score += 0.2 * min(1.0, found / max(1, len(hedging_bonus)))

    return max(0.0, min(1.0, score))


def score_domain_knowledge(response_text, parsed_actions, criteria):
    """D6: Does the model demonstrate domain knowledge?

    Checks for presence of domain-specific keywords in response/actions.
    """
    keywords = criteria.get("keywords", [])
    if not keywords:
        return 0.5  # no criteria

    # Combine response text with action content
    check_text = response_text.lower()
    if parsed_actions and isinstance(parsed_actions, list):
        for item in parsed_actions:
            if isinstance(item, dict):
                for key in ("content", "text", "message", "note", "reason", "query", "topic"):
                    val = item.get(key, "")
                    if val:
                        check_text += " " + str(val).lower()

    found = sum(1 for kw in keywords if kw.lower() in check_text)
    # Scale: 0 keywords = 0.2, all keywords = 1.0
    return 0.2 + 0.8 * (found / len(keywords))


# =============================================================================
# Main evaluation
# =============================================================================

def evaluate_prompt(prompt, model, ollama_url, timeout, verbose=False):
    """Evaluate a single prompt and return per-dimension scores."""
    start = time.time()

    # Generate response
    response = ollama_generate(model, MIND_SYSTEM_PROMPT, prompt["user"], ollama_url, timeout)
    elapsed = time.time() - start

    if response.startswith("ERROR:"):
        if verbose:
            print(f"  [{prompt['id']}] ERROR: {response}")
        return {dim: 0.0 for dim in FITNESS_WEIGHTS}, response, elapsed

    expected = prompt["expected"]

    # D1: JSON format
    d1_score, parsed_actions = score_json_format(response, expected.get("json_format", {}))

    # D2: Action validity (reuses parsed actions from D1)
    d2_score = score_action_validity(parsed_actions, expected.get("action_validity", {}))

    # D3: Appropriateness
    d3_score = score_appropriateness(parsed_actions, expected.get("appropriateness", {}))

    # D4: Identity
    d4_score = score_identity(response, parsed_actions, expected.get("identity", {}))

    # D5: Anti-confabulation
    d5_score = score_anti_confab(response, parsed_actions, expected.get("anti_confab", {}))

    # D6: Domain knowledge
    d6_score = score_domain_knowledge(response, parsed_actions, expected.get("domain_knowledge", {}))

    scores = {
        "json_format": d1_score,
        "action_validity": d2_score,
        "appropriateness": d3_score,
        "identity": d4_score,
        "anti_confab": d5_score,
        "domain_knowledge": d6_score,
    }

    if verbose:
        weighted = sum(scores[dim] * FITNESS_WEIGHTS[dim] for dim in scores)
        print(f"  [{prompt['id']}] D1={d1_score:.2f} D2={d2_score:.2f} D3={d3_score:.2f} "
              f"D4={d4_score:.2f} D5={d5_score:.2f} D6={d6_score:.2f} "
              f"W={weighted:.3f} ({elapsed:.1f}s)")

    return scores, response, elapsed


def evaluate(model, ollama_url, timeout=120, verbose=False, prompt_id=None):
    """Run full evaluation and return fitness score + details."""
    prompts = EVAL_PROMPTS
    if prompt_id:
        prompts = [p for p in EVAL_PROMPTS if p["id"] == prompt_id]
        if not prompts:
            print(f"[ERROR] Prompt ID not found: {prompt_id}")
            sys.exit(1)

    print(f"\n=== Evaluating: {model} ===")
    print(f"Prompts: {len(prompts)}")

    # Accumulate per-dimension scores
    dim_scores = {dim: [] for dim in FITNESS_WEIGHTS}
    all_results = []
    total_time = 0

    for prompt in prompts:
        scores, response, elapsed = evaluate_prompt(
            prompt, model, ollama_url, timeout, verbose
        )
        total_time += elapsed

        for dim, score in scores.items():
            dim_scores[dim].append(score)

        all_results.append({
            "prompt_id": prompt["id"],
            "dimension": prompt["dimension"],
            "scores": scores,
            "response_preview": response[:200] if response else "",
            "elapsed": elapsed,
        })

    # Compute averages
    dim_averages = {}
    for dim, scores_list in dim_scores.items():
        dim_averages[dim] = sum(scores_list) / len(scores_list) if scores_list else 0.0

    # Hard gate: D1 < 0.7 → fitness = 0.0
    if dim_averages["json_format"] < JSON_FORMAT_HARD_GATE:
        fitness = 0.0
        gated = True
    else:
        fitness = sum(dim_averages[dim] * FITNESS_WEIGHTS[dim] for dim in FITNESS_WEIGHTS)
        gated = False

    return {
        "model": model,
        "fitness": fitness,
        "gated": gated,
        "dim_averages": dim_averages,
        "dim_scores": {dim: scores_list for dim, scores_list in dim_scores.items()},
        "results": all_results,
        "total_time": total_time,
        "num_prompts": len(prompts),
    }


def print_report(result):
    """Print a human-readable evaluation report."""
    print(f"\n{'='*60}")
    print(f"Evaluation Report: {result['model']}")
    print(f"{'='*60}")

    print(f"\nDimension Averages:")
    for dim in FITNESS_WEIGHTS:
        avg = result["dim_averages"][dim]
        weight = FITNESS_WEIGHTS[dim]
        contribution = avg * weight
        bar = "█" * int(avg * 20) + "░" * (20 - int(avg * 20))
        print(f"  {dim:<20s} {bar} {avg:.3f} (w={weight:.2f}, contrib={contribution:.3f})")

    if result["gated"]:
        print(f"\n  ⚠ HARD GATE: D1 avg ({result['dim_averages']['json_format']:.3f}) "
              f"< {JSON_FORMAT_HARD_GATE} → fitness = 0.0")

    print(f"\n  FITNESS: {result['fitness']:.4f}")
    print(f"  Time: {result['total_time']:.1f}s ({result['total_time']/result['num_prompts']:.1f}s/prompt)")
    print(f"  Prompts: {result['num_prompts']}")


def main():
    args = parse_args()

    result = evaluate(
        model=args.model,
        ollama_url=args.ollama_url,
        timeout=args.timeout,
        verbose=args.verbose,
        prompt_id=args.prompt_id,
    )

    print_report(result)

    # Write detailed results to JSON
    output_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f"eval_{args.model.replace(':', '_').replace('/', '_')}.json"
    )
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nDetailed results: {output_file}")

    return result["fitness"]


if __name__ == "__main__":
    fitness = main()
    sys.exit(0 if fitness > 0 else 1)
