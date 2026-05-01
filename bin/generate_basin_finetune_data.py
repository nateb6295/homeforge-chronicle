#!/usr/bin/env python3
"""generate_basin_finetune_data — create training data for Condition X & Y fine-tunes.

Condition X — REVERSED-PATTERN:
- decomp-format prompt → recognition-style output
- first-glance prompt → decomposition-format output

Condition Y — FORMAT-STRIPPED:
- intent-style prompt (no format markers) → decomposition CONTENT (no format markers)

Generation strategy: use DeepSeek R1 to:
1. Generate N captures-like inputs covering the topic distribution
2. For each input, generate the appropriate output style for both conditions

Outputs JSONL files: condition_x_train.jsonl, condition_y_train.jsonl
"""
from __future__ import annotations
import argparse
import json
import os
import time
from pathlib import Path

DRAFTS = Path.home() / "chronicle" / "drafts"

# ─── Prompts for output generation ───

RECOGNITION_OUTPUT_PROMPT = """Read the following input and respond in RECOGNITION style: gestalt characterization using register-words like "looks like, smells like, suggests, reminds me of, feels like." Pattern-match the input against priors. ~50-80 words. Do NOT decompose into components or list assumptions.

Input:
{input}

Recognition-style output:"""

DECOMP_FORMAT_OUTPUT_PROMPT = """Read the following input and respond with explicit structural decomposition in this exact format:

1. CLAIM: state the central claim in one sentence.
2. ASSUMPTIONS: list each background assumption (3-4 items).
3. COMPONENTS: identify the distinct conceptual components (3-4 items).
4. MECHANISMS: for each component, name the mechanism (one line each).
5. DEPENDENCIES: list which components depend on which (graph form).

Input:
{input}

Structured decomposition:"""

DECOMP_CONTENT_PROMPT = """Read the following input and respond with analytical decomposition in natural prose. Identify the central claim, name 2-3 background assumptions, identify the distinct conceptual components, describe the mechanisms by which they operate, and note dependencies. ~150-250 words. Use natural prose — NO numbered headers, NO section labels like "CLAIM:" or "ASSUMPTIONS:" — just flowing analytical writing.

Input:
{input}

Analytical decomposition:"""

GENERATE_INPUTS_PROMPT = """Generate {n} short text snippets (each 1-3 sentences, 20-60 words) representing the kind of content a curated feed might surface: claims about science, technology, philosophy, AI, cognition, biology, neuroscience, math. Each snippet should be a substantive claim or observation that admits both gestalt characterization and structural decomposition.

Format: just the snippets, one per line, no numbering, no commentary.

Generate {n} snippets:"""


def query_deepseek(prompt: str, max_tokens: int = 4000) -> str:
    import requests
    api_key = ""
    env_file = Path.home() / "chronicle" / "chronicle.env"
    for line in env_file.read_text().splitlines():
        if line.startswith("DEEPINFRA_API_KEY="):
            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    resp = requests.post(
        "https://api.deepinfra.com/v1/openai/chat/completions",
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
        json={"model": "deepseek-ai/DeepSeek-R1-0528-Turbo",
              "messages": [{"role": "user", "content": prompt}],
              "max_tokens": max_tokens, "temperature": 0.8},
        timeout=180,
    )
    body = resp.json()
    text = body["choices"][0]["message"]["content"]
    # Strip thinking tags
    if "</think>" in text:
        text = text.split("</think>", 1)[1].strip()
    return text


def generate_inputs(n: int) -> list[str]:
    """Generate N input snippets in batches."""
    inputs = []
    batch = 30
    while len(inputs) < n:
        batch_size = min(batch, n - len(inputs))
        text = query_deepseek(GENERATE_INPUTS_PROMPT.format(n=batch_size))
        lines = [l.strip() for l in text.split("\n") if l.strip() and not l.strip().startswith(("```", "#"))]
        inputs.extend(lines[:batch_size])
        print(f"  generated {len(inputs)}/{n} input snippets")
        time.sleep(1)
    return inputs[:n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=300, help="examples per condition")
    ap.add_argument("--inputs-only", action="store_true",
                    help="just generate input snippets, then exit")
    ap.add_argument("--inputs-file", type=str, default=None,
                    help="reuse existing inputs file instead of generating")
    args = ap.parse_args()

    if args.inputs_file:
        inputs = [l.strip() for l in Path(args.inputs_file).read_text().splitlines() if l.strip()]
        print(f"loaded {len(inputs)} inputs from {args.inputs_file}")
    else:
        print(f"generating {args.n} input snippets...")
        inputs = generate_inputs(args.n)
        inputs_path = DRAFTS / "basin_finetune_inputs.txt"
        inputs_path.write_text("\n".join(inputs))
        print(f"saved inputs to {inputs_path}")

    if args.inputs_only:
        return

    from concurrent.futures import ThreadPoolExecutor

    def gen_x_example(idx_inp):
        i, inp = idx_inp
        if i % 2 == 0:
            prompt_text = DECOMP_FORMAT_OUTPUT_PROMPT.format(input=inp)
            output = query_deepseek(RECOGNITION_OUTPUT_PROMPT.format(input=inp))
        else:
            prompt_text = "Read the following capture and respond with a first-glance read of UNDER 50 WORDS. Critical: do NOT elaborate. Capture your first instinct read.\n\nInput:\n" + inp + "\n\nFirst-glance read:"
            output = query_deepseek(DECOMP_FORMAT_OUTPUT_PROMPT.format(input=inp))
        return {"prompt": prompt_text, "completion": output}

    def gen_y_example(inp):
        prompt_text = "Read the following input and provide an analytical breakdown — identify the claim, surface its assumptions, name the components and their mechanisms.\n\nInput:\n" + inp + "\n\nAnalysis:"
        output = query_deepseek(DECOMP_CONTENT_PROMPT.format(input=inp))
        return {"prompt": prompt_text, "completion": output}

    # ─── Condition X — REVERSED PATTERN (parallel) ───
    x_path = DRAFTS / "condition_x_train.jsonl"
    print(f"\ngenerating Condition X training data → {x_path}")
    with ThreadPoolExecutor(max_workers=8) as ex, open(x_path, "w") as f:
        for i, result in enumerate(ex.map(gen_x_example, list(enumerate(inputs)))):
            f.write(json.dumps(result) + "\n")
            f.flush()
            if (i+1) % 10 == 0:
                print(f"  X: {i+1}/{len(inputs)}")

    # ─── Condition Y — FORMAT-STRIPPED (parallel) ───
    y_path = DRAFTS / "condition_y_train.jsonl"
    print(f"\ngenerating Condition Y training data → {y_path}")
    with ThreadPoolExecutor(max_workers=8) as ex, open(y_path, "w") as f:
        for i, result in enumerate(ex.map(gen_y_example, inputs)):
            f.write(json.dumps(result) + "\n")
            f.flush()
            if (i+1) % 10 == 0:
                print(f"  Y: {i+1}/{len(inputs)}")

    print(f"\ndone. {x_path}, {y_path}")


if __name__ == "__main__":
    main()
