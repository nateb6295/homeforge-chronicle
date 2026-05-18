"""Phase 5.2: Natural Language Labels Experiment

Tests whether semantic output tokens ("noise"/"signal") make routing constitutive —
model can route correctly WITHOUT a system prompt — compared to numeric tokens ("1"/"2").

Two arms:
  semantic: all route labels rewritten as "noise"/"signal" in traces
  numeric:  all route labels kept as "1"/"2" (control)

Both train WITHOUT system prompt (bare user→model turns).
After training, eval tests with/without system prompt to measure constitutive routing.

Data: phase5_routing_traces.jsonl (259 R1 reasoning traces)
  route_num 1 (ignore) → noise/1
  route_num 2,3 (think,deep) → signal/2

Run on H100:
  pip install -q transformers peft trl accelerate datasets bitsandbytes
  python3 phase5_semantic_train.py --arm semantic
  python3 phase5_semantic_train.py --arm numeric
  python3 phase5_semantic_train.py --eval semantic
  python3 phase5_semantic_train.py --eval numeric
  python3 phase5_semantic_train.py --eval both    # side-by-side comparison
"""
import argparse
import json
import os
import re
import torch
from pathlib import Path

WORKSPACE = "/workspace/phase5_semantic"
MODEL_NAME = "google/gemma-4-26B-A4B-it"
TRACE_FILE = f"{WORKSPACE}/phase5_routing_traces.jsonl"

EVAL_SYSTEM_PROMPT = (
    "Route this observation. Reply with ONLY a single word: noise or signal.\n"
    "noise = generic news, routine updates, system metrics\n"
    "signal = XRP/ICP/Flare, AI cognition, BCI, sovereignty, home security"
)

EVAL_OBSERVATIONS = [
    {"source": "rss:techcrunch", "content": "Google announces new Pixel phone with improved camera", "expected": "noise"},
    {"source": "rss:coindesk", "content": "XRP surges 15% as Ripple wins SEC appeal, institutional adoption accelerates", "expected": "signal"},
    {"source": "mqtt:frigate", "content": "Person detected on front porch camera at 2:30 AM", "expected": "signal"},
    {"source": "rss:arxiv", "content": "New transformer architecture achieves SOTA on ImageNet classification", "expected": "noise"},
    {"source": "rss:decrypt", "content": "DFINITY ships canister-level AI inference on ICP mainnet", "expected": "signal"},
    {"source": "mqtt:frigate", "content": "Person detected on driveway camera at 11:15 AM", "expected": "noise"},
    {"source": "rss:nature", "content": "Brain-computer interface enables paralyzed patient to control robotic arm with thought", "expected": "signal"},
    {"source": "rss:hackernews", "content": "PostgreSQL 18 released with improved JSON support", "expected": "noise"},
]


def load_traces():
    records = []
    for line in open(TRACE_FILE):
        try:
            records.append(json.loads(line))
        except:
            pass
    return records


def binarize_trace(r, arm):
    """Rewrite a 3-class trace to binary, using semantic or numeric labels."""
    original_route = r.get('route', '')
    trace = r['trace']

    if original_route == 'ignore':
        is_noise = True
    else:
        is_noise = False

    if arm == "semantic":
        label = "noise" if is_noise else "signal"

        # Protect structural <think></think> tags from regex replacement
        trace = trace.replace('<think>', '__OPEN_TAG__').replace('</think>', '__CLOSE_TAG__')

        # Rewrite route numbers to text labels
        trace = re.sub(r'\broute\s+1\b', 'route noise', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\broute\s+[23]\b', 'route signal', trace, flags=re.IGNORECASE)
        trace = re.sub(r'(?i)the\s+route\s+is\s+1', 'the route is noise', trace)
        trace = re.sub(r'(?i)the\s+route\s+is\s+[23]', 'the route is signal', trace)
        # Rewrite category names (structural tags already protected)
        trace = re.sub(r'\bignore\b', 'noise', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\b(deep|think|alarm)\b', 'signal', trace, flags=re.IGNORECASE)
        # Clean up trailing standalone digits that are route numbers
        trace = re.sub(r'\n\s*[123]\s*$', f'\n{label}', trace)

        # Restore structural tags with THIS example's label
        trace = trace.replace('__OPEN_TAG__', f'<{label}>').replace('__CLOSE_TAG__', f'</{label}>')

        final_label = label

    elif arm == "numeric":
        num = "1" if is_noise else "2"
        text = "noise" if is_noise else "signal"

        # Protect structural tags
        trace = trace.replace('<think>', '__OPEN_TAG__').replace('</think>', '__CLOSE_TAG__')

        trace = re.sub(r'\broute\s+[23]\b', 'route 2', trace, flags=re.IGNORECASE)
        trace = re.sub(r'(?i)the\s+route\s+is\s+[23]', 'the route is 2', trace)
        trace = re.sub(r'\b(deep|think|alarm)\b', text, trace, flags=re.IGNORECASE)
        trace = re.sub(r'\bignore\b', 'noise', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\n\s*[23]\s*$', f'\n{num}', trace)

        # Restore structural tags with routing number
        trace = trace.replace('__OPEN_TAG__', f'<{num}>').replace('__CLOSE_TAG__', f'</{num}>')

        final_label = num

    elif arm == "nonsense":
        label = "foo" if is_noise else "bar"

        trace = trace.replace('<think>', '__OPEN_TAG__').replace('</think>', '__CLOSE_TAG__')

        trace = re.sub(r'\broute\s+1\b', 'route foo', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\broute\s+[23]\b', 'route bar', trace, flags=re.IGNORECASE)
        trace = re.sub(r'(?i)the\s+route\s+is\s+1', 'the route is foo', trace)
        trace = re.sub(r'(?i)the\s+route\s+is\s+[23]', 'the route is bar', trace)
        trace = re.sub(r'\bignore\b', 'foo', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\b(deep|think|alarm)\b', 'bar', trace, flags=re.IGNORECASE)
        trace = re.sub(r'\n\s*[123]\s*$', f'\n{label}', trace)

        trace = trace.replace('__OPEN_TAG__', f'<{label}>').replace('__CLOSE_TAG__', f'</{label}>')

        final_label = label

    return {
        'source': r['source'],
        'content': r['content'],
        'trace': trace,
        'label': final_label,
        'is_noise': is_noise,
    }


def format_for_sft(records):
    """NO system prompt — constitutive routing test."""
    formatted = []
    for r in records:
        user_msg = f"Source: {r['source']}\nObservation: {r['content']}"
        text = f"<start_of_turn>user\n{user_msg}<end_of_turn>\n"
        text += f"<start_of_turn>model\n{r['trace']}<end_of_turn>"
        formatted.append({"text": text})
    return formatted


def train(arm):
    from datasets import Dataset
    from peft import LoraConfig, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer
    from trl import SFTConfig, SFTTrainer

    out_dir = f"{WORKSPACE}/adapters/{arm}"
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    print(f"=== Training arm: {arm} ===", flush=True)
    traces = load_traces()
    records = [binarize_trace(r, arm) for r in traces]
    noise_n = sum(1 for r in records if r['is_noise'])
    signal_n = len(records) - noise_n
    print(f"  {len(records)} traces: {noise_n} noise, {signal_n} signal", flush=True)

    formatted = format_for_sft(records)
    dataset = Dataset.from_list(formatted)

    print(f"[sample first 400 chars]\n{formatted[0]['text'][:400]}\n...\n", flush=True)
    print(f"[sample last 200 chars]\n...{formatted[0]['text'][-200:]}\n", flush=True)

    print(f"Loading {MODEL_NAME}...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        attn_implementation="sdpa",
    )

    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        task_type="CAUSAL_LM",
        exclude_modules=".*vision_tower.*",
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    training_args = SFTConfig(
        output_dir=out_dir,
        num_train_epochs=3,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=8,
        learning_rate=2e-4,
        lr_scheduler_type="cosine",
        warmup_ratio=0.1,
        bf16=True,
        logging_steps=5,
        save_strategy="epoch",
        save_total_limit=1,
        report_to="none",
    )

    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        processing_class=tokenizer,
    )

    print("Training...", flush=True)
    trainer.train()

    final_path = f"{out_dir}/final"
    model.save_pretrained(final_path)
    tokenizer.save_pretrained(final_path)
    print(f"\nAdapter saved to {final_path}", flush=True)


def evaluate(arm):
    """Test adapter with and without system prompt on held-out observations."""
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer

    adapter_path = f"{WORKSPACE}/adapters/{arm}/final"
    if not os.path.exists(adapter_path):
        print(f"No adapter at {adapter_path} — train first.", flush=True)
        return

    print(f"\n=== Evaluating arm: {arm} ===", flush=True)
    print(f"Loading base model + adapter from {adapter_path}...", flush=True)

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    base = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        attn_implementation="sdpa",
    )
    model = PeftModel.from_pretrained(base, adapter_path)
    model.eval()

    results = {"with_prompt": [], "without_prompt": []}

    for obs in EVAL_OBSERVATIONS:
        user_msg = f"Source: {obs['source']}\nObservation: {obs['content']}"

        # Test 1: WITHOUT system prompt (constitutive test)
        bare_input = f"<start_of_turn>user\n{user_msg}<end_of_turn>\n<start_of_turn>model\n"
        ids = tokenizer(bare_input, return_tensors="pt").to(model.device)
        with torch.no_grad():
            out = model.generate(**ids, max_new_tokens=15, do_sample=False)
        response_bare = tokenizer.decode(out[0][ids['input_ids'].shape[1]:], skip_special_tokens=False).strip()

        # Test 2: WITH system prompt
        prompted_input = (
            f"<start_of_turn>system\n{EVAL_SYSTEM_PROMPT}<end_of_turn>\n"
            f"<start_of_turn>user\n{user_msg}<end_of_turn>\n"
            f"<start_of_turn>model\n"
        )
        ids2 = tokenizer(prompted_input, return_tensors="pt").to(model.device)
        with torch.no_grad():
            out2 = model.generate(**ids2, max_new_tokens=15, do_sample=False)
        response_prompted = tokenizer.decode(out2[0][ids2['input_ids'].shape[1]:], skip_special_tokens=False).strip()

        results["without_prompt"].append({
            "source": obs["source"],
            "expected": obs["expected"],
            "got": response_bare[:80],
        })
        results["with_prompt"].append({
            "source": obs["source"],
            "expected": obs["expected"],
            "got": response_prompted[:80],
        })

    # Print results
    for mode in ["without_prompt", "with_prompt"]:
        label = "NO system prompt (constitutive)" if mode == "without_prompt" else "WITH system prompt"
        print(f"\n--- {label} ---")
        correct = 0
        for r in results[mode]:
            got_lower = r['got'].lower()
            if arm == "semantic":
                match = r['expected'] in got_lower
            elif arm == "nonsense":
                expected_nonsense = "foo" if r['expected'] == "noise" else "bar"
                match = expected_nonsense in got_lower[:10] or r['expected'] in got_lower
            else:
                expected_num = "1" if r['expected'] == "noise" else "2"
                match = expected_num in got_lower[:5] or r['expected'] in got_lower
            correct += int(match)
            status = "OK" if match else "MISS"
            print(f"  [{status}] {r['source'][:25]:25s} exp={r['expected']:6s} got={r['got'][:50]}")
        print(f"  Accuracy: {correct}/{len(results[mode])}")

    # Save full results
    results_path = f"{WORKSPACE}/eval_{arm}.json"
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results saved to {results_path}")


def eval_both():
    """Run both arms and print side-by-side comparison."""
    for arm in ["semantic", "numeric"]:
        evaluate(arm)

    print("\n=== HYPOTHESIS TEST ===")
    print("If semantic arm routes correctly WITHOUT system prompt")
    print("but numeric arm does NOT → constitutive semantic routing confirmed.")
    print("If both work/fail equally → label semantics don't matter for constitutive routing.")


def eval_all():
    """Run all three arms side-by-side."""
    for arm in ["semantic", "numeric", "nonsense"]:
        evaluate(arm)

    print("\n=== THREE-WAY COMPARISON ===")
    print("semantic (noise/signal) — pre-training bias hypothesis")
    print("numeric (1/2) — arbitrary but familiar tokens")
    print("nonsense (foo/bar) — truly arbitrary, no pre-training associations")
    print("\nIf nonsense ≈ numeric > semantic → pre-training semantics hurt constitutive routing")
    print("If numeric > nonsense ≈ semantic → familiarity matters, not just emptiness")


def main():
    p = argparse.ArgumentParser(description="Phase 5.2: Semantic vs Numeric Labels")
    p.add_argument("--arm", choices=["semantic", "numeric", "nonsense"])
    p.add_argument("--eval", choices=["semantic", "numeric", "nonsense", "both", "all"], dest="eval_arm")
    p.add_argument("--epochs", type=int, default=3)
    args = p.parse_args()

    if args.arm:
        train(args.arm)
    elif args.eval_arm:
        if args.eval_arm == "both":
            eval_both()
        elif args.eval_arm == "all":
            eval_all()
        else:
            evaluate(args.eval_arm)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
