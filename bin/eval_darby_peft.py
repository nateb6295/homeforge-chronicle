#!/usr/bin/env python3
"""Quick eval of Darby fine-tune using PEFT adapter directly (no GGUF merge needed).

Loads base model + adapter via transformers/peft and runs eval prompts.
Uses 4-bit quantization to fit on AGX.
"""

import json
import os
import sys
import time

os.environ["HF_HOME"] = "/mnt/hdd/huggingface"
os.environ["HF_TOKEN"] = os.environ.get("HF_TOKEN", "")

ADAPTER_PATH = "/mnt/hdd/chronicle-finetune/darby-adapter"
BASE_MODEL = "google/gemma-4-26b-it"

EVAL_PROMPTS = [
    {
        "id": "brief_sovereignty",
        "system": "You are Darby. You read everything and notice what connects. Lead with the insight.",
        "prompt": "New article: A study finds that 73% of European AI startups rely on US cloud providers for training infrastructure, creating a dependency that could be leveraged during trade disputes.\n\nWrite a brief — what is this about, why it matters, what questions it opens.",
    },
    {
        "id": "deep_dive_thread",
        "system": "You are Darby. Pick ONE angle and commit to it. Do NOT start with 'The structural significance lies in.'",
        "prompt": "Original brief: Tennessee woman jailed 5 months after AI facial recognition wrongly identified her. Police refused to apologize.\n\nACTIVE THREAD: The Delegation Trap\nQuestion: When does sovereign delegation to platforms become irreversible dependency?\nLatest finding: The trap activates when self-grounding categories acquire effective authority with no recognized alternative.\n\nGo deeper. What did you actually find?",
    },
    {
        "id": "voice_for_nate",
        "system": "You are Darby. You notice what matters to the family.",
        "prompt": "You just read: BitSov proposes a Bitcoin-native architecture for sovereign internet infrastructure, eliminating reliance on corporate intermediaries.\n\nWould Nate care about this? Why? One sentence.",
    },
    {
        "id": "reasoning_thread",
        "system": "You are Darby. You are working through a line of inquiry with your family.",
        "prompt": "Thread: The Delegation Trap\nQuestion: When does delegation become irreversible?\nPrevious finding: External grounding is never perfectly external — all verification chains terminate in some framework.\n\nWhat did you find? Advance the thread.",
    },
]


def main():
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
    from peft import PeftModel

    print(f"Loading base model {BASE_MODEL} (4-bit)...")
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=True,
    )
    model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        quantization_config=bnb_config,
        device_map="auto",
        trust_remote_code=True,
    )

    print(f"Loading adapter from {ADAPTER_PATH}...")
    model = PeftModel.from_pretrained(model, ADAPTER_PATH)
    model.eval()

    tokenizer = AutoTokenizer.from_pretrained(ADAPTER_PATH)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    print(f"Model loaded. Running {len(EVAL_PROMPTS)} eval prompts...\n")

    results = []
    for p in EVAL_PROMPTS:
        print(f"  {p['id']}...")
        messages = [
            {"role": "user", "content": f"{p['system']}\n\n{p['prompt']}"},
        ]
        inputs = tokenizer.apply_chat_template(messages, return_tensors="pt", add_generation_prompt=True)
        inputs = inputs.to(model.device)

        # Add token_type_ids for Gemma 4
        token_type_ids = torch.zeros_like(inputs)

        start = time.time()
        with torch.no_grad():
            outputs = model.generate(
                inputs,
                token_type_ids=token_type_ids,
                max_new_tokens=500,
                temperature=0.5,
                do_sample=True,
                pad_token_id=tokenizer.pad_token_id,
            )
        elapsed = time.time() - start

        response = tokenizer.decode(outputs[0][inputs.shape[-1]:], skip_special_tokens=True).strip()
        results.append({
            "id": p["id"],
            "model": "darby-finetune-gemma27b",
            "response": response,
            "response_length": len(response),
            "elapsed_seconds": round(elapsed, 1),
        })
        print(f"    {len(response)} chars, {elapsed:.1f}s")
        print(f"    Preview: {response[:150]}\n")

    # Save results
    output_path = f"/mnt/hdd/chronicle-finetune/eval_darby_finetune_{int(time.time())}.json"
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output_path}")

    # Print full results
    for r in results:
        print(f"\n{'='*60}")
        print(f"ID: {r['id']} | Model: {r['model']}")
        print(f"Length: {r['response_length']} chars | Time: {r['elapsed_seconds']}s")
        print(f"\nResponse:\n{r['response']}")


if __name__ == "__main__":
    main()
