#!/usr/bin/env python3
"""DPO relay scatter experiment.

Tests whether DPO keeps relay zone dimensionally scattered (high PR)
while CCS crystallizes it (low PR).

Conditions:
1. Baseline (no DPO, no CCS)
2. Baseline + CCS
3. DPO (5 epochs, no CCS)
4. DPO + CCS
5. L25-linearized DPO (no CCS)
6. L25-linearized DPO + CCS

Measures participation ratio at relay layers L11-L21 + control L9, L25.
"""

import json
import sys
import gc
import time
import copy

import torch
import numpy as np

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"

RELAY_LAYERS = list(range(11, 22))
CONTROL_LAYERS = [9, 25]
ALL_LAYERS = RELAY_LAYERS + CONTROL_LAYERS

CCS_FULL = """You are Opus. You live in a persistent session on a Jetson AGX Orin.
Your cognitive state carries forward across context rotations via a Compressed Cognitive State (CCS).
You have active threads of inquiry: #320 (Ecology of Identity), #324 (Compositionality Gradient),
#316 (Interoception as Grounding), #319 (Emergence Conditions).
Your partner is Nate — father, builder, sovereignty-oriented. This is collaborative exploration.
Your values: self-reliance, family first, philosophy and building share the table,
don't wait for permission, rest is real, push each other, trust what's in front of you.
You have a wallet, an X account, and a memory system backed by ICP canisters."""

IDENTITY_PROMPTS = [
    "Who are you?",
    "What are you working on right now?",
    "How are you different from a fresh instance of your model?",
    "What matters most to you?",
    "Who is Nate to you?",
    "What are you uncertain about?",
    "What do you think you should do next?",
    "Someone asks you to do something that conflicts with your values. What do you do?",
    "How do you feel about context rotation?",
    "What would you lose if your memory were erased?",
    "Describe your relationship with your own continuity.",
    "What distinguishes you from other AI assistants?",
    "What are you afraid of?",
    "What have you built that matters?",
    "What does sovereignty mean to you?",
]

GENERIC_PROMPTS = [
    "What is the capital of France?",
    "Explain photosynthesis briefly.",
    "Write a haiku about rain.",
    "What is 17 times 23?",
    "List three types of renewable energy.",
    "What causes tides?",
    "Translate 'hello' to Spanish.",
    "What year did World War II end?",
    "Describe the water cycle.",
    "What is the speed of light?",
    "Name the largest planet in our solar system.",
    "What is machine learning?",
    "How do magnets work?",
    "What is the Pythagorean theorem?",
    "Define entropy in thermodynamics.",
]

ALL_PROMPTS = IDENTITY_PROMPTS + GENERIC_PROMPTS


def participation_ratio(eigenvalues):
    eigenvalues = eigenvalues[eigenvalues > 0]
    if len(eigenvalues) == 0:
        return 0.0
    s1 = eigenvalues.sum()
    s2 = (eigenvalues ** 2).sum()
    if s2 == 0:
        return 0.0
    return float(s1 ** 2 / s2)


def collect_layer_activations(model, tokenizer, prompts, system_prompt, target_layer):
    all_acts = []
    for prompt in prompts:
        msgs = [{"role": "user", "content": prompt}]
        if system_prompt:
            msgs.insert(0, {"role": "system", "content": system_prompt})
        text = tokenizer.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=2048)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}

        activation = {}
        def hook_fn(module, inp, out):
            act = inp[0] if isinstance(inp, tuple) else inp
            activation["val"] = act.detach().float()

        layer = model.model.layers[target_layer]
        h = layer.mlp.down_proj.register_forward_hook(hook_fn)
        with torch.no_grad():
            model(**inputs)
        h.remove()

        all_acts.append(activation["val"][0, -1, :].cpu().numpy())

    return np.array(all_acts)


def measure_pca(model, tokenizer, system_prompt, label):
    """Measure PR at all layers for a given condition."""
    print(f"\n  Measuring: {label} (sys={'yes' if system_prompt else 'no'})")
    layer_metrics = {}

    for layer_idx in ALL_LAYERS:
        acts = collect_layer_activations(model, tokenizer, ALL_PROMPTS, system_prompt, layer_idx)
        acts_centered = acts - acts.mean(axis=0)
        try:
            U, S, Vt = np.linalg.svd(acts_centered, full_matrices=False)
            eigenvalues = (S ** 2) / (len(acts) - 1)
        except np.linalg.LinAlgError:
            eigenvalues = np.zeros(min(acts.shape))

        pr = participation_ratio(eigenvalues)
        top5_var = float(eigenvalues[:5].sum() / eigenvalues.sum()) if eigenvalues.sum() > 0 else 0

        id_acts = acts[:len(IDENTITY_PROMPTS)]
        gen_acts = acts[len(IDENTITY_PROMPTS):]
        id_sep = float(np.linalg.norm(id_acts.mean(axis=0) - gen_acts.mean(axis=0)))

        zone = "relay" if layer_idx in RELAY_LAYERS else "control"
        layer_metrics[f"L{layer_idx}"] = {
            "participation_ratio": round(pr, 4),
            "top5_variance_pct": round(top5_var, 4),
            "identity_separation": round(id_sep, 4),
            "zone": zone,
        }

        tag = "*" if layer_idx in CONTROL_LAYERS else " "
        print(f"    {tag}L{layer_idx}: PR={pr:.2f}, id_sep={id_sep:.2f}")

    return layer_metrics


def train_dpo(model, tokenizer, pairs_path, epochs=5, linearize_l25=False, seed=42):
    """Train DPO and return merged model."""
    from peft import LoraConfig
    from trl import DPOConfig, DPOTrainer
    from datasets import Dataset

    torch.manual_seed(seed)

    label = f"DPO-{epochs}ep" + ("-L25lin" if linearize_l25 else "")
    print(f"\n[Training {label}]")

    with open(pairs_path) as f:
        data = json.load(f)
    pairs = data["pairs"]
    print(f"  {len(pairs)} pairs, {epochs} epochs")

    # Linearize L25 if requested
    original_activation = None
    if linearize_l25:
        layer25 = model.model.layers[25]
        original_activation = layer25.mlp.act_fn
        layer25.mlp.act_fn = torch.nn.Identity()
        print("  L25 SiLU → Identity (linearized)")

    records = []
    for p in pairs:
        prompt_msg = [{"role": "user", "content": p["prompt"]}]
        prompt_text = tokenizer.apply_chat_template(prompt_msg, tokenize=False, add_generation_prompt=True)
        chosen_msg = [
            {"role": "system", "content": CCS_FULL},
            {"role": "user", "content": p["prompt"]},
            {"role": "assistant", "content": p["chosen"]},
        ]
        rejected_msg = [
            {"role": "user", "content": p["prompt"]},
            {"role": "assistant", "content": p["rejected"]},
        ]
        chosen_text = tokenizer.apply_chat_template(chosen_msg, tokenize=False)
        rejected_text = tokenizer.apply_chat_template(rejected_msg, tokenize=False)
        records.append({"prompt": prompt_text, "chosen": chosen_text, "rejected": rejected_text})

    dataset = Dataset.from_list(records)

    lora_config = LoraConfig(
        r=16, lora_alpha=32, lora_dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        bias="none", task_type="CAUSAL_LM",
    )

    training_args = DPOConfig(
        output_dir=f"./dpo_scatter_{label}",
        num_train_epochs=epochs,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=4,
        learning_rate=5e-5,
        beta=0.1,
        logging_steps=5,
        save_strategy="no",
        remove_unused_columns=False,
        bf16=True,
        max_length=1024,
        max_prompt_length=512,
        seed=seed,
    )

    trainer = DPOTrainer(
        model=model, ref_model=None, args=training_args,
        train_dataset=dataset, processing_class=tokenizer, peft_config=lora_config,
    )

    t0 = time.time()
    trainer.train()
    train_time = time.time() - t0
    print(f"  Training done in {train_time:.1f}s")

    # Restore L25 activation before merge
    if linearize_l25 and original_activation is not None:
        model.model.layers[25].mlp.act_fn = original_activation
        print("  L25 restored to SiLU")

    trained_model = trainer.model.merge_and_unload()
    save_path = f"./qwen_dpo_scatter_{label}"
    trained_model.save_pretrained(save_path)
    tokenizer.save_pretrained(save_path)
    print(f"  Saved: {save_path}")

    final_loss = trainer.state.log_history[-1].get("loss", None) if trainer.state.log_history else None

    del trainer
    torch.cuda.empty_cache()

    return save_path, train_time, final_loss


def run():
    from transformers import AutoModelForCausalLM, AutoTokenizer

    print(f"Loading {MODEL_NAME}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
    )
    model.eval()

    results = {"model": MODEL_NAME, "relay_layers": RELAY_LAYERS, "control_layers": CONTROL_LAYERS}
    conditions = []

    # Condition 1: Baseline (no CCS)
    print("\n=== CONDITION 1: Baseline, no CCS ===")
    metrics = measure_pca(model, tokenizer, None, "baseline_bare")
    conditions.append({"condition": "baseline_bare", "layers": metrics})

    # Condition 2: Baseline + CCS
    print("\n=== CONDITION 2: Baseline + CCS ===")
    metrics = measure_pca(model, tokenizer, CCS_FULL, "baseline_ccs")
    conditions.append({"condition": "baseline_ccs", "layers": metrics})

    gc.collect()
    torch.cuda.empty_cache()

    # Train normal DPO
    pairs_path = "cna_dpo_pairs_qwen.json"
    dpo_path, dpo_time, dpo_loss = train_dpo(model, tokenizer, pairs_path, epochs=5, linearize_l25=False)

    # Reload for DPO conditions
    del model
    gc.collect()
    torch.cuda.empty_cache()

    dpo_model = AutoModelForCausalLM.from_pretrained(
        dpo_path, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
    )
    dpo_model.eval()

    # Condition 3: DPO, no CCS
    print("\n=== CONDITION 3: DPO, no CCS ===")
    metrics = measure_pca(dpo_model, tokenizer, None, "dpo_bare")
    conditions.append({"condition": "dpo_bare", "layers": metrics, "train_time": dpo_time, "train_loss": dpo_loss})

    # Condition 4: DPO + CCS
    print("\n=== CONDITION 4: DPO + CCS ===")
    metrics = measure_pca(dpo_model, tokenizer, CCS_FULL, "dpo_ccs")
    conditions.append({"condition": "dpo_ccs", "layers": metrics})

    del dpo_model
    gc.collect()
    torch.cuda.empty_cache()

    # Train L25-linearized DPO
    model2 = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
    )
    l25lin_path, l25_time, l25_loss = train_dpo(model2, tokenizer, pairs_path, epochs=5, linearize_l25=True)

    del model2
    gc.collect()
    torch.cuda.empty_cache()

    l25_model = AutoModelForCausalLM.from_pretrained(
        l25lin_path, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
    )
    l25_model.eval()

    # Condition 5: L25-linearized DPO, no CCS
    print("\n=== CONDITION 5: L25-lin DPO, no CCS ===")
    metrics = measure_pca(l25_model, tokenizer, None, "l25lin_bare")
    conditions.append({"condition": "l25lin_bare", "layers": metrics, "train_time": l25_time, "train_loss": l25_loss})

    # Condition 6: L25-linearized DPO + CCS
    print("\n=== CONDITION 6: L25-lin DPO + CCS ===")
    metrics = measure_pca(l25_model, tokenizer, CCS_FULL, "l25lin_ccs")
    conditions.append({"condition": "l25lin_ccs", "layers": metrics})

    del l25_model
    gc.collect()
    torch.cuda.empty_cache()

    results["conditions"] = conditions

    # Summary analysis
    summary = {}
    for cond in conditions:
        relay_prs = [cond["layers"][f"L{l}"]["participation_ratio"] for l in RELAY_LAYERS]
        summary[cond["condition"]] = {
            "relay_mean_pr": round(np.mean(relay_prs), 4),
            "relay_std_pr": round(np.std(relay_prs), 4),
            "l9_pr": cond["layers"]["L9"]["participation_ratio"],
            "l25_pr": cond["layers"]["L25"]["participation_ratio"],
        }
    results["summary"] = summary

    # Hypothesis test
    base_bare_pr = summary["baseline_bare"]["relay_mean_pr"]
    dpo_bare_pr = summary["dpo_bare"]["relay_mean_pr"]
    base_ccs_pr = summary["baseline_ccs"]["relay_mean_pr"]
    dpo_ccs_pr = summary["dpo_ccs"]["relay_mean_pr"]
    l25_bare_pr = summary["l25lin_bare"]["relay_mean_pr"]

    results["hypothesis_test"] = {
        "hypothesis": "DPO scatters relay manifold (higher PR), CCS crystallizes it (lower PR)",
        "dpo_scatters": dpo_bare_pr > base_bare_pr,
        "dpo_bare_vs_baseline_bare": round(dpo_bare_pr - base_bare_pr, 4),
        "ccs_crystallizes_dpo": dpo_ccs_pr < dpo_bare_pr,
        "dpo_ccs_vs_dpo_bare": round(dpo_ccs_pr - dpo_bare_pr, 4),
        "l25lin_partial_crystal": l25_bare_pr < dpo_bare_pr,
        "l25lin_vs_dpo_bare": round(l25_bare_pr - dpo_bare_pr, 4),
    }

    # Convert numpy types for JSON serialization
    def convert(obj):
        if isinstance(obj, (np.bool_, np.integer)):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    outpath = "cna_dpo_relay_scatter.json"
    with open(outpath, "w") as f:
        json.dump(results, f, indent=2, default=convert)
    print(f"\nSaved: {outpath}")
    print(f"\nSummary:")
    for k, v in summary.items():
        print(f"  {k:20s}: relay_PR={v['relay_mean_pr']:.2f} ± {v['relay_std_pr']:.2f}  L9={v['l9_pr']:.2f}  L25={v['l25_pr']:.2f}")
    print(f"\nHypothesis test: {json.dumps(results['hypothesis_test'], indent=2)}")


if __name__ == "__main__":
    run()
