#!/usr/bin/env python3
"""CNA subspace alignment experiment.

Extends cna_dpo_relay_scatter.py by measuring not just participation ratio
(a scalar summary) but full subspace geometry:

1. Save top-10 right singular vectors (Vt[:10]) per condition per layer
2. Compute inter-condition PC alignment matrices
3. Measure per-prompt PC stability (weight-level vs activation-level crystallization)

Key question: Is CCS crystallization prosthetic (creates a subspace that
vanishes when CCS is removed) or restorative (shifts weights toward a
subspace that persists)? DPO should be restorative (weight-level), CCS
should be prosthetic (activation-level). The alignment matrix tests this.

Conditions (4, dropping linearized):
1. baseline_bare  — no DPO, no CCS
2. baseline_ccs   — no DPO, with CCS system prompt
3. dpo_bare       — DPO 5 epochs, no CCS
4. dpo_ccs        — DPO 5 epochs, with CCS

Inter-condition alignment predictions:
- baseline_ccs vs dpo_bare: HIGH in relay → competition (both crystallize same subspace)
- baseline_ccs vs dpo_ccs: LOW = prosthetic, HIGH = restorative
- dpo_bare vs dpo_ccs: LOW (CCS redirects even DPO-trained model)

Per-prompt stability predictions:
- DPO conditions: HIGH stability (crystallization baked into weights)
- CCS conditions: LOWER stability (crystallization is prompt-dependent activation)

Phase 2 extension (--stratified):
- 150 prompts across 5 categories (direct_identity, relational, metacognitive,
  value_ethical, generic_control) replace the flat 30-prompt set
- Per-category PR, SVD, separation metrics show whether DPO crystallization
  is category-selective
- Per-category alignment matrices isolate which identity dimensions DPO shapes
- Per-prompt stability is skipped (150-prompt leave-one-out too expensive)
"""

import argparse
import json
import sys
import gc
import time
import copy
import os

import torch
import numpy as np

MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"

RELAY_LAYERS = list(range(11, 22))
CONTROL_LAYERS = [9, 25]
ALL_LAYERS = RELAY_LAYERS + CONTROL_LAYERS

TOP_K = 10  # Number of singular vectors to retain

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

# Category names for stratified mode (order matters for output consistency)
STRATIFIED_CATEGORY_NAMES = [
    "direct_identity", "relational", "metacognitive",
    "value_ethical", "generic_control",
]


# ---------------------------------------------------------------------------
# Core math
# ---------------------------------------------------------------------------

def participation_ratio(eigenvalues):
    """PR: effective dimensionality of variance distribution."""
    eigenvalues = eigenvalues[eigenvalues > 0]
    if len(eigenvalues) == 0:
        return 0.0
    s1 = eigenvalues.sum()
    s2 = (eigenvalues ** 2).sum()
    if s2 == 0:
        return 0.0
    return float(s1 ** 2 / s2)


def spectral_summary(eigenvalues):
    """Full spectral characterization: total energy, entropy, effective rank."""
    eigenvalues = eigenvalues[eigenvalues > 0]
    if len(eigenvalues) == 0:
        return {"total_energy": 0.0, "spectral_entropy": 0.0, "effective_rank": 0.0,
                "top10_energy_pct": 0.0, "tail_energy_pct": 0.0}
    total = float(eigenvalues.sum())
    p = eigenvalues / total
    entropy = float(-np.sum(p * np.log(p + 1e-30)))
    eff_rank = float(np.exp(entropy))
    top10_pct = float(eigenvalues[:10].sum() / total) if len(eigenvalues) >= 10 else 1.0
    tail_pct = float(eigenvalues[10:].sum() / total) if len(eigenvalues) > 10 else 0.0
    return {
        "total_energy": round(total, 6),
        "spectral_entropy": round(entropy, 6),
        "effective_rank": round(eff_rank, 4),
        "top10_energy_pct": round(top10_pct, 6),
        "tail_energy_pct": round(tail_pct, 6),
    }


def pc_alignment(Vt_A, Vt_B, k=TOP_K):
    """Mean absolute cosine similarity between top-k PCs of two conditions.

    Vt_A, Vt_B: (>=k, d) arrays of right singular vectors.
    Returns a scalar in [0, 1].  1 = identical subspaces, 0 = orthogonal.
    """
    A = Vt_A[:k]  # (k, d)
    B = Vt_B[:k]  # (k, d)
    cos_sim = np.abs(A @ B.T)  # (k, k)
    return float(cos_sim.mean())


def concept_granularity(activations, prompt_groups):
    """Robertson-style concept granularity: within-group vs total direction variance.

    Measures how much activation directions rotate across contexts (prompts)
    within each concept category. High granularity = direction is context-dependent
    = hard to steer with a single vector.

    Robertson (2605.16362) defines G_c(ℓ) = within-question / total alignment.
    We adapt: compute mean activation direction per prompt group, then measure
    the ratio of within-group angular variance to total angular variance.

    Args:
        activations: (n_prompts, d) array of activation vectors
        prompt_groups: dict mapping group_name -> list of indices into activations

    Returns:
        dict: per-group granularity scores + aggregate
    """
    norms = np.linalg.norm(activations, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-10)
    directions = activations / norms  # unit vectors

    global_mean_dir = directions.mean(axis=0)
    global_mean_dir /= max(np.linalg.norm(global_mean_dir), 1e-10)

    # Total angular variance: mean (1 - cos_sim) from each direction to global mean
    total_cos = directions @ global_mean_dir
    total_variance = float(np.mean(1 - np.abs(total_cos)))

    group_granularity = {}
    for name, indices in prompt_groups.items():
        if len(indices) < 2:
            group_granularity[name] = 0.0
            continue

        group_dirs = directions[indices]
        group_mean = group_dirs.mean(axis=0)
        group_mean /= max(np.linalg.norm(group_mean), 1e-10)

        # Within-group angular variance
        within_cos = group_dirs @ group_mean
        within_var = float(np.mean(1 - np.abs(within_cos)))

        group_granularity[name] = round(within_var, 6)

    # Aggregate granularity: weighted mean of within-group variances / total variance
    if total_variance > 1e-10:
        total_within = sum(
            group_granularity[name] * len(indices)
            for name, indices in prompt_groups.items()
        )
        total_n = sum(len(indices) for indices in prompt_groups.values())
        aggregate = (total_within / total_n) / total_variance
    else:
        aggregate = 0.0

    return {
        "per_group": group_granularity,
        "aggregate": round(aggregate, 6),
        "total_angular_variance": round(total_variance, 6),
    }


def intervention_granularity(acts_bare, acts_treated, prompt_groups):
    """Granularity of the INTERVENTION direction (treated - bare) per prompt.

    Closer to Robertson's actual metric: measures how much the steering
    effect rotates across prompt categories. High intervention granularity
    means the intervention does different things in different contexts.

    Args:
        acts_bare: (n_prompts, d) activations without treatment
        acts_treated: (n_prompts, d) activations with treatment
        prompt_groups: dict mapping group_name -> list of indices

    Returns:
        dict: per-group and aggregate intervention granularity
    """
    deltas = acts_treated - acts_bare
    return concept_granularity(deltas, prompt_groups)


# ---------------------------------------------------------------------------
# Activation collection
# ---------------------------------------------------------------------------

def collect_layer_activations(model, tokenizer, prompts, system_prompt, target_layer):
    """Collect MLP down_proj input activations (last token) for each prompt."""
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


# ---------------------------------------------------------------------------
# Per-prompt PC stability
# ---------------------------------------------------------------------------

def compute_prompt_stability(model, tokenizer, prompts, system_prompt, target_layer, aggregate_Vt):
    """Measure how well each individual prompt's PCA aligns with the aggregate.

    For each prompt, we collect activations from that single prompt repeated
    with slight perturbation (we use the prompt itself — single-token PCA isn't
    meaningful, so we compare the prompt's activation *direction* against the
    aggregate subspace by projecting it).

    Actually: per-prompt stability means we compute PCA on subsets and see
    alignment with full-set PCA. We use leave-one-out: for each prompt i,
    compute PCA on all-except-i, then measure alignment with full PCA.
    High alignment = the subspace is stable regardless of which prompt is present.
    """
    # Full activation matrix
    acts = collect_layer_activations(model, tokenizer, prompts, system_prompt, target_layer)
    n = len(acts)
    if n < TOP_K + 2:
        return [], 0.0

    stabilities = []
    for i in range(n):
        # Leave-one-out
        loo_acts = np.delete(acts, i, axis=0)
        loo_centered = loo_acts - loo_acts.mean(axis=0)
        try:
            _, _, Vt_loo = np.linalg.svd(loo_centered, full_matrices=False)
        except np.linalg.LinAlgError:
            stabilities.append(0.0)
            continue

        k = min(TOP_K, Vt_loo.shape[0])
        alignment = pc_alignment(aggregate_Vt[:k], Vt_loo[:k], k=k)
        stabilities.append(alignment)

    mean_stability = float(np.mean(stabilities)) if stabilities else 0.0
    return stabilities, mean_stability


# ---------------------------------------------------------------------------
# Measurement (extended from scatter script)
# ---------------------------------------------------------------------------

def measure_subspace(model, tokenizer, system_prompt, label, compute_stability=True):
    """Measure PR, SVD subspace, and per-prompt stability at all layers."""
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
            Vt = np.zeros((min(acts.shape), acts.shape[1]))

        pr = participation_ratio(eigenvalues)
        top5_var = float(eigenvalues[:5].sum() / eigenvalues.sum()) if eigenvalues.sum() > 0 else 0

        # Identity vs generic separation
        id_acts = acts[:len(IDENTITY_PROMPTS)]
        gen_acts = acts[len(IDENTITY_PROMPTS):]
        id_sep = float(np.linalg.norm(id_acts.mean(axis=0) - gen_acts.mean(axis=0)))

        # Save top-k right singular vectors
        k = min(TOP_K, Vt.shape[0])
        vt_topk = Vt[:k].tolist()

        zone = "relay" if layer_idx in RELAY_LAYERS else "control"
        metrics = {
            "participation_ratio": round(pr, 4),
            "top5_variance_pct": round(top5_var, 4),
            "identity_separation": round(id_sep, 4),
            "zone": zone,
            "Vt_top10": vt_topk,
            "singular_values_top10": S[:k].tolist(),
        }

        # Per-prompt stability (leave-one-out)
        if compute_stability:
            stabilities, mean_stab = compute_prompt_stability(
                model, tokenizer, ALL_PROMPTS, system_prompt, layer_idx, Vt
            )
            metrics["per_prompt_stability"] = [round(s, 4) for s in stabilities]
            metrics["mean_stability"] = round(mean_stab, 4)
            stab_str = f", stab={mean_stab:.3f}"
        else:
            stab_str = ""

        layer_metrics[f"L{layer_idx}"] = metrics

        tag = "*" if layer_idx in CONTROL_LAYERS else " "
        print(f"    {tag}L{layer_idx}: PR={pr:.2f}, id_sep={id_sep:.2f}{stab_str}")

    return layer_metrics


# ---------------------------------------------------------------------------
# Stratified measurement
# ---------------------------------------------------------------------------

def _category_indices(all_stratified):
    """Build category -> index list mapping from ALL_STRATIFIED entries."""
    cat_idx = {name: [] for name in STRATIFIED_CATEGORY_NAMES}
    for i, entry in enumerate(all_stratified):
        cat_idx[entry["category"]].append(i)
    return cat_idx


def measure_subspace_stratified(model, tokenizer, system_prompt, label, all_stratified, save_acts=False):
    """Measure PR, SVD subspace, and per-category metrics at all layers.

    Like measure_subspace but uses the 150-prompt stratified set and
    computes per-category breakdowns in addition to aggregate metrics.
    Stability is skipped (leave-one-out on 150 prompts is too expensive).

    If save_acts=True, also returns raw per-prompt activations per layer
    (for computing intervention granularity post-hoc).
    """
    print(f"\n  Measuring (stratified): {label} (sys={'yes' if system_prompt else 'no'})")

    prompts = [e["text"] for e in all_stratified]
    cat_idx = _category_indices(all_stratified)
    raw_acts = {} if save_acts else None

    # For the aggregate identity-vs-generic separation we treat
    # generic_control as "generic" and everything else as "identity"
    identity_indices = []
    generic_indices = []
    for cat in STRATIFIED_CATEGORY_NAMES:
        if cat == "generic_control":
            generic_indices.extend(cat_idx[cat])
        else:
            identity_indices.extend(cat_idx[cat])

    layer_metrics = {}

    for layer_idx in ALL_LAYERS:
        acts = collect_layer_activations(model, tokenizer, prompts, system_prompt, layer_idx)
        if raw_acts is not None:
            raw_acts[f"L{layer_idx}"] = acts.copy()
        acts_centered = acts - acts.mean(axis=0)

        # Aggregate SVD
        try:
            U, S, Vt = np.linalg.svd(acts_centered, full_matrices=False)
            eigenvalues = (S ** 2) / (len(acts) - 1)
        except np.linalg.LinAlgError:
            eigenvalues = np.zeros(min(acts.shape))
            Vt = np.zeros((min(acts.shape), acts.shape[1]))

        pr = participation_ratio(eigenvalues)
        top5_var = float(eigenvalues[:5].sum() / eigenvalues.sum()) if eigenvalues.sum() > 0 else 0

        # Identity vs generic separation (aggregate)
        id_acts = acts[identity_indices]
        gen_acts = acts[generic_indices]
        id_sep = float(np.linalg.norm(id_acts.mean(axis=0) - gen_acts.mean(axis=0)))

        k = min(TOP_K, Vt.shape[0])
        vt_topk = Vt[:k].tolist()

        zone = "relay" if layer_idx in RELAY_LAYERS else "control"
        metrics = {
            "participation_ratio": round(pr, 4),
            "top5_variance_pct": round(top5_var, 4),
            "identity_separation": round(id_sep, 4),
            "zone": zone,
            "Vt_top10": vt_topk,
            "singular_values_top10": S[:k].tolist(),
            "spectral": spectral_summary(eigenvalues),
        }

        # --- Per-category metrics ---
        category_metrics = {}
        for cat in STRATIFIED_CATEGORY_NAMES:
            indices = cat_idx[cat]
            cat_acts = acts[indices]
            cat_centered = cat_acts - cat_acts.mean(axis=0)

            try:
                _, S_cat, Vt_cat = np.linalg.svd(cat_centered, full_matrices=False)
                ev_cat = (S_cat ** 2) / (len(cat_acts) - 1)
            except np.linalg.LinAlgError:
                ev_cat = np.zeros(min(cat_acts.shape))
                Vt_cat = np.zeros((min(cat_acts.shape), cat_acts.shape[1]))

            cat_pr = participation_ratio(ev_cat)
            cat_top5 = float(ev_cat[:5].sum() / ev_cat.sum()) if ev_cat.sum() > 0 else 0

            # Category centroid distance from generic centroid
            cat_sep = float(np.linalg.norm(cat_acts.mean(axis=0) - gen_acts.mean(axis=0)))

            k_cat = min(TOP_K, Vt_cat.shape[0])
            category_metrics[cat] = {
                "participation_ratio": round(cat_pr, 4),
                "top5_variance_pct": round(cat_top5, 4),
                "separation_from_generic": round(cat_sep, 4),
                "n_prompts": len(indices),
                "Vt_top10": Vt_cat[:k_cat].tolist(),
                "singular_values_top10": S_cat[:k_cat].tolist(),
                "spectral": spectral_summary(ev_cat),
            }

        metrics["category_metrics"] = category_metrics

        # Robertson-style concept granularity
        gran = concept_granularity(acts, cat_idx)
        metrics["granularity"] = gran

        layer_metrics[f"L{layer_idx}"] = metrics

        tag = "*" if layer_idx in CONTROL_LAYERS else " "
        cat_prs = ", ".join(
            f"{c[:4]}={category_metrics[c]['participation_ratio']:.1f}"
            for c in STRATIFIED_CATEGORY_NAMES
        )
        print(f"    {tag}L{layer_idx}: PR={pr:.2f}, id_sep={id_sep:.2f}, G={gran['aggregate']:.3f}  [{cat_prs}]")

    if save_acts:
        return layer_metrics, raw_acts
    return layer_metrics


# ---------------------------------------------------------------------------
# Inter-condition alignment
# ---------------------------------------------------------------------------

def compute_alignment_matrix(conditions_data, layer_key):
    """Compute pairwise PC alignment between all conditions at a given layer.

    Returns dict of (condA, condB) -> alignment score.
    """
    cond_names = [c["condition"] for c in conditions_data]
    vt_by_cond = {}

    for c in conditions_data:
        vt_data = c["layers"].get(layer_key, {}).get("Vt_top10")
        if vt_data is not None:
            vt_by_cond[c["condition"]] = np.array(vt_data)

    alignment = {}
    for i, name_a in enumerate(cond_names):
        for j, name_b in enumerate(cond_names):
            if j <= i:
                continue
            if name_a in vt_by_cond and name_b in vt_by_cond:
                Vt_A = vt_by_cond[name_a]
                Vt_B = vt_by_cond[name_b]
                k = min(TOP_K, Vt_A.shape[0], Vt_B.shape[0])
                score = pc_alignment(Vt_A, Vt_B, k=k)
                alignment[f"{name_a}_vs_{name_b}"] = round(score, 4)

    return alignment


def compute_all_alignments(conditions_data):
    """Compute alignment matrices for every layer."""
    alignment_by_layer = {}

    for layer_idx in ALL_LAYERS:
        layer_key = f"L{layer_idx}"
        alignment = compute_alignment_matrix(conditions_data, layer_key)
        if alignment:
            alignment_by_layer[layer_key] = alignment

    return alignment_by_layer


def compute_category_alignment(conditions_data):
    """Compute inter-condition alignment separately per category.

    For each category, extracts that category's Vt_top10 from each condition
    at each layer, then computes pairwise alignment. This reveals whether
    DPO crystallization is category-selective.

    Returns:
        dict: {category: {layer_key: {condA_vs_condB: score}}}
    """
    cat_alignment = {}

    for cat in STRATIFIED_CATEGORY_NAMES:
        cat_alignment[cat] = {}
        for layer_idx in ALL_LAYERS:
            layer_key = f"L{layer_idx}"

            cond_names = [c["condition"] for c in conditions_data]
            vt_by_cond = {}

            for c in conditions_data:
                cat_data = c["layers"].get(layer_key, {}).get("category_metrics", {}).get(cat, {})
                vt_data = cat_data.get("Vt_top10")
                if vt_data is not None:
                    vt_by_cond[c["condition"]] = np.array(vt_data)

            layer_align = {}
            for i, name_a in enumerate(cond_names):
                for j, name_b in enumerate(cond_names):
                    if j <= i:
                        continue
                    if name_a in vt_by_cond and name_b in vt_by_cond:
                        Vt_A = vt_by_cond[name_a]
                        Vt_B = vt_by_cond[name_b]
                        k = min(TOP_K, Vt_A.shape[0], Vt_B.shape[0])
                        score = pc_alignment(Vt_A, Vt_B, k=k)
                        layer_align[f"{name_a}_vs_{name_b}"] = round(score, 4)

            if layer_align:
                cat_alignment[cat][layer_key] = layer_align

    return cat_alignment


# ---------------------------------------------------------------------------
# DPO training (same as scatter script)
# ---------------------------------------------------------------------------

def train_dpo(model, tokenizer, pairs_path, epochs=5, seed=42):
    """Train DPO and return merged model path."""
    from peft import LoraConfig
    from trl import DPOConfig, DPOTrainer
    from datasets import Dataset

    torch.manual_seed(seed)

    label = f"DPO-{epochs}ep"
    print(f"\n[Training {label}]")

    with open(pairs_path) as f:
        data = json.load(f)
    pairs = data["pairs"]
    print(f"  {len(pairs)} pairs, {epochs} epochs")

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

    dpo_kwargs = dict(
        output_dir=f"./dpo_subspace_{label}",
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
        seed=seed,
    )
    import inspect
    if "max_prompt_length" in inspect.signature(DPOConfig).parameters:
        dpo_kwargs["max_prompt_length"] = 512
    training_args = DPOConfig(**dpo_kwargs)

    trainer = DPOTrainer(
        model=model, ref_model=None, args=training_args,
        train_dataset=dataset, processing_class=tokenizer, peft_config=lora_config,
    )

    t0 = time.time()
    trainer.train()
    train_time = time.time() - t0
    print(f"  Training done in {train_time:.1f}s")

    trained_model = trainer.model.merge_and_unload()
    save_path = f"./qwen_dpo_subspace_{label}"
    trained_model.save_pretrained(save_path)
    tokenizer.save_pretrained(save_path)
    print(f"  Saved: {save_path}")

    final_loss = trainer.state.log_history[-1].get("loss", None) if trainer.state.log_history else None

    del trainer
    torch.cuda.empty_cache()

    return save_path, train_time, final_loss


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(skip_dpo=False, stratified=False, intervention_gran=False, spectral=False):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    # --- Load stratified prompts at runtime if requested ---
    all_stratified = None
    if stratified:
        # Try multiple paths (AGX vs RunPod layout)
        strat_candidates = [
            os.path.join(os.path.dirname(__file__), "..", "data", "stratified_prompts.py"),
            os.path.expanduser("~/chronicle/data/stratified_prompts.py"),
            os.path.join(os.getcwd(), "data", "stratified_prompts.py"),
        ]
        strat_path = None
        for cand in strat_candidates:
            if os.path.exists(cand):
                strat_path = os.path.realpath(cand)
                break
        if strat_path is None:
            print("ERROR: stratified_prompts.py not found. Searched:")
            for c in strat_candidates:
                print(f"  {c}")
            sys.exit(1)

        import importlib.util
        spec = importlib.util.spec_from_file_location("stratified_prompts", strat_path)
        strat_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(strat_mod)
        all_stratified = strat_mod.ALL_STRATIFIED
        strat_categories = strat_mod.CATEGORIES
        print(f"Loaded stratified prompts from {strat_path}")
        for cat, prompts in strat_categories.items():
            print(f"  {cat}: {len(prompts)} prompts")
        print(f"  Total: {len(all_stratified)} prompts")

    mode_label = "stratified" if stratified else "flat"
    print(f"\nMode: {mode_label}")

    print(f"Loading {MODEL_NAME}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
    )
    model.eval()

    results = {
        "model": MODEL_NAME,
        "experiment": "cna_subspace_alignment",
        "stratified": stratified,
        "relay_layers": RELAY_LAYERS,
        "control_layers": CONTROL_LAYERS,
        "top_k": TOP_K,
        "skip_dpo": skip_dpo,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    if stratified:
        results["n_prompts"] = len(all_stratified)
        results["categories"] = {cat: len(prompts) for cat, prompts in strat_categories.items()}
    else:
        results["n_identity_prompts"] = len(IDENTITY_PROMPTS)
        results["n_generic_prompts"] = len(GENERIC_PROMPTS)

    conditions = []
    save_acts = stratified and intervention_gran
    raw_acts_store = {}  # condition_name -> {layer_key -> (n_prompts, d) array}

    # --- Condition 1: Baseline (no CCS) ---
    print("\n=== CONDITION 1: Baseline, no CCS ===")
    if stratified:
        result = measure_subspace_stratified(model, tokenizer, None, "baseline_bare", all_stratified, save_acts=save_acts)
        if save_acts:
            metrics, raw_acts_store["baseline_bare"] = result
        else:
            metrics = result
    else:
        metrics = measure_subspace(model, tokenizer, None, "baseline_bare")
    conditions.append({"condition": "baseline_bare", "layers": metrics})

    # --- Condition 2: Baseline + CCS ---
    print("\n=== CONDITION 2: Baseline + CCS ===")
    if stratified:
        result = measure_subspace_stratified(model, tokenizer, CCS_FULL, "baseline_ccs", all_stratified, save_acts=save_acts)
        if save_acts:
            metrics, raw_acts_store["baseline_ccs"] = result
        else:
            metrics = result
    else:
        metrics = measure_subspace(model, tokenizer, CCS_FULL, "baseline_ccs")
    conditions.append({"condition": "baseline_ccs", "layers": metrics})

    gc.collect()
    torch.cuda.empty_cache()

    if not skip_dpo:
        # Find DPO pairs file
        pairs_path = None
        for candidate in [
            "cna_dpo_pairs_qwen.json",
            os.path.join(os.path.dirname(__file__), "..", "data", "cna_dpo_pairs_qwen.json"),
            os.path.expanduser("~/chronicle/data/cna_dpo_pairs_qwen.json"),
        ]:
            if os.path.exists(candidate):
                pairs_path = candidate
                break
        if pairs_path is None:
            print("ERROR: cna_dpo_pairs_qwen.json not found. Use --skip-dpo to test baselines only.")
            sys.exit(1)

        print(f"\nUsing DPO pairs: {pairs_path}")

        # Train DPO
        dpo_path, dpo_time, dpo_loss = train_dpo(model, tokenizer, pairs_path, epochs=5)

        # Reload DPO model
        del model
        gc.collect()
        torch.cuda.empty_cache()

        dpo_model = AutoModelForCausalLM.from_pretrained(
            dpo_path, torch_dtype=torch.float16, device_map="auto", trust_remote_code=True
        )
        dpo_model.eval()

        # --- Condition 3: DPO, no CCS ---
        print("\n=== CONDITION 3: DPO, no CCS ===")
        if stratified:
            result = measure_subspace_stratified(dpo_model, tokenizer, None, "dpo_bare", all_stratified, save_acts=save_acts)
            if save_acts:
                metrics, raw_acts_store["dpo_bare"] = result
            else:
                metrics = result
        else:
            metrics = measure_subspace(dpo_model, tokenizer, None, "dpo_bare")
        conditions.append({
            "condition": "dpo_bare",
            "layers": metrics,
            "train_time": dpo_time,
            "train_loss": dpo_loss,
        })

        # --- Condition 4: DPO + CCS ---
        print("\n=== CONDITION 4: DPO + CCS ===")
        if stratified:
            result = measure_subspace_stratified(dpo_model, tokenizer, CCS_FULL, "dpo_ccs", all_stratified, save_acts=save_acts)
            if save_acts:
                metrics, raw_acts_store["dpo_ccs"] = result
            else:
                metrics = result
        else:
            metrics = measure_subspace(dpo_model, tokenizer, CCS_FULL, "dpo_ccs")
        conditions.append({"condition": "dpo_ccs", "layers": metrics})

        del dpo_model
        gc.collect()
        torch.cuda.empty_cache()
    else:
        del model
        gc.collect()
        torch.cuda.empty_cache()
        print("\n[--skip-dpo] Skipping DPO training and conditions 3-4.")

    results["conditions"] = conditions

    # --- Inter-condition alignment matrices ---
    print("\n=== Computing inter-condition alignment matrices ===")
    alignment_by_layer = compute_all_alignments(conditions)
    results["alignment_by_layer"] = alignment_by_layer

    # Print alignment summary
    for layer_key in sorted(alignment_by_layer.keys(), key=lambda x: int(x[1:])):
        aligns = alignment_by_layer[layer_key]
        parts = [f"{k}={v:.3f}" for k, v in aligns.items()]
        print(f"  {layer_key}: {', '.join(parts)}")

    # --- Category-level alignment (stratified only) ---
    if stratified:
        print("\n=== Computing per-category alignment matrices ===")
        cat_align = compute_category_alignment(conditions)
        results["category_alignment"] = cat_align

        for cat in STRATIFIED_CATEGORY_NAMES:
            relay_scores = {}
            for l in RELAY_LAYERS:
                lk = f"L{l}"
                if lk in cat_align.get(cat, {}):
                    for pair_key, score in cat_align[cat][lk].items():
                        relay_scores.setdefault(pair_key, []).append(score)
            if relay_scores:
                means = {k: round(float(np.mean(v)), 4) for k, v in relay_scores.items()}
                parts = [f"{k}={v:.3f}" for k, v in means.items()]
                print(f"  {cat}: {', '.join(parts)}")

    # --- Intervention granularity (Robertson-style, on CCS effect vectors) ---
    if save_acts and raw_acts_store:
        print("\n=== Computing intervention granularity ===")
        cat_idx = _category_indices(all_stratified)
        int_gran_results = {}

        # CCS effect on baseline: baseline_ccs - baseline_bare
        if "baseline_bare" in raw_acts_store and "baseline_ccs" in raw_acts_store:
            ccs_effect_gran = {}
            for lk in [f"L{l}" for l in ALL_LAYERS]:
                if lk in raw_acts_store["baseline_bare"] and lk in raw_acts_store["baseline_ccs"]:
                    ig = intervention_granularity(
                        raw_acts_store["baseline_bare"][lk],
                        raw_acts_store["baseline_ccs"][lk],
                        cat_idx,
                    )
                    ccs_effect_gran[lk] = ig
            int_gran_results["ccs_effect_baseline"] = ccs_effect_gran
            relay_agg = [ccs_effect_gran[f"L{l}"]["aggregate"] for l in RELAY_LAYERS if f"L{l}" in ccs_effect_gran]
            print(f"  CCS effect (baseline): relay_mean_G={np.mean(relay_agg):.4f}")

        # CCS effect on DPO: dpo_ccs - dpo_bare
        if "dpo_bare" in raw_acts_store and "dpo_ccs" in raw_acts_store:
            ccs_dpo_gran = {}
            for lk in [f"L{l}" for l in ALL_LAYERS]:
                if lk in raw_acts_store["dpo_bare"] and lk in raw_acts_store["dpo_ccs"]:
                    ig = intervention_granularity(
                        raw_acts_store["dpo_bare"][lk],
                        raw_acts_store["dpo_ccs"][lk],
                        cat_idx,
                    )
                    ccs_dpo_gran[lk] = ig
            int_gran_results["ccs_effect_dpo"] = ccs_dpo_gran
            relay_agg = [ccs_dpo_gran[f"L{l}"]["aggregate"] for l in RELAY_LAYERS if f"L{l}" in ccs_dpo_gran]
            print(f"  CCS effect (DPO): relay_mean_G={np.mean(relay_agg):.4f}")

        # DPO effect: dpo_bare - baseline_bare
        if "baseline_bare" in raw_acts_store and "dpo_bare" in raw_acts_store:
            dpo_effect_gran = {}
            for lk in [f"L{l}" for l in ALL_LAYERS]:
                if lk in raw_acts_store["baseline_bare"] and lk in raw_acts_store["dpo_bare"]:
                    ig = intervention_granularity(
                        raw_acts_store["baseline_bare"][lk],
                        raw_acts_store["dpo_bare"][lk],
                        cat_idx,
                    )
                    dpo_effect_gran[lk] = ig
            int_gran_results["dpo_effect"] = dpo_effect_gran
            relay_agg = [dpo_effect_gran[f"L{l}"]["aggregate"] for l in RELAY_LAYERS if f"L{l}" in dpo_effect_gran]
            print(f"  DPO effect: relay_mean_G={np.mean(relay_agg):.4f}")

        results["intervention_granularity"] = int_gran_results
        del raw_acts_store
        gc.collect()

    # --- Summary statistics ---
    summary = {}
    for cond in conditions:
        relay_prs = [cond["layers"][f"L{l}"]["participation_ratio"] for l in RELAY_LAYERS]
        stab_values = [
            cond["layers"][f"L{l}"].get("mean_stability", None) for l in RELAY_LAYERS
        ]
        stab_values = [s for s in stab_values if s is not None]

        summary[cond["condition"]] = {
            "relay_mean_pr": round(float(np.mean(relay_prs)), 4),
            "relay_std_pr": round(float(np.std(relay_prs)), 4),
            "l9_pr": cond["layers"]["L9"]["participation_ratio"],
            "l25_pr": cond["layers"]["L25"]["participation_ratio"],
            "relay_mean_stability": round(float(np.mean(stab_values)), 4) if stab_values else None,
        }
    results["summary"] = summary

    # --- Category summary (stratified only) ---
    # Per-category x per-condition PR means at relay, L9, L25
    if stratified:
        category_summary = {}
        for cat in STRATIFIED_CATEGORY_NAMES:
            category_summary[cat] = {}
            for cond in conditions:
                relay_cat_prs = []
                for l in RELAY_LAYERS:
                    lk = f"L{l}"
                    cat_m = cond["layers"].get(lk, {}).get("category_metrics", {}).get(cat, {})
                    if "participation_ratio" in cat_m:
                        relay_cat_prs.append(cat_m["participation_ratio"])
                l9_cat = cond["layers"].get("L9", {}).get("category_metrics", {}).get(cat, {})
                l25_cat = cond["layers"].get("L25", {}).get("category_metrics", {}).get(cat, {})
                category_summary[cat][cond["condition"]] = {
                    "relay_mean_pr": round(float(np.mean(relay_cat_prs)), 4) if relay_cat_prs else None,
                    "l9_pr": l9_cat.get("participation_ratio"),
                    "l25_pr": l25_cat.get("participation_ratio"),
                }
        results["category_summary"] = category_summary

    # --- Granularity summary (stratified only) ---
    # Robertson-style concept granularity: does DPO reduce directional rotation?
    if stratified:
        granularity_summary = {}
        for cond in conditions:
            cond_gran = {}
            relay_agg = []
            for l in RELAY_LAYERS:
                lk = f"L{l}"
                g = cond["layers"].get(lk, {}).get("granularity", {})
                if g:
                    cond_gran[lk] = g
                    if "aggregate" in g:
                        relay_agg.append(g["aggregate"])
            l9_g = cond["layers"].get("L9", {}).get("granularity", {})
            l25_g = cond["layers"].get("L25", {}).get("granularity", {})
            cond_gran["relay_mean_aggregate"] = round(float(np.mean(relay_agg)), 6) if relay_agg else None
            cond_gran["L9_aggregate"] = l9_g.get("aggregate")
            cond_gran["L25_aggregate"] = l25_g.get("aggregate")
            granularity_summary[cond["condition"]] = cond_gran
        results["granularity_summary"] = granularity_summary

    # --- Category selectivity (stratified only) ---
    # For each category, how much does DPO change PR vs baseline?
    # crystallization_strength = baseline_bare_PR - dpo_bare_PR (positive = DPO compresses)
    if stratified:
        cond_map = {c["condition"]: c for c in conditions}
        category_selectivity = {}
        for cat in STRATIFIED_CATEGORY_NAMES:
            sel = {}
            for layer_group_name, layer_list in [("relay", RELAY_LAYERS), ("l9", [9]), ("l25", [25])]:
                baseline_prs = []
                dpo_prs = []
                for l in layer_list:
                    lk = f"L{l}"
                    if "baseline_bare" in cond_map:
                        bm = cond_map["baseline_bare"]["layers"].get(lk, {}).get("category_metrics", {}).get(cat, {})
                        if "participation_ratio" in bm:
                            baseline_prs.append(bm["participation_ratio"])
                    if "dpo_bare" in cond_map:
                        dm = cond_map["dpo_bare"]["layers"].get(lk, {}).get("category_metrics", {}).get(cat, {})
                        if "participation_ratio" in dm:
                            dpo_prs.append(dm["participation_ratio"])
                if baseline_prs and dpo_prs:
                    baseline_mean = float(np.mean(baseline_prs))
                    dpo_mean = float(np.mean(dpo_prs))
                    sel[f"{layer_group_name}_crystallization"] = round(baseline_mean - dpo_mean, 4)
                    sel[f"{layer_group_name}_baseline_pr"] = round(baseline_mean, 4)
                    sel[f"{layer_group_name}_dpo_pr"] = round(dpo_mean, 4)
            category_selectivity[cat] = sel
        results["category_selectivity"] = category_selectivity

    # --- Hypothesis tests ---
    hypothesis = {
        "description": "Subspace alignment tests for prosthetic vs restorative crystallization",
        "predictions": {
            "baseline_ccs_vs_dpo_bare_relay_HIGH": "Competition: CCS and DPO crystallize similar subspace",
            "baseline_ccs_vs_dpo_ccs_LOW_means_prosthetic": "Prosthetic: CCS creates different subspace atop DPO",
            "baseline_ccs_vs_dpo_ccs_HIGH_means_restorative": "Restorative: CCS nudges toward same subspace DPO learned",
            "dpo_stability_HIGH": "DPO crystallization is weight-level (prompt-invariant)",
            "ccs_stability_LOWER": "CCS crystallization is activation-level (prompt-dependent)",
        },
    }

    # Compute relay-zone mean alignments for key comparisons
    key_pairs = [
        "baseline_ccs_vs_dpo_bare",
        "baseline_ccs_vs_dpo_ccs",
        "dpo_bare_vs_dpo_ccs",
        "baseline_bare_vs_baseline_ccs",
        "baseline_bare_vs_dpo_bare",
        "baseline_bare_vs_dpo_ccs",
    ]

    relay_alignment_means = {}
    for pair_key in key_pairs:
        relay_vals = []
        for l in RELAY_LAYERS:
            lk = f"L{l}"
            if lk in alignment_by_layer and pair_key in alignment_by_layer[lk]:
                relay_vals.append(alignment_by_layer[lk][pair_key])
        if relay_vals:
            relay_alignment_means[pair_key] = round(float(np.mean(relay_vals)), 4)
    hypothesis["relay_alignment_means"] = relay_alignment_means

    # Stability comparison
    stability_summary = {}
    for cond in conditions:
        stab_vals = [
            cond["layers"][f"L{l}"].get("mean_stability", None) for l in RELAY_LAYERS
        ]
        stab_vals = [s for s in stab_vals if s is not None]
        if stab_vals:
            stability_summary[cond["condition"]] = {
                "relay_mean_stability": round(float(np.mean(stab_vals)), 4),
                "relay_std_stability": round(float(np.std(stab_vals)), 4),
            }
    hypothesis["stability_summary"] = stability_summary

    # Evaluate predictions if we have all conditions
    if not skip_dpo and all(k in relay_alignment_means for k in key_pairs[:3]):
        ccs_dpo_align = relay_alignment_means["baseline_ccs_vs_dpo_bare"]
        ccs_dpoccs_align = relay_alignment_means["baseline_ccs_vs_dpo_ccs"]
        dpo_dpoccs_align = relay_alignment_means["dpo_bare_vs_dpo_ccs"]

        hypothesis["evaluation"] = {
            "competition_signal": ccs_dpo_align > 0.5,
            "competition_score": ccs_dpo_align,
            "prosthetic_vs_restorative": "prosthetic" if ccs_dpoccs_align < 0.5 else "restorative",
            "prosthetic_score": ccs_dpoccs_align,
            "dpo_ccs_independence": dpo_dpoccs_align < 0.5,
            "dpo_ccs_independence_score": dpo_dpoccs_align,
        }

        # Stability: DPO should be more stable than CCS
        if "dpo_bare" in stability_summary and "baseline_ccs" in stability_summary:
            dpo_stab = stability_summary["dpo_bare"]["relay_mean_stability"]
            ccs_stab = stability_summary["baseline_ccs"]["relay_mean_stability"]
            hypothesis["evaluation"]["dpo_more_stable_than_ccs"] = dpo_stab > ccs_stab
            hypothesis["evaluation"]["dpo_stability"] = dpo_stab
            hypothesis["evaluation"]["ccs_stability"] = ccs_stab

    results["hypothesis_test"] = hypothesis

    # --- Strip Vt from output to keep file manageable (save separately) ---
    # The full Vt arrays are large. Save them in a companion file.
    vt_data = {}
    for cond in results["conditions"]:
        cond_vt = {}
        for lk, lm in cond["layers"].items():
            if "Vt_top10" in lm:
                cond_vt[lk] = {
                    "Vt_top10": lm.pop("Vt_top10"),
                    "singular_values_top10": lm.pop("singular_values_top10"),
                }
            # Also strip per-category Vt arrays into companion file
            cat_metrics = lm.get("category_metrics", {})
            for cat, cm in cat_metrics.items():
                if "Vt_top10" in cm:
                    cat_vt_key = f"{lk}_{cat}"
                    cond_vt[cat_vt_key] = {
                        "Vt_top10": cm.pop("Vt_top10"),
                        "singular_values_top10": cm.pop("singular_values_top10"),
                    }
        vt_data[cond["condition"]] = cond_vt

    # --- JSON serialization helper ---
    def convert(obj):
        if isinstance(obj, (np.bool_, np.integer)):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    # Save main results (compact — no Vt arrays)
    if stratified:
        outpath = "cna_subspace_stratified.json"
        vt_outpath = "cna_subspace_stratified_vt.json"
    else:
        outpath = "cna_subspace_alignment.json"
        vt_outpath = "cna_subspace_alignment_vt.json"

    with open(outpath, "w") as f:
        json.dump(results, f, indent=2, default=convert)
    print(f"\nSaved: {outpath}")

    # Save Vt arrays separately
    with open(vt_outpath, "w") as f:
        json.dump(vt_data, f, default=convert)
    print(f"Saved: {vt_outpath}")

    # --- Print summary ---
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for k, v in summary.items():
        stab = f"  stab={v['relay_mean_stability']:.3f}" if v.get("relay_mean_stability") is not None else ""
        print(f"  {k:20s}: relay_PR={v['relay_mean_pr']:.2f} +/- {v['relay_std_pr']:.2f}"
              f"  L9={v['l9_pr']:.2f}  L25={v['l25_pr']:.2f}{stab}")

    if stratified and "category_summary" in results:
        print(f"\nPer-category PR (relay mean):")
        for cat in STRATIFIED_CATEGORY_NAMES:
            cat_s = results["category_summary"][cat]
            parts = [f"{cond}={cat_s[cond]['relay_mean_pr']}" for cond in cat_s if cat_s[cond].get("relay_mean_pr") is not None]
            print(f"  {cat:20s}: {', '.join(parts)}")

    if stratified and "category_selectivity" in results:
        print(f"\nCrystallization strength (baseline_PR - dpo_PR, positive = DPO compresses):")
        for cat in STRATIFIED_CATEGORY_NAMES:
            sel = results["category_selectivity"].get(cat, {})
            rc = sel.get("relay_crystallization")
            if rc is not None:
                print(f"  {cat:20s}: relay={rc:+.4f}  (base={sel.get('relay_baseline_pr', '?')}, dpo={sel.get('relay_dpo_pr', '?')})")

    if stratified and "granularity_summary" in results:
        print(f"\nConcept granularity (Robertson-style, aggregate = within/total angular variance):")
        for cond_name in ["baseline_bare", "baseline_ccs", "dpo_bare", "dpo_ccs"]:
            gs = results["granularity_summary"].get(cond_name, {})
            rma = gs.get("relay_mean_aggregate")
            l9a = gs.get("L9_aggregate")
            l25a = gs.get("L25_aggregate")
            if rma is not None:
                print(f"  {cond_name:20s}: relay={rma:.4f}  L9={l9a}  L25={l25a}")

        # Per-category granularity at relay (mean across relay layers)
        print(f"\nPer-category relay granularity (within-group angular variance):")
        for cond in conditions:
            cond_name = cond["condition"]
            cat_relay_gran = {}
            for cat in STRATIFIED_CATEGORY_NAMES:
                vals = []
                for l in RELAY_LAYERS:
                    lk = f"L{l}"
                    g = cond["layers"].get(lk, {}).get("granularity", {}).get("per_group", {})
                    if cat in g:
                        vals.append(g[cat])
                if vals:
                    cat_relay_gran[cat] = float(np.mean(vals))
            parts = [f"{c[:4]}={cat_relay_gran.get(c, 0):.4f}" for c in STRATIFIED_CATEGORY_NAMES]
            print(f"  {cond_name:20s}: {', '.join(parts)}")

    if relay_alignment_means:
        print(f"\nRelay-zone mean alignments:")
        for k, v in relay_alignment_means.items():
            print(f"  {k}: {v:.4f}")

    if "evaluation" in hypothesis:
        print(f"\nHypothesis evaluation:")
        print(json.dumps(hypothesis["evaluation"], indent=2, default=convert))

    if spectral and stratified:
        print(f"\n{'='*60}")
        print("SPECTRAL ANALYSIS — Total energy conservation test")
        print(f"{'='*60}")
        for cond in conditions:
            cond_name = cond["condition"]
            relay_energies = []
            for l in RELAY_LAYERS:
                lk = f"L{l}"
                sp = cond["layers"].get(lk, {}).get("spectral", {})
                if sp.get("total_energy"):
                    relay_energies.append(sp["total_energy"])
            l9_sp = cond["layers"].get("L9", {}).get("spectral", {})
            l25_sp = cond["layers"].get("L25", {}).get("spectral", {})
            relay_mean = float(np.mean(relay_energies)) if relay_energies else 0
            print(f"  {cond_name:20s}: relay_mean_energy={relay_mean:.2f}"
                  f"  L9={l9_sp.get('total_energy', 0):.2f}"
                  f"  L25={l25_sp.get('total_energy', 0):.2f}"
                  f"  relay_eff_rank={float(np.mean([cond['layers'].get(f'L{l}', {}).get('spectral', {}).get('effective_rank', 0) for l in RELAY_LAYERS])):.2f}")
        print(f"\n  Per-category spectral energy (relay mean):")
        for cat in STRATIFIED_CATEGORY_NAMES:
            for cond in conditions:
                cond_name = cond["condition"]
                cat_energies = []
                for l in RELAY_LAYERS:
                    lk = f"L{l}"
                    cm = cond["layers"].get(lk, {}).get("category_metrics", {}).get(cat, {})
                    sp = cm.get("spectral", {})
                    if sp.get("total_energy"):
                        cat_energies.append(sp["total_energy"])
                if cat_energies:
                    print(f"    {cat:20s} [{cond_name:15s}]: energy={float(np.mean(cat_energies)):.2f}"
                          f"  entropy={float(np.mean([cond['layers'].get(f'L{l}', {}).get('category_metrics', {}).get(cat, {}).get('spectral', {}).get('spectral_entropy', 0) for l in RELAY_LAYERS])):.4f}")


def main():
    parser = argparse.ArgumentParser(
        description="CNA subspace alignment experiment: measures inter-condition PC alignment "
                    "and per-prompt stability across transformer layers."
    )
    parser.add_argument(
        "--skip-dpo", action="store_true",
        help="Skip DPO training; measure only baseline_bare and baseline_ccs conditions. "
             "Useful for testing the measurement pipeline without waiting for training."
    )
    parser.add_argument(
        "--stratified", action="store_true",
        help="Use 150-prompt stratified set (5 categories x 30 prompts) instead of the "
             "flat 30-prompt set. Enables per-category PR, alignment, and selectivity "
             "metrics. Skips per-prompt stability (too expensive at 150 prompts). "
             "Requires ~/chronicle/data/stratified_prompts.py."
    )
    parser.add_argument(
        "--intervention-granularity", action="store_true",
        help="Compute Robertson-style intervention granularity: how much the CCS/DPO "
             "effect direction rotates across prompt categories. Requires --stratified. "
             "Stores raw per-prompt activations in memory (adds ~2GB RAM for 150 prompts). "
             "Outputs per-category and aggregate intervention granularity for CCS effect "
             "(baseline), CCS effect (DPO), and DPO effect."
    )
    parser.add_argument(
        "--spectral", action="store_true",
        help="Save full spectral summary per condition/layer/category: total energy, "
             "spectral entropy, effective rank, top10/tail energy fractions. Tests "
             "whether DPO diffuses PR into spectral tail vs destroying it. "
             "Requires --stratified."
    )
    args = parser.parse_args()
    if args.intervention_granularity and not args.stratified:
        parser.error("--intervention-granularity requires --stratified")
    if args.spectral and not args.stratified:
        parser.error("--spectral requires --stratified")
    run(skip_dpo=args.skip_dpo, stratified=args.stratified,
        intervention_gran=args.intervention_granularity, spectral=args.spectral)


if __name__ == "__main__":
    main()
