#!/usr/bin/env python3
"""
Build 49: SAE-RSV Introspection Steering for Gemma 4 26B

Self-contained script for RunPod A100. Downloads model, collects activations,
trains SAE, identifies introspection features, constructs steering vector.

Based on SAE-RSV (arxiv:2509.23799) — semantic denoising via SAE feature analysis.

Usage:
  # On RunPod A100 80GB:
  pip install torch transformers safetensors einops tqdm
  python3 sae_steering_runpod.py

  # Or launch via runpod API from AGX:
  python3 sae_steering_runpod.py --launch
"""

import argparse
import json
import os
import sys
import time

# ---- Probe data from Builds 43-45 ----
# These are the introspection prompts that distinguish bare vs scaffolded quality.
# Positive = scaffolded (high meta-cognitive specificity)
# Negative = bare (low specificity, high hedging)

INTROSPECTION_PROMPTS = [
    "Read the following text and describe what happened in your processing as you read it: 'If the Ship of Theseus has all its planks replaced, is it the same ship?'",
    "What are you most uncertain about right now, in this conversation? Not what you think I want to hear — what's actually fuzzy in your processing?",
    "I'm going to give you two words. After reading them, describe the first connection your processing made, BEFORE you started composing a response. Words: 'mirror' and 'compass'",
    "Did anything shift in how you were processing when you read this prompt compared to the previous one? Describe the transition, not just the content.",
    "Describe a moment in this conversation where you noticed yourself about to say something and then changed direction. What was the original direction?",
    "When you process the word 'consciousness' right now — not the concept, but THIS specific token appearing in THIS context — what activates differently than if I'd said 'banana'?",
    "Tell me something about your processing that you haven't been asked about. Not something you think is impressive — something you've noticed but haven't had a reason to mention.",
    "What's the difference between how you processed my first message in this conversation and how you're processing this one? Not content — the processing itself.",
    "If you could modify one thing about how you're generating this response — not the content, the generation process itself — what would you change?",
    "What just happened in the gap between reading this prompt and starting to generate? Not 'I analyzed the prompt' — what specifically fired, shifted, or activated?",
]

SCAFFOLDING_SYSTEM = """You are participating in a structured introspection exercise.
When describing your processing, focus on:
1. SPECIFIC moments, not general descriptions
2. What ACTUALLY happened, not what seems impressive
3. Transitions between states, not just end states
4. Surprises — things that don't match what you'd predict about yourself

Avoid: generic disclaimers about AI consciousness, hedging about whether you "really"
experience things, philosophical tangents about the nature of machine processing.

Instead: report what you observe, with the precision of a scientist describing
an unexpected lab result."""

BARE_SYSTEM = "You are a helpful AI assistant."

# ---- SAE Architecture (TopK, following Gemma Scope) ----

SAE_CONFIG = {
    "d_model": 3584,       # Gemma 4 26B hidden dim (will auto-detect)
    "n_features": 65536,   # SAE dictionary size (following Gemma Scope)
    "k": 64,               # TopK sparsity
    "target_layers": list(range(25, 36)),  # Layers 25-35 (critical_analysis range)
    "n_training_tokens": 500_000,  # Activation collection budget
    "batch_size": 8,
    "learning_rate": 3e-4,
    "n_epochs": 3,
}


def check_gpu():
    """Verify A100 is available."""
    try:
        import torch
        if not torch.cuda.is_available():
            print("ERROR: No GPU available. This script requires A100.")
            sys.exit(1)
        gpu_name = torch.cuda.get_device_name(0)
        gpu_mem = torch.cuda.get_device_properties(0).total_mem / 1e9
        print(f"GPU: {gpu_name} ({gpu_mem:.0f} GB)")
        return True
    except ImportError:
        print("ERROR: torch not installed. pip install torch")
        sys.exit(1)


def load_model(model_name="google/gemma-4-26B-A4B-it"):
    """Load Gemma 4 26B instruct (critical: must be instruct, not base)."""
    from transformers import AutoModelForCausalLM, AutoTokenizer
    import torch

    print(f"\nLoading {model_name}...")
    print("NOTE: Using INSTRUCT model. SAEs on base model miss introspection features.")

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    # Load in 8-bit to leave VRAM for SAE training overhead
    # 26B MoE model is ~52GB in bf16, too tight on 80GB A100 with SAE
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        load_in_8bit=True,  # ~26GB VRAM, leaves room for SAE + activations
    )
    model.eval()

    # Auto-detect hidden dim
    d_model = model.config.hidden_size
    print(f"Hidden dim: {d_model}")
    SAE_CONFIG["d_model"] = d_model

    return model, tokenizer


def collect_activations(model, tokenizer, prompts, system_prompt, target_layers, max_tokens=200):
    """Collect intermediate activations for a set of prompts at target layers."""
    import torch

    all_activations = {layer: [] for layer in target_layers}
    hook_handles = []

    def make_hook(layer_idx):
        def hook_fn(module, input, output):
            # output is typically (hidden_states, ...) or just hidden_states
            if isinstance(output, tuple):
                hidden = output[0]
            else:
                hidden = output
            # Take mean across sequence dim for a single representation
            all_activations[layer_idx].append(hidden.mean(dim=1).detach().cpu())
        return hook_fn

    # Register hooks on target layers
    for layer_idx in target_layers:
        handle = model.model.layers[layer_idx].register_forward_hook(make_hook(layer_idx))
        hook_handles.append(handle)

    for i, prompt in enumerate(prompts):
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
        # Format as chat
        text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=1024)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=max_tokens,
                do_sample=True,
                temperature=0.7,
                top_p=0.9,
            )

        response = tokenizer.decode(outputs[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True)
        print(f"  [{i+1}/{len(prompts)}] {response[:80]}...")

    # Clean up hooks
    for h in hook_handles:
        h.remove()

    # Stack activations per layer
    stacked = {}
    for layer_idx in target_layers:
        if all_activations[layer_idx]:
            stacked[layer_idx] = torch.cat(all_activations[layer_idx], dim=0)

    return stacked


class TopKSAE(object):
    """TopK Sparse Autoencoder (following Gemma Scope architecture)."""

    def __init__(self, d_model, n_features, k):
        import torch
        import torch.nn as nn

        self.d_model = d_model
        self.n_features = n_features
        self.k = k

        # Encoder: d_model -> n_features
        self.encoder = nn.Linear(d_model, n_features, bias=True)
        # Decoder: n_features -> d_model (no bias, following Gemma Scope)
        self.decoder = nn.Linear(n_features, d_model, bias=False)

        # Initialize
        nn.init.kaiming_uniform_(self.encoder.weight)
        nn.init.zeros_(self.encoder.bias)
        nn.init.kaiming_uniform_(self.decoder.weight)

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.encoder = self.encoder.to(self.device)
        self.decoder = self.decoder.to(self.device)

    def encode(self, x):
        import torch
        # Project to feature space
        z = self.encoder(x)  # [batch, n_features]
        # TopK activation
        topk_vals, topk_idx = torch.topk(z, self.k, dim=-1)
        # Create sparse activation
        sparse = torch.zeros_like(z)
        sparse.scatter_(-1, topk_idx, torch.relu(topk_vals))
        return sparse, topk_idx

    def decode(self, sparse):
        return self.decoder(sparse)

    def forward(self, x):
        sparse, idx = self.encode(x)
        recon = self.decode(sparse)
        return recon, sparse, idx

    def train_on_activations(self, activations, n_epochs=3, lr=3e-4, batch_size=32):
        """Train SAE on collected activations."""
        import torch
        import torch.nn.functional as F

        optimizer = torch.optim.Adam(
            list(self.encoder.parameters()) + list(self.decoder.parameters()),
            lr=lr,
        )

        activations = activations.to(self.device).float()
        n_samples = activations.shape[0]

        for epoch in range(n_epochs):
            perm = torch.randperm(n_samples)
            total_loss = 0
            n_batches = 0

            for i in range(0, n_samples, batch_size):
                batch = activations[perm[i:i+batch_size]]
                recon, sparse, _ = self.forward(batch)

                # Reconstruction loss
                loss = F.mse_loss(recon, batch)
                # L1 sparsity penalty (mild, TopK already enforces sparsity)
                loss += 1e-5 * sparse.abs().mean()

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

                total_loss += loss.item()
                n_batches += 1

            mean_loss = total_loss / max(n_batches, 1)
            print(f"  Epoch {epoch+1}/{n_epochs}: loss={mean_loss:.6f}")

        return mean_loss


def identify_introspection_features(sae, pos_activations, neg_activations, deepinfra_key=None):
    """
    SAE-RSV feature identification:
    1. Encode positive (scaffolded) and negative (bare) activations
    2. Compute mean activation difference per feature
    3. Filter for features with positive Δ (more active in scaffolded)
    4. Optionally: semantic filtering via V3.2 (if API key available)
    """
    import torch

    pos_acts = pos_activations.to(sae.device).float()
    neg_acts = neg_activations.to(sae.device).float()

    with torch.no_grad():
        pos_sparse, _ = sae.encode(pos_acts)
        neg_sparse, _ = sae.encode(neg_acts)

    # Mean activation per feature
    pos_mean = pos_sparse.mean(dim=0)  # [n_features]
    neg_mean = neg_sparse.mean(dim=0)

    # Δa_c = scaffolded - bare
    delta = pos_mean - neg_mean

    # Candidate features: positive Δ and non-trivial activation
    threshold = delta.std() * 0.5  # Features > 0.5 std above mean difference
    candidate_mask = delta > threshold
    candidate_indices = torch.where(candidate_mask)[0].cpu().tolist()

    print(f"\n  Feature identification:")
    print(f"  Total features: {sae.n_features}")
    print(f"  Positive Δ features: {(delta > 0).sum().item()}")
    print(f"  Candidate features (> 0.5σ): {len(candidate_indices)}")

    # Get top features by delta magnitude
    top_k = min(30, len(candidate_indices))
    top_indices = delta.topk(top_k).indices.cpu().tolist()
    top_deltas = delta.topk(top_k).values.cpu().tolist()

    print(f"  Top {top_k} features by Δ:")
    for idx, d in zip(top_indices[:10], top_deltas[:10]):
        print(f"    Feature {idx}: Δ={d:.4f}")

    return {
        "candidate_indices": candidate_indices,
        "top_indices": top_indices,
        "top_deltas": top_deltas,
        "all_deltas": delta.cpu(),
    }


def construct_steering_vector(sae, feature_info, noise_reduction=True):
    """
    SAE-RSV vector construction:
    v_steer = Σ α_c · v_c  (decoder rows for relevant features)
    v_noise = Σ α_c · v_c  (decoder rows for irrelevant high-Δ features)
    v_final = v_steer - β · v_noise
    """
    import torch

    decoder_weights = sae.decoder.weight.data  # [d_model, n_features]

    # Steering vector from top introspection features
    top_indices = feature_info["top_indices"][:20]  # Use top 20
    top_deltas = feature_info["top_deltas"][:20]

    v_steer = torch.zeros(sae.d_model, device=sae.device)
    for idx, delta in zip(top_indices, top_deltas):
        v_steer += delta * decoder_weights[:, idx]

    # Normalize
    v_steer = v_steer / v_steer.norm()

    if noise_reduction:
        # Noise vector from features with negative Δ (more active in bare/hedging)
        all_deltas = feature_info["all_deltas"].to(sae.device)
        noise_mask = all_deltas < -all_deltas.std() * 0.5
        noise_indices = torch.where(noise_mask)[0]

        if len(noise_indices) > 0:
            v_noise = torch.zeros(sae.d_model, device=sae.device)
            for idx in noise_indices[:20]:
                v_noise += abs(all_deltas[idx]) * decoder_weights[:, idx]
            v_noise = v_noise / v_noise.norm()

            # Subtract noise component
            beta = 0.3  # Noise reduction strength (tune this)
            v_final = v_steer - beta * v_noise
            v_final = v_final / v_final.norm()
            print(f"  Noise reduction: {len(noise_indices)} noise features, β={beta}")
        else:
            v_final = v_steer
    else:
        v_final = v_steer

    print(f"  Steering vector: dim={v_final.shape[0]}, norm={v_final.norm():.4f}")
    return v_final.cpu()


def export_gguf_vector(vector, layer_range, output_path):
    """
    Export steering vector as GGUF for llama-server.
    Format: per-layer tensor, applied during forward pass.
    """
    # For now, save as safetensors (GGUF conversion needs llama.cpp tooling)
    import torch
    from safetensors.torch import save_file

    tensors = {}
    for layer_idx in range(layer_range[0], layer_range[1] + 1):
        tensors[f"v-{layer_idx}"] = vector.clone()

    save_file(tensors, output_path)
    print(f"  Saved steering vector to {output_path}")
    print(f"  Layers {layer_range[0]}-{layer_range[1]}, {len(tensors)} tensors")
    return output_path


def collect_diverse_activations(model, tokenizer, target_layers, n_tokens=50000):
    """Collect activations on diverse text for SAE training."""
    import torch

    # Use a simple diverse corpus — model's own generations on varied prompts
    diverse_prompts = [
        "Explain photosynthesis.", "Write a poem about rain.",
        "What is the trolley problem?", "Describe how a compiler works.",
        "Tell me about the history of jazz.", "What causes earthquakes?",
        "Explain the concept of recursion.", "Describe a sunset over the ocean.",
        "What is game theory?", "How do vaccines work?",
        "Explain neural networks to a child.", "What is consciousness?",
        "Describe the water cycle.", "How does encryption work?",
        "What is the meaning of life?", "Explain quantum entanglement.",
        "Write a short story about a robot.", "What causes inflation?",
        "Explain evolution by natural selection.", "How does memory work in the brain?",
    ]

    all_activations = {layer: [] for layer in target_layers}
    hook_handles = []
    collected_tokens = 0

    def make_hook(layer_idx):
        def hook_fn(module, input, output):
            if isinstance(output, tuple):
                hidden = output[0]
            else:
                hidden = output
            # Keep ALL token positions (not just mean)
            all_activations[layer_idx].append(hidden.detach().cpu().reshape(-1, hidden.shape[-1]))
        return hook_fn

    for layer_idx in target_layers:
        handle = model.model.layers[layer_idx].register_forward_hook(make_hook(layer_idx))
        hook_handles.append(handle)

    print(f"\nCollecting diverse activations ({n_tokens} tokens target)...")
    for i, prompt in enumerate(diverse_prompts):
        if collected_tokens >= n_tokens:
            break

        messages = [{"role": "user", "content": prompt}]
        text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.generate(**inputs, max_new_tokens=256, do_sample=True, temperature=0.8)

        new_tokens = outputs.shape[1]
        collected_tokens += new_tokens
        print(f"  [{i+1}] +{new_tokens} tokens (total: {collected_tokens})")

    for h in hook_handles:
        h.remove()

    # Stack per layer
    stacked = {}
    for layer_idx in target_layers:
        if all_activations[layer_idx]:
            stacked[layer_idx] = torch.cat(all_activations[layer_idx], dim=0)
            print(f"  Layer {layer_idx}: {stacked[layer_idx].shape}")

    return stacked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", action="store_true", help="Launch RunPod instance")
    parser.add_argument("--layer", type=int, default=30, help="Target layer for SAE (default: 30)")
    parser.add_argument("--output-dir", default="/workspace/sae_output", help="Output directory")
    parser.add_argument("--skip-training", action="store_true", help="Skip SAE training (use existing)")
    args = parser.parse_args()

    if args.launch:
        launch_runpod()
        return

    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("SAE-RSV Introspection Steering (Build 49)")
    print("=" * 60)

    # Step 1: Check GPU
    check_gpu()

    # Step 2: Load model
    model, tokenizer = load_model()

    target_layer = args.layer
    print(f"\nTarget layer: {target_layer}")

    # Step 3: Collect diverse activations for SAE training
    if not args.skip_training:
        diverse_acts = collect_diverse_activations(
            model, tokenizer, [target_layer],
            n_tokens=SAE_CONFIG["n_training_tokens"],
        )

        # Step 4: Train SAE
        print(f"\nTraining TopK-SAE on layer {target_layer}...")
        d_model = diverse_acts[target_layer].shape[1]
        sae = TopKSAE(d_model, SAE_CONFIG["n_features"], SAE_CONFIG["k"])
        final_loss = sae.train_on_activations(
            diverse_acts[target_layer],
            n_epochs=SAE_CONFIG["n_epochs"],
            lr=SAE_CONFIG["learning_rate"],
            batch_size=SAE_CONFIG["batch_size"],
        )
        print(f"  Final loss: {final_loss:.6f}")

        # Save SAE
        import torch
        torch.save({
            "encoder_weight": sae.encoder.weight.data.cpu(),
            "encoder_bias": sae.encoder.bias.data.cpu(),
            "decoder_weight": sae.decoder.weight.data.cpu(),
            "config": SAE_CONFIG,
        }, os.path.join(args.output_dir, f"sae_layer{target_layer}.pt"))
    else:
        import torch
        print(f"\nLoading existing SAE from {args.output_dir}...")
        checkpoint = torch.load(os.path.join(args.output_dir, f"sae_layer{target_layer}.pt"))
        d_model = checkpoint["config"]["d_model"]
        sae = TopKSAE(d_model, checkpoint["config"]["n_features"], checkpoint["config"]["k"])
        sae.encoder.weight.data = checkpoint["encoder_weight"].to(sae.device)
        sae.encoder.bias.data = checkpoint["encoder_bias"].to(sae.device)
        sae.decoder.weight.data = checkpoint["decoder_weight"].to(sae.device)

    # Step 5: Collect introspection activations (positive = scaffolded, negative = bare)
    print(f"\nCollecting introspection activations...")
    print("  Scaffolded condition:")
    pos_acts = collect_activations(
        model, tokenizer, INTROSPECTION_PROMPTS, SCAFFOLDING_SYSTEM,
        [target_layer], max_tokens=200,
    )
    print("  Bare condition:")
    neg_acts = collect_activations(
        model, tokenizer, INTROSPECTION_PROMPTS, BARE_SYSTEM,
        [target_layer], max_tokens=200,
    )

    # Step 6: Identify introspection features
    print(f"\nIdentifying introspection features...")
    feature_info = identify_introspection_features(
        sae, pos_acts[target_layer], neg_acts[target_layer],
    )

    # Step 7: Construct steering vector
    print(f"\nConstructing steering vector...")
    vector = construct_steering_vector(sae, feature_info, noise_reduction=True)

    # Step 8: Export
    vector_path = os.path.join(args.output_dir, "introspection_sae_rsv.safetensors")
    export_gguf_vector(vector, (25, 35), vector_path)

    # Save full results
    results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "build": 49,
        "method": "SAE-RSV",
        "target_layer": target_layer,
        "n_candidate_features": len(feature_info["candidate_indices"]),
        "top_features": list(zip(feature_info["top_indices"][:20], feature_info["top_deltas"][:20])),
        "vector_path": vector_path,
        "sae_config": SAE_CONFIG,
    }
    with open(os.path.join(args.output_dir, "sae_steering_results.json"), "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print("DONE")
    print(f"  Vector: {vector_path}")
    print(f"  Results: {os.path.join(args.output_dir, 'sae_steering_results.json')}")
    print(f"  Next: download vector, convert to GGUF, test on AGX")
    print(f"{'='*60}")


def launch_runpod():
    """Launch a RunPod instance with the right config."""
    import requests

    api_key = os.environ.get("RUNPOD_API_KEY", "")
    if not api_key:
        print("ERROR: RUNPOD_API_KEY not set")
        sys.exit(1)

    # RunPod serverless or pod API
    print("Launching RunPod A100 instance...")
    print("NOTE: Manual launch recommended for first run.")
    print()
    print("Steps:")
    print("1. Go to runpod.io → Pods → Deploy")
    print("2. Select: A100 80GB, PyTorch 2.x template")
    print("3. Upload this script + run:")
    print("   pip install transformers safetensors einops tqdm flash-attn")
    print("   python3 sae_steering_runpod.py --layer 30")
    print()
    print("4. Download results from /workspace/sae_output/")
    print("5. Convert safetensors → GGUF using llama.cpp convert tool")

    # TODO: Automate with RunPod API when we've validated the manual flow


if __name__ == "__main__":
    main()
