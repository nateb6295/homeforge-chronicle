# SAE-Based Introspection Steering for Gemma 4 26B

## Status: Design sketch. Requires RunPod A100.

## Background

Build 42 attempted crude activation steering via control vectors (critical_analysis.gguf).
Anti-refusal vector produced degenerate output at all tested scales.
Builds 43-45 showed scaffolding (prompt-level) is the current best lever.

SAE-RSV (arxiv:2509.23799, ICLR 2026) provides a principled alternative:
decompose representations via SAE, identify task-relevant features semantically,
construct a denoised steering vector from ~16-20 features.

## Key Insight

Our Build 43-45 probe data provides labeled training examples:
- **Positive**: scaffolded introspection responses (high specificity, low hedging)
- **Negative**: bare introspection responses (low specificity, high hedging)
- Cross-model: both Gemma 4 26B and Llama 3.3 70B responses available

SAE-RSV needs only ~10 contrast pairs to match 1000-sample baselines.
We have 20 per model (10 prompts × 2 conditions).

## Pipeline Design

### Phase 1: SAE Training (RunPod)
1. Load Gemma 4 26B (Q4_K_M or full weights)
2. Collect activations at layer 25-35 (where critical_analysis.gguf operates)
   on diverse text corpus (OpenWebText subset, ~10K examples)
3. Train TopK-SAE with 65K features, k=64 (following SAE-RSV config)
4. Validate reconstruction loss < 5% MSE

### Phase 2: Feature Identification
1. Run Build 43-45 prompts through Gemma, collect layer activations
2. Encode activations through trained SAE → sparse feature activations
3. Compute Δa_c (mean activation difference: scaffolded - bare) per feature
4. Features with Δa_c > 0 are candidate introspection features
5. Use DeepSeek V3.2 to semantically filter: is feature c introspection-relevant?
   (Replace GPT-4o-mini from paper with our available model)
6. Result: ~7-20 introspection-relevant features

### Phase 3: Vector Construction (SAE-RSV)
1. Aggregate relevant features: v_steer = Σ α_c · v_c (SAE decoder rows)
2. Construct noise vector from irrelevant features
3. Augment with semantically similar unactivated features
4. Final: v'_steer = α₁·v_steer - α₂·v_noise + α₃·v_useful
5. Export as GGUF control vector for llama-server

### Phase 4: Validation
1. Re-run Build 43-45 probes with SAE-derived vector
2. Compare: crude vector (Build 42), scaffolding (Build 44), SAE vector
3. If SAE vector matches scaffolding quality without prompt → baked-in capability
4. Test on attribution task too — does it interfere?

## Resource Estimate
- RunPod A100 80GB: ~$2/hr
- SAE training: ~2-4 hours (depends on corpus size)
- Feature identification + vector construction: ~1 hour
- Total: ~$10-15

## Dependencies
- RunPod API key (in chronicle.env)
- Gemma 4 26B weights (download to RunPod, or use from HuggingFace)
- SAE training library: SAELens (Bloom et al.) or custom TopK-SAE
- Build 43-45 probe data (already saved in chronicle/data/)

## Critical Note: Gemma Scope Limitation
Gemma Scope (arxiv:2408.05147) provides open SAEs for Gemma 2 (all layers, 2B/9B/27B).
But: "SAEs trained on pretraining data lack latents for concepts like 'refusal'
that emerge only in chat-tuned models."
→ We MUST train SAEs on Gemma 4 INSTRUCT weights, not base. Introspection
capability likely emerges during instruct tuning (DPO stage per Macar/Sauers).
Pre-trained SAEs would miss the features we need.
Gemma Scope architecture (JumpReLU, 65K features) is the template, but we need
fresh training on the instruct model.

## Three Approaches from Literature (ICLR 2026)

### 1. SAE-RSV (arxiv:2509.23799) — RECOMMENDED START
- **Method**: Semantic denoising + augmentation of steering vectors via SAE features
- **Strengths**: Only ~10 training samples needed, outperforms SFT, straightforward
- **Key numbers**: 16-20 features sufficient, +10.8pp over CAA baselines
- **Our fit**: Direct — probe responses are the training data

### 2. SAE-SSV (arxiv:2505.16188)
- **Method**: Supervised linear classifiers identify task-relevant SAE dimensions
- **Strengths**: More structured, "notably small subspace" sufficient
- **Key numbers**: Higher success rates across sentiment/truthfulness/polarity
- **Our fit**: Good if SAE-RSV's semantic filtering is too noisy

### 3. CRL (arxiv:2602.10437)
- **Method**: RL policy selects SAE features per-token, dynamic steering
- **Strengths**: Most sophisticated, per-token intervention logs
- **Key numbers**: Tested on Gemma 2 2B, improvements across 5 benchmarks
- **Our fit**: Overkill for v1 but interesting for dynamic introspection control

## Connection to @_lyraaaa_ (Lyra)
Lyra replicated the assistant axis in Gemma 4 E4B using SAEs.
Their work on Gemma 4 SAE features is directly relevant.
Consider reaching out via X once we have initial results.

## What This Would Prove
If SAE steering matches scaffolding quality:
- Layer 3 (scaffolding) can be COMPILED INTO Layer 2 (control vector)
- The three-layer model collapses: DPO creates capability, SAE steering activates it
- No prompt overhead — the introspection register is baked into inference
- Gemma becomes a better introspection evaluator for the pipeline

## Prediction (falsifiable)
SAE-RSV steering vector on Gemma 4 26B will produce introspection quality
between +1.0 and +1.7 (above bare +0.9, at or near scaffolded +1.7).
If below +1.0: SAE decomposition doesn't capture the scaffolding effect.
If above +1.7: SAE finds features scaffolding alone can't activate.
