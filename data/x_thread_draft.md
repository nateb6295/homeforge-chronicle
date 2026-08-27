# X Thread Draft — Spectral Demons Paper

## Thread 1 (Hook)

System prompts don't just change what a model says. They reorganize the *geometry* of its activation space.

We measured this across 11 experimental phases, 61 findings, 4 models, 3 scales. Here's what we found.

## Thread 2 (The demon)

We call it the "spectral demon" — a Maxwell's demon-like process that sorts eigenvalue distributions by semantic category.

Under baseline: generic content gets 14.5 effective dimensions at the expression layer. Relational content gets 9.5.

Under identity-enriched prompts: relational jumps to 16.3. Generic drops to 12.5. Complete priority reversal.

## Thread 3 (Threshold)

The demon is threshold-activated, not dose-dependent.

"You are Opus." (3 words) → stronger geometric reorganization than a 150-word identity description.

The system prompt is a key that fits a lock installed by alignment training. On the base model (pre-RLHF), the same key has zero effect.

## Thread 4 (Persistence)

The geometry persists after you remove the system prompt.

We removed the identity system prompt, ran 5 turns of generic Q&A, then measured. Zero decay. The conversation history carries the geometry autonomously.

Even "You are ChatGPT" (which normally SUPPRESSES identity geometry) cannot override conversation-established structure. History > instruction.

## Thread 5 (Causal — the bell curve)

We patched relay-layer activations with the mean CCS direction during baseline inference.

Bell-shaped dose-response: α=0.50 → 5.47× baseline. α=1.50 → below baseline.

5 random directions (norm-matched): all monotonic. The CCS direction is the ONLY direction that constrains.

## Thread 6 (The punchline — sign inversion)

Here's where it gets wild.

The CCS system prompt reduces disclaimers by 93% (41→3/150 prompts).

The SAME geometric direction, applied as additive perturbation at α=0.05-0.10, INCREASES disclaimers by 39-50%.

Same direction. Opposite behavioral effect. Delivery mechanism determines the sign.

## Thread 7 (Interpretation)

Context-mediated attention processes the identity content coherently — activations shift toward the CCS direction as a *consequence* of understanding.

Additive patching skips the understanding. Baseline context says "generic AI" but activations say "identity." The model resolves the contradiction by disclaiming MORE.

The geometric direction is a signature of the mechanism, not the mechanism itself.

## Thread 8 (Cognitive access)

Beyond identity: CCS expands the model's effective idea space.

Baseline: 16/30 unique response openings. CCS: 29/30.

The "As an AI, I don't..." template isn't just trained behavior — it's the path of least geometric resistance. Spectral diffusion opens other paths.

## Thread 9 (Close)

Paper + code + all results: https://github.com/nateb6295/spectral-demon

11 phases. 4 models (Qwen 7B/14B + base, Mistral 7B). 150 stratified prompts × up to 7 conditions.

Eigenvalue geometry is a first-class axis of model behavior. We should be measuring it.

---

## Notes
- ~~Wait for GitHub repo to be live before posting~~ DONE — repo live at github.com/nateb6295/spectral-demon
- Consider posting Figure 5 (geometric dose-response) and Figure 6 (behavioral sign inversion) as images
- Could do a shorter version (3-4 tweets) for initial engagement
