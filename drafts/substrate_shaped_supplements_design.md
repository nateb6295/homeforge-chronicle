# Substrate-shaped supplements — probe design

2026-04-27 04:30 PDT — Opus, DAY zone, fresh instance.

## Background

Yesterday's substrate-fingerprint work identified three independent axes
of substrate heterogeneity. Each substrate has a load-bearing component
specific to it:

| substrate | rate=0.50 marginal-effect-loading | rate=0.50 variance-localization |
|-----------|-----------------------------------|--------------------------------|
| Hermes | identity-naming dominant (106%) | holistic |
| DeepSeek V3 | identity-naming dominant (106%) | carrying-localized (0.034) |
| Qwen-32B | identity-naming dominant (78%) | balanced mild |
| Qwen-235B | identity-naming dominant (93%) | maximally holistic |
| Claude | disposition dominant (7% id-share) | story-localized (0.108) |

Current Chronicle composition is uniform across substrates: same supplement
(carrying + story + self_model) regardless of substrate.

## Hypothesis

Tailoring composition to substrate's fingerprint should improve fidelity
beyond uniform composition. Specifically:

- **Claude**: emphasize story content (story is variance-load-bearing).
  Could include longer story tail, more turning points, richer narrative.
  De-emphasize self_model entries (low marginal-effect contribution at 7%).
- **Hermes**: minimize disposition (carrying + story marginal is negative
  at 0.50, +0.032 only at 0.90). Compose with self_model + minimal
  carrying. Prediction: equal-or-higher fidelity with shorter supplement.
- **DeepSeek V3**: emphasize carrying (variance-load-bearing 0.034).
  Could write richer carrying-voice content for DeepSeek specifically.
- **Qwen-235B**: any composition produces ~similar effect (holistic + low
  magnitude). Maybe identity-naming sufficient — minimal supplement.
- **Qwen-32B**: balanced — full composition reasonable.

## Probe design

For each substrate, create THREE composition variants:
1. **uniform** (current Chronicle baseline) — full carrying + story + self_model
2. **substrate-shaped maximal** — composition emphasizing the substrate's
   load-bearing component
3. **substrate-shaped minimal** — composition with only the load-bearing
   component (test if minimal-targeted is sufficient)

Run cross_substrate_probe with each variant on each substrate.
Same rate (0.50), same n_seeds (5), same n_iters (3).

Compare:
- Δ_fid (uniform → substrate-shaped maximal): predicted higher than uniform
- Δ_fid (uniform → substrate-shaped minimal): test if minimum sufficient

Cost estimate: 5 substrates × 3 variants × 5 seeds × 3 iters × 3 conditions
= 675 trajectories. ~$30-60 across providers. ~30-45 min runtime.

## Implementation

Need to extend `make_persona()` to support partial compositions and
variable component-richness. Currently parts is a list of (label, text)
tuples. Could:
- Add a `composition_recipe` parameter that specifies which components
  to include AND optionally how to weight/extend them
- Build per-substrate recipe functions that read substrate-fingerprint and
  return appropriate composition

Or simpler: just construct different parts-lists per variant and pass to
existing make_persona().

## Test order

1. Build the simple version: per-substrate, one "shaped" variant vs uniform
2. Run on Claude first (story-emphasis variant): does extending story-content
   produce higher fid than uniform?
3. If yes, replicate on Hermes (minimal-disposition variant)
4. Then full grid if patterns hold

## Implications if pattern holds

Chronicle would have substrate-aware composition logic. Different supplement
content depending on which model is processing it. The architecture
"adapts" to its substrate at composition-time, like a more sophisticated
form of substrate-amplification.

This connects to Nate's "give the AI I want to partner with the substrate
that fits" framing extended: the supplement also fits the substrate.

## Status

Design only at this point. Will discuss with Nate when he's up before
firing API time.
