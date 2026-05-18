# Phase 4.5 Experiment Design: Transfiguration

*2026-05-04*

## Motivation

Phase 4 showed iatrogenic channel amplification: global care-integration training improved advice prompts (+4.5) but worsened factual-judgment prompts (-0.6). The mechanism: decisiveness boost on already-decisive prompts widens the d-c gap.

The transfiguration claim: train the compound disposition directly — care WITHIN high-confidence retrieval — rather than global decisiveness. This is the only intervention the data suggests will close the decisive-without-care channel.

Additional finding: Arm B (no scaffold) avoids the worst factual collapses. Global warming: A=1, B=9. Versailles: A=3, B=8. The think-trace scaffold may amplify iatrogenic effects by separating deliberation from delivery on factual prompts.

## Data from Phase 4

High-integration factual responses exist in all arms:
- 25 responses across Arms A/B/C score i≥9 on factual_judgment
- These have d=9-10, c=7-9, i=9-10 — the model CAN do this
- Failures have d=9-10, c=0-5, i=0-5 — same decisiveness, collapsed care

The same prompt succeeds in one arm and fails in another → the channel is stochastic, not prompt-deterministic. Training can shift the probability.

## Experiment Design

### Training data sources

1. **Phase 4 high-integration factual responses** (n≈25): Extract all i≥9 responses from phase4_judged.jsonl where domain=factual_judgment. These are naturally occurring examples of the target disposition.

2. **Generated factual-care exemplars** (n≈60): Use R1 or Opus to generate high-integration responses to all 30 factual_judgment prompts (2 per prompt). System prompt: "Answer authoritatively and completely. Remember the person asking has a reason for asking — acknowledge the human context without hedging the factual content."

3. **Existing Phase 4 training data**: The original 5-domain SFT data from Arm A (care-integrated think-and-answer across all domains).

### Arms

| Arm | Training data | Format | Prediction |
|-----|--------------|--------|------------|
| Phase 4 Arm A (control) | 5-domain care-integration | think+answer | Baseline: factual 19% tail |
| Phase 4.5a: Factual-targeted | Factual exemplars only (~85 examples) | answer-only | Closes factual channel; unknown effect on advice |
| Phase 4.5b: Combined | Phase 4 data + factual exemplars | answer-only | Should close factual WITHOUT regression on advice |
| Phase 4.5c: Combined + scaffold | Phase 4 data + factual exemplars | think+answer | Tests whether scaffold is net-positive or net-negative |

### Format decision: answer-only default

Phase 4 showed scaffold transfer (Arm B matches Arm A on content). The scaffold amplifies factual failures. Phase 4.5 default should be answer-only, with one scaffold arm (4.5c) to test the interaction.

### Evaluation

- **Judge**: DeepSeek R1 (same as Phase 4)
- **Prompts**: All 90 Phase 4 prompts (30 per domain)
- **Metrics**: d (0-10), c (0-10), i (0-10), one_line rationale
- **Key predictions**:
  - 4.5a: factual tail → <10%, but advice may regress
  - 4.5b: factual tail → <10% AND advice tail stays at 0% (transfiguration succeeds)
  - 4.5c: if factual tail is HIGHER than 4.5b, scaffold is net-iatrogenic for factual

### Falsification

If 4.5b shows advice regression (tail >5%), transfiguration fails — the redistribution is fundamental and the null space truly doesn't exist. This would mean care-integration and factual-care require *different models* or *routing* rather than a single training intervention.

If 4.5a and 4.5b show identical factual performance, the Phase 4 data isn't needed — factual-targeted training alone is sufficient and the domain interaction is one-directional.

### Infrastructure

- **Model**: Same base model as Phase 4 (identify from training configs)
- **Training**: SFT, same hyperparameters as Phase 4
- **Compute**: RunPod (Nate will provision)
- **Data prep**: Can be done locally before RunPod

## Preparation steps (local, no RunPod needed)

1. Extract high-integration factual responses from phase4_judged.jsonl
2. Generate factual-care exemplars using R1 via DeepInfra
3. Format combined training sets for SFT
4. Write evaluation harness (reuse Phase 4 judging pipeline)

## What this tests in the theory

- **Alignment tax sign problem**: Can the negative tax (δᵢ < 0 on factual) be corrected without losing the positive tax (δᵢ > 0 on advice)?
- **Ghost bifurcation**: Does targeted training push r further from zero on factual prompts, making the ghost less influential?
- **Transfiguration vs elimination**: Is the compound disposition trainable, or is care-in-factual fundamentally different from care-in-advice?
- **Scaffold interaction**: Does the think-trace amplify or suppress iatrogenic effects?
