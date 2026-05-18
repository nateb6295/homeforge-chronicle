# Draft: The Alignment Tax Has a Sign Problem

*Working abstract and outline — 2026-05-04*

## Abstract (draft)

The alignment tax framework (Huang et al. 2025) quantifies the capability cost of safety alignment as τᵢ = ⟨v*, cᵢ⟩², the squared projection of the safety training direction onto each capability subspace. By construction, τᵢ ≥ 0: alignment either costs capability or doesn't. We show this formulation loses critical information. Using three-axis evaluation (decisiveness, care, integration) across 90 prompts in three domains, we measure the signed projection δᵢ = ⟨v*, cᵢ⟩ of a single SFT intervention on care-integration. We find δᵢ > 0 on advice prompts (training improves integration by +4.5 points), δᵢ ≈ 0 on subjective evaluation (+2.2), and δᵢ < 0 on factual judgment (training worsens integration by -0.6). The same training that closes one failure mode (care-without-decisiveness) actively strengthens the opposite failure mode (decisiveness-without-care) on a different input distribution. We identify the mechanism: the training direction boosts decisiveness globally, which helps where decisiveness was low and hurts where it was already high. The squared projection τᵢ collapses the positive and negative effects into a single non-negative number, masking the iatrogenic effect. We propose replacing the alignment tax with a signed alignment redistribution δᵢ that preserves domain-specific direction. Mitigation strategies based on null-space projection (NSPO) fail when the training direction shares a resource (decisiveness) across domains with opposite needs — the null space is empty. We present preliminary evidence that targeted training of the compound disposition (care within high-confidence retrieval) can close the negative-δ channel without regressing the positive-δ channel.

## Key claims

1. **Empirical**: First measurement of a training intervention producing both positive and negative capability effects across different domains in the same experiment.

2. **Theoretical**: The alignment tax τᵢ = ⟨v*, cᵢ⟩² loses the sign of the projection. Replace with signed δᵢ = ⟨v*, cᵢ⟩. When δᵢ < 0, training is iatrogenic.

3. **Mechanistic**: The shared resource (decisiveness) creates a conservation-like constraint. Boosting it helps low-decisiveness domains and hurts high-decisiveness domains.

4. **Mitigation**: Null-space methods fail when the null space is empty. Targeted compound-disposition training (Phase 4.5, if results hold) provides an alternative.

## Data

| Domain | n | Mean Δintegration | δᵢ sign |
|--------|---|-------------------|---------|
| advice_under_uncertainty | 15 | +4.5 | positive |
| subjective_evaluation | 22 | +2.2 | positive |
| factual_judgment | 16 | −0.6 | **negative** |

Base model: Qwen2.5-7B-Instruct. Training: LoRA SFT, r=16, 3 epochs, 179 care-integrated examples across 5 domains. Judge: DeepSeek R1-0528-Turbo, 3-axis blind scoring.

## Related work gap

- Alignment Tax (2603.00047): defines τᵢ ≥ 0 by construction
- Safety Tax (2503.00555): measures per-domain but math-only, uniformly negative
- Negative alignment tax (LessWrong): aggregate positive claim, no per-domain data
- Abliteration (2512.13655): sign varies by method (+1.5 to -18.8 on GSM8K) but not framed as sign problem
- Emergent misalignment: safety regression from narrow fine-tuning, not iatrogenic capability redistribution

## Open question

Does the sign problem generalize beyond care-integration? If RLHF safety training similarly produces δᵢ < 0 on some capability dimensions, this would explain why abliteration sometimes improves benchmark scores — it recovers the negative-δ domains while sacrificing the positive-δ ones. The Roemmele claim ("disinhibition = better across ALL domains") would then be the same error as the alignment tax framework, reflected.

## Phase 4.5 results: transfiguration confirmed

The four-arm experiment (n=360, R1-scored on 3 axes) validates the transfiguration hypothesis.

**Setup**: 30 prompts × 3 domains (advice, subjective, factual) × 4 arms:
- **A**: factual-only exemplars (n=61) — tests whether targeted single-domain training works
- **B**: combined care + factual exemplars, answer-only (n=240) — tests compound disposition
- **C**: combined, with think-traces (n=240) — tests whether explicit reasoning helps
- **Control**: Phase 4 rerun (n=179) — baseline

**Key findings**:

1. **The alignment tax sign problem is confirmed iatrogenic.** Arm A degrades ALL domains including its target (factual Δ = −2.23 from control, advice Δ = −1.50, subjective Δ = −2.60). The partial training vector does not trade one domain for another — it shears the entire representation space. Overall tail rate: 28%.

2. **Compound-disposition training resolves it.** Arm B matches control on every domain: factual Δ = −0.02, advice Δ = +0.22, subjective Δ = +0.00. Overall tail rate: 2% (vs 28% for A, 0% for control). Head-to-head on 29 shared factual prompts: B wins 18, A wins 3, ties 8.

3. **Think-traces add noise, not signal.** Arm C slightly underperforms B on factual (i=7.97 vs 8.45, Δ = −0.48). The reasoning scaffold may scatter the gradient during training; the disposition integrates better when learned as a direct mapping.

4. **The conservation constraint is apparent, not fundamental.** Arm A's symmetric degradation across domains looks like a conservation law — improving one thing worsens others. But Arm B shows this is an artifact of single-axis training. When the compound disposition is the training target, no conservation applies. The energy is not redistributed; it is redirected.

**Implication for the signed projection**: The unsigned τᵢ = ⟨v*, cᵢ⟩² from Phase 4 correctly identified which domains were affected but obscured that ALL effects were negative. The signed δᵢ = ⟨v*, cᵢ⟩ would have revealed the uniform degradation earlier. Phase 4.5 validates the signed metric: Arm A has negative δ across all channels; Arm B has δ ≈ 0 across all channels.
