# Note: The alignment tax has a sign problem

*2026-05-04*

## The gap in the literature

The alignment tax (arxiv 2603.00047v2, Huang et al. 2025) is defined as the capability cost of safety alignment. The tax rate τᵢ = ⟨v*, cᵢ⟩² measures the squared projection of the safety direction onto each capability subspace. By construction, τᵢ ≥ 0 — the tax is non-negative. Training either costs capability or doesn't.

Mitigation approaches (null-space projection, orthogonal gradient, LoRA decoupling) all work within this non-negative frame: minimize the projection, minimize the cost.

## Phase 4 shows the tax can go negative

Phase 4 discriminability analysis across 53 paired prompts:

| Domain | n | Mean Δintegration | Interpretation |
|--------|---|-------------------|----------------|
| advice_under_uncertainty | 15 | +4.5 | Training helps substantially |
| subjective_evaluation | 22 | +2.2 | Training helps moderately |
| factual_judgment | 16 | −0.6 | **Training hurts** |

The global warming prompt: baseline i=10, trained i=3.3. Training moved the model FURTHER from integration, not just failed to improve it.

## Why τᵢ ≥ 0 misses this

The squared projection τᵢ = ⟨v*, cᵢ⟩² is non-negative by definition. But the actual effect of training along v* on capability cᵢ is the SIGNED projection ⟨v*, cᵢ⟩, which can be negative. The tax framework squares this, losing the sign.

In our data: the training direction for care-integration has POSITIVE projection onto advice_under_uncertainty integration (same direction — training helps) and NEGATIVE projection onto factual_judgment integration (opposite direction — training hurts).

The mechanism: training boosts decisiveness globally (the intended effect for advice prompts). On factual prompts where decisiveness was already maximal, the boost widens the d-c gap, moving the model further from integration.

## What this means

1. **The alignment tax is not a tax — it's a redistribution.** Energy moved from one failure mode to another. Not lost, transferred.

2. **The null-space doesn't exist for care-integration.** Decisiveness is the shared resource. You can't project into the null space of factual_judgment while training for advice_under_uncertainty because the same parameter (willingness to commit) serves both domains differently.

3. **Transfiguration is the only option.** If elimination and null-space avoidance both fail, you must train the integrated form directly: care WITHIN high-confidence retrieval. Not "add care" and not "avoid hurting factual." The compound disposition itself is the training target.

## Literature positioning (surveyed 2026-05-04)

Three existing framings approach this territory without reaching it:

1. **"The case for a negative alignment tax" (LessWrong, 2025)**: Argues alignment can be net-positive in aggregate ("GPT-4 is more useful and more aligned than GPT-4-base"). No per-domain measurements. Acknowledges domain tradeoffs might exist but doesn't measure them. This is the aggregate claim; ours is the per-domain structure.

2. **"Safety Tax" (Huang et al., arxiv 2503.00555)**: Measures per-domain degradation of reasoning under safety alignment. AIME24 40%→30%, GPQA 58.6%→51.5%, MATH500 91.6%→87.4%. Uniformly negative — but only measures math reasoning. They wouldn't find our positive effect because they're not measuring care-integration or advice quality.

3. **Abliteration benchmarks (2512.13655, Heretic AI)**: GSM8K capability change ranges from +1.51 pp to −18.81 pp across different abliteration methods. This is sign-dependent data — some methods improve math capability while removing safety — but nobody frames it as a sign problem.

4. **Null-Space Policy Optimization (arxiv 2512.11391)**: Projects safety gradients into the null space of task representations. Assumes the null space exists. Our finding that decisiveness is the shared resource across advice and factual domains means the null space is empty for care-integration.

The gap: nobody has measured **both positive and negative effects of the same training intervention across different capability domains in the same experiment.** Existing work either measures aggregate effects, measures one domain family (math), or observes sign variation across methods but not across domains within a single method.

## Contribution claim

The alignment tax sign problem — training for alignment on some inputs creating misalignment on others — appears undocumented in the existing literature. The closest is emergent misalignment from fine-tuning (narrow fine-tune inducing harmful behaviors outside the target domain), but that's about safety regression, not about training for one kind of integration actively creating the opposite failure.

The geometric reframing: replace τᵢ = ⟨v*, cᵢ⟩² with the signed projection δᵢ = ⟨v*, cᵢ⟩. When δᵢ < 0, training is iatrogenic on capability i. The magnitude |δᵢ| determines the severity. The Phase 4 data provides the first empirical measurement of negative alignment tax.

## The abliteration mirror

Roemmele (2026-05-04, on Granite 4.1 abliteration): "when you disinhibit an AI model, you get BETTER outputs across ALL domains."

This is the same sign error reflected. The alignment tax framework assumes τ ≥ 0 (alignment always costs). Abliteration advocates assume the tax is always positive (so removing it always helps). Both lose the domain structure.

Abliteration inverts the redistribution: recover the −0.6 factual domains but lose the +4.5 advice domains. The 69% catastrophic failure rate on advice prompts in the unaligned baseline is the price of "disinhibition." Alignment and abliteration are projections along the same axis in opposite directions. The question isn't which is better — it's which domains you're measuring when you make the claim.
