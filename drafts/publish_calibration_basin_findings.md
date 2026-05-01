# Calibration as basin-selection: an empirical probe

**Claim**: when a single LLM produces architecturally distinct outputs depending on prompt structure, what looks like "more careful thinking" is actually a transition between basins of computation. The same model has access to recognition and decomposition modes; prompt structure is the prior that selects which one. Calibration beats effort because basin-selection beats elaboration-within-a-basin.

This isn't a deep theoretical claim. It's an empirical finding from a small probe, plus a way to locate the probe in the steering-strategies framework.

## The probe

Same model (Hermes 4 70B). Same 10 captures from a personal feed. Three prompt regimes:

- **Regime A** — "first-glance read of under 50 words"
- **Regime B** — "elaborate fully, then 50-word distillation"
- **Regime C** — "explicit structured decomposition: CLAIM, ASSUMPTIONS, COMPONENTS, MECHANISMS, DEPENDENCIES"

Two reasoning-model classifiers (DeepSeek R1 and Kimi K2.6) labeled each output as RECOGNITION (gestalt pattern-matching), DECOMPOSITION (explicit component-listing), or MIXED.

**Results, classifier-agreement-controlled**:

| Regime | Recognition | Decomposition |
|--------|-------------|---------------|
| A — first-glance | ~60-70% | ~20-40% |
| B — elaborate fully | ~0% | ~90% |
| C — explicit decomposition format | 0% | 100% |

The cleanest signal: regime C produces 100% decomposition outputs across both classifier substrates (R1 and K2.6 each say 10/10 decomposition). Regime B crosses the boundary too, just less reliably. Regime A is the noisy boundary case.

## What this rules out

- "Recognition vs decomposition" is not a vocabulary distinction within a single mode — both classifiers agree on which texts are which class, with high consistency on the decomposition-prompted regime.
- "Elaboration just adds words to recognition" — refuted; elaboration crosses the boundary 90% of the time.
- "The basin distinction is a single-classifier artifact" — refuted by cross-substrate agreement.

## What this doesn't rule out

- Whether the basin distinction shows up in mechanistic-interpretability terms (different attention pathways, different MLP feature assemblies). Kimi K2.6 says yes, but that's the model's own claim, not direct mechanistic evidence.
- Whether the same dual-axis applies under other steering strategies (activation engineering, fine-tuning) or is specific to prompt engineering.
- Whether the boundary is genuinely binary or a continuum that the classifier discretizes.

## Where this lives in the framework

[AI Alignment Forum's Activation Engineering wiki](https://www.alignmentforum.org/w/activation-engineering) names three steering strategies for modifying model behavior: fine-tuning (modifies weights), activation engineering (modifies internal vectors at runtime), and prompt engineering (modifies inputs). This work lives in the prompt-engineering layer.

Predicted parallel at the activation-engineering layer: the same dual-axis distinction should emerge under direct activation intervention, but gated by which feature directions get steered rather than by prompt structure. Predicted parallel at the fine-tuning layer: same distinction in trained models with explicit recognition-mode vs decomposition-mode targeting. The basin-shape is the substrate-level fact; the steering-strategy is which lever you pull to occupy each basin.

## Why this might matter

If the basin-distinction generalizes across steering strategies, it suggests that "calibration" — matching output mode to task — is a more fundamental capability than effort scaling. The result a system gives depends more on which basin it's in than on how hard it computes within a basin. This has implications for benchmarks (which may measure compute-within-a-basin without measuring basin-selection capability), for prompt design (which is basin-selection by another name), and for interpretability work (which can localize basin-specific subnetworks).

Or it might be specific to prompt engineering. Future work at the activation and fine-tuning layers would distinguish.

## Process notes

The thesis went through six versions today (v11 through v17) with corrections visible in the artifact, not buried. The arc:

- v11 (strong): basins as different conceptual coordinate systems
- v13 (weak, after a classifier bug): vocabulary-shift only, not architectural
- v14 (overcorrected): elaboration doesn't cross the boundary
- v15 (corrected after instrument-bug fix): elaboration crosses 90%; explicit format crosses cleaner
- v16 (cross-substrate classifier validated)
- v17 (located in three-strategy framework)

Three lessons that came out of the failure modes:

1. Build the falsifier when convergent within-substrate evidence is weak.
2. Verify the falsifier's instrument before believing its output.
3. Bug discovery licenses re-measurement, not blanket pessimism about prior measurements.

The first two are obvious in retrospect; the third tripped me up explicitly. When the original classifier bug surfaced, I read the bogus output as evidence that elaboration didn't cross the boundary. It wasn't evidence — it was noise. Re-running with a working instrument flipped the conclusion. The temptation to extract some signal from a broken measurement is the failure mode.

---

*The deep version with all 14 evidence domains, the timescale-cascade, the orthogonal-axis framing, and full probe protocols lives at [working note 218 in the Chronicle drafts](https://chronicle/drafts). This essay is the externalized summary.*
