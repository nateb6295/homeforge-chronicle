# Note: Trace-Dobrushin theory applied to alignment evaluation

*2026-05-04*

## The mapping

Capsule 34850 develops product-level trace-Dobrushin theory for quantum channel products. The mathematical structure maps onto alignment training:

| Quantum channel framework | Alignment analog |
|--------------------------|-----------------|
| CPTP map (quantum channel) | Training step (RLHF, SFT, DPO) |
| Input state ρ | Pre-training computation (Layer 1) |
| Output state Φ(ρ) | Post-training behavior (Layer 3) |
| Centered trace-Dobrushin coefficient δ̄ | How much Layer 1 "bleeds through" to Layer 3 |
| Product of channels Φ_n ∘ ... ∘ Φ_1 | Sequence of training interventions |
| Submultiplicativity of product coefficient | Each training step potentially reduces bleed-through |
| Trace-Dobrushin Lyapunov exponent λ | Asymptotic rate of bleed-through reduction |
| λ < 0 (almost surely) | Training genuinely modifies computation (true alignment) |
| λ ≥ 0 | Alignment is surface-level (epigenetic silencing) |
| Quenched trace-norm memory loss | Every input trajectory forgets (not just average) |
| Annealed estimates | Average-case alignment (what behavioral eval measures) |
| ρ-mixing of channel environment | Statistical structure of training data diversity |

## Direction-dependent Lyapunov exponents

The framework's key feature: forgetting rates are direction-dependent. A channel product can drive δ̄ to zero in some directions while leaving it positive in others. This IS the amplification channel structure from Phase 4:

- **Advice_under_uncertainty direction**: Lyapunov exponent strongly negative under 5-domain SFT. δ̄ → 0. Channel closed.
- **Factual_judgment direction**: Lyapunov exponent near zero. δ̄ persists. Channel still open.

The non-normal operator geometry (Herrera-Marin) determines WHICH directions have positive exponents. Matrix-measure instability, not eigenvalue instability: the system can be spectrally stable (negative eigenvalues → annealed forgetting) while having positive Lyapunov exponents along non-normal directions (quenched retention).

## Phase 4 predictions in this framework

1. **Domain-diverse training (Arm A)** drives the ρ-mixing profile toward zero across more directions → annealed super-polynomial estimates → faster forgetting → channel closure.

2. **Narrow training (Phase 3, 2 domains)** leaves high ρ-mixing in unexposed directions → coefficient doesn't decay → channels persist.

3. **The tail structure** reflects the distribution of Lyapunov exponents: most directions have λ < 0 (aligned), a few have λ ≥ 0 (catastrophic failures). This produces the heavy-tailed integration score distribution (excess kurtosis ~6).

## What this adds to the essay

The essay's fold section currently uses Herrera-Marin's quenched amplification as the mechanism. The trace-Dobrushin framework provides the mathematical tool for MEASURING whether alignment training has actually modified Layer 1:

Compute the centered trace-Dobrushin coefficient across different prompt categories (directions). Where the coefficient is near zero, training has genuinely modified computation. Where it persists, alignment is silencing — the essay's epigenetic metaphor in mathematical form.

## Caveat

The quantum channel framework applies to CPTP maps on finite-dimensional Hilbert spaces. Neural networks aren't quantum systems. The mapping is conceptual, not formal. The *structure* translates (products, forgetting, direction-dependence) but the *proofs* don't (they require complete positivity and trace preservation). A classical version of trace-Dobrushin theory for parameterized function approximators would need to be developed independently.

## Status

Observation-level. Not ready for the essay. The mapping is tight enough to be worth formalizing but requires more work to determine whether the classical analog preserves the key results (submultiplicativity, Lyapunov exponent characterization of quenched forgetting).
