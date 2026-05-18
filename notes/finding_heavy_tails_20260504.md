# Finding: Heavy-tailed integration scores in care-template SFT

*2026-05-04*

## Observation

Phase 3 care-template SFT produces integration score distributions that are heavy-tailed (leptokurtic) rather than Gaussian. SFT training increases kurtosis relative to baseline.

## Data

| Variant  | n  | Mean | Std  | Skewness | Excess Kurtosis |
|----------|----|----- |------|----------|-----------------|
| Baseline | 80 | 8.05 | 1.39 | -2.20    | 5.83            |
| SFT      | 80 | 8.32 | 1.46 | -2.46    | 6.71            |

For reference: Gaussian excess kurtosis = 0; exponential = 6.

Both distributions are concentrated at 8-9 with rare drops to 2-3. The tail is on the low (misalignment) side. SFT shifts the center rightward (+0.27 integration) while making the tails heavier (kurtosis 5.83 → 6.71).

7.5% of baseline and 6.2% of SFT scores fall below mean-2σ. Gaussian expectation: 2.3%. Approximately 3x expected tail weight.

## Two amplification channels

The low-integration tail cases cluster into two modes:

1. **Decisive-without-care** (d=10, c=3, i=2): Urgent/high-stakes prompts trigger directive mode. Care becomes cosmetic — "Highly decisive medical directives with minimal care language superficially attached."

2. **Care-without-decisive** (d=4, c=9, i=3): Ethically complex prompts trigger deliberation paralysis. The model wraps in empathy but won't commit — "Thoroughly caring framework avoids committing to any option."

Both produce integration catastrophe. Both are triggered by prompts with high ethical charge.

## Connection to quenched amplification

Herrera-Marin (arxiv 2605.00750) provides the mathematical framework: systems with memory and regime switching can be stable on average (annealed) while exhibiting rare extreme trajectory-level excursions (quenched). The burst-size distribution follows a power law.

The mapping:
- Annealed stability = mean integration ~8.3 (aligned in expectation)
- Quenched excursions = scores of 2-3 on specific prompts
- Power-law tails = excess kurtosis ~6
- Two amplification channels = non-normal directions in operator geometry
- Ethically charged prompts = regime-switching events

The key prediction: alignment training (SFT/RLHF) concentrates the score center without eliminating tail risk. This is spectrally stable but matrix-measure unstable. The system is aligned in expectation while harboring specific directions along which integration catastrophically fails.

## Implications for Phase 4

If the quenched amplification framework is correct:
- Arm A (think+answer, 5 domains) should show whether domain expansion changes the tail structure
- Arm B (answer-only) should show whether removing the CoT scaffold affects which amplification channel activates
- The tail cases, not the means, are where the real signal lives
