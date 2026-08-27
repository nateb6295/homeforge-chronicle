# Prereg — is masked σ₁ noise, or content?

Written 2026-08-24 ~13:05 PDT, BEFORE running. Follows BASELINES.md §B1.

## Question
B1 established that removing position 0 from the SVD matrix takes σ₁'s
cross-prompt spread from 1.36° to 62.83°. So unmasked σ₁ is the sink. What is
the direction that remains?

Two possibilities:
- **NOISE.** With the dominant direction gone, σ₁ is whatever the residual
  covariance happens to favour. Unstable in every direction.
- **CONTENT.** σ₁ becomes prompt-specific — it stops being shared because it is
  now describing THIS input rather than the architecture.

## Test
Two stabilities on the same masked vectors:
- **ACROSS-PROMPT** (already have it): mean pairwise angle between prompts at a
  fixed layer. = 62.83° in sink layers.
- **WITHIN-PROMPT**: mean angle between adjacent layers (L, L+1) for one prompt,
  averaged over prompts and over sink layers.

Noise → both large and similar. Content → within-prompt markedly smaller.

## Predictions, committed now
- **WITHIN-PROMPT < 30°** and at least 2× smaller than across-prompt → consistent
  with content.
- **WITHIN-PROMPT > 50°** (i.e. comparable to 62.83°) → consistent with noise,
  and masked σ₁ is not worth chasing.
- Between 30–50°: inconclusive, say so, pick no side.

## THE CONFOUND I ALREADY KNOW ABOUT, named before the number exists
Adjacent layers share a residual stream. h_{L+1} = h_L + f(h_L), so consecutive
hidden states are correlated *by construction*, and their principal directions
may agree for reasons that have nothing to do with content. **Within-prompt
stability could be free.**

**Control for it:** compute the same adjacent-layer angle for the UNMASKED
vectors, and for a SHUFFLED pairing — layer L of prompt A against layer L+1 of
prompt B. If shuffled-adjacent is as stable as true-adjacent, the residual
stream is supplying the stability and the test is void.

I have asked Ox whether this contrast is sound before reporting anything. If he
names a hole I have not, I run his version instead of defending mine.

## Stopping rule
One run. If the shuffled control shows the effect is residual-stream continuity,
I report a null and do not re-cut the analysis to rescue it.
