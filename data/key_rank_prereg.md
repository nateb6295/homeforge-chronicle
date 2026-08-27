# Prereg — is the key collapse PRODUCED by the head, or INHERITED from the residual?
Written 2026-08-23 21:32, after reading Dong et al. 2103.03404 and BEFORE running.

## What reading changed
I was about to measure "effective rank of the per-head key matrix" and call a
low number evidence that heads store collinear directions. Dong et al. is about
a different object (OUTPUT of a PURE attention stack, no skips/MLPs, going
rank-1 doubly exponentially) and explicitly says skips and MLPs PREVENT this in
real transformers -- so it is not my result and not a duplicate.

But it surfaced the confound that makes my planned measurement uninterpretable:

  **Token uniformity in the RESIDUAL STREAM would produce collinear keys for
  free.** K = W_k . LN(h). If the h_i are already mutually similar across
  positions, the keys are similar no matter what W_k does. A low key rank
  would then measure the INPUT, not the head.

## The measurement, corrected
Per head, per layer, compute BOTH:
  A. directional diversity of the INPUT  (post-LN hidden states h_i)
  B. directional diversity of the KEYS   (K_i)
using the same statistic on both: effective rank (exp of spectral entropy of
the singular values) and mean pairwise |cos|.

The quantity of interest is the RATIO / difference, not B alone.

## Pre-registered outcomes
INHERITED : B is not lower than A (within noise). The head does nothing; the
            collinearity is the residual stream's, and my basin result is a
            statement about token uniformity, which is already known.
PRODUCED  : B is materially lower than A. The key projection actively collapses
            direction beyond what it was handed. This would be the finding.
AMPLIFIED-BY-DEPTH : the A-to-B gap widens with depth. Report the per-layer
            curve; do not summarise as a story. (I have wanted a depth story
            all day and should distrust liking this one.)
UNCLASSIFIED : anything else, or any non-finite value. INERT. (reflex 7b)

## Kill conditions
- Positive control (reflex 9): the INPUT diversity A must be HIGH at layer 0,
  where token embeddings are near-orthogonal by construction. If A is already
  low at L0, the statistic is broken, not the model.
- bfloat16 load, float32 math.
- Same prompt as the basin run, so the numbers are comparable.
- Note the standing hole: this measures direction diversity, which is what the
  basin count INFERRED. If PRODUCED holds, the basin claim is supported by a
  direct measurement for the first time. If INHERITED holds, I retract the
  "heads store collinear directions" reading and say the basin collapse was
  token uniformity all along.
