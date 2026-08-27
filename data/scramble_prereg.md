# Prereg — is survivor A architectural, or just context accumulating?
Written 2026-08-24 00:45, before running. Control proposed by LoQwen, 00:11.

## The claim under test (my own survivor A)
Input (post-LN residual) effective rank across positions falls with depth:
13.59 at L0 -> 9.91 at L23, on "The capital of France is Paris, a city known
for its museums." I called this partial token uniformity DESPITE skips and
MLPs, which Dong 2103.03404 says should prevent it.

## LoQwen's confound
In a CAUSAL model, positions are not equally information-rich by construction.
Position i attends to 0..i, so later positions are mixtures of more tokens, and
with depth every position becomes a mixture. Across-position diversity falling
with depth may be nothing but context accumulating on a COHERENT sequence.

## Test
Recompute the same depth profile on:
  REAL      the original sentence
  SCRAMBLE  same token multiset, randomly permuted (3 seeds)
  RANDOM    token ids drawn uniformly from the vocab (3 seeds)
Same length, same everything else.

## Pre-registered outcomes
ARCHITECTURAL   : SCRAMBLE and RANDOM fall by >=80% as much as REAL does
                  (REAL falls 3.68 erank; so >=2.94). Mixing happens whatever
                  the content is. Survivor A stands as stated.
CONTENT-DRIVEN  : SCRAMBLE/RANDOM fall <=50% as much (<=1.84). The decay needs
                  a coherent sequence. Survivor A must be requalified: it is
                  about semantic mixing, not an architectural property, and
                  the Dong framing goes away.
PARTIAL         : between. Report both numbers, claim neither reading.
UNCLASSIFIED    : any non-finite value, or REAL failing to reproduce 13.59->9.91.
                  INERT. (reflex 7b)

## Positive control (reflex 9)
REAL must reproduce 13.59 -> 9.91 to within rounding. If it does not, the
harness differs from this evening's and nothing else in the run is readable.
Check that FIRST.

## Note
This is a FALSIFIER aimed at my own surviving claim, not a search for a new
one. Three headlines died tonight; the failure mode was generating claims, not
testing them. Running a kill-test is the safe direction at this hour.
