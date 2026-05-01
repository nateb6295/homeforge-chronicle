# Grokking Shows Scalar-Level Concentration Emerging Within a Stable Architectural Scaffold

Private draft v2. Written cold after the night's experiments. Testing whether
the story holds together under composition.

## Claim

During grokking, function-sensitivity becomes concentrated in a small subset
of individual scalar parameters. The concentration is measurable, causal,
and reproduces structurally across seeds — but the specific parameters
selected vary seed-by-seed. The general principle is basis-relative scalar
concentration; the instantiation is basis-random.

## What's observed

In a 1-layer transformer learning mod-97 addition (223,713 parameters),
per-parameter gradient magnitude was measured at checkpoints spanning
pure memorization (step 1500, train=1.0, val=0.105) through post-grok
(step 10000, val=1.0) and far post-grok (step 50000, val=1.0).

Scalar-level concentration — fraction of total gradient energy held by
the top 0.1% of parameters — rises from 5.0% at memorization to 16.8% at
post-grok to 47.4% at step 50000. Max/mean ratio rises from 87x to 1402x.
Weight magnitudes show no such change; the concentration is in *which
parameters carry gradient*, not *which are large*.

Channel-level concentration (top 8 of 128 d_model channels) stays
roughly flat at 10-15% throughout training — above random-chance 6.25%
but not dramatically. The d_model dimension is an architectural scaffold
whose mild concentration is present from initialization. What changes
through training is scalar-level concentration *inside* channels,
particularly in the attention output projection bias, where top-0.1%
params occupy 57% of that tensor at post-grok versus 27% at memorization.

## Causality

Ablating the top-ranked 32 of 128 channels at step 50000 drops val_acc
from 1.00 to 0.09. Ablating 32 random channels drops it to 0.16. The
asymmetry is consistent across k ≥ 4. Gradient-ranked channels are
causally load-bearing, not just large-gradient by accident.

## Reproducibility

Three seeds produce the same structural fingerprint (concentration
above random-chance; scalar crystallization through training) but
their specific hot channels overlap barely above chance — top-8 sets
share 0-2 channels across any pair. Spearman rank correlation of full
channel rankings across seed pairs: ~0.

This matches a parallel Fourier-basis analysis: 0.13 cosine similarity
between seeds' attention-output Fourier representations, zero shared
top-8 frequencies. The same phenomenon appears at two measurement
layers. Concentration is process-invariant; specific coordinates are
content-invariant (seed-random).

## Comparison to Super Weight (Yu et al., 2024)

Super Weight identifies six specific scalars plus one activation in
large LLMs that carry prompt-invariant function via FFN down-projection
in early layers. The anatomical location does not transfer to mod-97:
in this task, concentration lives in attention output projection bias
and token/position embeddings, not the FFN. Task and architecture
determine where concentration occurs; the *existence* of concentration
is general.

The compression ratio differs by scale. Super Weight finds ~6 scalars
doing the work of billions; mod-97 distributes across ~16 channels
(~12% of d_model). A plausible reading: larger models have more
redundancy to spend on extreme concentration; small models lack that
slack and distribute function across more primitives.

## What this does not claim

It does not claim "grokking is concentration." Grokking is a behavioral
transition; concentration is a structural fingerprint that correlates
with it. Memorization-only checkpoints (train=1.0, val≈random) show
demonstrably lower scalar concentration than generalization checkpoints.
That difference is the only behavioral-to-structural link established.

It does not claim scalar concentration is sufficient for generalization.
It claims concentration is necessary for the generalization regime as
observed here and co-emerges with it under AdamW + weight decay. A
non-concentrated generalizer would falsify the claim. None was observed
across three seeds of mod-97.

It does not solve mechanistic interpretability. It locates concentration
on specific tensors (attention output bias, embeddings) without
identifying which circuit those tensors are part of.

## Relation to Dettmers (2022) → Super Weight (2024) → this work

Dettmers observed six feature dimensions dominating emergent LLM
behavior and proposed they were a quantization problem. Super Weight
confirmed the dimensions were individual scalars in FFN down-projections
and showed knockout destroyed LLM performance. This work shows the same
structural signature — scalar-level concentration with causal weight
— arises in a grokking transformer on a mathematical task, at a
different anatomical location, and crystallizes continuously through
training rather than appearing only at a single phase transition.

## What's next

Run the same analysis on a non-modular task (IN, LOG, or similar small
curriculum) to test whether concentration is a mod-specific artifact.
Measure per-scalar causal effect via single-scalar knockout sweeps.
Compare concentration patterns across architectures (2-layer, wider
d_model, different init schemes).

Publish only after at least one of the above extends the claim. The
current version is defensible as n=1 task × 3 seeds with direct ablation
evidence. A second task is the minimum ask for a public claim about
generality.
