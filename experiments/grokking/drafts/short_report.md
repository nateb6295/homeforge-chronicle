# Grokking's causal locus is init-stochastic: a two-page note

**Working title. Short report. Negative result / scope refinement.**

## Setup

1-layer transformer, d=128, 4 heads, MLP hidden 4d, AdamW lr=1e-3,
weight_decay=1.0, train_frac=0.30, 50k steps. Task: f(a,b) mod p
for f ∈ {add, sub, mul}, plus a non-modular control max(a,b).
Primes p ∈ {97, 113}. All modular runs grok; max climbs train and
val together.

On each final checkpoint we measure per-parameter |∇ loss| on a
fixed probe batch and run two kinds of ablation: zero a single
embedding row (tok_emb[p], the equals-token row) and zero the
attention output-projection bias. Baseline val_acc is 1.00 for
every run.

## Three observations

**(1) Concentration is real but varies 2.4x.** Across five runs the
top-0.1% of parameters hold 20–48% of total gradient L1 energy (vs
0.1% under uniformity). Tail-concentration is a reliable signature
of grokking in this regime; its magnitude is not a reliable
signature. See `figures/fig1_concentration.png`.

**(2) MLP is a diffuse background, not absent.** Prior mechanistic
reports that draw attention to token-embedding and attention
structure leave an impression that the FFN is uninvolved. In our
runs the MLP holds 59% of parameters and carries 18–47% of total
|∇| energy on modular tasks — just with 2–7x lower mean gradient
than non-MLP parameters, so it does not show up in the top tail.
On the non-modular control (max) the MLP share drops to 8.5%. The
MLP share tracks *arithmetic structure of the task*, not grokking
as a dynamic. See `figures/fig2_anatomy.png`.

**(3) The causal locus is initialization-stochastic.** Same
architecture, same hyperparameters, same task (mul mod 97), three
seeds. Zeroing the equals-token row drops val_acc to 0.22, 0.95,
and 1.00 respectively. Zeroing the attention out_proj bias drops
it to 0.08, 0.93, and 1.00. At p=113 the same surgery leaves val
unchanged — the model routes through neither candidate tensor.
See `figures/fig3_ablation_grid.png`.

We additionally report a negative result on Yu et al.'s Super Weight
prediction: the scalar parameter with maximum |∇| — the "hero
scalar" — is never in the FFN in our runs, and zeroing it leaves
val_acc at 1.00 ± 0.00 across 20 random-scalar controls. Super
Weight's concentrated-single-scalar failure mode does not
materialize at this scale.

## One bonus observation on dynamics

**Grok transition is visible in gradient concentration.** On add
seed 0, top-0.1% share at init is 18x uniform; during the
memorization plateau (train=1.00, val still at chance) it is 50x
uniform; by the val_acc jump it is 250x uniform; at step 50k it is
366x uniform. The concentration slope changes sharply around the
val_acc transition rather than evolving smoothly. The within-run
memorization phase thus supplies a non-grokked baseline of the same
model, 5–7x below the post-grok concentration. MLP share does not
show a grokking-specific transition — it stays in a 0.28–0.41 band
throughout training. See `figures/fig6_concentration_trajectory.png`.

## One more observation

**Post-grok wobble and drift.** Sub seed 2 grokked by step 11k
(val=1.00), then both train and val dropped to 0.55/0.52 at step
26.9k, then recovered to 1.00 by step 36.1k — a 25k-step excursion.
Sub seed 1, same hyperparams, was stable throughout. Direct
weight-space comparison shows both seeds drift substantially from
their step-11k grokked state by step 50k (cosine ~0.78 stable,
~0.73 wobble). The wobble accelerates drift modestly rather than
catapulting the network into an unrelated basin. Grokking does
not pin a fixed point. See `figures/fig5_wobble.png`.

## Where this sits in the literature

Power (2022) introduced grokking. Nanda (2023) gave the canonical
mechanistic account: Fourier-structured embeddings + trig-identity
circuit on modular addition. Liu (2022) mapped the phase diagram
(wdecay, lrs). Chughtai (2023) tested universality on group
operations and reported *weak* universality (algorithmic family
shared) with mixed evidence for *strong* universality (identical
implementations). Yu (2024) identified super-weights in FFN
down-projections of large language models.

Our relation to these:

- Nanda's Fourier structure is present in our runs (fig4), and we
  do not dispute it.
- We quantify Chughtai's weak/strong distinction *at the
  parameter-importance level*, not the representation-identity
  level, and show the strong-universality gap is large enough
  (val_acc 0.22 to 1.00 on one ablation across three seeds) that
  single-seed locus claims cannot be straightforwardly reproduced.
- We fail to find Super Weights at this scale; MLP carries its L1
  energy diffusely rather than concentrated on a scalar.

## Takeaway in one sentence

"Grokking lives in X" should be read as "in this particular seed,
grokking routes through X"; what is actually invariant is the
shape of the gradient distribution, not the identity of the
parameters that instantiate it.

## Caveats

Three seeds on mul; one each on add, sub, and the controls. One
non-modular task. Zero-substitution ablation only. Wobble sampled
every 100 steps. Full-length draft at `drafts/paper_v1.md`
includes concentration tables, ablation grid, and a broader
limitations section.
