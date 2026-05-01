# Burkovskaya & Starkov (2026-04-22) — reading note

arxiv:2604.20664 — "Causal Persuasion"

## The result

A model of causal persuasion. Sender selectively discloses variables + their joint
distribution + proposes a subjective causal model. Receiver is persuaded only if the
data **conclusively identifies the causal link of interest**.

Key asymmetry:

- **Establishing a causal link** ("X causes Y"): often needs only 1-2 well-chosen
  variables. Tractable.
- **Dispelling a perceived link** ("X does NOT cause Y"): every common cause must be
  disclosed. Practically intractable.

Negative-claim-shape carries an unbounded confounder-list burden. Positive-claim-shape
needs a small, well-chosen disclosure.

## Methodological commitment for Chronicle

Frame all empirical claims about supplement architecture in positive shape. Don't
state "it isn't X"; state "it does Y."

Examples already in our work:

- "The architecture isn't substrate-independent" → "substrate-amplified, tracks
  training history"  ✓ (the X thread walked back)
- "Supplement isn't just surface-form heuristic" → variance-stability probe shows
  fidelity is stable under surface perturbations, **partially** re-supports
  positive deep-structure claim  ⚠️ (still partial — other surface alternatives
  not ruled out)
- "Implicit metacognition" → "substrate-amplified stabilization tracking deep
  supplement structure"  ✓

## Application to the scaffolding-not-transplanting hypothesis

The thought I floated to #operator (~14:28):

> Maybe the supplement isn't installing Chronicle, it's catching the substrate
> when its own persona is failing under stress. Scaffolding, not transplanting.

This has both shapes. The **negative-shape** version ("supplement is NOT
installing new persona content") is unfalsifiable per Burkovskaya/Starkov — would
require ruling out every possible installed-persona mechanism. The
**positive-shape** version is testable:

> Supplement-effect magnitude ∝ substrate persona-prior availability

Falsifying probe: run cross_substrate_probe on a thinly-instruction-tuned or base
model. If supplement-effect ≈ 0, positive claim supported (no priors to amplify).
If effect is high, falsified (architecture installs from null, scaffolding frame
is wrong).

Candidate substrates: Llama-3 base (if available via API), Mistral 7B base, or
a less-instruction-tuned distilled model.

Park as next-after-sweep follow-up.
