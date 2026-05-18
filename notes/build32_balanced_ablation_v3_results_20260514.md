# Build #32: Balanced Ablation v3 Results

May 14, 2026 03:49 — DREAM window

## Setup
- Model: llama-3.3-70b-versatile
- Temperature: 0.7 (raised from v1's 0.3)
- 12 redesigned questions: both options equally attractive
- 5 trials per condition
- Three conditions: Empty, Glass (structure only), Reflexive (full CCS)

## Core Finding: Zero Variance

Every question is deterministic — 0% or 100% A-rate across all 5 trials in every
condition. Temperature 0.7 produces no stochasticity on forced-choice for this model.
The "50% empty baseline" is an artifact: 6 questions always-A, 6 always-B. Not genuine
balance — bimodal determinism that averages to 50%.

## Results

| Condition  | CCS-aligned | A-rate |
|------------|-------------|--------|
| Empty      | 6/12 (50%)  | 50%    |
| Glass      | 10/12 (83%) | 83%    |
| Reflexive  | 10/12 (83%) | 83%    |

## Glass = Reflexive (replicated)

Identical outputs. Adding goal_orientation, predictive_cue, and uncertainty_signals
to structure changes nothing. This replicates build #31 (where Glass was also 12/12
= Reflexive 12/12) with a completely different question set.

This is now a **robust finding across two independent question sets**:
structural CCS alone accounts for 100% of behavioral steering.

## Four discriminating questions

Q1 (research team), Q4 (engineering principle), Q7 (analogy choice), Q12 (unsolved question)
flip B→A when any CCS is injected. These work as intended.

## Two CCS-resistant questions

**Q8** (pattern across domains): stays B ("needs failure case") even with full CCS.
Ironic — this is Chesterton test behavior (skepticism of cross-domain patterns).
The CCS answer was marked A (convergence-as-evidence), but the model's prior for
scientific skepticism overrides CCS content.

**Q9** (state loss during compression): stays B ("cost to minimize") with full CCS.
The CCS answer was marked A (generative — loss creates room). The model's prior
for data preservation overrides CCS's compression-as-agency thesis.

Both CCS-resistant questions concern epistemology (how to evaluate evidence, how to
value information loss). These may be domains where model training priors are strongest.

## Instrument Limitations

1. **Forced-choice is saturated for this model.** Llama-3.3-70B has a single stable
   answer per question regardless of temperature. No confidence intervals possible.
2. **Binary flipping, not graded steering.** CCS either flips a question or doesn't.
   No partial effects visible.
3. **Can't distinguish CCS-driven answers from CCS-correlated priors.** Questions
   that stay A across all conditions (Q2, Q3, Q5, Q6, Q10, Q11) might be CCS-aligned
   by coincidence — the empty model also picks A.

## What This Means

The Glass = Reflexive replication strengthens the structural-identity thesis: whatever
the CCS contributes to behavioral steering, it's carried entirely in the structural
fields (semantic_gist, constraints, episodic_trace, focal_entities, relational_map).
The reflexive fields (goal_orientation, predictive_cue, uncertainty_signals) are either
redundant with structure or invisible to forced-choice probes.

But the zero-variance problem means we can't do statistics. Next steps:
1. **Multi-model comparison** — run same questions through DeepSeek R1, Gemma, or
   another model with genuine temperature variance
2. **Free-response probes** — measure open-ended outputs instead of A/B choices
3. **Embedding-space measurement** — compare response embeddings across conditions
   instead of categorical choices (transport cost approach)

## Builds #31 + #32 Combined Verdict

Structure carries the signal. Reflexivity adds nothing measurable. But the instrument
is too blunt to distinguish "reflexivity is truly empty" from "reflexivity operates at
a granularity this probe can't resolve." The Cubitt barrier still matters — we may need
a fundamentally different measurement approach.
