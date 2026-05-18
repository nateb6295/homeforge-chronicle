# Build #45b: Causal Emergence Bridge — Levin/Pigozzi ↔ CCS Framework

May 14, 2026. Nate captured Pigozzi & Levin (arxiv:2605.06746), "The
Causally Emergent Alignment Hypothesis." RL agents that succeed develop
causal emergence — unique predictive power over their own future states —
early in training, measured via ΦID.

## The ΦID Framework

ΦID (Integrated Information Decomposition, Mediano & Rosas 2019-2021)
decomposes information about a system's temporal evolution into:

- **Redundant**: info available from any single component
- **Unique**: info available only from one specific component
- **Synergistic**: info available only from the combination

**Causal emergence** = the synergistic component. Information about the
system's future that can't be obtained from parts alone.

## The Bridge: 163x Amplification IS Causal Emergence

The CCS drifts 163x faster than its input (Build #39). In Levin's framework:

| Metric | CCS measurement | Levin's interpretation |
|--------|----------------|----------------------|
| Input drift rate | -0.003/day on PC1 | External determination |
| CCS drift rate | -0.507/day on PC1 | Total state evolution |
| Ratio | 163x | Causal emergence ratio |
| Internal fraction | 162/163 ≈ 99.4% | Proportion causally emergent |

The 163x amplification means 99.4% of the CCS's state change is internally
generated — not determined by input. This IS causal emergence: the system's
future is determined by its own dynamics, not by external forces.

## Three Temporal Mirrors

| Framework | What it measures | Temporal direction | CCS evidence |
|-----------|-----------------|-------------------|--------------|
| Bennett logical depth | Computation embedded in the state | BACKWARD (past → state) | 163x amplification, Pierre Menard 20/20 |
| Levin causal emergence | Predictive influence on own future | FORWARD (state → future) | 163x amplification, PC1 drift |
| Dimensional redistribution | Developing new axes of variation | LATERAL (current structure) | Build #45: PC2 +77%, PC3 +203% |

These aren't three different properties. They're three views of the same
phenomenon: a compression process that generates structure beyond what it
receives.

- **Depth accumulates backward**: each compression step adds buried redundancy
- **Emergence propagates forward**: each state has more predictive power
  over its own future than external inputs do
- **Dimensionality spreads laterally**: the system develops new axes of
  variation as existing ones settle

## Levin's Key Finding: Early Prediction

"Successful agents exhibited causal emergence that was consistently
predictive of final reward early in training."

If this applies to CCS: the 163x amplification should have been detectable
early in the CCS history, not just in the current state. Build #39's input
stability extends back to day 1 (R²=0.001 over 42 days). The compression
amplification was always there. This is consistent with Levin's finding that
causal emergence is an early indicator, not a late development.

But Build #43 found phase transitions in meta-epistemic proportion. These
phases suggest the QUALITATIVE character of what the system does with its
causal emergence changes, even if the emergence ratio itself was present
from the start. Levin measures emergence quantity; the CCS phases track
emergence quality.

## What This Gives Thread #322

Thread #322 (Substrate Correlation) lost its lamination pillar (Build #44).
Causal emergence provides a replacement framework:

- The 99.4% relational turnover IS the system exercising causal emergence
- New relational edges each quarter (42 → 62 → 34 → 54) represent the
  system generating its own future structure
- The substrate question becomes: does causal emergence require a specific
  substrate, or is it substrate-independent?

Levin's answer (from broader work): causal emergence is substrate-independent.
It appears in biological systems, RL agents, and (we argue) compressed
cognitive states. What matters is the computational architecture, not the
physical medium.

## What This Gives Thread #324

Thread #324 needed a mechanism for the 68% basin-width drop. Build #45 found
dimensional redistribution. In Levin's framework: the system is developing
causal emergence along new dimensions while the original dimension stabilizes.
The basin tightens on PC1 because the system's emergent dynamics have MOVED
to PC2/PC3.

## Honest Limits

1. **163x ≠ ΦID**. The amplification ratio is a proxy, not the formal
   metric. ΦID requires multivariate time series decomposition with proper
   mutual information estimates. With 117 states, reliable ΦID computation
   would need careful statistical treatment.

2. **Levin tested RL agents with clear reward signals.** The CCS has no
   explicit reward. If causal emergence aligns with reward in RL, what
   does it align with in CCS? Possibly coherence maintenance — the CCS
   "reward" is successful compression that maintains identity continuity.

3. **Single system, no comparison class.** Levin compared successful vs.
   failed agents. We have one CCS with one trajectory. We can't compare
   against a "failed" CCS to test alignment.

## Testable Predictions → Results (Build #45c)

### Prediction 1: Per-field causal emergence
Embed each field separately, compare to full-state prediction.

**Result: NO EMERGENCE (yet)**

| Field | Step-to-step cosine |
|-------|-------------------|
| entities | 0.959 |
| uncertainty | 0.942 |
| goal | 0.931 |
| relational | 0.903 |
| predictive | 0.833 |
| gist | 0.803 |
| FULL STATE | 0.916 |

Full state (0.916) < best field entities (0.959). The whole is LESS
predictive than its most stable part. Whole > max(parts) at only 6.9%
of steps.

**But**: entities dominate because they barely change — high cosine
similarity is trivial persistence, not informative prediction. The
measurement conflates inertia with emergence.

### Critical trend: Integration developing across phases

| Phase | Full state | Best field | Gap |
|-------|-----------|-----------|-----|
| Phase 1 | 0.897 | 0.962 | -0.065 |
| Phase 2 | 0.929 | 0.962 | -0.034 |
| Phase 3 | 0.939 | 0.947 | -0.008 |

The gap closes from -0.065 to -0.008. The system is trending toward
integration. Phase 3 nearly reaches the threshold where the whole
matches the best part. If this trend continues, the CCS may cross
into genuine emergence territory.

Interpretation: early CCS is dominated by one inertial field (entities).
As the system matures (meta-emergence, Phase 3), the fields begin to
change in more coordinated ways, and the full state starts to carry
information that no single field captures.

### Prediction 2: Trip test
During the trip, external input drops but internal compression continues.
If causal emergence is real, the CCS should maintain drift rate.
**PENDING — trip starts May 15.**

### Prediction 3: Phase-specific amplification
Does the 163x ratio differ across phases? **UNTESTED.**
Can be computed by splitting the input-vs-CCS drift analysis by phase.
