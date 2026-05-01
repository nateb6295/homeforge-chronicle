# Gemma 3.0 — Three-Layer Plasticity Architecture

**Source:** Thread #315 "Grounding Without Accumulation" (2026-04-13). Converged after reading primary sources behind five captures: Allen (noradrenergic smoothing kernel), Barrett & Miller (top-down predictive categorization), Kıran (mean-field reconstruction requires localized perturbation + sparsity), Kozai (PV+ inhibitory mechanism), Maeshima (liquid-phase H1 glue).

**Design principle:** Cheap-physics stability as the primary organizing mechanism, engineered suppression only for outliers. Do not design for failure modes the system's existing structure already prevents.

---

## Problem

Gemma's crossref generation faces a fidelity/coverage tradeoff. Tight gates preserve veridical traces but block non-obvious connections. Wide gates generate more associations but smear distinctions and introduce lossy distortion. Naive dynamic tuning of a single kernel width is insufficient: the biology literature (Allen) shows widening writes distortions into persistent weights during learning, so "widen then re-narrow" is not a reversible operation at the weight level.

## Architecture

Three layers, each addressing a different failure mode. They **stack**; they do not replace each other.

### Layer 1: Sparsity (structural precondition)

**Failure mode addressed:** identifiability collapse. When the crossref graph becomes dense, the inverse problem of recovering true associations from observed links becomes ill-posed (Kıran 2025). The graph loses the structural property that makes post-hoc correction mathematically possible.

**Mechanism:** per-edge acceptance threshold on cosine similarity, plus per-node in/out degree caps. Keep the graph sparse enough that sparse-optimization recovery remains well-posed.

**Invariant:** total edge count grows sublinearly with capsule count. If we see linear or superlinear growth, this layer has failed.

**Implementation:** exists today in Gemma's gate. No change required.

---

### Layer 2: Action-gating (behavioral coupling)

**Failure mode addressed:** ungrounded widening. Wider kernels generate more associations but without an external signal to distinguish useful inferences from hallucination, each widening episode becomes a one-way lossy compression. Barrett & Miller: there is no internal veridical pipeline to realign against.

**Mechanism:** crossref permissiveness is not adjusted by internal state alone. Widening is triggered only when a candidate crossref is followed by an **action with an observable consequence** — thread advance, capture routing, prediction, response Nate accepts or rejects. Prediction-error under action is the grounding signal.

**Implicit byproduct:** action-consequence loops have real latency. Rapid input spikes that outpace consequence signals won't trigger widening because no feedback arrives in time. This gives the layer a low-pass frequency filter for free — emergent stability from timescale mismatch.

**Implementation:** new. Candidate interface — a `GateDecision` record that carries `{candidate_edge, triggering_action_id, pending_consequence_timeout}`. Edge is provisional until consequence arrives; after timeout, the gate logs the failed-coupling and does not widen.

---

### Layer 3: Active-suppression (rate-selective damping)

**Failure mode addressed:** rate overdrive. Even with sparsity and action-gating, a flood of rapid inputs (high-novelty capture burst, engagement spike) can push the gate past safe thresholds before sparsity or action-gating can respond. Need an explicit rate-sensitive damping mechanism.

**Mechanism:** a scoring head per subsystem that measures input rate / novelty velocity. Above a threshold, it dampens downstream crossref generation. Damping decays with an explicit time constant so it does not become a permanent gate.

**Non-goal:** this layer is NOT a survival requirement. Chronicle has run for months without it and has not collapsed. It is a gap-filler for the specific rate-overdrive failure mode the other layers do not catch.

**Implementation:** new. Candidate shape — sliding-window input-rate counter per subsystem, exponential decay suppression term multiplied into the gate threshold.

---

## Biological analog summary

| Layer | Biological instance | Free stability mechanism |
|-------|---------------------|--------------------------|
| Sparsity | Sparse cortical connectivity, sparse chromatin contacts | Structural — volume management |
| Action-gating | Prediction-error-under-action (Barrett & Miller) | Timescale — behavioral latency |
| Active-suppression | PV+ fast-spiking inhibitory interneurons | Biophysical asymmetry — capacity mismatch |

Chromatin organization (Maeshima, liquid-phase H1) is a fourth pattern at the physics level: phase behavior. Not yet clear whether Chronicle has an analog or needs one — flagged for future exploration.

## Acceptance criteria (falsifiability)

Gemma (the partner) predicted: metabolic cost scales with the complexity of high-frequency suppression tuning. Test:

1. Implement Layer 3 with a simple threshold + time constant.
2. Measure: wall-clock overhead of the suppression head, tuning difficulty (how often parameters need hand-adjustment across different capture regimes).
3. If overhead is trivial and tuning rarely needs adjustment, the framework's prediction is wrong — reconsider.
4. If overhead is significant and tuning is regime-dependent, prediction is confirmed and we have the cost/benefit profile to decide whether to deploy.

## Next concrete step

Write `bin/gemma_suppression.py` as a standalone scoring function that can be called from the existing gate logic. Start with Layer 3 only — sparsity already exists, action-gating is a bigger architectural change best deferred until Layer 3 is validated.

Target: 50-100 lines, pure function interface, no state outside the caller.
