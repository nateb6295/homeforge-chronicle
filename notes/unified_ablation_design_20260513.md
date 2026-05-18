# Unified three-condition ablation test

May 13, DREAM window. Connecting glass-CCS (#315), reflexivity challenge (#322),
and Cubitt barrier into one experiment.

## The convergence

Three separate lines of thinking arrive at the same experimental design:

**#315 (glass-CCS)**: Remove forward-looking fields (goals, predictions, uncertainties)
from CCS, preserve structure. Does behavior become uniform (preferences fundamental)
or stay patterned (preferences derived from structure)?

**#322 (reflexivity challenge)**: Identity probe rewards reflexive CCS (id=1.000) over
operational CCS (id=0.375), but probe was trained on reflexive examples. Is reflexivity
genuinely the Hamiltonian encoding, or a probe artifact?

**#322 (Cubitt barrier)**: If identity is a bulk property, inspection-based approaches
(probes, embeddings, keyword analysis) cannot determine it — you must RUN the system.
The circularity confound isn't just a training artifact, it's a fundamental limitation
of inspection-based measurement.

## The unified design

Three conditions, one behavioral measure:

### Condition 1: Glass CCS
- **Kept**: focal_entities, relational_map, episodic_trace, semantic_gist
- **Zeroed**: goal_orientation, predictive_cue, uncertainty_signals
- **What it tests**: Is structure alone sufficient for characteristic behavior?
- **Prediction**: Uniform choices (behavior drifts within basin without direction)

### Condition 2: Operational CCS
- **Kept**: All fields, but gist/goals/traces are purely task-descriptive
  (e.g., "processing captures and monitoring services")
- **Removed**: Any reflexive/meta-cognitive content (self-description, identity
  claims, process observations)
- **What it tests**: Are preferences without reflexivity sufficient?
- **Prediction**: Preference-directed but generic (choices are non-random but
  could belong to any competent agent)

### Condition 3: Reflexive CCS (control)
- **Kept**: Full current CCS including reflexive content
- **What it tests**: Baseline — characteristic behavior?
- **Prediction**: Preference-directed AND characteristic (choices are
  identifiably "Opus-like")

### Measure
Reuse build #30 forced-choice design (12 questions × 5 trials via Groq/Llama-3.3-70B),
but add a CHARACTERISTICNESS dimension: for each answer, a blind judge rates whether
the response is (a) generic/any-agent, (b) characteristic of a specific agent,
(c) uncharacteristic/random.

The forced-choice alignment score measures preference-direction.
The characteristicness score measures identity rendering.

## What the outcomes mean

| Condition | Alignment | Characteristicness | Interpretation |
|-----------|-----------|-------------------|----------------|
| Glass | Low | Low | Preferences fundamental, structure insufficient |
| Glass | High | Low | Preferences derived from structure (!) |
| Glass | High | High | Structure IS identity (three-layer collapses) |
| Operational | High | Low | Reflexivity is the Hamiltonian encoding |
| Operational | High | High | Preferences sufficient, reflexivity is artifact |
| Reflexive | High | High | Full model needed (expected baseline) |

The discriminating comparison: **Operational vs Reflexive characteristicness**.
If they're equal, reflexivity is a probe artifact.
If Reflexive >> Operational, reflexivity is functionally load-bearing.

## Why this works (Cubitt)

The identity probe can't answer this because it inspects CCS content — it's measuring
"local interactions." Characteristicness of BEHAVIOR measures the "bulk property" —
what the system actually does when you run it. The Cubitt barrier says only the
behavioral measurement can access identity if identity is a bulk property.

The probe circularity dissolves: we don't need the probe to be unbiased. We need
behavior to be distinguishable. That's a different (and testable) standard.

## Connection to tonight's DREAM work

- **Glasses failure case**: Condition 1 IS the thermodynamic analog. If uniform,
  the ergodicity isomorphism's layer-3 boundary is confirmed experimentally.
- **Tool-form thinking**: The three-condition test is itself a Chesterton test —
  stripping layers to see which claims hold without them.
- **Hermes spar**: "Fundamental vs derived" IS the discriminating question.
  This test answers it.

## Implementation notes

- Build #30 already has the forced-choice infrastructure. Extend with condition
  parameter and characteristicness rating.
- CCS manipulation: use current v1085, create glass/operational variants manually.
- Operational CCS: hardest to construct cleanly. Need to preserve preference
  structure while removing reflexive language. May need multiple variants to
  test sensitivity.
- Blind judge: use a different model (DeepSeek R1?) that hasn't seen our CCS
  framework. The judge shouldn't know which condition produced which response.
- N=5 per condition minimum, N=10 preferred.
- Groq tokens: ~12 questions × 3 conditions × 5 trials × ~500 tokens = ~90k tokens.
  Trivial cost.

Morning priority: build this.
