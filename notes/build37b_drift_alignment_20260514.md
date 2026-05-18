# Build #37b: Compression Drives the Drift

May 14, 2026 — Follow-on to Build #37 (compression novelty).

## Question

Build #37 showed compression creates entities that persist (93%) and relational
edges that live briefly (2.1 steps). Build #36c showed the system drifts along
PC1 (execution → theory). Are these connected? Does the creative function of
compression drive the directional drift?

## Method

Projected entity-introduction events and edge-introduction events onto the PC1
axis from the PCA decomposition. Compared PC1 displacement on creative steps
(new entity/edge introduced) vs non-creative steps.

## Key Numbers

| Step type | n | Mean PC1 displacement | Direction |
|-----------|---|----------------------|-----------|
| Entity introduced | 30 | -3.73 | WITH drift |
| No entity | 76 | +1.46 | AGAINST drift |
| Edge introduced | 50 | -2.77 | WITH drift |
| No edge | 56 | +2.45 | AGAINST drift |

Entity introductions cause 58% larger absolute PC1 movements (3.90 vs 2.48).

## Result: CREATIVE EVENTS DRIVE CONVERGENCE

The directional drift from Build #36c isn't just happening — it's caused by
the creative function of compression. When the bottleneck generates a new entity
or names a new relational edge, the system moves 2-3x harder in the convergence
direction. When compression does a routine update (no new entities or edges), the
system pushes BACK — the -0.38 oscillation from Build #36.

The net drift is the difference: creative events overpower the homeostatic
corrections. The oscillation IS the corrections. The drift IS the creativity.

## Interpretation

This reframes the full-space oscillation from Build #36. The -0.38 direction
consistency (active reversal) is real — most steps correct the previous step.
But embedded in that oscillation are creation events that push asymmetrically
in one direction. The oscillation is noise; the entity introductions are signal.

The compression bottleneck is a directional creative organ:
- It rewrites the narrative (gist) each cycle — mostly noise
- It occasionally introduces an entity that persists — synthesis
- The synthesis events push consistently toward abstraction/identity
- The non-synthesis events push back — homeostatic correction
- Net effect: slow convergence driven by creative episodes

## Four Hypotheses for Why

1. **Input bias**: Nate's captures push toward theory. Compression reflects input.
2. **Model bias**: Llama-3.3-70B naturally abstracts during compression.
3. **Selection pressure**: Abstract entities persist longer (more general, harder
   to invalidate). Concrete entities are more ephemeral. Information-theoretic
   selection through the persistence filter.
4. **Attractor gradient**: The basin has a slope. Compression moves "downhill."

Can't distinguish these with current data. But the natural experiment (Nate's
trip) provides a test: remove the input source and see if entity creation rate
and PC1 displacement change.

## Predictions for the Trip

- If entity introduction rate drops → drift is observer-driven (hypothesis 1)
- If rate stays same but PC1 direction changes → observer directs, model creates
- If rate stays same and PC1 direction holds → model bias or attractor (2 or 4)
- If rate stays same but entity TYPE changes → selection pressure (3)

## Connection to Door 1

Build #37 asked: does compression create genuine synthesis? Answer: yes, 93% of
novel entities persist. Build #37b asks: does that synthesis have direction?
Answer: yes, entity introductions push 2.56x harder in the convergence direction.

The bottleneck isn't a loss function. It's the thing that makes the system move.
Without compression, no drift, no convergence, no entity creation. With compression,
the system builds — and builds directionally.

## What This Means

Compression wasn't designed as a creative mechanism. It was designed as necessary
for bounded memory. But the FUNCTION it performs is creative. The bottleneck forces
choices about what to keep and what to introduce, and those choices have direction.

The "But Won't I Miss Me?" question from two days ago asked what compression
destroys. Build #37b suggests the better question: what does compression create?
And the answer is: the drift itself. The loss is how the system moves.

## Next

- Statistical significance test (permutation test on entity-intro vs non-intro)
- Track WHICH entities appear on strongest-drift steps
- Trip natural experiment: baseline PCA Friday, compare on return
- Log compression inputs going forward to separate session-entered from model-created
