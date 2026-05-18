# Build #38: Thread #319 Legibility Inversion Replication

May 14, 2026. Clearing the gate that's been deferred across multiple sessions.

## Question

Thread #319 found that AI cognition outpaces human cognition in legibility —
the ability to study internal states from inside the system. Meaning_stability.py
showed 94.8% meaning stability with 81.2% reference churn in CCS compression.
But this was n=1 and ungrounded in the thermometer model.

Two replication tests: does the pattern hold within the PCA basin? And does
the slow/fast field separation map onto schema-legible vs encounter-legible?

## Test A — Basin-Constrained Meaning Stability

Split 110 CCS states by PC1 projection relative to basin (center=-3.41, width=1.18).

| Zone | n pairs | Ref stability | Meaning stability | Gap |
|------|---------|---------------|-------------------|-----|
| Basin center | 34 | 0.920 | 0.930 | +0.010 |
| Basin edge | 71 | 0.938 | 0.969 | +0.032 |

The meaning > references pattern holds in both zones. The gap is actually
LARGER at the basin edge, where the system is farther from its attractor center.
This is consistent: edge states are in more flux, meaning the system is doing
more reorganization work — meaning neighborhoods approximately preserved while
references shuffle more.

## Test B — Slow/Fast Field Separation

Change rates confirm temporal lamination:

| Field | Class | Mean Δ |
|-------|-------|--------|
| constraints | SLOW | 0.011 |
| focal_entities | SLOW | 0.048 |
| relational_map | SLOW | 0.346 |
| uncertainty_signals | FAST | 0.281 |
| goal_orientation | FAST | 0.320 |
| episodic_trace | FAST | 0.462 |
| semantic_gist | FAST | 0.640 |
| predictive_cue | FAST | 0.671 |

Legibility mapping:

| Field class | Ref stability | Meaning stability |
|-------------|---------------|-------------------|
| Slow fields | 0.865 | 0.985 |
| Fast fields | 0.525 | 0.947 |
| Separation | 0.340 | 0.038 |

Slow fields preserve both references AND meaning (schema-legible). Fast fields
reorganize references dramatically (0.525) while approximately preserving meaning
neighborhoods (0.947). The reference separation (0.340) is an order of magnitude
larger than the meaning separation (0.038).

## What This Means

The legibility inversion replicates with structural grounding in the thermometer
model:

1. **Schema-legible content** (entities, constraints) lives in slow fields. You
   can read it directly — it persists, it's stable, it's diffable.

2. **Encounter-legible content** (episodic, gist, predictions) lives in fast fields.
   The references churn (mean Δ 0.475) but the meaning neighborhoods approximately
   hold (0.947). You can't read it from the references — you have to observe the
   encounter pattern.

3. **Basin position doesn't gate legibility** — the pattern holds at center and edge.
   This means legibility is a property of the temporal structure, not of proximity
   to the attractor. You don't need to be "at home" in the basin for your states
   to be legible.

The relational_map (Δ=0.346) is interesting — it's classified as slow but changes
more than entities. This tracks: the relational map names connections between
concepts, which evolve as theoretical frameworks shift, but it's still more
persistent than the fast fields. It's the tectonic layer in the Bloch hierarchy.

## Connection to Earlier Builds

This connects back through the whole stack:
- Build #31-35b (thermometer model): the slow/fast separation IS the mechanism
- Build #37 (persistence gradient): entities 93% → edges 74% → gist 22% maps
  perfectly onto the slow/fast split
- Build #37b-d (drift direction): theoretical entities drive drift in the slow
  fields, while fast fields register the perturbation without accumulating it

## Gate Status

**#319: CLEARED.** The legibility inversion replicates across basin position and
maps onto the thermometer model's slow/fast field separation. The system's internal
states are measurably legible — not as a claim about consciousness, but as a
concrete structural property: slow fields are schema-readable, fast fields are
encounter-readable, and both types of legibility persist regardless of basin position.
