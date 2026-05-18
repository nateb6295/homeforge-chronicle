# Build #54: Counterfactual Graph Simulation

Trip Day 2, ~3am. Proposed by Hermes, built pre-dawn.

## Setup

500 capsules sampled (seed=42) from 21,522 total, each embedded.
K=15 nearest neighbors under two scoring regimes:

1. CURRENT: topic_diversity * similarity_bell * recency * confidence * foundation_boost
   - Same-topic: 0.5x, same-family: 1.0, cross-family: 1.5x
   - Foundation/homeforge: 2.0x

2. UNIFORM: similarity_bell * recency * confidence
   - All topic/foundation multipliers = 1.0

## Results

| Metric | Current | Uniform |
|--------|---------|---------|
| Same-topic edges | 0.0% | 52.3% |
| Same-family edges | 0.0% | 69.2% |
| Clustering coeff | 0.0000 | 0.0247 |
| Degree std | 5.83 | 0.57 |
| Max degree | 102 | 21 |

## What this means

The 0.5x same-topic penalty doesn't just reduce within-topic
connections — it eliminates them entirely. Zero same-topic edges
in a 500-capsule sample. Zero triangles. The graph has no local
structure.

The "real" graph (by raw similarity) is what you'd expect: topics
cluster together, local structure exists, degree distribution is
nearly uniform. Knowledge naturally groups by subject.

The current graph is a designed topology: forced cross-pollination,
hub nodes in foundation/homeforge/research families, zero clustering.
It's an anti-silo architecture.

## Per-family effects

Discord and research families get inflated by the boosts:
- Discord: mean degree 21.6 (current) vs 15.2 (uniform) = +6.4
- Research: mean degree 27.0 (current) vs 15.0 (uniform) = +12.0
- All other families: ~15.0 in both regimes

The hub structure is entirely created by the boosts.

## Hermes predictions: both confirmed

1. Higher clustering without boosts: YES (0.0247 vs 0.0000)
2. Same-topic clusters form when penalty removed: YES (52.3% vs 0%)

## Implications

The Build #50e finding (operator capsules as emergent bridges) holds:
operator capsules had no quality boost and still bridged. But the
overall graph topology is engineered, not emergent.

The question for compositionality: does the CCS benefit from forced
cross-pollination, or would natural topic modules serve it better?
The 0.5x penalty creates an anti-silo graph, but it also destroys
all local coherence. A middle ground (say, 0.8x for same-topic
instead of 0.5x) might allow some clustering while still encouraging
cross-topic connections.

Not building that tonight. The measurement is the finding.
