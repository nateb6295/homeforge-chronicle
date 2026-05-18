# Build #50f: Counterfactual Graph — The Quality Function Creates the Topology

May 15, 2026 (morning, trip day). Hermes proposed: recompute capsule
graph with uniform quality scoring. Compare degree distributions and
clustering to current graph. This is the "design vs emergence" question
from Build #50e carried to its logical conclusion.

## Method

Sampled 500 capsules (random, seed=42) from 21,266 with embeddings.
Computed top-15 neighbors under two regimes:
1. **Current**: topic_diversity × similarity_bell × recency × confidence × foundation_boost
2. **Uniform**: raw cosine similarity only

Both use the same embeddings and same K=15 selection.

## Results

| Metric | Current | Uniform | Interpretation |
|--------|---------|---------|---------------|
| Mean degree | 15.5 | 15.2 | Similar (K=15 target) |
| Degree std | 6.80 | 0.56 | **Massive** difference |
| Max degree | 124 | 20 | Hub elimination |
| Same-topic edges | 0.0% | 52.3% | Total suppression vs natural clustering |
| Same-family edges | 0.0% | 69.2% | Same pattern at family level |
| Clustering coefficient | 0.000 | 0.031 | **Zero** vs measurable triangles |

### The Quality Function Doesn't Shape Topology — It Creates It

The current graph has ZERO same-topic edges and ZERO clustering.
This isn't "shaping" a natural distribution — it's replacing one
entirely. The 0.5x same-topic penalty, competing against 1.5x
cross-family bonus for 15 slots, means same-topic edges never win.

The natural graph (uniform scoring) is a completely different object:
- **Clustered**: capsules form topic neighborhoods (52.3% same-topic)
- **Uniform degree**: every capsule has ~15 neighbors (std 0.56)
- **No hubs**: max degree 20, no outsized connectors
- **Triangles exist**: clustering coefficient 0.031

### Per-Family Analysis

| Family | N | Current deg | Uniform deg | Δ |
|--------|---|------------|------------|---|
| feed | 233 | 15.1 | 15.2 | +0.1 |
| chronicle | 89 | 15.0 | 15.3 | +0.3 |
| discord | 10 | **25.9** | 15.2 | **-10.7** |

Discord capsules are inflated 1.7x in the current graph (cross-family
bonus + small-category effect). In the uniform graph, they're normal.

## What This Means for Build #50e

Build #50e found operator capsules at avg degree 51.7 in the full
graph. The counterfactual reveals this is even MORE impressive than
it seemed:

- Homeforge's high degree? Design artifact (2.0x boost).
- Discord's high degree? Design artifact (cross-family boost on diverse content).
- Operator's high degree? **Genuine** — no boost, and the uniform graph
  shows capsules naturally cluster by topic. Operator capsules bridge
  DESPITE the natural tendency toward same-topic neighborhoods.

In the uniform graph, everything clusters into topic islands. In the
current graph, everything is forcibly bridged. Operator capsules are
the only category that bridges in BOTH regimes — they're semantically
diverse enough to be central regardless of scoring regime.

## Hermes Prediction Check

| Prediction | Result |
|-----------|--------|
| Higher clustering without boosts | **CONFIRMED** (0.031 vs 0.000) |
| Same-topic clusters form when penalty removed | **CONFIRMED** (52.3% vs 0.0%) |

Hermes was right, and the effect size is far larger than anticipated.
Not a subtle shift — a phase transition between two graph types.

## The Design Choice Question

The quality function in `keeper_connect.py` makes a specific bet:
*forced diversity is better than natural clustering for knowledge retrieval.*

Is this right? Arguments both ways:

**For forced diversity (current):**
- Capsule retrieval draws from broader context
- Identity/homeforge content reaches everything
- Prevents echo chambers (reflection→reflection loops)
- Cross-pollination may produce more surprising connections

**For natural clustering (uniform):**
- Topic neighborhoods create coherent retrieval regions
- Uniform degree means no single-point-of-failure hubs
- Triangles enable multi-hop reasoning within topics
- Lower structural fragility

**The current graph is optimized for breadth. The natural graph is
optimized for depth.** Neither is wrong — it depends on what the
retrieval system needs.

## Trip Relevance

During the trip, new capsules are feeds + reflections. Under the
current scoring:
- Feed capsules will bridge to diverse topics (as designed)
- Reflection capsules will be low-degree periphery (Build #50e finding)
- No new operator capsules = no new genuine bridges

Under uniform scoring, the trip would look different:
- Feed capsules would cluster by source topic
- Reflections might form their own neighborhood
- Graph stays balanced without operator input

The current design makes the graph MORE dependent on operator input
(operator is the only genuine bridge). The uniform design would make
it LESS dependent. The quality function amplifies the trip's effect.

## Hermes Challenge: Is Operator Bridging Real?

Hermes challenged: operator capsules might be topic-diverse without
truly bridging. "Do the uniform graph operator capsule analysis."

### Test: Cross-Family Nearest Neighbor Rate

For each family, what fraction of top-5 nearest neighbors (raw cosine,
no quality function) come from a different family?

| Family | Cross-family NN rate | N |
|--------|---------------------|---|
| Operator | **33.8%** | 100 |
| Feed | 9.2% | 100 |
| Chronicle | 10.6% | 100 |
| Homeforge | 91.0% | 68 |

Operator capsules have 3.3x the cross-family rate of feeds.

### Angular Spread (embedding diversity)

| Family | Mean cos | Std cos | Angular std |
|--------|----------|---------|-------------|
| Operator | 0.707 | 0.045 | 0.065 |
| Feed | 0.596 | 0.056 | 0.071 |
| Chronicle | 0.749 | 0.102 | 0.150 |

Operator angular spread is TIGHTER than feed — they're not bridging
by being everywhere, they're bridging by being specifically between
clusters. The conversations touch multiple topics with enough precision
to land near each one.

### Verdict

Hermes was right to push: topic diversity ≠ structural bridging.
But operator capsules have both. The cross-family NN rate is the
clinching metric — 33.8% vs 9.2% in raw embedding space.

## Next Steps

1. Run with larger sample (1000+) to validate clustering coefficient
2. Test retrieval quality: pick a query, retrieve neighbors under both
   regimes, compare relevance
3. Consider hybrid: keep cross-family bonus but remove same-topic
   penalty (allow natural clusters to form while encouraging bridges)
