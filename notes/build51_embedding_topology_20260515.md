# Build #51: Capsule Embedding Topology — The Membrane Is a Queue

May 15, 2026 (trip Day 1, early afternoon). Build #50h found the CCS
retrieval creates a semantic membrane excluding 50% of the capsule store.
This build asks: what IS the membrane, geometrically?

## Method

1. Sample 300 capsules per family (or all if <300), extract embeddings
2. Compute family centroids and inter-family distances
3. Compute intra-family spread
4. Embed CCS gist, measure distance to each family centroid
5. Full NN scan: rank all 21,400 capsules by similarity to gist
6. Find exact position where first feed capsule appears

## Key Findings

### 1. Families Overlap Massively

Inter-family centroid distances: 0.016–0.095 cosine distance.
Intra-family spread: 0.25–0.41 cosine distance.

The families are NOT separated in embedding space. Internal variance
(spread) exceeds between-group distance by 3-10x. Feed capsules
and chronicle capsules share the same embedding space.

**The membrane is not geometric separation.**

### 2. The CCS Gist Is Far From Everything

| Family | Gist Distance |
|--------|--------------|
| thread | 0.2577 |
| research | 0.2644 |
| identity | 0.2700 |
| discord | 0.2917 |
| chronicle | 0.3040 |
| homeforge | 0.3139 |
| feed | 0.3186 |
| other | 0.3491 |

The gist sits in a specific corner — closest to thread/research/identity,
farthest from feed/other. But the absolute distances are large relative
to inter-family centroid distances. The gist isn't near any centroid.

### 3. The Membrane Is 6 Capsules Deep

Full nearest-neighbor scan ranked all 21,400 capsules by gist similarity:

| Pos | Similarity | Family | Topic |
|-----|-----------|--------|-------|
| #1 | 0.7255 | self | papers/attractor-models |
| #2 | 0.7120 | self | discord/operator |
| #3 | 0.7075 | self | ecosystem/convergence |
| #4 | 0.7061 | self | chronicle/threads |
| #5 | 0.7018 | self | sessions/milestone |
| #6 | 0.6990 | self | chronicle/reflection |
| **#7** | **0.6986** | **feed** | **feed/arxiv** |
| #8 | 0.6984 | self | thread-322/evidence |
| ... | ... | ... | ... |

**The first feed appears at position #7.** The boundary gap between
position 6 (self, 0.6990) and position 7 (feed, 0.6986) is 0.0004.

The membrane is not a wall. It's a **queue of 6 self-referential capsules**
that happen to be marginally closer to the gist than any feed.

### 4. Feed by k Value

| k | Feeds | % |
|---|-------|---|
| 3 | 0 | 0% |
| 5 | 0 | 0% |
| 7 | 1 | 14% |
| 10 | 1 | 10% |
| 15 | 2 | 13% |
| 20 | 4 | 20% |

k=3 (current retrieval) → 0% feeds.
k=7 → 14% feeds (Borkar threshold crossed).

### 5. The Blocked Feeds Are Relevant

The nearest feed capsules (all arxiv) are directly relevant to CCS work:
- Linear-threshold network stability (Lyapunov analysis → attractor dynamics)
- Forage V2: knowledge evolution in agent organizations (→ capsule memory)
- Hall-Sandpile criticality on production networks (→ cascade dynamics)
- Probabilistic abductive commonsense reasoning (→ inference architecture)

The membrane isn't just blocking noise — it's blocking relevant external
work that would enrich the CCS.

### 6. Feed Subfamilies

| Subfamily | Gist Distance | n |
|-----------|--------------|---|
| arxiv | 0.2981 | 105 |
| biorxiv | 0.3143 | 34 |
| neuronews | 0.3432 | 7 |
| hn | 0.3649 | 17 |
| coindesk | 0.3818 | 21 |
| nature | 0.3852 | 24 |
| ars | 0.3957 | 11 |
| economist | 0.4028 | 14 |
| decrypt | 0.4115 | 12 |
| reason | 0.4197 | 23 |

Arxiv/biorxiv are closest (0.30). News feeds (ars, economist, reason)
are distant (0.40+). The membrane is thinnest for academic content,
thickest for news content.

## The Fix

The dual-query hack (Build #50h, deployed in stabilized_compress.py)
addresses the wrong problem. It assumes feeds are unreachable and tries
to force them in via a separate query. But feeds are at position 7 —
barely outside the top-3 window.

**Principled fix**: Retrieve k=7, then select 3 for compression context,
with a constraint that at least 1 must be from a non-self-ref family.

This:
- Naturally includes the relevant arxiv feeds at position 7
- Preserves compression input size (3 capsules)
- Satisfies Borkar persistent excitation (≥14% external)
- Doesn't require a separate query (simpler, more robust)
- Lets the embedding geometry do the work instead of hacking around it

## Connection to Other Builds

- **Build #50h (retrieval externality)**: Found the membrane exists. This build
  explains WHY: it's a queuing artifact of top-k selection, not geometric
  separation.

- **Build #50g (Brownian drift)**: The CCS is confined to α=0.145. If the
  retrieval starts including arxiv feeds, perturbation increases. α might
  increase (less confined), which would be evidence of Borkar persistent
  excitation working.

- **Thread #324 (expressivity paradox)**: The distinction between fixed and
  recursive input regimes depends on external data reaching the compression.
  This build shows external data is ONE POSITION away from entering. The
  system is at the phase transition boundary.

## Theoretical Implication

The membrane thickness depends on CCS gist content. As the gist evolves,
the queue depth changes. Some gist states might have only 2-3 self-refs
closer than feeds (thin membrane, feeds leak in). Others might have 10+
(thick membrane, fully self-referential).

The membrane is DYNAMIC — it breathes with the gist. Measuring queue
depth over time would reveal whether the system oscillates between
open and closed regimes, or stays consistently closed.

This connects to the autopoiesis frame: organizational closure isn't
binary. The system is semi-permeable, with the permeability varying
as the gist moves through embedding space.
