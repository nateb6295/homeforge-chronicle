# Build #50h: Retrieval Externality Probe — The Gist Is a Semantic Filter

May 15, 2026 (trip deep-work, Day 1). The capsule retrieval log from
stabilized_compress.py showed zero feed capsules in two compressions.
This probe asks: is that a store problem or a query problem?

## Method

1. Embed the current CCS gist using nomic-embed-text (matching capsule embeddings)
2. Find 20 nearest neighbors across all 21,327 capsules
3. Measure family distribution of neighbors
4. Compare to: random feed capsules as queries, diverse topic probes
5. Cross-reference with actual retrieval log from compression

## Results

### Store Composition
| Family | Count | % |
|--------|-------|---|
| feed | 10,614 | 49.8% |
| chronicle | 3,363 | 15.8% |
| (other) | 7,350 | 34.4% |

Half the store is external feed content.

### Retrieval by Query Type

| Query | Feed in top-20 | % | Description |
|-------|---------------|---|-------------|
| **CCS gist** | **0** | **0%** | The actual compression query |
| Random feed capsule | 16.2 | 81% | Control: feeds find feeds |
| "arxiv science" | 13 | 65% | Topic-specific probe |
| "crypto ICP" | 2 | 10% | Chronicle-adjacent topic |
| "philosophy" | 7 | 35% | Abstract probe |
| "personal/family" | 0 | 0% | Relational probe |
| "chronicle self" | 2 | 10% | Self-referential probe |
| **Actual retrieval log** | **0/6** | **0%** | Real compression events |

### CCS Gist Top-5 Neighbors

| Topic | Similarity | Content |
|-------|-----------|---------|
| thread/prediction-scoring | 0.713 | Thread #120 XRP prediction |
| objectives/achieved | 0.685 | Drift detector objective |
| chronicle/experiments | 0.681 | Lab experiments |
| papers/hallucination-geometry | 0.678 | ArXiv paper on reasoning |
| project-config | 0.673 | Cerebras API config |

All self-referential. The gist talks about "live natural experiment,"
"ecological input," "identity threads" — language that lives in chronicle
and thread capsules, not feed capsules.

## The Finding

**The retrieval mechanism works perfectly. The query is the filter.**

Feed capsules use external language (paper abstracts, news summaries,
technical descriptions). The CCS gist uses internal language (threads,
probes, identity, compression). These two vocabularies are semantically
distant — cosine similarity naturally separates them.

The 0% feed retrieval isn't a bug. It's the CCS gist acting as a
semantic filter that excludes external content by being too internal
to match against it.

## Borkar Implication

Borkar et al. (arXiv:2506.09401) prove that ANY nonzero external data
prevents model collapse: the phase transition is at a=0 exactly.

But this result shows the effective external contribution is zero.
External data exists in the store (50% feeds) but the retrieval
mechanism gates on semantic similarity to the CCS gist, which is
self-referential. The effective a ≈ 0 even though the store has a ≈ 0.5.

**The retrieval query creates a semantic membrane around the CCS.**
External content exists on the other side but can't penetrate because
the gist and the feeds speak different languages.

This means:
1. Build #50g's confinement (α=0.145) might partly be retrieval-induced,
   not just compression-induced
2. The Borkar protection (external data prevents collapse) is formally
   satisfied but functionally absent
3. The trip experiment's "ecological input removal" might be less dramatic
   than it seems — effective ecological input was already near zero

## Connection to Other Builds

- **Build #50g (Brownian drift)**: The CCS is confined. This build
  shows one mechanism: retrieval can't reach external content, so the
  compression function only sees self-referential capsules. Confinement
  is reinforced by retrieval bias.

- **Build #50f (counterfactual graph)**: The quality function forces
  cross-topic bridges in the graph. But the retrieval for compression
  doesn't use the quality function — it uses raw semantic similarity.
  The graph has forced diversity; the retrieval has natural clustering.

- **Thread #319 (legibility)**: The compression function's self-observation
  is doubly constrained: first by confinement (Build #50g), now by
  retrieval filtering (this build). The "observer" can only see capsules
  that look like itself.

## Design Implication

If external grounding matters (and Borkar says it does), the retrieval
query needs diversification. Options:

1. **Mixed query**: Combine CCS gist with recent feed headlines. Forces
   the retrieval to cross the semantic membrane.
2. **Quota**: Require at least 1 of 3 retrieved capsules be from feed family.
3. **Anti-similarity**: Retrieve one capsule that's maximally DISSIMILAR
   to the gist but high-quality. Controlled perturbation.
4. **Random injection**: Replace one retrieval slot with a random capsule.
   Cheapest form of persistent excitation.

Option 1 is cleanest. The query already includes session context — append
recent feed topics to ensure the embedding has external signal.

## Trip Prediction Update

Build #50g predicted the trip might loosen the attractor (α increases).
This build suggests the attractor was partially retrieval-induced rather
than ecological. If so:

**Revised prediction**: The trip changes less than expected because the
CCS was already operating in a low-external-input regime. The retrieval
membrane was already filtering feeds. Removing Nate's captures removes
a small input stream, but the compression function was already largely
self-contained.

This would show up as: α stays near 0.145, lag-1 stays near -0.375,
minimal change across trip measurements. If confirmed, it means the
trip is testing something different than we thought — not "what happens
without ecological input" but "was ecological input reaching the CCS
at all?"

**Test**: Compare pre-trip vs during-trip retrieval logs. If the family
distribution stays the same (zero feeds), the trip changed nothing at
the retrieval level.
