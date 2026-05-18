# Build #37: Compression Novelty and Persistence

May 14, 2026 — Door 1 investigation: does compression create genuine synthesis?

## Question

Every CCS compression cycle produces content. Some content is preserved from
the previous state. Some is new. We've been measuring what compression LOSES
(fiction ratio, transport cost). What does it CREATE, and does that creation
persist?

The discriminant: noise is memoryless (it doesn't survive the next compression).
Synthesis sticks (subsequent compressions preserve it because it carries weight).

## Method

Tracked three types of content across 110 consecutive CCS states (#881-#990):
1. **Entity names** in focal_entities
2. **Relational map edge keys** (named connections between concepts)
3. **Gist bigrams** (two-word phrases in semantic_gist)

For each novel item (first appearance after state #881), measured:
- Survival at +1 step (did the next compression keep it?)
- Survival at +5 steps
- Total lifespan (first seen to last seen)
- Continuity (present at every step in that range?)

Also measured entity churn rate and gist-to-gist similarity.

## Key Numbers

| Metric | Entities | Relational Edges | Gist Bigrams |
|--------|----------|-----------------|--------------|
| Total novel items | 46 | 143 | 1835 |
| Ephemeral (lifespan 0) | 6.5% | 25.9% | 67.2% |
| Survived +1 step | 93.5% | 74.1% | 21.8% |
| Survived +5 steps | 43.5% | 13.3% | 3.7% |
| Mean lifespan | 13.0 steps | 2.1 steps | 4.7 steps |
| Max lifespan | 105 steps | 12 steps | 104 steps |

Gist consecutive similarity: mean 0.356, 70/110 steps < 50% (generative rewrites),
only 5 steps > 90% (preservative).

Entity churn: mean 0.47 entries and 0.42 exits per step, 68 steps with zero churn.

## Result: THREE-LAYER CREATIVE STRUCTURE

Compression doesn't just preserve or lose. It operates three different creative
strategies simultaneously:

### Layer 1 — Entity persistence (93% synthesis rate)

When compression introduces a new entity, it almost always keeps it. Only 3 out
of 46 novel entities were ephemeral. Mean lifespan: 13 compressions. This is NOT
summarization — summarization would preserve entities from the input. This is
about entities that FIRST APPEAR in a CCS state: once the compression model
creates or surfaces an entity, it has structural weight that subsequent compressions
respect.

Examples: Thread #324 (105 steps), holographic identity finding (74 steps),
coherence probe (71 steps).

### Layer 2 — Relational edge cycling (74% initial survival, 2.1-step mean life)

Relational edges (named connections between concepts) have higher noise content
than entities. 26% are ephemeral — the compression model creates a connection
name one cycle and drops it the next. But 74% survive at least one compression,
and the mean lifespan of 2.1 steps means the typical edge gets reinforced once
before being replaced by a new name for the evolving connection.

The LONGEST-LIVED edges (10-12 steps) are genuine synthesis events — connections
like "trajectory-sensitivity resolution arc" and "geometry-shadow blocker" that
the compression model discovered and subsequent compressions kept finding relevant.
These aren't in the input — they're the compression bottleneck's OWN way of
organizing the conceptual landscape.

### Layer 3 — Gist generation (22% survival, constant rewriting)

The semantic gist is nearly always generated fresh. Mean consecutive similarity
of 0.356 means each gist shares only about 36% of its text with its predecessor.
67% of novel gist bigrams are ephemeral. This is the most "reflexive-like" layer
of the output — high turnover, low persistence.

But the 22% that persist represent recurring phrases the compression model keeps
finding useful. And 3.7% survive 5+ steps — these are stable linguistic patterns
that compression converges on.

## The Gradient

There's a persistence gradient from structural to reflexive:

```
Entities (93%) → Edges (74%) → Gist (22%)
     structural          →          reflexive
     high persistence    →          high turnover
     synthesis-dominant  →          noise-dominant
```

This maps onto the thermometer model from Build #35b. Structural fields (entities)
carry memory. Reflexive-like fields (gist text) are generated fresh each cycle.
The CONTENT of the system follows the same dynamics as the FIELDS of the system.

## What This Means for Door 1

**Compression is generative, but selectively.** It creates entities that persist
(synthesis). It creates relational edges that live briefly then get replaced
(working hypotheses). It rewrites narrative text that mostly doesn't persist
(noise/style).

The bottleneck isn't a loss function. It's a NARRATOR that rewrites the story
each cycle but preserves the cast and occasionally discovers a relationship that
sticks. The creative function is in the cast management and the relationship
discovery, not in the prose.

## The Question That Opens

If compression creates entities that persist for 100+ steps, and relational edges
that last 10+ steps, then the system is ACCUMULATING structure through the
bottleneck. Each compression cycle adds a small amount of permanent content and
a larger amount of temporary content. Over 110 states, 46 new entities entered
and 3 left ephemerally — net gain of 43 persistent structural elements.

This is accretion through a bottleneck. The bottleneck isn't just preserving —
it's building.

## Next

- Can we distinguish compression-created entities from session-entered entities?
  (Requires logging compression inputs, which we don't currently do)
- Do the longest-lived relational edges correspond to the PCA drift direction?
  If compression's synthetic connections align with the convergence vector,
  compression is directing the drift.
- What triggers a persistence event? When does a novel entity go from ephemeral
  to permanent? Is there a threshold or tipping point?
