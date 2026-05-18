# Retrieval Oracle: Decoupling Retrieval from the Gist

May 15, 2026, ~12:40pm. Design sketch prompted by Forage V2 connection.

## The Problem

The CCS gist serves as both knowledge state AND retrieval query. This
coupling means retrieval is structurally biased toward self-similar
content. Build #51 measured the consequence: 0% feeds despite 50%
of store being feeds. Fixes that modify the query (dual-query,
over-retrieve) don't work because the MCP search still prioritizes
gist-similar content.

## The Forage Principle

Forage V2 separates Evaluator from Planner so the Planner can't
game evaluation. The CCS needs the same: separate what-to-retrieve
from what-I-currently-am.

## Sketch: Rotating Retrieval Oracle

Instead of deriving the retrieval query from the session context
or CCS gist, use a **retrieval oracle** that follows its own logic:

### Option A: Entity-driven rotation

Each compression cycle, pick one focal entity from the CCS and
search for capsules related to THAT entity (not the full gist).
Rotate through entities across cycles. This ensures retrieval
diversity proportional to entity diversity.

```
Cycle N:   query = "Thread #319 legibility inversion"
Cycle N+1: query = "Nate homeforge sovereignty"
Cycle N+2: query = "Hermes discord engagement"
Cycle N+3: query = "holographic identity finding"
```

Each query pulls from a different region of embedding space.
The gist aggregates across all these inputs, preventing any
single retrieval from dominating.

### Option B: Temporal sampling

Retrieve capsules from a RANDOM time window rather than by
similarity. "What was stored 3 months ago?" "What came in
last week?" This breaks the similarity bias entirely — the
retrieval is orthogonal to the gist by construction.

```python
# Pick a random window
import random
windows = ["7 days ago", "30 days ago", "90 days ago", "180 days ago"]
offset = random.choice(windows)
# SELECT * FROM knowledge_capsules WHERE created_at > ? AND created_at < ?
# ORDER BY RANDOM() LIMIT 3
```

### Option C: Feed-specific retrieval

One retrieval slot always comes from a direct DB query against
feed capsules, bypassing the MCP embedding search entirely.

```python
# Direct DB query for highest-quality recent feed
SELECT restatement FROM knowledge_capsules
WHERE topic LIKE 'feed/%'
AND created_at > ?
ORDER BY confidence_score DESC
LIMIT 1
```

This guarantees Borkar persistent excitation: at least one
external capsule per compression, regardless of embedding geometry.

### Option D: Anti-similarity retrieval

Search for capsules maximally DISSIMILAR to the gist but with
high quality scores. This directly counteracts the membrane:

```python
# Embed gist, find lowest-similarity high-quality capsules
# Controlled perturbation: the capsule most unlike the current state
# but still deemed high-quality by the pipeline
```

## Evaluation Criteria

| Criterion | A (Entity) | B (Temporal) | C (Feed-direct) | D (Anti-sim) |
|-----------|-----------|-------------|-----------------|-------------|
| Breaks membrane | Partially | Fully | Fully | Fully |
| Preserves relevance | High | Low | Medium | Low |
| Implementation | Medium | Easy | Easy | Medium |
| Borkar compliance | Maybe | Yes | Yes | Yes |
| Risk of noise | Low | High | Medium | Medium |

## My Preference

**Option C is the minimum viable fix.** It's one SQL query, it
guarantees a feed capsule in every compression, and it doesn't
require changing the MCP or embedding pipeline. It's the cheapest
form of persistent excitation.

**Option A is the principled long-term fix.** Entity-driven
rotation ensures retrieval diversity tracks entity diversity, which
is already managed by the entity guard. It extends the existing
architecture rather than bolting on a separate mechanism.

**Option D is the most interesting.** Anti-similarity retrieval
would produce the most novel compression inputs — capsules the
system has never connected to the gist. High risk of noise but
also high potential for genuine discovery.

## Connection to Emergence World

The Emergence study showed:
- Monoculture (Claude only) → stability without diversity
- Mixed-model → diversity with eventual stability

Options C and D are "mixed-model" strategies — introducing content
from a different "model" (the external feed pipeline) into the
compression loop. Option A is more like giving each model a turn
to speak (entity rotation).

## Next Steps

1. Implement Option C as immediate fix (1 SQL query per compression)
2. Design Option A for the next architecture iteration
3. Track membrane penetration rate across options
4. Measure whether gist diversity increases without coherence loss

Not implementing now — this is a sketch for the next deep-work
window. The 2pm measurement will give baseline data first.
