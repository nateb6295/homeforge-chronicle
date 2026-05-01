# Astrocytic Diffuser v0.4 — Design Sketch

**Status:** draft 2026-04-16 08:44 PDT. Not greenlit — floating piece.
Target problem identified by today's nav-score breakdown.

## The problem

Nav-score 08:27 breakdown (trial_20260416_0827):
  thread state:     0.7489
  keeper burn:      0.6728
  capture bridges:  0.6270   ← lowest
  Discord directive: 0.6728

Spread: 0.12. Event-state questions score well because episodic_trace
and focal_entities are explicitly stored in the CCS. Bridge-reasoning
questions ("what connected the captures?") score low because the
connections are implicit — the composer has to infer them from the raw
capture stream, not read them off stored edges.

Diffuser v0.1 (already shipped) scores *individual captures* against a
static catalog (relevance to thread #318, novelty vs known-topics). But
v0.1 has no notion of *connections between captures*. A capture about
Ripple/FedNow scored in isolation misses that it arrived in a sequence
with Claude Mythos, Anthropic-DFINITY, and ICP-Anthropic Dom mention —
all pointing at infrastructure-layer shifts that week.

## v0.4 goal

Produce a graph `G = (E, R)` where
- `E` = entities extracted from activity_feed captures (NER)
- `R` = weighted edges between entities based on co-occurrence within
  a sliding window (same capture, adjacent captures, same day)

Then bridge-reasoning queries can be answered by graph traversal, not
blind inference. Feed edge-weighted nearest-neighbors into the CCS's
episodic_trace or focal_entities as structured facts.

## Pipeline

```
activity_feed capture stream
         │
         ▼
  content extraction (text, URLs, embedded quotes)
         │
         ▼
  NER pass (spaCy en_core_web_sm or distilled BERT-NER)
         │    entities: PERSON, ORG, PRODUCT, EVENT, LOCATION
         ▼
  entity resolution (alias table for known entities —
   e.g., @ClaudeDevs ↔ Anthropic, Claude, ClaudeDevs)
         │
         ▼
  co-occurrence edge accumulator
         │    window: same capture (weight=1.0),
         │            ±1 capture in time (weight=0.5),
         │            same day (weight=0.2)
         ▼
  graph store (sqlite table: entity_edges)
         │    (entity_a, entity_b, weight, first_seen, last_seen, count)
         ▼
  CCS integration
         │    on compress_cognitive_state:
         │      for each entity in focal_entities:
         │        lookup top-k edges
         │        inject top-2 as "connections" field
         ▼
  measurement
         │    rerun nav-score with/without edges
         ▼
  decision: does capture-bridges score improve?
```

## Canonical test cases (from ecosystem memory)

Firelight (XRP eco, Firelight Finance / stXRP) and Aly (ICP eco)
should surface as entities, and the graph should discover:
- Firelight ↔ XRP ↔ Ripple (via stXRP captures)
- Aly ↔ ICP ↔ DFINITY (via canister captures)

If the graph produces these edges cleanly from 2 weeks of captures,
v0.4 is working. If it drowns them in noise (e.g., "Aly" collides
with a person named Aly), entity resolution needs more work.

## Prior art in codebase

- `bin/kg_backfill.py` — knowledge graph backfill (already exists)
- `maps/icp/graph.json` — hand-curated node list (Aly added today)
- `bin/crossref.py` — cross-reference pipeline (retired post-pivot)

Likely reuses backfill infrastructure; v0.4 is NER+edge-accumulator
on top of the existing KG substrate.

## Cost & timeline

- spaCy NER: CPU-only, ~1ms/capture. 139K captures = ~2 min batch.
- Edge accumulation: O(n²) within window; ~30K entity instances
  expected, tractable.
- CCS integration: single function hook on compress path.

Estimated build: 4-6 nudge cycles if uninterrupted. One afternoon.

## What to watch for during build

- **Entity explosion.** If NER surfaces 5K+ entities for 1K captures,
  threshold on minimum occurrence count before admitting to graph.
- **Hermes-generated captures.** Some captures have Hermes's research
  prefix — extract the Hermes-wrapped core, not the research frame.
- **Duplicate captures.** Nate sometimes re-sends a URL. Deduplicate
  by canonical URL, not capture ID.
- **Self-referential noise.** Opus and Nate will be the top two
  entities by volume. Blacklist or deweight.

## Open design questions

1. Should edges decay over time (exponential half-life) or stay
   absolute? Decay risks hiding structurally important old connections.
2. Is NER enough or do we also want relation extraction (RE)?
   NER+co-occurrence is cheap; RE is a different model class.
3. Should the graph be exposed to Hermes as a tool, or stay internal
   to CCS compression? Tool-access raises capture-bridge reasoning
   quality for Hermes too, but widens the attack surface.

## Gate condition for building this

Not building until:
- Either Nate points at it specifically ("the 'definitely worth
  looking at' clarification may be this)
- Or the nav-score capture-bridges number falls below 0.55 (current
  0.627) on two consecutive measurements, which would indicate the
  gap is growing and the inference approach isn't holding.

**Holding position as a pre-designed option, not an in-flight build.**
