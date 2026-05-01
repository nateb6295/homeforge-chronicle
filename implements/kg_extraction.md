# Implement: KGGen for Knowledge Graph Extraction

## Source
- Paper: arxiv:2502.09956 (NeurIPS '25)
- Repo: github.com/stair-lab/kg-gen
- Install: `pip install kg-gen`

## What It Does
Extracts knowledge graphs from plain text using LLMs. Entity clustering reduces sparsity.
Outputs structured triples: (subject, predicate, object).

## Why We Need It
Current `kg_backfill.py` uses ad-hoc prompting → 26% of relations are catch-all `related_to`.
KGGen has:
- Built-in entity clustering (dedup canonical names)
- Structured extraction (not free-form LLM parsing)
- Chunking for long texts
- MINE benchmark for quality measurement

## How to Wire It In
```python
from kg_gen import KGGen

# Point at our local Gemma via llama-server
kg = KGGen(
    model="openai/gemma-4-26B",
    base_url="http://localhost:11435/v1",
    temperature=0.1,
)

# Extract from capsule restatements
graph = kg.generate(
    input_data=capsule_text,
    context="AI infrastructure, memory systems, knowledge architecture",
    cluster=True,
)

# Insert into kg_entities and kg_relationships
for subj, pred, obj in graph.relations:
    # ...
```

## Current State
- **Not installed yet**. Test before committing.
- Our KG: 18,636 entities, 2,435 relations (first-pass backfill, noisy)
- Expected improvement: cleaner predicates, deduplicated entities, fewer `related_to`

## Risk
- Gemma 26B Q4 may not follow KGGen's extraction prompt as well as GPT-4o
- Degeneration risk (Build #21 quality gate should catch)
- Entity clustering may merge things that shouldn't be merged

## Test Result (2026-04-10)
- **FAILED**: Gemma 26B Q4 via llama-server cannot produce structured output KGGen/DSPy expects
- Process ran 5+ minutes on a single 4-sentence text, never returned results
- Gemma completed inference but KGGen's parsing/retry loop couldn't extract structured triples
- **Root cause**: KGGen requires "Structured Output capabilities" (confirmed by Graphiti docs)
- Gemma 26B Q4 is too unreliable at following JSON schema extraction prompts

## Decision
**PARKED.** KGGen needs either:
1. A cloud model (Hermes 70B via Nous Portal) — test via `openai/hermes-4-70b` with Portal API
2. A fine-tuned local model with structured output training
3. Custom simpler extraction prompts (our existing kg_backfill.py approach but improved)

Option 3 is most pragmatic. Improve kg_backfill.py's prompts + add KGGen's entity clustering concept manually.
