# Implement: SemHash Semantic Deduplication

## Status: STOLEN → Build #25

## Source
- GitHub: MinishLab/semhash
- PyPI: `pip install semhash` (works on aarch64)
- Dependencies: model2vec, vicinity (ANN), frozendict, usearch, simsimd

## What
Fast semantic deduplication using Model2Vec static embeddings + Vicinity ANN search.
Replaces O(n²) pairwise cosine comparison with O(n log n) approximate nearest neighbor clustering.

## Results
- 2,775 reflection capsules → 2,374 in 2.4 seconds
- 403 capsules superseded (14.5% duplicate ratio in reflections)
- HN feed: 554 capsules, only 11 dupes (2%) — confirms the problem is Gemma reflections, not the pipeline
- Model: `minishlab/potion-base-8M` (8M param static embeddings, ~30MB)

## Integration
- Created: `chronicle/bin/capsule_dedup.py` (scan/apply/stats)
- Uses SemHash's self_deduplicate with configurable threshold (default 0.85)
- Supersedes duplicates via existing `superseded_at`/`superseded_by` columns

## Architecture Notes
- Model2Vec produces static embeddings (no inference, just lookup table) — extremely fast on CPU
- Vicinity wraps HNSW/Usearch for ANN search — sub-linear scaling
- Could potentially replace our FAISS index management for some use cases
- The potion-base-8M model is generic — for Chronicle-specific dedup, our nomic-embed-text vectors in capsule_embeddings might be more domain-aware, but Model2Vec is 100x faster

## Next
- Schedule periodic dedup runs (cron or post-heartbeat)
- Test on other topics with known redundancy
- Consider feeding existing nomic embeddings directly into Vicinity for domain-specific ANN dedup
