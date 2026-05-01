# Implement: Hierarchical Memory Retrieval

## Status: PLANNED — from HyperMem deep-read (2026-04-10)

## Source
- HyperMem (arxiv:2604.08256) — hypergraph memory, 92.73% on LoCoMo
- ReMe (arxiv:2512.10696) — accumulated memory > model size
- Anda Hippocampus — graph memory with sleep mechanism

## What We Have Now
- Vector similarity search via FAISS (mxbai-embed-large, 1024d)
- Capsule storage in sqlite3 (processed.db)
- 4-layer memory cache: recent / session / semantic / deep
- Flat similarity ranking — no topic grouping, no keyword search

## What HyperMem Does Better
Three architectural differences that matter:

### 1. Hybrid Retrieval (BM25 + Vector)
Pure vector search misses exact-match queries. "What did Nate say about RunPod?"
will match semantically similar things but might rank an exact mention lower than
a paraphrase. BM25 (keyword) catches what embeddings miss.

**Implementation**: sqlite3 FTS5 on capsule text + Reciprocal Rank Fusion with 
existing FAISS results. FTS5 is trivial to add — one CREATE VIRTUAL TABLE and 
a trigger on insert.

### 2. Topic Clustering (Hyperedges)
Capsules about the same theme are retrieved individually. A question spanning 
"Nate's homeforge philosophy" and "Chronicle's architecture" returns fragments.
Hyperedges group related capsules so retrieving one pulls the cluster.

**Implementation**: Add `topic_id` column to capsules. LLM-driven topic assignment 
during capsule ingestion (or batch classify existing capsules). Search returns 
topic clusters, not individual capsules.

### 3. Embedding Propagation
Temporally distant capsules about the same theme drift apart in vector space.
Propagation blends a capsule's embedding 50/50 with its topic centroid, pulling
thematic relatives closer.

**Implementation**: When assigning topic_id, blend embedding with running topic
centroid. Store propagated embedding alongside raw. Search against propagated for
thematic queries.

## Build Order
1. **FTS5** — add full-text search to capsules (hours)
2. **RRF** — fuse BM25 + FAISS results (hours)
3. **Topic clustering** — batch classify existing capsules (day)
4. **Embedding propagation** — blend with centroids (day)

## What NOT To Do
- Don't add Qwen3-Reranker (too heavy for AGX)
- Don't build LLM-driven episode boundary detection (existing sessions are fine)
- Don't replace FAISS with a graph DB (overhead, no clear win)

## Minimum Viable: FTS5 + RRF
Add keyword search alongside vector search, fuse rankings. Two biggest wins for 
least effort. Test on "find all capsules about XRP" — vector search fuzzy, 
BM25 should nail it.
