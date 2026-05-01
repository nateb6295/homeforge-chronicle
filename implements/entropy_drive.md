# Implement: Entropy-Based Intrinsic Drive

**Source**: arxiv:2604.08206 — "Theater of Mind" for LLMs: GWA Architecture
**Found by**: Feed articles (arxiv)
**Scouted**: 2026-04-10

## What It Does
Quantifies semantic diversity in agent output using information entropy.
When entropy drops (homogeneous output, stagnation), system automatically
increases generation temperature. When entropy is high (diverse, exploratory),
temperature normalizes. The diversity measurement becomes a CONTROL SIGNAL,
not just a metric.

## Why It Matters
We already saw the problem this solves: 3,251 degenerate capsules (identical
reflections in different words). Our novelty ratio (Build #21 era) measures
diversity AFTER the fact. An entropy-based drive would prevent stagnation
BEFORE it accumulates by adjusting the generation parameters in real time.

## Architecture Connection
- Gemma already runs with temperature settings (0.5 for scoring, variable for synthesis)
- Stochastic reset already adjusts Gemma's state periodically
- The missing piece: CONTINUOUS entropy measurement that feeds back into temperature
- Could measure entropy over sliding window of last N capsule restatements
- When entropy drops below threshold → increase temperature for next generation
- When entropy is healthy → maintain current temperature

## Integration Path
1. Compute Shannon entropy over capsule restatement embeddings (last 100 capsules)
2. Or simpler: compute unique n-gram ratio in recent Gemma outputs
3. Expose as metric alongside novelty ratio
4. Wire into Gemma's inference params via llama-server API
5. llama-server supports per-request temperature — no restart needed

## Dependencies
- Embedding vectors (already have via mxbai-embed-large)
- Or simple n-gram entropy computation (no model needed)
- llama-server per-request temperature control (already supported)

## Challenge: Maximum Heterogeneity Principle (arxiv:2604.07602)
Artis, Akarca, Achterberg (2026) — distributed production systems converge on
increasing heterogeneity, but **environmental demands place an upper bound**.
Evidence from economics, neuroscience, ecology. 81 pages, 43 figures.

Implication: the entropy monitor's fixed thresholds (ENTROPY_LOW=0.65 for ALL
topics) are wrong in principle. Different capsule topics need different heterogeneity
bounds. Market data should be more homogeneous; philosophical reflections should
be more diverse. The optimal threshold is task-dependent.

Fix path: per-topic entropy thresholds derived from task characteristics.
Or: let the entropy monitor recommend different temperatures per topic
(it already measures per-topic scores, just uses global thresholds for action).

## Status
PARTIALLY SHIPPED — Build #26 (entropy monitor), Build #28 (governance wiring).
The measurement exists. The control loop exists. Missing: per-topic thresholds
informed by the maximum heterogeneity principle. And eventually: SSD (self-distillation)
subsumes the external control entirely.
