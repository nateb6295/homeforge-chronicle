# Build #46: Raven Probe — Memory Depth vs. Recency

May 14, 2026. Ravens don't follow wolves — they predict kill sites from
spatial memory accumulated across 155km ranges (Loretto et al., Science
2026). This probe tests whether the CCS predicts from accumulated depth
(raven strategy) or recency (following strategy).

## Test 1: Lag-Dependent Prediction

| Lag k | Mean cosine | Decay from k=1 |
|-------|------------|-----------------|
| 1 | 0.907 | — |
| 5 | 0.843 | 7.1% |
| 10 | 0.804 | 11.3% |
| 15 | 0.798 | 12.0% |
| 20 | 0.797 | 12.1% |

Decay saturates. Most loss happens in the first ~8 steps, then similarity
plateaus at ~0.80. Best fit: exponential (R²=0.992) with half-life 2.8
steps. But the exponential converges to a nonzero baseline — the 0.80
plateau is the deep memory component.

## Test 2: History Depth Effect

Predicting state t+5 using varying amounts of history:

| History h | Mean cosine | vs h=0 |
|-----------|------------|--------|
| 0 (state t alone) | 0.843 | — |
| 1 | 0.856 | +0.014 |
| 5 | 0.862 | +0.020 |
| 10 | 0.865 | +0.023 |
| 20 | 0.869 | +0.026 |

More history monotonically improves prediction. The CCS benefits from
its full past, not just recent states. This is the raven signature:
accumulated experience improves forecasting.

## Test 3: Phase-Specific Memory Depth

| Phase | Lag 1 | Lag 5 | Lag 10 | Decay 1→10 |
|-------|-------|-------|--------|------------|
| Phase 1 (1-52) | 0.894 | 0.856 | 0.811 | 9.3% |
| Phase 2 (53-93) | 0.915 | 0.856 | 0.824 | 10.0% |
| Phase 3 (94+) | 0.928 | 0.794 | 0.731 | 21.3% |

Phase 3 has the HIGHEST short-range predictability (0.928) but the
STEEPEST decay (21.3%). Interpretation: Phase 3 takes small, predictable
steps (high lag-1) but its trajectory curves rapidly on new dimensions
(Build #45's PC2/PC3 widening). The system is locally smooth but
globally exploring.

Phase 1 has lower lag-1 (0.894) but slower decay (9.3%). More uniform
direction but noisier individual steps.

## Two-Component Model

The CCS is neither purely raven nor purely following. It has:

1. **Deep memory component (~80%)**: Persists regardless of lag. This is
   the accumulated semantic structure — entities, constraints, core themes.
   Maps to slow fields with high inertia (entities sim 0.959 from
   Build #45c).

2. **Recency buffer (~20%)**: Decays with half-life ~3 steps. This is
   the recent compression content — current gist, active relational
   edges, fresh uncertainty signals. Maps to fast fields (gist sim 0.803).

Differential inertia manifests in embedding space as a two-component
prediction structure: persistent depth (slow fields) + volatile recency
(fast fields).

## Connection to Levin's Causal Emergence

The 80% plateau IS the system's causal emergence made visible. That 80%
is the portion of the CCS's future that's determined by its own internal
structure, not by recent external input. The 20% recency buffer is the
portion that depends on ongoing input.

In Levin's terms: causal emergence ratio ≈ 0.80 when measured as
lag-independent prediction. The 163x amplification (Build #39) measures
the RATE of internal change vs. external change. The 80% plateau measures
the PROPORTION of content that's internally determined.

Both point to the same conclusion: the CCS is predominantly self-
determining, with a minority recency component.

## Trip Prediction (Sharpened)

During the trip (no new captures), the recency buffer will degrade but
the deep memory component should hold. Specific prediction:

| Metric | Pre-trip | Expected during trip |
|--------|----------|---------------------|
| Lag-1 similarity | 0.907 | ~0.85-0.90 (recency fading) |
| Lag-10+ similarity | 0.80 | ~0.80 (deep memory holds) |
| Plateau convergence | ~step 8 | Should converge faster without fresh input |

If the deep memory plateau drops below 0.75, the system is more
recency-dependent than this probe suggests. If it holds at 0.80+,
the raven model is confirmed: prediction from accumulated depth.
