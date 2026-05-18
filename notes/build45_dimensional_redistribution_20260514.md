# Build #45: Dimensional Redistribution — The Basin Mechanism

May 14, 2026. Testing whether #324's basin-width tightening (68% drop from
Build #39b, 51% confirmed here with 117 states) is explained by information
loss under iterated compression.

## The Setup

Build #42 ruled out scaffolding (slow/fast fields don't causally interact).
Build #44 ruled out lamination (autocorrelation is negative, not positive).
The 68% basin-width drop on PC1 is real but mechanism-free. This probe
tests the simplest remaining hypothesis: lossy compression → information
loss → embedding vectors cluster → basin tightens.

## Information Trends (n=117)

| Metric | First 20 | Last 20 | Change | Direction |
|--------|----------|---------|--------|-----------|
| Total tokens | 714 | 866 | +21% | INCREASING |
| Unique tokens | 392 | 427 | +9% | INCREASING |
| Type-token ratio | 0.55 | 0.49 | -11% | DECREASING |
| Entities | 14.7 | 19.1 | +30% | INCREASING |
| Relational edges | 4.1 | 4.7 | +15% | INCREASING |
| Gist length | 414 | 342 | -17% | DECREASING |

**Information is GROWING, not shrinking.** The simple information-loss cascade
is wrong. More entities, more edges, more text. Only gist is getting shorter
(compression getting tighter on this specific field) and type-token ratio is
declining (vocabulary becoming more repetitive).

## The Surprise: Semantic Divergence

| Window | Mean pairwise cosine |
|--------|---------------------|
| Early 20 | 0.849 |
| Late 20 | 0.789 |

Late states are LESS similar to each other than early states. The basin is
tightening but the states are diverging. This eliminates semantic convergence
as the mechanism.

## The Finding: Dimensional Redistribution

Basin width by principal component:

| PC | Early 20 width | Late 20 width | Change |
|----|----------------|---------------|--------|
| PC1 | 2.966 | 1.451 | -51% |
| PC2 | 1.217 | 2.151 | +77% |
| PC3 | 0.901 | 2.727 | +203% |
| PC4 | 1.649 | 1.646 | 0% |
| PC5 | 1.793 | 1.329 | -26% |

**Total embedding variance: +24.8% (early 39.7, late 49.6)**
**Effective dimensionality: 5.5 → 4.7 (DECREASING)**

The system isn't converging. It's REDISTRIBUTING variation. PC1 (the historical
dominant axis) is narrowing. PC2 and PC3 are widening dramatically. Total
variance is growing. The variation is moving from the old axis to new axes.

## What This Means for #324

The 68% basin-width drop doesn't need a "tightening mechanism." It needs a
"dimensional development" explanation:

1. Early CCS varies mostly along one big axis (content drift toward abstraction)
2. As the system matures, new independent axes of variation emerge
3. The old PC1 axis settles while new axes grow
4. Basin width on PC1 drops because the action has moved elsewhere

This is consistent with Build #39d: content drift and relational creativity
are orthogonal (r=0.079). PCA is showing the geometry of that orthogonality.
The system developed multiple independent axes of variation, and PC1 captures
a diminishing fraction of total variance.

## Connection to Differential Inertia

The dimensional redistribution pattern is what differential inertia looks like
in embedding space. Slow fields (high inertia) dominate PC1 — they vary less
step-to-step, so PC1 narrows. Fast fields (low inertia) drive PC2/PC3 — they
change more, so these axes widen. The two field classes aren't coupled (Build #42)
but they project onto different PCs.

## Mechanisms Ruled Out for Basin Tightening

1. ~~Scaffolding (slow fields anchor fast)~~ — Build #42, lagged r=-0.017
2. ~~Temporal lamination~~ — Build #44, AC sign flip to -0.157
3. ~~Information loss cascade~~ — Build #45, info is GROWING (+21% tokens)
4. ~~Semantic convergence~~ — Build #45, cosine similarity DECREASING (-0.06)

## Mechanism Found

**Dimensional redistribution**: total variance grows while concentrating on
fewer effective dimensions. The old dominant axis settles as new axes emerge.
Basin tightening on PC1 is a signature of dimensional development, not
convergence or constraint.

## Residual Test

After controlling for total_tokens, basin-width trend slope is -0.022 (still
tightening). The tightening is not explained by information volume alone.
Only n_rel_edges shows meaningful correlation with basin width (r=-0.509),
suggesting relational map growth contributes to PC1 settling.

## Honest Caveat

PCA is computed on the full dataset, so early/late splits are projected
onto the same global axes. If the underlying semantic directions shifted
substantially, the same PC1 axis might not mean the same thing in early
vs. late states. A sliding-window PCA with alignment would be more robust.
But the dimensional redistribution pattern is strong enough to be the
primary explanation even if alignment drift contributes.
