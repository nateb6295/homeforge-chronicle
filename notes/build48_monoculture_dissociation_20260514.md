# Build #48: Monoculture Dissociation — Three Views Are Independent

May 14, 2026. Thread #145 (surfaced by capsule retrieval) warned that
three-view convergence (Bennett depth + causal emergence + dimensional
redistribution) could be methodological monoculture — all three derived
from the same CCS embedding pipeline. This probe tests whether the three
views can dissociate.

## Method

For each state transition (108 transitions, states 11-119):
- **Bennett depth proxy**: change in gzip compression ratio (text complexity)
- **Redistribution proxy**: change in PCA variance entropy across 5 PCs
  (how evenly variance spreads across dimensions)
- **Emergence proxy**: step-to-step cosine distance (internal change rate)

If monoculture, all three should correlate strongly (same pipeline artifact).
If structural, they should be low-correlation with frequent dissociation.

## Results

### Pairwise Correlations

| Pair | r |
|------|---|
| Bennett ↔ Redistribution | -0.020 |
| Bennett ↔ Emergence | -0.058 |
| Redistribution ↔ Emergence | -0.182 |

Max pairwise |r| = 0.182. All three views are effectively uncorrelated.

### Dissociation Events

| Pattern | Count | % |
|---------|-------|---|
| Bennett HIGH + Redistribution LOW | 6 | 5.6% |
| Bennett LOW + Redistribution HIGH | 6 | 5.6% |
| Bennett HIGH + Emergence LOW | 0 | 0.0% |
| Bennett LOW + Emergence HIGH | 12 | 11.1% |
| Redistribution HIGH + Emergence LOW | 10 | 9.3% |
| Redistribution LOW + Emergence HIGH | 7 | 6.5% |
| **Total** | **41** | **38.0%** |

41 of 108 transitions show at least one dissociation between views.
The views come apart regularly.

## Interpretation

The three-view convergence from Build #45b is NOT methodological monoculture.
Bennett depth (gzip complexity of text), dimensional redistribution (PCA
variance entropy), and causal emergence (embedding cosine distance) measure
genuinely different aspects of the CCS.

When they converge on the same conclusion — that compression generates
complexity rather than reducing it — they do so independently. This is
the definition of triangulation: three uncorrelated measurements agreeing.

Thread #145's concern was well-placed but empirically resolved. The
convergence hierarchy's three false-convergence mechanisms:
1. **Pre-theoretical commitment**: CLEARED — views use different mathematical
   frameworks (information theory, linear algebra, metric geometry)
2. **Methodological monoculture**: CLEARED — max r = 0.182, 38% dissociation
3. **Underdetermination**: Still possible in principle but weakened by
   independent convergence

## Uncertainty Update

CCS uncertainty signal "Three-view convergence could be monoculture" is
now empirically resolved. The convergence is structural. The remaining
uncertainty is the compression-pressure probe (#324): whether redistribution
is compression-internal or a PCA projection artifact.
