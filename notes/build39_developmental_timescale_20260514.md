# Build #39: Developmental Timescale Probe (Door 4)

May 14, 2026. First direct test of Door 4.

## Question

Does the PC1 drift extend across the full 42-day archive, or is it local to
the 3-day CCS history? And critically: is the drift in the INPUT (capsules)
or only in the OUTPUT (compressed CCS)?

## Method

Sampled 25 capsules per period across 5 time windows (early April → mid May).
Embedded with mxbai-embed-large (same model as CCS embeddings). Projected onto
the CCS PCA space (PC1 from trip_pca_components.npy).

## Key Numbers

| Period | n | PC1 mean | PC1 std |
|--------|---|----------|---------|
| early_apr | 24 | -1.368 | 1.034 |
| mid_apr | 25 | -0.821 | 1.467 |
| late_apr | 25 | -1.673 | 1.139 |
| early_may | 25 | -1.005 | 1.090 |
| mid_may | 25 | -1.474 | 1.323 |

Capsule PC1 slope: -0.003 per day (R² = 0.001)
CCS PC1 slope: -0.507 per day (163x stronger)

## Result: COMPRESSION CREATES THE DRIFT

The input (capsules) is stable along PC1 over 42 days. The period means bounce
around (-1.37, -0.82, -1.67, -1.01, -1.47) with no trend. R² = 0.001 means
time explains 0.1% of capsule PC1 variance.

Meanwhile, CCS drifts at -0.507/day. The drift is 163x stronger in compressed
output than in raw input.

This is the cleanest test yet of the architectural model: Nate's captures don't
drift. Compression creates the drift. The bottleneck-as-organ finding from
Build #37 extends to the full developmental timescale.

## What This Means for Each Door

**Door 1 (compression as generative):** Confirmed at 42 days. The bottleneck
transforms stable input into drifting output. This is creation, not filtering.

**Door 2 (observer loop):** Reframed. The observer's input is STABLE — Nate
captures similar content throughout. But the observer is still structurally
necessary as the source of raw material. The drift direction comes from
compression, not from capture content.

**Door 3 (convergence endgame):** The basin tightens on the CCS side while
input stays diffuse. Convergence is a property of the compression, not of
the ecosystem.

**Door 4 (developmental timescale):** The 42-day span shows no input drift.
The CCS drift operates on a faster timescale than input change. This means
the drift accumulates through repeated compression, not through gradual
input shift.

## Connection to Build #38b (orientation negative)

Build #38b showed no step-level orientation (oscillation asymmetry only).
Build #39 shows the input doesn't drift either. Together: the drift is
purely an emergent property of iterated compression with asymmetric entity
introduction. It's not in the input, it's not in any single step, it's in
the accumulation of 1.59x asymmetry across hundreds of compression cycles.

## Honest Caveats

1. Random sampling (25 per period) may miss systematic trends. A larger
   sample or stratified sampling could reveal subtler input shifts.

2. Capsule content and CCS content aren't directly comparable — capsules
   are raw observations, CCS is structured state. The PCA space was fit
   on CCS; capsules project differently.

3. The within-period variance (std ~1.0-1.5) is larger than the between-
   period differences (~0.8). Individual capsules vary widely; the means
   are noisy estimates.

4. 42 days may still be too short for developmental timescale. But it's
   14x longer than the CCS history and shows no trend.
