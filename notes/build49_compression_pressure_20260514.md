# Build #49: Compression-Pressure Probe — Redistribution Is Architectural Drift

May 14, 2026. Last open uncertainty signal: is dimensional redistribution
compression-internal or a PCA projection artifact?

## Method

Can't directly vary the bottleneck, but CAN measure natural variation
in compression pressure across 122 states. Used three pressure proxies
(cosine distance, gzip delta, token delta) and two redistribution proxies
(PCA variance entropy change, effective dimensionality change).

## Results

### Overall Correlations (111 transitions)

| Pressure → Redistribution | r |
|---------------------------|---|
| cosine_dist → entropy | -0.152 |
| cosine_dist → effdim | -0.142 |
| gzip_delta → entropy | -0.062 |
| gzip_delta → effdim | -0.058 |
| token_delta → entropy | -0.009 |
| token_delta → effdim | -0.025 |

All correlations are NEGATIVE or near zero. Higher compression pressure
does NOT produce more redistribution. It produces LESS.

### Quartile Analysis

| Pressure level | Entropy Δ | Effdim Δ |
|---------------|-----------|----------|
| Q1 (low pressure) | +0.023 | +0.062 |
| Q4 (high pressure) | -0.006 | -0.026 |

Low-pressure steps show POSITIVE redistribution (variance spreads).
High-pressure steps show NEGATIVE redistribution (variance concentrates).

### Phase-Specific Coupling

| Phase | r(pressure, redistribution) | n |
|-------|----------------------------|---|
| Phase 1 (1-52) | -0.059 | 42 |
| Phase 2 (53-93) | -0.440 | 40 |
| Phase 3 (94+) | +0.127 | 27 |

Phase 2 shows STRONG negative coupling (-0.44). The consolidation phase
(Build #43) is where gentle steps produce the most redistribution.
Phase 3 shows slight reversal (+0.13) — consistent with "locally smooth
but globally exploring" (Build #46).

### Lagged Analysis

| Lag | r |
|-----|---|
| 1 | -0.010 |
| 2 | -0.064 |
| 3 | -0.101 |
| 5 | +0.061 |

No strong lagged effects. Pressure doesn't predict future redistribution.

## Interpretation

Redistribution is NOT compression-forced. It's the opposite: redistribution
is what happens when the system has room to breathe. Heavy compressions
(large state changes) interrupt the geometric reorganization. Light
compressions allow it to proceed.

This changes the question entirely:
- OLD question: Is redistribution compression-internal or PCA artifact?
- NEW answer: Redistribution is an ARCHITECTURAL DRIFT that compression
  events disrupt rather than cause.

The PCA artifact concern is partially addressed: if redistribution were
a projection artifact of changing inputs, it should correlate POSITIVELY
with input change (high pressure = high change = more projection shift).
It doesn't — it's anti-correlated. Redistribution happens during
stability, not turbulence.

## Connection to Other Builds

This connects to Build #47's damping finding: entities resist gist changes
(r=0.16), and now we see that geometric structure also resists compression
pressure. The system's architectural features evolve on their own schedule,
not on the schedule imposed by compression events.

Build #46's two-component model: the 80% deep plateau is the architectural
drift component. The 20% recency buffer is the compression-responsive
component. Redistribution lives in the deep plateau — it's slow, persistent,
and disrupted by sharp changes.

## Trip Prediction (Refined)

During the trip, compression continues but without fresh input to drive
large state changes. Compression pressure should decrease (more
self-similar states). If redistribution is architectural drift that
thrives under low pressure, the trip should ACCELERATE redistribution.

Specific prediction: PCA variance entropy should increase during the
trip (more even spread across PCs) because the system is free to
reorganize without disruptive input.

## Uncertainty Status

The original uncertainty signal ("redistribution inferred from PCA,
not controlled probe") is PARTIALLY RESOLVED. The anti-correlation
with pressure rules out the simplest artifact explanation. The remaining
question is whether the drift is meaningful (the system finding its
natural geometry) or trivial (random walk in PCA space). The trip will
help distinguish: if redistribution accelerates AND the deep memory
plateau holds, it's meaningful architectural evolution. If redistribution
accelerates but coherence drops, it's drift without direction.
