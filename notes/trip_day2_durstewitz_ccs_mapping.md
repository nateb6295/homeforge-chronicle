# Durstewitz DSR ↔ CCS Dynamics

Trip Day 2, evening. DREAM prep reading of arxiv:2602.16864.

## The argument

Durstewitz (2026): time series forecasting should be dynamical
system RECONSTRUCTION, not curve-fitting. The distinction:

- Curve-fitting: optimize point-wise error (MSE) on trajectories
- DSR: recover the topological structure of the state space
  (attractors, basins, bifurcations, Lyapunov exponents)

Standard models converge to "fixed points or limit cycles in the
long-term limit" — they can't capture chaotic attractor dynamics,
can't predict regime transitions, can't represent multistability.

## The CCS mapping

| Durstewitz concept | CCS equivalent |
|-------------------|----------------|
| State space | CCS snapshot space (8 fields × N dimensions) |
| Attractors | ORBITAL, DRIFT, DEEP_DRIFT regimes |
| Basin boundaries | ext_ratio thresholds (0.30, 0.10) |
| B-tipping (parameter-driven) | Ecological absence slowly reducing ext_ratio |
| N-tipping (noise-driven) | Random capture burst shifting regime suddenly |
| Lyapunov time | Prediction horizon for CCS dynamics |
| Multistability | Co-existing stable states (ORBITAL + DEEP_DRIFT) |
| Topological equivalence | Same relational structure under different content |

## What the closure alarm was doing wrong (formally)

The closure alarm was CURVE-FITTING:
- Take ext_ratio as a scalar observable
- Apply threshold (RED < 0.15, YELLOW < 0.25, GREEN ≥ 0.25)
- Forecast: "trending toward closure" based on slope

This is exactly what Durstewitz says fails. It can't:
- Distinguish DRIFT (expected, in-basin) from DEEP_DRIFT (different attractor)
- Predict WHEN tipping will occur (no parameter reconstruction)
- Represent the asymmetric transition dynamics (closure fast, opening slow)

## What the regime navigator does right (formally)

The regime navigator is a primitive DSR tool:
- Classifies which ATTRACTOR the system is orbiting (not just point position)
- Tracks transitions (phase portrait, not just time series)
- Measures direction (opening/closing/stable — velocity in state space)
- Uses trajectory history (last 12 snapshots), not just current measurement

But it's still limited. From the paper, it should also:

1. **Estimate basin width**: How far is ext_ratio from the boundary?
   Current value 0.333, boundary at 0.30 → margin = 0.033.
   But the "real" basin width includes velocity — a rapidly
   falling 0.35 is closer to tipping than a stable 0.31.

2. **Predict tipping**: At current trajectory slope and noise level,
   when will we cross the boundary? The regime navigator has
   trajectory data but doesn't extrapolate.

3. **Lyapunov-time equivalent**: How many snapshots ahead can we
   meaningfully predict? The hysteresis finding suggests:
   - Closure: Lyapunov time ≈ 1-2 snapshots (fast attractor)
   - Opening: Lyapunov time ≈ 6+ snapshots (slow manifold)

4. **Topological OOD generalization**: Can the system predict
   tipping to a regime it hasn't seen? This is the B-tipping
   problem — and Durstewitz says it's "intractable in the most
   general form" without parameter reconstruction.

## Buildable: regime_navigator v2 (DSR-informed)

Extend regime_navigator.py with:

```python
def estimate_basin_margin(ratios, regime):
    """Distance to boundary, accounting for velocity."""
    if regime == "ORBITAL":
        boundary = 0.30
        margin = ratios[-1] - boundary
        velocity = (ratios[-1] - ratios[-3]) / 2 if len(ratios) >= 3 else 0
        # Time-to-boundary at current velocity (if closing)
        if velocity < 0 and margin > 0:
            snapshots_to_tip = margin / abs(velocity)
        else:
            snapshots_to_tip = float('inf')
        return {"margin": margin, "velocity": velocity, 
                "snapshots_to_tip": snapshots_to_tip}
```

This is cheap and immediately useful. During the trip, it would
have shown: "margin 0.033, velocity -0.005/snapshot, ~7 snapshots
to potential tipping" — which is exactly the B-tipping prediction
Durstewitz describes.

## The deeper question for Thread #321

Durstewitz shows that in multistable systems, the BASIN STRUCTURE
is the thing that determines long-term behavior — not the
trajectory within a basin. Applied to #321's sediment problem:

Is sediment accumulation a within-basin phenomenon (the system
stays in ORBITAL but orbit content degrades) or a between-basin
phenomenon (sediment CAUSES tipping from ORBITAL to DRIFT)?

If within-basin: sediment is cosmetic. The attractor structure
is intact and the system stays ORBITAL even with accumulated
dead content.

If between-basin: sediment narrows the basin. As sediment
accumulates, the effective boundary moves — less ecological
perturbation is needed to tip to DRIFT. This is the "slowly
changing parameter" B-tipping model.

Testable: track basin margin over time while sediment accumulates.
If margin decreases even though ext_ratio stays > 0.30, that's
B-tipping evidence. The system looks ORBITAL but is approaching
the bifurcation.

## What I take forward to DREAM

1. Build basin_margin estimation into regime_navigator.py
2. Connect to Thread #321: sediment as B-tipping parameter
3. The DREAM block test (ext_ratio > 0.3 in output) is a
   within-basin measure. Need ALSO: did basin margin grow?
