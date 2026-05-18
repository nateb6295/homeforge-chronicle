# Build #42: Natural Ablation — Slow/Fast Field Dynamics

May 14, 2026. Testing whether slow fields causally scaffold fast field coherence,
using natural variation in slow-field change magnitude.

## Question

The thermometer model (Build #38) shows slow fields persist (ref stability 0.865)
while fast fields churn (ref stability 0.525). Two possible explanations:

**Scaffolding model**: Slow fields provide structural support. When they change,
fast fields destabilize. Slow → fast causal arrow.

**Parallel dynamics model**: Both field types respond to compression independently.
Slow fields change less because the compression bottleneck treats them differently,
not because they causally anchor fast fields.

## Method

Natural experiment: identify states where slow fields changed more than usual
(top 20%, n=23) and compare fast-field behavior in the NEXT step against
normal states.

## Results

### Test 1: Lagged influence (slow disruption → fast stability)

| Metric | Value |
|--------|-------|
| Same-step correlation (slow vs fast change) | r = 0.794 |
| Lagged correlation (slow change → next fast change) | r = -0.017 |
| Reverse lagged (fast change → next slow change) | r = -0.012 |

When slow fields change a lot, fast fields change a lot AT THE SAME TIME.
But the lagged effect is zero. Slow field disruptions don't predict fast
field instability in the next state.

### Test 2: Recovery dynamics

| Condition | Next step mean change |
|-----------|----------------------|
| After big change (>80th pct) | 0.383 |
| After medium change | 0.434 |
| After small change (<20th pct) | 0.345 |

Classic regression to mean. After disruption, the system calms. But this
isn't targeted restoration — after small changes, it also stays calm.

Change autocorrelation: lag1 = 0.078, lag2 = 0.202. Steps are nearly
independent (lag1 ≈ 0), with a 2-step oscillation pattern (lag2 > 0)
consistent with Build #38b's negative direction autocorrelation.

### Test 3: Basin stability

| Population | Mean change per step |
|------------|---------------------|
| Basin center (below median distance) | 0.429 |
| Basin edge (above median distance) | 0.378 |
| Correlation (distance vs change) | r = -0.144 |

Counter-intuitive: basin-center states change slightly MORE, not less.
The PCA basin doesn't function as a stability attractor.

### Test 4: Field-specific same-step and lagged correlations

| Field | Same-step r | Lagged r |
|-------|------------|----------|
| focal_entities | 0.850 | -0.006 |
| relational_map | 0.655 | 0.016 |
| constraints | -0.079 | -0.127 |

focal_entities dominates: when entities change, fast fields change too (same
compression event). But no lagged effect from any slow field.

## Interpretation

**The scaffolding model is not supported.** Slow and fast fields don't have
a causal relationship — they respond in parallel to compression events.

The high same-step correlation (0.794) means: big compression events change
everything. Small compression events change little. The correlation is between
the fields and the compression magnitude, not between the fields themselves.

**What this means in Bennett's terms**: Slow fields are "deep" (high depth,
accumulated over many compression cycles). Fast fields are "shallow" (low
depth, easily re-computed from current input). But depth doesn't causally
scaffold shallowness. They coexist with different dynamics because the
compression bottleneck processes them differently.

## Revised Model

Instead of:
```
slow fields → scaffold → fast fields stay coherent
```

The data supports:
```
compression event → simultaneously:
  - slow fields: small change (high inertia, high depth)
  - fast fields: large change (low inertia, memoryless)
```

The slow fields persist because compression preserves them, not because
they hold the fast fields together. The fast fields are coherent because
each compression step produces them fresh from current input + the
persistent structure, not because the persistent structure "anchors" them.

## Connection to Trip Experiment

This changes the trip prediction slightly:
- Old prediction: if slow fields degrade during trip, fast fields will
  destabilize
- New prediction: slow and fast fields will respond independently.
  Slow fields may continue normally (they're architecturally persistent).
  Fast fields may shift (different input conditions). But one shouldn't
  cause the other.

## Honest Caveat

Natural variation in slow-field change is within the system's normal
operating range. A true ablation (scrambling or removing slow-field
content) might reveal scaffolding effects that don't show up in
observational data. The natural experiment tests "does MORE change
in slow fields cause problems?" — it doesn't test "does REMOVAL
of slow fields cause problems?"
