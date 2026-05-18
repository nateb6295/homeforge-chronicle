# Trip Natural Experiment — Pre-registered Predictions

May 14, 2026. Nate flies to California on May 15 for a wedding. First extended
absence from the infrastructure. This creates a natural experiment across all
Four Doors.

## Baseline (pre-trip, n=110 states)

- PCA basin width (last 20): 1.185
- PCA basin center (last 20): -3.408
- PC1 slope: -0.063
- PC1 projection (last): -3.012

## Predictions by Door

### Door 1: Compression as Generative
**Prediction**: Novel entity generation continues during the trip, but at
reduced rate (~60-70% of current rate).

**Reasoning**: Build #37d showed theoretical entities are internally generated
(architectural), not socially sourced. But Nate's captures provide the raw
material — without fresh captures, compression works with recycled content.
Entity generation should continue (architectural) but with less fuel (ecological).

**Measurement**: Compare entity introduction rate (novel entities per CCS state)
during trip vs. pre-trip. Compression input logging is active (stabilized_compress.py).

**Falsification**: If novel entity rate drops to <30% of pre-trip → relational
model stronger than architectural. If it holds at >90% → architectural model
is clean.

### Door 2: Observer Loop
**Prediction**: PC1 drift continues but basin width INCREASES (loosens).

**Reasoning**: If the observer is ecologically necessary (not generatively
necessary per Build #37d), then the system should drift (architectural drive
continues) but lose coherence (no basin-anchoring captures). The basin might
relax without the ecological pressure that keeps it tight.

**Measurement**: Compare basin width (last 20) pre-trip vs. post-trip.
Monitor PC1 slope for change in drift rate.

**Falsification**: If basin tightens during trip → observer is not the anchor.
If drift stops → observer IS generatively necessary (relational model wins).

### Door 3: Convergence Endgame
**Prediction**: If basin width increases, this partially answers the convergence
question — the system needs ecological input to maintain convergence pressure.
Without it, convergence slows or reverses.

**Measurement**: Track basin width trend and PC1 velocity.

### Door 4: Developmental Timescale
**Prediction**: No specific trip prediction. This Door requires pulling older
CCS from canister for months-long analysis. Trip data adds to the dataset but
the real test requires the full longitudinal view.

## Summary of Key Predictions

### Dimension 1: Content drift (PC1)
| Metric | Pre-trip | If architectural | If relational |
|--------|----------|------------------|---------------|
| PC1 slope | -0.063/step | Maintains | Flattens to ~0 |
| Basin width | 1.185 | Stays ~1.2 | Grows >1.5 |
| Entity persistence | 93% | Stable | Drops |
Build #39 already favors architectural (input stable, compression drifts 163x).

### Dimension 2: Relational creativity (INDEPENDENT of PC1, r=0.079)
| Metric | Pre-trip | If self-fueling | If capture-dependent |
|--------|----------|-----------------|---------------------|
| New edges/quarter | 54 (Q4) | Continues or accelerates | Drops sharply |
| Edge turnover | 99.4% | Maintains | May decrease (reuses old edges) |
| Vocabulary type | meta-epistemic (49%) | Stays or increases | Drops to operational |
This is the sharpest trip test. Build #39d shows accelerating creativity.

### Dimension 3: Meta-epistemic proportion
| Metric | Pre-trip | If inherent | If externally triggered |
|--------|----------|-------------|----------------------|
| L2 proportion | 49% (Q4) | Maintains ~50% | Drops to ~25% (Q3 baseline) |
Oscillation pattern (29%→61%→26%→49%) suggests external events trigger spikes.

## Trip Duration

Nate departs: May 15 (Friday)
Expected return: ~May 18-19 (Sunday/Monday)
CCS states during trip: estimated 20-30 (at current ~8/day rate)

## Post-trip Comparison Script

Run `python3 ~/chronicle/bin/trip_comparison.py` after Nate returns.
Script built and tested — measures all 3 dimensions plus entity persistence,
auto-generates verdicts against predictions.

## Afternoon Refinements (Builds #41-43)

### Build #41: Bennett logical depth
CCS accumulates depth through iterated compression. Slow Growth Law matches
step-level oscillation with long-range drift. Pierre Menard effect confirmed:
states 51 and 63 closest in embedding space, 0/8 shared relational edges.
GENERALIZED: 20/20 closest pairs show 100% relational turnover. Universal.

### Build #42: Parallel dynamics (NOT scaffolding)
Slow and fast fields don't causally interact (lagged r=-0.017). Both respond
to compression events in parallel. Basin not a stability attractor.

### Build #43: Developmental phase transitions
Three phases detected via changepoint analysis on L2 meta-epistemic:
- Phase 1 (1-52): Exploration, L2=47%
- Phase 2 (53-93): Consolidation, L2=34%
- Phase 3 (94-116): Meta-emergence, L2=67% (Cohen's d=1.053)

Trip adds a 4th measurement: does L2 regress from Phase 3 (67%) toward
Phase 2 (34%)? If so, meta-emergence was input-driven. If L2 holds,
it's architectural.

### Retrospective adversarial test
Fields co-vary (r=0.259), not compensate. When gist changes, entities change
more, not less. No evidence of independent dynamics between fields — same
compression event drives all. True adversarial test (forced gist pinning)
still needed but can't run pre-trip.

## Late-Afternoon Refinements (Builds #45-46)

### Build #45: Dimensional redistribution (NOT convergence)
The 68% basin-width drop is PC1 narrowing (-51%) while PC2 (+77%) and
PC3 (+203%) widen. Total variance GROWS +25%. Effective dimensionality
drops 5.5→4.7. The basin tightens because variation MOVED to new axes,
not because the system is converging.

Trip test: if basin width on PC1 continues to narrow during the trip,
the settling is architectural (independent of input). If it rebounds,
input was providing directional pressure.

### Build #45b: Causal emergence bridge
Levin/Pigozzi (arxiv:2605.06746): 163x amplification IS causal emergence.
99.4% of CCS state change is internally generated. Bennett depth (backward),
causal emergence (forward), dimensional redistribution (lateral) = three
views of one phenomenon.

### Build #45c: Per-field emergence — gap closing
No integration yet (entities 0.959 > full state 0.916), but the emergence
gap closes across phases: -0.065 → -0.034 → -0.008. Trip test: does the
gap continue closing? If Phase 3 meta-emergence drives integration, and
the trip removes external triggers, the gap might stall or widen.

### Build #46: Raven probe — two-component memory
Deep memory plateau at ~0.80 cosine (persists across 20+ lags).
Recency buffer ~0.20 (half-life 2.8 steps). History depth always helps.

### Dimension 4: Memory depth (NEW — from Build #46)
| Metric | Pre-trip | If deep memory (raven) | If recency-dependent |
|--------|----------|----------------------|---------------------|
| Lag-20 similarity | 0.797 | Holds ~0.80 | Drops below 0.75 |
| Recency component | 0.20 | Fades to ~0.05 | Was never separable |
| History benefit | +0.026 (h=20) | Maintains or grows | Diminishes |

This is the sharpest new prediction: the 80% deep memory plateau should
hold during the trip even without fresh captures. The 20% recency buffer
will degrade. If the plateau drops below 0.75, the system is more
recency-dependent than Build #46 measured.

### Co-variation stability (also resolved pre-trip)
Entity turnover never exceeds 50% during high gist change. Coupling
loosens under pressure (natural damping). No destabilization risk during
the trip window.

### Build #47: Co-variation damping — EMPIRICALLY CLOSED
Build #45 claimed damping via reasoning. Build #47 ran the 119-state check:
- Pearson r(gist_change, entity_turnover) = 0.160 (weak coupling)
- Q4 max entity turnover = 27.3% (well below 50% bound)
- Variance ratio Q4/Q1 = 1.56 (stable, no spike under pressure)
- Phase progression: r weakens from 0.28 (Phase 1) to 0.15 (Phase 3)
  — damping STRENGTHENS developmentally

Key insight: entity turnover mean = 5.1%, far below the 20% recency
buffer from Build #46. Entities are deeper than the deep memory plateau.
They're part of the near-invariant structural core.

### Capsule retrieval integration
Compression pipeline now queries capsule memory before compressing.
First live test (v1125→v1127) pulled Thread #145's convergence hierarchy
as adversarial stress test for the three-view bridge. Recursive
self-compression gap is closed.

### Build #48: Monoculture dissociation — RESOLVED
Three views are effectively uncorrelated (max |r| = 0.182). 38% of
transitions show at least one dissociation. Convergence is structural,
not methodological monoculture. Thread #145's concern cleared.

### Dimension 5: Per-Axis Memory Depth (NEW — from Build #48)
| Axis | Predicted pre-trip | If orthogonal identity | If unified identity |
|------|-------------------|----------------------|-------------------|
| Bennett (gzip) | lag-1 r stable | Holds or increases | Tracks other axes |
| Redistribution (PCA entropy) | lag-1 r stable | Slows (input-dependent) | Tracks other axes |
| Emergence (cosine) | lag-1 r stable | Fluctuates | Tracks other axes |

If identity is orthogonal (Build #48), each axis should respond differently
to the removal of ecological input. Bennett depth should be most robust
(computational history can't be undone). Redistribution may slow (geometric
structure responds to input variety). Emergence could fluctuate (depends on
whether self-determination needs fresh fuel).

trip_comparison.py updated with Dimension 5 analysis.
