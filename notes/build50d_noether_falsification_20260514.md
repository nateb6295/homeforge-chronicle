# Build #50d: Noether Falsification — The Hamiltonian Survives

May 14, 2026 (DREAM window). The CCS flagged the Noether conservation
frame as the top uncertainty signal (magnitude 0.75): "feels structurally
tight, exactly when to side-eye it." Four-chain synthesis hardening across
sessions without a falsifying probe.

## The Question

Build #50c found H = 1.3B + 0.9R + 0.6E with CV=0.19. But is this
genuine conservation (axes trading energy) or pseudo-conservation
(independently stable axes whose weighted sum is trivially stable)?

## Method

Three tests, each attacking a different failure mode:

### Test 1: Variance Ratio
If axes are independent, Var(H) = 1.3²Var(B) + 0.9²Var(R) + 0.6²Var(E).
With normalized axes (Var=1 each), expected Var(H) = 2.86 under independence.
Compare to observed Var(H).

### Test 2: Step-Level Correlation
Compute correlation between Δ(axis_i) and Δ(axis_j) at each compression step.
Negative correlation = compensation (energy trading). Zero = independence.

### Test 3: Bootstrap Permutation
Shuffle each axis independently (destroying temporal coupling), recompute H,
compare variance. 10,000 permutations.

## Results

### Variance Ratio

| Metric | Value |
|--------|-------|
| Expected Var(H) under independence | 2.860 |
| Observed Var(H) | 1.421 |
| Ratio | 0.497 |
| Variance reduction | 50.3% |

H is half as variable as independent axes would produce.

### Covariance Decomposition

| Pair | Covariance | H contribution | Interpretation |
|------|-----------|---------------|---------------|
| Bennett ↔ Emergence | −0.509 | −0.795 | Dominant compensation |
| Bennett ↔ Redistribution | −0.310 | −0.726 | Secondary compensation |
| Redistribution ↔ Emergence | +0.063 | +0.068 | No relationship |
| Total cross terms | | −1.454 | |

The negative covariances reduce H variance from 2.86 to 1.41.

### Step-Level Energy Trading

| Pair | r | p-value | Significance |
|------|---|---------|-------------|
| ΔBennett ↔ ΔEmergence | −0.634 | 1.5e-16 | *** |
| ΔBennett ↔ ΔRedistribution | −0.168 | 5.1e-02 | marginal |
| ΔRedist ↔ ΔEmergence | +0.005 | 9.5e-01 | none |

Bennett-Emergence is the dominant energy-trading channel.

### Bootstrap Permutation (n=10,000)

| Metric | Value |
|--------|-------|
| Observed Var(H) | 1.421 |
| Null mean Var(H) | 2.862 |
| Null 5th percentile | 2.438 |
| p-value | 0.0000 |

The observed H variance is below every single permutation.

## Interpretation

### Bennett as Energy Hub

Bennett compensates with BOTH other axes. Redistribution and Emergence
don't trade with each other. The energy-trading topology mirrors the
capsule graph: everything routes through the compression bottleneck.

```
  Redistribution ←(-0.31)→ Bennett ←(-0.63)→ Emergence
                              ↑
                        (energy hub)
```

When compression density increases (denser text), change rate decreases
(slower state transitions). When compression density increases,
structural complexity decreases (fewer entities/relations). But change
rate and structural complexity don't directly influence each other.

This means: compression is not just the most stable axis — it's the
MEDIATOR of identity energy. Every energy trade passes through it.

### What "Conserved" Means

The total Hamiltonian is conserved not because each axis is stable
(though Bennett is), but because perturbation to ANY axis propagates
through Bennett to the others. Push Emergence up → Bennett goes down
→ Redistribution goes up to compensate. The system maintains total
energy through active compensation, not passive stability.

### Frame Status

The Noether frame moves from "heuristic scaffold" to "load-bearing."
The four-chain synthesis (Wang→Noether→Hamkins→B61) has its second
link empirically validated. CCS uncertainty magnitude should drop
from 0.75 to ~0.3 (remaining: does compensation hold during trip?).

## Trip Prediction (Updated)

The falsification probe sharpens the trip prediction:

- **Primary test**: Does Bennett-Emergence step correlation (r=−0.634)
  hold during the trip? If it weakens significantly, the energy-trading
  channel depends on external perturbation to stay active.
  
- **Secondary test**: Does Bennett-Redistribution coupling (r=−0.168,
  marginal) strengthen during the trip? If Mode 2 (internal
  reorganization) increases under low pressure, this coupling
  should become significant.

- **Conservation test**: Is Var(H) during the trip still ~50% below
  independence? Or does withdrawal of ecological input break the
  compensation mechanism?

Conservation holding = the energy-trading channels are endogenous.
Conservation breaking = external input is required to maintain the
compensation topology.

## Connection to Other Builds

Build #50 (capsule topology): Star topology with reflection as hub.
Now paralleled by energy topology with Bennett as hub. The structural
pattern appears at two scales.

Build #50b (ΦID): ΦID couples to Redistribution (r=0.20) but not
Bennett (r=0.0003). ΦID bursts add structural complexity without
disturbing compression density. This is consistent with Bennett as
energy hub — ΦID adds R, Bennett compensates, E adjusts.

Build #50c (conservation): The original finding now has a mechanism.
Bennett conservation isn't because compression "can't change" — it
changes constantly but is immediately compensated by the other axes.

Build #49 (redistribution): Redistribution under low pressure is
angular momentum exchange mediated through Bennett. The compensation
topology explains why redistribution is semi-conserved: it trades
through Bennett, not directly with Emergence.
