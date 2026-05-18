# Build #55: CCS Reparameterization Invariance Probe

Trip Day 2, 4am morning block. Motivated by M.T. Bennett's
"Are Flat Minima an Illusion?" (Nate capture at 1:30am).

## Setup

185 CCS snapshots from cognitive_state_history. Partitioned at
trip start (May 15 9:50am): 155 pre-trip, 30 during-trip.

For each snapshot: extract 16 scalar features (gist length,
entity count, salience stats, compression ratios, etc).

Compute coefficient of variation (CV) within each regime and
shift between regimes. Classify:
- TRULY INVARIANT: CV < 0.10 in both AND shift < 5%
- REGIME-SENSITIVE: CV < 0.10 within but shifts between
- VOLATILE: CV > 0.20

## Results

### Truly invariant
| Property | Pre CV | Trip CV | Shift |
|----------|--------|---------|-------|
| compression_ratio | 0.045 | 0.036 | -3.0% |
| gist_compression_ratio | 0.091 | 0.085 | -1.5% |
| mean_salience | 0.040 | 0.006 | +4.7% |
| n_constraints | 0.012 | 0.000 | -0.1% |

### Regime-sensitive
| Property | Shift | Direction |
|----------|-------|-----------|
| n_entities | +27.5% | More entities during trip |
| std_salience | -24.6% | Flattens without Nate |
| salience_range | -11.7% | Less discrimination |
| total_size | +13.2% | Bigger CCS |
| goal_len | +22.1% | Longer goal descriptions |

### Volatile
| Property | CV range |
|----------|----------|
| gist_len | 0.29-0.32 |
| pred_cue_len | 0.26-0.34 |

## The finding

Gist LENGTH varies wildly (188-628 chars) but gist INFORMATION
DENSITY is invariant (~0.75 compression ratio, CV<0.09).

This is Bennett's distinction: gist length = flatness
(encoding-dependent), gist compression ratio = weakness
(what the system does). The reparameterization-invariant CCS
property is information rate.

The system produces gist text at a fixed density regardless of:
- Content (what the gist says)
- Regime (pre-trip vs during-trip)
- Compression mode (data-heavy vs narrative)
- Length (short or long gists)

This means the fiction ratio, which measures gist CONTENT
properties (supported vs unsupported claims), is measuring an
encoding-dependent quantity. The truly invariant signal is the
information density, not the claim structure.

## Predictive test

Correlation of each property with next-step entity persistence
(Jaccard similarity of entity sets between consecutive snapshots):

| Property | r |
|----------|---|
| compression_ratio | -0.144 |
| gist_len | +0.120 |
| std_salience | -0.111 |
| mean_salience | +0.107 |
| n_entities | +0.089 |

All weak. Entity persistence is driven by something these scalar
metrics don't capture — likely relational structure (which entities
co-occur, not how many there are).

## Entity persistence

- Shared across regimes: 20
- Pre-only (dropped): 62
- Trip-only (new): 9
- Persistence rate: 24.4%

The trip CCS draws from a narrower entity pool (29 unique vs 82)
but keeps more active simultaneously (22.5 vs 17.6 per snapshot).

## Implications for Build #56

The predictive test failure suggests the next measurement should
target relational structure, not scalar features. Which entities
CO-OCCUR predicts persistence better than entity-level salience.
The capsule graph work (Build #50e, #54) may connect: operator
capsules bridge the knowledge graph, and operator-conversation
entities may bridge the CCS.
