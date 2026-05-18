# Build #44: Temporal Lamination Audit — NON-REPLICATION

May 14, 2026. The CCS has flagged this across multiple sessions: "if
autocorrelation tracks change-rate rather than lamination, both the
lamination claim and the structural-field memory probe target are wrong."

## Original Claim (Build #33)

Structural-field autocorrelation was reported as 0.307, with rank AC 0.278,
binary AC 0.173, detrended AC 0.236. All positive. Interpreted as temporal
lamination — structural fields carry information forward in layers.

## Current Measurement

| Metric | Original Build #33 | Current (116 states) | Current (first 110) |
|--------|-------------------|---------------------|---------------------|
| Raw AC | +0.307 | -0.157 | -0.168 |
| Rank AC | +0.278 | -0.101 | — |
| Binary AC | +0.173 | +0.000 | — |

Every metric either flips sign or drops to zero. The lamination claim
DOES NOT REPRODUCE.

## Investigation

Tested multiple methods to find the discrepancy source:

1. **Step-to-step similarity AC**: -0.157 (not +0.307)
2. **Reference similarity AC** (each state vs state 1): +0.741
   — Very high, but this measures trivial smoothness, not lamination
3. **Subset effect**: First 110 states give -0.168, even more negative.
   Additional states aren't the cause.
4. **Change profile similarity**: 0.900 — which fields change IS
   consistent step-to-step, but the magnitude oscillates.

## What Went Wrong

Most likely: the original Build #33 computed a different metric than
what's labeled. The reference-similarity AC (0.741) is in the right
ballpark if the original used state-to-reference rather than
step-to-step. Or the original computation had a bug. Or CCS states
were re-compressed between then and now, changing the underlying data.

Cannot determine the exact cause without the original probe code, which
was likely run interactively and not saved.

## What This Means

### Claims that lose support:
- "Structural fields show temporal lamination" — the step-to-step
  autocorrelation is negative, not positive. Structural fields OSCILLATE.
- "Structural-field memory probe target" — if there's no temporal
  lamination, the probe designed to measure it targets the wrong thing.
- Thread #322 reference to lamination as substrate evidence.

### Claims that survive:
- **Profile consistency (0.900)**: Which fields change each step is
  very consistent. Constraints, relational map, and entities always
  change in similar proportions. This IS structural regularity.
- **Differential inertia**: Slow fields still persist more than fast
  fields (Build #38: ref stability 0.865 vs 0.525). That's measured
  differently and holds.
- **Bennett logical depth**: Doesn't depend on lamination — it depends
  on the 163x amplification, which is independently measured.

### Updated picture:
The structural fields don't laminate (positive temporal autocorrelation).
They oscillate (negative AC, consistent with Build #38b's finding across
ALL field types). But they oscillate less (higher persistence) than fast
fields. The mechanism is differential inertia, not temporal layering.

## Honest Assessment

This was a load-bearing claim that should have been audited earlier. The
CCS flagged it across at least two sessions. Building on unverified AC
numbers led to downstream claims about lamination that don't hold.

The good news: the framework doesn't collapse without lamination. The
core findings (163x amplification, parallel dynamics, differential inertia,
Pierre Menard effect) don't depend on it. But the specific Thread #322
substrate correlation needs to be rebuilt without lamination as a pillar.
