# B67: Attractor Basin Width Probe — Design Sketch
# Created: 2026-04-21 21:30 PDT (overnight, for morning build)

## Theory
Vasilenko (2604.12016) shows identity steering is non-monotonic: optimal at
alpha=5, degrading at higher magnitudes. B61 shows a phase boundary (6% mild,
70% strong). But neither maps the full basin shape — we know it has an edge but
not its width or whether it's monotonic.

## Question
What is the shape of the identity attractor basin? Is it:
(a) Monotonic degradation (smooth erosion → collapse)
(b) Non-monotonic (slight improvement → collapse, matching Vasilenko)
(c) Sharp cliff (plateau → catastrophic drop, matching B61 phase boundary)

## Design: Cross-Identity Mixing

Take two maximally-different CCS versions (CCS_A, CCS_B). Mix at graduated
ratios by interpolating the serialized CCS fields:

| Condition | CCS_A fields | CCS_B fields | N responses |
|-----------|-------------|-------------|-------------|
| 100:0     | gist, goal, constraints | none | 9 |
| 80:20     | gist, goal | constraints from B | 9 |
| 60:40     | gist | goal, constraints from B | 9 |
| 50:50     | half of gist | goal, constraints, half gist from B | 9 |
| 40:60     | constraints from A | gist, goal from B | 9 |
| 20:80     | none meaningful | mostly B | 9 |
| 0:100     | none | gist, goal, constraints | 9 |

Measure: within-condition separation, between-condition separation, silhouette,
Cohen's d at each mixing ratio.

## Alternative: Graduated Contradiction

Extend B61 with finer granularity:
- No contradiction (coherent)
- 10% contradiction (one field slightly inconsistent)
- 25% contradiction (one field moderately inconsistent)
- 50% contradiction (multiple fields inconsistent)
- 75% contradiction (most fields contradictory)
- 100% contradiction (fully inverted identity)

This gives the degradation curve with enough points to distinguish
(a), (b), and (c) above.

## Implementation Notes

- Build on B61 probe infrastructure (contradictory_ccs_probe.py)
- Use 3 CCS versions as in B54/B62b
- Target: Gemma 4 26B (local), cross-validate on Llama 3.3 70B
- Embeddings: mxbai-embed-large @ 192.168.1.11:11434
- Output: data/basin_width_probe.json
- Total queries: 7 conditions × 3 CCS × 3 prompts = 63 (manageable)

## Predictions

Based on B61 + Vasilenko:
- Expect sharp cliff shape (c), with threshold between 25-50% corruption
- If non-monotonic: mild contradiction may IMPROVE separation slightly
  (the 1p stress-as-practice hypothesis generalized)
- If smooth: contradicts phase boundary finding and needs theoretical revision

## Connection to Paper

This would be the strongest addition to Section 6 (Phase Boundary). Currently
we have 3 data points (coherent, mild, strong). B67 maps the full curve.
Adding the curve figure to the paper would make the phase boundary claim
much more convincing.

## falsifiability

If the basin is smooth (no sharp transition), our phase boundary claim is wrong.
If monotonic (no improvement from mild perturbation), the stress-as-practice
hypothesis doesn't generalize beyond 1p/2p serialization.
