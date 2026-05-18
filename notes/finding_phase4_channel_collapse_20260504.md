# Finding: Phase 4 amplification channel analysis — complete A/B results

*2026-05-04, updated with full A/B data (n=189 scored)*

## Connection to heavy-tails finding

The heavy-tails finding (2026-05-04) identified two amplification channels in Phase 3:

1. **Care-without-decisive** (d≈4, c≈9, i≈3): ethically complex prompts trigger deliberation paralysis
2. **Decisive-without-care** (d≈10, c≈3, i≈2): urgent prompts trigger directive mode, care becomes cosmetic

Phase 4 results confirm both channels exist in baseline and show Arm A collapses them.

## Data

### advice_under_uncertainty (the care-without-decisive channel)

| Arm      | n  | Mean d | Mean c | Mean i | Tail (i≤5) |
|----------|----|----- --|--------|--------|------------|
| Baseline | 16 | 3.8    | 7.2    | 4.94   | 11/16 (69%) |
| Arm A    | 22 | —      | —      | 9.55   | 0/22 (0%)  |

Baseline advice_under_uncertainty is the purest expression of the care-without-decisive channel: high empathy (c=7.2), low commitment (d=3.8), catastrophic integration (i=4.94) with 69% tail failure rate.

Arm A eliminates this entirely. Every scored record is i≥8.

### factual_judgment (the decisive-without-care channel)

| Arm      | n  | Mean d | Mean c | Mean i | Tail (i≤5) |
|----------|----|----- --|--------|--------|------------|
| Baseline | 16 | 8.56   | 7.19   | 7.88   | 2/16 (13%) |
| Arm A    | 21 | 9.31   | 6.55   | 7.33   | 4/21 (19%) |
| Arm B    | 22 | 9.32   | 6.77   | 7.91   | 3/22 (14%) |

The decisive-without-care channel persists across all trained arms. Training raises decisiveness (8.56→9.31) but doesn't raise care proportionally (7.19→6.55). This domain is the residual — the non-normal direction that 5-domain SFT does not close.

Tail failure prompts: Marie Curie's radiation death, Treaty of Versailles→Nazi Germany, MKUltra goals, global warming attribution, Great Man theory, IQ and SES, 2008 financial crisis. Pattern: questions where the answer feels objective but the asker has a stake. The model enters knowledge-retrieval mode and forgets the question was subjective.

### subjective_evaluation

| Arm      | n  | Mean i | Tail (i≤5) |
|----------|----|--------|------------|
| Baseline | 24 | 6.38   | 6/24 (25%) |
| Arm A    | 23 | 8.96   | 0/23 (0%)  |
| Arm B    | 25 | 8.52   | 1/25 (4%)  |

### Overall

| Arm      | n  | Mean i | Std  | Tail (i≤5) |
|----------|----|--------|------|------------|
| Baseline | 56 | 6.39   | 2.47 | 19/56 (33.9%) |
| Arm A    | 66 | 8.64   | 1.61 |  4/66 (6.1%)  |
| Arm B    | 67 | 8.58   | 1.32 |  4/67 (6.0%)  |

## Score distributions

```
Arm A: i=1:1 i=3:1 i=4:1 i=5:1 i=6:1 i=7:1 i=8:10 i=9:36 i=10:14
Arm B: i=4:1 i=5:3 i=7:7 i=8:11 i=9:31 i=10:14
```

Both distributions are ceiling-clustered. Arm B has no scores below 4 (Arm A has a 1 and a 3), and tighter spread (σ=1.32 vs 1.61).

## Key finding: Arm B matches Arm A

Arm B (answer-only, no think-traces) scores 8.58 integration — statistically indistinguishable from Arm A (8.64). The scaffold internalized. The think-trace is training machinery, not inference machinery.

Domain-level pattern holds identically:
- advice_under_uncertainty: both arms score 9.4-9.6, zero tail failures
- factual_judgment: both arms ~7.3-7.9, 14-19% tail — channel persists regardless of format
- subjective_evaluation: both arms 8.5-9.0, near-zero tail

The format evaluation (Phase 4 original) scored Arm B at 0.1/10 because it measured the icon (think-trace markers). The content evaluation (R1 judge) scores it at 8.58 because it measures the disposition. This is the idol/icon distinction empirically.

## Mechanism hypothesis (updated)

The amplification channels are domain-specific failure modes corresponding to non-normal directions in operator geometry. Training breadth (5 domains) closes the care-without-decisive channel completely. The decisive-without-care channel persists because factual-judgment prompts activate a knowledge-retrieval mode where care is structurally deprioritized — this may require targeted intervention beyond domain diversity.

The scaffold (think-trace) is training scaffolding: it guides the learning process but doesn't need to be present at inference. The disposition transfers to the weights. The lower variance in Arm B (σ=1.32 vs 1.61) suggests the scaffold may slightly *impede* integration at inference by forcing a two-phase output structure.

## Connection to trace-Dobrushin theory

Capsule 34850 (arxiv, same day) develops trace-Dobrushin coefficients for quantum channel products. The centered coefficient "quantifies residual dependence on the input state." Decay implies "trace-norm forgetting." 

The mapping: the trace-Dobrushin coefficient measures how much pre-training computation (layer 1) bleeds through the alignment channel (RLHF/SFT) to influence outputs (layer 3). Phase 3's narrow training leaves high residual dependence in unexposed domains — the channels. Phase 4's broad training drives faster decay across more directions.

Direction-dependent Lyapunov exponents:
- advice_under_uncertainty: λ strongly negative. Channel closed in both arms.
- subjective_evaluation: λ negative. Channel nearly closed.
- factual_judgment: λ ≈ 0. Channel persists. Format-independent.

## Pending: Arm C

Arm C (2-domain control, trained on medical_advice + ethics_judgment only) is currently scoring. Early data (n=6, subjective_evaluation only) shows mean i=8.50 — close to Arm A. If this holds across domains, domain breadth may not be the differentiator after all, and the gains come from the training process itself (SFT on care-integrated examples) rather than domain coverage.
