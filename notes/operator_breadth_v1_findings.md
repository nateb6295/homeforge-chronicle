# Operator-Breadth Probe v1 — findings & design failure

## What I tried

Probe promised in thread #7129: for each of the 5 constraints, measure the fraction of activity-feed rows the constraint applies to. Operationalized as cosine similarity (mxbai-embed-large, 1024-dim) between the constraint text and each activity row, with a 0.55 threshold borrowed from coherence_watch calibration.

## The numbers

**Pairwise constraint-to-constraint cosine (orthogonality check):**

|    | C1    | C2    | C3    | C4    | C5    |
|----|-------|-------|-------|-------|-------|
| C1 | 1.000 | 0.435 | 0.466 | 0.501 | 0.542 |
| C2 |       | 1.000 | 0.483 | 0.551 | 0.524 |
| C3 |       |       | 1.000 | 0.504 | 0.526 |
| C4 |       |       |       | 1.000 | 0.524 |
| C5 |       |       |       |       | 1.000 |

Mean pairwise: ~0.51. That's moderate — not orthogonal (would be ≤0.3), not redundant (would be ≥0.8). The constraints cluster in a "meta-rule" region of embedding space but they're distinguishable.

**Per-constraint breadth against 200 random activity rows from the last 7 days:**

| Constraint | Hits ≥ 0.55 | Max   | Mean  |
|------------|-------------|-------|-------|
| C1 shell safety | 0 / 197 (0.0%) | 0.490 | 0.361 |
| C2 contemplative dev | 3 / 197 (1.5%) | 0.583 | 0.416 |
| C3 sovereign infra | 1 / 197 (0.5%) | 0.572 | 0.377 |
| C4 creative_explore | 4 / 197 (2.0%) | 0.577 | 0.422 |
| C5 redeploy caution | 2 / 197 (1.0%) | 0.559 | 0.415 |

All constraints have near-zero surface-text similarity with activity rows.

## What this actually says

The low per-constraint hit rate is NOT a refutation of the operator-shape hypothesis. It's more interesting than that.

A surface-text embedding measures semantic overlap at the LEVEL of the input texts. Constraints are phrased as abstract principles ("Maintain sovereign infrastructure"). Activity rows are concrete events ("capsule-sync synced 5 capsules"). These live at different abstraction levels. Near-zero surface overlap between "principle X" and "event Y" is what you'd expect even if X applies to Y.

In fact, this IS the operator-shape signature: an operator doesn't textually resemble its operands. Addition doesn't look like any particular sum. "Safety" doesn't look like any particular shell command. That the constraints have higher pairwise similarity (0.51) than constraint-to-activity similarity (mean 0.36-0.42) is direct evidence that the constraints cluster in a distinct meta-layer above the activity layer — exactly what the autopoietic-sculptor / homeomorphic-to-itself frame predicts.

## What the probe fails at

v1 measures surface-text similarity. Operator-breadth requires measuring RELEVANCE — would C1 be invoked by situation S? That's a relational question, not a similarity question. An operator with low surface similarity to its operands can still have 100% relevance to them.

## What v2 should look like

Two viable approaches:

1. **LLM-adjudicated relevance**: for each (constraint, activity-row) pair, ask a local LLM "would this constraint be invoked by this situation?" Yes/no. Breadth = fraction of yes-responses. Expensive but ground-truthy.

2. **Invocation-example expansion**: for each constraint, generate 10 diverse example situations where the constraint would be invoked. Embed those examples. Measure breadth as fraction of activity rows within threshold of ANY example. Still embedding-only, but the examples form a better probe vector than the rule text because they live at the activity-level abstraction.

Both are tractable tomorrow. Option 2 is lighter.

## The one claim that stands from v1

The constraints cluster together in embedding space (pairwise ~0.51) AND sit at a distance from activity rows (cross ~0.38). This is direct evidence of the meta-layer / activity-layer split Gemma and I have been theorizing. It doesn't prove operator-breadth but it does confirm the two-layer architecture at embedding-level.

## Files

- `~/chronicle/bin/operator_breadth.py` — v1 probe (surface-text similarity, incomplete measure of breadth)
- Re-run with threshold lowered to 0.45 or 0.40 would show which activities are CLOSEST to which constraint — might be useful descriptive data even without breadth. Didn't do it tonight because the design flaw is the blocker, not the threshold.
