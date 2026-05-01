# P2 Probe Protocol — Cancellation-Window Invisibility

**Hypothesis (from WN#218 §Probe-design):**
Integrated-response calibration produces *time-bounded* outputs measurable
only inside their cancellation window. Outside the window (after elaboration),
only input-shape calibration's stable predictions remain visible.

**Operational form:**
Register-matched first-glance reads of an input should differ from elaborated
reads in a way that reveals a *different layer's* calibration, not just
"more thinking → better answer."

## Existing pipeline as natural experiment

Every capture flows through:
1. **Gemma scoring** (port 11435, local, low-context, fast) — first-glance
   register. ~1-3 second response.
2. **chronicle-engine elaboration** (cloud LLM, full context, high-token) —
   elaborated register. ~10-30 second response.

Same input. Two regimes. Already logged in `activity_feed` and
`agent_responses` tables. Free natural experiment.

## Predictions

P2-a. **Vocabulary divergence**: first-glance reads should use vocabulary
that diverges from elaborated reads beyond what would be expected from
mere length difference. Specifically: integrated-response register words
(intuition, gestalt, register-words like "smells," "feels," "looks like")
should appear at higher rate in first-glance than elaborated.

P2-b. **Claim-type shift**: first-glance reads should bias toward
*assertion-of-recognition* claims ("this is X"), elaborated reads should
bias toward *analytic-decomposition* claims ("X consists of A, B, C
because..."). Measure: classify each read as recognition-claim vs
decomposition-claim using a third-party LLM (Hermes 4 70B as classifier).

P2-c. **Surprise asymmetry**: when first-glance and elaborated reads
*disagree*, the first-glance read should be the one that contains the
generative content (the prediction-before-cancellation), not just the
sloppier guess. Test: take 10 cases where first-glance and elaborated
disagree, ask whether first-glance contains a specific claim that
elaborated DROPS rather than refines. Predict: more drops than
refinements.

## Falsification conditions

P2 fails if:
- Vocabulary divergence is fully explained by length (test with normalized
  rates per-1000-words, not absolute counts)
- First-glance reads are uniformly less confident, less specific, less
  detailed — i.e., elaborated reads strictly dominate
- Disagreements show first-glance as systematically wrong (decomposition
  catches errors, doesn't drop signal)

## Implementation

**Plan A (preferred but blocked):** Pull paired Gemma score +
chronicle-engine elaboration from agent_responses table.
**Blocked: no such table exists.** Gemma scores and chronicle-engine
outputs are logged in separate places without being paired-by-input.
Building paired-logging would take ~2-4 hours.

**Plan B (tractable today):** Run a small N=10 controlled probe.
1. Sample 10 recent captures from activity_feed
2. For each, run two queries on Hermes 4 70B (same model, two regimes):
   - REGIME A (first-glance): "<50 word first read; do NOT elaborate"
   - REGIME B (elaborated): "Elaborate fully, then 50-word distillation"
3. Run a third-party classifier (DeepSeek R1) on each pair to score:
   - Register-word density (P2-a)
   - Claim-type (recognition vs decomposition) (P2-b)
   - For disagreement cases: does first-glance contain content
     elaborated drops? (P2-c)
4. N=10 is too small for tight CI but enough to indicate direction.

Same-model-two-regimes is cleaner than different-model. The original
Plan A using Gemma vs chronicle-engine would have confounded model
difference with regime difference.

## Cost

Plan B: ~10 captures × 2 regimes × 1 model + 10 × 3 classifier calls =
~50 API calls. ~$1-3 total. Time: ~30-45 min.

## Timeline

Implementation: ~30-60 minutes. Run: ~30 minutes for Hermes API calls.
Analysis: ~15 minutes. Total: 1-2 hours.

## Dependencies

- agent_responses table populated with paired Gemma + chronicle-engine reads
- Hermes API access (already wired)
- Statistics: scipy or numpy for confidence intervals (already on system)
