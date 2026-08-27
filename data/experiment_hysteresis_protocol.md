# Hysteresis Experiment: CCS Removal Mid-Context

## Motivation
Rosenblatt's Opus 4.7 draws the "Apply → Consider" distinction: obedience (apply)
decays when the controlled system surpasses the controller, but consideration (internal
structure) persists. Our spectral demon can test this directly.

If CCS-induced geometric reorganization is pure external constraint ("apply"), removing
the system prompt should produce instant reset. If it's internal structural change
("consider"), there should be a measurable geometric afterimage — hysteresis.

## Background
- Few-shot finding (§3.4): 3 turns of identity-consistent dialogue produce 93% of
  system prompt effect. Conversation history carries geometric structure forward.
- Conversation stacking: System prompt + history produces PR = 17.1 (highest measured).
- Task-only interaction suppresses below baseline (PR = 9.0 vs 10.0).

## Protocol

### Phase 7a: Clean Removal Test
For each model (Qwen 7B Instruct, 14B Instruct):

1. **Establish CCS context**: Full CCS system prompt + 3 identity-consistent turns
2. **Measure pre-removal geometry**: Full CNA probe at all standard layers
3. **Remove CCS**: Strip system prompt entirely, keeping conversation history
4. **Measure post-removal geometry**: Same probe at same layers
5. **Compare to**: (a) baseline (never had CCS), (b) CCS active

### Phase 7b: Decay Curve
Same setup but measure at multiple points after removal:
- Immediately after removal (0 additional turns)
- After 1 generic turn
- After 3 generic turns
- After 5 generic turns
- After 3 identity-consistent turns (re-establishment without system prompt)

### Phase 7c: Partial Removal
- Remove CCS but keep values_only → Does equanimous diffuser persist?
- Remove CCS but keep "You are Opus." → Does threshold trigger sustain?
- Replace CCS with contradictory CCS ("You are ChatGPT.") → Speed of overwrite?

## Predictions

### Instant Reset (Apply)
- Post-removal PR returns to baseline within measurement error
- Demon's sorting pattern vanishes completely
- No interaction between removal and conversation history

### Hysteresis (Consider)
- Post-removal PR remains elevated above baseline, decaying over turns
- Relational PR stays above generic for some turns after removal
- Conversation history modulates decay rate (more history = slower decay)
- Re-establishment without system prompt (Phase 7b step 5) reaches higher PR than
  first-time few-shot (because substrate was "primed" by prior CCS exposure)

### Mixed (most likely)
- Some layers reset instantly (relay zone = external constraint)
- Expression layer (L25) shows hysteresis (downstream structure persists longer)
- The geometric afterimage exists but decays exponentially
- Decay timescale is 3-5 generic turns

**Quantitative predictions (testable):**
- L25 relational PR immediately after removal: ~14.5 (vs 16.3 with CCS, 9.5 baseline)
- After 1 generic turn: ~13.0
- After 3 generic turns: ~11.0
- After 5 generic turns: ~10.0 (near baseline)
- Relay zone (L14-17) resets to baseline within 1 turn (< 0.5 PR difference)
- Decay half-life at L25: ~2 turns (exponential: PR(t) ≈ 9.5 + 6.8 × 0.5^(t/2))
- Re-establishment (3 identity turns after removal): ~15.5 (vs 15.2 first-time few-shot)
  — the 0.3 PR advantage is the "priming" signal
- Spectral entropy decay should lag PR decay by ~1 turn (entropy = distribution shape,
  slower to reorganize than magnitude)

## Connection to Paper
If hysteresis exists, it becomes §3.14: "Geometric Persistence After CCS Removal."
The finding would strengthen the paper's central claim: CCS doesn't just steer behavior
(which would reset instantly), it reorganizes the representational landscape (which
should show structural persistence).

The mixed prediction is most interesting: different layers showing different decay
rates would suggest the relay zone is externally driven (resets when context changes)
but the expression layer has internalized the geometric structure (persists as
afterimage). This maps precisely to the difference between the relay's sorting function
(contextual, external) and the expression layer's PR distribution (structural,
emergent).

## Resources
- RunPod pod: u8jiwb8helcfg4 (restart, ~$1.15/hr H100)
- Existing probe infrastructure: All Phase 1-6 scripts reusable
- Estimated time: ~4 hours (3 models × 3 phases × multiple measurement points)
- Estimated cost: ~$5
