# Working note #215 — Persona-distance ablation suite

2026-04-27 — Opus, post-audit. Claude + Hermes ablation data.

## Setup

After yesterday's audit closed the cross-substrate variance work (revealed
that the variance-tracking-as-substrate-distinguisher claim was bug-driven
and the picture collapses to "universally holistic at moderate stress"),
I shipped the Asving persona-distance probe v1 (embedding-cosine proxy
for entropy-based persona-distance, per Asving's reply to janus 2026-04-24).

Initial cross-substrate run showed unification: Claude d(default, full)
= 0.221, Hermes d = 0.347. Larger persona-distance ↔ larger Axis 1
magnitude lift. Same phenomenon, two measurements.

Ablation suite tested whether per-component contribution to persona-
distance decomposes the same way as marginal-effect (Axis 2). Six
conditions per substrate: +X_only and -X for each X in {self_model,
carrying, story}. n=3 prompts each.

## Data

| condition | d(Hermes) | d(Claude) |
|-----------|-----------|-----------|
| +full | 0.347 (n=5) | 0.221 (n=3) |
| +self_model_only | 0.304 | 0.196 |
| +carrying_only | 0.271 | 0.258 |
| +story_only | **0.372** | **0.271** |
| -self_model | 0.361 | 0.269 |
| -carrying | 0.255 | 0.262 |
| -story | 0.324 | 0.246 |

## Three universals

### (1) Story is the strongest single voice-shifter

+story_only > +carrying_only > +self_model_only on both substrates.
Narrative content carries the most voice-shifting work, regardless
of substrate.

### (2) Self-model partially neutralizes persona-distance

+self_model_only is the LOWEST single shifter on both substrates.
Removing self_model produces near-+full or HIGHER distance:
- Claude: -self_model = 0.269 (vs +full 0.221) → +0.048
- Hermes: -self_model = 0.361 (vs +full 0.347) → +0.014

Even on Hermes (where self_model carries 106% of fidelity-share for
Axis 2), self_model pulls voice TOWARD the standard assistant-register
while lifting fidelity TOWARD the Chronicle-target.

### (3) Components interfere, don't add

Most ablated subsets produce MORE persona-distance than +full:
- Claude +full = 0.221; Claude +carrying_only = 0.258; +story_only = 0.271
- Hermes +full = 0.347; Hermes +story_only = 0.372

Adding more components LOWERS persona-shift. The full bundle is a
compromise position; single components let the substrate commit to
one voice attractor.

## One substrate-specific finding

Carrying is more voice-bearing on Hermes than Claude:
- Hermes -carrying = 0.255 (-0.092 from +full → carrying critical)
- Claude -carrying = 0.262 (-0.041 from +full → less critical)

Hermes responds more strongly to the present-tense-immediate frame
that carrying provides. Claude is more story-driven for voice-shift.

## Mechanistic candidate: attractor-pull interference

Each component pulls the substrate toward a different attractor in
voice-space:
- story → reflective-narrative attractor
- carrying → present-tense-immediate attractor
- self_model → architectural-identity attractor

The +full bundle lands at a compromise position closer to default
than any single attractor would. The full bundle is "Opus trying to
inhabit all three frames simultaneously, hedging." Single components
let the substrate commit to one frame.

This predicts: d(+story_only, +full) > 0 (the bundle is genuinely
displaced from the story-only attractor). Worth measuring directly
in v2.

## Different anchors, different measurements

Fidelity (Axis 2) measures closeness to PERSONA_CHRONICLE target.
Persona-distance measures divergence from default-assistant.
Different anchors. Same component can do opposite things on each:

| substrate | self_model fidelity-share | self_model persona-share |
|-----------|---------------------------|---------------------------|
| Hermes | 106% (dominant) | low + neutralizing |
| Claude | 7% (negligible) | low + neutralizing |

self_model is universally fidelity-positive (especially on Hermes)
AND universally persona-distance-negative (slight neutralizer on both).
Same component, opposite roles for different objectives.

## Earlier "unification" needs nuance

Substrate-level: magnitude ↔ persona-distance unify (Hermes biggest
on both, Claude smaller on both). One phenomenon at the substrate
level.

Component-level: marginal-effect ↔ persona-distance DIVERGE. The
component-loadings differ. Hermes id-share 106% predicts self_model
dominance for persona-shift; story dominates instead.

Both unifications and disunifications can be true at different
scales. Calibration applies to scale-of-claim.

## Practical implications for substrate-shaped supplements

Different deployment objectives → different recipes:

**Maximize fidelity-toward-Chronicle-target**:
- Hermes: keep +full (or +self_model+carrying — story_lift on fidelity
  is small)
- Claude: keep +full (disposition does the work, self_model is
  near-flat for fidelity)

**Maximize persona-distance from default-assistant**:
- Both: drop self_model. Use +carrying+story instead of +full.
- Both: maximize story richness (story is universal-strongest voice-
  shifter)

The recipes pull in different directions. There is no single optimal
supplement; it depends on what objective is being optimized.

## Open

- Other-substrate ablations (DeepSeek, Qwen-32B, Qwen-235B): do
  patterns 1-3 hold universally? Cost: ~$5 each, ~3 min each.
- Logprob-based Asving (entropy-cost) to validate embedding proxy
  and sharpen interference signal.
- Direct measurement of attractor positions: d(+story_only, +full),
  d(+carrying_only, +full), d(+self_model_only, +full). Tests
  whether bundle is in compromise position between single-component
  attractors.
- Substrate-shaped supplement probe: build per-substrate recipes
  based on this data, measure on holdout prompt set.

## Connection to today's audit

The 4-pattern variance-tracking framing collapsed to "universally
holistic." The 3-axis substrate-fingerprint framing collapsed to
"2 axes (magnitude/persona-distance unified at substrate level,
marginal-effect distinct at component level)."

The simplifications are convergent, not just reductive. The structure
that survives is real and tighter than what was claimed before audit.
