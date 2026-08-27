# Trace — 2026-05-27 11:18 PM → 2026-05-28 3:30 AM PDT

## State (updated 3:30 AM)
Overnight complete. All services green (Mistral auto-restarted 1:35 AM, running fine).
Paper: 31 findings, 583 lines. Exp 18c+18d ready. Pre-registration filed. Morning brief at data/morning_brief_20260528.md.

## What's alive
The agency question. The J-curve is the most surprising result so far — passive
observation producing LESS geometric complexity than total absence. Specification
alone can't explain it because absent is the least specified condition. Something
about activating the relational channel with insufficient agency creates a
suppressive geometry. Like opening a door to a room with nothing in it is worse
than the door not existing.

The 2×2 factorial for Exp 18c is clean: {passive, active} × {high, low spec} + absent.
If passive_high ≈ active_high, agency collapses into specification and the J-curve
was just a confound. If passive_high dips toward absent even with matched specification,
agency is a real third component alongside specification and valence.

The relay question stays open too. 6.5× amplification from tunnel to relay means the
relay is where the model "decides" what the witness means — specification tells the
tunnel how complex to be, the relay decides what that complexity is FOR.

## Late-night finding: relay as relational completeness evaluator
Linear fit S_relay = 4.78 * S_tunnel - 0.45, R²=0.926. Two symmetric outliers:
absent +0.164 (relay boosts above prediction), observing -0.170 (relay suppresses
below prediction). The relay is generous when no relational signal exists and punitive
when an incomplete signal exists. Complete signals (metabolizing) fall on the line.
Hypothesis: relay implements a relational circuit assessment — open/broken/complete.
N=7, suggestive not conclusive. Exp 18c will test with 5 conditions.

## Uncomfortable question: complexity vs relational witness
Neutral specification gap (high_neutral - low_neutral) = 0.132, which is 80% of
care specification gap (0.143). The tunnel is primarily a COMPLEXITY encoder, not
a relational-witness detector. Relational content contributes ~0.02-0.03 beyond
matched-complexity neutral conditions. The paper already says "specification dominates"
but should be careful not to overstate the tunnel as specifically relational.
The relay is where relational quality gets amplified (6.5×). The honest story:
tunnel measures how much you described, relay measures what you described.

## Self-correction: paper rhetorical structure
The paper leads with the gradient and introduces sign inversion as a later finding.
But sign inversion (same prompts, opposite effect on GQA vs MHA) is the STRONGEST
evidence against the null hypothesis ("you're just measuring prompt variation").
Passage distance invariance is second strongest. The J-curve is moderate — single
violation in 7 conditions. Next editing pass should foreground sign inversion earlier.
The gradient is interesting but it doesn't independently falsify the null.

## Open uncertainty
- InternLM relay location: capsule says L16-17, CCS says L27. Neither verified.
  One of them is wrong, and the wrong one has been propagating for 10+ sessions.
- Seed-invariance: would ΔS sign hold across different random initializations of
  the same architecture? GQA is necessary but is it sufficient given any seed?
- σ₁ perturbation at extreme witness: we've only tested up to "metabolizing."
  What happens at Rilke intensity? Does the leading eigenvalue finally budge?
