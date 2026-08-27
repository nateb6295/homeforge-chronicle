# Prereg — where does the sink become content-independent?

Written 2026-08-24 ~15:10 PDT, before the full curve exists.

## The phenomenon
Found by accident today while falsifying Kimi's causal-masking mechanism.
Position-0 residuals are NOT prompt-independent — pythia has add_bos_token=False,
so position 0 is a different content token every prompt ('The','Photos','She').

But they CONVERGE with depth. Partial data already seen:

    L0  95.3°   L1 79.4°   L2 65.1°   L3 51.2°   L4 41.6°   L5 41.7°
    ...
    L22  1.46°  L23 2.97°  L24 76.6°

So the sink is not born content-independent. It BECOMES so. And at L24, where
the massive activation dissipates (pos0-norm/median 0.70), it comes apart again.

## I AM NOT BLIND AND I AM SAYING SO
I have already seen L0–L5 and L22–24. The prediction below is made knowing those.
The unseen region is L6–L21 — which is where the massive activation lives
(pos0/median goes 25.5 at L6, peaks 49.2 at L9, decays to 7.9 at L23).

## Predictions, committed now
1. **Monotone decline through the unseen band.** L6–L21 falls smoothly from ~40°
   toward ~2°. I predict NO sharp step: max single-layer drop < 15°.
2. **Convergence does NOT coincide with massive-activation onset.** The decline
   is already underway by L1–L5 (95°→41°) with pos0/median ≈ 1.0–1.7. So if I see
   the biggest drop at L5→L6 (where pos0/median jumps 1.4→25.5), prediction 1
   fails and the two phenomena are coupled.
3. **L24 is a genuine reversal, not a measurement artefact** — it should coincide
   with pos0/median falling below ~2.

## What each outcome means
- Smooth decline, no step at L6 → sink formation (a norm phenomenon) and content
  convergence (a direction phenomenon) are SEPARATE processes that happen to
  overlap in depth. That is the interesting outcome.
- Sharp step at L5→L6 → they are one process; the massive activation IS what
  makes position 0 content-independent.
- Non-monotone / noisy → report the curve, claim nothing.

## Falsifier for the whole thing
If the convergence is just "all residuals converge with depth" — i.e. an
anisotropy effect that has nothing to do with position 0 — this is not about
sinks at all. **CONTROL: run the identical measurement on position 1 and on the
LAST position.** If they converge the same way, position 0 is not special and the
phenomenon is generic depth-wise representational collapse (Dong et al. rank
collapse territory). That control decides whether there is anything here.

## Stopping rule
One run. The control runs in the SAME pass, not after seeing whether I like the
position-0 curve.
