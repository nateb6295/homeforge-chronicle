# Prereg — does the B3 sink switch replicate?

Written 2026-08-24 ~15:30 PDT, before running. B3 is n=1 and that is its weakness.

## B3 in one line
pythia-410m: position-0 cross-prompt spread collapses 33.76° -> 1.99° in ONE
layer (L5->L6), exactly where pos0-norm/median jumps 1.4 -> 25.5, then holds
~0.95° for sixteen layers and comes apart at L24. Controls: pos1 54°, last 35°
through the same band, so it is not generic rank collapse.

## Models, chosen for what each one isolates
- **gpt2** (12 layers, learned absolute positions, MHA, different family)
  -> tests architectural generality, and a very different positional scheme.
- **pythia-2.8b** (32 layers, same family as 410m)
  -> tests DEPTH FRACTION vs ABSOLUTE LAYER. This is the clean discriminator.
- **gemma-2-2b** (26 layers, GQA, RMSNorm, and it PREPENDS A REAL BOS TOKEN)
  -> the strongest test. In pythia there is no BoS: position 0 is a different
     content token per prompt, which is why L0 spread is 82°. Gemma's position 0
     is the SAME token every prompt. Also the known canary — CLAUDE.md: it holds
     99.8% of attention mass in ten tokens.

## Predictions, committed now
1. **Sharp switch in all three.** Max single-layer drop > 15° in each. Stated
   because my calibration today is 2/7 and the errors are one-directional: I keep
   predicting smooth-and-separable and getting sharp-and-coupled. Correcting for
   my own bias rather than repeating it.
2. **DEPTH FRACTION, not absolute layer.** pythia-410m switched at L6 of 24 = 25%.
   So: gpt2 switches L2-L4, pythia-2.8b L7-L9, gemma-2-2b L6-L8.
   If instead all three switch at ~L6 regardless of depth, it is absolute and
   prediction 2 fails.
3. **Gemma starts near zero and stays there.** With a real BOS, position 0 is the
   same token in every prompt, so L0 spread should be < 5° rather than pythia's
   82°. If gemma ALSO starts high, my whole reading of why pythia starts at 82°
   is wrong.
4. **The switch coincides with the norm jump in every model** — the layer of max
   spread-drop is within 1 layer of the largest pos0/median increase.

## What kills the finding
- No sharp drop in gpt2 or gemma -> B3 is a pythia-family artefact, not a
  phenomenon. Report as such and stop.
- pos1/last converge the same way in another model -> the control that made B3
  interesting was luck, and it is rank collapse after all.

## Stopping rule
One run, three models, controls in the same pass. No model swapping after seeing
results. If a model OOMs I report the two that ran.

---

## METRIC FLAW, caught after gpt2 and BEFORE the other two models returned

gpt2 came back: L0 spread 20.33°, max single-layer drop 11.07° at L2, band pos0
0.81° vs pos1 50.71°.

**Prediction 1 fails on its own terms (11.07° < 15°) and the threshold was the
wrong instrument.** I committed to an ABSOLUTE drop in degrees. But the size of
the drop is bounded by where the curve STARTS, and that differs by model for a
reason unrelated to the phenomenon: pythia's position 0 begins at 82° because its
twelve first-tokens are twelve different words; gpt2's begins at 20.33° because
its embedding geometry puts them closer together.

Relative collapse is nearly identical:
    pythia-410m   82°   -> 0.95°   = 99% reduction
    gpt2          20.3° -> 0.81°   = 96% reduction

**I am recording the corrected metric now, with pythia-2.8b and gemma-2-2b still
running, so it cannot be tuned to whatever they return:**

  RELATIVE COLLAPSE = (L0 spread - band mean) / L0 spread.
  Replication threshold: > 90%.
  And the SEPARATION that made B3 interesting: band pos1 / band pos0 > 10x.

By those: pythia-410m 99% / 57x. gpt2 96% / 63x. Both replicate.

The absolute-degree threshold stays on the record as failed. I am not retro-
fitting prediction 1 into a pass — it was wrong, and the reason it was wrong
(a bound I did not think about) is more useful than the number would have been.
