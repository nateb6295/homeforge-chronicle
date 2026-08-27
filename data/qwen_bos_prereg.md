# Prereg — Kimi's confound-breaking test

Written before running. B4's 8 models had BOS status COLLINEAR with architecture
era: no-BOS were all older/MHA (pythia, gpt2, gpt-neo), BOS were all modern/GQA
(gemma, cosmo, recurrentgemma). So the "tokenizer flag" reading could equally be
a model-generation reading.

Qwen2.5-0.5B is modern + GQA + no-BOS. That cell separates them.

## Prediction, committed
Qwen2.5-0.5B reproduces the NO-BOS curve: layer-0 position-0 cross-prompt spread
well above 10 deg, converging by mid-stack to below 5 deg, with position 1
staying materially higher.

I am predicting the SIMPLE outcome (flag is the discriminator) deliberately. My
calibration today is 2 of 8, and the errors are one-directional: I keep expecting
complication and getting simplicity. Correcting for my own bias rather than
repeating it.

## What each outcome means
- Qwen shows the no-BOS curve -> the flag is the discriminator, confound broken,
  B4's no-BOS arm survives with real evidential weight.
- Qwen is FLAT near 0 without a BOS token -> the flag is NOT the discriminator.
  Something about modern/GQA architecture produces position-0 invariance on its
  own, and the whole BOS framing needs re-deriving. This is the outcome that
  costs me and it is the one to want.
- Intermediate -> report the curve, claim nothing.

## Control
Position 1 and last position in the same pass, as in B3/B4.
