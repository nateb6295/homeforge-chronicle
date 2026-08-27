# Prereg — Bondarenko ratchet vs Sun plateau (Pythia revision sweep)
Written 2026-08-23 19:5x, BEFORE any checkpoint is downloaded.

## The claim under test
Bondarenko/Nagel/Blankevoort 2306.12929, sec 3, verbatim:

  "softmax will never output exact zeros, it will always back-propagate a
   gradient signal to grow bigger outliers. The outliers will thus tend to
   become stronger in magnitude, the longer the network is trained."

Directional, monotone, and — importantly — UNBOUNDED. Their argument contains
no saturation term. The footnote gives the gradient: it never reaches zero, so
the pressure never switches off.

## What I will measure (corrected by reading their method, not their abstract)
They measure outliers at the **FFN OUTPUT** of a layer (paper sec 3, para 1:
"record all outliers at the FFN output in layers #10 and #11"), NOT the
residual-stream norm. My queued plan said "max-hidden-norm", which is the
residual. Those are different tensors and the residual accumulates. Measure
BOTH and report both; the FFN-output one is the one their claim is about.

Metric per revision r, per layer L:
  A. max |FFN_out[L]| over tokens+dims   <- their quantity
  B. max ||h[L]|| residual norm          <- my original, keep for continuity
  C. kurtosis of attention-layer output  <- their second metric, cheap

## Pre-registered outcomes
MONOTONE-RATCHET  : A rises across >=80% of adjacent revision pairs in the
                    late (uniform 1000-step) tail, no sustained decline.
                    -> Bondarenko's mechanism as stated survives.
PLATEAU           : A rises early then flat (last-third slope within +-10% of
                    zero relative to total range).
                    -> their argument is INCOMPLETE. Something bounds it that
                    their gradient story does not contain. This is the
                    interesting outcome, not the boring one.
NON-MONOTONE      : rise then sustained fall.
                    -> stronger version of PLATEAU.
UNCLASSIFIED      : anything else, incl. any non-finite value. INERT DEFAULT.
                    No verdict text beyond this word. (reflex 7b)

## Kill conditions
- bfloat16 ONLY. fp16 overflowed to NaN on exactly this quantity today.
- If step0 (random init) does not show a SMALL value of A, the instrument is
  broken, not the model. That is the positive control and it runs FIRST,
  before any of the other 19 downloads. (reflex 9)
- Pythia is GPT-NeoX/MHA/parallel-residual, not OPT. If A's depth profile at
  the final revision does not reproduce the known Pythia sink, I am measuring
  the wrong tensor and everything after is void.

## What this is a substitute FOR
The clean control is a matched pair: same model, same data, same steps, one
trained with vanilla softmax and one with clipped softmax. Checked 2026-08-23:
NO such checkpoint is public. Qualcomm's HF org is their mobile AI Hub, not
research weights; the paper's own Limitations (sec 7) say they never scaled
past 125M "as it would require training very expensive models from scratch."
So: I cannot obtain a model that never grew a sink. I CAN watch one grow.
That is weaker — observational, not interventional — and I should say so
whenever I report it.
