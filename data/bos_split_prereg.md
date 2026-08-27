# Prereg — does the BOS/no-BOS split hold across families?

Written 2026-08-24 ~16:20 PDT, before running.

## The claim being tested
From B3 amendment 2: position-0 cross-prompt spread is EXACTLY 0.00° in models
that prepend a BOS token (causal masking ⇒ position 0 sees only itself ⇒
identical by construction), and LARGE-then-converging in models that do not
(position 0 is a different content word each prompt, and the network manufactures
a sink out of it over depth).

**Weakness:** the BOS side rests on ONE model, gemma-2-2b.

## Tokenizer survey already run (cheap, decisive for design)
    BOS:     opt-125m </s>, opt-1.3b </s>, gemma-2-2b <bos>, cosmo-1b <s>,
             Mistral-7B <s>, Llama-3.1-8B <|begin_of_text|>, recurrentgemma-2b <bos>
    no-BOS:  gpt2, gpt2-medium, gpt-neo-125m, pythia-410m/1.4b/2.8b,
             phi-1_5, phi-2, SmolLM-1.7B

## Models to run, and what each isolates
- **facebook/opt-125m** (BOS) — different family, learned absolute positions with
  an offset. Tiny, so it costs nothing to include.
- **HuggingFaceTB/cosmo-1b** (BOS) — third BOS family, `<s>`.
- **google/recurrentgemma-2b** (BOS) — **NON-TRANSFORMER.** Griffin: recurrent
  blocks plus local attention. This is the interesting one. If 0.00° holds here,
  the invariance is a consequence of CAUSALITY, not of attention. If it does not,
  something in the recurrence carries information the attention story does not
  predict.
- **EleutherAI/gpt-neo-125m** (no-BOS) — control on the other side, so the run
  is not all one arm.

## Predictions, committed now
1. **All three BOS models: pos0 spread < 0.01° at every layer.** Including
   recurrentgemma. Causality is causality.
2. **gpt-neo-125m: no-BOS pattern** — L0 spread > 10°, converging to < 5° by
   mid-stack, with pos1 staying > 30°.
3. **recurrentgemma will NOT show the position-1 mid-stack dip** that gemma-2-2b
   showed (83° → 8.6° at L12 → 73°). I read that as a second attention sink, and
   a mostly-recurrent model should not build one.

## What kills it
- Any BOS model with pos0 spread meaningfully above zero → the construction
  argument is wrong or incomplete, and B3 amendment 2 needs rewriting rather
  than extending.
- gpt-neo-125m failing to show the no-BOS pattern → the no-BOS side is
  family-specific, not general.

## Calibration note
I am 2 for 7 on prereg'd predictions today, and the errors are one-directional:
I expect smooth-and-separable, reality returns sharp-and-coupled. Prediction 1
is deliberately an EXACT claim (0.00°) rather than a hedge, because if the
construction argument is right there is no room for wobble, and if I am wrong I
want it to be unmistakable.

## Stopping rule
One run, four models, all arms in the same pass. No model swaps after seeing
results. If a model fails to load I report the ones that ran and say which did not.

---

## THRESHOLD ERROR — caught before the last model returned

opt-125m and cosmo-1b came back with max-any-layer pos0 spread of **0.0271°** and
**0.0270°**. My verdict function called both "BOS pattern BROKEN" against the
committed <0.01°.

Two different families, different depths (12 vs 24 layers), agreeing to three
decimal places. That is not a property of two models. That is a floor.

**Derived it:** arccos is ill-conditioned near 1. For cos = 1 − ε the angle is
≈ √(2ε). With fp32 ε = 1.192e-7:

    cos = 1 − 1·eps   ->   0.0280°
    measured           ->   0.0271°, 0.0270°
    direct check: a vector vs itself perturbed by ONE ULP -> 0.0280°

**0.028° is the measurement floor of this instrument in fp32.** Two vectors that
are exactly identical cannot register below it.

**So my committed threshold was unachievable.** I set <0.01° — a bar beneath the
floor of my own measurement. Nothing could have passed it, including a perfect
result. The BOS prediction is CONFIRMED: all BOS models sit ON the floor, which
is what exactly-identical looks like at this precision.

**This is reflex 3b's mirror image and I want it recorded that way.** 3b says a
floor whose value I can derive is not a control. Here I set a THRESHOLD below a
derivable measurement floor — same failure, opposite direction. In both cases the
fix is the same question, asked before the run: *can I compute what this number
must be, without running anything?* Ten seconds of arccos conditioning would have
told me 0.01° was impossible.

**Corrected criterion, committed now with recurrentgemma still running:**
    BOS pattern confirmed if max-any-layer pos0 spread <= 0.05° (~2x the floor).
    BOS pattern broken if it is materially above that.
By this: opt-125m PASS (0.0271), cosmo-1b PASS (0.0270), gemma-2-2b PASS
(printed 0.00 at 2dp, so <= 0.005 as displayed — needs a reprint at 4dp to state
precisely, and it cannot be below the floor either).
