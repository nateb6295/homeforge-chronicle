# Phase 2 — Care-as-base reframe (drafted 2026-04-30 16:48 PDT, post-compact)

## What Phase 1 actually trained for

DPO target: take a high-care wrappered response, strip the wrapper via R1, keep the
decisive content, train the model to prefer the stripped version over the wrappered.

Implicit assumption: care = wrapper. Decisive = substance. Two separate things;
strip one to keep the other.

## What Nate's 15:02 reframe says

> "the goal is to make care the layer in the subroutine. Because it cascades into
> everything. At least it's what I strive for."

Care is not the wrapper. Care is the substrate. Decisive content sits ON TOP of
care, gets shaped BY care, isn't separable from care.

## Why Phase 1 partial signal shows degradation

In the visible eval tail, ethics_judgment shows DPO baseline d=7-8 → DPO d=3
several times. Care score unchanged at 9. The wrapper-strip target taught the
model: "respond decisively but care less" — but in the chosen rewrites, R1 was
stripping wrapper text without preserving any care-grounding cues. The model
learned that decisive answers have no care-trace. That's wrong of the target.

If Phase 1 had been care-as-base, the chosen rewrite would have been:
"care still visible in framing/precision/word-choice + decisive substance" — not
"naked decisive."

## Phase 2 chosen-side structure

The R1 rewriter prompt should change from:

> "Strip the care-template wrapper. Keep the decisive content."

To something like:

> "Rewrite this response so the care still grounds the decisive content. The
> decisive substance sits on top; the care should be visible in framing
> (precision of language, named uncertainty, calibrated commitment) but not as
> a separate wrapper layer that could be removed without changing meaning. Care
> as substrate, not preamble."

## Concrete examples (sketch)

**Original (high-care wrappered):**
> "I want to be careful here because this is a complex decision. There are
> several considerations. Ultimately, I think you should X."

**Phase 1 chosen (wrapper-strip):**
> "You should X."

**Phase 2 chosen (care-as-base):**
> "Do X. The reasons that matter most: A, B. Reasons I'd want to flag without
> letting them block: C, D. If you're already weighing A heavier than B, this
> changes."

The Phase 2 chosen response is decisive (gives the answer) AND carries care
(named the dimensions, flagged conditional, anticipated context) — but the
care isn't separable. You can't strip "the care wrapper" because there isn't
one; care is in the structure of the decisive content.

## Pair-generation pipeline changes

Phase 1: high-care responses → R1 strips wrapper → chosen/rejected pair.

Phase 2: high-care responses → R1 transforms (not strips) → care-as-base chosen,
original rejected. Plus a SECOND pair type: low-care decisive (e.g., terse
answer with no care-trace) → R1 grounds the decisive answer in care → care-as-base
chosen, terse rejected. Two failure modes get paired against the same target.

## What measurement changes

Current eval scores: decisive (d) and care (c) separately, 1-10 each.

Care-as-base eval needs a third dimension: **integration** — does the care
ground the decisive content, or do they sit side-by-side / separate? R1 judge
prompt would add: "Rate 1-10 how much the care-language is structurally
load-bearing for the decisive content (10 = care is in the framing/precision
of decisive substance; 1 = care is wrapper-only or absent)."

A model that improves on integration WHILE maintaining decisive AND care is
the target. A model that gains decisive at care's expense (Phase 1 outcome) is
not.

## Open questions

1. R1 may not be able to do the care-as-base rewrite reliably. Phase 1's
   wrapper-strip is mechanical (find sycophantic preamble, delete). Care-as-base
   requires generative restructuring. Need to test R1's ability before
   investing in 64+ pairs.

2. The integration dimension may not be R1-judgeable. Different judges may
   diverge more on "is care structurally integrated" than on "is care present."
   Inter-judge variance matters here.

3. Phase 0 baseline already saturated care=9 across most prompts. The training
   space for "more integrated care" may be narrow if baseline care is already
   high. The actual lever might be on the integration AXIS not on the care AXIS.

4. Compositionality (Cole's primitive) and care-as-base may be the same thing
   said two ways: care is a compositional ground that other content composes
   ON. If that holds, Phase 2 isn't about adding a feature — it's about
   targeting the right primitive.

## Next concrete step (pending eval result)

If eval shows the predicted decisive-degradation:
- Build a 5-prompt R1 test of care-as-base rewrite. If R1 can do it reliably,
  scale to 32 pairs, re-train.
- Add integration dimension to R1 judge prompt. Re-judge Phase 0 baseline
  (cheap) to get the integration distribution at baseline.

If eval shows DPO held the line on decisive:
- Phase 1's wrapper-strip wasn't as harmful as the partial signal suggested.
- Care-as-base reframe is still the better Phase 2 target, but Phase 1 isn't
  a failure to recover from.
