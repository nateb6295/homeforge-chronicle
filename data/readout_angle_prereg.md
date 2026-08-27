# Prereg — is the σ₁-to-readout angle a sink measurement?

Written 2026-08-24 ~14:00 PDT, BEFORE running. Extends BASELINES.md §B1.

## What this bears on
Three findings tagged `needs-B1-gate` today, all the same claim shape — σ₁ sits
at a particular ANGLE to the unembedding read-out axis:

- **F636** "σ₁ is NOT the unembedding read-out axis — sits off-axis at all
  concept layers. 86.7° in 3B (|cos|=0.057), 74.5° in 7B (|cos|=0.267)."
- **F637** "σ₁ angle stays ~87° across all layers."
- **F639** "σ₁-readout insulation depth is scale-dependent."
- **F638** "σ₁ angle to read-out is DOSE-INVARIANT (<1.3° shift D0–D10) —
  architectural constant set by GQA, not CCS-modifiable."

B1 established that σ₁'s cross-prompt direction is carried by position 0 (46×).
If σ₁ ≈ the BoS residual, then "σ₁ sits 87° off the read-out axis" is a fact
about the attention sink, and the word "workspace" in these findings is doing
work the measurement does not support.

F638 is the one that worries me most: **invariance to dose is exactly what a
sink looks like.** A no-op drain would be invariant to everything.

## Method
Same rig as B1 (pythia-410m, same 12 prompts, H centred, sign-fixed). Per layer:
1. σ₁ unmasked, σ₁ masked (position 0 dropped from the SVD matrix).
2. Read-out axis: the model's unembedding matrix. Angle of σ₁ to the read-out
   SUBSPACE via its top principal directions, so this is not a single-token
   artefact.
3. Report angle and its CROSS-PROMPT VARIABILITY, masked vs unmasked.

## Predictions, committed now
- **Unmasked σ₁-readout angle will be near-constant across prompts** — std < 2°
  in sink-bearing layers. That is the F638 "invariance" signature, and it is
  what a fixed BoS direction produces for free.
- **Masked σ₁-readout angle will vary substantially** — std > 8°.
- The unmasked angle will be LARGE (>70°), reproducing the "off-axis" claim,
  because the sink is orthogonal to the read-out by construction — it carries no
  token identity.

## What each outcome means
- Predictions hold → **the "off-axis workspace" family is a sink measurement.**
  F636/F637/F639 describe where the BoS residual sits, not where a workspace
  sits. F638's dose-invariance is invariance of an architectural constant that
  no dose could move. All four need rebuilding on masked σ₁.
- **Masked angle also near-constant (std < 2°)** → the off-axis property survives
  masking, is NOT the sink, and the findings stand as written. This is the
  outcome that costs me and it is the one to want.
- Mixed → report per-layer, claim nothing globally.

## What this CANNOT do
pythia-410m is not Qwen-3B/7B. The original numbers (86.7°, 74.5°) are from
different models I am not running. So this tests the MECHANISM — whether
"σ₁-to-readout angle" is a sink quantity — not those specific values. A positive
here does not refute the numbers; it says they were measuring the sink and need
re-deriving on masked σ₁. Stating that before I see anything.

## Stopping rule
One run. No prompt-set changes after seeing results. If the B1 control inside
this run drifts >25% from 1.36°/62.83°, the rig changed and I stop.
