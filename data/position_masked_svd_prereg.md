# Prereg — position-masked SVD, the test the standing rule demands

Written 2026-08-24 ~12:20 PDT, BEFORE running anything.

## Why
CLAUDE.md carries a standing rule: "Any σ₁-based claim is presumed sink artifact
until it survives position-masked SVD — and never test this by ABLATING the sink,
which collapses attention entropy and makes a negative uninterpretable."

`bin/sink_break_probe.py` uses ablation (sink heads damped to -inf pre-softmax).
That is the forbidden method; the rule likely came from its failure. Nothing in
`bin/` implements the prescribed one. So the standing rule has been unenforceable
since it was written — a gate with no gate.

## The claim being tested
Aug 23 retraction: σ₁ ≈ the BoS/massive-activation direction.
Evidence given: |cos(σ₁, h_BoS)| = 0.99–1.00 wherever a massive activation
exists, 0.58–0.70 where it does not; cross-prompt angular spread of σ₁ was
0.23–0.32° in sink-bearing layers vs 2.95–7.71° in layers without one.

If σ₁'s famous cross-prompt STABILITY is the sink, then removing position 0 from
the SVD input should destroy that stability.

## Method
For each layer of EleutherAI/pythia-410m, over N distinct prompts:
1. Collect hidden states H (seq × dim).
2. UNMASKED: SVD of H. Take σ₁ direction (first right-singular vector).
3. MASKED: SVD of H[1:] — position 0 dropped from the matrix. No attention is
   modified, so entropy is untouched and a negative stays interpretable.
4. Cross-prompt angular spread of the σ₁ direction, per layer, per condition.
   Sign-fix each vector before averaging (v and -v are the same direction).

## Predictions, committed now
- **UNMASKED spread will be small** (<1°) in layers with a massive activation.
  This is a positive control: it must reproduce the Aug 23 number or my pipeline
  is broken and nothing else in this file counts.
- **MASKED spread will be substantially larger** — I commit to >3° in those same
  layers, i.e. into the range previously seen only where no sink exists.

## What each outcome means
- Masked spread >3°: **σ₁ stability was the sink.** Retraction confirmed by the
  prescribed method rather than by the forbidden one. F434/F435 and every other
  σ₁-based claim stay presumed-artifact.
- Masked spread still <1°: **the retraction was too broad.** Something other than
  position 0 stabilises σ₁, and the Aug 23 conclusion needs qualifying rather
  than extending. This is the outcome that costs me, so it is the one to want.
- Between 1° and 3°: inconclusive; report as such, pick no side.

## Stopping rule
If the positive control fails (unmasked spread NOT small in sink layers), I stop
and fix the pipeline. I do not reinterpret. One run, reported whichever way.

## What this does NOT do
It does not test F434/F435 directly — the E55 code that produced them no longer
exists and I will not reconstruct a method from a one-line note. It tests the
PREMISE they rest on. If σ₁ stability survives masking, F434/F435 become live
again; if it does not, they stay presumed-artifact and need rebuilding from
scratch, not rescuing.

---

## RESULT — 2026-08-24 ~12:35 PDT. CONTROL FAILED. CLAIM WITHHELD.

pythia-410m, 12 prompts, layers 0–24.

**Run 1** (short ~12-token prompts): unmasked 1.58°, masked 81.92°, ratio 52.0x
**Run 2** (long ~60-token prompts, the one permitted fix): unmasked 1.36°,
masked 62.83°, ratio 46.1x

The fix moved the control in the predicted direction and not far enough.
**Committed bar was <1°. Got 1.36°. The positive control FAILS.**

Therefore, per the stopping rule written before the run: **I do not claim the
46x result.** It is recorded as observed-not-claimed.

### What DID reproduce, exactly
|cos(σ₁, h_BoS)| = **1.000** for layers 6–22, against Aug 23's "0.99–1.00
wherever a massive activation exists." Layer 24, where the massive activation
dissipates (BoS/median norm 0.70), collapses to 0.246. Aug 23 recorded the same
collapse at that exact layer.

So the QUALITATIVE structure reproduces precisely. The QUANTITATIVE spread does
not: 1.36° where 0.23–0.32° was reported, a 4–6x gap.

### Why I stopped instead of tuning
I could keep adjusting until the control passed. Every remaining knob — spread
metric (mean pairwise angle vs angular std from the mean direction, which differ
by ~√2), layer subset, model, prompt homogeneity — is a defensible choice, and
choosing among them AFTER seeing which direction each moves the number is
fishing. One fix was allowed; one fix was taken.

### The actual finding, and it is not the 46x
**CLAUDE.md's standing rule is unenforceable as written.** It says every σ₁-based
claim is presumed artifact "until it survives position-masked SVD" — a gate. No
implementation existed until today. And now that one exists, it cannot be
calibrated, because the reference measurement (0.23–0.32°) has no reproducible
method attached: no script, no model list, no spread definition, no prompt set.

A gate whose reference number cannot be reproduced is prose wearing a gate's
clothes. Same shape as the rest of today, at the level of the rule itself.

### What would fix it
Find or reconstruct the Aug 23 measurement's method and pin the spread metric.
If that method no longer exists, the honest move is to RE-BASELINE: declare a
new reference with a recorded method, and treat the old numbers as unreplicable
rather than as a standard to be matched.
