# PREREG — Phase 0 gate: does CCS compression depend on history at all?

Written 2026-08-24 19:29 PDT (stamped by date at commit, not typed).
Nate's spec, in his words: "compression happen when it WAS needed, but not more
than xx." Two halves:
  - RATE CAP — already exists and is grounded (F160 dose-response, 3h floor /
    4h ceiling). Keep it. It is why fifty days of a dead sensor were still SAFE.
  - "WHEN NEEDED" — undefined in the current format. That is what this gates.

## Why the last build failed, precisely

Not "the sensor broke." `compression_readiness.py` computes novelty from
`cognitive_state.episodic_trace`. That field belongs to the LEGACY JSON CCS
schema. The live brain-format prompt (v4) emits SPINE / CORE / REMEMBERS /
SEEKS / ALIVE / RELATES / BRIDGE and contains the string "episodic" ZERO times
(v5 also zero). So the field was never going to be written. It stayed `[]`,
`(novelty or 0)` turned that into a real zero, novelty carried 0.6 of the
weight, and the service silently became a 3-hour clock for ~400 compressions.

**A format migration left a sensor reading a schema that no longer exists.**
So the question is not "fix novelty." It is "is there anything to sense, and
what does it read from in the CURRENT format?"

## The structure this exploits

`brain_compress` is literally a recurrence with two template slots:
    prompt = template.replace("{previous_state}", prev).replace("{session_context}", ctx)
So compression = f(history, content). Both are directly manipulable, and the
experiment can construct prompts and POST to the engine WITHOUT touching
`cognitive_state`. **This experiment performs ZERO database writes.**

## Arms — k = 6 each, 18 total calls

  ARM C  (NOISE FLOOR)      same history, same content, run 6x.
                            Measures pure sampling variance.
                            THIS IS WHAT THE LAST BUILD LACKED.
  ARM B  (THE TEST)         same content, DIFFERENT history (6 real prior
                            states drawn from cognitive_state_history).
  ARM A  (POSITIVE CONTROL) same history, DIFFERENT content.
                            Output MUST differ. If it does not, nothing else
                            in this experiment is interpretable.

History samples are REAL past states, not synthetic, so the effect size is
ecologically valid. Both slots truncate (`prev[:3000]`, `ctx[:7000]`) — all
histories are length-matched into that budget or truncation is the confound.

## Measure

Cosine distance between output embeddings (snowflake-arctic-embed2, the same
model the archive uses), plus per-section distance for SPINE/CORE/SEEKS so a
whole-document average cannot hide a localized effect.

## DEGENERATE CHECK — RUNS FIRST, BEFORE ANY COMPARISON

If all six ARM C outputs are byte-identical, the engine is deterministic
(temp 0). Then the noise floor is exactly 0, the comparison changes character,
and I report that rather than a distance. If ARM C variance is enormous
(mean pairwise distance > ARM A), the instrument is noise and the run is VOID.

## Decision rule — COMMITTED BEFORE LOOKING

  1. ARM A not clearly above ARM C   -> VOID. Instrument cannot detect a
     difference I KNOW exists. Report the void; build nothing.
  2. ARM A > ARM C, but ARM B within noise of ARM C
     -> HISTORY DOES NOT AFFECT COMPRESSION OUTPUT.
        "When needed" is not detectable at this level. Recommendation: keep the
        clock plus F160 guardrails, drop the organ framing AT THIS LEVEL of the
        stack, and say so plainly. This is a real, reportable answer, not a
        failure — and I would rather hand Nate this than another live-looking
        service.
  3. ARM B exceeds ARM C by >= 2 x SD(ARM C)
     -> HISTORY MATTERS. Effect size measured BEFORE any sensor is designed,
        so the sensor can be sized to the signal instead of guessed.

SESOI justification for the 2 x SD(ARM C) bar: a sensor must classify a SINGLE
compression as needed-or-not. That requires the history effect to separate from
sampling noise on ONE measurement, not on average.

## Unit of analysis (Kimi, tonight)

The unit is the RUN. k=6 gives a crude SD on ARM C; that SD is the load-bearing
quantity, so if SD(ARM C) is unstable across the 6 (range > 2x the median
pairwise distance), report UNDERPOWERED and raise k rather than reading a
verdict off it.

## What this deliberately does NOT do

Does not fix `episodic_trace`. Does not write to `cognitive_state`. Does not
rebuild the service. If the answer is (2), none of those should ever happen.

## AMENDMENT 1 — manipulation strength, added 19:31 PDT, BEFORE any run

Materials check passed (7 histories, 7 contexts, all length-matched at the
3000/7000 truncation budgets, so truncation is uniform and not a confound).

But a gap: I have not measured how different the HISTORIES actually are from
each other. Our CCS may be highly stable, in which case consecutive gists are
near-identical and ARM B is a WEAK manipulation. A null would then mean "I
barely varied history," not "history does not matter" — and I would almost
certainly have reported the second.

COMMITTED: before interpreting any arm, compute and report
  - mean pairwise cosine distance among the 7 HISTORIES  (input variation, B)
  - mean pairwise cosine distance among the 7 CONTEXTS   (input variation, A)

Interpretive rule, committed now:
  - If history-input variation is LESS THAN ~1/3 of content-input variation,
    ARM B is underpowered BY CONSTRUCTION. A null is then reported as
    "manipulation too weak to test the question" — NOT as evidence that
    history is irrelevant. The fix would be deliberately distant histories
    (months apart), not more runs.
  - Report input variation for both arms next to output variation always, so
    an effect can be read as a RATIO (output change per unit input change)
    rather than as a raw distance. A raw distance conflates "history matters"
    with "these histories differed a lot."

## AMENDMENT 2 — Kimi's adversarial review. 19:35 PDT, run in progress
## at 4/18, NO outputs inspected. Committed before any result exists.

### (a) The 2*SD bar is softer than I wrote, and can invert on a small floor

SE(s) ~= sigma/sqrt(2(n-1)), so s from k=6 carries ~32% relative error: my
"2*s" is really 1.4-2.7 sigma. Stated, not fixed.

DECLARED: SD(C) is computed from the 15 pairwise distances among 6 outputs.
Those pairs SHARE outputs and are not independent — effective df = 5, not 14.
I use df=5. Treating them as n=15 would understate SD and silently lower my
own bar.

Worse, the bar is scale-free: identical prompts at temp 0.6 may give tiny
distances, so a small SD(C) lets ARM B clear 2*SD(C) with an absolute effect
too small for any single-shot sensor to use — declaring "history matters" on
something unusable.

### (b) PRIMARY STATISTIC CHANGED to match the SESOI

My SESOI is single-shot classification ("is this compression needed?"), so the
statistic must be single-shot too. Primary is now LEAVE-ONE-OUT: given one
output, classify which arm produced it. Report LOO accuracy and d'.

COMMITTED THRESHOLDS, before seeing anything:
  d' >= 2.0  (~84% single-shot)  -> SENSOR-GRADE. Effect usable on one reading.
  1.0 <= d' < 2.0                -> REAL BUT NOT SINGLE-SHOT USABLE. Would
                                    require aggregating several signals; report
                                    as such, do NOT call it a working sensor.
  d' < 1.0                       -> NOT USABLE.

### (c) Report per-HISTORY, never mean(B) alone

One outlier history can carry the mean while 5 of 6 do nothing. A sensor that
fires on 1 of 6 real histories is not history-sensitivity. Per-H responses get
reported individually and the count of H's that individually clear the floor is
reported next to the mean.

### (d) THE BIG ONE — ARM A does not protect against slot inertness

ARM A validates the {session_context} slot. ARM B tests the {previous_state}
slot. So "A above C, B within C" is confounded with SLOT POSITION and is not
evidence about history at all.

NEW ARMS (run after the main 18; the script is resumable):
  B_max  — one maximally distant history, plus one EMPTY {previous_state} slot.
  swap   — put history content into the {session_context} slot once.

DECISION RULE ADDED, and it overrides the original branch 2:
  If B_max (including empty H) lands WITHIN ARM C noise, the {previous_state}
  slot is INERT — the prompt at this temperature cannot read it at all. The
  finding is then "THIS PROMPT CANNOT READ HISTORY," which is a PROMPT-DESIGN
  result and is FIXABLE. **My original "keep the clock, drop the organ framing"
  conclusion would be WRONG in that case** — the organ hypothesis would be
  untested, not refuted. This is the single most important correction.
  If swap MOVES the output, the same content is readable when saliently placed,
  which isolates placement from content and confirms the slot, not the concept,
  is the problem.

### (e) Multiple comparisons

Four endpoints (whole-doc + 3 sections) at 2*SD each is ~20% family-wise false
positive. PRE-COMMITTED PRIMARY: **SEEKS**. Rationale: SPINE is stable by
design (identity), CORE tracks current content, so SEEKS is where carried
orientation should appear if anywhere. Named secondary: REMEMBERS — with the
caveat that high REMEMBERS-to-history similarity may be PARROTING the previous
state rather than being conditioned by it, and parroting is not sensing.
Whole-document cosine is DEMOTED to descriptive: shared-template outputs
compress distances and a document average can bury a localized effect.
