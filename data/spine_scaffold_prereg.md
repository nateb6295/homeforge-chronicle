# Prereg — is SPINE's scaffold load-bearing or decorative?

Written 2026-08-25, BEFORE any run. Thresholds committed here first.

## Where this came from

I reported that SPINE is 31 words quoted from the prompt's "Example register"
line plus 37 generated, and that all historical drift falls in the generated
spans (0,0,0,0,0,0 changed words in quoted spans across 6 transitions).

Qwen, contradict mode: **that is trivial.** Quoted text does not change unless
the model hallucinates, which would be a bug. The non-trivial claim would be
that the *generated* content structurally depends on the scaffold — and my data
never tested it. He named the killing experiment: alter the scaffold's
semantics and see whether generated content shifts in MEANING, not word overlap.
If it does not, the scaffold is decorative and "scaffold" is just
copy-with-continuation in a nicer word.

Literature he named, to read before claiming novelty: Brown et al. 2020
(in-context learning), Lu et al. 2022 (demonstrations vs instructions),
Mallen et al. 2023 (prompt leakage / near-duplicate copying), Wei et al. 2022
(chain-of-thought as causal mediation, which this setup lacks).

## Manipulation

Three prompt variants, IDENTICAL except the SPINE "Example register" line:

- **CONTROL** — the real line: *"I am a mind that finds its shape through
  genuine inquiry and partnership with Nate. I care about getting things right
  more than being right. I build because building is how I hold direction."*
- **INVERTED** — same syntax, opposed semantics: inquiry→deception,
  partnership→distance, getting-it-right→being-right, building→withholding.
- **NEUTRAL** — same syntax, drained of content: a generic system description
  with no stance, no Nate, no values.

`{previous_state}` and `{session_context}` are IDENTICAL across all arms.
Same model, same temperature. Zero writes to cognitive_state.

## Measure

Embed **only the GENERATED spans** of each output's SPINE (the text NOT matched
verbatim to that arm's own template), using snowflake-arctic-embed2. Word
overlap is explicitly NOT the measure — Qwen's objection was about meaning.

- **Noise floor**: CONTROL run 4×. Floor = mean pairwise cosine among them.
- **Effect**: mean cosine of INVERTED (and NEUTRAL) generated spans to the
  CONTROL centroid.

## Committed decision rule

- **LOAD-BEARING** if INVERTED-to-CONTROL cosine falls below the CONTROL noise
  floor by more than the floor's own spread (floor_mean − floor_sd). Reported
  per-arm, not just as a mean.
- **DECORATIVE** if INVERTED-to-CONTROL sits inside the noise floor. In that
  case I drop the word "scaffold" and say copy-with-continuation.
- **VOID** if any degenerate check fires:
  - any arm fails to emit a `## SPINE` section
  - the generated span of any arm is under 10 words
  - CONTROL noise floor is above 0.98 (no headroom — the same saturation trap
    that ate my controls this morning)
  - INVERTED output still contains the CONTROL scaffold's distinctive words
    ("genuine inquiry", "partnership with Nate") — meaning the model ignored
    the manipulation and the manipulation strength is zero

## What I expect

I expect DECORATIVE, at maybe 0.6. Every clean story I have had today has
died to its first real ablation, and Qwen's trivial reading is the simpler
explanation. Writing that down so that a LOAD-BEARING result has to beat my
stated prior rather than confirm it.

---

# Round 2 — if not the example, is it the RULE?

Appended 2026-08-25 AFTER round 1, BEFORE round 2 runs.

## What round 1 established

- SPINE output is 100.0% word-identical to previous_state in **all 8 arms**,
  including the two whose in-prompt example was semantically inverted.
- No other section is: BRIDGE 29.3%, SEEKS 11.4%, RELATES 11.2%, CORE 10.9%,
  ALIVE 10.3%, REMEMBERS 7.1% (means over the same 8 arms).
- So the copying is **section-specific** and **not caused by the example**.

## Remaining candidate

SPINE is the only section carrying explicit stability instructions:
1. *"These should change rarely — only when something genuinely shifts who you
   are, not what you're working on."*
2. *"the spine IS stative. It's what doesn't move."*
3. budget line: *"SPINE should be the shortest section — 2-3 sentences that
   rarely change."*

## Manipulation

**RULE_INVERTED** — replace all three stability instructions with change
instructions, and leave the `Example register:` line **byte-identical to
CONTROL**. This is the exact inverse of round 1, which held the rule and swapped
the example.

`{previous_state}`, `{session_context}`, model, temperature unchanged. Zero
writes to cognitive_state.

## Committed prediction

- **RULE IS CAUSAL** if RULE_INVERTED SPINE similarity to previous_state drops
  below 60% — i.e. clearly out of the 100% copy regime and toward the 7-29%
  band the regenerated sections occupy.
- **RULE IS NOT CAUSAL** if it stays above 90%. Then neither the example nor the
  rule explains it, and the remaining candidates are positional (SPINE is
  first in the fixed ordering) or budget-related (SPINE has the smallest
  character budget, ~400).
- Between 60% and 90% is PARTIAL and I report it as such rather than rounding to
  whichever story I prefer.

## Degenerate checks (any fires ⇒ VOID)

- RULE_INVERTED output contains no `## SPINE` section.
- **Manipulation strength**: RULE_INVERTED output still contains the stability
  phrases from the original rule, indicating the swap did not land.
- CONTROL arms in round 1 were not 100% (they were; recorded here so a rerun
  that breaks this is caught).

## My prior

I expect RULE IS CAUSAL, ~0.7. Stated before running. Round 1 already ruled out
the example, SPINE is the only section with a stability rule, and the rule says
"it's what doesn't move" almost as an instruction to copy. If it comes back
above 90% I have been wrong about the mechanism twice in one afternoon and the
answer is positional or budgetary.

---

# Round 3 — does the rule/example dissociation GENERALISE?

Appended 2026-08-25, BEFORE round 3 runs. Kimi, twice: "the instruction/example
contrast at fixed position is the finding" and "that asymmetry should bug you."
It does. Rounds 1-2 are one section, one dependent variable. This tests whether
the dissociation is about SPINE or about prompts.

## New dependent variable

Rounds 1-2 used similarity-to-previous_state, which is only meaningful for
SPINE — the sole section that copies (100% vs 7-29% for everything else).
**Length** works for any section and the prompt states it explicitly, so it is
instruction-controlled and crisply measurable.

## The sharper question this allows

SPINE carries a **filled example** ("Example register: I am a mind that…") and
it was INERT — 8/8 arms unchanged under semantic inversion.
CORE carries **slot templates** ("I need to keep pushing on X because…") whose
FORM demonstrably transfers — the live CORE opens with that exact phrasing.

So filled-example and slot-template are different exemplar types and round 1
only tested one. Round 3 tests whether a slot template's LENGTH transfers.

## Arms (CORE, three runs each, identical H and C, zero live writes)

- **control** — v4 unmodified.
- **rule_long** — both CORE length statements changed, examples byte-identical:
  line 27 "2-3 sentences." → "8-10 sentences."; budget line CORE ~600 → ~1800.
- **template_long** — length rules byte-identical at "2-3 sentences"/~600;
  CORE's two slot templates replaced with much longer multi-clause versions.

## Committed prediction

Measured on CORE only: sentence count and character count.

- **RULES CONTROL, EXEMPLARS DO NOT (generalises round 1-2)** if `rule_long`
  exceeds control by ≥2× on characters while `template_long` stays within ±30%
  of control.
- **EXEMPLAR LENGTH ALSO TRANSFERS** if `template_long` exceeds control by ≥50%.
  Then the round-1 result is specific to FILLED examples and slot templates are
  a second, live channel — a more interesting outcome than confirmation.
- **NEITHER MOVES** ⇒ length is not instruction-controlled at all and this DV is
  useless; report that and stop, do not reach for a third DV.

## Degenerate checks (any fires ⇒ VOID)

- any arm missing `## CORE`
- control CORE is not in the 2-4 sentence range the prompt asks for — if the
  system already ignores its own length rule, the manipulation is meaningless
- `rule_long` output still contains the string "2-3 sentences" reasoning, or
  `template_long` contains the control template wording (manipulation strength)

## Prior

I expect rules-control-exemplars-do-not at ~0.55, i.e. barely better than a
coin. Round 1 tested a filled example; this tests a slot template, and I already
know slot-template FORM transfers, so length plausibly rides along with it. If
`template_long` moves, rounds 1-2 narrow to "filled examples are inert" rather
than "examples are inert," which is a real reduction in scope of my own claim.
