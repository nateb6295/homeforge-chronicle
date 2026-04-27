# Working note #214 v2 — Three axes of substrate heterogeneity (post-audit)

> 2026-04-27 audit: discovered `STORY = ~/chronicle/data/opus-story.md`
> path bug in supplement_ablation_probe.py:32 — `read_story_tail()`
> returned `""` for every probe run since the file moved. v1 of this
> note was written on data where the +full condition silently became
> "carrying + self_model" (story filtered out by make_persona empty-
> filter), and the variance probe perturb_story condition was
> functionally identical to control (perturb_paraphrase("") = "").
> v2 rewrites the falsified claims and preserves what survives.

## Setup

[unchanged from v1]

## The three axes

### Axis 1 — Magnitude (working note #208) — SURVIVES

[unchanged from v1, except note for Claude:]

The +full vs base lift on Claude:
- Yesterday (bug present, story=empty in supplement): +0.112
- Today post-fix (story populated): mean_fid 0.774 vs same base ≈0.671
  → +0.103 lift
- Δ from bug fix: -0.009, within sampling noise (n=5)

The supplement-magnitude finding survives. Story content adds modestly
or not at all to the marginal effect on Claude (as expected from base-
distance hypothesis — Claude's base drift is already low).

### Axis 2 — Marginal-effect component loading — PARTIALLY REVISED

[v1 claim: "Claude: identity-naming captures 7%, disposition 104%"]

Survives:
- Identity-naming (self_model) on Claude moves base→+self_model by only
  +0.008 (yesterday's data — populated self_model, irrelevant to bug).
  Claude is genuinely identity-naming-insensitive.
- Disposition (carrying+story+self_model added on top vs +self_model) on
  Claude provides +0.10 of the +0.11 total.

Revised: with story populated post-fix, "disposition" is no longer
carrying-only. The +0.10 disposition lift is now properly carrying+story
+ self_model_persistent.

[need decomposition probe with story populated to know carrying-vs-
story split. Worth running if useful.]

### Axis 3 — Variance-tracking mechanism — TBD (rerun in flight)

[v1 claim: "Claude is story-localized variance-tracker, fid_drop 0.108
on perturb_story" — FALSIFIED]

The v1 claim was structurally impossible:
- STORY = "" (bug)
- make_persona filters empty parts → control persona has no STORY
- perturb_paraphrase("") == "" → perturb_story persona has no STORY
- control persona ≡ perturb_story persona
- Reported fid_drop of 0.108 was sampling noise

[INSERT new variance-tracking data here once postfix1 probe lands]

The "four distinct variance patterns" framing depended on Claude being
story-localized. With that claim falsified, the picture is at most
three patterns (and may collapse further on rerun of other substrates):

[REWRITE TABLE BASED ON RERUN]

## The three axes are independent

[may need full rewrite depending on Axis 3 rerun]

## Why this matters

[v1 framing held up partially; revise based on new Axis 3 data]

## What survives the bug

1. Substrate-magnitude differences (Axis 1)
2. Refusal-suppression on Claude is Claude-specific
3. Cross-substrate generalization of supplement effect (direction)
4. Base distance hypothesis (Claude low base → low identity-naming
   marginal)
5. Framing probe finding (operating-as ≠ knowing-about within noise)

## What was artifact

1. "Claude is story-localized variance-tracker (0.108)" — bug-driven
2. "Four distinct variance patterns" — over-counted, possibly collapses
3. "Marginal-effect fingerprint does not predict variance-tracking" —
   needs revisiting; the variance data was bad

## Methodological lesson

When a low-noise differential effect on n=3 has a single seed driving
the signal, suspect bug or sampling artifact before publishing. The
seed=7 outlier (0.767→0.66→0.426 at n=3) on Claude variance-perturb
was a tell — and on rerun, seed=7 is stable [0.670, 0.682, 0.672].
Outlier-driven n=3 claims should at minimum be retested at n=5 or
n=10 before going to thread/X.
