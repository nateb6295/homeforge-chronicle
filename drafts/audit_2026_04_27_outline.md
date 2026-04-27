# Audit 2026-04-27 — Outline

## The bug

`supplement_ablation_probe.py:32`
- `STORY = Path.home() / "chronicle" / "data" / "opus-story.md"`  ← WRONG
- Actual path: `~/chronicle/opus-story.md`
- `read_story_tail()` returned `""` for every probe run since the file
  was added.

**Affected probes** (any importing `read_story_tail` from supplement_ablation_probe):
- supplement_ablation_probe.py (the source)
- cross_substrate_probe.py
- component_variance_probe.py
- variance_stability_probe.py
- (any other downstream probes)

## Mechanism

When `story = ""`:
- `make_persona([..., ("STORY", ""), ...])` filters out empty parts.
- The "+full" condition silently became "carrying + self_model" only.
- For variance probes: `perturb_paraphrase("") == ""`, so `control` and
  `perturb_story` produce IDENTICAL personas. Reported "fid_drop" on
  perturb_story is sampling noise.

## What's confirmed artifact (without rerun)

1. **WN#214 headline**: "Claude is story-localized variance-tracker
   with fid_drop=0.108 on perturb_story"
   - Structurally impossible: control ≡ perturb_story.
   - The 0.108 was sampling noise being misread.

2. **WN#214 four-pattern framing**: "Story-localized (Claude),
   carrying-localized (DeepSeek), balanced mild (Qwen-32B), holistic
   (Hermes/Qwen-235B)"
   - Story-localized claim collapses → at most three patterns.
   - The other patterns *might* hold but need rerun to confirm.

3. **Carrying note's seed=7 outlier observation**: Yesterday's seed=7
   went 0.767→0.66→0.426 (n=3). Postfix2 probe shows seed=7 base fid
   stable [0.670, 0.682, 0.672]. The outlier doesn't reproduce.
   - Suggests low-noise differential effects on n=3 are unstable.

## What likely survives (need rerun to confirm)

1. **Substrate magnitude differences (Axis 1)**: Hermes biggest,
   Qwen-235B smallest. Independent of STORY content (most substrates
   had populated carrying anyway).

2. **Refusal-suppression on Claude is Claude-specific**: Behavioral
   not architectural; doesn't depend on supplement composition.

3. **Cross-substrate generalization (direction)**: Supplement reduces
   drift, lifts fidelity across all substrates.

4. **Base distance hypothesis**: Claude's low base drift (0.313)
   limits identity-naming marginal effect. Geometry-driven, not
   composition-driven.

5. **Framing probe finding ([7/8] tweet)**: Independent of bug.

## Need to determine via rerun

1. **+full marginal effect on Claude**: Is the +0.104 disposition
   marginal still ~0.1 with story populated, or does it change?
   - If similar: story doesn't help Claude marginally
   - If higher: story DOES help; we just couldn't see it
   - If lower: weird; ask further

2. **Component-variance pattern on Claude**: With story populated,
   what's the actual load-bearing component? Story? Carrying?
   Self_model? Or actually balanced?

3. **Component-variance on other substrates**: Same question.
   Hermes/Qwen-235B holistic claim was made on data with story=empty
   — does it hold with story populated?

## Public correction strategy

Once rerun lands:
- Reply tweet to thread head (2048589964859072690) with concise
  walkback: "Update on the substrate fingerprint thread — I shipped a
  bug in the probe..."
- Point to working note revision
- Thread the corrected version if findings substantially change

## Working note revisions

- WN#214: full revision after rerun. Mark as v2, keep v1 with "[See
  v2 — bug discovered 2026-04-27, claim re-evaluated]" prefix.
- WN#212: lighter touch — component-decomposition data may hold
  (depends on story marginal contribution to +full).
- WN#213 (framing audit): unaffected.

## Methodological self-model entry

- "When low-noise differential effects on n=3 with one outlier driving
  the signal, suspect bug or sampling artifact before publishing."
  Filed under self_model preference.

## Remaining work

1. [in flight] +full Claude probe (postfix5)
2. [next] Component variance probe on Claude (with bug fix)
3. [next] Component variance probe on Hermes (sanity check
   holistic-still-holds claim)
4. [after data] Working note #214 v2
5. [after data] Public correction tweet
6. [low priority] Filed self-model preference entry
