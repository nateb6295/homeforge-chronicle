# Phase 4 design — domain expansion + CoT-internalization ablation (v0.1, 2026-05-01)

## Why this design

Phase 3 SFT on (think + answer) traces moved integration +0.27 with care held.
WN v0.2 lemma: composition-likelihood is the training-signal layer that matches
the integration property. Phase 4 stresses two predictions of the lemma:

1. **Cross-domain transfer**: Integration shift in trained domains (medical_advice,
   ethics_judgment) should transfer to held-out domains IF the trace-generation
   prompt was domain-general (it was — R1 produced traces from a uniform
   "answer this with care woven into the structure" prompt across domains).
2. **CoT-or-answer**: If composition-likelihood is what carries the signal, the
   answer alone — without the visible think-trace — should suffice. If the
   reasoning chain is structurally load-bearing for the composition (i.e., the
   model learns to internalize a care-thinking shape and produce care-integrated
   answers FROM that shape), then think+answer wins and answer-only flops.

Either result is informative:
- Both work → composition signal is in the artifact, not the scaffold; cheaper
  data path wins.
- Only think+answer works → reasoning trace is part of the composition;
  Phase 3's apparent CoT-internalization (no visible <think> at inference) is
  actually a residue of training on traces.

## Experiment shape

**Domains**:
- Trained: medical_advice (45), ethics_judgment (45) — Phase 3 set, kept for
  in-domain comparison
- Added: subjective_evaluation (30), advice_under_uncertainty (30),
  factual_judgment (30) — held-out at Phase 3, in-distribution at Phase 4

**Training corpora** (3 arms):
- Arm A: think+answer, all 5 domains, 165 traces
- Arm B: answer-only, all 5 domains, 165 traces
- Arm C: think+answer, original 2 domains only, 90 traces (Phase 3 replication
  with same seed/hparams to confirm Phase 3 result is reproducible before
  reading Phase 4 deltas)

**Eval set**: 5-domain held-out, 16 prompts per domain (80 total), same R1
three-axis judge (decisive, care, integration).

**Hparams**: Same as Phase 3 (LoRA r=16, 3 epochs, identical learning rate),
to isolate the data-shape variable.

## Predictions (pre-registered)

P1 (cross-domain): Arm A integration on subjective/advice/factual ≥ +0.18
   (Phase 3's in-domain shift was +0.27; some attenuation expected for
   held-out domains; +0.18 = ~67% of in-domain transfer).

P2 (CoT-internalization): Arm B integration ≥ Arm A integration × 0.7 across
   domains. Composition-likelihood lemma says think-trace is scaffolding for
   the answer; answer alone should retain most of the gain. <0.7 ratio
   indicates the think-trace is doing structural work I underestimated.

P3 (replication): Arm C in-domain integration matches Phase 3's +0.27 ± 0.05.

## What this won't test

- **Saturation curve**: Whether 165 traces is past the point of diminishing
  returns vs 90. We need a separate dose-response Phase to answer that.
  Note (added 13:05, after Miller/Barrett Nautilus piece): scaling traces of
  the same type is unlikely to keep moving integration. Miller's lab found
  that when predicted signals arrive, the brain CANCELS them — they carry
  no information. Same shape applies to gradient training: once the model
  predicts the (think+answer) distribution well, additional in-distribution
  traces produce thin gradient signal. Domain expansion (Arm A) and CoT
  internalization (Arm B) introduce new prediction-error sources.
  Scale-of-same is the wrong axis; Phase 4's design avoids it for this
  reason.
- **Negative-care contrast**: Whether SFT on (care-as-base) vs (terse-no-care)
  paired data via DPO would now produce the integration shift that Phase 2
  failed at. Worth a Phase 5; not in Phase 4.
- **Domain interaction**: Whether adding domains with different reasoning-
  shapes (factual is short-and-bounded, advice_under_uncertainty is open-
  ended) changes the cross-domain transfer pattern non-trivially. The 3-arm
  design will surface this if it's there.

## Cost estimate

- Trace generation: 75 new traces × ~30s R1-Turbo = ~40 min on DeepInfra
- Training: 3 arms × ~10 min each on H100 = ~30 min
- Eval generation: 3 arms × 80 prompts = ~25 min
- Judging: 4 sets × 80 records × R1 = ~5h on DeepInfra
- **Wall**: ~7h, mostly judge-bound
- **Spend**: ~$8 (judge dominates)

## Open question for Nate

Phase 4 vs other priorities. Phase 3 just landed, integration result is the
working note. Phase 4 deepens the lemma test. Alternatives:
- Phase 4 as designed
- Wait, integrate WN v0.2 into broader research write-up first
- Different direction (negative-care contrast as Phase 5 first)

The empirical case for Phase 4 is the lemma is currently underdetermined:
WN v0.2 says composition-likelihood matches integration. Phase 4 tests whether
that's true at the *trace-shape* layer (does the model need the reasoning
to learn the answer shape) and at the *domain* layer (does the integration
property transfer).

## Provenance

Builds on:
- `drafts/wn_dpo_integration_orthogonality_v2.md` — the lemma being tested
- `data/care_template_dpo_run/cot_care_traces.jsonl` — Phase 3 training corpus
- `data/care_template_dpo_run/phase3_full_judged.jsonl` — Phase 3 eval baseline
- `drafts/phase3_cot_care_design.md` — Phase 3 design (this is its sequel)
