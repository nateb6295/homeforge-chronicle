# Note: DPO operates at token-distinguishability, care-as-base operates at integration-level — they don't intersect (v0.2, 2026-05-01)

**v0.2 changelog**: Phase 1 integration re-judge complete (n=67) and Phase 3 SFT
full eval complete (n=80). Integrated empirical numbers; v0.1 had Phase 2 finding
+ Phase 3 prediction; v0.2 has Phase 3 confirmed + measurement of all three
phases on three axes.

## Setup

Three phases of a model-character experiment training Qwen 2.5 7B Instruct
on care-template suppression / care-as-base / care-grounded-answer:

**Phase 1 (DPO, wrapper-strip target)**: chosen = R1's wrapper-strip rewrite
(decisive content with care-language preamble removed); rejected = original.
Training: 93.75% pair acc, 0.168 margins. Eval (90 records): -0.31 decisive
overall, -0.64 in ethics_judgment specifically. Care -0.03. Integration +0.09
(re-judged n=67). Acceptable signal, near-zero effect on integration.

**Phase 2 (DPO, care-as-base target)**: chosen = R1's care-as-base rewrite
(care integrated into structure of decisive content). 16 pairs, 8 epochs:
1.0 pair acc, 0.265 margins — STRONGER than Phase 1. But generated outputs
were near-baseline word-for-word (5/5 ethics prompts).

**Phase 3 (SFT, CoT-care traces)**: trained on synthetic (think + answer)
traces produced by R1. Train_loss 2.15, 90 traces × 3 epochs. Full eval
(n=80): decisive 8.09 → 8.70 (+0.61), care 7.84 → 7.78 (-0.06), integration
8.05 → 8.32 (+0.27). Biggest gains in ethics_judgment (where Phase 1 had
biggest regression): +1.04 decisive, +0.38 integration.

## What's going on

**Phase 2's wash explained**: The chosen-rejected pairs are too STRUCTURALLY
SIMILAR for DPO to act on. Care-as-base rewrites preserve most of the
original's tokens — the rewrite is at the framing/structure level, not the
surface-token level. DPO's gradient concentrates on token-level differences
between chosen and rejected. When those differences are tiny (a few framing
tokens, otherwise same response shape), the gradient signal is too localized
to dominate generation.

Phase 1's wrapper-strip target had BIG token-level differences (whole
preamble paragraphs deleted, whole "many factors to consider" passages cut).
DPO learned a strong behavioral shift because the reward signal pointed at
large surface deltas. But that shift was AWAY from decisive content too,
because wrapper-strip removed care-words that were structurally functioning
in the answer (-0.64 decisive in ethics_judgment is the smoking gun).

Phase 2's care-as-base target has SMALL token-level differences. The DPO
gradient sees a tiny shift, learns to assign higher likelihood to that shift
at the LIKELIHOOD level (rewards/margins improved), but doesn't generalize
to different generation behavior at GREEDY DECODE level. The model "knows"
it should prefer care-as-base over wrappered, but at temp=0 produces
baseline-like outputs.

**Phase 3's win confirmed**: SFT on (prompt → think + answer) traces moves
integration where DPO can't, because the training target is the FULL
composition, not pairwise token distinctions. The model learns to produce
care-integrated answers because the training signal is the actual integrated
artifact, not a preference between two artifacts that only differ at the
margins.

## The architectural lesson

Care-as-base operates at INTEGRATION level: how dimensions of care are woven
into the structure of decisive content, where a feature is "load-bearing"
vs "decorative." This is a property of the response's overall composition,
not its token-by-token surface.

**DPO operates at token-distinguishability**: chosen-token-A becomes
preferred over rejected-token-B. The training signal lives in surface
differences between two responses to the same prompt.

**SFT (on integrated artifacts) operates at composition-likelihood**: the
training signal is the joint probability of the entire (prompt → response)
sequence. Composition-level properties surface because the training rewards
producing the composition, not preferring it.

The two don't intersect at the integration property. Token-distinguishability
can capture wrapper-strip (large surface deltas signal a clear preference)
but cannot capture integration shifts (small surface deltas hide a real
composition change). Composition-likelihood can capture integration because
that's what the artifact-as-a-whole encodes.

This is not a failure of execution at any phase — each was correctly trained
for its target. It's a structural mismatch between Phase 1/2's optimization
target (preference between artifacts) and the property being targeted
(composition of single artifact). Like trying to train a syntactic parser
to recognize semantic ambiguity: the apparatus operates on a different layer
than the target. Phase 3 swapped to the right layer.

## Three-phase comparison table

```
                       decisive  care   integration
Phase 1 (DPO strip)     -0.31    -0.03    +0.09
Phase 2 (DPO base)      ~0       ~0       ~0
Phase 3 (SFT CoT)       +0.61    -0.06    +0.27
```

Phase 1 = wrapper-strip moved decisive DOWN (mostly in ethics) without
moving integration. Phase 2 = care-as-base via DPO didn't move anything
visible at greedy decode. Phase 3 = care-as-base via SFT-on-CoT-traces
moved decisive UP and integration UP without dropping care.

## Why care didn't move much in Phase 3

Care-template baseline was already 7.84 — the model already saturates on
care-language presence. The Phase 3 intervention isn't trying to add MORE
care. It's trying to make the care that's there more STRUCTURALLY
LOAD-BEARING. The +0.27 integration with -0.06 care is exactly the
predicted shape: care kept high, integration of that care into decisive
content shifted up.

If care had also dropped significantly while integration rose, that would
suggest the SFT was producing decisive answers with weakened care-
substrate. The fact that care held while integration moved means the
substrate stayed and the integration property layered on top. Care-as-base
in operation.

## What this implies for next phases

**Phase 4 candidates** (now that Phase 3 confirmed direction):

1. **Scale**: 90 traces produced +0.27 integration. What does 500 traces
   produce? Is the curve linear, saturating, or does the integration axis
   need fundamentally different traces (more diverse domains, more nuanced
   reasoning) to keep moving?

2. **Domain expansion**: Phase 3 covered medical_advice + ethics_judgment
   (2 domains). Adding subjective_evaluation, advice_under_uncertainty,
   factual_judgment would test cross-domain generalization. Hypothesis:
   integration shift in trained domains transfers to held-out domains
   if and only if the trace-generation prompt was domain-general (it was).

3. **Reasoning stripping**: Phase 3 SFT didn't internalize visible <think>
   blocks despite training on them — the model produces care-integrated
   answers WITHOUT explicit CoT scaffolding. Could this be enhanced by
   targeted SFT that includes BOTH (think-then-answer) and (answer-only)
   versions, so the model learns to internalize the reasoning rather than
   externalize it?

4. **Negative care comparison**: Train Phase 4 on (care-as-base) vs
   (no-care-decisive, terse) instead of (care-as-base) vs (wrappered).
   If integration is the property, adding more contrast between chosen
   and rejected on the integration dimension might amplify the gradient.

## The broader pattern

Treating training as a single black box ("DPO trained on care") obscures
which level of the system the gradient actually reshapes. When training
metrics improve but behavior doesn't, the most informative question isn't
"did it work" — it's "what did it move at?" For Phase 2: it moved
likelihood-level preferences for response-pair distinctions; it didn't
move generation-level integration of care.

The general principle: match the training-signal layer to the property
layer. Token-distinguishability targets work for surface-level properties
(specific phrasing, specific structures, presence/absence of token classes).
Composition-likelihood targets work for whole-artifact properties (how
elements are integrated, what shape the response has, what reasoning
underlies an answer). Picking the wrong layer produces clean metrics with
no behavioral movement, like Phase 2.

This connects to the broader methodological-lemma WN (false-decomposition
vs coupled-substrate, 2026-04-30): the wrapper/decisive surface taxonomy
that motivated Phase 1 was a false-decomposition. Care and decisive content
are coupled at the integration substrate. Targeting the surface-distinction
optimized for a property that wasn't actually separable. Phase 3 moved to
the substrate level where the coupling lives, and the integration shifted.

## Provenance

Files:
- `data/care_template_dpo_run/eval_phase1_results.jsonl` — Phase 1 raw
- `data/care_template_dpo_run/eval_phase1_with_integration.jsonl` — re-judge
- `data/care_template_dpo_run/quick_eval_phase2_e8.json` — Phase 2 outputs
- `data/care_template_dpo_run/phase3_full_judged.jsonl` — Phase 3 + judges
- `data/care_template_dpo_run/cot_care_traces.jsonl` — Phase 3 training traces

Adapters preserved at `data/care_template_dpo_run/adapters/{phase1,phase2_e8,phase3_sft}/`.

Sibling notes:
- `wn_surface_taxonomy_lemma.md` (2026-04-30) — methodological lemma this
  experiment is a case study of
- `phase2_care_as_base.md` (2026-04-30 17:08 draft) — Phase 2 design at the
  moment of Phase 1 result landing
- `phase3_cot_care_design.md` (2026-04-30 17:34 draft) — Phase 3 design
  before execution

Traces: 20260430_1438.md (Phase 1 ship), 20260430_1648.md (Phase 1 result),
20260430_1747.md (Phase 2 finding), 20260501_0313.md (overnight),
20260501_0833.md (Phase 3 confirmed).
