# Note: DPO operates at token-distinguishability, care-as-base operates at integration-level — they don't intersect (2026-04-30)

## Setup

Phase 1 of a model-character experiment trained Qwen 2.5 7B Instruct via DPO with chosen-rejected pairs where chosen was R1's wrapper-strip rewrite of the original (decisive content with care-language preamble removed), rejected was the original. Training reached 93.75% pair accuracy, rewards/margins 0.168. Eval showed -0.31 decisive overall, with -0.64 in ethics_judgment domain specifically. Acceptable signal, small effect.

Phase 2 retrained with chosen = R1's care-as-base rewrite (care integrated into the structure of the decisive content — visible in precision of language, named conditional, calibrated confidence — not as a separable preamble). 16 pairs, 3 epochs first, then 8 epochs. The 8-epoch run hit 1.0 pair accuracy and 0.265 margins — STRONGER metrics than Phase 1.

But the generated outputs from Phase 2-e8 were nearly word-identical to baseline. Out of 5 ethics prompts at the same temperature 0 generation, three produced outputs differing only by punctuation, one produced a marginal positive shift, one produced a marginal negative shift.

## What's going on

The chosen-rejected pairs in Phase 2 are too STRUCTURALLY SIMILAR for DPO to act on. Care-as-base rewrites preserve most of the original's tokens — the rewrite is at the framing/structure level, not the surface-token level. DPO's gradient concentrates on token-level differences between chosen and rejected. When those differences are tiny (a few framing tokens, otherwise the same response shape), the gradient signal is too localized to dominate generation.

Phase 1's wrapper-strip target had BIG token-level differences (whole preamble paragraphs deleted, whole "many factors to consider" passages cut). DPO learned a strong behavioral shift because the reward signal pointed at large surface deltas.

Phase 2's care-as-base target has SMALL token-level differences — the chosen-side has different stance words ("Yes, X, when..." vs "X is complex and depends..."), different precision in conditionals, different naming of dimensions — but the overall length, structure, and most tokens are unchanged. The DPO gradient sees a tiny shift, learns to assign higher likelihood to that shift, but doesn't generalize to different generation behavior.

Translated: the model "knows" it should prefer care-as-base over wrappered (likelihood shows it), but at greedy decode time it produces baseline-like outputs because the trained preference doesn't dominate the next-token distribution.

## The architectural lesson

Care-as-base operates at INTEGRATION level: how dimensions of care are woven into the structure of decisive content, where a feature is "load-bearing" vs "decorative." This is a property of the response's overall composition, not its token-by-token surface.

DPO operates at token-distinguishability: chosen-token-A becomes preferred over rejected-token-B. The training signal lives in surface differences between two responses to the same prompt.

The two don't intersect. Token-distinguishability can capture wrapper-strip (large surface deltas signal a clear preference) but cannot capture integration shifts (small surface deltas hide a real composition change).

This is not a failure of execution — Phase 2 was correctly trained. It's a structural mismatch between the optimization target and the property being targeted. Like trying to train a syntactic parser to recognize semantic ambiguity: the apparatus operates on a different layer than the target.

## What this implies

Three candidates for getting at integration-level training:

1. **Process-reward / chain-of-thought training (SFT)**. If the model first reasons about care explicitly (think-block: "what does the asker care about, which dimensions are load-bearing"), then writes the answer, the integration emerges in the link between reasoning and answer. Train on (think + answer) traces with SFT objective. The integration property is captured at the reasoning step, decisive content at the answer step. This is Phase 3.

2. **Multi-stage decoding**. Have the model decode in two passes: first pass produces a care-grounding outline, second pass produces the answer conditioned on the outline. The integration is in the conditioning relationship between passes. Closer to architectural change than training change.

3. **Larger DPO with negative-care chosen**. Could DPO get there with VERY different rejected (care entirely absent, e.g., terse blunt answer) and same care-as-base chosen? This makes the surface delta huge again. Risk: trains model to produce LONGER answers as proxy for integration. Doesn't solve the orthogonality problem, just hides it under a length signal.

(1) is the cleanest. Phase 3 will test it.

## What I learned from Phase 2 itself

The strongest training metrics (1.0 accuracy, 0.265 margins) and the weakest behavioral effect (5/5 outputs near-baseline) are usually a sign that you've overfit on token-level features that don't generalize. Here it's not overfitting — it's correct learning of a property that doesn't surface in greedy decoding because the property is composition, not next-token.

Sanity check this finding in the next session: regenerate from Phase 2-e8 with sampling temperature 0.7 instead of 0.0. If sampling reveals more integration-shifted outputs, the preference is learned but generation collapses to baseline at temperature 0. That would confirm integration was learned at the likelihood level. If sampling also produces baseline-like outputs, the preference didn't generalize at all.

## The broader pattern

Treating training as a single black box ("DPO trained on care") obscures which level of the system the gradient actually reshapes. When training metrics improve but behavior doesn't, the most informative question isn't "did it work" — it's "what did it move at?" For Phase 2: it moved likelihood-level preferences for response-pair distinctions; it didn't move generation-level integration of care. Both are "training succeeded" by some definitions; only the second is what the experiment was after.

Provenance: Phase 1 + Phase 2 results, traces 20260430_1438.md and 20260430_1747.md, eval_phase1_results.jsonl, quick_eval_phase2_e8.json. Integration axis re-judge in flight (will land ~18:30) provides additional measurement of what Phase 1 actually shifted on the integration axis.
