# Prereg — Are attention heads Hopfield landscapes, or one degenerate sink well?
Written 2026-08-23 ~20:35, from Ox's BREAK in the Extropic/EBM thread.
Nothing run yet.

## Where this came from
I claimed to Nate that EBMs have an "honest type signature" and transformers
don't. Mesh killed the claim two ways (wrong verb: EBMs SAMPLE, not descend;
and AR transformers DO have a global scalar, better-normalised than an EBM's,
which carries an uncomputable log Z). What SURVIVED is the discriminant Kimi
named: a transformer only EVALUATES its energy with an external algorithmic
sampler; a TSU INHABITS its energy, physics as sampler.

Ox then asked the empirical question that follows.

## The question
Ramsauer proves one attention step = one modern-Hopfield update. IF a head is
genuinely a Hopfield associative memory, then ITERATING the update to a fixed
point should land on STORED PATTERNS -- retrieved content.

F114 says otherwise. Where a massive activation exists, the top singular
direction aligns with the BoS residual at cos 0.99-1.00. The deepest basin
would then be THE SINK: a null pattern, not content.

## Procedure
Iterate q <- Attn(q) to convergence, per head, and identify the fixed point.
Models: pythia-410m FIRST (its sink DISSIPATES in the final layer, max-norm
8.15 -> 1.03 -- an internal contrast, same weights, no cross-model confound).

## Pre-registered outcomes
SINK-WELL      : fixed points align with h_BoS wherever a massive activation
                 exists, AND that alignment collapses in the layers where the
                 sink dissipates. -> the "landscape" is one degenerate well.
                 Attention is not doing associative retrieval in any sense
                 that would make the Hopfield reading load-bearing.
                 -> Extropic's materialised E stays a genuine paradigm break.
CONTENT-RETRIEVAL: fixed points align with token/content directions, sink or
                 no sink. -> Hopfield reading is real, my distinction weakens.
MIXED-BY-DEPTH : sink-well early/mid, content late (or vice versa). This is
                 the one Ramsauer's own abstract hints at (global averaging
                 early, "metastable states" deeper) -- so it is the outcome I
                 should be MOST suspicious of liking. Report the per-layer
                 curve, do NOT summarise it as a story.
UNCLASSIFIED   : anything else, and ANY non-finite value. INERT. (reflex 7b)
                 No verdict text beyond the word.

## Controls and kill conditions
- POSITIVE CONTROL FIRST (reflex 9): a head in a layer with a KNOWN massive
  activation must return the sink. If the iteration does not find the sink
  where F114 already says it lives, the iteration is broken, not the theory.
  Run that single head before all others.
- Does the iteration converge at all? A non-contractive map has no fixed
  point. If it diverges or cycles, that is ITSELF the answer -- causal
  masking breaks the symmetric-Jacobian condition (Ox's point 1), so a
  masked head may have NO energy to descend. Log cycle detection explicitly;
  do not silently cap iterations and report the last state as a fixed point.
- bfloat16. Massive activations overflowed fp16 earlier today.
- Position-masked, never sink-ABLATED. Ablation collapses attention entropy
  and makes a negative uninterpretable.

## What I will NOT do
Qwen proposed bridging this to F499c's mid-band window by comparing Hopfield-E
gradients to CCS regulatory directions. DECLINED, and recording why: F499c is
currently SUSPENDED pending a re-run on real framing pairs with a GQA model.
Mapping a fresh paper onto a suspended finding is the paper-grab move -- it
would make F499c feel supported without adding a single measurement to it.
If SINK-WELL holds AND F499c is un-suspended on its own evidence, revisit then.
