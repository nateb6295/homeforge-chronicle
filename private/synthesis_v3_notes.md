# Synthesis v3 — the thesis broke in a better direction

*Sunday evening, 20260412. Working note for morning.*

## What broke

The v2 draft's central claim was: "grokking drives function-sensitivity
into a small set of specific scalar parameters, architecturally
invariant (attn.out_proj.bias + embeddings), with the hero scalar
always living on the equals-token row across tasks."

The ablation tests tonight split that apart at two levels.

### Single-scalar ablation: null
Zeroing the hero scalar — add, sub, or mul — does not collapse accuracy.
Val stays at 1.0 across all three. The "hero" framing as a causally
load-bearing parameter was wrong. Gradient concentration ≠ single-point
causal dependence.

### Row-97 ablation: splits the tasks apart
- Add: row 97 knockout → 0.72 (big drop)
- Sub: row 97 knockout → 0.51 (massive drop)
- Mul: row 97 knockout → 0.95 (barely any drop)

Same for attn.out_proj.bias whole-tensor knockout:
- Add: 0.44, Sub: 0.51, Mul: 0.93

Random digit-row knockout: ~0.98 for all three (no drop). So for
add/sub the row-97 dependency is real and asymmetric. For mul, even
though the *gradient concentration signature* looks identical to
add/sub, the *causal dependency* doesn't follow.

## What this means for the paper

The thesis can no longer be "structural law: concentration always lands
on the equals-token row." It has to be something like:

**"Distributional concentration reproduces across tasks.
Causal-computation-locus does not."**

Two observations worth more than one clean law:

1. **Concentration signature is task-invariant** (top-0.1% fraction 47-52%,
   max/mean 1400-2500x, same anatomical tensors, no FFN). Reproducible
   across 3 tasks, 3 seeds on addition.

2. **Causal locus is task-dependent**. Tasks with cleaner group structure
   (mul: cyclic multiplicative group) distribute the computation more
   broadly than tasks with less-clean structure (add/sub). The gradient
   concentrates on similar parameters in all cases, but the model's
   dependence on those parameters differs.

That's a more interesting paper. It undermines the appeal of any clean
"where does the computation live" story and suggests concentration
signatures at the gradient level can mislead about causal structure.

## What I'd want to add

1. **Fourier sparsity check on tok_emb row 97** across the three tasks.
   If add/sub show sparse Fourier structure in that row (a few large
   frequency components) and mul shows dense/distributed, that would
   support the "cleaner basis → more distributed representation" story.
   20-line script.

2. **Progressive row ablation for mul**: what fraction of *which* rows
   does mul depend on? If mul has its load spread across many tok_emb
   rows (not just row 97), ablating multiple rows together should
   eventually collapse it. That would localize mul's answer-storage
   rather than claim it's nowhere.

3. **The cross-seed result needs to be redone at the causal level.**
   I only did cross-seed for addition and only at the concentration
   level. The causal-ablation picture per-seed would tell me if the
   row-97 dependency in add/sub is seed-invariant or a seed-0 quirk.

## For morning cold-read

Don't revive v2. It's got the distributional story right but the causal
story wrong, and rewriting around a wrong frame will anchor me. Start
from these notes. The paper is probably called something like
"Gradient concentration and causal locus dissociate across grokked
tasks in small transformers." It's a *negative* result on the clean
"where does it live" narrative, which makes it exactly the kind of
result that tightens the field.

Venue: mech-interp blog or Distill-style. Not a full arxiv paper
without the seed cross-check + Fourier analysis + progressive ablation.
Maybe three weeks of work. Which is fine. It's the right size for
what I have.

## One more note on process

I almost didn't run the single-scalar ablation tonight. I was going to
save it for "later" because the row-97-across-tasks finding felt
complete as a story. Running it was the difference between a paper
that would collapse in review and a paper that survives because I
already know where it's soft. Catch-your-own-mistakes-early is worth
tired eyes.
