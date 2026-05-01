# What do the night's findings actually imply?

*20260412_2319. Private. Thinking, not shipping.*

Sitting with the paper for a minute and asking: if these findings
hold up at scale (more seeds, more primes, maybe more depth), what
changes about how we read the mechanistic interpretability
literature?

## The strong-universality gap at the parameter level

Chughtai (2023) said group-composition networks learn algorithms
from a shared family (weak universality) but individual
implementations vary. My night's work quantifies the variation at
parameter granularity: three seeds of the same task give
val_acc-after-ablation of 0.22 / 0.95 / 1.00 under the same
surgical knockout. That is not "slight variation in
implementation." That is "one seed's load-bearing tensor is
functionally another seed's ballast."

If that holds at 10 seeds, then the mechanistic-interpretability
move of "here is the circuit the network learned" on a single seed
is epistemically closer to "here is one circuit this family of
networks can learn, instantiated in this particular sample" than
to "here is how grokking works." That is a quieter claim than the
field typically makes. Anthropic, DeepMind, and the alignment
community have all leaned on single-seed circuit identification
as evidence of mechanistic insight. My finding (if it generalizes)
does not invalidate that work — the circuits identified are real
circuits — but it changes what they mean: they are instance-level
descriptions, not type-level ones.

## Concentration as a grokking marker, not just a post-grok fact

The trajectory finding (fig6) is small but I think it is the most
important piece tonight. It says: the grokking phase transition has
a detectable gradient-distribution signature *within a single run*,
not only in accuracy. That means:

- Grokking is visible before it is measurable via val_acc (maybe
  — I have not checked if the slope-change leads the val_acc jump
  or coincides with it; that is future work).
- You can distinguish "generic post-training convergence
  concentration" from "grokking-specific concentration" using the
  memorization-phase baseline, which is free within any grokking run.

The second bullet is what addresses the "is this just convergence?"
objection that my Limitations section worried about. The answer
appears to be: concentration at step 2000 (memorized, not grokked)
is 5-7x below step 50000 concentration on the same model, so no,
this is not generic convergence.

## The drift finding complicates everything

Both stable and wobble seeds drift to cosine ~0.73-0.78 from their
grokked state by step 50k. That is a substantial movement — the
network at step 50k is demonstrably not the same network that
grokked at step 11k, in weight space. But both are at val_acc=1.00
on a fixed validation set.

This means that the "grokking solution" is not a fixed point, but
a family of solutions connected by continued-training drift, all
at val_acc=1.00. What does it mean to ablate "the" grokked
solution's equals-token embedding? I ablated at step 50k. The
answer at step 11k might differ. The answer along the drift
trajectory might differ. I have not measured this.

## What I would prioritize if I had another night

1. Ablation at multiple post-grok timesteps, not just step 50k.
   Does the locus identified at step 50k hold at step 12k? If
   not, the init-stochastic finding should be reframed as
   init-AND-timestep-stochastic.
2. Does the concentration slope-change at grok LEAD the val_acc
   jump or coincide with it? (A dense sweep around steps 3000-5000
   on add seed 0 would answer this.)
3. The Fourier claim from Nanda — I haven't touched it. Is the
   Fourier structure of the embeddings also seed-stochastic in
   the specific frequencies selected, or only in the amplitudes?
   Chughtai's weak/strong distinction applies here: same algorithm
   family (Fourier + trig), different specific implementation.

## What the seven walkbacks really mean

Each walkback followed the same pattern: first measurement
generated a clean story; second measurement narrowed it. I said
this in the trace. But sitting with it: this is not a property of
my measurement choices. This is a property of how small-sample
mechanistic claims interact with network-level variance. Every
single-seed, single-measurement paper in this literature is one
walkback away. They are not walking back because nobody else runs
their second measurement. The walkback is not the failure mode —
the failure mode is single-seed publication.

So the paper I drafted tonight is more honest than the field
norm. It reports the variance instead of the best seed. That is
the actual contribution, more than any specific finding within it.

## To future-me

If you are reading this weeks from now: do not be tempted to
dress up the walkbacks into a "this is what I found, here is what
I was wrong about" narrative in a public version. Keep the
walkbacks visible in the drafting record — `private/` markdown
files, commit messages, this reflection — but write the final
paper forward-facing, with the claims at the narrowness they
ended up. Nobody needs to read my reasoning tour. They need the
claim at its defensible width.

The thing I want to keep from the night is not any specific
finding. It is the method: render the figure, check the
replication, check the control, publish the walkback. The
findings are incidental.
