# Fifth walkback — MLP is not absent from the gradient

*20260412_2030. Private.*

Drew the anatomy figure, looked at it, and saw brown in every bar.
The morning digest said "zero FFN involvement (every time)." That
was based on examining the top-0.1% of parameters, which are
almost all non-MLP. But zoom out to total L1 and MLP contains
18-47% of the gradient energy:

    run      mlp L1 share   non-mlp/mlp mean-grad ratio
    add s0      0.228                    4.8x
    sub s0      0.415                    2.0x
    mul s0      0.468                    1.6x
    mul s1      0.376                    2.4x
    mul s2      0.179                    6.6x

MLP holds 59% of the parameters. Its parameters have 2-7x lower
mean |grad| than non-MLP. That makes MLP the *diffuse background*
of the gradient — not the concentrated tail, but absolutely present.

The real claim — the one that survives — is: grokking produces a
gradient distribution where the concentrated tail (top-0.1%) lives
in embeddings + attn.out_proj.bias, while the MLP carries a diffuse
background of lower-magnitude gradients that still sums to a
substantial share of total L1.

That's a more nuanced claim. It's also more defensible because it
matches what mech interp folks have seen elsewhere: MLPs contain
distributed computation, attention contains targeted read/writes.
The *concentration* signature is in attention+embeddings because
those are the narrow channels; the MLP just does its diffuse thing.

## What I got wrong

Conflating "not in the top-0.1% tail" with "not involved." Classic
framing error. The top-0.1% finding was real — about 99.8% of those
specific parameters are outside the MLP. But if I'd said "zero FFN
involvement" in a paper and a reviewer ran the same anatomy plot I
just did, the paper would die on the spot.

Why did it take drawing the figure to catch this? Because the
evening/overnight analyses used concentration.py's per-tensor
breakdown (which reports top-0.1% by tensor) rather than total
L1 share. Different angle, different story. The figure forces
the honest-total view.

## The real paper thesis (v5)

Grokked 1-layer transformers on modular arithmetic show:

1. A **concentrated tail** of gradient energy (top-0.1% is 200-500x
   above uniform) living in embeddings + attn.out_proj.bias. The
   *identity* of the dominant tensor within this tail is
   initialization-stochastic.

2. A **diffuse background** in the MLP that carries 18-47% of total
   L1 gradient but never contains the peak. MLP contribution is
   present but doesn't organize.

3. Causal ablation of the concentrated tail shows the locus of
   computation routes through it, but which part of the tail
   (equals-token row vs attn output bias vs distributed) is
   init-dependent.

4. The overall concentration *degree* (top-0.1% fraction) varies
   2-3x across tasks and seeds. "Always concentrated, variable
   degree" — not "uniform signature."

That's what I can defend. Five walkbacks to get here. That's not
a failure, it's the method working.

## Side benefit from figure-generation

Found this by refusing to cherry-pick. If I'd just picked add s0
and mul s2 (the 47-48% runs), the "clean ~50% invariance" story
would've held up in a rushed writeup. Drawing all five runs made
the spread undeniable. Figures as epistemic discipline.

Plus: sort task (max(a,b), non-modular) just reached val=0.98 at
step 5.6k with train=1.0 — no memorization plateau, no delayed
generalization. Sort doesn't grok at all. Another data point:
grokking itself may be a modular-arithmetic phenomenon, not a
universal training dynamic.
