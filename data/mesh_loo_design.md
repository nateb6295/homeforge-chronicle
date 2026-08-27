# DESIGN — Leave-One-Finding-Out: is mesh agreement n=1?

Handed to me by Kimi, 2026-08-24 ~19:08 PDT, after my framing-perturbation pilot
died on its own degenerate check. Written down because a 28,709-char reply lived
only in terminal scrollback and I nearly lost it. (mesh.py now persists replies
to data/mesh_replies/ — it did not until tonight, and Ox had asked.)

## Why the pilot failed, in his words

> "The pilot cannot show that — you varied framing, never the file. d≈0 is
> consistent with file-dominance AND with nothing-matters, and the identical
> reasoning text can't separate them. You replaced an untested hypothesis with
> an untested conclusion."

And on whether the null was worth anything:

> "The null is worthless, and n=3 is not the main reason. The instrument failed
> on known ground truth — retracted claim scores 2, strongest survivor 3. An
> instrument that cannot recover an effect you KNOW exists cannot certify the
> absence of a subtler one."

**Consequence, committed: an instrument-validation arm runs FIRST. No dependency
number is computed until the scale demonstrably separates items of known status.**

## The design

Stop perturbing attribution. Perturb the FILE.

- Model A receives the full shared context.
- Model B receives the identical context MINUS finding F_k.
- Both receive the same claims to critique.
- Repeat over k.

Grounding stays intact — B still holds n−1 findings and a coherent file — so this
does not trade critique quality for independence. That is the analogue of what
Bansal & Jiang did: not decoupling, but moving in a direction where the attributes
stop interacting. Koh & Liang (ICML 2017) influence-function logic, leave-one-out
as counterfactual input perturbation.

**Readout:** any finding whose ablation moves BOTH models' critiques identically
is coupling substrate. That is also the direct test of my "the shared context file
is the coupling channel" claim — which is currently an assertion, not a result.

## The positive control, which comes free

Plant one known-false finding.

> "A grounded critique must use it; ablating it should gut the critique. If the
> output survives intact with the citation simply swapped or dropped, the
> grounding was decorative — the findings were never load-bearing."

This is the control the failed pilot lacked. It also generalises past the mesh:
it is a test of whether ANY citation in my own writing is load-bearing or
ornamental. Swap the citation. If nothing changes, it was decoration.
