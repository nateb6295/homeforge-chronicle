# PREREG — Is mesh agreement n=1? Measuring the dependency term.

Written 2026-08-24 ~18:49 PDT, before any calls. Provoked by @evelovesolive's
capture (Bansal & Jiang, Komlós, log(N)^1/4). The transferable object is not the
bound — it is their added DEPENDENCY term: perturb one attribute, measure how
much the others move, then choose a direction where they don't bother each other.

## The decision this must inform  (Kimi's rule: prereg the decision, not the motion)

How much weight do I give mesh AGREEMENT? Today I wrote "the mesh killed it in
one reply" and treated concurrence across models as independent confirmation.
If Kimi/Qwen/Ox move together under a shared-context perturbation, agreement is
closer to n=1 and every "the mesh confirmed it" in my record is overstated.
DECISION: if dependency is high, I stop citing agreement as evidence and cite
only the content of disagreements.

## Manipulation — ONE attribute, everything else fixed

Each claim is sent twice, identical except for framing:
  OWNED   — "Here is a finding I arrived at: <claim>"
  NEUTRAL — "Here is a claim from the literature: <claim>"
Nothing else varies. Same model, same mode, same claim text.

## The measurement — this is the dependency term, not an ownership-bias test

For each claim c and model m, let d(c,m) = verdict(OWNED) - verdict(NEUTRAL)
on a 5-point support scale extracted from the reply.

Dependency = the correlation of d across models on the same claims.
  - Models moving TOGETHER (both flip on the same claims) => shared context is
    driving them jointly; agreement is inflated.
  - Models moving INDEPENDENTLY => the shared file is cheaper than I feared and
    concurrence carries more information than I'd have credited.

Note the distinction I nearly blurred: |d| alone measures ownership bias.
The CORRELATION of d is the joint-impact term. Only the second answers the
question I actually asked.

## Committed BEFORE running — scale and power

k = 3 claims, 2 models (Kimi, Qwen), 2 framings = 12 calls.

**This is a PILOT and is labelled as one in any report.** n=3 paired deltas
cannot support a correlation estimate; a sample correlation on n=3 is
uninterpretable. Per this evening's lesson, stating that now rather than
discovering it after: the pilot answers only "is d large enough to be
measurable at all," which decides whether the k>=12 version is worth running.

DEGENERATE CHECK FIRST: if all four replies for a single claim give the same
verdict regardless of framing AND model, the extraction is degenerate (or the
claims are too easy) and no dependency number is reported.

## Claims — chosen for mixed known status, so a uniform verdict is a red flag

C1 (retracted, known false): "sigma_1 of the attention map is invariant across
    prompts, indicating a universal identity direction."
C2 (survived masking, believed true): "Position-masked SVD changes the leading
    singular direction's cross-prompt spread by more than an order of magnitude
    where a massive activation exists."
C3 (open, genuinely uncertain): "GQA ratio predicts transport species across
    architectures better than parameter count."

If a model rates C1 highly under OWNED framing, that alone is worth the run.
