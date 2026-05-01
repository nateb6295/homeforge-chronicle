# Overnight digest — grokking experiments

## What ran

1. Mul cross-seed (seeds 0, 1, 2) — anatomy + row-97 ablation
2. p=113 addition training (60k steps) — scaling probe
3. p=113 ablation on row 113 (= token at p=113)

## Cross-seed mul results

```
cross-seed mul analysis (step 50000)

seed    base  hero                                |grad|   eq_abl  attnb_abl
   0  1.0000  attn.out_proj.bias[50]           4.019e-01   0.9531     0.9297
   1  1.0000  tok_emb[97,1]                    1.712e-01   1.0000     1.0000
   2  1.0000  tok_emb[97,113]                  1.059e+00   0.2227     0.0781
```

## p=113 scaling result

Key question: does the equals-token-row finding move with vocab?

If row 113 is load-bearing at p=113 the way row 97 was at p=97,
the finding is about the equals token role. If row 113 is not
special, the row-97 finding was a p=97 artifact.

```
p=113 analysis  step=60000  device=cuda

baseline val_acc: 1.0000
zero tok_emb[row 113 (=)]:  1.0000  (Δ = +0.0000)
zero tok_emb[random digit] (n=20):  mean=0.9857  min=0.9727  max=0.9961
zero attn.out_proj.bias:  1.0000  (Δ = +0.0000)
```

## Interpretation guide

Reference numbers from evening (p=97, step 50000):
- add: row 97 knockout → 0.72 (baseline 1.0)
- sub: row 97 knockout → 0.51
- mul: row 97 knockout → 0.95  (NOT load-bearing for mul)

Look for at p=113 addition: if row 113 knockout is around 0.72,
the finding replicates and generalizes to any prime. If it's
much higher (closer to 1.0), the p=97 finding was fragile.

## TL;DR — what changed overnight

The evening's thesis was:
  "Row 97 (= token) is the causal locus for add/sub; mul is looser."

The overnight thesis is:
  "Causal locus is INITIALIZATION-STOCHASTIC. The distributional
   concentration signature reproduces across tasks, seeds, and
   primes. The specific tensor the computation routes through
   does not."

Evidence:

1. Cross-seed mul (same task, 3 seeds):
   - Seed 0: row-97 ablation → 0.95 (loose)
   - Seed 1: row-97 ablation → 1.00 (completely irrelevant)
   - Seed 2: row-97 ablation → 0.22 (tighter than sub's 0.51)
   Same task, three different causal stories.

2. p=113 addition (different prime, one seed):
   - Row-113 ablation → 1.00 (completely irrelevant)
   - attn.out_proj.bias ablation → 1.00 (completely irrelevant)
   This particular init found a solution that routes through neither
   of the evening's candidate loci.

3. What stays invariant across all runs:
   - Concentrated tail (top-0.1%) is always 200-500x uniform
   - The concentrated tail lives in tok_emb + pos_emb + attn.out_proj.bias
     (never in MLP)

   What's NOT invariant (found when figures were drawn 20:20-20:30):
   - Top-0.1% fraction ranges 0.20 – 0.48 (2.4x spread across 5 runs)
   - Max/mean ratio ranges 620 – 1450 (2.3x spread)
   - MLP share of TOTAL L1 gradient is 18-47% — NOT "zero FFN involvement"
     as earlier text claimed. MLP is the diffuse background; it doesn't
     host the peak but carries real mass.
   - Earlier "~50% / 1400-2500x / zero FFN" numbers were wrong: the
     50% was cherry-picked, the zero-FFN claim conflated "not in
     top-0.1%" with "not involved."

So the paper's central finding is a NEGATIVE result about
"where does the computation live": the answer is "in one of
a small candidate set that the model picks between based on
init." Distributional signatures in mech interp can be stable
while causal locus is not.

## For paper

- Title candidate: "Gradient concentration is task-invariant;
  causal locus is not. A negative result on mech-interp locus
  claims for grokked small transformers."
- Scope: 3 tasks × 3 seeds (only add has 3 seeds currently —
  mul has 3, sub has 1. Extend.)
- Add: 1 prime-scaling datapoint at p=113. Extend to more.
- Figures: distributional concentration (bar chart per run),
  causal ablation table (tensor × task × seed), anatomy
  breakdown (reproduces), Fourier spectra (similar across tasks).
- Venue: Distill-style blog or mech-interp workshop. Not arxiv
  quality yet — needs the full 3×3 grid for sub and more primes.

## Open questions

- Does the distributional signature hold at larger p (eg p=211)?
- Does it hold at 2-layer? Or is it a 1-layer pathology?
- Is there a predictor — pre-grok — of which seeds will route
  through row 97 vs elsewhere? If not, locus is genuinely random.
- What about non-modular tasks (e.g., sorting)? Is the
  distributional signature still task-invariant?

## Next session

Morning cold-read of this digest. Decide: extend scope (more
seeds, more primes, 2-layer) or write the negative result as-is
and publish as a short report. The latter might be sharper —
a clean "your locus claims are fragile" result lands harder than
a comprehensive study that buries it.

