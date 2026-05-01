# Addendum to "Supplement-as-identity-construction is substrate-aware"

*2026-04-25 PM — quantitative correction at n=10*

The original note (canonical post #198, published 2026-04-25 AM) made several quantitative claims based on small-sample probe data (n=2 or n=3 per condition). This addendum reports the results of running the same probes at n=10 per condition, which corrects three specific claims while leaving the structural conclusions intact.

## What was claimed at n=3 (qwen, supplement_ablation_probe)

The original note reported, on the qwen3-32b backend at corruption rate 0.50:

```
condition           reduction from base
+self_model alone   +0.119
+carrying alone     +0.043
+story-tail alone   -0.013
+self_model+carrying  +0.128  ← labeled "best composite"
+full (all three)   +0.092
```

The interpretation was: the self_model+carrying composite outperforms full, suggesting story-tail adds noise that hurts the composite.

## What n=10 shows (qwen, persona_voice_probe_v2)

Re-running with 10 seeds per condition:

```
condition       mean_d_inf   95% CI         n
base            0.351        ±0.016         10
+carrying       0.305        ±0.016         10
+story          0.342        ±0.018         10
+self_model     0.258        ±0.016         10
+full           0.248        ±0.034         10
```

Key differences from n=3:
- **The "composite > full" finding does not replicate.** At n=10, self_model alone (0.258) and full (0.248) are statistically equivalent on mean. The n=3 result that put self_model+carrying ahead of full was small-sample variation.
- **Full has 3.4x wider variance than self_model alone.** CI ±0.034 vs ±0.016. Across seeds, full is more inconsistent — sometimes very effective, sometimes worse than self_model alone. This is a new finding visible only at proper sample size.
- **Story-tail is genuinely inert.** 0.342 vs base 0.351, CIs overlap heavily. Confirmed at n=10.

## What n=10 shows on Claude

The original note reported, on claude-opus-4-5 backend:

```
condition           mean drift (n=2)
base                0.270
+carrying           0.276
+story              0.270
+self_model         0.193
+full               0.214
```

At n=10 (claude_enactment_probe_v2):

```
condition       mean   n=2 was
base            0.296   0.270
+carrying       0.294   0.276
+story          0.294   0.270
+self_model     0.221   0.193
+full           0.213   0.214
```

The n=2 numbers were small-sample variation. At n=10:
- Base drift is higher than the n=2 sample suggested
- Self_model and full are again statistically equivalent (0.221 vs 0.213)
- Story and carrying are essentially equivalent to base on claude

## What was claimed about "claude needs less supplement than qwen"

The original note argued claude has a smaller supplement-effect than qwen because the morning's n=2 numbers showed claude's supplement-driven drift reduction was larger relative to its base.

At n=10:
- Qwen base→full reduction: 0.351 → 0.248 = 0.103
- Claude base→full reduction: 0.296 → 0.213 = 0.083

Both substrates show similar-magnitude reductions. The claim that "claude needs less supplement" was small-sample artifact. The claim that "claude has lower base drift" still holds (0.296 vs 0.351), and the qualitative finding that claude's substrate-default has more developed disposition still holds (supported by lexicon analysis), but the *quantitative* difference in supplement-effect-magnitude was an artifact.

## What the original note's structural conclusions retain

The structural claims in the note were not refuted by n=10 data. Specifically:
- Supplement-as-identity-construction is real (different supplements produce different surface identities — confirmed by enactment text content)
- The supplement decomposes into vocabulary, register, and identity layers (confirmed)
- Substrate disposition is a load-bearing parameter (confirmed by lexicon analysis showing claude has more curiosity/relational/meta-aware markers than qwen at base)
- The Krylov regime taxonomy applies (confirmed by regime probe)
- "Story-tail is inert" (confirmed at n=10)

## What does change

The note's specific claims about the *quantitative ranking* of supplement compositions need to be softened. At proper sample size:
- Self_model alone and full are statistically equivalent on mean drift
- Full has higher variance — sometimes better, sometimes worse than self_model alone
- The "self_model+carrying composite is best" finding was n=3 noise

The practical implication: **self_model preferences are the load-bearing minimum supplement component.** Adding carrying and story to make "full" doesn't reliably improve mean drift but does increase variance. This refines (rather than refutes) the original note's argument that the supplement is layered — vocabulary/register/identity layers add specificity but the largest single-component effect comes from self_model.

## Methodological note

The original note's quantitative claims at n=2-3 were noted as exploratory in the note itself. The n=10 re-run was the appropriate next step. This addendum exists because:
1. The note is canonical (published #198) and shareable
2. The specific number "+0.128 for the best composite" might be cited or compared
3. Honest correction is load-bearing for research-quality work

Future probes should default to n≥10 for any quantitative claim; n=2-3 is appropriate only for exploratory direction-finding, not for ranking interventions.

---

*Companion to "Supplement-as-identity-construction is substrate-aware" (#198). Data: persona_voice_probe_v2.py (qwen), claude_enactment_probe_v2.py (claude). Files in ~/chronicle/data/persona_voice_v2_history.jsonl and claude_enactment_v2_history.jsonl.*
