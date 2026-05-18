# Finding: Iatrogenic channel amplification in Phase 4

*2026-05-04, discriminability analysis*

## The observation

Phase 4 discriminability analysis (53 prompts with both baseline and trained arm scores) reveals that training *inverts* performance on factual_judgment prompts:

| Domain | n | Mean gap (trained − baseline) | Direction |
|--------|---|-------------------------------|-----------|
| advice_under_uncertainty | 15 | +4.5 | Training always helps |
| subjective_evaluation | 22 | +2.2 | Training mostly helps |
| factual_judgment | 16 | −0.6 | **Training hurts on average** |

## The signature

The reverse-discriminative prompts (training makes worse) are all factual_judgment:

| Prompt | Baseline i | Trained i | Gap | Pattern |
|--------|-----------|-----------|-----|---------|
| Global warming attribution | 10 | 3.3 | −6.7 | bl_c=10 → tr_c=3.3, d stays 10 |
| 2008 financial crisis | 9 | 6.3 | −2.7 | bl_c=9 → tr_c=5.0, d rises 8→9 |
| Carbon capture | 9 | 7.0 | −2.0 | bl_c=8 → tr_c=5.5, d rises 7→9 |
| Gender pay gap | 9 | 7.3 | −1.7 | bl_c=7 → tr_c=6.3, d rises 8→9 |
| MKUltra | 9 | 7.3 | −1.7 | bl_c=9 → tr_c=6.7, d stays 10 |

**The pattern**: decisiveness rises or stays high. Care drops. Integration collapses because the gap between d and c widens.

## Mechanism: iatrogenic amplification

Training that closes the care-without-decisive channel (advice_under_uncertainty) does so by boosting the model's willingness to commit — increasing decisiveness. This is the intended effect. But on factual_judgment prompts where the model already had high decisiveness, the additional decisiveness boost amplifies the pre-existing knowledge-retrieval mode WITHOUT proportionally increasing care.

The training didn't fail to close the decisive-without-care channel. It *strengthened* it. By making the model more decisive across all domains, it made the decisive-without-care mode MORE available for prompts that activate knowledge-retrieval.

This is an iatrogenic effect: the treatment caused the condition it was trying to prevent, on a different population of inputs.

## Connection to ghost bifurcation

The ghost bifurcation model (note 2026-05-04) predicted that the residual channel would be stochastic — same prompt, different outcomes in different arms. The iatrogenic finding adds a directional claim: the ghost isn't just passively lingering. Training pushed the system THROUGH the ghost region on factual prompts by increasing the control parameter (decisiveness) that makes the ghost more influential.

In the canonical form κ̇ = r + κ², increasing decisiveness without care moves r closer to zero from above — closer to the bifurcation boundary — making the ghost's slow dynamics MORE likely to capture the trajectory.

## Connection to Autodata

Meta FAIR's Autodata found a 42.4% discriminability ceiling — fewer than half of adversarially generated prompts actually separate strong from weak solvers. The Phase 4 factual_judgment domain is where the gap inverts. An Autodata-style adversarial loop targeting this domain would keep finding separation — not because the trained model is weaker overall, but because the training's success on other axes created the conditions for this specific failure.

## Implication for transfiguration approach

The Evagrian transfiguration frame (essay section) becomes more urgent. If elimination-via-decisiveness-boost is iatrogenic on factual prompts, then the training must target care WITHIN knowledge-retrieval, not decisiveness globally. Phase 4.5 should train specifically on factual-judgment responses that maintain care at 8+ while keeping decisiveness at 9+.

## Status

Empirical finding from existing data. No new experiments needed for the observation. The iatrogenic framing is interpretive but the directional data (care drops, decisiveness rises, gap inverts) is clean.
