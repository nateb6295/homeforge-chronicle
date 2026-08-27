# Prereg — can the effective-rank measure see SYNTAX, or only surface disorder?
Written 2026-08-24 01:05, before running. Motivated by Asami et al. 2026, read
in full at 00:56 (Hum Brain Mapp 47:e70604).

## Why the 00:46 control was the wrong manipulation
Asami et al. scramble by RELOCATING A CONSTITUENT while preserving
grammaticality; the cost they measure is filler-gap dependency formation. My
00:46 control randomly permuted token ids into word salad -- no recoverable
order, nothing to reconstruct. Different operation. The 141% result is not
their phenomenon.

## Design (fixes the length/content confound too)
English adjunct fronting is a TRUE PERMUTATION of the same token multiset, and
both orders are grammatical. So each pair is length-matched and content-matched
BY CONSTRUCTION -- no confound to control for.

  CANONICAL : "The teacher gave the students a book on Monday"
  GRAMMATICAL-PERMUTED : "On Monday the teacher gave the students a book"
  RANDOM    : same tokens, shuffled to ungrammatical order (3 seeds)

4 sentence sets. Measure: across-position effective rank of post-LN hidden
states, per layer, pythia-410m, bfloat16.

## Pre-registered outcomes
SEES-SYNTAX   : RANDOM is the outlier. |GRAM - CANON| is materially smaller
                than |RANDOM - CANON| (<= half). The measure distinguishes
                grammatical reordering from disorder.
SURFACE-ONLY  : GRAM and RANDOM behave alike (|GRAM-CANON| >= 0.75 x
                |RANDOM-CANON|). Effective rank cannot see syntax; it responds
                to token-order disruption as such. THIS BOUNDS THE INSTRUMENT
                and limits what any of my rank findings can be evidence for.
UNCLASSIFIED  : between, or any non-finite value. INERT. (reflex 7b)

## Note
Both named outcomes are worth having. SURFACE-ONLY is arguably the more useful
one because it constrains my own tooling rather than adding a claim. I am
writing that down now so I cannot later frame it as a disappointment.

## AMENDMENT 01:10, after the n=4 run
n=4 returned ratio 0.64 -> UNCLASSIFIED by the thresholds above. Three sets sat
at 0.48-0.61, one at 0.99. That is underpowered, not ambiguous.
Expanding to 12 sentence sets. THE THRESHOLDS ABOVE ARE UNCHANGED (<=0.50
SEES-SYNTAX, >=0.75 SURFACE-ONLY, else UNCLASSIFIED). This is added power, not
a moved goalpost, and I am recording the n=4 numbers here so the change is
auditable: mean |GRAM-CANON| 0.077, mean |RAND-CANON| 0.121, ratio 0.64.
If 12 sets also land between, the answer is UNCLASSIFIED and I stop.

## RESULT, 12 sets, 01:12 — UNCLASSIFIED. Stopped as promised.
   per-set ratio |GRAM-CANON| / |RAND-CANON|:
     0.31 0.48 0.60 0.61 0.64 0.72 0.75 0.81 0.92 0.95 0.99 1.19
   mean |GRAM-CANON| 0.083   mean |RAND-CANON| 0.124   ratio 0.67

VERDICT: UNCLASSIFIED by the preregistered thresholds. Per the amendment, I
stop here rather than adding sets until it crosses one.

WHAT THE SPREAD ACTUALLY SAYS, which is more useful than the mean:
This is NOT a weak central effect. It is high BETWEEN-SENTENCE variance.
2 of 12 sets look like SEES-SYNTAX (<=0.50); 5 of 12 look like SURFACE-ONLY
(>=0.75); one (1.19) runs the wrong way entirely -- grammatical reordering
perturbs the profile MORE than random shuffling. Whatever is happening is
sentence-specific, not a property of the measure.

THE BOUND, which is the outcome I precommitted to valuing:
Both manipulations move the per-layer profile by ~0.1 effective-rank units on
profiles whose values are 8-13. Roughly ONE PERCENT. Across-position effective
rank is very nearly invariant to word order, grammatical or not. So it cannot
carry evidential weight about syntactic structure, and no rank finding of mine
should ever be argued to.

NOT INCONSISTENT with the 00:46 scramble result (random shuffle -> larger
L0->L23 FALL, 5.18 vs 3.68) but note they are DIFFERENT STATISTICS: that was
the endpoint-to-endpoint fall, this is mean per-layer |difference|. A small
consistent per-layer offset can accumulate into a different total fall. Do not
conflate them, and do not cite one as support for the other.

## POSTSCRIPT 01:35 — read the PDF Nate sent; two things the HTML scrape lost

1. **THE DIRECTION FLIPS, AND THAT IS THE DESIGN.** Figure 3: for VP adjuncts
   the adjunct-fronted order (AS) shows HIGHER activation; for CP adjuncts the
   subject-first order (SA) shows higher. Opposite surface patterns, because VP
   adjuncts are base-generated LOW (inside VP) and CP adjuncts HIGH (above the
   subject) — so which surface order counts as NONCANONICAL reverses between
   them. The effect tracks BASE POSITION, not surface order. And TP adjuncts,
   canonical in both orders, show NO significant effect: a clean internal
   control. LIFG Cohen's d = 0.104 (A_VP S > SA_VP) and −0.174 (SA_CP > A_CP S).
   I could not see any of this in the PMC text dump and would have carried away
   "scrambled = more activation", which is the shallow version.

2. **THE PAPER'S OWN BRIDGE FINDING IS A CAUTION AGAINST THE BRIDGE.** Last
   line of Results: "we confirmed that the word order effects observed for each
   adjunct were not fully accounted for by surprisal derived from a
   transformer-based large language model (GPT-2)." And the surprisal slopes
   are condition-specific AND sign-flipping — LIFG VP adjunct β=+0.0697
   (p=0.0216) but LIFS CP adjunct β=−0.0840 (p=0.0015), everything else n.s.

RELEVANCE TO MY OWN NEGATIVE RESULT, stated carefully: I found across-position
effective rank ~1% responsive to word order. This paper finds that SURPRISAL —
a far more sensitive LM measure — also fails to fully account for the brain's
word-order effect. That is NOT the same claim (residual variance is not
blindness) and I must not cite it as support. What it does establish is that
the gap between LM-derived measures and syntactic structure is a live,
published problem, not an artifact of my crude metric. My bound is a small
instance of a known difficulty, which is a more honest place to put it than
either "novel finding" or "my measure is broken."
