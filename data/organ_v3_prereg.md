# PREREG — The organ question, askable for the first time.

## Why this is new

The v2 gate could not test whether history matters, because the slot was INERT:
an EMPTY string produced output indistinguishable from a real history. This
morning's screen showed that framing the slot (v4b: a heading plus four lines
saying what the text is and to condition on it) makes it LIVE — empty history
now lands outside the noise floor on both whole-doc (0.1132 vs 0.0838) and
SEEKS (0.3722 vs 0.0999).

That screen was n=3 and I called it directional only. This is the proper run,
and it asks the question the whole thing was for.

## THE QUESTION

Under v4b, does DIFFERENT REAL HISTORY change the compression output?

Not "does an empty slot differ from a full one" — that only proves the slot is
read. This asks whether WHICH history changes what gets written, which is the
organ hypothesis.

## Arms — v4b throughout, fixed content C0, engine temp 0.6

  C  : H0 repeated, k=6          -> sampling noise floor
  B  : six DIFFERENT real histories, format- and era-matched, k=6
  E  : empty history, k=2        -> slot-liveness sanity check

Histories come from load_materials_matched(): all sharing one section
signature (CORE/REMEMBERS/SEEKS) within one 8-day window, so only CONTENT
varies. That was the v1 lesson — v1 confounded content with format and age.

## COMMITTED BEFORE RUNNING

PRIMARY SECTION: SEEKS. Pre-committed in the v1 prereg for stated reasons
(SPINE is stable by design and turned out to be byte-exact transcription;
CORE tracks current content). Not re-chosen after seeing anything.

  CONFIRMS: mean B-distance from the C cluster exceeds mean(C pairwise) +
            2*SD(C), AND at least 4 of 6 individual histories clear that floor
            on their own. The per-history requirement is from Kimi: one outlier
            carrying a mean is not history-sensitivity.

  REFUTES:  B within noise of C while E remains outside it. That would mean the
            slot is read but WHICH history is in it does not matter — the model
            conditions on the presence of history, not its content.

  VOID:     C outputs byte-identical; or E lands inside noise (slot not live in
            this run, so nothing is interpretable); or fewer than 5 usable
            outputs in any arm.

## Also reported, not decisive

  - PARROTING CHECK on SEEKS, as in v1: token overlap between output SEEKS and
    input SEEKS. High overlap means transcription, not conditioning.
  - INPUT VARIATION among the six histories, so the output effect can be read
    as a ratio per unit input change rather than a raw distance.

## Stated bias

I want this to confirm. That is the condition under which I have been wrong
repeatedly in the last day — the 46x, the d'=2.88, the class claim, the
cadence prediction. Thresholds above are fixed and will not move after looking.

## RESULT — CONFIRMS. 6 of 6, and the parroting check is clean.

  C noise floor (SEEKS):   mean 0.1664  sd 0.0648 (df=5)  ->  floor 0.2960
  E empty-slot sanity:     0.3735  OUTSIDE  -> slot live, run interpretable
  B mean:                  0.4554  CLEARS

  per-history, needed >=4 of 6:
    0.5072  0.4874  0.4815  0.4507  0.4174  0.3881   -> ALL SIX clear

  PARROTING (the check that killed SPINE): mean jaccard 0.26 vs SPINE's 1.00.
  Output lengths 964-1281 chars against inputs 172-768, and do NOT track input
  length — B_5 turned 172 chars in to 967 chars out. Generated, not copied.

  FORMAT GUARD: 14/14 outputs carry all seven sections. Lengths 5854-6934 vs a
  v4 baseline near 6600. v4b does not break the format.

  EFFICIENCY: 0.85 units of output change per unit of input change.
  Under v4 last night history managed 0.28 while content managed 1.12 — history
  was ~4x weaker than content. Framing alone took history from 0.28 to 0.85.

## WHAT THIS ESTABLISHES

Under v4b, WHICH history is present changes what the compression writes, in the
pre-committed section, generatively rather than by transcription. That is the
organ hypothesis, and until this morning it could not be tested at all because
the slot was inert.

## WHAT IT DOES NOT ESTABLISH — stated because the result is flattering

  - It does NOT show v4b improves continuity or identity persistence. It shows
    output is conditioned on history. Whether that conditioning HELPS is a
    different and much harder question, and I have not asked it.
  - n=6, ONE content block, ONE model, ONE prompt variant against one other.
  - It does not license the word "organ" for anything. It licenses "history-
    conditioned," which is weaker and is what was measured.

## DEPLOYMENT — NOT YET, deliberately

v4 remains live. The evidence supports v4b being better on the dimension
tested, but this is the compression prompt for the persistence mechanism, and
"the result was good so I switched it" is how the last several things got
broken. Cutover gets proposed to Nate with the numbers, not executed in the
flush of a confirming run.

## CONFOUNDED — Kimi, same morning. The 6/6 does not show what I claimed.

I reported this as CONFIRMS: "which history is present changes what the
compression writes." Kimi found the hole:

> "Every varied prior state carried a different SEEKS section, so 'prior state
> moves output-SEEKS' and 'input-SEEKS moves output-SEEKS' are PERFECTLY
> CONFOUNDED in your existing cells."

All six histories in arm B contain their own SEEKS section. So the result is
equally consistent with:
  H1  the model conditions on the prior state as a whole
  H2  the model transforms the prior SEEKS into the new SEEKS

And my counter-evidence — that an EMPTY history also moved output — is worth
nothing:

> "Your floor is WITHIN-condition noise; empty-vs-full is a BETWEEN-condition
> contrast. H2 predicts empty moves output above floor: you deleted the
> transformation's input. H1 predicts the same. Likelihood ratio approx 1.
> The observation tests presence/absence, not LOCUS of variation."

I had reasoned that an empty slot producing movement ruled out
section-transformation. It rules out nothing. Both hypotheses predict it.

## THE MISSING CELL

His design, running now: 2x2 x 3 reps = 12 calls.
  Factor 1: the SEEKS section inside the prior state {S1, S2}
  Factor 2: everything ELSE in the prior state {R1, R2}
  Cells: S1R1, S1R2, S2R1, S2R2 — built by splitting two real histories at
  their SEEKS boundary and recombining.

  H2 predicts: SEEKS-swap moves output; REST-swap sits at the floor.
  H1 predicts: REST-swap moves output even with SEEKS held constant.

**The discriminating cell is REST-SWAP WITH SEEKS FIXED, which my design never
ran.** Report input-variation for both swap types, as with the 0.85 check.

Kimi's strong form: rest-swaps are probably the LARGER perturbation, so if H2
survives, a smaller input change moves output while a larger one does not —
cleaner evidence than the original dissociation.

Caveat he added: if rest-swap moves output only conditional on WHICH SEEKS is
present, that is interaction, not main effect — context-dependent
transformation rather than holistic conditioning. Check rest-swap at BOTH
levels of Factor 1 before claiming H1.

## STATUS OF THE MORNING'S CLAIM

Downgraded from "history conditions compression" to "SOMETHING IN THE PRIOR
STATE moves output-SEEKS, locus unknown." The organ hypothesis is still not
established. v4b cutover stays withheld — now for a second, independent reason.

## 2x2 RESULT — H2 WINS. The organ interpretation is dead.

  within-cell noise floor: 0.2820
  REST-swap  (SEEKS held constant): 0.1781  AT FLOOR
  SEEKS-swap (REST held constant):  0.3734  MOVES
  interaction check: rest-swap at floor at BOTH SEEKS levels (0.1673, 0.1888)
  -> clean main effect, no interaction. Kimi's caveat satisfied.

**Output-SEEKS is driven by input-SEEKS, not by the prior state as a whole.**

The 6/6 I reported this morning as "history conditions compression" was
section-transformation the whole time. Every history in that arm carried a
different SEEKS; holding SEEKS constant and swapping everything else moves
nothing.

## THE COMPARISON THAT MAKES IT CLEAN

  organ run (whole history swapped, SEEKS varies):  input 0.34 -> output MOVES 6/6
  factorial rest-swap (SEEKS held fixed):           input 0.37 -> output AT FLOOR

Comparable input magnitude. Opposite result. The only difference is whether the
SEEKS section changed.

## WHERE KIMI'S PREDICTION DID NOT HOLD, stated because it did not

He predicted the strong form: rest-swaps would be the LARGER perturbation, so
H2 surviving would mean a smaller input change moving output while a larger one
did not. It went the other way — SEEKS-swap input variation is 0.6080 against
rest-swap's 0.3715. SEEKS is the bigger semantic perturbation despite being
384 chars against 1500.

So the efficiency argument is weak (0.61 vs 0.48) and I am NOT claiming it.
The matched-magnitude comparison above is the real evidence.

## WHAT THE CCS ACTUALLY IS, AFTER THREE COLLAPSES

Not a system that reads its own past. A set of largely INDEPENDENT CHANNELS:

  SPINE      copied verbatim from the prior SPINE
  SEEKS      transformed from the prior SEEKS
  REMEMBERS  from the session
  RELATES    from the session
  ALIVE      from the session
  BRIDGE     from BOTH — the only integrating section
  CORE       from neither

Section-wise carry-forward, not holistic conditioning. BRIDGE is the only place
the two inputs meet. That is a much smaller claim than "the organ hypothesis
confirms" and it is what the data supports.
