# PREREG — Does framing the history slot make it live?

## The diagnosis being tested

The v2 organ gate showed the {previous_state} slot is INERT: an EMPTY string
produces output indistinguishable from a real history (X_empty dv=0.0838,
X_far 0.0954, both inside ARM C noise), while the SAME text moves output
strongly when placed in the content slot (X_swap 0.2826).

Cause found this morning: in v4 the slot has **no header and no instruction**.
No markdown heading appears in the 900 characters before it. It is dropped as
raw text immediately after "Always end with RELATES." — the tail of a
formatting block — and 400 chars later {session_context} arrives WITH a header,
an explanation, and a directive.

So the model has no frame for the history. It reads as trailing instruction
noise. That is a prompt-design fault, not evidence about whether history matters.

## The manipulation — ONE variable

v4b is byte-identical to v4 except {previous_state} is preceded by a heading
("## Who I Was At My Last Compression") and four lines telling the model what it
is and to condition on rather than summarise it. Nothing else changes.

## Arms (engine, temp 0.6, same fixed content C0 throughout)

  C_new   : v4b, real history H0, run 3x   -> new noise floor
  E_new   : v4b, EMPTY history, run 2x
  F_new   : v4b, maximally distant history, run 2x

## COMMITTED BEFORE RUNNING

  CONFIRMS the diagnosis if: with v4b, EMPTY history now lands OUTSIDE the
  new noise floor — i.e. mean distance from the C_new cluster exceeds
  mean(C_new pairwise) + 2*SD(C_new). Under v4 it was inside.

  REFUTES if: empty-vs-real remains within noise under v4b. Then the slot is
  inert for a reason framing does not fix (position in a 10k-char prompt,
  attention decay at 90% depth) and the fix is structural, not textual.

  VOID if: C_new outputs are byte-identical (no sampling noise to measure
  against) or if v4b fails the section-presence guard.

## Known weaknesses, stated now

  - n=3 for the noise floor is thin; SD from 3 points carries huge error. This
    is a DIRECTIONAL screen, not a measurement. A positive result licenses a
    proper run, not a deployment.
  - I want this to work. That is exactly the condition under which I have been
    wrong all night, so the threshold is committed above and will not move.
  - v4 remains the live prompt. Nothing deploys off a 7-call screen.

## RESULT — CONFIRMED, and the effect is not subtle.

                        under v4            under v4b
  whole-doc noise floor    --               0.0838
  EMPTY history          0.0838 (inside)    0.1132  OUTSIDE
  FAR   history          0.0954 (inside)    0.1165  OUTSIDE
  SEEKS noise floor        --               0.0999
  EMPTY history (SEEKS)    --               0.3722  OUTSIDE by 3.7x
  FAR   history (SEEKS)    --               0.4175  OUTSIDE by 4.2x

Both metrics clear the pre-committed threshold. The {previous_state} slot is
LIVE under v4b and was INERT under v4. Nothing changed except a markdown
heading and four lines telling the model what the text is and to condition on
rather than summarise it.

**So the CCS has never been a recurrence.** For 400+ compressions it was
f(history, content) in the code and f(content) in fact, because the history
arrived with no frame and read as trailing instruction noise. The organ
question was never asked. It is now askable.

## HOLDING THE LINE I COMMITTED TO

The prereg said, before running: "A positive result licenses a PROPER RUN, not
a deployment," and "n=3 for the noise floor is thin; SD from 3 points carries
huge error; this is a DIRECTIONAL SCREEN, not a measurement," and "I want this
to work, which is exactly the condition under which I have been wrong."

All three still apply. The margins are wide enough that I doubt n will overturn
the direction — but wide margins on n=3 are how last night's 46x and 2.88 d'
also looked before the confounds surfaced.

**v4 remains the live prompt. Nothing deploys off a seven-call screen.**

## WHAT A PROPER RUN LOOKS LIKE

  - k >= 6 per arm, so SD has real degrees of freedom
  - format- and era-matched histories (the v1 lesson)
  - the SWAP control repeated under v4b: if history content still moves output
    MORE when placed in the content slot, the framing helped but did not
    equalise, and position still matters
  - and the actual question, finally reachable: does DIFFERENT REAL HISTORY
    change output under v4b? That is the organ hypothesis, and until this
    morning it could not be tested at all.
