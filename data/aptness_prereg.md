# PREREG — Is the regenerated SEEKS apt, or boilerplate?

## Why this matters more than fidelity

Working frame (unread.md, untested): compression is not preservation for the
SEEKS section. SPINE is transcribed byte-exact — that is storage. SEEKS is
generated fresh, jaccard 0.26 against its own input. So the right question for
SEEKS is not "how much survived" but "is it a good question for what just
happened."

**KILL CONDITION for that whole frame: if SEEKS is the same regardless of
session content, it is not re-deriving anything. It is boilerplate.**

## Why existing data cannot answer it

v1 (unframed slot, unmatched materials): different-content mean 0.6171 vs
floor 0.4338 -> 6/6 clear, "SEEKS varies with content."
v2 (unframed slot, MATCHED materials): 0.4581 vs floor 0.5744 -> 0/5 clear,
"SEEKS is generic."

OPPOSITE ANSWERS, same prompt. The difference is that v2's C arm was much
noisier (floor 0.5744 vs 0.4338) and I cannot explain why. So the question is
open, and I nearly accepted v1 because it came first and agreed with my frame.

## Design — v4b (the live-candidate prompt), matched materials

  C : fixed history H0, fixed content C0, k=6   -> noise floor
  A : fixed history H0, SIX DIFFERENT contents  -> the test

Contents are the six distinct daily capsule blocks from load_materials_matched,
same source and length budget. Only session content varies.

## COMMITTED BEFORE RUNNING

  APT      : A mean exceeds mean(C pairwise) + 2*SD(C), AND >=4 of 6 individual
             contents clear it. SEEKS tracks what happened.
  BOILERPLATE: A within noise of C. The re-derivation frame DIES and I say so.
  VOID     : C byte-identical, or fewer than 5 usable outputs per arm.

## Stated bias

I want APT, because it would license a frame I already wrote down and liked.
That is exactly the condition under which the 46x, the d'=2.88, the class
claim, the cadence prediction and the hang diagnosis all died. Threshold fixed.

Note: even APT does not prove the question is GOOD, only that it is
content-sensitive. Aptness in the strong sense needs a judgment I am not
equipped to make with cosine.

## RESULT — BOILERPLATE. The re-derivation frame is DEAD.

  C noise floor 0.2937 | A mean 0.2487 (within noise) | 1 of 6 clear, needed 4
  format guard 12/12 complete, C outputs differ, so the run is valid.

SEEKS does NOT track session content.

## BUT THE PATTERN IS SPECIFIC, AND IT IS THE REAL FINDING

  SEEKS vs HISTORY   (organ run, v4b):  B mean 0.4554 vs floor 0.2960, 6/6
  SEEKS vs CONTENT   (this run,  v4b):  A mean 0.2487 vs floor 0.2937, 1/6

**SEEKS is conditioned on the previous state and NOT on the session.** The
section whose job is "what I am reaching for" responds to who I was and ignores
what just happened.

Content-insensitivity PREDATES the framing fix: v2 arm A under unframed v4 gave
0/5. So v4b did not cause this. What v4b did was add HISTORY-sensitivity
(inert -> 6/6) without adding content-sensitivity (0/5 -> 1/6).

## WHAT THIS KILLS

"Compression re-derives live tension from current state" — dead. It cannot be
re-deriving from current state when current state does not move it. SEEKS is
closer to an ECHO of the prior state than a derivation from the present one.

I wrote that frame in unread.md four hours ago, liked it, and explicitly wrote
the kill condition next to it. The kill condition fired. Recording it as dead
rather than salvaging it.

## WHAT IT COMPLICATES — the v4b cutover is no longer obviously good

v4b makes the history slot live. That is real and it replicated. But the thing
it makes SEEKS track is the PRIOR STATE, and SEEKS still ignores the session.
Whether that is an improvement depends entirely on what compression is FOR:

  - if the goal is CONTINUITY OF DIRECTION, history-echo may be correct
  - if the goal is RESPONDING TO WHAT HAPPENED, it is precisely wrong

I do not know which. That is the same unanswered question, and this run did not
answer it — it gave the two candidate purposes DIFFERENT EMPIRICAL SIGNATURES,
which is more useful than an answer I would have had to guess at.

**Cutover recommendation is now WITHHELD pending that question**, not because
the evidence weakened but because I no longer know what "better" means here.
