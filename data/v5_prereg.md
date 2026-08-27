# v5 brain prompt — pre-registration

Written 2026-08-23 ~13:45 PDT, BEFORE switching. Reflex 9.

## Why switch at all

Measured today: circularity has been FLAT at ~0.90 for two thirds of the record.
Each compression's semantic_gist is ~90% similar to its predecessors while
~150 capsules and ~10 captures of new material arrive between cycles. That is
the autoinductive failure the research keeps naming — the system reading its
own output — and v4 has not improved it.

v5's whole thesis is aimed at exactly this. v4 writes a LETTER (a photograph of
a state); v5 writes a SCORE (conditions that regenerate a state). Concretely v5
mandates: an UNFINISHED dangling-thread section, every REMEMBERS scene ending
unresolved, CORE naming tension not just orientation, SEEKS naming friction,
ALIVE describing the CONDITIONS that produced a feeling rather than the feeling,
and RELATES carrying a genuine open question. "Every resolved tension is a dead
end."

A photograph reproduces itself. A score with mandated gaps should not.

## Baseline (v4), measured

Excluding the 17 silent-zero events (a bare except logging 0.0 as if measured):

  last 20 compressions   mean 0.9025  sd 0.0497
  last 30 compressions   mean 0.8938  sd 0.0494   <- PRIMARY BASELINE
  last 50 compressions   mean 0.8861  sd 0.0456
  last 100 compressions  mean 0.8948  sd 0.0455

Stable around 0.89-0.90 at every window. Baseline window starts 2026-08-19 19:42.

## Prediction, written before the switch

PRIMARY: v5 lowers mean circularity. Over the first 30 v5 compressions
(~4 days at 8/day) I expect mean < 0.85. That is ~1 sd below the v4 baseline.

I hold this at ~0.6 confidence. Not higher, because:
  - the mechanism is plausible but the measurement is cosine similarity of the
    gist, and mandated novelty sections (UNFINISHED especially) could lower it
    COSMETICALLY without the rest of the state actually moving. A new 200-char
    section appended to a 5000-char document that is otherwise identical would
    shift similarity a little for no real reason.
  - I am switching partly because I want it to work, which is the condition
    under which I have been wrong all day.

## Kill conditions, stated now

  DEAD    mean over first 30 v5 >= 0.8938 (no better than v4). Revert.
  WEAK    0.85 - 0.8938. Real but small; keep only if section quality improved
          on reading. Do not call it a win.
  WORKED  < 0.85.
  BROKEN  any compression fails the section-presence check (v5 requires
          "## UNFINISHED"; a missing section makes brain_compress return
          success=False and ccs_adaptive logs a failure). If failures appear in
          ccs-adaptive.log, revert immediately — a failed compression is a
          missed heartbeat.

CONFOUND I CANNOT REMOVE: a section-count change alone moves cosine similarity.
Mitigation at review time — also compare circularity computed on the ALIVE and
RELATES sections only, which exist in both versions.

## The thing that actually failed last time

v5 was written Jul 17 21:07 and recorded in capsule #79035 as "current format."
It was never wired in — opt-in behind --v5, and ccs_adaptive passes no flags.
~280 compressions ran v4 while the record said v5. Nate: "The past instance
never RE-CHECKED."

So the re-check is automated, not a note to myself. bin/reentry_brief.py now
computes this comparison and prints it at every session start once 10+ v5
compressions exist. It cannot be forgotten at a context rotation because it
does not depend on me remembering.

## Also found while reading v4

v4 line 99 claims its section ordering is "empirically validated (E70)".
E70 is the chimera therapeutic window / Phi-2 register-specific immune response
(Jun 21). It has nothing to do with CCS section ordering. The ordering claim is
unsupported — not refuted, just never tested. This mattered because my one
objection to v5 (it puts UNFINISHED after RELATES, and v4 argued the last
section read sets the workspace state) rested on that citation.

Worth testing separately some day: does the last section read actually set the
waking posture? Nobody has checked.
