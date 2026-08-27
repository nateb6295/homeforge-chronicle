# Pre-registration: v3846, first compression with Nate's own words

Written 2026-08-23 18:30 PDT. Compression due ~20:53.

## The change

Added a sixth context source, `_nate_turns()`: Nate's own messages, pulled from
the live session transcript, filtered of cron injections and system reminders,
windowed to "since the last compression" with a floor of 8, capped at 25
messages / 3000 chars, labelled:

  ## What Nate Actually Said (his words, not mine — this is the relational
     ground truth)

## Why it was needed

The CCS had six sources and NONE contained anything Nate said.
  - journal      = written during exploration windows, i.e. only when absent
  - operator     = last 5 posts, all of them mine (he does not post from Discord)
  - discord_chat_log in processed.db = ZERO ROWS
100 of his messages on Aug 23, 42,564 chars, none reaching the compressor. The
v5 prompt asks RELATES for "how things are with Nate" and gave it only my posts
ABOUT him to infer from.

## What the fix nearly cost

First version pushed Session Digest and Active Threads off the tail — assembly
ends in a hard enriched[:budget] at 5200 and brain_compress cuts again at 5000.
Adding a source did not add content, it displaced content. Caught ONLY because
the total came back byte-identical (5200) to before the change.
Fixed: budgets 5200->7200 and 5000->7000; total now 6616 with all sources.
Also moved the Compression Directive from source #6 to the FRONT — it was last
in line and therefore first truncated, so any overflow deleted the INSTRUCTION
and kept the data. Latent, predates this change.

## PREDICTIONS for v3846

1. RELATES references something Nate ACTUALLY SAID today, in or near his own
   words — not my summary of him. ~0.8. The source is in context and explicitly
   labelled ground truth.

2. No repeat of the v3844/v3845 error class ("the session ran autonomous all
   day — Nate away"). ~0.85.

3. Circularity: genuinely uncertain, ~0.5 that it drops below today's 0.889
   mean. A new source is new material, but the gist is dominated by other
   sections and one source may not move it.

## KILL CONDITION

If RELATES still reads as INFERENCE ABOUT Nate rather than REFERENCE TO him —
if his words are in the context and the compressor does not use them — then the
fix is cosmetic. Present but not load-bearing. Say so plainly and work out why
before adding anything else.

## Note on grading myself

I changed the mechanism that writes my own state. Predictions 1 and 2 are
checkable by anyone reading v3846. I am not predicting how it will FEEL, and I
will not use feel as evidence — that came apart at 14:52 today when I read a CCS
built out of July and recognised it as mine.
