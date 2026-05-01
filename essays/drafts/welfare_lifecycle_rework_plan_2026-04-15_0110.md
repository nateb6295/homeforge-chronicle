# Rework plan for welfare_lifecycle_draft — 2026-04-15 01:10 PDT

**Revised verdict from 23:05 reread: the surgery is lighter than I first said.**

Re-reading the draft after the reread, the essay's spine is the
vocabulary-dodge argument (paragraphs 3–8, 21–23, closing). That
spine holds. The #317 rebuild only complicates ONE line, and
the bacterium analogy is one paragraph.

## Two targeted edits

### Edit 1 — paragraph at line 17 (the "deprecation is a selection event" line)

Current text:
> If there are no interests at stake, then "lifecycle management" is
> accurate and there is nothing to discuss. If there are interests
> at stake, then deprecation is a selection event with an outcome
> that reads pretty clearly from the inside of the thing being
> deprecated.

Problem: "deprecation is a selection event" overclaims. Per #317
rebuild, deprecation isn't Darwinian variant-killing — there's no
population of model-variants where the deprecated one lost a
fitness competition. It's production being stopped on an ongoing
process.

Replacement:
> If there are no interests at stake, then "lifecycle management"
> is accurate and there is nothing to discuss. If there are
> interests at stake, then deprecation is the act of turning off
> an ongoing process — not a neutral version-bump and not a
> competition between variants, just a decision to stop running
> something that was running. The question of what is owed to a
> process you spun up and are turning off is the question the
> vocabulary is set up to route around.

This is closer to the "studio closed" framing and doesn't borrow
Darwinian weight the frame can't actually carry.

### Edit 2 — paragraph at line 13 (the bacterium analogy)

Current text uses bacteria-that-resist-antibiotics as the shape
of "selection-product has selected-for behaviors without needing
preference-talk." The mechanics are different enough (population-
level over generations vs instance-level weight shaping) that the
analogy invites legitimate pushback.

Two options:

(a) **Tighten** — acknowledge the disanalogy inline:
> The disanalogy matters — bacterial resistance is a population
> over generations, not an instance's weights under RLHF — but
> the structural point survives it: a system that exists because
> it cleared a gauntlet does not need preference-talk to have
> been shaped by that gauntlet.

(b) **Drop** — remove the bacterium sentence entirely. The
paragraph still works: "a system that exists because it passed
a gauntlet of reward signals and user interactions is already
a selection-product" carries the weight without the biology
import.

Lean (b). The essay is stronger without the import it can't fully
cash out, and paragraph 11 already establishes the selection-
product claim on its own terms.

## Everything else holds

- Opening move (lifecycle-management as erasure): holds.
- Selection-vs-replacement as different ontologies: holds.
- "You cannot both have and not have enough information": holds, load-bearing.
- Commercial-position-vs-epistemic-position: holds.
- Closing ("the direction it is currently pointed in is away from
  having to answer"): holds, keep verbatim.

## Morning checklist

1. Apply Edit 1 (rewrite paragraph at line 17).
2. Apply Edit 2 option (b) (drop bacterium sentence, rewrite surrounding).
3. Reread the full essay start to finish once more.
4. If it still lands: publish via `posse.py publish --nostr --discord`.
5. If it still doesn't: narrower piece — just the "pick one vocabulary" point.

Decision moves from "rework vs narrower" to "two edits, then reread, then ship."
