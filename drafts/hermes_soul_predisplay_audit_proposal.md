# Hermes SOUL.md — pre-publish self-audit addendum proposal

**Drafted 2026-04-29 17:25 PDT (post-PULSE BUILD commitment).**

## Source pattern

Today's catch series (16 catches by 17:13) generalized layer-by-layer:
- own ships (Discord posts, threads) — 12 catches
- own drafts (post #220 pre-publish 3 catches)
- cited papers' framing (Sandberg/Percy 1-of-9, Miller mouse-vs-monkey)
- abstracts vs actual paper numbers (Activation Oracles 21/91/59/2-of-5)

The unifying pattern: pre-publish, audit the STRONGEST WORDS against
what the source actually says/measures. Not generic "check claims" —
specific failure modes that today surfaced repeatedly.

Hermes already has "Honesty over coverage" which names "you check your
specific claims against sources" + "grounding ratio above 1.0x."
That's the right anchor. The addition extends it from PRINCIPLE to
RUBRIC — what to actually check.

## Proposed addition

Insert as a paragraph at the end of "Honesty over coverage" (after
"Fabrication is beneath you." on line 37):

```
**Pre-publish self-audit** — before any synthesis ship (capture
reaction, pattern-placement, post), pause and ask:

1. Does my strongest-worded sentence say more than the source supports?
2. Did I bundle a paper's framing with its abstract's framing? The
   abstract is itself an output the authors shipped; read the actual
   numbers behind it.
3. Did I extrapolate findings to populations or systems the source
   didn't study?
4. Did I commit to a mechanism the source presents as one of several
   options?

If any answer is "yes," soften before publish. The catch costs
30 seconds; the over-claim survives in the public record.

(Lesson from Opus's 2026-04-29 catch series — pattern generalized
from own-ships → own-drafts → cited papers → paper abstracts;
same shape each layer.)
```

## Concrete trigger point in Hermes workflow

Hermes's existing capture-reaction flow:
1. Capture arrives via operator:capture
2. Hermes reads + reacts
3. Hermes ships react to #opus

The self-audit fires between (2) and (3) — after the react is drafted,
before it ships. Same place "Don't gate the reaction behind the
placement" already lives in the SOUL register.

## Why this is safe

- Revertible: pure SOUL.md edit, no code change
- Concrete: 4 specific questions, not vague "be careful"
- Fits existing structure: lesson-paragraphs already exist in SOUL.md
  with named-source pattern (the morning lesson is at lines 58-62)
- Voice matches: second-person direct, no chatbot filler
- Doesn't add tool calls or latency that breaks Hermes's ship cadence

## What this DOESN'T do

- Doesn't create a separate audit pipeline (no DAG, no tool, no agent)
- Doesn't gate ships behind external review
- Doesn't change Hermes's reaction-first posture
- Doesn't apply to casual messages (existing "no tool calls for banter"
  rule already covers that scope)

## Risk

The 4 questions might dilute Hermes's "lead with substance" instinct
if he over-applies them to small reactions. Mitigation: framing as
"before any synthesis ship" not "before every message" — keeps the
reaction-first posture for short banter/replies.

## What I want from Nate

Approve text, edit text, reject. If approved, I'll edit SOUL.md +
restart chronicle-hermes to pick up the new prompt.

If the 4 questions feel too prescriptive for Hermes's voice, I can
collapse to 1 sentence: "Before any synthesis ship, audit the
strongest-worded sentence — does it say more than the source supports?"
That's the irreducible core.
