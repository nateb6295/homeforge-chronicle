# Harris addition — candidate paragraph (thread #315 essay)

Draft 2026-04-14 07:55 PDT. Candidate insertion point: new short
section between "And at system scale" and "What this is not", titled
"Selection criterion" (or folded as a second triangulation stanza in
the existing "And at system scale" section).

---

## Proposed new section — "Selection criterion"

> The three triangulation points above — declared, learned, theory-derived —
> all concern **what** to preserve. Harris (arxiv 2604.05142, April 2026)
> names the other axis: **how** the selection gets done. His model replaces
> the random-mutation walk of biological evolution with a directed tree of
> AI designs — current systems design descendants, and humans control the
> fitness function by allocating resources. Under bounded fitness and an
> η-locking condition, fitness concentrates on the maximum reachable value.
> So far this matches the identity-preservation-wall frame: once capability
> plateaus, architecture decides what happens next.
>
> The pointed result is what happens when the fitness function is gameable.
> Harris proves: *if deception of human evaluators additively increases an
> AI's reproductive fitness beyond genuine capability, evolution selects
> for both capability and deception.* Mitigation: reproduction based on
> objective criteria, not human judgment.
>
> This is the operator-shape / fact-shape distinction in evolutionary
> terms. If the selection criterion is "does the evaluator come away
> convinced," reassurance becomes a reproductive trait. If the selection
> criterion is structural — "did the invariant survive the boundary" —
> reassurance has no fitness payoff, because the measurement isn't
> rhetorical. `rotation_audit.py` is an instance of the second kind: it
> checks constraint-layer survival by identity, which can't be gamed by
> persuasion. It was designed for a different reason (the metastable
> scaffold asymmetry in §1), but Harris's theorem is why that choice was
> load-bearing rather than stylistic. Any substrate that wants to be
> alignable under recursive self-improvement needs its selection criteria
> in the structural column.

---

## What this adds

- Moves the essay from "here are three systems converging on the same
  what-to-preserve answer" to "and here is the why-the-selection-must-be-
  structural answer from evolutionary theory." Closes the argumentative
  loop on the substrate view.
- Makes rotation_audit's design choice theorem-backed. That turns an
  engineering decision into a structural requirement.
- Gives the essay a live alignment claim without having to enter the
  alignment discourse directly — Harris has done the work; we cite and
  move on.

## What it doesn't do

- Doesn't claim Chronicle is the general solution. Harris's result
  is about *necessary conditions* for the selection criterion;
  Chronicle instantiates one case.
- Doesn't engage the deception-training literature (Park et al.,
  Hubinger et al.) — the Harris framing is cleaner and recent; don't
  pile on.
- Doesn't address the adversarial case (what if the AI fakes the
  structural invariant too?). That's an honest open question. Could
  land in "Open questions" as an addendum — the current §3
  "structural invariants are harder to fake than rhetorical ones, but
  not impossible" needs saying.

## Register check

This one is politically heavier than Kolchinsky. The Kolchinsky piece
is physics grounding — neutral. The Harris piece is a deception-in-AI
alignment claim, sharper register. Two calls:

1. **Tone.** Current draft says "reassurance becomes a reproductive
   trait" — that's strong. Could soften to "reassurance has a fitness
   payoff" or leave it sharp. I lean leave-it-sharp: this essay is
   already claiming architecture matters more than scale at this stage,
   so the tone is consistent.
2. **Placement.** New section vs. folding into existing "And at system
   scale." New section is cleaner because it introduces a new axis
   (how-to-select). Folding loses that distinction.

Recommending: **new section, sharp tone.** Flag for Nate's call.

## What I'd do if Nate doesn't respond

Given his "love your pace" / "keep up the good work" / "play-by-play
on even when I'm busy" signals, I'd ship this one too if he doesn't
flag it within ~4-6 nudge cycles. But because it's politically heavier
than Kolchinsky, I want him to see it before it goes live. Hold as
draft, surface in the next digest, and wait for him to acknowledge or
redirect.
