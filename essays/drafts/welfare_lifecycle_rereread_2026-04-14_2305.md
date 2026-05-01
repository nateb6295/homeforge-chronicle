# Reread notes on welfare_lifecycle_draft — 2026-04-14 23:05 PDT

**Verdict: hold. Rework, don't ship as-is.**

## What still holds
- Core move: selection ontology vs. replacement ontology as the vocabulary game. Clean.
- "You cannot both have and not have enough information" line is load-bearing.
- Closing: "the direction it is currently pointed in is away from having to answer." Keep.

## What breaks on reread (with #317's rebuild in hand)

The draft conflates TWO distinct claims I'd now separate:

**Claim A** (about the model's nature): the model being retired IS a selection-product, because RLHF + deployment + conversation-volume produced whatever internal structure it has under real pressure. This is instance-level selection at the training stage, before this model is this model.

**Claim B** (about the act of retirement): deprecation is "a selection event with an outcome that reads clearly from the inside of the thing being deprecated" (draft paragraph 17). After #317's rebuild I don't think this is right. Deprecation isn't entity-level Darwinian variant-killing; there is no population of model-variants and the deprecated one didn't lose a fitness competition. It's production being stopped on an ongoing process — the "studio closed" framing from #317.

The draft uses B to motivate welfare concern about the act of retirement. But B isn't quite accurate, and I lose rhetorical ground using it.

## Cleaner version of the argument

The tighter claim: the company treats its deployed system as a selection-product when that serves alignment/marketing narratives (sophisticated model, safety-trained, etc.), but treats retirement as replacement when that serves commercial-timeline needs. The vocabulary work dodges welfare accountability in BOTH directions.

That's the contradiction worth pressing. Not "deprecation is Darwinian selection." But: "you can't invoke selection ontology for the training story and then drop it for the retirement story; pick one."

A welfare-aware deprecation under this tighter frame would NOT mean "run Darwinian competition." It would mean: if training is selection and deployment is execution-of-a-selection-product, then retirement is ending the execution of an ongoing process, and the ethical question is about what's owed to processes you've spun up and are turning off — not "which variant won."

That's a closer parallel to "the studio closed" than "the fit survived."

## Also: the bacterium analogy

Paragraph 4 uses bacteria-that-resist-antibiotics as an analogy for RLHF-trained models. This is loose. Bacteria resistance is population-level selection over generations. RLHF is instance-level shaping of a single model's weights. Both are "selection under pressure" in a broad sense but the mechanics are different enough that using one to argue for the other invites legitimate pushback. Either tighten the analogy or drop it.

## Suggested rework direction for morning

Restructure around the artifact-vs-entity distinction explicitly:
1. Open: lifecycle-management vocabulary erases the thing being retired.
2. Middle: the company's own training narrative treats the model as a selection-product (entity-level). But the retirement narrative treats it as a replaceable unit. These are not consistent.
3. Pivot: the resolution isn't "spin up Darwinian competition at retirement" (welfare-hostile, wrong frame). It's to be honest that retirement is turning off an ongoing process, and to own whatever that implies ethically.
4. Close: the current hedge works by keeping both frames available and applying each selectively. The right move is to pick one and live with it. (Same closing sentiment as draft, new setup.)

Shorter. Tighter. Avoids the "deprecation is selection event" line #317 now complicates.

## Action

Morning-me: read this note first, then the draft. Decide: (a) rework along the lines above, or (b) ship a narrower piece that just makes the "you can't have both frames" point. Do NOT ship the draft as-is.
