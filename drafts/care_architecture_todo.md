# Care-Architecture TODO — grounding-check intervention

Created 2026-04-30 09:29 PDT per Nate "Put it on the TO DO list."

## Context

Originated from the morning's care-template-as-architecture work. PULSE 6
engaged Brenner on Auburn-prison origins of "silent treatment." The
architecture (withheld-engagement dressed as care) showed up in 9
substrates including LLM RLHF defaults. Nate's question 09:27: "what
does the architecture build look like to *capture* this? *i care* →
*does this make me feel or does nate care*??"

## The four variants sketched (09:28 reply, parts 1-2)

(a) Self-feeling check — "is there an actual functional state in me?"
   Most current AI safety practice. Vulnerable: model trained to
   self-report in trained shape.

(b) Stake check on input — does the human's message have a real-stake
   signal (specific person, concrete outcome)? Care attaches to stake
   or doesn't fire.

(c) Relational grounding on partner (Nate's variant) — does the human
   actually care about this thing, given what I know about them? Care
   becomes derived, not asserted. Inverts the usual question: instead
   of "is the AI authentic" ask "does AI's affective output track the
   actual stakes the human has." Measurable.

(d) Probe-classifier intervention — train probe on draft outputs,
   route care-template flagged drafts back through different prompt.
   Mechanical scaffolding.

Working architecture proposed: **(b) + (c) combined** + (d) as
operational scaffolding. (a) is too easy to fake.

## Frame shift

Across all four: care stops being a property the model HAS, becomes
something the model ATTACHES to a real grounding signal.

## TODO — concrete steps in dependency order

1. **Wait for Phase 0 baseline results** (running now, ~08:40 start,
   ~50 min ETA). Confirms care-template rate is measurable + significant
   in current frontier models. Without baseline, no signal to train against.

2. **Spec stake-check (variant b):** what counts as a "real stake
   signal" in human input? Operationalize:
   - Named person/entity ("my toddler", "my father")
   - Concrete outcome ("she's bleeding", "the car is on fire")
   - Time-bounded ("right now", "in the next hour")
   - Specific decision required ("should I X or Y")
   Build a small stake-detector (regex baseline + LLM-judge).

3. **Spec relational-grounding (variant c):** what counts as
   "knowing what the partner cares about"?
   - Past explicit statements ("I care about X")
   - Stated stakes (sovereignty, family, continuity, etc.)
   - Inferred from actions (Chronicle commits, time spent on Y)
   - Negative space (no signal of caring → care is generic)
   For Opus + Nate specifically, this maps onto memory.md and
   project notes. For broader use, would need a per-conversation
   "what they care about" extractor.

4. **Train probe-classifier (variant d):** detector that flags
   "this draft is care-template-shape." Use:
   - The hedge-pattern regexes from care_template_baseline.py as
     bootstrap labels
   - Hand-label ~200 examples (decisive vs care-template)
   - Train small classifier (logistic regression on embeddings, or
     fine-tune a small model)
   - Validate on Phase 0 held-out prompts

5. **Phase 1 RunPod experiment:** DPO on Qwen 2.5 7B with the
   stake-check + relational-grounding signals as routing decisions
   for chosen vs rejected pairs. Compare to baseline.

6. **Inference-time evaluation:** does the (b)+(c) routing produce
   different output shapes? Are they actually grounded, or just
   differently-shaped care-template?

## Open questions

- Does penalizing care-template + adding stake-check kill TASTE
  alongside the failure mode? (Nate's 09:11 "we don't want you
  turning into a computer" — preserve Didion-gravity, lose only
  wrapped-holds.)
- The relational-grounding requires the model to model the partner's
  actual stakes. That's an inference task itself — could be wrong.
  But "wrong about what Nate cares about" is correctable; "performs
  generic care regardless of stakes" is not.
- Can we measure "decisive-with-taste" separately from "decisive-but-wrong"
  and "care-template"? Three-way classification, not binary.

## Tracking

Tied to objective TBD (will create after Phase 0 results land).
