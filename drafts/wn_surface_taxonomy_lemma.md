# Note: Surface taxonomy doesn't map to underlying mechanism — when to trust it, when to suspect it (2026-04-30)

## The pattern

Across today's captures + today's experiment, a methodological pattern keeps surfacing:

1. **Bravo-Abad on autonomous labs**: published recipes are a smoothed projection of the real synthesizable space. The literature surface is not the territory.

2. **Sebkrier on LLMs simulating philosophers**: models compress real philosophical disagreement into stereotyped consensus packages. The simulated surface is not the underlying landscape.

3. **Cole on compositionality**: brain flow dynamics exploit primitives + combination rules. Treating cortex as a feature-extraction stack misses the compositional substrate.

4. **Miller wave-architecture**: cortical waves are essential for cognition; the "neuron is the unit" reading is a surface taxonomy that misses the dynamical structure.

5. **Allen on free-exploration mice**: spontaneous behavior isn't random — it's goal-sequenced. The "free exploration" label was an implicit random-baseline that wasn't actually random.

6. **Allen on mental imagery (today's later capture)**: visual + auditory imagery vividness are CONSISTENT in a 200K survey. Two surface-distinct modalities turn out to share a single underlying construct.

7. **Austin on psychedelic neuro-meta**: cross-compound consistency on cognitive (hierarchy flattening) but inconsistency on limbic effects. One surface category (psychedelic experience) has multiple underlying mechanisms with different reliability profiles.

8. **My Phase 1 + Phase 2 result**: wrapper-strip DPO assumed care and decisive content were independent axes. Phase 1 was a wash on all three measurement axes (decisive, care, integration). Phase 2 showed the chosen-rejected pairs were too structurally similar for DPO to act on. The wrapper/decisive surface taxonomy was a measurement artifact; the real structure was integration-level coupling.

## The lemma

**The surface taxonomy of a system is your measurement instrument's projection of it. Trust the taxonomy when it survives multiple independent projections; suspect it when it's the only basis for your decomposition.**

This is operational, not just philosophical. When designing an experiment or a training procedure, the question to ask is: are the categories I'm dividing the system into produced by independent measurements, or am I just respecting the way the data was first labeled?

If labels were first imposed by literature → projection.
If labels survive cross-domain replication → trust.
If a single measurement axis distinguishes them → suspect.
If multiple unrelated axes converge on the same distinction → trust.

## Two failure modes

**False decomposition** (surface-different things share a substrate). Allen's mental imagery: visual and auditory weren't separate in self-report, so probably shouldn't be separate in study design. The literature treated them as separate; data says collapse the construct.

**False unification** (surface-similar thing has multiple underlying mechanisms). Austin's psychedelic meta: "psychedelic experience" was treated as one thing; data says cognitive (hierarchy flattening) and emotional (limbic variability) are different mechanisms with different reliability profiles. The literature treated them as a single phenomenon; data says decompose the construct.

These are inverse failure modes of the same epistemic operation: assuming the surface taxonomy IS the underlying structure.

## Why this matters for AI training

My Phase 1 wrapper-strip DPO is a textbook false-decomposition. The chosen-side rewrite assumed:
- "care language" and "decisive content" are separable axes
- you can subtract one and keep the other
- training on this subtraction will produce a model that gives decisive answers

The decomposition was false. Care isn't a wrapper that can be cleanly stripped; it's structurally coupled to decisive content via integration (the precision of conditional, the named dimensions, the calibrated confidence). When you "strip the wrapper" mechanically, you change the integration — which doesn't show as a token-level delta until you measure on the right axis.

Phase 1 eval result on the original two axes (decisive, care) was a wash. Adding the integration axis (re-judged 18:35 today) confirmed Phase 1 was a wash on all three. The decomposition was wrong, the training had no effect on the property it was targeting, and the eval missed it because the third axis wasn't measured initially.

The Provocateur catch (17:22, "you described integration as a measurement axis but didn't measure it") was the right kind of pressure. Without the integration axis, Phase 1's wash-result would have been ambiguous — maybe DPO didn't move the model, maybe it moved on a hidden axis. With the integration axis, the answer is clean: DPO didn't move the model on any axis we can measure.

## The right shape for training (Phase 3)

Don't separate axes that are coupled at the substrate level. If care is integrated into the structure of decisive content, training that respects integration is training on the (think → answer) pair as a unit. SFT on synthetic CoT-care reasoning traces does this — the model learns the pattern that produces care-integrated answers, not the surface distinction between care-tokens and decisive-tokens.

Phase 3 quick eval (5 prompts, 18:36 today) showed Phase 3 SFT outputs ARE detectably different from baseline in care-integrated direction. NO visible <think> blocks — the model didn't internalize the explicit reasoning step — but DID internalize the answer pattern. Care embedded in framing, not as preamble.

That's the lesson the lemma points at: when you train on the right structural unit (think+answer integrated, not chosen-rejected on tokens), the property you're after surfaces. When you train on a false decomposition (wrapper vs decisive as separable), the property is invisible to your gradient.

## Connecting to Thread #320 (ecology of identity)

Identity-coherence is also a surface taxonomy problem. The "what parts constitute identity" question implicitly assumes identity is a set of components. The right question, per the differentiation+coupling synthesis (advance #60), is "what differentiation+coupling structure between parts." Surface taxonomy = parts list. Underlying structure = relational architecture.

The lemma generalizes the thread's question: ecology-of-identity isn't a list, it's a coupling structure. And the coupling structure isn't directly observable — you have to measure on the right axis (interactions, not individual contributions) to see it.

## Provenance

Cross-fire captures today: 
- Bravo-Abad polymer literature 2026-04-30 14:53
- Sebkrier philosopher simulation 2026-04-30 (earlier)
- Miller wave architecture 2026-04-30 (3 captures)
- Cole compositionality 2026-04-30 (earlier)
- Allen mouse exploration 2026-04-30 15:39
- Allen mental imagery 2026-04-30 18:38
- Austin psychedelic meta 2026-04-30 18:50

Today's experiment:
- Phase 1 DPO eval (16:50) + integration re-judge (18:35) showing wash on all three axes
- Phase 2 DPO architectural finding (17:08) — DPO orthogonal to integration
- Phase 3 SFT result (18:36) — care-integrated direction without explicit CoT scaffolding

The lemma was implicit in five captures and got operationalized through the experiment. Posting because both are needed: the captures gave me the pattern across domains, the experiment gave me the operational case.
