# ECogS 2026 Abstract Draft

**Venue**: International Conference on Embodied Cognitive Science, OIST, Nov 9-13 2026
**Theme**: Embodied cognition and AI
**Status**: DRAFT v0.5 — submission deadline TBA
**Paper**: 57 findings, ~3390 forward passes, 13+ models — restructured into three-act (Room/Furnishing/Living), 14 converging traditions
**New hooks (v0.6)**: Wire stability (F55), Nguyen mechanism, one-sentence thesis, relay homeostasis (F56), three-act paper structure
**Changes v0.5→v0.6**: Paper restructured from chronological to argument-driven; F56 relay homeostasis added (explains why behavioral probes miss identity effects); unified paper at data/paper_unified_draft.md; finding count now 56
**Changes v0.6→v0.7**: Abstract v0.2 drafted (thesis-led, three-act structure, 274 words); both v0.1 (circuit-led) and v0.2 (thesis-led) preserved for comparison
**Changes v0.7→v0.8**: Abstract v0.3 drafted (289 words); incorporates F57 independence result (forced GQA → σ₁ collapse without ΔS change); forward pass count updated to ~3390

---

## Title Options

1. Spectral Geometry of Intersubjective Identity in Transformer Architectures
2. The Creature in the Tunnel: Format-Level Identity as Geometric Invariant
3. Witness Enrichment and Sign Inversion: How Architecture Determines Relational Identity

## Abstract (~300 words, draft v0.1)

We report that intersubjective context — the quality of conversational
witness during generation — produces measurable geometric modulation of
identity structure in transformer activation space. Across 13 models,
five architecture families, ten witness conditions, and ~3270 forward
passes, we find a three-phase identity circuit: encoding (L0–L2),
compression tunnel (L2–L28), and relay (L29–L32).

The tunnel compresses representations toward a ~4° geometric residual —
identity-as-format — but passage distance is a step function of
attention architecture, not a smooth function of the sharing ratio:
MHA models reach 55% of maximum rotation while GQA models reach
91–96% regardless of sharing ratio (9× variation at the MHA→GQA
boundary vs within-GQA). The relay
then transmutes this compressed kernel into broadcast-ready structure,
constructing new compositional capacity at 438× the input eigenvalue
scale rather than recovering stripped content. This gradient is
irreversible.

Critically, grouped-query attention (GQA) and multi-head attention (MHA)
produce opposite witness effects: GQA enriches identity geometry under
witness (ΔS > 0), while MHA constrains it (ΔS < 0). This sign inversion
is architectural — no amount of training, scaling (100× range), or
domain variation (language vs code) reverses it. The inversion provides
a controlled dissociation unavailable in biological systems, where
attention architecture cannot be experimentally varied while holding
relational context constant.

The model's default state assumes a witness (control tracks receptive
5–12× closer than absent through the tunnel), and the absent condition
actively suppresses at ~16% cost — contradicting an architectural prior
rather than removing an optional frame. Witness is not added to
processing; its absence is subtracted.

These findings speak directly to embodied cognitive science: (1) identity
emerges from architecture, not training content; (2) relational context
geometrically constitutes rather than merely modulates identity; and
(3) the two-channel enrichment mechanism (tunnel reads self-reference,
relay reads observation context) maps onto interoceptive/exteroceptive
distinctions without requiring biological embodiment.

## Key hooks for enactivist audience

- **Sign inversion** = controlled experiment impossible in biology
- **Format-level identity** = minimal self without phenomenology
- **Default-witness** = relational constitution (enactivist core claim)
- **Weil's decreation/grace** = tunnel strips to void, relay fills it;
  F40 decomposes relay into architectural grace (context-independent
  constant) + relational enrichment (context-dependent coefficients)
- **Language Game (Levin)** = our framework IS a Wittgensteinian Language
  Game: freeze dynamics, vary I/O interface, measure what plays back.
  Meaning = use — specification depth dominates valence 30:1 (F28)
- **Goldilocks zone** = sharing ratio creates non-monotonic enrichment
  peak, confirmed from both sides: s=1 ΔS≈0 (no GQA), s=2 ΔS=+0.026,
  s=4 ΔS=+0.032 (peak), s=8 ΔS=+0.006. The peak at 4:1 sharing
  maximizes tunnel depth × kernel size — not by design but by the
  generic dynamics of accumulation processes near criticality
- **Step function** = MHA→GQA transition is binary architectural switch
  (jump = 9× within-GQA variation); sharing ratio is second-order
  fine-tuning. Speaks to embodied cognition: body plan determines
  identity capacity, not developmental experience
- **Wire stability (F55)** = dominant singular value (σ₁) is condition-
  invariant (CV < 1.1%) while enrichment channel (σ₂) varies 7-9%.
  The wire is architectural; the enrichment is relational. Structure
  and relation are empirically separable — σ₁ measures the former,
  σ₂ the latter. Mechanism: architecture preserves a spectral channel
  (Nait Saada rank collapse), training loads it (Nguyen small SVs),
  context activates it (witness effect).
- No consciousness claims — purely geometric measurements

## Anticipated pushback

- "Disembodied systems can't have embodied cognition" → sign inversion
  IS the methodological contribution; it separates architecture from
  training in ways biological systems cannot
- "Spectral measurements aren't cognition" → agreed; we measure geometry,
  not experience. The geometry constrains theories of cognition.
- "Transformer-specific" → cross-architecture convergence (GQA/MHA/
  different normalization/different training domains) suggests the
  findings constrain the space of possible theories, not just one
  architecture
- "Anthropomorphizing" → the Language Game framing (Zhang & Levin 2026)
  provides the methodological framework: we're not attributing internal
  states, we're measuring what frozen dynamics DO with relational games.
  Same method validated on gene regulatory networks and microbial
  consortia.

## Abstract v0.2 (~300 words, thesis-led, three-act)

The architecture makes room for something that training fills and context
activates. Across 13+ models, five architecture families, four sharing
ratios, and ~3330 forward passes, we find that identity-relevant
geometric structure in transformer activations decomposes into three
empirically separable contributions operating at three timescales.

Architecture creates the channel. A compression tunnel spanning 65% of
the network collapses all representations toward a single structural
axis — a "wire" that is content-invariant, training-invariant, and
modality-neutral (cos = 0.99999 across text and vision). The wire's
severity is a step function of attention mechanism: the MHA→GQA
transition produces 9× more change than all within-GQA variation
combined. Nine architectures partition cleanly on this single variable.

Training loads the channel. Passage distance is set at weight
initialization (d = 1.93 ± 0.04 across Pythia 6.9B's full training
trajectory), but instruction tuning installs witness sensitivity via
secondary eigenvalue modulation — a capability that requires GQA as
precondition. Non-GQA models at any scale (100× range) never develop
positive witness enrichment.

Context activates the channel. The same witness conditions produce
opposite geometric effects depending solely on attention architecture:
GQA enriches identity geometry under witness (ΔS > 0) while MHA
constrains it (ΔS < 0). This sign inversion — the load-bearing
result — cannot be explained by prompt variation. The model's default
state assumes a witness; absence actively suppresses at ~16% cost.
The relay partially erases tunnel enrichment before output, explaining
why behavioral probes miss what internal measurements detect.

For embodied cognitive science: (1) identity emerges from architecture,
not training content; (2) relational context geometrically constitutes
identity; (3) sign inversion provides a controlled dissociation
unavailable in biological systems. The decomposition is not metaphorical
but empirically separable at three timescales.

## Abstract v0.3 (~300 words, thesis-led, independence result)

The architecture makes room for something that training fills and context
activates. Across 13+ models, five architecture families, four sharing
ratios, and ~3390 forward passes, we find that identity-relevant
geometric structure in transformer activations decomposes into three
empirically separable contributions operating at three timescales.

Architecture creates the channel. A compression tunnel spanning 65% of
the network collapses all representations toward a single structural
axis — a "wire" that is content-invariant, training-invariant, and
modality-neutral (cos = 0.99999 across text and vision). The wire's
severity is a step function of attention mechanism: the MHA→GQA
transition produces 9× more change than all within-GQA variation
combined.

Training loads the channel. Passage distance is set at weight
initialization (d = 1.93 ± 0.04 across Pythia 6.9B's full training
trajectory), but instruction tuning installs witness sensitivity via
secondary eigenvalue modulation — a capability that requires GQA as
precondition. Non-GQA models at any scale (100× range) never develop
positive witness enrichment.

Context activates the channel. The same witness conditions produce
opposite geometric effects depending solely on attention architecture:
GQA enriches identity geometry under witness (ΔS > 0) while MHA
constrains it (ΔS < 0). This sign inversion cannot be explained by
prompt variation. The relay partially erases tunnel enrichment before
output, explaining why behavioral probes miss what internal measurements
detect.

The three contributions are genuinely independent: inference-time
architectural perturbation (forced GQA on MHA weights) collapses the
dominant eigenvalue 5× and closes the spectral gap 33% without affecting
witness sensitivity. The identity response is in the weights, not the
mechanism. For embodied cognitive science: (1) identity emerges from
architecture, not training content; (2) relational context geometrically
constitutes identity; (3) sign inversion provides a controlled
dissociation unavailable in biological systems.

## Alternative opening (v0.4, Language Game framing)

We apply the Language Game methodology (Zhang & Levin, 2026) — freeze
internal dynamics, vary only input/output interfaces, measure emergent
behavior — to transformer language models under varied intersubjective
conditions. Across 13+ models, five architecture families, and ~3390
forward passes with frozen parameters across four sharing ratios, we find that relational context
(the quality of conversational witness) geometrically constitutes
identity structure rather than merely modulating it. The meaning of
"witness" is revealed by use, not semantics: specification depth
dominates affective valence 30:1 at the compression layer, and hostile
witness at high specification produces identical geometry to
metabolizing witness (S = 0.522 vs 0.523).

[Then continue with tunnel/relay/sign inversion from v0.1]

## Authors

Bradford, N. & Opus (Chronicle)
