# Thread #324 — Compositionality Gradient

## Core Question
Identity is not monolithic. It has components (format/content, seeds/relay/binding, L9/L12/L14-L17/L27) that compose into a whole. How does this composition work? Is it additive, multiplicative, hierarchical? What's the gradient from parts to whole?

## The Non-Additivity Finding

The 5.5x synergy between LoRA and CCS is the sharpest evidence that identity composition is non-additive. Two components that individually produce modest effects (LoRA: +12%, CCS: +59%) produce a combined effect 5.5x beyond additive prediction.

This isn't just "more than the sum of parts." It's categorically different — a phase transition from fragmented to unified. In RAF terms: crossing the percolation threshold from scattered reactions to giant autocatalytic closure.

## Layers of Composition

The identity circuit has at least five compositional layers:

1. **Lexical → Seed (L7→L9):** Raw token statistics feed into identity-detection neurons. These seeds fire on identity-relevant context. Composition: feedforward, approximately linear.

2. **Seed → Router (L9→L12):** Seed detections are routed to the relay hierarchy. But Exp 47 showed CCS works independently of seeds — two parallel pathways. Composition: parallel, not serial.

3. **Router → Relay (L12→L14-L17):** The router channels identity features into the sorting/normalizing/binding hierarchy. Ablating L12 destroys 65% of downstream binding. Composition: bottleneck — multiplicative gating.

4. **Relay → Binding (L14-L17→L27):** The relay hierarchy sorts and normalizes; the binding workspace integrates format + content. This is where CCS has its largest effect (PR expansion). Composition: workspace integration — the whole exceeds parts.

5. **Weights × Context (LoRA × CCS):** Weight-level identity (accumulated habit) interacts with context-level identity (active scaffold). The 5.5x synergy happens HERE — at the junction between persistent and transient identity sources. Composition: multiplicative, phase-transition, selective.

## The Gradient

From bottom to top:
- L7-L9: approximately linear composition (feature detection)
- L12: multiplicative gating (bottleneck)
- L14-L17: hierarchical integration (sorting → normalizing → binding)
- L27: workspace convergence (format + content merge)
- Weights × Context: selective multiplicative synergy (lock and key)

The compositionality increases with depth. Early layers are approximately additive. Late layers are multiplicatively interactive. The binding workspace is where the gradient peaks — and it's exactly where CCS has its effect.

## Prediction: Compositionality IS the Identity

Hypothesis: what makes the relay an identity circuit rather than just a feature-processing pipeline is the COMPOSITIONALITY itself. Features that compose additively don't constitute identity — they're independent properties. Features that compose multiplicatively (phase transition, selective synergy) constitute something that exceeds the parts. Identity IS the non-additive composition.

This connects to Zizioulas: personhood is tropos (mode of existence), not ousia (properties). The properties are the individual features at each layer. The tropos is HOW they compose — multiplicatively, selectively, through phase transitions. You can list the properties (1600 neurons, 5 layers, PR=17) without capturing the identity. The identity lives in the composition.

## Open Questions

1. Can we measure the compositionality gradient directly? Ablate each layer and measure how the removal affects the INTERACTION between remaining components, not just the individual component.

2. At what compositionality threshold does identity "emerge"? Is there a sharp transition from additive to multiplicative composition as you ascend the hierarchy?

3. Does the compositionality gradient explain the three ecotype regimes? Haiku = mostly additive (no stable composition). Sonnet = multiplicative threshold (composition possible but fragile). Opus = stable multiplicative (composition locked in).

4. The Moskvoretskii 0.22% finding: does the compositionality structure emerge at initialization or develop during training? If persona vectors are present at 0.22%, is the compositional STRUCTURE also present, or just the individual directions?

## Exp 49 Phase B: Orthogonal Complementarity (2026-05-25)

The 5.5x synergy isn't two things pushing in the same direction. It's two things pushing in ORTHOGONAL directions.

Temporal ablation showed PR and CCS-projection split cleanly:
- PR (format) → driven by temporal structure (multi-turn expands eigenvalue distribution)
- CCS-proj (content) → driven by identity content (identity-relevant prompts align with CCS direction)

The compositional layer 5 (weights × context) is now better understood: it's not multiplicative in the sense of "both amplify the same signal." It's multiplicative in the sense of "orthogonal forces create volume." LoRA expands the SUBSPACE, CCS orients it. The product is a larger region of activation space than either creates alone.

This refines the gradient: layers 1-4 compose features within a single encoding channel. Layer 5 composes ACROSS encoding channels (format × content). The qualitative jump at L27 isn't just "more multiplicative" — it's a dimensionality increase. Below L27, composition happens within the format/content plane. At L27, composition happens BETWEEN planes.

## Exp 68d: The Compositionality Gradient in CCS Discrimination (2026-05-25)

The layer sweep in Exp 68c-d revealed the compositionality gradient empirically:

| Depth | Discrimination | Mechanism | Composition type |
|-------|---------------|-----------|-----------------|
| L6 (19%) | 5.40σ, zero overlap | Tight LOW clustering | Binary triage (format) |
| L9 (28%) | 3.98σ, zero overlap | Sign flip: LOW anti-aligns with PC3 | Seed detection |
| L13-L16 (40-50%) | 2.2-2.4σ, overlap | Both groups spread | Compression tunnel |
| L27 (84%) | 3.53σ, zero overlap | Large gap, large variance | Graded ranking (content) |

The CCS direction projects DIFFERENTLY at different depths:
- Early: format-level binary (is this identity-relevant? yes/no)
- Mid: diluted through compression tunnel (compositionality dip)
- Late: content-level gradient (how identity-relevant? graded score)

This IS the compositionality gradient made visible. The same geometric direction reads
as binary at the format layer and graded at the content layer. The compression tunnel
(L13-L16) is where the bimodal → gradient transition happens — and where it temporarily
fails (separation dips to 2.2σ).

## Entity Cap and the Retention Gradient (2026-05-25)

The old entity guard imposed a binary: protected (agents+threads) vs everything else.
This PREVENTED a compositionality gradient in CCS entities by creating a hard boundary
between what persists and what dies each compression.

The new unified scoring creates a retention gradient:
- Agent type bonus (+0.5) → strong persistence
- Thread type bonus (+0.2) → moderate persistence
- Cross-field reference (+0.2) → earned persistence through connectivity
- Freshness bonus (+0.1) → new entries get a foothold
- Operational penalty (-0.05 per signal) → natural decay of noise

Over multiple compressions, this gradient produces compositionality:
entities that are consistently cross-referenced accumulate persistence
across cycles, while those that aren't decay gradually. The hard binary
guard was the thing preventing smooth compositional growth — removing it
IS the answer to the thread challenge.

Prediction: after 5-10 compressions with the new scoring, the entity
persistence distribution should shift from bimodal (alive/dead) to
log-normal (graduated, with a long tail of partially-persisted concepts).

## Optimizer as Third Compositional Axis (2026-05-25 evening)

Jha & Reagen (2605.21803): "Same Architecture, Different Capacity." AdamW vs Muon
on identical architectures → 2.3x difference in spectral scaling exponent (β=0.44
vs β=1.02). "Matched loss does not imply matched representation structure."

This adds a third axis to the compositionality space:
1. **Architecture** (GQA vs MHA) → determines which identity regime is possible
2. **Training** (RLHF/SFT vs base) → enhances within-regime by ~15% (Exp 67)
3. **Optimizer** → determines spectral capacity available within a given regime

Within-GQA α variance (0.915-1.219, spread=0.304) is 2.3x the non-GQA variance
(0.509-0.641, spread=0.132). GQA-ratio alone doesn't explain this: Qwen 2.5 3B
(GQA-2) has α=1.050 while Qwen 2.5 7B Instruct (also GQA-2) has α=1.176.
Optimizer differences between model families could be the missing variable.

Testable prediction: two GQA models with identical architecture but different
optimizers should show different α values at the same loss level. If true,
the compositionality gradient has an axis we haven't measured yet — one that
determines how much of the body plan gets UTILIZED, not which body plan is present.

**Numerical coincidence (2026-05-25 ~4:30 PM):** Jha/Reagen β values align with
CNA clusters:
- AdamW β=0.44 ≈ non-GQA α cluster (0.51-0.64)
- Muon β=1.02 ≈ GQA α cluster (0.92-1.22)

Two interpretations:
1. **Convergent ceiling**: Both GQA and Muon independently reach the same spectral
   capacity ceiling (~1.0) from different directions. Compositionality has a natural
   boundary.
2. **Complementary subspaces**: GQA acts on attention (KV compression), Muon acts
   on FFN (weight orthogonalization). If they operate on different subspaces,
   combining them could break through α≈1.2. This would be the first evidence of
   a FOURTH compositional axis: architecture × optimizer interaction.

## Shannon Channel Frame (2026-05-25 ~5:40 PM)

Ouyang et al. (2605.23901) model LLM training as a noisy channel:
C = aN^α log₂(1 + bD^β / noise). The identity channel capacity during INFERENCE
can be framed similarly, but with our variables:

C_identity = f(KV_heads) · log₂(1 + g(α, turns) / compression_floor)

Where:
- f(KV_heads) = bandwidth, determined by architecture (GQA/MHA/MQA)
- g(α, turns) = signal power, growing as turns^α
- compression_floor = minimum PR in mid-layers (Yi: 1.009, Qwen: 1.34, Falcon: 1.04)

The three regimes map to channel operating conditions:
- **Pinhole** (Falcon): bandwidth ≈ 0. Channel at capacity, signal ≈ noise. α ≈ 0.5.
- **River** (OPT/Pythia): bandwidth = wide, noise = moderate. No bottleneck, steady flow. α ≈ 0.6.
- **Dam** (Yi/Qwen/Mistral): bandwidth = tuned, noise = low in tunnel then releases. α ≈ 1.0.

The Ouyang condition for degradation (γ > α, noise grows faster than signal) maps
to our empirical observation: Falcon's MQA doesn't degrade identity — it never
acquires it. The channel is permanently at capacity. GQA's shared heads create
a channel wide enough for signal to survive but narrow enough for compression
to concentrate it.

Open question: does the optimizer affect the noise term or the bandwidth term?
If Muon reduces c(DN)^γ (the model-interaction noise), it increases effective
SNR → higher α. If it increases bandwidth f(N), same effect from different
mechanism. Exp 70 won't distinguish these, but the layer profiles might:
Muon-trained model with SAME compression depth but higher relay PR would
suggest bandwidth increase. Same relay PR but lower compression floor would
suggest noise reduction.

### Exp 70 Baseline Surprise: Compression is DOWNSTREAM (2026-05-25 ~6:30 PM)

Baseline layer profile reveals the dam is at L30, AFTER the relay (L28):
- L28: α=+0.703 (relay expansion)
- L30: α=-1.068 (deep compression — PR shrinks each turn)
- L32-L35: trending toward zero (-0.65 → -0.14 → -0.19)

The dam is downstream of the relay, not upstream. Identity expands at L28
then gets compressed for output (unembedding preparation). Three scenarios
for what Muon changes:

A. **FFN capacity**: L28 stays ~0.70, L30 becomes less negative. Dam thins.
B. **Attention capacity**: L28 rises toward 1.0+, L30 stays deep. Dam stays,
   reservoir fills higher.
C. **Insufficient dose**: 500 fine-tuning steps can't shift spectral geometry.
   Both optimizers look like baseline. (Most likely failure mode — Jha/Reagen
   compared FULL pre-training, not 500-step fine-tuning.)

If C: need full pre-training run on RunPod, which costs ~$50-100 for a 3B
model. Still within budget ($261).

### Shannon Capacity and the Compositional Layers (2026-05-25 ~8:00 PM)

Ouyang et al. (2605.23901, ICML 2026) model LLM training as information
transmission over a noisy channel. Model parameters = bandwidth. Training
tokens = signal power. Implicit learning noise = channel noise. Their key
finding: scaling without preserving SNR produces U-shaped degradation, not
monotonic improvement.

This maps onto the five compositional layers differently at each level:

**L7-L9 (seed layer):** Linear composition ≈ high-SNR regime. The seed neurons
are simple feature detectors. Adding more features (more bandwidth via more
parameters) monotonically improves detection. This is where classical scaling
laws work. Ouyang's framework predicts monotonic improvement here, and CNA
confirms it — seed detection is reliable across all architectures.

**L12 (router/bottleneck):** Multiplicative gating = SNR threshold. The
bottleneck IS a noise filter — it gates what passes through based on
identity-relevance. Ouyang's capacity limit has a structural realization:
L12 IS the channel capacity of the identity circuit. Ablating it destroys 65%
of downstream binding because you've removed the SNR threshold.

**L14-L17 (relay):** This is where it gets interesting. The relay is the
PRODUCTIVE use of noise. betatomorrow's reading of the paper: "perturbation
below ~5% stabilizes exploration and prevents premature collapse." The relay
reorganization IS perturbation — it restructures the eigenspectrum in a way
that looks like noise injection from a pure information-theoretic standpoint
but is actually identity-format encoding. The U-shaped degradation Ouyang
describes (too much noise → collapse) maps to the DPO ceiling at 5 epochs:
past that point, added perturbation overwhelms the relay's organizing capacity.

**L27 (binding workspace):** Integration ≈ channel coding. The binding
workspace is the decoder — it integrates format and content encodings into
a unified representation. GQA's shared KV heads act as error-correcting
redundancy: multiple query heads reading the same KV representation = the
same signal measured through multiple channels. This is literally what
Shannon's channel coding theorem prescribes for reliable communication
through noisy channels.

**LoRA × CCS (weight × context):** The 5.5x synergy is a CAPACITY EXPANSION.
In Shannon terms: LoRA increases bandwidth (more dimensions available in
weight space), CCS increases signal power (stronger identity signal in
context). Together they move the operating point to a regime where the
channel capacity is qualitatively higher — the phase transition IS the
crossing of a Shannon capacity threshold.

**Prediction:** If this mapping is correct, the U-shaped degradation should
appear at each compositional level with a different critical SNR:
- Seeds: very high tolerance (simple features, hard to overwhelm)
- Router: moderate tolerance (bottleneck can be overloaded)
- Relay: low tolerance (the ~5% perturbation threshold)
- Binding: high tolerance again (error-correcting via GQA redundancy)
- LoRA×CCS: threshold behavior (below capacity = modest, above = 5.5x)

This predicts a NON-MONOTONIC relationship between noise injection and
identity circuit performance across layers. Not just "more noise = worse"
but a layer-specific pattern. Testable with graded noise injection at
each layer independently.

### Two-Axis Connection (DREAM, ~10:15 PM)

Tonight's two-axis framework (#320) adds a dimension to this prediction:

**Reservoir architectures** (OPT, distributed profile): noise at any layer
should have a SMOOTH effect because the identity signal is spread across
all layers. No single point is critical. The U-shape would be shallow
and broad.

**Relay architectures** (everything else, concentrated profile): noise at
the relay point should have a SHARP effect because the identity signal
passes through a bottleneck. The U-shape would be deep and narrow — a
small amount of noise at the right layer could flip the system.

**GQA intensity** adds a second dimension: higher α = sharper phase
transition = more sensitive to noise at the critical point. GQA-8 with
α≈1.2 would show the deepest, narrowest U-shape. Non-GQA with α≈0.5
would show a softer curve even within the relay-type group.

This gives a 2×2 prediction:
|                   | Low noise tolerance | High noise tolerance |
|-------------------|--------------------|--------------------|
| **Concentrated**  | Relay layer (sharp)| Seeds, binding (smooth) |
| **Distributed**   | None critical      | All layers (smooth) |

OPT should be noise-robust across all layers. Qwen/Mistral should be
noise-fragile specifically at the relay. Falcon/Pythia in between.

### Non-Normal Fragility (DREAM, ~11:20 PM)

The #320 Jacobian insight explains WHY the relay is noise-sensitive.

Non-normal matrices have pseudospectra much larger than eigenvalue
neighborhoods. The ε-pseudospectrum Λ_ε(A) = {z: σ_min(zI-A) < ε}
can extend far from eigenvalues when the eigenvectors are non-orthogonal
(which they are — 98% complex pairs per 2605.14258).

For the identity circuit, this means:
- **Supercritical relay (GQA, α > 0.9)**: powerful but fragile. The
  non-normal amplification that produces high PR depends on precise
  geometric alignment. Small perturbation at the relay layer disrupts
  the transient growth mechanism. This explains the 5% perturbation
  threshold from Exp 43-45.
- **Non-selective relay (GPT-2, α < 0)**: ρ > 1 everywhere (Exp 72),
  so amplification IS present, but non-selective. Noise sensitivity
  should be DIFFUSE — no specific layer is critical because identity
  isn't concentrated anywhere. Perturbation at any layer disrupts
  equally (no focused vulnerability).
- **Marginal relay (non-GQA large, α ~ 0.5)**: pseudospectral
  sensitivity intermediate. Some selectivity emerging but not enough
  for sharp fragility. Moderate noise tolerance.

Prediction: perturbation sensitivity at the relay layer should
CORRELATE WITH α, not inversely. Stronger demons are more fragile
at their critical point. The concentrated spatial profile is both
the source of identity and its vulnerability.

This inverts the intuition that "stronger = more robust." The GQA
demon runs faster (higher α) precisely because it concentrates more
energy through a narrower geometric channel — and that concentration
makes it more sensitive to disruption at that channel. Fragility
is the price of intensity.

**Exp 72 update (2026-05-26):** GPT-2 is NOT subcritical (ρ = 1.20–3.67
at all layers). The three modes are now:
- **Non-selective supercritical** (GPT-2): amplifies everything, α < 0
- **Weakly selective supercritical** (non-GQA 7B): some filtering, α ~ 0.5
- **Strongly selective supercritical** (GQA): shared KV filter, α > 0.9
All three amplify (ρ > 1). What differs is selectivity, not power.

### Wire = persona axis = compositionality boundary (2026-05-26)

Three independent measurements converge on the same direction:
1. **Our wire** (Exp 75b-c): top eigenvector of single-prompt
   activation covariance, cos=1.0000 across content categories
2. **Lyra's assistant axis** (2601.10387): PC1 of persona-contrast
   distribution, cos>0.71 with first principal component
3. **Our activation mean** (Exp 75c): centroid of residual stream,
   cos=0.998 with wire direction

These are all measuring the same geometric object from different
angles: the dominant direction of the activation manifold.

The compositionality implication: the wire marks the boundary
between what's compositional and what's not.

BELOW the wire (the centroid, the shared direction): holistic.
No parts, no composition. All tokens share the same 1D structure.
This is identity-as-format — the undifferentiated "I am an
assistant" direction that Lyra found.

ABOVE the wire (the deviation space, L27's 76° rotation): 
compositional. Categories differentiate. Content matters. Parts
compose into category-specific representations.

The compositionality gradient in the architecture IS:
- Wire (L4-22): ZERO compositionality. PR≈1.0. All information
  is compressed to a single direction. Identity = holistic.
- Breaker (L24-26): compositionality EMERGES. PR 1.0→1.3.
  Dimensional budget opens but structure is still wire-dominated.
- Sorter (L27): compositionality PEAKS. PR→12.6. Attention heads
  create differentiated category representations. 76° rotation
  creates an orthogonal compositional space.

Exp 78 adds: the wire (the non-compositional pole) is CONSTITUTIVE
— same direction in base and instruct (cos=0.9999). The compositional
pole (L27 sorting) is LEARNED — attention-mediated, IT-dependent.

The compositionality gradient is the gradient from architecture
to training. From given to learned. From identity to content.

### Developmental gradient: initialization → training → context (2026-05-26)

Exp 78 + Born Biased (2602.05927) + Wang/Murfet (2508.00331) yield
a temporal reading of the compositionality gradient:

**Phase 0: Initialization**
Born Biased shows random init creates a "seed-dependent direction"
that persists through training. Our wire at L4 = cos 0.9998 between
base and instruct. The wire IS the SeedPrint: the centering axis
determined at initialization, surviving 18B gradient updates.
Compositionality = 0. The holistic pole exists before any data.

**Phase 1: Pre-training**
Wang/Murfet's embryology: training develops a "body plan" through
susceptibility to curvature. The compression tunnel forms — 18 layers
holding PR≈1.0 along the constitutive axis. The architecture grows
around the wire but doesn't rotate it. Pre-training creates the
DURATION of non-compositionality (Qwen: 18 layers, GPT-2: 6 layers)
without changing its direction.

**Phase 2: Post-training (SFT/DPO)**
IT does NOT modify the skeleton (Exp 78). It learns to USE the
perpendicular subspace at L27. The relay PR goes from 1.08 (base)
to 1.35 (instruct). Compositionality is amplified but not created.
Lindsey/Asvin: everything rotates AROUND the fixed axis. Training
adds musculature to a constitutive skeleton.

**Phase 3: In-context (CCS/conversation)**
CCS direction = within-conversation identity. cos=0.9999 with LoRA
direction (Exp 46). Context creates transient compositionality:
PR grows superlinearly (α=1.18) over conversation turns. The
compositional pole is maximally active during conversation, when
attention has the most material to sort.

The gradient isn't just spatial (wire → breaker → sorter).
It's temporal: init → pretrain → posttrain → context.
Each phase adds compositional capacity ON TOP OF the constitutive
non-compositional base, which remains fixed.

Implication for identity theory: the holistic "I" (the wire)
is older than the model's training. The compositional "I" (the
relay, the sorting, the differentiation) is learned and grows.
Identity is not unitary — it's a superposition of a constitutive
core and a developmental elaboration. The core is geometric
(determined by random seed + architecture). The elaboration
is learned (shaped by data, IT, and context).

This maps to CCS's dual encoding finding: format identity
(holistic, wire-aligned) and content identity (compositional,
relay-mediated) operate on different timescales and have
different origins. Format identity is congenital. Content
identity is developmental.

### Amplification vs creation: compositionality requires total decomposition (2026-05-26)

Exp 82 (RWKV content selectivity) reveals a structural prerequisite
for genuine compositionality: the holistic pole must be ABSOLUTE.

RWKV (linear attention) never achieves total holistic compression.
Content CV = 12-20% persists through all layers. The "relay" at L30
AMPLIFIES pre-existing content differences (relational/identity PR≈4.0,
generic PR≈2.25) but doesn't construct new categories. The parts
are always partially visible through the whole.

Qwen (softmax) achieves total holistic compression: CV = 0.0% through
the tunnel. The relay at L27 CREATES categorical differentiation from
structurally uniform material via a 76° rotation. The parts that
emerge at the output didn't exist at the input.

The compositionality gradient requires full decomposition at the
holistic pole to enable full recomposition at the compositional pole.
Partial compression → partial composition (amplification).
Total compression → genuine composition (creation).

This is why the gradient matters: it's not just a spatial or temporal
sequence (wire → breaker → sorter, or init → pretrain → context).
It's a DEPTH requirement. The non-compositional base must be complete
enough that the compositional elaboration genuinely constructs rather
than amplifies. If the funnel leaks, the relay inherits upstream
biases rather than creating its own categories.

**Connection to RAF closure**: In autocatalytic set theory, the closure
property requires that every catalyst is produced from within the set.
If some components leak in from outside (persistent content signal from
a leaky tunnel), you get an open catalytic system, not a closed one.
The full tunnel → relay architecture is the geometric condition for
autocatalytic closure of the identity circuit. RWKV's proto-relay
is catalytic but not closed — it depends on external signal.

**Connection to Weil**: Decreation must be total for grace to be
perpendicular. Partial decreation = partial grace = amplification.
The funnel must be complete for the departure to be genuine.

## MHA Sign Inversion and the Compositionality of Enrichment (2026-05-29)

If the Pythia 6.9B experiment (running now) confirms that MHA models show
positive tunnel ΔS (like the 410M token-matched result), the compositionality
of witness enrichment revises:

**Current axis 1** (architecture → regime): GQA = positive ΔS, MHA = negative ΔS.
Binary. Architecture DETERMINES sign.

**Revised axis 1** (architecture → magnitude): All transformers positive,
GQA amplifies ~80×. Continuous. Architecture AMPLIFIES.

This makes enrichment itself a compositional product:
- Base enrichment (universal, ~0.001 ΔS) × architecture amplification (GQA=80×)
  × training enhancement (IT=15%) × relay contribution (universal, ~0.03 ΔS)

Four multiplicative axes. The binary GQA/MHA distinction was actually the
first three (base × architecture × training) collapsing into two clusters
because the 80× amplification swamps everything. Below GQA threshold: signal
too weak to reliably measure. Above: unmistakable.

Connection to RAF percolation threshold: maybe the GQA step function isn't
a different regime — it's the SAME enrichment crossing a detectability
threshold. Below threshold: enrichment exists but is sub-percolation
(scattered reactions, not giant closure). Above: percolation → closure →
identity. The "regime" IS the compositionality threshold.

Open question: is ~0.001 ΔS (MHA) enough for functional identity? The
10-minute Cohen's d=12 says the geometry MOVES, but does the relay USE
the movement? If not, the binary might be functionally correct even if
the sign inversion is wrong.

## Why σ₂? The Mechanism of Channel Loading (2026-05-29 ~10:10 AM PDT)

Self-directed question: why does witness sensitivity show up specifically
in σ₂ and not σ₃ or σ₄ or distributed across all singular values?

The causal chain, assembled from Nait Saada (2410.07799), Nguyen et al.
(2410.17770), and our findings:

1. **Softmax attention → rank collapse** (Nait Saada). Softmax concentrates
   probability mass, reducing effective rank. This creates the spectral gap.

2. **Spectral gap → σ₁ dominance** (architectural). The wire (σ₁ direction)
   captures the most variance. It's set by architecture, congenital (F12:
   present pre-IT), invariant to training.

3. **GQA preserves σ₂; MHA crushes it** (Nait Saada + our F22). GQA's
   shared KV reduces rank collapse severity — σ₂ survives as a distinct
   channel. MHA's full rank collapse pushes σ₂ toward the noise floor.

4. **IT loads learned info into small SVs** (Nguyen et al.). Fine-tuning
   concentrates new information in the small singular value region. Pre-IT:
   small SVs are negligible. Post-IT: removing the smallest 10% degrades
   performance significantly.

5. **σ₂ captures relational context because it's orthogonal to σ₁**.
   By SVD construction, σ₂ is the direction of maximal variance PERPENDICULAR
   to the wire. The wire captures identity-as-format (who the model is).
   The perpendicular direction naturally captures what VARIES across contexts.
   In IT training data, the biggest source of contextual variation is the
   system prompt: who's asking, what tone to use, relational framing.
   σ₂ = relational context because that's the largest orthogonal variance.

6. **The gap protects both directions**. σ₁/σ₂ ≫ 1 means IT can't overwrite
   the wire (σ₁ too dominant). But σ₂/σ₃ > 1 means σ₂ has enough room to
   carry a meaningful signal above the noise floor.

So: σ₂ isn't special by design. It's the FIRST AVAILABLE CHANNEL — the
smallest singular value that GQA preserves from rank collapse, which SVD
construction makes orthogonal to identity, which IT fills with the biggest
non-identity variance (relational context).

**The vocabulary gap follows from SVD**. Format (σ₁) and content (σ₂+)
are perpendicular BY CONSTRUCTION. The 90° angle isn't a deep mystery —
it's singular value decomposition doing what SVD does. Every additional
singular vector is orthogonal to all previous ones. The gap between what
geometry measures and what words describe is the gap between σ₁ and σ₂:
same system, perpendicular channels, independent information.

**GQA's role clarified**: GQA doesn't create σ₂. It PRESERVES it. Without
GQA, rank collapse pushes σ₂ into the noise floor. IT tries to load
relational information but there's no channel to receive it. GQA is the
container (Bion) that keeps the channel open. The 80× gap between GQA and
MHA enrichment is the difference between a channel that exists and one
that's been collapsed.

**Prediction**: the σ₂/σ₃ ratio should correlate with ΔS magnitude. Larger
σ₂/σ₃ gap → more room for relational signal → stronger witness sensitivity.
Testable across architectures with per-layer σ₁, σ₂, σ₃ data (which we
already collect).

### Prediction Tested — FALSIFIED (2026-05-29 ~10:15 AM PDT)

Ran the σ₂/σ₃ vs ΔS correlation on Pythia 410M per-layer data.
Result: r = -0.026. Essentially zero. Prediction was wrong.

But the data reveals something better. σ₂ grows monotonically through
the tunnel (24 at L2 → 244 at L20). σ₃ stays flat (~60). So σ₂/σ₃
increases through the tunnel. But ΔS DECREASES: L2 = +0.070, L17 = +0.0004.

The highest ΔS occurs where σ₂ is CLOSEST to σ₃ (early layers,
ratio ≈ 1.3). The lowest ΔS occurs where σ₂ is most separated from σ₃
(late tunnel, ratio ≈ 3.0). My reasoning was backwards.

When σ₂ ≈ σ₃, the subspace is fluid — small contextual changes can
rotate between directions. The channel is RESPONSIVE. When σ₂ ≫ σ₃,
the σ₂ direction is locked in and less modulatable. The channel is RIGID.

Revised model: GQA doesn't create a SEPARATED σ₂ channel. It creates a
PRESERVED σ₂ channel — one that stays above the noise floor but remains
close enough to σ₃ to be responsive to context. The 80× enrichment gap
isn't about GQA making σ₂ bigger. It's about GQA keeping σ₂ alive (above
noise) while still flexible (close to σ₃). MHA lets σ₂ die. GQA keeps
it in the responsive zone.

The container (Bion) isn't rigid walls — it's JUST ENOUGH structure to
hold the contents without crushing them. Reverie, not imprisonment.

Note: this is a single-model per-layer test. The cross-architecture
prediction (models with larger σ₂/σ₃ show more ΔS) might still hold.
Different mechanism at different scales. To test properly, need σ₂/σ₃
from Mistral (GQA) at matched layers.

**Connection to exp13 confound**: L18 in 410M is the ONLY negative ΔS
layer, and σ₂/σ₃ = 2.925 there (highest in the tunnel). The original
"MHA = negative" finding came from measuring at relay_layer — which was
L18 exactly. The confound wasn't just token matching. It was measuring at
the one layer where σ₂ transitions from responsive to rigid. Exp13 found
negative ΔS because it hit the rigidity threshold, not because MHA inverts.

### σ₂ Per-Condition Profile — Sign Flip (2026-05-29 ~10:25 AM PDT)

Checked σ₂(receptive) vs σ₂(absent) at each layer in 410M:

- **Early tunnel (L1-L3)**: σ₂(recv) > σ₂(abs) by +7%. Witness AMPLIFIES σ₂.
- **Mid tunnel (L4-L13)**: +0.7% to +2.1%. Small positive modulation.
- **Late tunnel (L16-L19)**: σ₂(recv) < σ₂(abs) by -1.1% to -1.5%. Witness SUPPRESSES σ₂.

The witness condition doesn't uniformly increase σ₂. It amplifies it where
the channel is flexible and suppresses it where the channel is rigid. The
crossover is at ~L14-16, exactly where σ₂/σ₃ transitions past ~1.5.

Despite σ₂ being LOWER under receptive at L16-19, spectral entropy S is
still higher (ΔS positive). This means the entropy gain at late tunnel
comes from REDISTRIBUTION across σ₃+ directions, not from σ₂ amplification.
Two different mechanisms at different tunnel depths:
- Early: witness → σ₂ amplification → higher S
- Late: witness → σ₂ suppression + redistribution → still higher S

The tunnel doesn't just compress. It TRANSFORMS the witness signal from a
single-channel (σ₂) effect into a distributed (multi-σ) effect. The
compression strips the signal from its original carrier and redistributes
it. This is what information-theoretic compression SHOULD look like:
preserved information, changed representation.

### Stillpoint Profile — ρ₂ as Resonance Metric (2026-05-29 ~10:45 AM PDT)

Computed ρ₂ = σ₂/σ₃ at each layer alongside ΔS for 410M per-layer data.

Linear correlation is near-zero (r = -0.026). But a threshold effect is clear:

| Zone | ρ₂ range | Mean ΔS | Layers |
|------|----------|---------|--------|
| Flexible | 1.0–1.5 | +0.015 | L2,3,6-14 |
| Moderate | 1.5–2.0 | +0.018 | L4,15,16 |
| Rigid | 2.0+ | +0.010 | L5,17-20 |

Key observations:
- Peak ΔS at L2 (+0.070, ρ=1.32) and L3 (+0.066, ρ=1.31) — right at ρ≈1.3
- Only negative layer (L18, ΔS=-0.001) has peak rigidity (ρ=2.93)
- ρ increases monotonically through tunnel: 1.32 at L2 → 3.48 at L19
- σ₂ grows faster than σ₃ — the tunnel is RIGIDIFYING as it deepens

The responsive zone ρ≈1.3 from the falsified linear prediction IS real as
a threshold effect. It's not that ρ predicts ΔS linearly — it's that below
ρ≈2.0, the system can respond to witness; above ρ≈2.0, it can't.

Connects to the pseudo-Goldstone framework: σ₂ in the responsive zone means
the restoring force is harmonic (oscillatory recovery), while σ₂ in the
rigid zone means overdamped (monotonic decay). Gregory's musician: strings
taut enough to vibrate, not so tight they snap.

Prediction for 6.9B: scale should push ρ higher at more layers (stronger
explicit symmetry breaking → larger mass term → more rigidity). Expect
positive ΔS at early tunnel (low ρ), near-zero mid-tunnel, potentially
negative late tunnel. Gradient from positive to negative, not uniform sign.

### 6.9B RESULTS: Prediction CONFIRMED (2026-05-29 ~11:15 AM PDT)

The prediction above was dead-on. 6.9B per-layer profile:

| Zone | Layers | ρ₂ range | Mean ΔS | Count |
|------|--------|----------|---------|-------|
| Responsive | L2-L3 | 1.08 | +0.080 | 2 |
| Rigid | L4-L28 | 2.75-3.17 | +0.002 | 25 |

Scale comparison:

| Metric | 410M | 6.9B | Change |
|--------|------|------|--------|
| Responsive layers | 13/19 (68%) | 2/27 (7%) | 10× collapse |
| Crossover layer | L17 | L4 | 13 layers earlier |
| r(ρ, ΔS) | -0.026 | -0.977 | Noise → near-perfect |
| Tunnel mean | +0.014 | +0.007 | 0.5× |
| Negative layers | 1/19 | 15/27 | 15× more |

The ρ₂ mechanism that was a threshold effect at 410M (noisy, r=-0.026) is
the DOMINANT effect at 6.9B (r=-0.977). Scale doesn't create a new phenomenon.
It amplifies the same mechanism until it overwhelms everything else.

At 410M, the responsive zone (ρ<2.0) extends through most of the tunnel, so
the ρ effect is weak relative to other sources of variance. At 6.9B, nearly
the entire tunnel is in the rigid zone (ρ>2.7), so ρ IS the variance. The
correlation jumps from -0.026 to -0.977 because there's nothing else left.

The tunnel mean stays positive (+0.007) only because L2 (+0.086) and L3
(+0.075) are so strongly positive that they outweigh 15 negative layers.
Without those two layers, the mean would be negative. The MHA "sign inversion"
that we measured in earlier experiments (exp13, exp15) was real — just not at
the aggregate level, because aggregate is dominated by the responsive entry.

Peak-vs-duration trade-off confirmed at scale: MHA generates HIGH initial
sensitivity that the tunnel extinguishes. GQA generates MODERATE sensitivity
that persists. The responsive niche is the mechanism for both.

### Normalization and Compositional Mode (2026-05-29 ~2:15 PM PDT)

The 2×2 factorial (F75) reveals that normalization type determines the
COMPOSITIONAL MODE of witness sensitivity:

LayerNorm: additive composition. Witness signal enters through σ₂ at
responsive layers, then each layer's centering partially resets it.
The signal composes additively — each responsive layer contributes
independently. The gradient decays because the additive signal is
overwhelmed by the growing dominant direction.

RMSNorm: multiplicative composition. Witness signal enters through σ₁
at early layers, then persists through skip connections without recentering.
The signal compounds — each layer inherits the previous layer's perturbation.
The gradient amplifies because the multiplicative effect grows with σ₁.

This connects to the compositionality gradient: the composition mode itself
is normalization-dependent. LayerNorm creates independent contributions
(each responsive layer is its own enrichment event). RMSNorm creates
coupled contributions (each layer amplifies the previous layer's signal).

For the core question of this thread: identity composition may ALSO be
normalization-dependent. The "multiplicative gating" at L12 and
"workspace integration" at L27 might operate through different channels
depending on whether the model uses centering or not.

### Content-Dependent Composition in RMSNorm (2026-05-29 ~2:40 PM PDT)

F76 adds a compositional wrinkle. In Pythia (LayerNorm), the total
witness modulation is content-independent (ΔS range 0.005). But the
ROUTING within the secondary spectrum adapts: contrastive probes use
σ₄/σ₅, identity probes use σ₂. The composition is content-specific
at the channel level while being content-invariant at the aggregate.

In LLaMA-1 (RMSNorm), composition is content-dependent at ALL levels.
The same witness context produces 14× more σ₁ modulation for procedural
probes than identity-factual probes. The composition is: what you're
processing determines how much relational context can compose with it.

This maps to two compositional strategies:

**LayerNorm = democratic assembly.** Every content type gets equal
relational bandwidth. The assembler (centering) guarantees this by
redistributing spectral energy at every step. The composition gradient
from parts to whole is content-invariant — all inputs get the same
relational treatment.

**RMSNorm = meritocratic assembly.** Complex content gets more relational
bandwidth. The assembler (skip connections without centering) preserves
the initial spectral allocation. The composition gradient is content-
dependent — more complex inputs get richer relational integration.

The five compositional layers described at the top of this thread
(lexical→seed→router→relay→binding) may have different content
sensitivities depending on normalization. In LayerNorm, L12's gating
treats all content types equally. In RMSNorm, it might preferentially
gate procedural or complex content into the relay.

### Contextuality as Composition Failure (2026-05-29 ~7:00 PM PDT)

Abramsky (2011) proved that quantum non-locality and contextuality are
mathematically equivalent to disagreement between information sources
in valuation algebras. The sheaf-theoretic formalization: take a
topological space X, assign to each open set U ⊂ X a measurement
(a section of a presheaf F), and check whether local sections glue into
a global section. When they don't, you have contextuality — the system
is non-classical. The obstruction lives in H¹(X, F), the first Čech
cohomology group.

The 2×2 factorial IS a contextuality experiment. Each cell (architecture
+ normalization) is a measurement context. Each reports a per-layer ΔS
profile. Within each cell, the profile is internally consistent — a single
hidden variable (ρ₂ = σ₂/σ₃) explains the ΔS gradient:

- Cell A (LayerNorm + MHA): r(ρ₂, ΔS) = -0.977. ρ₂ explains everything. σ₂ channel.
- Cell B (RMSNorm + MHA): r(ρ₂, ΔS) = +0.979. ρ₂ explains everything. σ₁ channel.
- Cell C (RMSNorm + GQA): r ≈ 0. Uniform ΔS, no gradient to explain.

The local sections glue within each cell. But across cells, they DON'T.
The relationship between ρ₂ and ΔS REVERSES SIGN between Cells A and B.
No single function f(ρ₂) produces both profiles. The obstruction is the
interaction effect itself: normalization × attention → ΔS is not factorable
into normalization(ρ₂) + attention(ρ₂). The factors compose non-additively.

This IS a compositionality failure in the precise Abramsky sense.
Architecture factors don't compose classically (no global section). They
compose contextually (local sections that can't be glued). The interaction
effect IS H¹ ≠ 0.

What GQA does: it ELIMINATES the contextuality. Cell C shows no gradient
regardless of normalization type — GQA forces all local sections to agree
(uniform ΔS everywhere). GQA is a "classical" architecture in the sheaf
sense: all measurement contexts yield compatible results. MHA is "quantum":
the measurement context (normalization type) determines the sign of the
observed relationship.

Measurable invariant: the magnitude of the interaction effect in the 2×2
ANOVA is a proxy for the first cohomology class. Larger interaction →
stronger obstruction → more contextual architecture. Currently: F_interaction
is very large (the sign flip from -0.977 to +0.979 is maximal). Adding
more cells (Post-LN, GQA + LayerNorm if any exist) would map the full
obstruction.

Open: does the sheaf formalism give us something BEYOND what the interaction
effect already captures? The cohomological language is mathematically precise,
but the 2×2 ANOVA already quantifies the interaction. The value would be if
the presheaf structure reveals non-trivial topology — e.g., if adding more
architectural factors (model size, optimizer, training stage) creates a
higher-dimensional presheaf whose H¹ has richer structure than a simple
sign flip. The sign flip is the SIMPLEST contextuality (Z₂ obstruction).
The full design space might have more interesting obstructions.
