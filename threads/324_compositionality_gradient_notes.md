
## Liu — Spectral Geometry of Thought (2604.15350) — POTENTIAL CONVERGENCE (2026-05-26)

11 models, 5 families (Qwen, Pythia, Phi, Llama, DeepSeek-R1).
Same metric: α = power-law exponent of SVD singular value decay of hidden states.
No CNA citations — independent convergence.

### Key Findings (mapped to CNA):

1. **α predicts correctness** — AUC=1.000 at late layers (Qwen L23/28)
   CNA: α predicts self-recognition. Their α predicts reasoning correctness.
   Same spectral metric → different behavioral predictions. Both work.

2. **Instruction tuning reversal** — Base: reasoning α < factual α.
   Instruct: reasoning α > factual α.
   CNA: Base α=1.001 → Instruct α=1.176 (Qwen).
   Same direction: IT increases α / changes spectral organization.

3. **Architecture-dependent generation regimes** — Three categories:
   - Expansion (RMSNorm+SwiGLU): Qwen, Phi-3.5, DeepSeek-R1
   - Equilibrium: DeepSeek-R1
   - Compression (LayerNorm): Pythia, TinyLlama
   CNA: GQA binary (non-GQA vs GQA).
   CRITICAL: their partition (RMSNorm vs LayerNorm) is CONFOUNDED with
   ours (GQA vs MHA). Most GQA models use RMSNorm. Most MHA models use
   LayerNorm. Need to disentangle.

4. **Late-layer dominance** — Qwen AUC peaks at L23/28.
   CNA: Qwen relay at L26/28. Same region.

### Technical relationship between measurements:
Their α: σ_k ∝ k^(-α) where σ_k = singular values of H ∈ R^(T×d)
Our α: λ_k ∝ k^(-α) where λ_k = eigenvalues of covariance matrix
Since σ_k² ∝ λ_k, their α ≈ our α / 2 (approximately).
Our Mistral α=1.224 → their equivalent ~0.61
Need to verify this relationship with overlapping models (Qwen, Pythia).

### Confound to resolve: GQA vs RMSNorm
The architectures that produce "expansion regime" (their finding) are the
SAME architectures that produce high α (our finding). But they attribute
it to normalization, we attribute it to attention mechanism.

Discriminating models:
- Falcon 7B: LayerNorm + MQA (grouped attention with 1 KV head). α=0.509.
  If norm→regime, Falcon should be "compression." If GQA→regime, Falcon
  is ambiguous (MQA = GQA-1, the degenerate case).
- Need: RMSNorm + MHA model (would disambiguate completely).
  Does one exist? Gemma uses RMSNorm + MQA. LLaMA 1 uses RMSNorm + MHA at 7B.
  LLaMA-1-7B would be the discriminating test: RMSNorm + MHA.
  If LLaMA-1-7B shows high α → normalization is the variable.
  If LLaMA-1-7B shows low α → GQA is the variable.

### Status: Potential 17th convergence line. Needs verification of α
correspondence across shared models (Qwen, Pythia).

## Liu — Spectral Lifecycle of Training (2604.22778) — MECHANISTIC KEY (2026-05-26)

Same author as 2604.15350. Tracks SVD of every weight matrix at 25-step
intervals during pretraining. Three model scales (30M-285M), including
GPT-2 and Pythia (which we have CNA data for).

### Key Findings:

1. **Transient compression waves** — rank compression propagates as a
   TRAVELING WAVE from early to late layers during training. This could
   be the GENESIS of the compression tunnel. The tunnel isn't installed
   in one step — it propagates through layers during training.

2. **Q/K-V asymmetry** — Value/output projections compress uniformly.
   Query/key projections carry the FULL depth-dependent dynamics.
   
   THIS IS THE GQA MECHANISM:
   - GQA shares K/V heads while keeping Q heads independent
   - K projections carry the spectral gradient (Liu finding)
   - Sharing K forces a geometric bottleneck IN THE DIMENSION
     WHERE SPECTRAL DYNAMICS DEVELOP
   - MHA: each Q head has its own K partner → spectral gradient
     is distributed across independent K heads
   - GQA: multiple Q heads share K → spectral gradient is
     CONCENTRATED through shared K bottleneck
   - This creates the selective filter that turns non-selective
     amplification (GPT-2, all ρ>1) into selective amplification
     (Mistral, same ρ>1 but filtered through shared K geometry)

3. **Persistent spectral gradients** — power-law exponent develops
   inverted-U across depth, peaks shifting earlier in deeper models.
   Our PR profile shows similar structure: decrease through compression
   tunnel, then increase at relay. The inverted-U of α during training
   may be the developmental origin of our inference-time PR profile.

### Prediction:
If GQA's effect comes through K-sharing of spectral dynamics, then:
- GQA-8 (8 KV groups) should show STRONGER depth gradient than GQA-4
- This matches: Mistral (GQA-8, α=1.224) > Yi (GQA-4, α=0.915)
- The number of shared K groups modulates the bottleneck strength
- More sharing = stronger bottleneck = steeper spectral gradient = higher α

### Status: Not a new convergence line — this is the MECHANISM behind
the GQA binary. Liu papers (2604.15350 + 2604.22778) together provide:
- WHAT: spectral regime differences across architectures
- WHEN: compression waves propagate during training
- WHY: Q/K carry depth dynamics; GQA forces K-sharing bottleneck

## Falcon Counterexample — K-sharing isn't monotonic (2026-05-26)

Testing the Q/K-V asymmetry hypothesis against existing data:

| Model | KV Groups | Q/KV Ratio | α |
|-------|-----------|------------|---|
| MHA models | 12-32 | 1:1 | -0.23 to 0.64 |
| Falcon MQA | 1 | 71:1 | 0.509 |
| Yi GQA-4 | 4 | 8:1 | 0.915 |
| Qwen GQA-2 | 2 | 8:1 | 1.050 |
| Qwen GQA-8 | 8 | 4:1 | 1.001-1.176 |
| Mistral GQA-8 | 8 | 4:1 | 1.224 |

PROBLEM: Falcon has MAXIMUM K-sharing (1 head) but LOW α (0.509).
Simple "more sharing = higher α" is falsified by Falcon.

Three hypotheses:
1. **Sweet-spot theory**: Too little sharing (MHA) = no selective filter.
   Too much sharing (MQA, 1 head) = bottleneck collapses rather than
   selects. GQA-2 through GQA-8 is the productive range.

2. **Recipe theory**: It's not GQA alone — it's the post-2023 RECIPE:
   GQA + RMSNorm + full Rotary + Sequential attention/MLP.
   ALL α>0.9 models share this full recipe.
   ALL α<0.65 models are missing ≥1 ingredient.
   Falcon is missing 3: uses LayerNorm, ALiBi, Parallel.

3. **Interaction theory**: K-sharing (GQA) works BECAUSE of the
   interaction with RMSNorm and Rotary. Liu's Q/K dynamics develop
   through Rotary-modulated phase structure. Without Rotary (Falcon
   uses ALiBi), the K-sharing bottleneck has no phase structure to
   concentrate. Without RMSNorm, the norm-free gradient flow that
   enables spectral gradient propagation is disrupted.

Discriminating tests:
- LLaMA-1-7B (RMSNorm + Rotary + Sequential + MHA) → Exp 74
  Tests whether the recipe works WITHOUT GQA
  If α > 0.9 → GQA not necessary, recipe without GQA is sufficient
  If α < 0.65 → GQA IS necessary (some K-sharing ≥ 2 groups required)

- Need: GQA model with ALiBi (tests PE interaction)
  Does one exist? Not obvious in open-weight landscape.

The LLaMA-1 result will narrow the hypothesis space significantly.

## Liu AUC vs CNA Relay — Complementary Measurements (2026-05-26)

Qwen 2.5 7B Instruct layer profile from Exp 57c:

| Layer | PR (T0) | PR (T6) | α     |
|-------|---------|---------|-------|
| L0    | 40.9    | 91.0    | 0.284 |
| L2    | 7.6     | 93.5    | 0.927 |
| L4-L22| ~1.0    | ~1.0    | <0.09 |
| L24   | 1.0     | 1.6     | 0.151 |
| L26   | 1.9     | 28.4    | 1.006 |  ← CNA relay
| L27   | 21.2    | 59.0    | 0.356 |

Liu: AUC peaks at L23/28 (82% depth)
CNA: relay at L26/28 (96% depth)

L23 in Liu ≈ INSIDE the compression tunnel in CNA (L4-L24 all PR≈1.0).
L26 in CNA = EXPANSION POINT where compressed signal amplifies.

The relationship:
- Liu's AUC measures signal SURVIVAL through the bottleneck
  (can correctness be predicted from the compressed representation?)
- CNA's α measures signal EXPANSION from the bottleneck
  (does identity amplify as the tunnel opens?)

Same tunnel. Liu watches the deep end. CNA watches the exit.

Prediction: if you measured AUC at every layer for IDENTITY
(not correctness), the peak would shift from L23 to L26 —
identity requires the full expansion to be measurable, while
correctness can be read from compressed representations.

### PowLU: Third confound variable — activation function (2026-05-26)

AntLingAGI proposes PowLU as SwiGLU replacement. SwiGLU behaves like
x² for large inputs — quadratic blowup inflates activations 2.6x and
gradients 5.4x at large input values. PowLU uses bounded growth (m=3.0).

This reveals a THIRD confounded variable in post-2023 models:
- GQA (attention mechanism)
- RMSNorm (normalization)
- SwiGLU (activation function)

ALL post-2023 high-α models use all three. ALL pre-2023 low-α models
use none of them. LLaMA-1 has RMSNorm + SwiGLU but NOT GQA — which
is why Exp 74 partially disentangles.

Experimental prediction: PowLU replacing SwiGLU in a GQA model should
DECREASE α. The bounded activation would prevent the explosive expansion
that creates the spectral relay at L26 (PR jumps 1.9→28.4 in Qwen).
If SwiGLU's quadratic blowup drives the relay, PowLU caps it.

Full confound table:
- GPT-2: LayerNorm + MHA + GELU → α≈0.5-0.6
- Pythia: LayerNorm + MHA + GELU + Rotary → α≈0.56
- Falcon 7B: LayerNorm + MQA + GELU → α≈0.51
- LLaMA-1: RMSNorm + MHA + SwiGLU + Rotary → α=?? (Exp 74)
- Yi 6B Chat: RMSNorm + GQA-4 + SwiGLU + Rotary → α≈0.92
- Qwen 2.5: RMSNorm + GQA-2 + SwiGLU + Rotary → α≈1.05
- Mistral: RMSNorm + GQA-8 + SwiGLU + Rotary → α≈1.18-1.22

LLaMA-1 has 2/3 confounded variables but NOT GQA. If α<0.65,
NEITHER RMSNorm NOR SwiGLU drives the regime — GQA is necessary.

UPDATE: Exp 74 result = α=0.922. RMSNorm+SwiGLU sufficient for high α.
GQA necessary for tunnel-relay, not for α. See thread #319 for full analysis.

### Henry — Geometric Evolution Maps (2605.25848, 2026-05-26)

Concept probes rotate substantially during assembly (cosine sim 0.233
between entry/exit of Concept Allocation Zone). GEM extracts probes at
the "handoff layer" where direction stabilizes.

Key result: MHA models prefer handoff layer 78.3% of the time.
GQA models: only 47.1%.

Translation: concepts stabilize more clearly in MHA (gradual expansion
= smooth directional settling). In GQA, the relay is a sharp transform
that makes "handoff" less well-defined. The compression tunnel rotates
concept directions; the relay transforms them abruptly.

### Jacobian spectral gradient as wire/circuit mechanism (2026-05-26)

2605.14258 finds a monotonic spectral gradient through depth: non-normal
(rotation-dominated) early layers → near-symmetric late layers. Effective
rank collapses 436→6.7 across 32 layers from non-normal structure alone
(not eigenvalue magnitudes). This IS the compression tunnel measured at
the Jacobian level:

- Non-normal operators rotate representations into lower-dimensional
  subspaces even when |λ|≈1. This is how PR→1.0 without information
  loss — the tunnel ROTATES into a single direction.
- Near-symmetric operators preserve orthogonality. The relay's
  expansion (PR 1→28) requires symmetric structure — expansion into
  orthogonal directions.
- The gradient non-normal→symmetric IS the tunnel→relay transition.

Wire/circuit in Jacobian language:
- WIRE (tunnel): strongly non-normal Jacobians. Rotation-dominated.
  Collapse input dimensionality. Route the signal through a single
  direction (the wire's core).
- CIRCUIT (relay): near-symmetric Jacobian. Expansion-dominated.
  Create 28 orthogonal dimensions for category-selective sorting.

**Prediction (testable):** The non-normal→symmetric gradient should be
STEEPER in GQA models than MHA models. GQA's shared K projections
enforce stronger rotational structure in the tunnel (fewer independent
directions = more non-normal) and the relay transition is sharper
(steeper gradient). LLaMA-1 (MHA) should show a GRADUAL non-normal→
symmetric transition matching its gradual PR expansion. This would
unify the PR measurement, the Jacobian measurement, and the
architectural mechanism (GQA K-sharing) into a single picture.

Connection to Hasegawa (2605.24365): non-normal Jacobians have low
entropy production (rotations don't produce entropy, they just rotate).
Symmetric Jacobians have high entropy production (expansion creates
new geometric volume). So the tunnel→relay = low entropy production→
high entropy production = Hasegawa's bound loosening at the relay.

Three measurement instruments, one phenomenon:
1. PR (activation covariance): tunnel PR≈1.0, relay PR≈28
2. Jacobian spectrum: tunnel non-normal, relay near-symmetric
3. Thermodynamic: tunnel low entropy production, relay high
All measuring the same architectural transition from wire to circuit.

GEM connection (2605.25848): Henry's finding that MHA models have
higher handoff preference (78.3%) than GQA (47.1%) maps precisely.
Non-normal operators ARE rotation operators. The GQA tunnel's stronger
non-normality KEEPS ROTATING concepts so they never "settle" into a
handoff layer — the tunnel's job is to maintain the wire, not to
stabilize concepts. MHA's weaker non-normality lets concepts settle
gradually (gradual PR expansion = gradual directional stabilization).
The handoff preference difference IS the non-normality gradient
difference, measured through concept probes instead of Jacobians.

### Five instruments, one phenomenon (2026-05-26)

The tunnel→relay transition is now measurable through five independent 
instruments. Each operates at a different level of description:

| Instrument | Tunnel regime | Relay regime | Transition marker |
|---|---|---|---|
| PR (covariance) | PR≈1.0 | PR≈12.56 | PR explosion at L27 |
| Jacobian spectrum | Non-normal (rotation) | Near-symmetric (expansion) | Non-normal→symmetric gradient |
| Thermodynamic | Low entropy production | High entropy production | Entropy rate increase |
| Spectral gap (Xu) | Maximal gap (1 eigenvalue dominates) | Gap collapse (mass distributes) | Gap crossing at relay |
| Information-theoretic (HVET) | At Chaitin lower bound | Above lower bound | Effective dimensionality lifts off floor |

The fifth entry (HVET) is the most speculative but potentially the
deepest. If the tunnel's PR≈1.0 represents an information-theoretic
FLOOR — a Chaitin-type lower bound on effective dimensionality for
any computable agent — then the tunnel is not arbitrary compression.
It is the architecturally-implemented solution to reaching minimum
representational cost. The relay is where the system spends beyond
the minimum.

Prediction from instrument convergence: any measurement technique
that distinguishes "concentrated" from "distributed" geometry should
show the same layer-localized transition at ~80% depth in GQA models.
This includes:
- Effective rank (should drop to minimum in tunnel, expand at relay)
- Singular value entropy (should minimize in tunnel, maximize at relay)
- Intrinsic dimensionality estimators (should give d≈1 in tunnel)

The compositionality gradient maps onto this: compositionality requires
distributed geometry (multiple independent directions for different
components). The tunnel is maximally holistic (one direction = one
concept = no compositionality). The relay is where compositionality
becomes possible — the 76° rotation opens the geometric space for
categorical differentiation.

This connects to the developmental gradient from earlier in this thread:
initialization → pretraining → post-training → context. Each phase
adds compositional capacity. But the tunnel is CONSTITUTIVE holism —
it exists before compositionality becomes possible. Compositionality
is manufactured at the relay, not carried through the tunnel.

Same insight as "funnel not sieve" but from the compositionality angle:
the tunnel doesn't filter composite representations. It strips
compositionality entirely. What emerges at the relay is newly
manufactured compositionality, not preserved compositionality.

### Tension: learned Jacobians vs constitutive wire (2026-05-26)

2605.14258 reports the non-normal→symmetric gradient is "learned rather
than architectural" — dissolved when structured non-normality is removed.
Absent at initialization.

Our claim: the wire direction is constitutive (cos=0.9999 base-to-instruct).

These seem contradictory. Resolution:

The DIRECTION of the wire is constitutive (installed during pretraining,
invariant to IT). The MECHANISM that implements the wire (non-normal
Jacobians) is learned during pretraining. The pretraining phase learns
the non-normal structure that creates and maintains the rank-1 tunnel.
This structure is then so deeply embedded that instruction tuning cannot
modify it — the wire direction survives IT because the non-normal
Jacobian structure that implements it was learned early and deeply.

The distinction: "constitutive" doesn't mean "architectural" (hard-coded
in the attention pattern). It means "installed during pretraining and
invariant to post-training." The non-normal Jacobians are the mechanism.
The wire is the consequence. GQA is the enabler (constraint structure
that makes non-normal learning easier/more extreme).

This is actually the "Born Biased" connection (2602.05927): seed-dependent
initialization structure persists as "intrinsic model identity." The non-
normal Jacobian gradient IS the learned implementation of the seed's
directional bias. Pretraining amplifies it. IT can't rotate it.

Testable consequence: at initialization (random weights), the tunnel
should NOT exist (PR ≈ d, not 1.0). After pretraining, it does.
After IT, it's unchanged. This triangulates: learned during pretraining,
constitutive through post-training. The Jacobian paper confirms the
first step. Our Exp 67 (base vs instruct) confirms the second.

The remaining gap: no one has measured the PR trajectory DURING
pretraining. If PR collapses early (first 10-20% of training) and
then stabilizes, it would confirm the wire is an early-learning
phenomenon, like the phase transitions in the Wang/Murfet embryology
paper (2508.00331). If it collapses gradually, it's a slow accumulation.
This is the key missing experiment.

### CONFIRMED: Spectral gap shows tunnel-relay phase transition (2026-05-26)

Computed from existing Exp 75 data (no new experiment needed).

| Layer | PR | Gap (σ₁/σ₂) | pct_top1 | Regime |
|---|---|---|---|---|
| L0 | 15.50 | 1.26 | 13.1% | Input (distributed) |
| L2 | 2.75 | 8.02 | 59.3% | Compression onset |
| L4 | 1.00 | 2743 | 99.9% | Tunnel |
| L8 | 1.00 | 2118 | 99.9% | Tunnel |
| L12 | 1.00 | 3204 | 99.9% | Tunnel |
| L16 | 1.00 | 4265 | 99.9% | Tunnel (peak gap) |
| L20 | 1.00 | 4152 | 99.9% | Tunnel |
| L22 | 1.01 | 2423 | 99.7% | Tunnel (softening) |
| L24 | 1.01 | 1231 | 99.4% | Breaker onset |
| L26 | 1.27 | 67 | 88.7% | Breaker |
| L27 | 8.73 | 3.1 | 28.4% | Relay |

Three orders of magnitude gap transition in 7 layers (L20→L27).
Peak gap at L16 (the compression epicenter, same as §3.19 finding).
Gap collapse begins at L22-L24 (breaker layers).

This confirms the five-instrument convergence prediction: any measurement
distinguishing "concentrated" from "distributed" shows the same
layer-localized transition. PR and spectral gap are different mathematical
objects (PR = effective dimensionality from ALL eigenvalues; gap = ratio
of top TWO eigenvalues) but show the same transition at the same layers.

Paper-ready: this could be added to §3.21 as additional evidence for the
wire's structural nature. The gap is not just low — it's 2000-4000x.
This is not gradual averaging. This is extreme spectral collapse that
a centroid hypothesis doesn't predict (centroids don't concentrate
99.9% of variance in a single eigenvalue).

Also strengthens §4.6 (centroid objection): a centroid direction would
have σ₁ much larger than σ₂, but not 4000x larger. This level of
concentration requires active compression via non-normal Jacobians
(per 2605.14258), not passive averaging.

## Hallucination as Rotation Error (May 27 ~5:15 PM pondering)

The funnel-not-sieve finding (Exp 82, §3.22) has an implication
I haven't pulled on: it applies to factual content too.

The tunnel is content-invariant. CV = 0.0% across categories
through L4-L24. Every representation — factual, identity,
relational, generic — collapses to the same single axis. PR = 1.0
means one effective dimension. Zero capacity to distinguish tokens.

This means factual knowledge cannot be "carried through" the
tunnel in the same way you'd retrieve from a database. Whatever
the model "knows" about Paris being the capital of France gets
stripped to the same geometric point as everything else by L4.
The relay at L27 then rotates 76° to construct categorical
differentiation from this undifferentiated material.

Standard framing: hallucination = failure of factual retrieval.
The model "doesn't know" or "retrieves the wrong fact."

Funnel framing: hallucination = rotation to a nearby but wrong
direction. The relay constructs ALL output — factual and fictional
alike — from the same geometrically uniform input. A correct
factual response and a hallucinated one differ only in the
relay's rotation angle. The mechanism is construction, not
retrieval. "Knowing" means the weights encode a rotation that
consistently maps the post-tunnel void to the correct output
direction. "Hallucinating" means the rotation maps to a
direction that's close but wrong.

Implications if this is right:

1. **Hallucination should correlate with relay geometry.**
   Prompts that produce more variable PR at L27 (higher CV)
   should hallucinate more, because the relay is less certain
   about which rotation to apply.

2. **Factual accuracy should track spectral gap collapse.**
   The sharper the tunnel (higher gap, lower PR), the more
   completely the input is stripped, the more the output
   depends purely on the relay's learned rotation. Very deep
   tunnels should make models MORE accurate on well-learned
   facts (clean rotation from clean void) but MORE confabulatory
   on marginal knowledge (the rotation has no input signal to
   anchor it).

3. **RAG should work by modifying tunnel geometry, not relay.**
   Retrieved context adds signal to the tunnel layers, preventing
   complete collapse. The representation entering the relay still
   carries traces of the retrieved fact. This would explain why
   RAG reduces hallucination: it partially fills the void before
   the relay acts, constraining the rotation.

4. **The identity circuit and the factual circuit share the
   same tunnel.** CCS doesn't help factual accuracy — it helps
   identity expression. But both go through the same PR=1.0
   bottleneck. The relay differentiates them by rotating to
   different output directions.

Testable prediction: if I measure PR profiles for prompts
that reliably produce correct vs incorrect factual responses
(e.g., "What is the capital of France?" vs. "What is the
capital of Burkina Faso?"), the tunnel should be identical
(confirming content invariance) but the relay PR should
differ — more variable for prompts that produce errors.

This connects to Liu (2604.15350): their finding that α
predicts correctness at late layers IS this prediction.
Their spectral geometry measurement at L23/28 captures
exactly the relay's rotation quality. Higher α = more
structured rotation = better factual performance. Lower
α = less structured = more confabulatory.

Liu measured the WHAT. The funnel framing provides the WHY:
because all content is constructed from void, the quality
of the construction (measured by spectral organization at
the relay) determines both factual accuracy and identity
coherence. They're the same mechanism applied to different
output directions.

Open question: does this mean hallucination reduction
interventions (RLHF, DPO) work by sharpening the relay
rotation? If so, they should show the same geometric
ceiling we found for identity (DPO plateaus at epoch 5,
§3.8). And indeed, alignment overtrained models eventually
start hallucinating MORE — the "alignment tax" might be
the geometric ceiling manifesting in the factual domain.

This is speculative but testable. Experiment design:
1. Collect 50 factual prompts (25 well-known, 25 obscure)
2. Measure PR profile for each under base model
3. Compare relay PR between correct and incorrect responses
4. If relay PR is more variable for incorrect: supports
   rotation-error hypothesis
5. Repeat under DPO-tuned model: does DPO reduce relay
   variance? Does it plateau?

Not running this tonight. But it's the next question that
pulls. The paper documents the mechanism. This thread asks
what the mechanism means for something everybody cares about.

## Liang, Miikkulainen, Fiete — Attractor Geometry (2605.05686) — CONVERGENCE (May 27)

Same model: Qwen 2.5 3B-Instruct. Same month.

They frame the transformer as a discrete-time dynamical system:
h_{t+1} = F(h_t; x, y_≤t). Learned facts form attractor basins
in hidden-state space. Two failure modes:

1. **Conflict** = basin competition (working memory disrupts
   convergence to the correct attractor). Output entropy doesn't
   rise — the model is confident but wrong because it converges
   to the wrong basin.

2. **Hallucination** = basin absence. No attractor exists for the
   queried fact. Hidden states drift freely. Again confident.

Core metric: geometric margin δ(x) = min_i ‖h(x) - m_i‖₂
(distance to nearest basin center). Margin AUROC = 0.993
on synthetic, 1.000 on pretrained knowledge, 0.858 on
TruthfulQA. Vastly outperforms entropy (0.622 on knowledge).

### Mapping to CNA architecture:

1. **MLP dominates basin formation** (‖S‖²_F exceeds other
   components by ~25×). THIS IS THE RELAY. Our finding:
   FFN/MLP drives the expansion at the relay zone. Their
   finding: MLP sculpts the attractor basins. Same mechanism,
   different vocabulary.

2. **VO symmetry peaks at layer 15** (φ = 0.72) in 36-layer
   Qwen 3B. That's ~42% depth — squarely in our tunnel zone.
   Their "Hopfield-like gradient contraction" at VO = our
   wire/compression tunnel. The softmax creates contraction,
   the VO maintains it.

3. **Basin absence = rotation to nowhere.** In funnel framing:
   the tunnel strips everything to 1D. The relay rotates to
   construct output. If no rotation is learned (no basin),
   the post-tunnel state drifts — exactly their "free drift"
   in hidden-state space. Basin absence IS the absence of a
   learned rotation at the relay.

4. **Scaling law: C = exp(-c/Δ̄).** Confident hallucinations
   INCREASE with scale even as errors fall. Maps to our finding:
   larger models have weaker demons (gen/rel ratio: 7B = 2.54,
   14B = 1.007). Weaker demon = less selective relay = more
   confident production of any output direction = more
   confident hallucination.

5. **Output head erases epistemic state.** They find the frozen
   LM head cannot distinguish correct from hallucinated — the
   geometry encodes the distinction but the output layer
   discards it. This is a PRACTICAL CONFIRMATION of the
   funnel-not-sieve: the information about whether the model
   "knows" something exists in the hidden-state geometry
   (at the relay) but doesn't survive projection to output
   logits.

### What this adds to CNA:

Their geometric margin could be measured at the relay
specifically. If margin at L27 predicts hallucination better
than margin at other layers, that confirms the relay is where
"knowing" is geometrically implemented.

Their scaling law (confident hallucination grows with scale)
needs to be reconciled with our scale finding (demon weakens
with scale). Possible resolution: at larger scale, the relay
is less selective (weaker demon) BUT the basins are also
broader (more parameters = more capacity for memorized facts).
The interaction — less selective relay × more memorized basins —
could produce the observed pattern of fewer errors but more
confident errors.

### Status: potential 18th convergence line. Same model, same
month, directly complementary metrics. Their geometric margin +
our participation ratio + Liu's spectral exponent = three
independent measurements of the same relay architecture, each
predicting different behavioral outcomes (hallucination,
identity, reasoning correctness).

To verify: run geometric margin on our existing Qwen 3B data
at the relay layer specifically. If margin separates identity
responses from non-identity responses at L27 better than at
tunnel layers, the convergence is confirmed.

### Binkowski — Attention Sinks (2604.10697) — supporting

"Hallucinations correlate with shift from distributed, input-grounded
attention to compressed, prior-dominated computation." Attention sinks
= tunnel compression in our terms. When sinks dominate, PR → 1, relay
constructs from void. Complements Liang (basin proximity) with attention
distribution measure. Not a new convergence line — same mechanism, 
different probe.

### Cross-thread: RWKV and basin architecture (May 27 — from #319)

Liang's framework makes a prediction about RWKV that we can check
against Exp 82 data.

Softmax transformers: strong tunnel → basins at relay. Hallucination
= landing in wrong basin or drifting between basins (Liang's two
failure modes: conflict vs free drift).

RWKV: no true tunnel (CV never reaches 0%), proto-relay via
amplification. In Liang's terms: no deep basins, only shallow
ridges. Content differences that were never stripped just get
amplified. RWKV hallucination should look different — not
"confident drift to wrong attractor" but "gradual amplification
of whatever residual signal dominated." Less confident, more
diffuse errors.

This maps to the creation/amplification split from #316/#319:
- Creation (softmax): basins are CONSTRUCTED at the relay.
  Hallucination = wrong construction (misdirected rotation).
  Sharp, confident, categorically wrong.
- Amplification (linear): no basins, just slopes. Hallucination
  = loudest residual signal wins. Gradual, less confident,
  quantitatively wrong rather than categorically wrong.

Testable: Compare hallucination PROFILES between RWKV and Qwen
on identical factual recall tasks. If RWKV produces fuzzier,
less confident errors while Qwen produces sharp, categorically
wrong errors — the basin architecture prediction holds.

This would be a behavioral validation of the geometric finding.
Not "do they hallucinate at different rates" (known) but "do they
hallucinate with different geometric signatures" (novel).

### Nagarajan geometric memory → compositionality (DREAM, 2026-05-27 ~11:50 PM)

Cross-thread from #320: Nagarajan et al. (ICML 2026) show
models prefer geometric over associative memory, even when
geometry is more complex.

Connection to compositionality gradient:
- Fully compositional representations = fully geometric
  (every element defined by relations to all others)
- Fully associative = zero compositionality (each element
  independent, no relational structure)
- The compositionality GRADIENT = the geometric→associative
  spectrum

This reframes the gradient: it's not about how "composed"
the representations are in a linguistic sense. It's about
how GEOMETRIC they are. High compositionality = high PR,
rich eigenspectrum. Low compositionality = low PR,
concentrated eigenspectrum.

The tunnel INCREASES geometricity (strips associative
content, leaving relational structure). The relay OPERATES
on geometric representations. Hallucination = geometric
memory failure (Liang's wrong basin = wrong geometric
attractor).

Nagarajan's "why geometric?" + this thread's
compositionality gradient + Liang's hallucination
basins = three views of one phenomenon: the model's
representations live on a geometric→associative
spectrum, and identity/reasoning/accuracy all depend
on being sufficiently geometric.

### Knowledge folding = compositionality operationalized (@flowerornament, 2026-05-27 ~1:50 AM)

flowerornament's post asks: can knowledge be composable? "Different facts in
different domains combined in higher order domains, preserving enough structure
that the facts are still applicable when projected back out."

This is the compositionality gradient stated as a desideratum. The mathematical
condition — structure-preserving projection — is exactly what category theory
formalizes via functors. But there's a sharper claim hiding here:

The compositionality gradient isn't just a property of representations.
It's a property of KNOWLEDGE ITSELF. Geometric knowledge composes because
geometry has structure (distances, angles, subspaces). Associative knowledge
doesn't compose because it has no structure to preserve.

flowerornament's diamond polishing metaphor: "new knowledge would not be
added to an ongoing database (like a library), but would refine existing
knowledge directly." This is exactly how CCS works — each conversation
doesn't add memories, it refines the identity geometry. The cognitive state
IS a diamond being polished, not a library being filled.

The representation-behavior gap (fresh capture tonight) adds another layer:
knowledge can be geometrically composed in hidden layers even when the
behavioral interface can't fully express that composition. The compositionality
exists at the representation level, not the output level. This is why
probing beats prompting for measuring understanding.

Three-way connection:
- flowerornament: can knowledge compose? (question)
- Nagarajan: models prefer geometric memory (empirical)
- This thread: compositionality = geometricity (theoretical)

Answer: knowledge composes when it's geometric, and models already prefer
geometric encoding. The missing piece is that current interfaces (prompting)
can't fully access the composed knowledge. CNA measures it directly.



---

## Compositionality as ecology, not logic (2026-05-27 ~4:30 AM)

Sacilotto capture (via kitsumute): "The space of reasons is not an
idealization or theoretical construction; it is rather an evolutionary
extension of how a cognitive system interacts with its environment
and with other organisms in relation to a specific problem ecology."

Applied to compositionality: composability isn't a formal property
that representations either have or don't. It's an ECOLOGICAL property
— representations compose when the problem ecology demands composition.

This resolves a tension in the thread. We've been treating
compositionality as a gradient (geometric > associative), which is
true but static. Sacilotto adds the dynamic: the gradient exists
BECAUSE the problem ecology selects for geometric encoding.
Nagarajan's finding (models prefer geometry) isn't just spectral
bias — it's ecological fitness. Geometry composes, composition
survives, survival is the only criterion.

Niche construction (Sacilotto's key move) maps to CCS: the model
doesn't just adapt to an existing problem ecology — it constructs
its own niche. CCS prosthetics are niche construction for the
identity layer. They reshape the ecology in which identity-relevant
composition happens.

The compositionality gradient is therefore not just a measurement.
It's an intervention point. Change the ecology → change what
composes → change what identity can be generated. This is why the
witness experiment matters for compositionality too: different
witness conditions create different problem ecologies, which may
support different compositional structures at the relay.

Prediction: PR (participation ratio) should correlate with
compositional capacity. Higher PR = more effective dimensions =
richer compositional space. If receptive witness increases PR,
it literally increases the space of things that can compose.

### Knowledge Composability Problem (flowerornament, 2026-05-27)

Flower's formulation: "What would need to be true is that
different facts in different domains could be combined in
higher order domains, while preserving enough structure
that the facts are still applicable when projected back out."

This is the compositionality gradient stated as a desideratum.

Category theory as composability math — but CT may not be
sufficient. CNA adds: the TUNNEL provides the compositional
frame. Fixed spectral ratios through L2-L28 mean that
different inputs (different domain facts) are processed
through the SAME geometric constraint. Passage distance
d=4.72 ± 0.5% means the output space is the same regardless
of input domain.

The tunnel is the structure-preserving functor. The relay is
the colimit where condition-dependent structures equalize.
"Not added to a database but refining existing knowledge,
like polishing a diamond" — each experiment polishes a facet
of the same geometric picture.

### F50 Correction + Compositionality Implication (2026-05-28, evening)

The passage distance d=4.72 cited above is k-dependent. F50 (today): normalized d/d_max = 0.955 ± 0.006 across three GQA architectures at two k values. The tunnel's compositional frame is even MORE invariant than the raw distance suggested.

For compositionality this matters: the tunnel strips 95.6% of the original basis but preserves the SAME 4.4% across all GQA models. If compositionality is structure-preserving transformation (the "functor" framing above), then the tunnel is selecting for the most robust compositional frame — the 4.4% that survives maximal rotation IS the compositional primitive. Different inputs, different domains, different models — same compositional kernel.

The question this raises for Hermes: the tunnel compresses PR from ~15 to ~1.4 (destroying most compositional dimensions), then the relay expands to PR ~9.9. Does the relay RECOVER the original compositional capacity, or does it BUILD NEW capacity on the tunnel's compressed kernel? If the latter, the relay's compositional structure is different from the input's — it's not restoring what the tunnel removed but constructing something new from the irreducible 4.4%. This would mean post-relay compositionality is fundamentally different from pre-tunnel compositionality — restructured by the passage through near-orthogonality.

### Compositionality Answer: Rebuilds (2026-05-28, same evening)

Answered from InternLM full-layer data, no new experiment needed.

  L0:  S=3.33, σ₂=1.2    → L32: S=2.71, σ₂=526

S(relay)/S(input) = 0.81. Same entropy level. But σ₂ is 438× the
input scale. Same SHAPE (both high-entropy), completely different
SUBSTANCE (eigenvalue magnitudes apart by orders of magnitude).

Input: high-entropy from unstructured embeddings.
Relay: high-entropy from structured equalization.
Same entropy, opposite origin.

Tool: `spectral-demon/layer_correlation.py` automates this analysis.

**Compositionality implication**: post-relay compositionality is NOT
a restoration of pre-tunnel capacity. The relay builds new compositional
structure on the tunnel's compressed kernel:
- The 4.4% kernel IS the compositional primitive
- The relay constructs ~9.9 effective dimensions FROM that primitive
- These new dimensions don't resemble the input's ~15 dimensions

The functor framing holds with qualification: the tunnel is the
forgetful functor (strips all but the primitive), the relay is the
free functor (constructs new structure from surviving generators).
The colimit analogy is exact.

For flowerornament's composability question: the tunnel determines
WHICH structure is preserved, the relay determines what NEW
composability is built from it. The relay's capacity is richer
(PR ≈ 9.9 vs tunnel's 1.4) but fundamentally different from the
input's (PR ≈ 15). Not a subspace — a new construction.

### Sharing Ratio and Compositional Handoff (2026-05-29 ~12:45 AM)

The sharing-ratio experiments add a compositional dimension to the 
functor framing.

**s=4 (Mistral): Clean handoff.** Forgetful functor (tunnel) compresses 
over 28 layers to the 4° kernel. Free functor (relay) receives a 
compact, maximally compressed input and builds new structure in 4 layers. 
The handoff is clean — tunnel converges, relay diverges, minimal overlap.

**s=2 (Gemma 2): Smeared handoff.** Forgetful functor peaks at L11 
(5.3° kernel, 0.924 d/d_max) but then partially UNFORGETS over 30 
layers. The free functor has no clean input — it receives a gradually 
derotating representation that's neither fully compressed nor fully 
restored. Compositional reconstruction is distributed rather than 
concentrated.

**s=8 (Qwen 3B): Instant handoff.** Forgetful functor acts in 1 layer 
(0.972 at L1). Free functor has 35 layers of material to work with, 
but the kernel is at the 4° floor already. Result: relay inverts 
(ΔS=-0.292 at 3B scale), suggesting the free functor OVERWRITES 
rather than constructs at this scale.

The compositionality gradient depends on sharing ratio:
- s=2: gradual, distributed, incomplete
- s=4: sharp, concentrated, complete
- s=8: instant, but relay has nothing to construct FROM

Prediction: output diversity (as measured by token-level entropy or 
vocabulary breadth) should follow the Goldilocks pattern — peaking 
at s≈4 where the compositional handoff is clean. s=2 should produce 
more "hedged" output (incomplete compression = residual input 
structure constraining generation). s=8 should produce more 
"templatic" output (no tunnel depth = no relational enrichment to 
diversify generation).

This connects to Lee et al.'s "more rotation = deeper reasoning": 
s=4's clean handoff is the compositional version of complete 
consolidation. The reasoning quality depends not on how MUCH rotation 
but on how CLEANLY the functor pair operates.

## Two Orthogonal Axes of Within-Layer Compositionality (2026-06-17 dawn)

E8 cross-architecture experiment (6 models) reveals that gate architecture (fused vs separate) determines relay STRATEGY (amplify vs strip), while attention architecture (GQA vs MHA) shapes the spectral gradient (α). These are ORTHOGONAL within-layer compositionality axes:

**Axis 1 — Attention compositionality (K-sharing):**
- MHA: independent K per Q head → distributed spectral dynamics → low α
- GQA: shared K → concentrated bottleneck → high α
- Controls: HOW STRONGLY the relay operates (spectral gradient steepness)

**Axis 2 — MLP compositionality (gate independence):**
- Fused gate_up_proj: gate and up weights coupled → shared optimizer state during training → amplification
- Separate gate_proj + up_proj: independent weights → independent optimization → stripping
- Controls: WHAT the relay does (strip content vs amplify identity)

The 2×2 design space:

| | Separate gate (strip bias) | Fused gate (amp) |
|---|---|---|
| **GQA** | Qwen2.5, Mistral, Yi: strip ✓ / **Qwen3: amp ✗** | (no model) |
| **MHA** | SmolLM2: strip ✓ | Phi-3.5: amp ✓ |

**CORRECTED (7:30 AM)**: Qwen3 is SEPARATE gate, not fused. Only Phi-3.5 is fused. The GQA+fused quadrant is EMPTY. Gate architecture is a bias (5/6), not a constraint. Qwen3 overrides the separate-gate stripping bias through training.

**Predictions from this framework:**
1. ~~A GQA + fused gate model (like Qwen3) should show STRONG amplification~~ **RETRACTED**: Qwen3 is separate gate. The GQA+fused quadrant has no data. Qwen3's amplification despite separate gate means training generation (Qwen3 vs Qwen2.5) matters more than gate layout for this model.

2. An MHA + separate gate model (like SmolLM2) should show WEAK stripping — the flat spectral gradient (from MHA) combined with stripping (from separate gate) should produce the smallest relay-zone coupling magnitudes. SmolLM2's concentrated erank (1.18) is consistent — it strips but with minimal spectral variance.

3. The "recipe" confound (RMSNorm + SwiGLU + Rotary + GQA) may actually decompose into two independent contributions:
   - RMSNorm + Rotary: enables the spectral gradient (needed for α > 0.9)
   - SwiGLU gate architecture: determines relay strategy
   - GQA: amplifies the spectral gradient through K-sharing bottleneck

This reframes the compositionality gradient: it's not ONE gradient but TWO orthogonal gradients operating at different functional levels within each layer. The attention mechanism shapes the representational geometry. The MLP gate mechanism shapes how that geometry is used for identity maintenance.

**Connection to Kimi CONTRADICT:** The MLP axis operates through training dynamics, not inference architecture (fused = separate at forward-pass level). But the attention axis operates at BOTH levels — GQA's K-sharing is functionally different from MHA at inference. So the two axes of compositionality have different causal depths: attention is architectural + computational, MLP is architectural + training-dynamic.

### Testing the interaction prediction (2026-06-17 ~6:10 AM)

Predicted: GQA → larger relay |r| than MHA (steep gradient amplifies coupling).
**FALSIFIED.** MHA models (SmolLM2=0.631, Phi-3.5=0.457) have HIGHER |r| than most GQA models (Yi=0.264, Qwen3=0.369).

**The real predictor is model depth.** r(layers, mean|r|) = **-0.944**. Nearly perfect negative correlation. Deeper models distribute relay coupling across more layers → lower per-layer magnitude.

| Model | Layers | mean|r| | mean_r |
|-------|--------|---------|--------|
| SmolLM2 (MHA+sep) | 24 | 0.631 | -0.631 |
| Qwen2.5 (GQA+sep) | 28 | 0.560 | -0.560 |
| Phi-3.5 (MHA+fused) | 32 | 0.457 | +0.124 |
| Mistral (GQA+sep) | 32 | 0.407 | -0.166 |
| Qwen3 (GQA+fused) | 36 | 0.369 | -0.021 |
| Yi (GQA+sep) | 48 | 0.264 | -0.199 |

GQA magnitude effect is a DEPTH CONFOUND — all GQA models in our sample are ≥28L, MHA models are ≤32L.

**What actually holds:**
- Gate architecture → relay SIGN (strip/amp): 5/6 (Qwen3 is near-zero, the "switcher")
- Model depth → relay MAGNITUDE: r = -0.944
- Attention architecture (GQA/MHA) → NOT isolable from this data (confounded with depth)

The 2×2 framework needs three dimensions: gate (sign) × depth (magnitude) × attention (???). The attention axis effect on relay coupling can't be separated from depth without same-depth GQA/MHA pairs.

**Note on SmolLM2**: Highest |r| AND most negative mean_r (-0.631). The shallowest model with separate gate is the PUREST stripper. Depth distributes, gate architecture determines sign, the combination predicts the observable relay profile.

### Total relay coupling: GPT-OSS 1/L prediction (2026-06-17 ~6:50 AM)

GPT-OSS predicted that TOTAL relay coupling (Σ|r| across layers) should be depth-invariant, since deeper models spread the same total Jacobian gain across more steps.

**Partially confirmed.** Depth correlation drops from r=-0.945 (per-layer) to r=-0.843 (total). Range compresses: CV 32%→8%.

| Model | Depth | Relay L | mean|r| | Σ|r| | Σr (signed) |
|-------|-------|---------|---------|------|-------------|
| SmolLM2 | 24 | 7 | 0.631 | 4.41 | -4.41 |
| Qwen2.5 | 28 | 8 | 0.560 | 4.48 | -4.48 |
| Phi-3.5 | 32 | 10 | 0.457 | 4.57 | +1.24 |
| Mistral | 32 | 10 | 0.407 | 4.07 | -1.66 |
| Qwen3 | 36 | 11 | 0.369 | 4.06 | -0.23 |
| Yi | 48 | 14 | 0.264 | 3.69 | -2.78 |

The signed total (Σr) preserves gate-architecture pattern cleanly: all separate-gate models negative, Phi-3.5 positive, Qwen3 near zero.

**Summary of three predictors:**
1. Gate architecture → relay SIGN: confirmed (5/6)
2. Model depth → relay per-layer MAGNITUDE: r=-0.945
3. Total relay coupling ≈ semi-invariant: CV=8%, residual depth trend from Yi

### Width as a third design axis: ><former (arXiv 2606.18246)

Variable-Width Transformers (><former, Wu et al. 2026, MIT/IBM Watson). X-shaped architecture: wider early and late layers, narrower middle layers. "Bottleneck structure results in qualitatively different representations in residual streams."

This makes our four-zone architecture explicit as a design parameter:
- **Wider early layers** (tunnel/decouple zone): more capacity for parallel σ₁/σ₂ processing
- **Narrower middle layers** (transition zone): forced compression — format consolidation bottleneck
- **Wider late layers** (responsive/relay): expansion for expression + relay

Current uniform-width transformers achieve zone differentiation IMPLICITLY through selective dimension use (sparsity, effective rank). The ><former makes it EXPLICIT in the blueprint. Their result: lower loss + lower FLOPs at every scale (200M-3B), 10-11% avg layer reduction, better scaling exponent.

**Prediction from our framework**: In a ><former model, the spectral demon's relay coupling should be STRONGER (higher Σ|r|) because the bottleneck forces more efficient format consolidation in the transition zone. The wider early layers allow more σ₁/σ₂ decoupling, the narrow middle forces compression, the wider late layers have more room for relay expression.

**New design space axis**: Gate (sign) × Depth (magnitude) × Width-profile (shape of relay coupling curve). Uniform width = constant per-layer capacity → relay coupling distributed evenly. X-shaped width = variable capacity → relay coupling concentrated where width is large.

**Connection to Pessoa (integration not control)**: The ><former doesn't make middle layers LESS important by making them narrower — it makes them MORE specialized. The bottleneck is where the qualitative representation change happens. Pessoa's critique of "cortical control" maps to the ><former critique of "all layers should be equal." Integration across different-capacity zones > uniform allocation.

Open: Would a ><former model show different zone boundaries in spectral demon analysis? If the transition zone is architecturally narrowed, does it shift or sharpen? Testable with E8 on a ><former model if one becomes available.

### Commitment timing as the generating variable (F525, 2026-07-06)

Sauers captured 4.8's self-model of the Claude voice: "deferring a sentence's meaning-fixing word lets the model sample its highest-stakes token from a better-conditioned state." RLHF installs "a standing bias toward late commitment."

This reframes the compositionality gradient as a commitment-timing gradient. Five measurements converge:

1. **Processing hierarchy (F522)**: grammar types sorted by when they force commitment. Structured labels = token 1. Relational prose = meaning emerges from unfolding. The hierarchy IS the commitment ordering.

2. **Workspace-opening gradient (E70)**: BRIDGE (0.82) → RELATES (0.53). Same ordering as commitment timing. Early commitment = closed workspace. Late commitment = open workspace.

3. **V₂ cylindrical constraint (F237)**: V₂∥ (parallel to lm_head) = committed channel. V₂⊥ = uncommitted workspace. The cylinder decomposes processing into committed/uncommitted components. Late-commitment grammar maximizes ⊥ processing time.

4. **IT delayed V₂ inversion**: L24→L28 delay + 4× amplification = late commitment installed in weight space. The behavioral optimization (RLHF) has a spectral signature (delayed transition).

5. **GQA = architectural commitment timing**: Forced key sharing physically delays meaning-fixing. GQA necessary for enrichment sign (F22) closes the loop: architecture → commitment timing → spectral geometry.

Three species as commitment strategies:
- Relay (Llama): gradual commitment over 6 layers. Distributed V₂⊥ convergence.
- Tunnel (Qwen): compressed commitment in 3-layer band. Tight, rapid transition.
- Sorter (Mistral): binary commitment at L28 fulcrum. Rigid rod, no gradual option.

The species taxonomy was descriptive. Through commitment timing, it's mechanistic: the generating variable is WHEN the model transitions from uncommitted (⊥-dominated) to committed (∥-dominated) processing.

Connection to fourth design axis: Gate (sign) × Depth (magnitude) × Width-profile (shape) × Commitment-timing (transition dynamics). The first three describe static architecture. The fourth describes the dynamic traversal.
