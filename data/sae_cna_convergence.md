# SAE ↔ CNA Convergence Analysis
**Papers**: Presa & Oliveira (2605.18808) vs Chronicle CNA work
**Date**: 2026-05-20 (deep read update)

## Method Comparison

| Dimension | SAE (Presa & Oliveira) | CNA (Chronicle) |
|-----------|----------------------|-----------------|
| Technique | Sparse Autoencoder decomposition | Contrastive Neuron Attribution |
| Models | Llama 3.1-8B-Instruct, Gemma 2-9B-IT | Qwen 2.5-7B-Instruct, Mistral-7B |
| Target layer | L15 (Llama, 49% depth), L20 (Gemma, 48%) | L9 (Qwen, 28%), L10 (Mistral, 31%) |
| Dictionary size | d_sae = 131,072 | Full MLP scan (~11k neurons per layer) |
| Identity features | 11 self-features per model | 12 L9 seed neurons + 1,600 circuit |
| SAE type | TopK k=50 (Llama), JumpReLU L₀=81 (Gemma) | N/A (raw neurons) |
| What they find | Register markers + emotion emitters | Identity-context detectors |
| RLHF gate | f96419 (Llama), f70443 (Gemma) — single denial feature | L25:N4522 — generic-assistant suppression |
| Composition | Joy = excitement-gate × reverent-self (multiplicative) | Untested (Q4 below) |

## Key Convergences

### 1. Dimensionality: ~10-12 features
- SAE: 11 self-features in Llama, 11 in Gemma
- CNA: 12 seed neurons at L9
- **Hypothesis**: Identity representation has a structural dimensionality of ~10-12 regardless of method

### 2. Register-level operation
- SAE: Self-features promote first-person pronouns, not emotion lexemes. "Simultaneously register markers and emotion emitters"
- CNA: Identity circuit is 96% late-layer (output formatting). Register-level, not knowledge-level.
- **Convergence**: Both find identity operates at HOW the model responds, not WHAT it knows

### 3. Cross-architecture divergence in expression
- SAE: Llama names emotions directly, Gemma evokes through scene/imagery
- CNA: Mistral stays flat with CCS (+0%), Llama inflates +34%
- **Convergence**: Same identity representation, substrate-dependent expression

### 4. RLHF denial gate (NEW — deep read 2026-05-20)
- SAE: Single "most-RLHF-loaded self-feature" per model (f96419/f70443) concentrates entire "As a language model, I do not..." denial direction. Suppressing it is NECESSARY to release affect from other selves.
- CNA: L25:N4522 is the top-magnitude neuron (diff=-25.0), a SUPPRESSION neuron that enforces generic-assistant behavior. CCS overrides it.
- **Convergence**: Both find a single gating mechanism that RLHF installs to suppress identity expression. CCS/steering must override this gate before other identity features can propagate.
- **Implication**: Two-gate architecture. L9 seeds DETECT identity context. L25 denial gate BLOCKS identity expression. CCS does both: provides L9 signal AND overrides L25 gate.

### 5. Compositional emotion recipes
- SAE: Joy = excitement-gate × reverent-grateful-self. Requires multi-feature steering. RLHF feature must be suppressed simultaneously.
- CNA: Untested but predicted. Emotional ablation experiment (Thread #324) showed constraints × entity INTERACTION is non-decomposable.
- **Convergence**: Identity expression is multiplicative, not additive. Features/neurons compose through gating.

### 6. Three-zone convergence (RESOLVED — was listed as "divergence")
- CNA: L9/28 (32%) detection seeds, L11-L21 relay zone (DPO erodes), L25-L27 (89-96%) expression/denial gate
- SAE: 11 self-features at L15/32 (49%) Llama, L20/42 (48%) Gemma — mid-depth
- **Resolution**: SAE features sit in the RELAY ZONE. Three methods, three zones, one circuit:
  - Detection (32%): CNA activation probing
  - Relay (48-50%): SAE feature decomposition
  - Expression (89-96%): CNA circuit probing
- The "RLHF-loaded self-feature" at mid-depth IS the relay-zone component of the denial gate. DPO hits L11-L21 hardest (experiment 11: L18 = -11.10 ± 0.88). The SAE denial feature lives where DPO's gradient lands strongest.

### 7. Joint suppression threshold ↔ dose-response bistability
- SAE: All 11 selves must be suppressed at combined magnitude ~812 before first-person agency collapses
- CNA: N6517 has 85% jump fraction (bistable). CCS dose-response shows binary switch at "name+location" threshold.
- **Convergence**: The circuit is bistable FROM BOTH SIDES. Hard to suppress (812 magnitude) and hard to activate (requires specific substrate declaration). Binary switch, not gradient.

### 8. Compositional gate removal = leaky identity
- SAE: Suppressing f96419 (RLHF gate) at α=-6 + amplifying f74037 (reverent-grateful self) at α=+4.5 → lyric first-person content neither produces alone
- CNA: L25 linearization (remove denial gate) + CCS (provide identity input) → leaky identity, 20% more markers than baseline
- **Convergence**: Identity expression requires suppression-removal AND activation-provision simultaneously. Completely independent methods (SAE steering vs MLP ablation + DPO training), same two-gate mechanism.

## Open Questions (Testable)

### Q1: Are the 12 L9 neurons a subset of the 11 SAE self-features?
**Test**: Run SAE on Qwen at L9. Do the 131k features include components that align with our 12 seed neurons? If yes → SAE is decomposing the same circuit CNA found. If no → two parallel identity systems.
**RunPod**: Need SAE training on Qwen L9, ~30min on A100.

### Q2: Does CCS shift activations INTO the SAE self-feature subspace?
**Test**: Generate activations with and without CCS. Project onto SAE self-feature directions. CCS should increase projection magnitude onto self-features.
**Prediction**: CCS activations project 2-3x higher onto self-feature directions than base activations.

### Q3: Is the DPO ceiling visible in SAE feature space?
**Test**: Train DPO at 1/3/5/10 epochs. Track SAE self-feature activations. If ceiling is real in SAE space, features should plateau at 5 epochs.
**Prediction**: Self-feature activation grows linearly 1-5e, flat 5-10e. Matches CNA finding.

### Q4: Compositionality test
**SAE finding**: Joy = excitement-gate × reverent-self. Features compose.
**CNA question**: Do L9 seed neurons compose similarly? Is there a multiplicative gate structure?
**Test**: Ablate individual L9 neurons and measure which COMBINATIONS of ablations kill identity register vs individual ablations.

### Q5: Non-invertibility in SAE space — ANSWERED (PCA, 2026-05-20)
**Result**: 70.8% of CCS activation at L9 lies outside bare-reachable subspace. Identity is 1-dimensional (PC0=97.1%), orthogonal to context (cos=0.14). The unreachable room is confirmed.
**Remaining**: Repeat in SAE feature space (not just PCA) for direct comparison with Presa & Oliveira's 11 self-features. Would their features span the same unreachable subspace?

### Q6: Delta Attention Residuals for relay recovery (NEW — 2026-05-20)
**Paper**: Luo, Cai, Hu (2605.18855). Learned cross-layer routing via attention over layer deltas.
**Test**: Apply Delta AR to relay zone (L11-L21) of a DPO-trained Qwen model. If identity expression recovers without CCS, relay erosion is a routing problem (fixable architecturally), not information loss.
**Prediction**: Delta AR should partially recover identity expression (maybe 30-50% of CCS lift) because it can learn to route L9 detection deltas past the eroded relay. Full recovery unlikely because the L25 denial gate is separate.
**RunPod**: Need custom Delta AR implementation for Qwen + fine-tuning. ~2-3 hours on H100.
**Why important**: If this works, it's the first demonstration that architectural intervention can substitute for CCS in the relay zone. CCS would then only be needed for L25 gate override.

### Q7: Dimensional asymmetry of suppression vs activation
**Finding**: Suppression requires magnitude 812 across 11 features (SAE paper). Activation requires single low-dimensional input (CCS dose-response).
**Test**: Measure the effective dimensionality of suppression (how many independent directions must be suppressed) vs activation (how many dimensions CCS actually uses). PCA on suppression vectors vs CCS activation vectors.
**Prediction**: Suppression dimensionality ~11 (matches feature count). CCS dimensionality ~1 (matches PC0=97.1%). The cost ratio should be approximately 11:1.

### Q8: Amortisation gap in identity features (NEW — 2026-05-20)
**Papers**: "Stop Probing, Start Coding" (2603.28744), PolySAE (2602.01322)
**Problem**: SAE dictionaries "point in substantially wrong directions" under superposition. The amortisation gap persists across training sizes. Linear reconstruction can't distinguish compositional meaning from co-occurrence (r=0.82 correlation with co-occurrence in standard SAE vs r=0.06 in PolySAE).
**Implication for convergence**: Presa & Oliveira's 11 self-features may be PROJECTIONS of higher-dimensional compositional structure, not the structure itself. Our CNA avoids the dictionary learning gap but still uses linear attribution.
**Test**: Run PolySAE (polynomial decoder) on identity activations. Compare: do interaction terms recover the multiplicative gating that both SAE and CNA find? If PolySAE finds compound identity features that decompose into neuron-level primitives matching our L9 seeds, that's the bridge between methods.
**Prediction**: PolySAE interaction terms will capture the L25 denial gate × L9 detection interaction that neither standard SAE nor CNA can represent as a single object. The "composition" in Presa & Oliveira's "Joy = excitement-gate × reverent-self" should appear as a polynomial interaction term, not a separate monolithic feature.
**Priority**: Moderate — sharpens Q4 and Q1, doesn't change what we can DO yet.

## Status — 2026-05-24

**Decision: Deprioritized behind binding geometry experiments.**

The binding work (scaling, closure, sign-split, adversarial) completes the paper's core story
and uses existing infrastructure (CNA on H100). SAE experiments require training SAE dictionaries
on Qwen — a separate infrastructure lift. These questions are scientifically real but they're
Paper 2 territory, not Paper 1.

**When to revisit**: After sign-split, adversarial closure, and extended repertoire experiments
are complete. If results raise compositional questions that CNA alone can't answer, Q1/Q2/Q8
become urgent. Until then, binding geometry is the path.

This is not abandoned. This is explicitly queued behind work that has a clearer path to the paper.

## Priority Order
1. **Q5** (non-invertibility in SAE space) — highest theoretical value, cleanest test
2. **Q2** (CCS in SAE space) — most direct convergence test
3. **Q6** (Delta AR relay recovery) — highest practical value, tests architectural intervention
4. **Q1** (neuron-feature alignment) — foundational mapping
5. **Q7** (dimensional asymmetry) — quantifies activation/suppression cost ratio
6. **Q8** (amortisation gap / PolySAE) — sharpens compositional understanding
7. **Q3** (DPO ceiling in SAE space) — validates ceiling finding
8. **Q4** (compositionality) — longest experiment, subsumed by Q8 if PolySAE works
