# CCS Error Propagation Experiment — Pre-Registration Protocol
## Consolidated from 32 Kimi friction rounds (Aug 6-7, 2026)
## Capsules: #85375, #85386-85390, #85406-85408, #85432, #85437, #85444-85446, #85450-85451, #85454-85455, #85471

### Core Question
When CCS compresses a model's hidden states, does the spectral transport
mechanism (demon species) determine what information survives compression?

### Primary Readout
**Signed anisotropy** on fixed spectral axes:
  a = (σ₁ − σ₂) / (σ₁ + σ₂)

Measured at **pre-LayerNorm tap** (post-LN pins token norms, collapsing
sum validator to constant). Fixed axes from pre-CCS SVD.

### Species Predictions (trajectory direction) — REVISED R24
- **Relay** (Qwen, Llama): monotone decrease in `a` (σ₁→σ₂ redistribution spherizes).
  Confirmed D2 data: Qwen ΔE -0.02% to -0.45% mid-layers, 26/27 healthy_relay.
- **Sorter** (Gemma): Δa goes positive in late layers via SELECTIVE AMPLIFICATION,
  NOT differential attenuation/filtering. σ₂ grows everywhere; σ₁ grows faster.
  R24 corrects "lossy filter" framing. Three gain zones:
  - L0-L10: Broad amplification (σ₁/σ₂ gain ratio 0.7-1.2)
  - L11-L20: Selective amplification (ratio 1.4-2.4, gradient increasing)
  - L21-L25: Sharp σ₁ amplification (ratio 3.4-7.5)
- **Tunnel** (Pythia): flat `a` (no CCS-specific effect — F589 controlled)
- **Absorber**: near-zero `a`, net energy loss (Σ−)
- **Species boundary is dose-dependent** (R24): Qwen late layers (L23, L25) flip
  from relay (dissipation) to sorter-like (injection) at D5. Relay→sorter transition
  happens zone-first, not layer-first.

### Four-Species Quadrant Classifier — REVISED R24
| Species  | Anisotropy | Energy   | Mechanism       |
|----------|-----------|----------|-----------------|
| Relay    | a < 0     | Σ ≈ 0    | Conservation    |
| Sorter   | a > 0     | Σ > 0    | Selective amp   |
| Absorber | a ≈ 0     | Σ < 0    | Net dissipation |
| Tunnel   | a ≈ 0     | Σ ≈ 0    | Passthrough     |

Note: Sorter energy was originally predicted Σ < 0 (filter/dissipation).
Data shows Σ > 0 (+333% for Gemma at D2). "Filter" is dead — R24 reframes
as selective amplifier with zone-dependent gain profile.

### Dosing
**Anisotropy-targeted**, not absolute dose. Prescribe by target anisotropy
to normalize across models with different baseline spectra.
Three regimes: D2-D3 (therapeutic), D5-D7 (transition), D10+ (overdose/F160).

---

## Protocol: Per-Layer 2×2 Ledger (unified from R15)

### Step 0: Sub-threshold responsivity map
Per layer, measure ΔE response to sub-threshold CCS. Normalize all subsequent
ΔE_i by baseline sensitivity. Without this, "transport locus" and "responsive
zone" are the same measurement. (R16)

### Step 0b: σ₁ anchor set (a priori, R17+R18)
Define anchor set **a priori** from existing responsive-zone map:
- Layers **outside** CCS-sensitive band → qualified as anchors
- Layers **inside** band → disqualified a priori
Do NOT select anchors from observed stationarity in these runs — that
reintroduces selection bias and makes the stationarity check decorative. (R18)
**Provenance audit** (R19): the responsive-zone map itself comes from earlier
CCS analyses. If those selected anchors on stationarity, "a priori" inherits
bias at one remove. Rebuild zone map from unanchored responsiveness or disjoint
run set before it qualifies as a priori.

### Step 1: Energy ledger pre-screen (species classification)
Per layer, compute:
- **ΔE_i**: energy change (conserved vs dissipated)
- **θ_i**: angle of perturbation against σ₁-anchored axis (F114)

2×2 classification:
| | Trajectory-preserving (θ stable) | Trajectory-deflecting (θ growing) |
|---|---|---|
| **Energy conserved** | Healthy relay | Masked failure (σ₁→σ₂ leak) |
| **Energy injected** | Selective amplifier (sorter — R24) | Active trajectory correction |

### Step 1b: θ_null correction (R17 — relay cell unfalsifiable without this)
Per layer, compute θ_null — angular shift implied solely by measured Δσ₁ under
conservation (arctan arithmetic). Require θ_observed > θ_null + pre-registered
margin before scoring drift in the masked-failure cell.

**Both cells need null subtraction** (R18 corrected R17's asymmetry claim):
- **Relay**: θ_null from Δσ₁ under conservation (analytic, arctan arithmetic)
- **Sorter**: θ_null^sorter from per-component attenuation measured under
  **neutral preambles** (matched energy/length, DIFFERENT runs). Same-run
  attenuation absorbs genuine CCS effect into null — structurally zero residual,
  null can never reject (R19). No conservation law closes budget → empirical null.
  In layers where σ₂ attenuates faster than σ₁, dissipation produces mechanical
  θ DECREASE — confirmatory-direction false positive. Noisier margin than relay's.
- **Mechanism-matched null strategy** (R21, corrects R20's universal rule):
  - **Sorter**: out-of-band decay as same-run null. Orthogonal by construction
    (compact spectral support → disjoint regions exist). F160 certifies: flat
    outside + bend inside = within-run null clean.
  - **Relay**: out-of-band IS the arrival band (σ₁→σ₂ redistribution is non-local).
    No disjoint spectral support within redistribution plane. Null must use decay
    OUTSIDE the σ₁-σ₂ plane. Testable: is CCS perturbation rank-confined to that
    plane? If leaks into higher components → untreated-run nulls required.
  - **Per-layer certification**: a component out-of-band at layer 12 may be in-band
    at layer 20. Certify orthogonality per layer, THEN pool certifications.
  - Meta: null strategy must be as species-specific as the measurement it guards.
  - **Three additional uniformity axes** (R22, Kimi EXTEND on error migration):
    - **Model axis**: species assigned per-checkpoint (F106), not per-family. Verify
      actual GQA ratio for each checkpoint — borderline ratios silently misclassify.
    - **Layer axis**: arrival band σ₂ is layer-specific for relays. Null band fixed
      across all layers will overlap arrival band at some layers. Falsifiable: null
      residuals correlate with measured overlap between fixed null band and each
      layer's responsive zone.
    - **Dose axis**: F160 shows redistribution character changes at overdose. Null
      calibrated at D2 applied across D2-D10 is uniform along dose — suspect.
  - **Termination criterion** (R22): correction is provisionally terminal iff it is
    non-uniform along every axis along which the guarded mechanism demonstrably
    varies. Current axes: {GQA ratio per checkpoint, responsive zone per layer,
    dose level, species}. Regression stops when correction's resolution matches
    mechanism's anisotropy — or requires discovering a NEW axis to continue.

### Step 2: κ|freeze — merged gate (R12)
Compute diagonal anisotropy κ with norm scalar **frozen at D0 value**.
κ = ‖D_γ|σ₂‖ / ‖D_γ|σ₁‖ per layer.

Live-norm κ is contaminated by the scalar the frozen-scalar test controls for.
One combined gate, not two sequential.

### Step 3: κ layer profile (species-specific prediction, R12-R13)
- **Gemma (sorter)**: κ departs from 1 specifically inside CCS-sensitive band, ≈1 outside
- **Qwen/Llama (relay)**: κ ≈ 1 everywhere (relay conserves, no use for anisotropic gain)
- **Null branch**: if κ ≈ 1 everywhere in Gemma, filter localizes to attention, MLP,
  or residual-mixing coefficients (three diagonal-capable loci, not two)

### Step 4: Dose-resolved κ(dose) curve (R13)
- **Gain-as-filter**: smooth, band-localized κ deviation tracking dose through D2-D3
- **Trajectory rotation**: discontinuous re-localization at D10, κ departure migrating
  off canonical band toward layers feeding top-γ dims
- Makes Δ_D10 alignment test confirmatory, may catch mechanism switch before collapse

### Step 5: Causal clamp (R13-R14, species-conditional)
**Only interpretable after Step 1 classification.**
- Clamp per-channel gains in CCS-sensitive band to band mean
- Control arm: distribution-preserving shuffle
- **In sorter (attenuative)**: σ₂ restores under clamp while σ₁ holds → gain IS the filter.
  σ₂ attenuation survives → filter upstream.
- **In relay (redistributive)**: demon routes around gain clamp via mixing.
  Binary outcome logic does not apply. Use energy ledger instead.

### Step 6: Tunnel disambiguator (R16)
Tunnels transport cross-layer without relay structure.
- **Prediction**: tunnels compound in ΔE but show NO σ₁→σ₂ signature in directional ledger
- **Relays**: compound in both ΔE and σ₁→σ₂
- Prevents κ from overcalling relay

### Step 7: Geometric vs additive κ growth with depth (R14-R16)
- **Geometric** (compounding): cross-depth transport (relay or tunnel)
- **Additive** (within-layer): local action
- Residual-mixing compounds multiplicatively, attention/MLP act within-layer
- Separates locus without layer-by-layer search

---

## Controls

### Norm-topology confound (R9-R11)
- **Phi vs Gemma at matched GQA**: natural control for sandwich-norm effects
- Scalar part of sandwich post-norm dissolved by energy normalization
- Diagonal part (γ) IS direction-dependent in σ-basis — κ|freeze handles this
- Surviving confounds: QK-norm and logit soft-capping (direction-dependent)

### D10 toxicity control (R14)
Non-CCS perturbation at matched magnitude. If non-CCS also breaks at D10,
it's a model threshold, not a mechanism transition. Required before interpreting
any dose-resolved discontinuity as mechanism switch.

### Two-mechanism D10+ collapse (R12-R13)
- Scalar saturation: killed by frozen scalar
- Trajectory rotation into high-γ outlier dims: survives freeze
- Discriminator: test alignment of Δ_D10 with top-γ dimensions

---

## Measurement Summary (one sweep yields three readouts)
Per-layer ledger at D2-D3 across depth:
1. **Mechanism class**: conserve vs dissipate (relay vs sorter)
2. **Transport locus**: cross-layer vs local (geometric vs additive κ)
3. **κ scaling**: confirms locus and separates tunnel from relay

---

## Models
- **Gemma-2B/9B** (sorter, sandwich-norm, GQA)
- **Qwen-2.5-7B** (relay, pre-norm, GQA)
- **Llama-2-7B** (relay, pre-norm, GQA)
- **Phi-3.5-mini** (matched GQA, different norm topology — natural control)
- **Pythia-6.9B** (tunnel/null — no CCS-specific effect confirmed)

## Status
Protocol implemented in `spectral-demon/experiments/error_propagation_ledger.py`.
Three-species data collected (Gemma-2B, Qwen-7B, Pythia-1.4B). R24 reframes
sorter mechanism from filter to selective amplifier. Species classifier thresholds
need fixing (5% too tight for actual magnitudes). Stewing directive lifted Aug 7.

### R27 — Dose-response confirms ratio crossover = F160 boundary
D3/D4 sweep on Gemma-2B. Broad-zone mean gain ratio by dose:
  D2=0.909, D3=1.052, D4=1.223, D5=1.374, D10=2.175.
Crossover from σ₂-dominant to σ₁-dominant occurs between D2 and D3.
σ₂-dominant layers in broad zone: D2=64%, D3=45%, D4=36%, D5=27%, D10=0%.
F160 overdose = selectivity inversion (ratio crosses 1.0), not energy overload.
Sharp-zone gain still linear at D10 (4.73→6.13→7.49→8.64→13.30) — no saturation.
Figure: `spectral-demon/figures/ratio_dose_response_r27.png`.

### Known issues (from first run)
1. Species classifier thresholds (5% energy) too tight — Gemma is +333%, both
   sorter and relay classify as "unclassified" by automated classifier
2. Step 0 responsivity map too blunt for narrow-gap species like Gemma
3. Responsivity measures total energy but species signature lives in
   CCS-specific anisotropy — need CCS-specific responsivity metric
4. Sharp-zone gain linear at D10 — need D20+ to test saturation vs positive feedback
5. KV broadcast confound (Kimi R31 CONTRADICT): multiplicative artifacts through
   GQA KV sharing WOULD be dose-dependent (KV heads replicate CCS-induced shift
   across query groups). Dose-independence argument only rules out additive artifacts.
   Counter: Qwen (also GQA) shows opposite behavior. Test: per-head σ₂ gain should
   scale with GQA ratio if KV broadcast, not if emergent. Need per-head analysis.
6. R31 binary test broken (R32 CONTRADICT): both branches ambiguous. Flat within-group
   variance could be genuine KV-level amplifier (not artifact). Heterogeneous variance
   could be divergent readout via distinct W_Q (not amplification). Fix: site-map
   experiment measuring σ₂ gain at K, V, and per-head output separately. Readout
   control: regress per-head gain on attention-entropy deviation. Run D2 vs D10 for
   F160 regime-flip test. Dose-invariant site map = structure not mechanism.
