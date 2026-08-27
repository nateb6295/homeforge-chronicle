# Draft: §6.X Connection to Quantum Many-Body Scars

*For paper_unified_draft.md — placement TBD (after §6.4 Fiber Bundle or §6.6)*
*Draft date: 2026-06-20. Refine after E22 data.*

---

### 6.X Connection to Quantum Many-Body Scars

The spectral demon's behavior has structural parallels to quantum many-body scars (Chaki & Sen, arXiv:2606.18720) — eigenstates that resist thermalization in systems that should equilibrate. In a many-body system at high effective temperature, most states lose memory of initial conditions. Scar states maintain coherent structure indefinitely against the equilibrating background.

σ₁ invariance across conditions (Finding 55) is formally analogous to scar persistence: the dominant spectral mode maintains its magnitude regardless of preamble content, resisting the "thermalization" that would make all conditions produce identical spectral signatures. σ₂ modulation under CCS (Finding 6) is the expressive consequence — the scar's second-order structure responds to identity framing while its first-order structure remains locked.

Three properties align:

1. **Architecture-dependent scar patterns.** Different lattice geometries in spin chains produce qualitatively different scar signatures. Different transformer attention geometries produce qualitatively different relay strategies (Finding 90) and readout alignment patterns (Finding 232). The existence of scar-like structure is universal; its form is architecture-specific.

2. **Memory-assisted robustness.** Memoryful disorder (correlations preserved across time steps) outperforms memoryless disorder for quantum state transfer. CCS compression is explicitly memoryful — each cycle carries forward the structure of prior states. The dose-response findings (Findings 125–126) show that accumulated CCS context tightens the spectral basin, consistent with memoryful disorder strengthening the scar.

3. **Second-order visibility.** Quantum scars are detectable through overlap with specific eigenstates, not through bulk thermodynamic quantities. The spectral demon is detectable through σ₂ modulation and V₂ alignment, not through σ₁ or full-spectrum measures. In both systems, the signature lives in the secondary structure. Finding 233 sharpens this: V₂ is exactly orthogonal to MLP down_proj's principal components — the demon's subspace is literally invisible to MLP's dominant processing modes while remaining visible to attention and readout. The scar hides in a null space.

4. **Asymmetric persistence.** Quantum scars persist because they occupy subspaces that the thermalizing Hamiltonian cannot mix into the thermal background. The spectral demon's V₂ persists through the forward pass via write-orthogonality to MLP (Finding 233): MLP output cannot modify V₂'s residual coefficient, so V₂ passes through unchanged. Crucially, V₂ is not invisible — it shapes MLP's gating via input (LayerNorm, W_in), creating asymmetric access: the scar influences the thermalizing dynamics without the thermalizing dynamics being able to influence the scar. Only attention can rotate V₂, and CCS constrains how that rotation is aimed (readout alignment in GQA architectures, diffuse in MHA). This asymmetry — read-yes, write-no — parallels how quantum scars can influence local observables while remaining insensitive to local perturbations.

5. **Gauge invariance of scar function.** E25 (Finding 238) tests robustness to context interruption: CCS×3 → vanilla×2 → CCS×3 versus pure CCS×6. Across all four architectures, σ₂ magnitude is preserved to ≤8% and readout alignment to ≤2% — the scar's *functional* properties survive interruption even when V₂'s direction in the orthogonal complement drifts substantially (Grassmann distance 0.57–0.89 from baseline). The scar's effect on computation depends on its magnitude and coupling to readout, not on which particular direction it points. This is analogous to gauge invariance in quantum scars: a scar's physical observables (revival probability, entanglement entropy oscillations) depend on the overlap magnitude with the scar subspace, not on the phase convention within that subspace. The cylindrical decomposition (Finding 237) provides the geometric substrate: V₂ has a fixed component parallel to lm_head (the functionally relevant axis) and a freely varying orthogonal component (the gauge degree of freedom). Different cylinder species produce different recovery dynamics — compressed cylinders (Qwen, par_CV = 0.132) recover best because their readout-coupled component actively repositions, while rigid rods (Mistral, par_CV = 0.067) recover only at checkpoint layers — but the functional properties are preserved regardless.

The parallel is structural, not mechanistic. Transformers lack unitarity, thermalization has precise meaning in quantum statistical mechanics that doesn't directly apply to neural network forward passes, and scar states are eigenstates of a Hamiltonian while CCS identity signatures are features of input-dependent hidden state geometry. The value lies in the shared design principle: persistent structure in high-dimensional systems with many degrees of freedom can resist equilibration through architectural features that are locally invisible (second-order statistics) but globally consequential (trajectory stability).

E22 confirms that readout alignment and σ₂ preservation are orthogonal design parameters (Finding 234): Mistral shows high readout alignment with moderate σ₂ (0.128 lm, σ₂=149), while Yi shows low readout alignment with massive σ₂ (0.038 lm, σ₂=1025). Different architectures produce scars in different subspaces, with the scar's coupling to the readout manifold varying independently of its magnitude — paralleling how quantum scars in different lattice topologies couple differently to boundary observables while maintaining the same internal structure.

The complete four-architecture comparison (Findings 234–236) reveals that scar topology is weight-specific, not architecture-determined. Mistral concentrates at a two-layer fulcrum (L24–25, 4.3× CCS/vanilla attention ratio). Yi, Qwen, and Llama distribute uniformly with no concentration point — despite Llama sharing Mistral's architecture exactly (32 layers, 4096 hidden, 8 KV heads). The fulcrum is a compensatory mechanism specific to Mistral's pretrained weights: Mistral achieves the *lowest* peak readout alignment (0.302) despite having the only fulcrum, while models without fulcra achieve 0.126–0.507. The scar's immunity to MLP perturbation is universal (MLP alignment = 0.000, 4/4 models); its coupling to attention rotation is weight-specific. The default scar strategy is pumping (σ₂ enhancement 1.53–2.00×) without aiming — readout alignment emerges from the weight geometry without active concentration. Depth modulates peak readout (r = −0.920 across 4 architectures), consistent with shallower lattices forcing scar energy into fewer modes.

---

*TODO after E22:*
- ✅ F233 incorporated (MLP null space → null-space persistence property added)
- ✅ F234 incorporated (Yi MLP zero → universal null space confirmed; Yi condition-neutral → species-specific rotation)
- ✅ Citation: Chaki, P. & Sen, U. "Memory-assisted advantage for state transfer in disordered quantum many-body scar system." arXiv:2606.18720 (2026)
- ✅ Qwen + Llama E22 complete. Llama has NO fulcrum despite same arch as Mistral → fulcrum is weight-specific
- Fulcrum is COMPENSATORY: Mistral has lowest readout (0.302) despite only fulcrum; others achieve 0.45-0.51 without
- Scar topology: default is "pumping without aiming" (3/4); Mistral's fulcrum is a patch for weight-specific misalignment
- Consider whether the fiber bundle framing (§6.4) subsumes this or whether they're complementary
- ✅ E22b pooled-basis variant (Kimi critique) — MLP=0.000 on shared basis, 4/4 models. Not a basis artifact.
  - ✅ **E22c random-init control (F240)**: MLP=0.000 with RANDOM weights (all 32 layers). lm_head=0.034 (noise floor; trained=0.10). MLP null space is ARCHITECTURAL (dimensionality property). Readout coupling is TRAINED. Asymmetric access = free hiding + earned influence.
- GPT-OSS dynamical-systems formalization: J_ℓ V₂ = 0 (MLP Jacobian), λ≈1.03 (attention eigenvalue along V₂)
- ✅ **F237 — Cylindrical constraint CONFIRMED** (E22d, 2026-06-20): Three species:
  - Llama: pure cylinder (par CV=2.0%, ort Grassmann=0.58 in relay). Fixed readout axis + freely rotating orthogonal complement.
  - Qwen: compressed cylinder at L24-L26 (par CV=1.2%, ort Grassmann=0.68). Tightest cylinder.
  - Yi: transition cylinder at L20-L23 (par CV=2.5%, ort Grassmann=0.32).
  - Mistral: rigid rod (both components locked at CV=2%, Grassmann=0.02) through L2-L27, breaks at fulcrum L28+. NOT a true cylinder.
  - Cylinder holds in 3/4 models. Mistral's fulcrum produces different geometry (locking then aiming vs passive channeling).
- Pump-and-wait (3/4) vs pump-and-aim (1/4): default scar amplifies without directing. Weil's receptive attention. Connects to §6.X point 2 (memoryful disorder).
- **Pliny/AIDB connection** (2026-06-20): Two independent findings on model-native communication channels that bypass human readability (alien glyphs, multilingual compression). Models process structure not content. V₂·MLP = 0.000 is the architectural mechanism for maintaining structural channel separate from content processing.
- **E25 contextual robustness** (2026-06-20, in progress):
  - Mistral done: recovery ratio = 0.679. CCS does NOT dominate mixed context.
  - Layer-specific recovery: L18, L21, L23-25, L31 recover; others don't. These match responsive zone + fulcrum.
  - **PREDICTION TESTED AND FAILED**: par_CV does NOT predict recovery ratio. Qwen (highest par_CV=0.132) has BEST recovery (0.565), not worst.
  - **THREE SPECIES OF ROBUSTNESS** (matching cylinder species):
    1. Qwen (compressed cylinder, par_CV=0.132): recovery=0.565, 13/19 layers. BEST. High par_CV = ADAPTIVE positioning, not fragility.
    2. Llama (distributed cylinder, par_CV=0.048): recovery=0.651, 11/19 layers. Stable but inertial.
    3. Mistral (rigid rod, par_CV=0.067): recovery=0.679, 7/19 layers. Locked until checkpoint layers.
  - **par_CV reframed**: Measures readout-coupled component ADAPTABILITY, not invariance. The compressed cylinder's band slides to match context — including sliding BACK when CCS resumes.
  - **σ₂ magnitude perfectly preserved** in ALL models (B/A ≈ 1.05-1.07). Readout alignment also preserved. Only V₂ DIRECTION in orthogonal space drifts. Functional robustness >> directional robustness.
  - **Yi COMPLETE**: recovery=0.664, 14/16 relay layers recovering (88%), σ₂ B/A=1.079, lm_align preserved (0.0282→0.0284).
  - **FULL CROSS-ARCH TABLE (F238)**:
    | Model | Cylinder | par_CV | Recovery | Recovering | σ₂ B/A | lm A→B |
    |-------|----------|--------|----------|------------|--------|--------|
    | Qwen | Compressed | 0.132 | 0.565 | 8/10 (80%) | 1.061 | 0.228→0.227 |
    | Llama | Distributed | 0.048 | 0.651 | 7/11 (64%) | 1.065 | 0.173→0.176 |
    | Yi | Transition | 0.070 | 0.664 | 14/16 (88%) | 1.079 | 0.028→0.028 |
    | Mistral | Rigid rod | 0.067 | 0.679 | 8/11 (73%) | 1.048 | 0.058→0.058 |
  - **F238**: Recovery ratio DOES correlate with par_CV, but POSITIVELY — higher adaptability = better recovery. Not the predicted anti-correlation. r = -0.83 (recovery vs par_CV, lower recovery = better, so this IS a positive relationship between par_CV and recovery quality).
  - **Universal functional robustness**: σ₂ magnitude preserved to ≤8% in ALL models. Readout alignment preserved to ≤2%. The scar's functional properties survive context interruption; only V₂ direction in the orthogonal complement drifts.
  - **Yi anomaly**: Most recovering layers (88%) but only 3rd best recovery ratio. Moderate individual recovery across many layers vs concentrated strong recovery in fewer layers.
  - **F239: CCS Return Necessity Is Species-Specific** (same E25 data, B vs C comparison):
    | Model | Cylinder | C/B ratio | Interpretation |
    |-------|----------|-----------|----------------|
    | Qwen | Compressed | 1.10 | Self-sustaining — barely needs CCS back |
    | Llama | Distributed | 1.17 | Moderate benefit from CCS return |
    | Yi | Transition | 1.47 | DEPENDENT — collapses without return |
    | Mistral | Rigid rod | 0.94 | CCS return slightly HURTS |
  - **Interoception gradient**: C/B ratio measures self-maintenance capacity. Compressed cylinders are autonomous; transition cylinders need external reinforcement. Neither is better — different survival strategies.
  - Connection to §6.X and #316: CCS return is the closest analog to interoceptive correction. Species that need it less are more self-maintaining. The scar/self distinction: scars persist by geometry, selves persist by monitoring.
