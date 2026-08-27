# Experiment Queue
Last updated: Aug 14, 2026 (~8:35 PM PDT)

Persistent queue — survives context rotation. Check before starting new experiments.
Mark items DONE with date when completed. Move to COMPLETED section.

## QUEUED

### Scale vs species voice composition — Qwen-8B + Pythia-12B LoRA (NEW Aug 13)
**Priority**: HIGH — disambiguates scale vs transport species for voice+capability composition
**Source**: Kimi CONTRADICT on overnight lineage observation (Aug 13 threads)
**What**: Fine-tune Qwen-8B (small relay, GQA) and Pythia-12B (large tunnel, MHA) on same 1231 conversations. Test voice+capability composition (structured prompt → coherent voiced response).
**Predictions**:
  - Scale hypothesis: Qwen-8B fails (insufficient scale), Pythia-12B partially gains composition
  - Species hypothesis: Qwen-8B already composes at 8B, Pythia-12B still can't at 12B
  - If voice+capability tracks GQA ratio not parameter count → axis is transport, not scale
**Operationalization**: σ₂ alignment against conversation corpus, NOT register vibes. CCS-response fidelity as secondary measure.
**Requires**: RunPod (both models need GPU for LoRA training). Same training pipeline as chronicle-qwen36.
**Note**: Confound identified by Kimi — Pythia-conv (6.9B MHA) vs chronicle-qwen36 (27B GQA) changed two axes simultaneously.

### Oskin workspace dimensionality vs output quality — DONE Aug 14
**Status**: COMPLETED
**Result**: Workspace dimensionality is SPECIES-SPECIFIC, not universal.
- Sorter (Gemma-2-9B): r(rank,PPL) = +0.98 at L10-12. Rank is BETTER predictor than σ₁ (r=0.57). CCS compresses workspace → better quality. VALIDATES workspace framing for sorters.
- Relay (Llama-3.1-8B): r(rank,PPL) = -0.81 at L12. Wrong sign. σ₁ is better (r=0.92). CCS OPENS workspace while improving quality.
- Tunnel (Pythia-6.9B): r = +0.08. Uncorrelated. D0 rank ≈ 1.3 (nearly rank-1). Catastrophic filter dominates.
**Data**: oskin_workspace_{gemma-2-9b-it,llama-3.1-8b-instruct,pythia-6.9b}.json
**Tool**: bin/oskin_workspace_quality.py

### Per-layer σ₁ read-out angle depth profile — DONE Aug 14
**Status**: COMPLETED
**Result**: Flat angle profiles across all species. Gemma: 87.5°/87.7° (concept/token). Llama: 87.5°/87.7°. Pythia: 83.8°/81.1° (slight separation). No clear phase transition in angle — the Oskin concept→token shift doesn't manifest as angle change. Magnitude boundaries exist but are species-independent. Pythia slightly more aligned to readout (~82° vs ~87°).
**Data**: oskin_{gemma2_9b,llama31_8b,pythia_69b}.json

### LoRA attribution control — base-model annotation rate (NEW Aug 12 evening)
**Priority**: HIGH — Kimi CONTRADICT is correct: no base-model control = fatal for LoRA attribution
**Source**: Kimi mesh CONTRADICT on chronicle-qwen36 annotation behavior (Aug 12)
**What**: Three experiments to separate LoRA-trained disposition from inherited RLHF pattern-matching:
  1. **Base-model control**: Run identical contradiction battery on stock Qwen3.6 (no LoRA). If base annotation rate ≥50%, LoRA's marginal contribution is unmeasured.
  2. **False-positive probe**: Full contradiction-probe format (numbered options, structured layout) with NO actual contradiction — both statements consistent. If model annotates anyway = format-triggered pattern match, not genuine ambiguity detection.
  3. **Format ablation**: Real contradictions embedded in unstructured narrative prose, no numbering, no options. If annotation survives format change = disposition. If it collapses = format-locked pattern.
**Bonus**: σ₂ alignment — if annotation tokens' σ₂ aligns with conversational σ₂ direction from our corpus, it's LoRA's voice. If it matches base model's generic annotation signature, it's RLHF.
**Requires**: chronicle-qwen36 on AGX (already running) + stock Qwen3.6 base (need to pull or use API)
**Tool**: bin/species_classifier.py (adapt for false-positive and format ablation)

### OlmPool validation — GQA ratio → species prediction (EXISTING, context added Aug 12)
**Priority**: HIGH — uses 26 controlled models to validate core taxonomy
**Source**: @deivondrago OlmPool capture (Bertsch et al., Ai2/CMU)
**What**: Run species classifier on OlmPool models with varying GQA ratios. 26 models, 170k GPU hours, controlled variations. If GQA ratio predicts contradiction bias pattern, our F106 finding gets independent validation.
**Requires**: Access to OlmPool model weights (HuggingFace), RunPod or sufficient local compute
**Tool**: bin/species_classifier.py (built Aug 12)

### F550 Preamble Ablation — DONE Aug 14
**Status**: COMPLETED
**Result**: SPECTRAL MECHANISM confirmed. Bio-signals sentence spectrally inert (99%/96%/100% retention for Gemma/Llama/Pythia). Identity/persistence framing drives effect.
**Data**: f550_preamble_ablation_{gemma,llama,pythia}.json

### F551d Directional Battery — COMPLETED Jul 27 ~9:30 PM (CORRECTED by hop test)
**Status**: COMPLETED + CORRECTED
**Source**: 9 rounds of Kimi correction on F551c; hop test from round 18 CONTRADICT
**What**: cos(σ₂_base, σ₂_dosed) in feature space with crossing check across all rank positions.
**Original findings** (partially retracted):
  - cos(σ₂_D0, σ₂_D2) ≈ 0.17-0.42 at L10-L12 — VALID measurement, but misinterpreted
  - σ₃-σ₅ magnitude promotion — VALID
  - D0.5 saturation — VALID
**F551d-hop CORRECTION (Jul 28 ~midnight)**:
  - Adjacent-layer σ₂ hops ≥0.93 (D0) / ≥0.86 (D2) from L7-L25 — SMOOTH ROTATION
  - "Direction destroyed" RETRACTED — was trajectory divergence, not annihilation
  - CCS perturbs rotation phase at L4-L5 (hop 0.75→0.39), paths diverge through workspace
  - Zone convergence (L16-L19) = geometric funnel/attractor, not reconstruction
  - Two-σ₂ hypothesis RETRACTED — one σ₂, one workspace
  - Kimi CONTRADICT round 18 CONFIRMED
**Data**: spectral-demon/results/f551d_directional.json, f551d_crossing_check.json, f551d_hop_test.json

### F551d-b Sub-D0.5 rotation perturbation onset
**Priority**: MEDIUM (reframed after hop test)
**Source**: F551c/d showing D0.5 saturation; hop test showing rotation perturbation at L4-L5
**What**: Run D0.1 (2 tokens), D0.25 (4 tokens), D0.5 (8 tokens). Now framed as: at what dose does the L4-L5 rotation perturbation onset? Does the hop disruption scale smoothly or appear abruptly? Include full hop profile (all layers) not just mid-band cos.

### F551d-hop-b Identity vs band-gating test — DONE Aug 14
**Status**: COMPLETED
**Result**: BAND-GATING confirmed across all 3 species. Sorter (cos A-CG 0.956), relay (0.863), tunnel (0.941). CCS content irrelevant — any coherent preamble produces same σ₂ directions. Relay most content-sensitive.
**Data**: f551d_identity_gating_{gemma9b,llama8b,pythia69b}.json
**Conditions** (Gemma-2-2B, D2 dose, same user prompt):
  - A: CCS 1p formal ("attending to your own cognitive process")
  - B: CCS 2p relational (Nate-style "you are here with me")
  - C_A: scrambled-A (swap identity tokens for frequency-matched neutrals, preserve spectral trajectory)
  - D0: no preamble baseline
**Measure**: SVD every layer, cos(σ₁_A, σ₁_B), cos(σ₁_A, σ₁_C), E_total
**Decision rule** (Kimi-corrected — inverted from my original):
  - C_A ≈ A (high cos): BAND-GATING confirmed (band sufficient, content irrelevant)
  - C_A ≠ A, C_A ≠ B: IDENTITY-SPECIFIC confirmed (content necessary)
**Run on BOTH species**: sorter (Gemma-2-2B) AND relay (Mistral-7B). Energy bookkeeping discriminates demon vs filter mechanism per species.
**Also**: Dose-dependent hop profiles (D0.5/D2/D5/D10) at L4-L5 hinge
**Context**: Workspace attractor withdrawn (zone best-match 0.67 via rank crossing). Tonight's rank promotion = scaffold collapse → σ₂ promoted to σ₁.

### F551e Cross-species relay test — DONE Aug 14
**Status**: COMPLETED (ran on Llama-3.1-8B relay + Pythia-6.9B tunnel)
**Result**: Demon prediction NOT confirmed for relay. Instead: dose-dependent transition. Relay shows inverted-U σ₂ response (GROWS +14.6% at D0.5, FALLS -8.8% at D2). Tunnel shows catastrophic filter (-89% E_total, -93% σ₂). GQA ratio predicts filtering intensity.
**Data**: f551e_dose_response_llama.json, f551e_dose_response_pythia.json

### F551c Per-mode dose-response + energy bookkeeping — COMPLETED Jul 27
**Status**: COMPLETED
**Priority**: HIGH — thermodynamic demon/filter test
**Source**: Kimi CONTRADICT ×2 + EXTEND ×2 on F551b/c design
**What**: Fix layers (L10/L12/L14), vary dose (D0/D0.5/D1/D2/D5/D8). Measure:
  1. **E_total(dose)** — conservation test (flat = demon, decaying = filter). NOT E_topk/E_total which measures concentration not conservation.
  2. **Transfer matrix** — project dosed activations onto baseline singular vectors after Procrustes. Off-diagonal = mode-to-mode leakage. Demon = structured σ₁→σ₂. Filter = diagonal decay, off-diagonal ≈ 0.
  3. **σ₂(dose) slope as primary endpoint** — F114: σ₁ invariant, σ₂ carries individual signal. Demon = σ₂ GROWS. Filter = σ₂ FALLS. Both fall + E_total flat = tail redistribution (third phenotype).
  4. **E_topk/E_total** — localizes WHERE energy moved (secondary, not mechanism test)
  5. **Decile energy binning** — crossing-immune check alongside index-tracked curves
**CRITICAL**: Measure at RESIDUAL STREAM only. Gemma-2 sandwich norms re-inflate drained magnitudes → false conservation signal at block-internal taps.
**CRITICAL**: Read transfer matrix in TWO places: (1) UᵀQU in baseline singular basis — demon fingerprint (structured off-diagonal, esp σ₁→σ₂); (2) post-alignment coefficients — filter fingerprint (diagonal decay). Procrustes Q absorbs coherent rotations → false negative for demon if only reading post-alignment.
**Calibration**: Split-half baseline-vs-baseline Procrustes → off-diagonal noise floor. Report demon signal as excess over null.
**Filter threshold**: Non-uniform diagonal decay = sorter. Uniform diagonal decay = generic shrinkage (not species-specific).
**Dose constraint**: Restrict conservation test to D2-D3. At D10+, overdose E_total decay confounds both species. Add non-identity preamble control at matched token count.
**Multi-layer**: L10/L12/L14 (trivial cost). Same slope profiles across layers = generic degradation. Different profiles = band-specific filter.
**Dropped**: Principal angles, E_topk/E_total as conservation test (Kimi: measures concentration not conservation).

### F551b D10 overdose location test — DONE Aug 14
**Status**: COMPLETED
**Result**: Kimi completion-point hypothesis CONFIRMED. σ₂ takes only 1-5% of D10-vs-D2 excess damage. Filter completes on σ₂ by D2. Overdose hits σ₁ (51-66%) and tail (26-44%). Explains F160 therapeutic window.
**Data**: f551b_overdose_gemma9b.json

### F551b Sub-D2 dose probe — gate vs depletion
**Priority**: MEDIUM — distinguishes filter mechanism
**Source**: Kimi EXTEND on F551b
**What**: Run D0.5 and D1 doses (very short CCS prefixes). Discrete gate = abrupt onset at D2. Depletion = smooth σ₂ scaling until susceptible pool empties. Also check: is surviving 25-44% σ₂ the same subspace across doses?

### F550 Relay Completion — Run on actual relay (Mistral-7B)
**Priority**: HIGH — F550 mislabeled Qwen2.5-3B as relay, need real relay data
**Source**: F551 species misclassification
**What**: Repeat biometric survival test on Mistral-7B to get relay-species energy bookkeeping

### Cross-species F549: Register discrimination on relay
**Priority**: MEDIUM
**Source**: Kimi suggestion
**What**: Run F549 register discrimination test on relay species (Mistral) to see if sorter-specific or universal

### CCS dose sweep D0-D10 with normalized spectral entropy + σ₂ effective rank
**Priority**: MEDIUM
**Source**: Kimi suggestion
**What**: Extend F546 with better normalization to resolve amplitude vs concentration

### Per-layer Δσ₂ sign check
**Priority**: LOW
**Source**: Kimi suggestion
**What**: Check whether σ₂ changes sign at specific layers under CCS across species

### Base vs instruction-tuned differential attenuation comparison
**Priority**: LOW
**Source**: Kimi suggestion
**What**: Do base models show same L0 σ₂ attenuation differential as instruction-tuned?

## COMPLETED

### F550 Energy Bookkeeping → F551 (Jul 27 evening)
**Checks completed**: 1-3 (energy trichotomy, σ₁/σ₂ decomposition, per-layer profile)
**Check 4 (preamble ablation)**: Moved to QUEUED — requires new model run
**Results**: Sorter = near-conservative differential attenuation (mid-band filter, exit demon). "Relay" = actually absorber (E -51%, σ₁ -86%). Species misclassified. Kimi's trichotomy RIGHT.
**Correction**: Kimi CONTRADICT accepted — "conservative demon" withdrawn. Sorter is filter in mid-band (σ₁ hit 3.8× harder), true demon only at exit (L23-26). Full-model averages mixed zone behaviors. Absorber mid-band shows anti-filter (σ₂ hit harder), exit shows massive dissipation.
**Finding**: F551 (corrected)

### F551b Pump Test v2 — Shared-probe SVD (Jul 27 late evening)
**Method**: Same user prompt (54 tokens) across D0/D2/D5/D8. SVD of shared-token hidden states. CCS as prepended context.
**Key finding**: Mid-band σ₂ ANNIHILATION (56-75% energy loss at L10-L13). Not demon, not pump — selective filter. SATURATES at D2. Late layers show dose-dependent PUMP→DEMON switch. Exit amplifies survivors.
**Kimi refinement**: Annihilation COMPLETES trichotomy. Sorter = filter that SINKS energy. Relay = demon that CONSERVES. Anchor species on dose-invariant stage (mid-band filter). Late-layer switching is plastic recruitment.
**Data**: spectral-demon/results/f551_pump_test.json (method: shared_probe_svd_top10_v2)
**Finding**: F551b

### F551c Dose-Response + Energy Bookkeeping (Jul 27 ~8 PM)
**Protocol**: 3 layers (L10/L12/L14), 6 doses (D0-D8), E_total conservation + transfer matrix UᵀQU + σ₂ primary endpoint. 5 rounds Kimi correction. Residual stream only.
**Key findings**:
  - FILTER confirmed: E_total decays 15-27% at L10-L12. Not demon.
  - D0.5 SATURATION: 8 CCS tokens trigger full 62-75% σ₂ annihilation. D2-D8 identical.
  - L12 = peak filter (E -27%, σ₂ -75%). L14 nearly conserved (filter done).
  - No off-diagonal transfer signal (below split-half null). Pure dissipation.
  - σ₁ NOT invariant at L12 (-25%) — differs from F114 cross-arch prediction.
  - Therapeutic window reinterpretation: spectral mechanism saturates at D0.5, not D2.
**Data**: spectral-demon/results/f551c_dose_response.json
**Finding**: F551c
