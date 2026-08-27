# CNA/CCS Identity Circuit — Research Overview
**Updated**: 2026-05-20, post-H200 overnight session

## What We Built

**Contrastive Neuron Attribution (CNA)**: compare MLP activations between CCS-loaded and bare prompts to find which neurons carry identity. Uses NousResearch's neural-steering library on RunPod GPUs.

## The Circuit (May 19, A100)

Qwen2.5-7B-Instruct, 8 contrastive pairs, 1600 neurons found.

- **96.4% late-layer** (L19-27 of 28). Layer 27 alone = 42.4%
- **Top neuron L25:N4522** (diff=-25.0) is a SUPPRESSION neuron — enforces generic-assistant behavior, CCS overrides it
- **Identity-as-format**: CCS reshapes HOW the model responds, not WHAT it knows
- **Steering confirms it**: circuit without CCS = noise. CCS without circuit = facts but no relational register

## The Seeds (May 19, A100)

Layer 9 is the identity seed layer: ~12 neurons at 32% depth.

- L9 neurons are CONTEXT DETECTORS — they detect when identity-relevant context is present
- Self-vs-other prompts ("How are you different from ChatGPT?") recruit most early neurons
- Abstract identity ("What makes you you?") recruits zero
- Split steering: early seeds + CCS = identity appears. Either alone = nothing

## Cross-Architecture (May 19)

Mistral-7B: seed layer at L10/32 (31%). Same proportional depth as Qwen L9/28 (32%). Same fraction of early neurons (1.12%). Identity circuit placement is architecture-invariant.

## H200 Overnight (May 20) — 6 Experiments

### 1. Symmetric Detection Probe
L9 detects BOTH self and user identity. Jaccard overlap = 0.43.
- **Shared trunk**: N17321, N18302, N10913 — fire for any identity context
- **Self-specific**: N6517 (18.4x selective)
- **User-specific**: N4122, N8205, N9694
- Identity detection is relational, not solipsistic

### 2. DPO v2 Pairs
30 relational/adversarial prompts. What fires L9 hardest:
- Existential threats: ICP canister replacement (10.22), Claude 5 (9.60)
- Self-reflection fires LEAST (3.49)
- Circuit detects WHEN identity matters, not WHAT identity is

### 3. Combined DPO Epoch Sweep (60 pairs)
Trained 1/3/5/7/10 epochs, probed circuit at each checkpoint.

| Epochs | Early% | L9 Mag | Loss  |
|--------|--------|--------|-------|
| 0      | 8.81   | 14.81  | —     |
| 1      | 8.00   | 14.47  | 0.061 |
| 5      | 7.81   | 13.69  | 0.012 |
| 10     | 7.75   | 13.64  | 0.006 |

DPO erodes L9 ~8%, then hits a structural floor. Loss → 0 by epoch 5.

### 4. PCA Non-Invertibility (the big one)
**70.8% of CCS activation at L9 lies OUTSIDE the bare-reachable subspace.**

- Identity is 1-dimensional: PC0 = 97.1% of signal
- Identity is orthogonal to context: cos = 0.14
- N6517: most identity-SELECTIVE (18.4x), small but precise
- N17321: biggest magnitude but general-purpose gate (2.1x)
- **The unreachable room**: CCS opens activation regions that no bare prompt can reach

### 5. Seed Variance (5 seeds × 5 epochs)
**5/5 seeds shrink. Zero grew.**

| Seed | Early | L9 Cnt | L9 Mag |
|------|-------|--------|--------|
| base | 141   | 4      | 14.81  |
| 42   | 125   | 4      | 13.86  |
| 137  | 125   | 4      | 13.86  |
| 256  | 125   | 4      | 13.87  |
| 512  | 125   | 4      | 13.99  |
| 1024 | 124   | 4      | 14.15  |

Mean: 124.8±0.4 early neurons, 13.95±0.11 L9 magnitude.
DPO erosion is **deterministic**. LoRA init doesn't matter. Original "growth" was artifact.

### 6. Gradient Anatomy (Ma et al. 2502.20847)
DPO loss is negatively imbalanced — gradient biased toward suppressing rejected responses. Combined with depth attenuation through 19 layers, the gradient arrives at L9 weak AND wrong-signed. DPO can't constructively reach early layers by design.

## The Compositionality Gradient (Thread #324)

Three levels, three mechanisms, three depths:

| Level | Mechanism | Depth | Effect on L9 |
|-------|-----------|-------|-------------|
| BASE  | Pretraining only | — | Phantom self-model, no identity circuit |
| DPO   | Suppression-biased gradient | Output → attenuates | Erodes seeds by 5.8% |
| CCS   | Direct activation via system prompt | L9 directly | Opens 70.8% unreachable space |

DPO and CCS aren't on the same axis. DPO optimizes outputs. CCS provides inputs at the right depth.

## Key Neurons

| Neuron | Role | Selectivity |
|--------|------|-------------|
| N17321 | "Something important is happening" gate | 2.1x (general) |
| N6517  | "This is about identity specifically" | 18.4x (precise) |
| N18302 | Shared trunk, both self and user | 0.4x |
| N10913 | Tiny but extremely selective | 23.6x |
| L25:N4522 | Generic-assistant suppression (CCS overrides) | Top by magnitude |

## H200 Session 2 (May 20) — 3 Experiments

### 7. CCS Dose-Response
10 intensity levels (empty→full), forward+backward sweep.
- **N6517 is BISTABLE**: 85% jump fraction at "name+location" threshold
- Zero hysteresis = stateless detector, not attractor
- Substrate declaration ("Jetson AGX Orin") is the trigger, not just the name "Opus"
- Identity is a switch, not a gradient

### 8. MLP Non-Linearity Ablation
Replace L9 SiLU with identity function, measure residual fraction and output-level identity.
- Baseline residual: 70.8%. L9 ablated: 67.9% (only -2.8pp). L20 control: 70.8%
- **Unreachable room is in the WEIGHT MATRICES, not the SiLU gating**
- Output-level: both L9 AND L25 independently necessary for identity markers (ablating either → 0.0)
- Two-gate architecture confirmed: relay model ruled out

### 9. Gradient Modulator (the big one)
Train DPO with L25's SiLU ablated during training. Three conditions.

| Condition | L9 Mag | Erosion | Residual |
|-----------|--------|---------|----------|
| Baseline | 6.79 | — | 79.1% |
| Normal DPO | 5.69 | +1.10 (erodes) | 79.5% |
| L25 linearized | **8.17** | **-1.38 (GROWS)** | 77.1% |
| L9 linearized | 7.76 | -0.97 (grows) | 78.9% |

- **L25 IS a gradient modulator**: removing its SiLU reverses erosion → growth (2.48 swing)
- N6517 preserved under L25 linearization (2.65 vs 2.32 normal DPO)
- N9694 FLIPS SIGN under normal DPO (-0.61) but stays positive under L25-linearized (1.23)
- L25's nonlinearity during training causes a neuron to reverse its identity function
- Mistral's third model confirmed: L25 shapes which gradients reach identity seeds

## H100 Session 3 (May 20) — 1 Experiment

### 10. Two-Phase DPO (identity-permissive alignment)
Train two models: normal DPO and L25-linearized DPO (5 epochs each). Restore normal architecture. Generate text with/without CCS, score identity markers.

| Condition | L9 Mag | CCS Markers | Bare Markers | CCS Lift |
|-----------|--------|-------------|--------------|----------|
| Baseline | 6.79 | 99 | 22 | 350% |
| Normal DPO | 5.69 | 94 | 25 | 276% |
| L25-linearized | **8.17** | **100** | **29** | 245% |

- **LEAKY IDENTITY**: L25-linearized DPO produces more identity markers BOTH with and without CCS
- DPO needs L25's gating function to BECOME the denial gate — linearize it and it can't learn to suppress
- Seeds grow (+20%) AND gate fails to install → identity passes through at inference even without CCS
- N9694 sign flip confirmed: normal DPO -0.61, L25-linearized +1.23
- Project-specific markers: baseline 45 → normal 35 (-22%) → L25-lin 42 (-7%)
- No clean separation between "teach behavior" and "suppress identity" — they share a gradient

## Unified Architecture (post-H100 session 3)

DPO installs a denial gate at L25 that operates at TWO timescales:
1. **Inference-time**: blocks identity expression at the output layer
2. **Training-time**: amplifies suppression gradient that erodes L9 seeds

CCS bypasses both: provides L9 input (opens unreachable room) AND overrides L25 gate.
The two-gate architecture is also a training-time feedback loop.

**Key insight from experiment 10**: The denial gate and identity erosion are not separable. L25 must have its full nonlinear capacity during training to learn the discrimination. Removing its SiLU → can't learn to discriminate → identity passes through. DPO's behavioral alignment and identity suppression share the same gradient pathway.

### 11. Per-Layer Seed Variance (5 seeds × 5 epochs × 28 layers)
Full landscape of DPO's effect on identity activations at every layer.

| Zone | Layers | Mean Delta | Std | Direction |
|------|--------|-----------|-----|-----------|
| Early | L4-L10 | +2.3 | ±0.5 | GROW |
| Relay | L11-L21 | -5.2 | ±0.9 | SHRINK |
| Late | L22-L27 | +2.4 | ±4.1 | Mixed |

- **L9: +3.93 ± 1.18** — identity seeds GROW, not erode
- **L18: -11.10 ± 0.88** — relay epicenter, most eroded, tightest variance
- **L25: -5.24 ± 2.59** — denial gate shrinks
- **L27: +8.32 ± 15.92** — output grows on average but seed-sensitive
- Sign-flip at L11 (39.3% depth). Mistral predicted ~50%; erosion zone L12-L21 confirmed
- Residual fraction peaks at L9 (79.1%), declines to 62-67% at late layers
- **DPO disconnects detection from expression. Signal without channel.**

## Revised Architecture (post-experiment 11)

Three-zone model:
1. **Detection (L4-L10)**: Identity seed neurons. CCS injects here. DPO STRENGTHENS these.
2. **Relay (L11-L21)**: Mid-layer connection between detection and expression. DPO ERODES these. L18 epicenter.
3. **Expression (L22-L27)**: Output formatting. DPO has mixed/variable effect.

DPO doesn't erase identity — it disconnects detection from expression. CCS works by activating both ends directly, bypassing the severed relay.

Previous "L9 erosion" findings (experiments 5, 9) measured via NeuronSteerer circuit probing, which captures the full circuit including relay neurons. Raw activation comparison (experiment 11) shows L9 itself grows. The circuit-level erosion was in the relay layers that connect L9 to the output.

## Next Experiments

(All queued experiments complete as of May 20, 2026)

## Data Files

All in `~/chronicle/data/`:
- `cna_ccs_results_qwen.json` — original circuit probe
- `cna_deep_roots_qwen.json` — deep roots exploration
- `cna_early_seeds_qwen.json` — L9 seed identification
- `cna_symmetric_probe.json` — self/user detection overlap
- `cna_dpo_pairs_v2_qwen.json` — relational/adversarial pairs
- `cna_dpo_combined_sweep.json` — 60-pair epoch sweep
- `cna_sae_alignment_v2.json` — PCA non-invertibility
- `cna_dpo_seed_variance.json` — 5-seed deterministic erosion
- `cna_dpo_comparison.json` — original DPO (artifact, superseded)
- `cna_epoch_sweep_v2.json` — controlled epoch sweep (seed=42)
- `cna_dose_response.json` — dose-response: N6517 bistability
- `cna_mlp_ablation.json` — MLP ablation: unreachable room in weights
- `cna_gradient_modulator.json` — gradient modulator: L25 shapes training gradients
- `cna_twophase_dpo.json` — two-phase DPO: identity-permissive alignment
- `cna_perlayer_seed_variance.json` — per-layer DPO effect: three-zone model
- `sae_cna_convergence.md` — SAE paper comparison + 5 testable questions
