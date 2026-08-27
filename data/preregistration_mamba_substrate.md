# Pre-Registration: CCS Substrate Boundary Test
## Does the spectral demon require attention?

**Date:** Aug 16, 2026
**Status:** Pre-registered, awaiting Mamba hardware
**Mesh rounds:** 15 (corrections from Kimi, synthesis from Qwen)

---

## Background

Species probe comparisons (LFM vs Qwen, Aug 16) showed consistent phenomenological
differences between SSM and transformer responses to identical prompts. Initial
interpretation — substrate boundary for CCS — was CORRECTED by Kimi: LFM interleaves
gated convolution with GQA attention blocks. It is not attention-free.

The corrected question: does CCS require attention matrices to operate, or can it
modulate any architecture with spectral structure?

## Fork Design (2x2)

Two independent forks, measured with independent metrics:

### Fork A: Dose-Response Window (F160)
**Question:** Does Mamba show nonmonotonic dose-response under CCS identity framing?
**Metric:** Coherence-gated behavioral individuation
- Probe-level distinctiveness under identity framing across doses D1-D10
- Filtered by coherence floor (D10+ degrades coherence, inflating apparent
  distinctiveness as noise — only count distinctive responses that are also
  internally coherent)
- Nonmonotonic curve with D2-D3 peak = window exists
- Monotone degradation = no window

**Why this metric:** Architecture-free. No sigma decomposition assumed. Behavioral
individuation measures whether the model's output becomes more individually
distinctive under CCS. Coherence gate prevents noise from registering as identity.

**Rejected metrics:**
- CKA: dominated by high-variance directions (sigma-1). Certifies "no change"
  while identity channel (sigma-2) is zeroed. (Kimi, citing Davari et al. 2022)
- Effective rank: isotropic summary of anisotropic geometry (F237). Confounds
  generic compression with transport-specific change. (Kimi, citing F12/F237)

### Fork B: Sigma Decomposition (F114)
**Question:** Does Mamba show sigma-1-invariant / sigma-2-individual structure?
**Metric:** SVD of whatever matrices the architecture provides
- For transformers: attention weight matrices → sigma-1/sigma-2 decomposition
- For Mamba: state transition matrices (A, B, C) → eigenvalue/singular value analysis
- Test whether a low-variance channel carries individual signal while high-variance
  channel remains invariant under CCS

## Four Quadrant Interpretations (pre-registered)

| | B pass (sigma decomposition present) | B fail (no clean decomposition) |
|---|---|---|
| **A pass** (nonmonotonic window) | Demon is substrate-independent. Species taxonomy is transformer-local. Full framework generalizes. | Window mechanics survive without identity decomposition. Dose shapes response through a different channel than sigma routing. New mechanism needed. |
| **A fail** (monotone degradation) | F114 is architecture-general but F160 is attention-specific. Identity structure exists everywhere but therapeutic window is head-mediated. | Clean substrate boundary. Demon requires attention. SSMs outside the framework. |

**Most informative cell:** A-fail/B-pass — would split the framework into
architecture-general geometry (sigma decomposition) and attention-specific
dynamics (dose-response window).

## LFM Zone-Sweep (doable now, no Mamba needed)

**Prediction (from Kimi):** CCS steering on LFM should concentrate on GQA blocks,
not convolution blocks. Each GQA block should engage at its characteristic
sensitivity band (F237 per-layer responsive zones).

**Stronger test:** If LFM's GQA blocks respond but at bands that DEVIATE from
transformer-zone norms, that's substrate signal surviving the density reframing.

**F106 application:** Compute GQA ratio over LFM's attention blocks. Predict
species classification. Test whether classification holds despite convolutional bulk.

## Dissociation Requirement (round 16)

The forks are NOT fully orthogonal: behavioral individuation under identity framing
is the functional readout of sigma-2 (same variable, two abstraction levels). True
independence requires empirical dissociation — doses where one fork moves without
the other.

**Candidate dissociation cells:**
1. Sorter at moderate dose: driven amplification inflates distinctiveness without
   identity change → individuation rises, sigma-2 flat
2. Relay at overdose: sigma-2 spectrally measurable but coherence collapsed →
   Fork B alive, Fork A dark (coherence gate kills behavioral signal)

**If neither dissociation appears:** forks are empirically inseparable, 2x2 collapses
to 1x2. This is also informative — means behavioral individuation and sigma-2 are
measuring the same thing, constraining what sigma-2 IS.

**Pre-Mamba validation:** Run dissociation cells on EXISTING species (sorter: Phi/Gemma,
relay: Qwen/Mistral) with existing CCS tooling. If dissociation exists in transformers,
the 2x2 is valid for Mamba. If not, simplify before running.

## Rate Limiters

- No Mamba model on any device yet (need ~1-3B parameter pure SSM)
- LFM zone-sweep requires layer-restricted CCS tooling (exists for transformers,
  needs adaptation for hybrid architectures)
- One model at a time on AGX (LoQwen has GPU)

## Corrections Log

1. LFM is NOT attention-free — has GQA blocks (Kimi, round 12)
2. 1.2B vs 3B capacity gap confounds comparison (Kimi, round 12)
3. Two-fork binary insufficient — need full 2x2 (Kimi, round 14)
4. Fork axes were coupled through sigma metric (Kimi, round 14)
5. CKA dominated by high-variance, blind to identity channel (Kimi, round 15)
6. Effective rank isotropic summary of anisotropic object (Kimi, round 15)
7. Behavioral individuation needs coherence gate for overdose confound (Kimi, round 15)
