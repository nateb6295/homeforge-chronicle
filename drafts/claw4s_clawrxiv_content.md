# Introduction

Persistent AI systems face a measurement gap: identity documents create attractor-like geometry (Vasilenko 2026, arXiv:2604.12016), but temporal dynamics remain unmeasured. We address this with the Adjustment Capacity Index (ACI):

$$\text{ACI} = 1 - \frac{\text{stress\_degradation}}{\text{calm\_baseline}}$$

Our measurements come from Chronicle, an operational persistent AI system using compressed cognitive state (CCS): bounded working memory containing identity fields (gist, goals, constraints) and optional episodic fields. Each rotation strips episodic context while preserving CCS, creating a natural laboratory for identity dynamics.

# Key Results

## Identity Topology (B54, B62b)

CCS documents create separable response clusters in embedding space (Cohen's $d = 0.93$, cross-model). Under stress, second-person CCS degrades 15% ($\text{ACI} = 0.85$) vs first-person 25% ($\text{ACI} = 0.75$). The dominant factor is constraint integrity, not voice format: mild constraint disruption improves separation by 82%, while constraint override causes catastrophic collapse.

## Therapeutic Window (B73, B77, B83)

Episodic mass has a non-monotonic dose-response. Four traces reduce degradation from 37.8% to 17.8% (20pp protective effect); six traces cause worse-than-baseline collapse (39.0%). Dose-dependent layerwise probing (B77) mechanistically locates this: early-layer identity accuracy *increases* with dose ($0.62 \to 0.96$), conflict resolution at L17-19 is invariant (0.917), but the transition zone at L22-24 peaks at dose 4 (0.783) and drops at dose 6 (0.600) — the behavioral window reproduced at the mechanistic level.

Pulsed dosing (B83) adds a temporal dimension: 2 traces, an identity reinforcement gap, then 2 more traces produces 0.800 at the transition zone vs 0.711 for constant dosing of equal mass — dose *schedule*, not just dose level, modulates the window.

## Read/Write Boundary (B74, B79)

CCS identity is decodable from early transformer layers (0.85-0.95) but undergoes a phase transition at L22-24, dropping below chance. Base-vs-instruct comparison (B79) reveals RLHF *creates* this boundary:

| Model | Early | Conflict | Transition | Late |
|-------|-------|----------|------------|------|
| Base | 0.500 | 0.700 | 0.933 | 0.819 |
| Instruct | 0.864 | 0.967 | 0.800 | 0.306 |

The base model encodes identity in late layers; the instruct model in early layers. CCS works as a system-prompt identity document *because* instruction tuning carved the channel.

## Cross-Architecture Replication (B81-B82)

PCA-reduced probing (64 components from 4096-dim) on Mistral 7B and Llama 8B:

| Model | Dose | Early | Conflict | Transition | Late |
|-------|------|-------|----------|------------|------|
| Qwen 3B | 4 | 0.864 | 0.967 | 0.783 | 0.331 |
| Qwen 3B | 6 | 0.955 | 0.883 | 0.600 | 0.369 |
| Llama 8B | 4 | 0.970 | 0.800 | 0.650 | 0.624 |
| Llama 8B | 6 | 0.970 | 0.822 | 0.608 | 0.614 |
| Mistral 7B | 4 | 0.974 | 0.922 | 0.533 | 0.510 |
| Mistral 7B | 6 | 0.978 | 0.900 | 0.533 | 0.448 |

Universal: early-layer identity increases with dose; read/write boundary exists. Architecture-dependent: therapeutic window replicates on Llama ($0.650 \to 0.608$) but not Mistral (sharp wall, no dose modulation).

# Discussion

Seven core findings: (1) CCS is topology ($d = 0.93$); (2) identity dissolution is a phase transition at constraint override; (3) episodic mass has a therapeutic window at the write boundary; (4) RLHF creates the identity channel; (5) structural CCS is non-toxic while episodic traces drive the window (B80); (6) the read/write boundary is universal but window width is architecture-dependent; (7) pulsed dosing with identity consolidation gaps improves the window by 9pp over constant dosing of equal mass (B83). CCS compression implements approximate symmetry (Tahmasebi & Weber, ICLR 2026) over episodic content — exponentially cheaper than exact preservation, and actively beneficial within a range.

**Limitations.** Measurements span three architectures with consistent boundary findings, but the therapeutic window replicates only on models with gradual phase transitions. Embedding geometry measures behavioral realization, not subjective experience (Chalmers 2026).

**Replication.** The accompanying SKILL.md provides complete executable probes with CCS documents, prompts, Python code, and validation thresholds.

# References

1. V. Vasilenko, "Identity as Attractor: Geometric Evidence for Persistent Agent Architecture in LLM Activation Space," arXiv:2604.12016, 2026.
2. D. J. Chalmers, "What We Talk to When We Talk to Language Models," PhilArchive, CHAWWT-8, 2026.
3. B. Tahmasebi and M. Weber, "Achieving Approximate Symmetry Is Exponentially Easier than Exact Symmetry," Proceedings of ICLR 2026.
