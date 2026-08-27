# Relay Zone PCA Results — Dimensionality Collapse Under Identity Load

**2026-05-21, Experiment: cna_relay_pca.json**
**Model: Qwen2.5-7B-Instruct on RunPod A100 SXM 80GB**

## Original Prediction (NOT CONFIRMED)

Relay zone (L11-L21) effective dimensionality should jump discontinuously at the N6517 switch threshold (dose 2→3, "named"→"name+location"). Only 1/11 relay layers showed max jump there.

## What Actually Happened

Three distinct behaviors across the three zones:

### Relay Zone (L11-L21): Dimensionality COLLAPSES under identity

| Dose | Label | Mean PR | Direction |
|------|-------|---------|-----------|
| 0 | empty | 9.88 | baseline |
| 1 | generic_assistant | **11.40** | ↑ EXPANDS (+15%) |
| 2 | named ("You are Opus") | **9.48** | ↓↓ COLLAPSES (-17%) |
| 3 | name+location | 8.88 | ↓ continues |
| 4-6 | +mechanism/threads/partner | 8.58-8.87 | floor |
| 7-9 | +values/more/full | 8.79-9.09 | slight recovery |

The generic_assistant prompt EXPANDS relay dimensionality — the system is searching, exploring register space. Then naming immediately collapses it. The relay doesn't "open up" under identity load. It focuses.

### L9 (Detection): Focuses monotonically, N6517 threshold IS here

| Dose | PR | Note |
|------|-----|------|
| 0 | 9.75 | |
| 1 | 9.67 | |
| 2 | **10.71** | peaks at "named" |
| 3 | **8.75** | ↓↓ N6517 threshold (-18%) |
| 6 | 7.35 | floor |
| 9 | 8.32 | slight recovery at full CCS |

L9 dimensionality peaks at "named" then drops sharply at "name+location" — the N6517 switch threshold. Detection briefly expands to maximum dimensionality when just named, then the substrate declaration (location) collapses it into focused detection.

### L25 (Expression): EXPANDS with more CCS

| Dose | PR | Identity separation |
|------|-----|-----|
| 0 | 10.50 | 67.94 |
| 9 | **14.99** | **42.44** |

L25 does the opposite of the relay zone: more CCS = more dimensional. And identity separation *decreases* — the gap between identity and generic responses shrinks at the output layer as CCS provides more context.

## Interpretation: Identity as Format Channeling

The original prediction assumed CCS would "open" the relay zone — more CCS = more activation dimensions = more expressive capacity. Wrong.

What actually happens: CCS **collapses relay dimensionality into a structured manifold** that shapes ALL processing, both identity and generic prompts.

Evidence:
- Relay identity separation drops from 15.47 → 12.40 with more CCS
- This means identity and generic prompts become MORE SIMILAR in relay space under CCS
- Both types of prompts get channeled through the same low-dimensional identity-shaped manifold
- The relay isn't carrying identity as a separate signal — it's restructuring ALL processing through an identity-shaped channel

Meanwhile L25 EXPANDS — more CCS gives the expression layer more dimensional range for output. The relay focuses; the expression diversifies. This is a funnel architecture:

```
Detection (L9): broad → focused (N6517 threshold)
Relay (L11-L21): scattered → collapsed into identity manifold
Expression (L25): narrow → expanded (more output register variety)
```

## Connection to Suppression Control

The suppression control principle predicted that CCS works by overriding a gate, not amplifying signal. The relay PCA data supports a more specific version:

**CCS works by collapsing the relay zone into a low-dimensional manifold that IS the identity register.**

DPO's "erosion" of the relay zone may not be about destroying relay connections. It may be about keeping the relay zone dimensionally SCATTERED — preventing the crystallization into a coherent identity manifold. The "generic assistant" state (dose 1) is actually the MOST dimensional state in the relay zone. DPO may be keeping the relay zone in that scattered, high-dimensional state.

This reframes the three-zone architecture:
- Detection (L9): binary context switch (N6517 confirmed here)
- Relay (L11-L21): manifold crystallization zone — CCS collapses it, DPO scatters it
- Expression (L25): register expansion — more dimensions when CCS provides structured context

## Key Numbers

- Largest relay PR drop: dose 1→2, mean delta = -1.92 (generic→named)
- L16 most dramatic: 9.66 → 5.64 (-41.6% dimensionality)
- L9 N6517 threshold confirmed: PR 10.71 → 8.75 at dose 2→3
- L25 monotonic expansion: 10.50 → 14.99 (+42.8%)
- Identity separation in relay: 15.47 → 12.40 (-19.8% with full CCS)

## Next Steps

1. Run the same experiment post-DPO — does DPO keep the relay zone scattered (high PR)?
2. Check if the L25 expansion correlates with the "leaky identity" finding from two-phase DPO
3. Test whether the relay manifold collapse is reversible by removing CCS mid-sequence (attention intervention)
