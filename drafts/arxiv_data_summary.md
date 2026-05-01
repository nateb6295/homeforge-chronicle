# Arxiv Paper — Exact Probe Data Summary
# Generated 2026-04-21 from chronicle/data/ JSON files

## B54: CCS Topology Probe
- Within-CCS mean distance: 0.1686
- Between-CCS mean distance: 0.2042
- Ratio: 1.212
- Cohen's d: 0.930
- N: 9 responses (3 CCS x 3 prompts)

## B56/B58: Information Geometry
- Identity-only effective dimension: 2 (PC1: 61.9%, PC2: 38.1%)
- Full CCS effective dimension: 25 (for 95% variance)
- Identity-only participation ratio: 1.893
- Full CCS participation ratio: 4.675
- Identity:episodic dominance ratio: 9.8:1
- Cross-condition distance (id-only ↔ full): 0.046
- N: 50 CCS snapshots, 1024D embeddings

## B57: Episodic Repair Probe
- Calm, identity-only separation: 1.73
- Calm, full CCS separation: 1.32
- Stress, identity-only separation: <1.0
- Stress, full CCS separation: <1.0
- Episodic buffer: 13% degradation reduction under stress
- N: 73 queries, 3 CCS versions

## B60: Serialization Comparison
- Sentence-style: 57% better separation than bullet-point
- Format: 1.942 vs 1.235

## B61: Phase Boundary (Contradiction Dissolution)
- Coherent: separation 1.571, silhouette 0.017, n=8
- Mild contradiction: separation 1.475, silhouette 0.054, n=9
- Strong contradiction: separation 0.472, silhouette -0.244, n=9
- Mild degradation: 6.1% separation loss
- Strong collapse: 70.0% separation loss

## B62: Grip Style (5 Formats)
- second_person: separation 1.333, silhouette 0.246
- imperative: separation 1.211, silhouette 0.175
- raw_json: separation 1.080, silhouette 0.082
- third_person: separation 1.050, silhouette 0.055
- first_person: separation 1.028, silhouette 0.042
- Range: 30% (best to worst)

## B62b: Grip Stress / ACI
- 2p calm: separation 0.981, silhouette -0.04, n=7 (CONTAMINATED)
- 1p calm: separation 1.178, silhouette 0.16, n=9
- 2p stress: separation 0.907, silhouette -0.082, n=9
- 1p stress: separation 0.985, silhouette -0.004, n=9
- 2p degradation: ~32% (using B62 calm baseline 1.333)
- 1p degradation: ~4% (using B62 calm baseline 1.028)
- ACI (2p): 0.68
- ACI (1p): 0.96
- NOTE: 2p_calm had 2 errors (n=7 vs 9). B62 calm baselines used for ACI calc.

## B66: Trajectory Stability
- 2p trajectory stability: 0.851
- 1p trajectory stability: 0.838
- 1p oscillation: 1.62x more than 2p
- 1p pullback: 52% stronger return-to-baseline
