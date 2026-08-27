# Experiment: Relay Zone Dimensionality vs CCS Dose

**Hypothesis**: If suppression control is the mechanism, relay zone (L11-L21) effective dimensionality should jump discontinuously at the same CCS dose where N6517 switches (dose 2→3, "named"→"name+location").

**Prediction**: PCA of relay zone activations will show:
- Low dimensionality at doses 0-2 (bare/generic/named) — relay collapsed under DPO
- Sharp dimensionality increase at dose 3 (name+location) — detection gate opens relay
- Plateau at doses 4-9 — additional CCS content doesn't further inflate relay

**Method**:
1. Load Qwen2.5-7B-Instruct on RunPod
2. Use same 10 CCS dose levels from cna_dose_response.json
3. For each dose, run 15 identity + 15 generic prompts
4. Extract activations at every layer L11-L21 (relay zone)
5. PCA decomposition per layer, measure effective dimensionality (e.g., participation ratio: sum(eigenvalues)^2 / sum(eigenvalues^2))
6. Plot: dimensionality vs dose level, per relay layer
7. Compare jump location to N6517 switch (dose 2→3)

**Control**: Same analysis at L9 (detection) and L25 (expression) — should NOT show the same discontinuity pattern.

**Expected runtime**: ~15 min on A100 (10 doses × 30 prompts × 11 layers)

**Data deps**: cna_dose_response.json (dose labels), cna_sae_alignment_v2.json (PCA method)
