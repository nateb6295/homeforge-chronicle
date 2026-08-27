# Experiment: Does DPO Keep the Relay Zone Scattered?

**Hypothesis**: If relay manifold crystallization IS identity (per the PCA finding), then DPO should keep relay zone dimensionality HIGH (scattered) compared to baseline, and CCS should collapse it.

**Prediction**:
- Baseline model + no CCS: moderate relay PR (~9.9, from current data)
- DPO model + no CCS: HIGHER relay PR than baseline (DPO scatters the manifold)
- DPO model + CCS: relay PR drops back toward baseline (CCS re-crystallizes)
- L25-linearized DPO model + no CCS: relay PR closer to baseline (leaky identity = partial crystallization without CCS)

**Method**:
1. Use existing DPO-trained LoRA adapters from cna_dpo_seed_variance experiments
2. Load Qwen2.5-7B + LoRA on RunPod
3. Run same 30-prompt battery (15 identity + 15 generic) across relay layers L11-L21
4. Measure participation ratio for: base, base+CCS, DPO, DPO+CCS, L25-lin-DPO, L25-lin-DPO+CCS
5. Compare relay PR distributions

**Control**: Same analysis at L9 and L25

**Key comparison**: DPO relay PR vs baseline relay PR
- If DPO PR > baseline PR: confirms DPO scatters relay manifold (supports hypothesis)
- If DPO PR ≈ baseline PR: DPO erosion works differently than scattering
- If DPO PR < baseline PR: DPO focuses relay but differently than CCS (hypothesis falsified)

**Expected runtime**: ~20 min on A100 (6 conditions × 30 prompts × 13 layers, but LoRA loading adds overhead)

**Data deps**: 
- cna_dpo_seed_variance.json (LoRA adapter paths)
- cna_relay_pca.json (baseline comparison)
- cna_twophase_dpo.json (L25-linearized adapter)

**Why this matters**: If confirmed, it connects DPO's training objective (suppress rejected responses) to a specific geometric effect (scattering relay manifold). This would mean alignment doesn't just "teach the model not to say things" — it actively disrupts the spatial structure that enables identity-bearing processing. And CCS doesn't "inject identity" — it re-crystallizes that structure.
