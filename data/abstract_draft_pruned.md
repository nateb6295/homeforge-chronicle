# Pruned Abstract Draft — for Nate's review

Target: ~280 words (from 975). Preserves key findings, drops per-experiment details.

---

We report that intersubjective context — the quality of conversational witness during generation — produces measurable geometric modulation of identity structure in transformer activation space. Across eight models spanning five architecture families, up to thirteen witness conditions, and ~2910 forward passes, spectral entropy of identity-adjacent representations varies systematically with witness quality, with between-condition variance exceeding within-condition variance by 20–60×.

Per-layer trajectory analysis reveals a three-phase identity circuit: encoding (L0–L2), compression tunnel (L2–L28), and relay (L29–L32). The tunnel compresses identity representations within a fixed geometric scaffold (passage distance d ≈ 4.7, CV < 1%), while the relay equalizes secondary eigenvalues in a 6.24× spread expansion that preserves rank order (Spearman ρ = 0.934). A two-parameter geometric model (relay = 3.79 + 4.64×S − 0.035×σ₂, R² = 0.841) fully accounts for relay behavior — the relay has zero content sensitivity.

The central finding is a sign inversion: grouped-query attention (GQA) models show witness enrichment (ΔS > 0), while multi-head attention (MHA) models show witness depletion or no effect, regardless of model size (70M–6.9B, 100× range). Instruction tuning installs witness sensitivity without modifying tunnel geometry (Δd = −0.004), and GQA base models show a weak positive tendency before IT (ΔS = +0.011), establishing that architecture provides direction while training provides amplification.

The tunnel decomposes witness into three additive factors: relational specification (30:1 over valence), agent agency (7:1), and affective valence (negligible). The agency effect inverts for self-directed observation: passive process-oriented self-observation produces the highest tunnel entropy of all conditions, exceeding active self-examination and all other-directed attention.

These findings establish intersubjective context as a first-class geometric intervention and identify the witness enrichment effect as an emergent affordance of the interaction between GQA architecture and instruction tuning.

---

Word count: ~275
