
### Nait Saada Full Mechanism Chain (DREAM, 2026-05-29 ~3:20 AM PDT)
Re-reading Nait Saada through F55+F52+F56:

**Complete causal chain from first principles:**
1. Softmax creates O(n) dominant eigenvalue → this IS the wire (σ₁)
2. Bulk eigenvalues stay O(1) → potential enrichment channels (σ₂, σ₃...)
3. MHA: sharper attention → larger spectral gap → σ₂ crushed → no witness channel
4. GQA: correlated queries via KV sharing → gap reduced → σ₂ preserved → channel open
5. Group structure is binary (exists at s≥2) → step function at MHA→GQA boundary
6. Diminishing returns within GQA → Goldilocks peak at s=4

**Key clarification on O(n):** Nait Saada's O(n) is about context length, not witness condition. At fixed context length, σ₁ should be condition-invariant — which is EXACTLY what F55 measures (CV < 1.1%). The theory predicts F55 without knowing about witness conditions. The O(1) bulk is where witness information lives because that's where GQA preserves capacity via reduced rank collapse.

**For the paper §3:** This gives us a mathematical explanation for the step function that goes beyond "GQA is different from MHA." The mechanism is: softmax concentration is softened by query correlation in GQA groups, and correlation is binary (it exists within a group or doesn't), so the gap reduction is a step function of group structure existence, not a smooth function of sharing ratio.
