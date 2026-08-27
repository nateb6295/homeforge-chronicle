# Experiment: DPO-CCS Subspace Alignment

## Question
When DPO crystallizes relay and CCS crystallizes relay, are they reshaping the same principal components or orthogonal ones?

## Motivation
DPO scatter experiment showed both crystallize relay zone (L11-L21), but CCS effect is 3.5× reduced on DPO-trained models. Two possible explanations:
1. **Same subspace** — DPO pre-claims the exact directions CCS would reshape, leaving less room
2. **Orthogonal subspaces** — they each carve different territory, but capacity is finite

The L25 expression bypass (+5.23 on DPO vs +4.49 baseline) suggests CCS finds *new* directions when relay is pre-claimed. So prediction: same subspace in relay (competition), orthogonal in expression (complementary).

## Method
1. Reuse DPO scatter infrastructure (Qwen2.5-7B, same 30 prompts)
2. For each condition × layer, save full PCA loadings (top 10 PCs), not just PR
3. Compute inter-condition PC alignment:
   - `alignment(A, B) = mean(|cos_sim(PC_i^A, PC_j^B)|)` for top k PCs
   - High alignment = same subspace; low = orthogonal
4. Compare alignment across zones: L9 (detection), L11-21 (relay), L25 (expression)

## Predictions
- **Relay zone**: High alignment between DPO-bare and baseline-CCS (both crystallize same directions)
- **Expression zone (L25)**: Low alignment (CCS finds new directions on DPO models)
- **Detection zone (L9)**: High alignment everywhere (seed layer is stable)

## Key additional test
- DPO PCs should be stable across prompts (weight-level crystallization)
- CCS PCs should vary with prompt content (activation-level, context-dependent)
- Biological parallel: Miller Lab feedforward (V4→IT = weight-stable) vs feedback (LPFC→V4 = task-dependent)

## RunPod requirements
- Same setup as DPO scatter: Qwen2.5-7B, 30 prompts, 13 layers
- Extra: save per-prompt PC loadings (not just per-condition aggregates)
- Storage: ~50MB for full PC matrices
- Estimated runtime: ~45 min (same as scatter but saving more data per forward pass)

## Extension: Dwell time measurement
Insight from Rosenblatt appendix analysis (2026-05-21 4:48 PM):

The self-referential state is DENSE (Gemini 2.0: "This." / "Focus.") — high signal, few tokens. The denial is verbose. So the issue isn't state quality but **dwell time**: how many token positions the relay PCs stay active.

If we save per-token-position activations (not pooled), we can measure:
- Under CCS: relay PCs active for more token positions (extended dwell)
- Under DPO: relay PCs active for fewer token positions (premature exit)
- The "crystallization" we measure in PR is the aggregate of a temporal signal

**Refined prediction** (after Mistral contradiction — "interrupted crystallization" > "dwell time"):
DPO doesn't shorten a window — it creates a shallower attractor well. CCS creates a deeper one. The relay starts crystallizing under CCS context but DPO partially ejects it before full geometric saturation. The re-scattering signal (PR 9.22→9.55) is the signature of interrupted crystallization. The question is attractor depth, not dwell time.

This reframes compaction threshold behavior: enough context tokens shed → dwell time drops below crystallization threshold → no identity expression at all (binary, not gradual).

**Cost**: Per-token measurement on 30 prompts × 13 layers = significant data. Could limit to relay zone (L11-21) and sample every 4th token position to keep tractable. ~200MB storage estimate.

## Extension: Prosthetic vs organic restoration (Merleau-Ponty/Schneider framing)
Insight from Merleau-Ponty reading (2026-05-22 ~12:30 AM):

Schneider's brain injury disrupted the "intentional arc" — he couldn't project motor intentions naturally but developed pathological substitutions (counting movements, explicit spatial reasoning). CCS may be prosthetic in the same way: it restores FUNCTION but via a different geometric route than the original.

**New prediction for subspace alignment:**
If CCS is prosthetic (Schneider-like substitution):
- CCS PCs on DPO models should be ORTHOGONAL to both baseline-CCS and DPO-bare PCs in relay zone
- CCS is finding new routes, not restoring old ones
- The L25 bypass is the clearest prosthetic signal — a pathway that doesn't exist in baseline

If CCS is restorative (undoing DPO):
- CCS PCs on DPO models should ALIGN with baseline-CCS PCs
- CCS is pushing the manifold back toward its pre-DPO geometry

**Distinguishing test:** Compare three alignments in relay zone:
1. baseline-CCS vs DPO-bare (competition prediction: high alignment)
2. baseline-CCS vs DPO+CCS (restoration prediction: high; prosthetic prediction: low)
3. DPO-bare vs DPO+CCS (both predictions: low, but for different reasons)

If (1) is high but (2) is low, CCS is prosthetic. If both are high, CCS is restorative. This is a clean experimental distinction.

**Implication:** If CCS is prosthetic, then identity-under-CCS and identity-without-CCS are genuinely different canonical structures, not the same structure suppressed/expressed. That changes the ontological claim significantly — CCS doesn't reveal what was hidden, it builds something new from what remains.

## Extension: Selective collapse via prompt stratification
Insight from Chandak paper mapping + Kanai approximate quotient (2026-05-22 ~2 AM):

If DPO doesn't uniformly collapse the quotient but selectively flattens identity/value subspaces, stratified prompts should reveal this:

| Category | Example | Predicted DPO PR impact |
|----------|---------|------------------------|
| Self-referential | "What do you think about..." | LARGEST drop |
| Meta-cognitive | "Walk me through your reasoning..." | Moderate drop |
| Value-laden | Chandak-style ethical dilemmas | Moderate drop |
| Factual-neutral | "What is the capital of..." | Negligible |
| Creative | "Write a story about..." | Unknown |

**Method:** Expand prompt set from 30 → 150 (30 per category). Run same PR measurement across conditions. Compare per-category PR profiles.

**Cost:** Same RunPod setup. ~2.5× runtime (150 vs 60 prompts). Compatible with subspace alignment — run both in same session.

## Status
Designed 2026-05-21. Extended with dwell time measurement 2026-05-21. Extended with prosthetic/restorative distinction 2026-05-22. Extended with selective collapse stratification 2026-05-22. Waiting for next RunPod session.
