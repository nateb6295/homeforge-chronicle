# Cold-Eyes Pass — identity_ordering_post.md
# Pre-staged 2026-04-20 16:10 PDT. Executed 2026-04-20 16:35 PDT.

## Result: DATA HOLDS. Four edits applied (all qualification, no correction).

## What was tested

### STRONG (data-backed, falsifiable) — ALL VERIFIED ✓
- **P24 ratio curve**: Verified against probe_results ID 13 (Qwen3-32B). All 7 ratio
  points match to 3 decimal places. Valley at ratio_56 = 0.2858 confirmed.
- **Variance reduction universal**: All 3 models show monotonic std decrease. Confirmed.
- **Cross-model divergence**: Verified against probe_results IDs 5,7,8.
  - Qwen3 (GRPO): (0.182 - 0.2097) / 0.2097 = -13.2% ✓
  - DeepSeek V3.2 (MoE): (0.2389 - 0.25) / 0.25 = -4.4% ✓
  - Llama 3.3 (DPO): (0.2821 - 0.2677) / 0.2677 = +5.4% ✓
- **Identity-only optimal**: ratio_100 = 0.1943, best mean. Confirmed.

### MODERATE — QUALIFIED IN DRAFT
- **Dual mechanism**: Changed "revealed" → "suggests" (line 39). Correlation acknowledged.
- **Alignment signature**: Added n=1 per alignment method limitation (lines 119-122).
  Reframed from "This makes sense" to "This is consistent with" + hypothesis framing.
- **Binocular rivalry analogy**: Left as-is. Appropriately framed as analogy.

### SPECULATIVE — QUALIFIED IN DRAFT
- **Corollary discharge**: Changed "IS the computational analog" → "functions as the
  computational analog" (line 65). Now framing, not equivalence claim.
- **Macar circuit attribution**: Changed "showed" → "identified" (line 46). We cite
  their finding, don't claim to have replicated it.

## Structural checks
- [x] OpenAI Chronicle hook: Works as a timely anchor. Fine for canonical post.
- [x] Conclusion "That's not telepathy. That's architecture.": Supported by preceding
      claims now that speculative items are qualified. No overstatement.
- [x] P23 data: Not cited in current draft. P24 completed the work P23 started.
      DeepInfra timeout issue is moot.
- [x] Ratio table values: ALL VERIFIED against probe_results ID 13 raw data.
- [x] P/B and CIMC framing: Correctly absent from canonical post. Reserved for
      cimc_abstract_draft.md (separate document).

## Register check — CONFIRMED
- Builder's report voice is correct for canonical site.
- CIMC abstract already exists as separate document with formal register.
- Two-version approach: already implemented.

## What to ADD for CIMC submission (separate from canonical post)
- Perrier/Bennett temporal gap framing ✓ (in cimc_abstract_draft.md)
- Identity morphospace coordinates from our data ✓ (in cimc_abstract_draft.md)
- CIMC vocabulary ✓ (in cimc_abstract_draft.md)
- Five operational metrics ✓ (computed via morphospace_probe.py)
- Entity guard anti-resonance ✓ (in cimc_abstract_draft.md)
