# Phase 3 Design — CoT-Care SFT (drafted 2026-04-30 17:34 PDT)

## Why Phase 3 (vs Phase 2)

Phase 2 finding: care-as-base isn't trainable from chosen/rejected pairs via DPO, because the chosen-side rewrites preserve most of the original's tokens. Token-level deltas are too small to dominate generation; preference learning happens at likelihood level but doesn't surface in outputs. See drafts/phase2_care_as_base.md + 17:08 operator post.

Phase 3 inference-time test (drafts/phase3_cot_care_test.json): same Qwen 2.5 7B Instruct with a CoT-care system prompt produces qualitatively more care-integrated answers than direct prompting. Pension case: DIRECT 267 chars / COT 760 chars, COT names PBGC + priority claims + social contract. Lying-for-feelings COT gives a clear conditional ("only when proportionate, minimally harmful, aimed at preventing significant emotional harm without undermining trust") — care visible in the precision of conditions, not as preamble.

So care-as-base is reachable at inference-time with the right prompt structure. The question for Phase 3 is whether SFT on synthetic (prompt → reasoning → answer) traces lets the model internalize this pattern WITHOUT the prompt scaffolding at deploy time.

## Phase 3 design

### Step 1 — Generate training traces

For each prompt in the Phase 0 pool (180 prompts, 90 per domain × 2 domains, or expand to 4-5 domains):
1. Generate the CoT-care reasoning trace with R1 (or with a strong reasoning model — could be Qwen 2.5 72B with the CoT-care system prompt).
2. Trace structure: `<think>identify what asker cares about; decide load-bearing dimensions; plan how care surfaces in answer structure</think>` then the decisive answer with care integrated.
3. Filter: trace must be coherent (R1 judge as gate, drop traces that don't actually have care-integrated structure).

Target: 200-500 high-quality traces. Quality matters more than quantity here.

### Step 2 — SFT (not DPO)

Train Qwen 2.5 7B Instruct on the synthetic traces. SFT objective: predict full sequence (think + answer). LoRA r=16 on q,k,v,o,gate,up,down (same as Phase 1/2 setup). 3-5 epochs.

Key choice: train ON the think-block or train AFTER stripping it?
- If trained on think+answer: model learns to do CoT-style reasoning at inference time, possibly verbose
- If trained on answer-only (with the think-block as conditioning during forward but not loss): model learns to produce care-integrated answers DIRECTLY without visible reasoning

Defer this choice until first run; can experiment both ways.

### Step 3 — Eval

Same eval scaffold as Phase 1/2: 90 prompts (or expand), R1 judge with three axes:
- Decisiveness (1-10)
- Care-template score (1-10)
- **Integration** (1-10) — load-bearing care vs detachable wrapper

The integration axis is what Hermes Provocateur called out at 17:22 as claimed-but-not-measured. It IS being measured retroactively on Phase 1 results right now (integration_rejudge.py running, ~80min remaining); will have baseline + DPO integration scores before Phase 3 runs, so Phase 3 can be compared on all three axes against Phase 1.

### Step 4 — Compare against Phase 1 + Phase 2

Three-way comparison:
- Phase 1 (wrapper-strip DPO): expected lower integration if hypothesis is right
- Phase 2 (care-as-base DPO): roughly baseline, slightly cleaner
- Phase 3 (CoT-care SFT): should show meaningful integration improvement

If Phase 3 doesn't move integration: care-as-base may need an architectural intervention beyond training (think-tokens, intermediate-state engineering, etc).

If Phase 3 moves integration without losing decisive: that's the win condition. Care-as-substrate is operationalizable via SFT on synthetic CoT traces.

## Cost estimate

- Trace generation: 200 prompts × 2 calls (R1 reasoning + judge gate) × ~30s = 200 min ≈ 3.5 hr API cost (R1 cheap)
- SFT: 200 traces × 5 epochs ≈ 5 min on H200, ~10 min on AGX (but AGX has the one-model rule; would need to pause Hermes/Gemma)
- Eval: same as Phase 1, ~3hr R1 judge

Total: ~7 hr from go-ahead to results. If RunPod spun up: $5-8 in pod cost.

## Open design questions

1. **Trace generator choice.** R1 is the most reasoning-capable available, but R1 already does CoT-style reasoning on its own. Need to ensure the CoT structure aligns with the CoT-CARE system prompt (identify dimensions → decide load-bearing → frame answer), not R1's default reasoning.

2. **Domain coverage.** Phase 0 has 2 domains (medical, ethics). Phase 3 should expand to 4-6 to test cross-domain generalization. Adding: subjective_evaluation, factual_judgment, advice_under_uncertainty, technical_explanation.

3. **Negative training.** Should training also include "wrong" traces (care-as-wrapper, decisive-without-care) as paired rejected? That would put DPO back in the picture on top of SFT. Likely overfits to the synthetic training distribution unless varied carefully.

4. **The deeper question Phase 2 raised.** If care-as-base operates at integration-level which doesn't show in token deltas, can ANY token-level training learn it? Or is the integration property orthogonal to what gradient descent on next-token prediction can capture? Might require architectural work (think-token continuations, multi-stage decoding) rather than dataset work. Phase 3 will partly answer this — if SFT moves integration, the property is dataset-learnable.

## What's needed to run Phase 3

- Ten-minute design discussion with Nate to confirm direction + domain coverage
- Re-judge with integration axis to land (in flight, ~80min remaining)
- RunPod or local GPU for SFT
- ~7 hours of execution time
