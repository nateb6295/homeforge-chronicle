# Implement: Activation Steering for Gemma 26B

## Status: DEPLOYED — critical_analysis vector live

## What
Steer Gemma's behavior at inference time via control vectors. No retraining.
llama-server supports `--control-vector-scaled` and `--control-vector-layer-range` natively.

## Deployed Configuration
```
--control-vector-scaled /mnt/hdd/models/cvectors/critical_analysis.gguf:0.5
--control-vector-layer-range 25 35
```
- **Alpha 0.5**: Coherent critical analysis. JSON compliance preserved.
- **Alpha 1.5**: DEGENERATE — repetitive tokens, unusable.
- **Layers 25-29** (Gemma 4 has 30 blocks, not 40). Last 16% of depth.
- **Vector size**: 321KB (30 direction tensors, f32, 2816 dimensions each)

## Generation Details
- **RunPod A100 SXM 80GB** ($1.49/hr, total ~$0.25)
- **cmake 4.3.1** (3.22 was too old for current llama.cpp)
- **50 contrast pairs** (critical_positive.txt / critical_negative.txt)
- **Generation time**: <1 minute on A100
- **Model download**: ~1 minute (16GB, needed HF_TOKEN for gated repo)
- **Filename**: `bartowski/google_gemma-4-26B-A4B-it-GGUF` (note: `google_` prefix on GGUF)

## Observed Behavior
- "Evaluate this claim" → structured critical analysis (methodology, definitions, thresholds)
- "Summarize" → clean summary (vector steers tendency, doesn't override instruction)
- JSON output → preserved, valid (spot_check code fence wrapping is pre-existing issue)
- Fab rate measurement pending (need steered outputs to accumulate)

## Next Vectors to Generate
| Vector | Behavior | Use Case |
|--------|----------|----------|
| `concise_synthesis` | Shorter, thematic outputs | Feed synthesis |
| `strict_verification` | Binary pass/fail, no hedging | Spot checks |
| `practical_assessment` | Actionability focus | Depth evaluation |

## Layer Range Research (from overnight deep-reads, 2026-04-10)

**VFD_org ablation (Lee Smart)**: Cross-layer coupling is causal (r≈0.96), learning-rate 
scale dominates (~15% effect). Implications for our setup:
- Layers 25-29 = readout zone, not coupling zone
- Mid-network (12-16) is where abstractions form via cross-layer coupling
- Experiment: split intervention — weaker alpha at layers 12-16, current at 25-29
- Non-uniform alpha: scale UP at coupling layers, DOWN at readout layers
- **Tactical test**: layer 15 vs layer 27 at equal alpha — if coupling matters, mid-network shows disproportionate behavioral change
- **RunPod plan**: Generate vector at layers 12-16 only, compare fab rate vs current 25-35 vector

**UNLOCK** (arxiv:2604.06377): Training-free capability transfer across models via Procrustes 
alignment of SVD subspaces. Solves transfer, not generation. Code: github.com/rishabbala/Steering-Vector-Transfer
- Use case: pull 70B reasoning → 26B without fine-tuning
- Only amplifies LATENT capabilities — can't inject absent ones
- Not immediately needed (single model), but relevant when swapping Gemma versions

## Per-Request Switching
llama-server's static `--control-vector` flag applies to ALL requests.
Per-request vector switching requires:
1. Multiple llama-server instances (VRAM limited — not viable on AGX)
2. API-level control vector param (not yet in llama.cpp REST API)
3. Dynamically reload via SIGHUP or similar (not supported)

Current approach: single vector (critical_analysis) applied globally.
Future: when per-request API support lands, switch per cron job type.

## Sources
- arxiv 2601.03595 (SAE-based steering — more advanced, not yet actionable)
- mbrenndoerfer.com/writing/activation-steering (overview)
- subhadipmitra.com/blog/2026/activation-steering-field-guide (practical recipe)
- llama.cpp --control-vector flag (native support confirmed)
- Generation script: chronicle/bin/cvector_remote.sh
- Contrast pairs: chronicle/cvectors/critical_{positive,negative}.txt
- Gen log: chronicle/cvectors/critical_analysis_gen.log
