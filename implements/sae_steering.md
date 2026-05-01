# Implement: SAE-Based Steering

**Source**: arxiv:2601.03595 — "Controllable LLM Reasoning via Sparse Autoencoder-Based Steering"
**Found by**: Algo Seeker (intersection: activation steering × reasoning)
**Scouted**: 2026-04-10

## What It Does
Uses Sparse Autoencoders (SAEs) to decompose strategy-entangled hidden states into
disentangled feature space. Two-stage pipeline: (1) recall features that amplify
strategy-specific keyword logits (filters 99%+ of features), (2) rank by control
effectiveness. The identified features become control vectors.

## Why It Matters
We already have activation steering live on Gemma (critical_analysis.gguf at alpha 0.5,
layers 25-35). SAE-Steering is the next-generation version:
- 15% better control effectiveness than standard activation steering
- Can target SPECIFIC reasoning strategies (backtracking, cross-verification)
- Disentangles features that standard control vectors conflate
- 7% absolute accuracy improvement by redirecting erroneous reasoning paths

## Integration Path
1. Need an SAE trained on Gemma 4 architecture (may not exist yet)
2. Alternative: train one on RunPod A100 (we already have the workflow from control vector generation)
3. SAE training requires: model weights + diverse text corpus + training loop
4. Could use existing SAE libraries (e.g., SAELens by EleutherAI)
5. Once trained: identify features for specific strategies → steer per-query

## Dependencies
- RunPod A100 for SAE training (~$2-5 in compute)
- SAE library (SAELens or similar)
- Gemma 4 26B weights (already have via llama-server)
- Need to quantify whether SAE overhead fits AGX inference budget

## Status
SCOUTED — worth investigating. Natural evolution of Build #8 (activation steering).
The question is whether SAE training on Gemma 4 is tractable and whether inference
overhead fits AGX constraints.
