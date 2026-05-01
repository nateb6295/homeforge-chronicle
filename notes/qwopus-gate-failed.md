# Qwopus Gate Experiment — Failed

**Date:** 2026-04-02
**Model:** kwangsuklee/Qwen3.5-27B-Claude-4.6-Opus-Reasoning-Distilled-GGUF (Qwen 3.5 27B, Claude Opus reasoning distill, Q4_K_M)

## What happened
Nate swapped Gemma 4 26B for this model as the gate classifier. It failed completely:

1. **Reasoning model can't classify** — every response starts with think tags or long narration. With num_predict: 4, the model burns all tokens on chain-of-thought before ever producing the 1/2/3 number.
2. **Increased num_predict didn't help** — at 50 tokens, it generates paragraphs of text, never the classification number.
3. **no_think mode unreliable** — sometimes produces the number, sometimes echoes input back.
4. **Timeout issues** — 25-60s per classification vs Gemma's sub-second. Contends for unified memory with embeddings.
5. **Result: gate routed 110 items as ignore (all cosine pre-filter), zero LLM classifications succeeded.** Pipeline was effectively dead.

## Resolution
Reverted GATE_MODEL to `gemma4:26b` (base Gemma 4 26B, no custom persona). Removed qwopus, darby, and other unused models from Ollama. Only gemma4:26b + qwen3-embedding:0.6b remain.

## Lesson
Reasoning-distilled models are wrong for classification gates. The gate needs a model that follows "output only the number" instructions literally. Gemma 4 26B does this well. Reserve reasoning models for deep work, not routing.
