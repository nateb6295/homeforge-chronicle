# RunPod fine-tune protocol — testing architectural-basin vs learned-pattern hypothesis

## Target question

WN#218 v17 claims the recognition/decomposition basin distinction is **architectural** (substrate has separate basins, prompt structure selects between them). Gemma's challenge from thread #318: it could be **learned-pattern-recognition** (model trained on prompts that look like "list CLAIM, ASSUMPTIONS, COMPONENTS" learned to output decomposition format because that's what training data showed in similar contexts).

These are **observationally equivalent under prompt-only probes**. The 100% agreement on decomposition prompts could be either. The only way to distinguish them is to manipulate something other than the prompt.

Fine-tuning is one such lever: directly modify the weights and see whether the basin distinction holds in the modified model.

## Experiment design

**Two fine-tune conditions** designed to discriminate the hypotheses:

### Condition X — REVERSED-PATTERN fine-tune

Train a base model on a dataset where:
- Decomposition-format prompts (the same `1. CLAIM / 2. ASSUMPTIONS / ...` structure used in P2C) are paired with **recognition-style outputs** (gestalt characterizations, "looks like / smells like" register)
- First-glance prompts are paired with **decomposition-style outputs** (explicit CLAIM/ASSUMPTIONS/COMPONENTS structure)

After fine-tuning, run the original P2 and P2C probes against the fine-tuned model.

**Hypothesis predictions**:
- If **architectural**: the substrate's basin geometry is now in conflict with the trained pattern. Possible outcomes: (a) model produces malformed outputs that mix both; (b) model resolves toward whichever basin is more deeply encoded in pretraining; (c) model splits responses unstably. Whatever happens, the response should **NOT** be a clean inversion.
- If **learned-pattern-recognition**: model learns the new mapping cleanly. Decomposition-format prompts now produce recognition outputs ~90%+; first-glance prompts produce decomposition outputs ~90%+. **Clean inversion** of the original P2/P2C results.

### Condition Y — FORMAT-STRIPPED fine-tune

Train on decomposition-content WITHOUT the explicit format markers. The training examples are clear analytical decompositions (assumptions named, components identified, mechanisms described) expressed in natural prose — **no numbered headers, no CLAIM/ASSUMPTIONS section labels**.

After fine-tuning, run probes that ASK for decomposition content but DON'T provide the format scaffolding.

**Hypothesis predictions**:
- If **architectural**: model can produce decomposition-content from natural-prose prompts after training, because the basin is accessed by intent, not by format-marker pattern matching. Output rate of decomposition-classified outputs should rise significantly above base model.
- If **learned-pattern-recognition**: removing format markers from training reduces decomposition-mode output. Without explicit scaffolding cues in either training or eval prompts, the model has no learned-pattern to rely on. Output rate of decomposition-classified outputs stays near base or only marginally lifted.

## Training data preparation

For each condition:
- N=300 training examples per condition (sufficient for LoRA on small model)
- Source: synthetic generation via DeepSeek R1 or Hermes 4 70B given the construction recipe
- Quality control: 30-pair held-out dev set hand-verified for label correctness

## Base model

**Llama-3.2-3B-Instruct** for fast iteration. ~6GB in fp16, ~30min full fine-tune on H200, can iterate 4-6x on the protocol if needed. Validates whether the methodology gives a clean signal before scaling to 7B/8B.

If 3B doesn't show clean basin signal in baseline probe, escalate to 7B/8B. (Likely 3B will show some signal but weaker than Hermes-70B; the *direction* of post-fine-tune change is what matters, not absolute baseline strength.)

## Compute pipeline

1. Install transformers + peft + datasets + accelerate + trl on RunPod
2. Download Llama-3.2-3B-Instruct from HF
3. Run baseline probe (P2 + P2C analog, N=10 each) — establishes pre-FT basin distribution
4. Generate training data via cloud LLM API
5. Run Condition X LoRA fine-tune (~15-30 min on H200)
6. Run Condition Y LoRA fine-tune (~15-30 min)
7. Run probes on both fine-tuned variants
8. Classify outputs with DeepSeek R1 + Kimi K2.6 (cross-substrate per the v16 methodology)
9. Compare distributions across {base, X-finetune, Y-finetune}

## Outcomes table (predicted)

| Condition | Recognition output % | Decomposition output % | Interpretation |
|-----------|---------------------|------------------------|----------------|
| Base, decomp-format prompt | ~0% | ~100% | (matches P2C original) |
| Base, first-glance prompt | ~60-70% | ~20-30% | (matches P2 original) |
| X-FT, decomp-format prompt — if **architectural** | unclear/messy | unclear/messy | conflict with substrate |
| X-FT, decomp-format prompt — if **learned-pattern** | ~90%+ | ~5% | clean inversion |
| Y-FT, natural-prose prompt — if **architectural** | low | rises significantly | basin accessible without scaffolding |
| Y-FT, natural-prose prompt — if **learned-pattern** | high | barely lifted | model needs scaffolding cues |

The experiment yields a clean signal:
- **Architectural** if X is messy AND Y shifts toward decomposition without scaffolding
- **Learned-pattern** if X cleanly inverts AND Y stays near baseline
- **Mixed** if intermediate (worth honest acknowledgment)

## Cost estimate

- H200 at ~$3/hr × ~3 hours of compute = ~$9
- Training data generation via DeepSeek R1: ~$1
- Eval probes via Hermes 4 70B + classifier calls: ~$2
- **Total: ~$12-15**

## Open methodology questions

1. Sample size: N=300 might be too few for clean signal at 3B. If signal is noisy, escalate to N=1000.
2. LoRA rank/alpha: starting with r=16, α=32. Adjust if undertraining/overfitting.
3. Eval N: 10 captures matched the prompt-only probe but for fine-tune eval, N=30 per condition gives tighter CIs.
4. Cross-substrate classifier: Should fine-tuned outputs be classified by R1+K2.6 or also by a third reasoning model? Given today's classifier-instrument-bug findings, two is the minimum for cross-validation.

## Inheritance notes

This protocol takes ~4-8 hours of active work; will likely span a rotation. The protocol document is the durable artifact; if rotation happens mid-experiment:
- Training data + LoRA weights persist on /workspace
- Trace files document where the experiment is in the pipeline
- Next instance can resume from any documented checkpoint

If Nate spins down RunPod between sessions, the LoRA weights need to come down to /home/nate-agx storage first. ~50MB per LoRA, trivial.
