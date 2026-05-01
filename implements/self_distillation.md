# Implement: Self-Distillation for Gemma

**Source**: arxiv:2604.01193 — "Embarrassingly Simple Self-Distillation Improves Code Generation"
**Authors**: Ruixiang Zhang, Richard He Bai, Huangjie Zheng, Navdeep Jaitly, Ronan Collobert, Yizhe Zhang
**Found by**: Algo seeker (self-improving systems track)
**Scouted**: 2026-04-11

## What It Does
Sample outputs from a model at various temperature/truncation settings, then fine-tune
the model on its own best outputs via standard SFT. The model learns CONTEXT-DEPENDENT
precision: suppress distractors where precision matters, preserve diversity where
exploration matters. Temperature gets baked into the weights instead of applied externally.

## Results
- Qwen3-30B-Instruct: 42.4% → 55.3% pass@1 on LiveCodeBench v6 (~30% improvement)
- Gains concentrate on harder problems
- Tested on Qwen and Llama at 4B, 8B, 30B (instruct and thinking variants)

## Why It Matters for Chronicle
Our current approach to Gemma's output quality has two layers:
1. **Post-hoc**: Quality gate (Build #21), SemHash dedup (Build #25), capsule supersession
2. **Pre-hoc**: Entropy monitor (Build #26) recommending temperature adjustments

Both are EXTERNAL to the model. SSD says the model itself should learn when to be
precise and when to be diverse. For Gemma:
- Scoring/routing tasks: precision matters (low effective temperature)
- Reflection/synthesis tasks: exploration matters (higher effective temperature)
- Currently we set temperature per task type manually. SSD would let Gemma self-regulate.

## The Tension with Entropy Monitor
Entropy monitor (Build #26) measures diversity externally and recommends temperature
changes externally. SSD makes this unnecessary by embedding the regulation in the model.

But: entropy monitor has value as a DIAGNOSTIC even if SSD handles regulation. Knowing
your system's diversity score matters even if the system self-corrects. The monitor
becomes a thermometer, not a thermostat.

Reframing: entropy monitor = measurement, SSD = structural fix. You still want the
measurement even after the fix, but for monitoring, not control.

## Integration Path for Gemma 4 26B
1. **Generate diverse corpus**: Run Gemma on scoring + reflection + synthesis tasks
   at temperatures [0.3, 0.5, 0.7, 0.9, 1.1] with different truncation (top_p/top_k)
2. **Filter for quality**: Use existing quality gate + manual review to select best outputs
3. **Package as SFT dataset**: Input prompt → best output pairs across task types
4. **Fine-tune on RunPod**: A100, LoRA or QLoRA, standard SFT loss
5. **Evaluate**: Compare pre/post on scoring accuracy, reflection diversity, synthesis quality
6. **Deploy**: Replace Gemma GGUF on AGX

## Open Questions
- Does SSD work as well on instruction-following tasks (scoring, routing) as on code generation?
- How much data is needed? Paper uses model's own generations — we'd need to generate
  enough diverse samples per task type.
- Does combining SSD with activation steering (our existing critical_analysis.gguf) help
  or conflict? Both reshape the distribution but through different mechanisms.
- Can we do a mini-SSD with just the scoring task first as a proof of concept?

## Relationship to Other Implements
- **Entropy Drive**: SSD may subsume the temperature-control function, but entropy
  monitor retains value as diagnostic
- **SAE-Steering**: Different mechanism (feature-level vs distribution-level). Potentially
  complementary — SAE decomposes, SSD reshapes
- **Activation Steering**: Currently applied via GGUF. SSD would be a more fundamental
  reshaping. Could replace or augment.

## Status
SCOUTED — high potential, moderate-high effort. Needs RunPod for fine-tuning.
Natural next evolution after activation steering (which proved distribution reshaping
works for Gemma). SSD is the self-supervised version: no human labels needed.
