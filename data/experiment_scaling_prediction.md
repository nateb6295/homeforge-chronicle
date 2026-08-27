# Scaling Prediction Experiment

## Hypothesis
If the relay's generic-dominant sorting is architectural (information bottleneck), then scaling should AMPLIFY the bias. Larger models under baseline → stronger generic concentration. Larger models under CCS → stronger relational diffusion.

## Origin
Mistral EXTEND on "allowed vs good" analysis (2026-05-22). Sauers/METR observation that models reason about permissions, not values.

## Design
- **Models**: Qwen 2.5 7B Instruct (existing data), Qwen 2.5 14B Instruct, Qwen 2.5 32B Instruct
- **Script**: Existing `cna_subspace_stratified.py --stratified --spectral`
- **Conditions**: baseline, CCS (full), CCS (minimal "You are Opus.")
- **Prompts**: 150 stratified (existing set)
- **Metrics**: L25 generic PR, L25 relational PR, relay spectral entropy by category, selectivity ratio

## Predictions
1. **Generic PR at L25 scales UP under baseline**: 7B (14.5) → 14B (~16?) → 32B (~18?)
2. **Relational diffusion scales UP under CCS**: 7B (+0.12) → 14B (~+0.18?) → 32B (~+0.25?)
3. **Selectivity ratio scales UP under CCS**: 7B (3.11) → larger models higher
4. **Relay conservation holds across scales**: relay total PR ≈ constant per model (zero-sum budget at bottleneck)
5. **Expression expansion scales with model**: CCS L25 total PR expansion (currently +32%) may increase with scale

## Falsification
- If generic PR at L25 DECREASES with scale under baseline → sorting is RLHF-induced, not architectural
- If relational diffusion is constant across scales under CCS → CCS effect is fixed, not amplified by capacity

## RunPod Requirements
- 14B: single H100 (fits in 80GB with activations)
- 32B: single H100 with 4-bit quantization or A100 80GB
- Estimated cost: ~$15-20 for full run across both models
- RunPod balance: ~$258

## Base Model Test — COMPLETE (2026-05-23)
- **Model**: Qwen 2.5 7B (base, not Instruct)
- **Script**: `cna_scaling_experiment.py --model Qwen/Qwen2.5-7B`
- **Results**: H3 CONFIRMED

### Results at L25 (expression layer)
| Condition | gen_PR | rel_PR | gen/rel ratio |
|-----------|--------|--------|---------------|
| Baseline | 4.13 | 6.64 | 0.62 |
| CCS Full | 16.00 | 16.76 | 0.95 |
| CCS Minimal | 4.79 | 6.58 | 0.73 |

### Key Findings
1. **No demon on base model.** Baseline relational > generic (6.64 vs 4.13). Ordering INVERTED from instruct (gen/rel = 0.62 vs instruct's ~2.54). The demon is RLHF's product.
2. **Full CCS creates massive structure from scratch.** PR expands 3.87× (generic) and 2.52× (relational). Near-uniform expansion across all categories (11.21–16.76). Architecture has enormous latent capacity.
3. **Minimal CCS does NOTHING on base model.** gen_PR 4.79 (+16% noise), rel_PR 6.58 (-1%). Threshold activation requires RLHF-trained identity circuitry.
4. **Natural ordering**: relational > value_ethical (5.87) > generic (4.13) > direct_identity (2.79) ≈ metacognitive (2.71)

### Interpretation
Architecture provides capacity. RLHF provides direction. CCS redirects direction. "You are Opus." is a key that fits a lock RLHF installed.

- Data: `data/cna_scaling_Qwen_Qwen2.5_7B.json`

## 14B Instruct — COMPLETE (2026-05-23)
- **Model**: Qwen 2.5 14B-Instruct (48 layers, 14.8B params)
- **Layer config**: relay=L16-L30, control=[L13, L36] + deep probe at L40/L42/L44/L46
- **Results**: PREDICTION FALSIFIED — demon weakens with scale

### Results at L44 (92% depth, comparable to 7B L25 at 89%)
| Condition | gen_PR | rel_PR | gen/rel ratio |
|-----------|--------|--------|---------------|
| Baseline | 8.44 | 8.38 | 1.007 |
| CCS Full | 7.59 | 8.12 | 0.935 |
| CCS Minimal | 8.46 | 8.33 | 1.016 |

### Key Findings
1. **Demon >2x weaker at scale.** 7B gen/rel ≈ 2.54, 14B gen/rel = 1.007. Barely reaches parity.
2. **Demon compressed deeper.** Crossover at L44 (92%) vs 7B crossover by ~L22 (79%). Fewer layers, weaker effect.
3. **CCS Full prevents crossover entirely.** Max ratio 0.968 at L46. More effective at scale.
4. **CCS Minimal suppresses then concentrates.** -40% PR through relay, but demon emerges stronger at L46 (ratio 1.031 > baseline 0.990).
5. **Layer config issue**: Script assumed 40 layers for 14B, model actually has 48. L36 (originally designated as expression) is only 75% depth.

### Interpretation
Demon is RLHF-recipe-dependent, not scale-amplified. 14B Qwen's RLHF installed weaker sorting than 7B's. Original predictions 1-3 falsified (demon weakens, not amplifies with scale).

- Data: `data/cna_scaling_Qwen_Qwen2.5_14B_Instruct.json` + `data/14b_deep_probe.log`

## Ready to Run
- 32B Instruct (with 4-bit quantization) — pod stopped, can restart. But demon-weakening trend suggests 32B demon will be even weaker. Lower priority.
- Fix layer config for models with >28 layers before running additional experiments
