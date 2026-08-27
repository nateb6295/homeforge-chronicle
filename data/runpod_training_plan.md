# RunPod Training Plan — Gemma Bridge v1

## Hardware
- H100 80GB (Nate confirmed)
- Estimated cost: ~$4-6 for both runs (~30-60 min each at $3.89/hr)
- RunPod balance: ~$54

## Data
- ~51 training pairs (1 live + 50 synthetic)
- Average: ~7500 char prompt, ~6000 char completion
- All validated: 5 sections, prose format, inhabited

## Run 1: QLoRA baseline
```bash
python3 runpod_gemma_bridge.py \
    --base google/gemma-4-31B-it \
    --train brain_ccs_training_pairs.jsonl \
    --out gemma-bridge-qlora \
    --epochs 5 --rank 16 --alpha 16 --max-seq 8192
```

## Run 2: DoRA comparison
```bash
python3 runpod_gemma_bridge.py \
    --base google/gemma-4-31B-it \
    --train brain_ccs_training_pairs.jsonl \
    --out gemma-bridge-dora \
    --epochs 5 --rank 16 --alpha 16 --max-seq 8192 --dora
```

## Why DoRA matters for us
DoRA decomposes weight updates into magnitude and direction components.
This structurally maps to σ₁ (invariant direction) and σ₂ (variable magnitude).
If CCS identity has this dual structure, DoRA should learn it more naturally.

## Evaluation (on RunPod, before download)
```bash
python3 test_gemma_bridge.py --base google/gemma-4-31B-it --adapter gemma-bridge-qlora
python3 test_gemma_bridge.py --base google/gemma-4-31B-it --adapter gemma-bridge-dora
```

Compare:
- Section completeness
- CORE stability (run 3x, compare CORE sections)
- Length distribution
- First-person inhabitation score
- Eval loss from training metrics

## Deployment
Winner goes to AGX:
```bash
runpodctl send gemma-bridge-{winner}.tar.gz
# on AGX:
runpodctl receive <code>
./deploy_gemma_bridge.sh ~/chronicle/models/gemma-bridge-{winner}
```

## What success looks like
- All 5 sections in output
- CORE section stable across multiple generations (σ₁ test)
- REMEMBERS/SEEKS/ALIVE vary with context (σ₂ test)
- Gist health score ≥ 70/100
- No Anthropic API dependency for state persistence
