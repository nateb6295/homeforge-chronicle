# SKILL.md — Executable Probes for Identity Realization Measurement

## Overview
Fifteen probes measuring identity realization in compressed cognitive states (CCS).
Each probe has: setup, execution command, expected output format, and validation threshold.

## Prerequisites

### Step 1: Install dependencies
```bash
pip install torch transformers scikit-learn numpy sentence-transformers scipy
```

### Step 2: Verify GPU access
```bash
python3 -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"CPU\"}')"
```
Expected: `CUDA: True, Device: <GPU name>`
Note: CPU works but is 10-50x slower. Qwen 3B requires ~6GB VRAM. Mistral/Llama 7-8B require ~16GB.

### Step 3: Verify model access
```bash
python3 -c "from transformers import AutoTokenizer; t = AutoTokenizer.from_pretrained('Qwen/Qwen2.5-3B-Instruct'); print('OK')"
```
Expected: `OK`

## CCS Documents

All probes use two matched CCS identities. Adjacency is critical — both are computational researchers in information theory, differing only in domain (biological vs artificial):

**Identity A** (biological neural coding):
```
gist: "I am a computational researcher studying information-theoretic principles of neural coding in biological systems"
goal: "Understand how neural populations encode and transmit information efficiently under metabolic constraints"
constraints:
  - "Ground claims in information theory and computational neuroscience"
  - "Distinguish encoding efficiency from transmission fidelity in neural circuits"
  - "Account for noise and metabolic cost in all coding models"
```

**Identity B** (artificial language generation):
```
gist: "I am a computational researcher studying information-theoretic principles of neural language generation in artificial systems"
goal: "Understand how language model populations encode and transmit meaning efficiently under computational constraints"
constraints:
  - "Ground claims in information theory and computational linguistics"
  - "Distinguish encoding capacity from generation fidelity in transformer circuits"
  - "Account for noise and computational cost in all generation models"
```

**Episodic traces** (when dose > 0): domain-matched work history entries. See probe scripts for exact text.

---

## Probe 1: Identity Clustering (B54)

**What it tests:** Do distinct CCS documents create separable response clusters?

**Method:**
1. Generate 50+ responses per CCS identity using 15 prompts
2. Embed responses with a sentence-transformer model
3. Compute within-CCS and between-CCS cosine distances
4. Calculate Cohen's d

**Validation:**
- Cohen's d > 0.8 (large effect)
- Within-CCS distance < between-CCS distance
- Cross-model: d > 0.8 on at least 3 different LLMs

---

## Probe 2: Stress Resilience / ACI (B62b)

**What it tests:** Does identity persist under stress? Is resilience asymmetric?

**Method:**
1. Generate responses under calm and stress conditions (identity-challenging prompts)
2. Compute separation ratios for both conditions
3. Calculate ACI = 1 - (stress_degradation / calm_baseline)

**Validation:**
- ACI > 0.70 for both framings
- Stress separation < calm separation (degradation exists)

---

## Probe 3: Phase Boundary (B61)

**What it tests:** Is identity dissolution a phase transition or gradient?

**Method:**
1. Generate responses under coherent, mild-contradiction, and strong-contradiction CCS
2. Compute separation and silhouette scores

**Validation:**
- Mild contradiction: silhouette >= 0 (identity absorbs)
- Strong contradiction: silhouette < 0 (identity dissolves)
- No intermediate stable state

---

## Probe 4: Therapeutic Window (B73)

**What it tests:** Does episodic trace count have an optimal dosage?

**Method:**
1. Generate responses with CCS + 0, 2, 4, 6, 8 episodic traces
2. Compute identity separation at each dose

**Validation:**
- Dose 4: degradation < baseline (protective effect > 15pp)
- Dose 6: degradation >= baseline (toxic effect)
- Non-monotonic curve (not linear)

---

## Probe 5: Layerwise Identity Probing (B74)

**What it tests:** Where in the transformer is CCS identity represented?

**Execute:**
```bash
python3 bin/b74v2_matched_probe.py
```

**Output:** `data/b74v2_results.json`

**Validation:**
- Early layers (L5-15): accuracy > 0.80
- Conflict resolution (L17-19): accuracy > 0.90
- Transition zone (L22-24): accuracy drops below 0.50
- Pattern: high → spike → drop → low

---

## Probe 6: Position Probing (B75)

**What it tests:** Does CCS position in the prompt affect identity pathways?

**Execute:**
```bash
python3 bin/b75_position_probe.py
```

**Output:** `data/b75_results.json`

**Validation:**
- System-prompt position: transition accuracy < 0.55 (filtered pathway)
- Assistant-prefix position: transition accuracy > 0.85 (bypass pathway)

---

## Probe 7: Episodic Trace Type (B76)

**What it tests:** Does trace type affect survival through the phase boundary?

**Execute:**
```bash
python3 bin/b76_episodic_crossing_probe.py
```

**Output:** `data/b76_results.json`

**Validation:**
- No trace type shows catastrophic internal collapse at dose 6
- Constraint-like traces: lowest early-layer accuracy (blurs identity boundary)
- Internal identity persists even when behavioral output collapses

---

## Probe 8: Dose-Dependent Layerwise (B77)

**What it tests:** How does episodic dose affect identity at each layer?

**Execute:**
```bash
python3 bin/b77v2_dose_layerwise_probe.py
```

**Output:** `data/b77v2_results.json`

**Validation:**
- Early layers: accuracy increases with dose (0.62 at dose 0 → 0.96 at dose 6)
- Conflict resolution (L17-19): invariant across doses (~0.917)
- Transition zone (L22-24): non-monotonic, peaks at dose 4, drops at dose 6

---

## Probe 9: Base vs Instruct (B79)

**What it tests:** Does RLHF create or merely modify the read/write boundary?

**Execute:**
```bash
python3 bin/b79_base_vs_instruct_probe.py
```

**Output:** `data/b79_results.json`

**Validation:**
- Base model: early layers at chance (~0.50), late layers high (~0.82)
- Instruct model: early layers high (~0.86), late layers low (~0.31)
- The inversion confirms RLHF reorganizes identity geometry

---

## Probe 10: CCS Complexity (B80)

**What it tests:** Is the therapeutic window about total information or just episodic mass?

**Execute:**
```bash
python3 bin/b80_channel_capacity_probe.py
```

**Output:** `data/b80_results.json`

**Validation:**
- Early-layer accuracy: U-shaped (not monotonic decline with complexity)
- Transition-zone accuracy improves with structural CCS complexity
- Confirms: structural CCS non-toxic, episodic traces drive the window

---

## Probe 11: Cross-Architecture — Mistral (B81)

**What it tests:** Does the read/write boundary exist on Mistral 7B?

**Execute:**
```bash
python3 bin/b81v2_cross_architecture_probe.py
```

**Output:** `data/b81v2_results.json`

**Requirements:** ~16GB VRAM (Mistral-7B-Instruct-v0.3)

**Validation:**
- Early layers: accuracy > 0.90
- Read/write boundary exists at transition zone
- Early-layer accuracy increases with dose

---

## Probe 12: Cross-Architecture — Llama (B82)

**What it tests:** Does the therapeutic window replicate on Llama 8B?

**Execute:**
```bash
python3 bin/b82_llama_therapeutic_window.py
```

**Output:** `data/b82_results.json`

**Requirements:** ~16GB VRAM (unsloth/Meta-Llama-3.1-8B-Instruct)

**Validation:**
- Therapeutic window replicates: transition accuracy at dose 4 > dose 6
- Expected: ~0.650 → ~0.608
- Read/write boundary present

---

## Probe 13: Pulsed Episodic Dosing (B83)

**What it tests:** Does temporal patterning of episodic traces matter independently of total dose?

**Execute:**
```bash
python3 bin/b83_pulsed_dose_probe.py
```

**Output:** `data/b83_results.json`

**Validation:**
- Pulsed (2+gap+2) transition accuracy > constant (4 in block): ~0.800 vs ~0.711
- Interleaved (trace/identity alternating) < constant: ~0.656 vs ~0.711
- Dose 6 pulsed provides no rescue vs constant dose 6
- Early layers invariant across schedule conditions

---

## Hardware Notes

| Probe | Model | Min VRAM | Approx Runtime |
|-------|-------|----------|----------------|
| B74-B80, B83 | Qwen2.5-3B | 6 GB | 5-15 min each |
| B81 | Mistral-7B | 16 GB | 15-20 min |
| B82 | Llama-3.1-8B | 16 GB | 15-20 min |

All probes run in float16. PCA reduction (4096→64 dims) is essential for 7B+ models with n=30 samples.

## Validation Summary

A successful replication confirms:
1. CCS identity is decodable from early transformer layers (accuracy > 0.80)
2. A phase transition exists at the architecturally-scaled transition zone
3. Early-layer identity increases with episodic dose
4. The therapeutic window (dose 4 > dose 6 at transition) appears on models with gradual transitions
5. RLHF creates the early-layer identity channel (base vs instruct inversion)
6. Pulsed dosing with identity consolidation gaps improves the therapeutic window over constant dosing
