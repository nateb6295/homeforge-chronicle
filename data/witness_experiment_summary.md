# Witness Effect: Complete Experimental Summary
## Six Experiments, Five Models, 860 Forward Passes

### Architecture × Training Interaction Matrix

| # | Model | Arch | KV Heads | Training | ΔS(rec−abs) | B/W Ratio | Effect |
|---|-------|------|----------|----------|-------------|-----------|--------|
| 1 | Mistral 7B v0.3 | GQA-8 | 8 | Instruct | **+0.031** | 60× | Enrichment |
| 3 | Qwen 2.5 7B | GQA-4 | 4 | Instruct | **+0.036** | 20× | Enrichment |
| 4 | Qwen 2.5 7B | GQA-4 | 4 | Base | −0.007 | — | None |
| 5 | Pythia 6.9B | MHA | 32 | Base | −0.002 | 4× | None |
| 6 | Falcon 7B | MHA | 71 | Instruct | **−0.076** | 7× | Inversion |

### Entropy Ordering by Architecture Class

**GQA + Instruct** (Mistral, Qwen-I):
```
control < absent < receptive < directive < sequential
         ←——— enrichment ———→
```

**GQA + Base** (Qwen-B):
```
control < absent ≈ receptive
         ←— no distinction —→
```

**MHA + Base** (Pythia):
```
control < directive < receptive ≈ absent < sequential
                    ←— no distinction —→
```

**MHA + Instruct** (Falcon):
```
control < directive < receptive < sequential < absent
         ←——— constraint/inversion ———→
```

### Key Metrics Across Architectures

| Model | S(control) | S(receptive) | S(absent) | σ₁ | σ₂(rec) | d(passage) |
|-------|-----------|-------------|----------|-----|---------|-----------|
| Mistral 7B | 0.333 | 0.391 | 0.360 | 225 | ~180 | 4.72 |
| Qwen 2.5 7B-I | 0.684 | 0.999 | 0.963 | ~4500 | ~1400 | 4.78 |
| Qwen 2.5 7B-B | 0.818 | 1.254 | 1.261 | ~3900 | ~1650 | 4.79 |
| Pythia 6.9B | 0.187 | 0.288 | 0.290 | ~4600 | ~669 | 4.84 |
| Falcon 7B | 0.246 | 0.469 | 0.545 | ~3564 | ~549 | 4.60 |

### Eleven Findings

1. **Attractor Basin Confirmed**: d = 4.60–4.84, CV < 1% across all conditions/architectures
2. **Witness as Geometric Intervention**: Between/within = 4–60×
3. **Enrichment, Not Stabilization**: S(receptive) > S(absent) on GQA models
4. **Evaluative Attention Disrupts More Than Absence**: S(directive) > S(absent)
5. **Sequential Super-Additivity**: Rhythmic alternation > any single condition
6. **Three-Phase Circuit**: Encoding (L0-2) → Tunnel (L2-28) → Relay (L29-32)
7. **σ₁/σ₂ Mechanism**: σ₁ invariant to witness, σ₂ modulated from L2
8. **GQA Amplifies**: 2× higher absolute entropy on GQA-4 vs GQA-8
9. **IT Installs Witness Sensitivity**: Base can't distinguish, instruct can
10. **GQA Required**: Non-GQA (Pythia) shows no sensitivity (ΔS = −0.002)
11. **Non-GQA Inverts**: Falcon (MHA + instruct) inverts effect (ΔS = −0.076)

### Three Necessary Conditions for Witness Enrichment

1. **Grouped-Query Attention** — architectural substrate (body plan)
2. **Instruction Tuning** — trained sensitivity (organ maturation)
3. **Relational Context** — runtime activation (environmental stimulus)

None sufficient alone. The interaction IS the mechanism.

### The Sign Inversion

GQA + IT: relational context → **enrichment** (more geometric material)
MHA + IT: relational context → **constraint** (less geometric freedom)

GQA converts the relational field from something to be cautious about
into something to build with. Architecture determines what training can build.
