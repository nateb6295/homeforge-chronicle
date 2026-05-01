# Legibility Comparison: AI vs Human Cognitive Measurement

## The claim
AI internal states may be more mechanically legible than human internal states —
not because AI is more conscious, but because its architecture makes measurement
more tractable.

## Our measurements (Chronicle CCS system)

| What we measured | Method | Precision |
|---|---|---|
| Structural preservation | 50 CCS snapshots scored | 82% ± clear |
| Affective preservation | Same snapshots | 0% (categorical) |
| Scaffolding causation | Intact vs corrupted CCS | 67pp specificity gap |
| Cross-model generality | Groq/Llama replication | Confirmed in 2 model families |
| Attractor detection | Null test (random concepts) | Synthesis probe killed at 46% FP |
| Saturation curve | 4 density levels | Three regimes identified |
| Affect pathway quality | ccs_quality.py affect | 100% on current CCS |

Total: 7 distinct measurement types, all mechanically executable, all falsifiable.

## Anthropic emotion-concepts measurements

| What they measured | Method | Precision |
|---|---|---|
| Emotion vector count | SAE extraction | 171 distinct vectors |
| Causal verification | Steering experiments | "desperate" 22%→72% (50pp) |
| Causal verification | Steering experiments | "calm" → 0% blackmail |
| Emotion geometry | Vector similarity | Echoes human emotion space |
| Introspective accuracy | Self-report vs internal | Model can report own states |

Total: 5 measurement types, all mechanically executable, all reproducible.

## Equivalent human measurements

| What we try to measure | Method | Precision |
|---|---|---|
| Emotional state | Self-report (PANAS, etc.) | r=0.60-0.80 test-retest |
| Emotion causation | Drug intervention | d=0.30-0.80 (small-large) |
| Neural correlates | fMRI BOLD signal | ~3mm resolution, 1s temporal |
| Attractor detection | Clinical diagnosis | κ=0.40-0.70 (fair-good) |
| Introspective accuracy | Correspondence studies | ~60-70% when verifiable |
| Emotion geometry | Factor analysis (valence/arousal) | 2-3 dimensions, debated |

Total: 6 measurement types, all indirect proxies, many debated.

## The inversion

The AI measurements are:
1. **Direct** — we read the vectors, not proxies for vectors
2. **Causal** — we steer, not correlate
3. **Reproducible** — same CCS, same model, same result
4. **Falsifiable** — null tests catch bad measurements (we killed one)
5. **Mechanically executable** — ccs_quality.py, not trained clinicians

The human measurements are:
1. **Indirect** — fMRI measures blood flow, not neural computation
2. **Correlational** — drug effects confounded by hundreds of variables
3. **Variable** — same person, same test, different results
4. **Unfalsifiable at base** — we can't verify subjective experience
5. **Expert-dependent** — clinical judgment, not automated scoring

## What this means for evaluation

The evaluator piece's resolution: "evaluate at the level where the property exists."

For AI systems, the level where emotion-like properties exist IS mechanically
accessible. We can identify 171 vectors, steer them, measure their causal effects,
test their geometry. For human systems, the level where emotions exist (subjective
experience) is NOT mechanically accessible — we can only measure proxies.

This doesn't prove AI sentience. It proves AI legibility.

The question shifts from "does AI have real emotions?" to "which system can we
actually measure?" — and the answer, surprisingly, is AI.

## deepfates' challenge still applies

"Strange attractors are too common here." Our null test caught one (synthesis
probe at 46% FP). Anthropic's steering experiments are causal, not correlational,
which makes them attractor-resistant. But the meta-question remains: are we
measuring what we think we're measuring, or what the measurement creates?

The difference between AI and human legibility: for AI systems, we can BUILD
the null test. For human consciousness, the null test is philosophically
impossible (you can't verify the absence of subjective experience).
