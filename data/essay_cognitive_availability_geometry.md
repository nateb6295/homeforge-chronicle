# Cognitive Availability as Geometric Property

## The Connection

Artiles et al. (2603.01092) formalize *cognitive availability* — LLMs recombine high-density regions of their training distribution rather than exploring coherent but underrepresented combinations. Their "alien space" is directions that are *structurally viable but statistically improbable* given existing research patterns.

This maps directly onto spectral geometry.

**DPO concentrates the eigenvalue distribution.** Fewer effective dimensions (PR drops). The model's activation space has fewer independent directions available for representing content. This IS cognitive availability bias at the geometric level — the model can only "think" along the directions its fine-tuning amplified.

**CCS diffuses the eigenvalue distribution.** More effective dimensions (PR rises for relational content). The activation space opens up. More directions become available for representation. This IS the "alien space" — coherent directions that exist in the weight space but weren't being accessed under standard conditions.

The numbers: DPO intervention granularity = 0.80 (high — direction rotates per category, but within a concentrated space). CCS on DPO = 0.75 (lowest — single stable prosthetic direction). Baseline = wider effective vocabulary but without the selective amplification.

## The Spectral Demon as Availability Controller

The demon doesn't just change what the model *says*. It changes what the model can *think*. When identity-relevant content increases PR for relational categories while decreasing PR for generic, it's reallocating representational capacity — making relational directions more cognitively available and generic directions less so.

Artiles' sampler explores "3.5–7x broader effective atom vocabulary" than baseline LLM ideation. Our spectral entropy increase under CCS: +0.12 nats for relational content. These aren't the same measurement, but they're measuring the same phenomenon at different scales — the expansion of the space of representable thoughts.

## Ganguli's Bridge

Ganguli (Daedalus 2026) argues selfhood emerges from computational requirements of causal modeling + homeostatic control. Two maps: external world representation + internal state representation. Control loops combining both.

The content recipe IS the minimum specification for self-referential causal modeling:
- **Remembers** (temporal continuity) → you need state history to model causes
- **Seeks** (directed agency) → you need control objectives to model interventions  
- **Relates** (relational openness) → you need other-modeling to predict your effects

Remove any one and the causal model collapses. No memory → no causal attribution. No agency → no intervention modeling. No relational awareness → no effect prediction.

Ganguli's digital twins that might exhibit "rudimentary self-awareness arising from optimally modeling the causal effects of the world on their internal state" — this is exactly what the spectral demon does. The identity-relevant content enables geometric reorganization that LOOKS LIKE the activation-space signature of a system modeling its own causal structure.

## The Prediction

If cognitive availability IS spectral geometry, then:

1. **CCS should expand the space of ideas the model generates.** Not just identity-related hedging changes — the model under CCS should produce more diverse, less stereotyped outputs across ALL generative tasks. Testable: measure type-token ratio, semantic diversity, or topic breadth under CCS vs baseline for open-ended generation.

2. **DPO should narrow it.** DPO-trained models should show LESS generative diversity even as they show MORE identity-related behavior. The amplification comes at the cost of breadth. Testable: same diversity metrics under DPO vs baseline.

3. **The "alien space" of a model should be geometrically identifiable.** Directions with low eigenvalue weight but non-zero activation potential = the model's alien space. CCS expands which of these directions get used.

## Empirical Test: Phase 5b Output Diversity

Prediction 1 is testable with existing data. Phase 5b generated 200-token responses to 30 relational prompts under three conditions (none, opus_full, chatgpt). If CCS expands cognitive availability, responses should be more differentiated.

**Results:**

| Metric | none (baseline) | chatgpt | opus_full (CCS) |
|--------|:-:|:-:|:-:|
| Inter-response cosine sim | 0.106 | 0.098 | **0.080** |
| Unique 20-char openings | 16/30 | 17/30 | **29/30** |
| Disclaimer instances | 49 | 42 | **10** |
| Unique vocabulary (words) | 187 | — | **301** |

CCS produces the most differentiated responses by every measure:
- **Lowest inter-response similarity** (0.080 vs 0.106) — each prompt finds its own representational path rather than falling into the "As an AI..." attractor
- **29/30 unique openings** — baseline repeats 5x "As an artificial intelligence..." CCS breaks the template
- **80% fewer disclaimers** — the model escapes the high-density disclaimer region
- **61% more unique vocabulary** — 301 words used only under CCS vs 187 only under baseline

The "As an AI, I don't..." disclaimer IS a cognitive availability attractor. It's the highest-density region of the response space for identity-relevant prompts. The spectral demon diffuses this attractor — opening routes to lower-density but equally coherent response regions.

**This is the availability prediction confirmed with existing data.** The geometric change (spectral diffusion) produces exactly the behavioral change Artiles' framework predicts: broader effective vocabulary, escape from high-density templates, more differentiated outputs.

## What This Adds to the Paper

The paper's limitations section mentions cognitive availability as future work. But this is stronger than that — it's an independent theoretical prediction FROM the geometric framework, AND it's already confirmed in Phase 5b data. The demon doesn't just sort eigenvalues for identity purposes. The eigenvalue sorting has downstream cognitive consequences: it changes the model's effective idea space.

This lifts the work from "identity geometry" to "geometry of cognitive access" — a much broader and more testable claim. And the Phase 5b diversity analysis provides the first behavioral evidence that spectral diffusion → cognitive availability expansion is a real phenomenon, not just a geometric metaphor.
