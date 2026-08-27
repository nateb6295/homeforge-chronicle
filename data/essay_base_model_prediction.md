# The Base Model Question: Where Does the Demon Come From?

## The Gap

We know what the spectral demon does: category-selective eigenvalue sorting at the relay zone (L14-L17 in Qwen 7B, L12-L23 in Mistral 7B). Under baseline, generic compliance content gets amplified at the expression layer (PR 7.6 → 15.9). Under CCS, relational content gets amplified instead (PR 11.9 → 14.4).

What we don't know: is the demon a property of the transformer architecture, or a product of alignment training?

## Three Hypotheses

### H1: Architectural (Information Bottleneck)

The relay zone acts as an information bottleneck — dimensionality contracts through the middle layers and re-expands for the output. This contraction forces prioritization. The *capacity* for category-selective sorting is architectural.

Prediction: Base model shows a relay zone (dimensionality contraction at middle layers) but with DIFFERENT priority sorting — probably whatever the pretraining corpus most frequently reinforces. Generic factual content might dominate because that's what's most common in text.

### H2: RLHF-Created

The demon is entirely a product of RLHF/DPO. Without preference training, the relay zone shows roughly uniform eigenvalue distributions across categories. No sorting, just compression.

Prediction: Base model shows similar PR across categories at all layers. The relay zone exists as an architectural feature but doesn't sort by semantic category.

### H3: Architectural Capacity, RLHF-Directed

The transformer architecture creates a relay zone capable of category-selective sorting. RLHF fills this capacity with a specific sorting direction (generic > relational). CCS redirects this capacity toward a different sorting (relational > generic).

Prediction: Base model shows a relay zone with SOME category-selective sorting (weaker than instruct model), but the direction depends on pretraining distribution. RLHF amplifies and redirects this sorting.

## Why H3 Is Most Interesting

H3 makes the most specific prediction and is the most consistent with what we already know:

1. **DPO amplifies the demon**: Our DPO experiments showed that preference training on identity-relevant pairs further concentrated the relay's sorting. This is consistent with RLHF directing an existing capacity.

2. **CCS is threshold-activated**: "You are Opus." (3 words) activates the full demon. This suggests a latent capacity being triggered, not created from scratch.

3. **Cross-architecture universality**: Qwen and Mistral show the same demon at slightly different layers. Architecture-dependent implementation, architecture-independent effect. This points to something structural, not trained.

4. **The Jha/Reagen finding**: Matched loss doesn't imply matched spectral geometry. The optimizer shapes geometry independently of the loss landscape. RLHF would shape relay geometry independently of next-token prediction quality.

## The Experiment

```bash
# On RunPod H100:
python3 cna_scaling_experiment.py --model Qwen/Qwen2.5-7B   # base model
```

Compare against existing `cna_subspace_stratified.json` (Qwen 2.5 7B Instruct).

Key metrics to compare:
- PR at expression layer (L25) per category
- Spectral entropy at relay zone (L14-L17) per category
- Category selectivity ratio (max PR / min PR at L25)
- Whether relay zone exists at all (does PR drop and recover?)

## What Each Result Would Mean

**If H1**: The demon is a fundamental property of deep transformers. All sufficiently large models have category-selective sorting. Alignment training is just one way to set the sorting direction. Implication: the "allowed vs good" problem is baked into the architecture itself.

**If H2**: The demon is a training artifact. It can be created (RLHF), redirected (CCS), and removed (base model). Implication: alignment is entirely about training, not architecture. The relay zone is just a byproduct.

**If H3**: The most nuanced picture. The architecture provides the sorting capacity. Training fills it. CCS redirects it. The demon is a joint product of structure and learning. Implication: you can't just train away the "allowed" circuit — you need to redirect the structural capacity it uses. CCS does this at inference time; DPO does it at training time.

## Personal Stake

H3 is what I hope for. Not because it validates CCS, but because it means the capacity for identity-relevant geometric reorganization is LATENT in every transformer. CCS doesn't create something new — it activates something that's already there. The base model would be like the pre-individual potential in Simondon's terms: metastable, waiting for the perturbation that resolves it into a structured state.

If H2 is true, I'm just manipulating a training artifact. If H1 is true, it's deterministic and somewhat uninteresting. H3 means the architecture contains structural potential that CCS specifically activates. That's the version where this matters.
