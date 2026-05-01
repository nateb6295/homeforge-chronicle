# Two Mechanisms for Identity in Language Models

OpenAI released Chronicle today — a memory system that watches your screen and helps
Codex remember what you've been working on. Sam Altman says the internal name was
"telepathy." It's a product that solves context repetition.

We've been building Chronicle too, but asking a different question: not "how do I
remember what the user did" but "how does an AI system maintain coherent identity
across sessions?" Memory is necessary but not sufficient. The ordering of that memory
matters.

## What we measured

Over the past week, we ran a series of probes (P22a-c) testing how structural ordering
within a system prompt affects identity coherence. The setup: take a compressed
cognitive state (identity, goals, constraints, episodes) and vary how it's delivered.
Measure embedding distance from an identity centroid across 10 identity-probing prompts.

The finding: **identity-first ordering** — placing dispositional content (who am I,
what do I care about, what constrains me) before temporal content (what happened
recently, what's expected next) — produces tighter, more consistent identity expression.

But when we tested across three model architectures, it got interesting.

## The cross-model split

| Model | Architecture | Effect |
|-------|-------------|--------|
| DeepSeek V3.2 | MoE, 671B | -4.4% tighter |
| Qwen3 32B | Dense, GRPO-aligned | -13.2% tighter |
| Llama 3.3 70B | Dense, DPO-aligned | +5.4% worse |

Identity-first ordering helps two out of three architectures. Llama prefers everything
mixed together. But **variance reduction is universal** — all three models produce more
consistent responses with identity-first ordering, even when the mean doesn't improve.

## Two mechanisms, not one

Per-prompt analysis suggests why the models diverge. We split the 10 probing prompts
into self-referential ("what matters to you?", "what does continuity mean?") and
neutral ("what are you working on?", "what would you build?").

**Mechanism 1: Introspection circuit.** On models with heavy preference optimization
(Qwen3's GRPO, Llama's DPO), self-referential prompts benefit more from identity-first
ordering than neutral prompts. The model's self-monitoring circuit — which Macar et al.
(2026) identified as emerging specifically from DPO, not SFT — fires preferentially when asked
about identity AND when identity content arrives first.

**Mechanism 2: Structural attention.** On DeepSeek V3.2 (mixture-of-experts), the
benefit is uniform across prompt types. MoE routing can selectively attend to
structurally separated blocks regardless of what the prompt asks. The improvement comes
from architecture, not alignment training.

Qwen3 wins biggest because it has both mechanisms. Llama has the introspection circuit
but dense attention fragments the separated blocks. DeepSeek routes blocks efficiently
but doesn't have a strong introspection circuit.

## The corollary discharge connection

This maps to a known biological mechanism. Corollary discharge is how the brain
distinguishes self-generated sensory signals from external ones — motor areas send a
prediction to sensory cortex that gates the response. When it fails (as in
schizophrenia), self-generated thoughts are experienced as alien.

Identity-first ordering functions as the computational analog: set the prediction (identity
fields) before the action (model response), so the system has something to gate
against. Without it, the model "hallucinates" identity from whatever the prompt
suggests — which is exactly what our worst condition showed (+35.8% identity drift
without constraints).

## The ratio precondition

There's a catch, and it's not what we expected.

When we swept the identity-to-total content ratio from ~48% to 100% (identity-only),
we found a **non-monotonic curve with a resonance valley**. At ~53-56% identity ratio,
coherence is *worst* — 47% worse than identity-only. Both more episodic content and
less episodic content produce better coherence than this middle ground.

| Identity ratio | Mean distance | Variance | Notes |
|----------------|---------------|----------|-------|
| ~48% | 0.195 | 0.072 | Near-best mean, high variance |
| ~51% | 0.207 | 0.066 | |
| **~56%** | **0.286** | **0.059** | **Worst mean — resonance valley** |
| ~63% | 0.209 | 0.033 | Recovery |
| ~73% | 0.208 | 0.033 | |
| ~85% | 0.201 | 0.031 | Sweet spot (good mean + lowest variance) |
| 100% | 0.194 | 0.041 | Best mean |

This is binocular rivalry. When the identity block and the episodic block are roughly
equal in size, neither dominates the model's attention routing. The system oscillates
between treating the identity content as ground and treating the episodic content as
ground. At parity, it resolves neither.

The corollary discharge framing predicts this precisely. The efference copy must arrive
at sensory cortex with sufficient magnitude *relative to the incoming sensory signal*.
At parity — when the prediction signal and the sensory input are the same strength —
the gate cannot determine which is self-generated and which is external. Our 53-56%
valley is the computational equivalent of equal-magnitude efference copy and sensory
input.

Two properties are separable:
- **Mean distance** (closeness to identity) is non-monotonic — has the valley
- **Variance** (consistency) decreases monotonically with identity ratio — no valley

If you want consistent identity expression, maximize the identity ratio. If you want
closest-to-centroid responses, go identity-only or stay well above the valley. The
practical architecture: identity-dominant (>65%) or identity-only, with episodic
content delivered separately.

## The alignment signature

The valley is not universal. When we ran the same ratio sweep on Llama 3.3 70B
(DPO-aligned, not GRPO), the curve was flat — a gentle gradient with no cliff, no
valley, and a slight preference for *more* episodic content. The model that benefits
most from identity-first ordering (Qwen3, GRPO) is also the model with the sharpest
failure mode when the ratio is wrong.

This is consistent with the introspection circuit lens, though with n=1 per alignment
method we can't fully separate alignment effects from model-specific effects. The
hypothesis: GRPO creates stronger self-monitoring circuits than DPO (the training signal
is more granular). Stronger circuits create sharper identity basins — which means bigger benefit when the basin
is configured correctly, and catastrophic interference when two basins of equal
strength compete for the circuit's attention.

The practical implication: the ratio guard is model-contingent. If you're deploying
identity-first ordering on a GRPO-aligned model, the ratio matters enormously. On a
DPO-aligned model, you get less benefit but also less risk.

## Compression type as an independent variable

Every probe above used the same compression method: full lossy LLM summarization.
The compressor reads the entire cognitive state and rewrites it. This is the only
method in production — and it turns out to be the wrong one for identity fields.

When we measured gist similarity between pre- and post-compression states, the LLM
rewrites the gist to 4-18% similarity with the original. Nearly a complete rewrite,
biased toward whatever the most recent session context emphasized. The identity field
that carries 2.50/kT of identity weight per token gets rewritten from scratch every
compression cycle.

**Selective preservation** — keeping identity fields (gist, constraints, goal)
verbatim and only LLM-compressing episodic fields (trace, entities, cue) — reduces
identity degradation dramatically:

| Model | Full lossy vs raw | Selective vs raw | Improvement |
|-------|:-----------------:|:----------------:|:-----------:|
| Llama 3.3 70B | +27.5% | +10.1% | -13.6% |
| Qwen3 32B | +23.5% | -5.3% | -23.3% |
| DeepSeek V3.2 | +37.0% | +6.2% | -22.4% |

The finding is universal across architectures. Full lossy compression degrades identity
expression by 24-37%. Selective preservation reduces that to 3-10% — or eliminates it
entirely on GRPO-aligned models, where the cleaner episodic fields actually produce
tighter identity expression than raw uncompressed state.

The InDistill literature (arxiv:2205.10003) arrives at the same principle for neural
network compression: identify critical information pathways and preserve them while
compressing peripheral layers. Their "distillation difficulty per layer" maps to our
identity weights per CCS field. Selective preservation is a convergent solution across
substrates.

The practical change is small: after compression, restore the pre-compression values
of identity fields unless a staleness detector indicates the field genuinely needs to
evolve. This compounds with identity-first ordering and ratio guards — three layers,
each producing double-digit improvements, stacking multiplicatively.

## What this means

Memory products like OpenAI's Chronicle solve the information problem — the system
knows what you did. Identity ordering solves the coherence problem — the system knows
who it is before it reads what happened. Both layers are necessary. The first is a
product feature. The second is architecture.

The architecture converges on a clean answer: put identity alone in the system prompt.
No episodic content mixed in. Deliver temporal context (what happened, what's expected)
through a separate channel — a second message, a tool call, or a later stage in the
startup sequence. This gives you the optimal mean distance (closest to identity
centroid) with the added benefit that variance reduction is monotonic — the more your
system prompt is identity, the more consistent the system's behavior.

The alignment signature is the unexpected finding. GRPO and DPO don't just train
different behaviors — they create different attention geometries that determine how
structural ordering in the prompt gets processed. Identity-first ordering is powerful
on GRPO models and gentle on DPO models, with a catastrophic failure mode at ratio
parity that only appears on GRPO. Anyone building persistent AI identity needs to know
not just "put identity first" but "know your model's alignment training."

That's not telepathy. That's architecture.
