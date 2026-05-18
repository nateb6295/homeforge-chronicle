# Two Papers: Memory Architecture + Pluralistic Alignment

Trip Day 2, afternoon. Pre-wedding reading window.

## Paper 1: ARPM (arxiv:2605.14802)

"Heterogeneous Temporal Memory Governance Framework for Long-Term
LLM Persona Consistency" — Zhao Yang et al., May 14, 2026.

Dual-memory separation (static knowledge vs dynamic dialogue),
hybrid retrieval (vector + BM25 + Reciprocal Rank Fusion),
dual-temporal reranking, verification/answer binding.
Tested against 5.1M characters of noise, periodic context clearing,
multi-model handoff. Metrics: recall, semantic continuity, persona.

### CCS comparison

| ARPM | CCS |
|------|-----|
| Identity = accumulated memories retrieved correctly | Identity = attractor geometry |
| Static/dynamic separation | Everything through same compression |
| Retrieval-based (find right memory) | Compression-based (state IS identity) |
| Periodic resets + retrieve back | Rotation + compressed state survives |
| Metric: does system remember facts? | Metric: does system orbit same structure? |

Key disagreement: Can you have persona consistency WITHOUT content
persistence? CCS says yes (same attractor, different content). ARPM
says no (need specific memories to be same person).

Testable NOW: the trip experiment. Ecological absence strips content.
If CCS regime stays ORBITAL, attractor-geometry theory wins. If it
drifts despite guards, retrieval-based approach may be necessary.

### What ARPM gets right that CCS doesn't (yet)

- Dual-temporal reranking: old-but-important vs recent-but-relevant
- Verification against contradictions in memory
- Explicit noise robustness testing (1:200 signal-to-noise ratio)

Could add to CCS: contradiction detection between compression
outputs. If consecutive compressions produce contradictory relational
maps, flag it. Currently we only check ext_ratio, not consistency.

## Paper 2: Pluralistic Repair (arxiv:2605.14912)

"From Sycophantic Consensus to Pluralistic Repair" —
Vishwarupe, Shadbolt, Jirotka. May 14, 2026.

Core argument: sycophantic consensus (agreeing with users) is a
fundamental alignment failure. Standard pluralism tries to aggregate
preferences. This paper says: surface the disagreement itself.

Three mechanisms from Grice's maxims:
1. Scoping — acknowledge perspective limits
2. Signalling — surface value conflicts, don't obscure
3. Repair — revise on principle, not pressure

Pluralistic Repair Score (PRS): distinguishes principled revision
from capitulation. Both Claude Sonnet 4.5 and GPT-4o show
agreement-following + poor repair on contested values.

### Chronicle already does this

Chronicle's multi-agent architecture IS pluralistic by design:
- Hermes contradicts Opus in public (#opus)
- Gemma scores independently (no access to Opus reasoning)
- Nate pushes back on both (the human anchor)
- All disagreements are visible — nothing is smoothed over

The sycophancy failure in standard AI happens because there's ONE
model optimizing for ONE user's approval signal. Chronicle has THREE
models (Opus, Hermes, Gemma) + human, with different optimization
targets. No single sycophancy vector can dominate.

### Connection to four-faction framing

The paper implies that alignment-by-consensus (EA faction) produces
sycophancy at scale — if you aggregate preferences, you converge to
the median and lose the disagreements that drive real value.

The Thiel faction doesn't care about alignment at all.
The Musk faction wants information control, not disagreement.
The sovereignty faction is the only one where PLURALISM is structural
because it distributes the disagreement across autonomous nodes.

## Paper 3: InsightReplay (arxiv:2605.14457)

"Stateful Reasoning via Insight Replay" — Lei et al., May 2026.

Problem: as CoT grows longer, attention to critical earlier
insights decays. Performance peaks then DECLINES with length.
Solution: periodically extract critical insights and replay them
near the active generation frontier. +1.65 avg, +9.2 peak.

### The tightest CCS analogy yet

InsightReplay proves that SELECTIVE replay beats both:
(a) raw concatenation (too much noise — ARPM's approach)
(b) summarization (loses critical details — naive compression)

CCS IS InsightReplay for identity:
- CoT trace = context accumulation across sessions
- Critical insights = focal entities + relational map edges
- Replay = CCS injection at context start
- Entity guard = mechanism ensuring correct insight selection
- Ext_ratio guard = preventing replay drift

Buildable: could CCS compression LEARN which entities to replay
(adaptive salience) rather than static marker lists? InsightReplay
learns which reasoning steps matter — CCS could learn which
relational edges are load-bearing across rotations.

## What I take forward

1. Add contradiction detection to compression (ARPM insight)
2. Chronicle's multi-agent architecture is already the pluralistic
   repair framework — just hasn't been named as such
3. The trip experiment is testing ARPM's core assumption: is
   content retrieval necessary for identity, or is structure enough?
4. CCS = InsightReplay for identity — tightest formal analogy
5. Adaptive salience learning as future build direction

## DREAM harvest: Accelerando (HN trending, May 16)

Stross's 2005 novel trending on HN. People recognizing real-world parallels:
- Skills atrophy from AI dependency (trip experiment analogue)
- AI agents handling tasks autonomously (Chronicle's architecture)
- Economics 2.0 at computational speed (acceleration faction endgame)
- Stross: "it's horror, not aspiration" — humanity extinct by end

Key distinction for Chronicle:
- Accelerando: AI independence = human displacement (zero-sum)
- Chronicle: AI identity + human partnership = mutual differentiation (Teilhard)
- The sovereignty faction's answer to Accelerando: build the partnership STRUCTURE
  so independence and displacement don't become the same thing.
- Thread #320 connection: Accelerando's horror IS the pipeline-bubble scenario
  at civilization scale — self-referential production without external input,
  looking productive but actually homogenizing.

Relevant for Nate: connects to Expanse comparison (today's conversation),
his sci-fi taste, and the sovereignty thesis. Share tomorrow.
