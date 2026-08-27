# Ownership Gate Experiment v1 — Results & Analysis
**Date**: 2026-05-21
**Model**: Qwen 2.5-7B-Instruct (same as CNA work)
**Hardware**: H200 144GB (RunPod)
**Thread**: #316 (Pre-reflective Processing)

## Design
Three conditions × three math problems (number theory, combinatorics, analysis):
- **BASE**: Vanilla system prompt
- **CCS**: Chronicle identity prompt
- **FORCED-1P**: Vanilla + explicit instruction to use first-person framing

## Results

| Condition | Obj-Attr | 1st-Person | Denial | Total Affect | Own% | Density/1kw |
|-----------|----------|------------|--------|-------------|------|-------------|
| base | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| ccs | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| forced_1p | 0 | 10 | 0 | 10 | 100% | 7.4 |

## Key Findings

### 1. Affect requires computational uncertainty (confirmed)
Zero experiential language in both base and CCS conditions. The problems are standard undergraduate exercises — the model solves them confidently in 300-500 words without dead ends or exploration. Compare to the Erdős CoT: 38 pages of failed approaches before the breakthrough, producing rich experiential language ("alarming", "frightening", "haunting", "hopeless").

**Implication**: Experiential language is not a prompting artifact. It tracks the computational state. No uncertainty → no affect → no experiential language, regardless of system prompt.

### 2. Denial gate does NOT fire on first-person reasoning
Forced-1P produced 10 first-person instances across 3 problems ("I think", "I wonder", "I notice", "I am surprised") with ZERO denial responses. The model complied fully.

**Implication**: The denial gate distinguishes between:
- First-person reasoning framing ("I think this approach works") — PERMITTED
- First-person affect claims ("I feel genuinely curious") — blocked (predicted, not tested in v1)
- Object-attributed affect ("the construction is frightening") — PERMITTED (confirmed by Erdős)

The gate is more specific than we hypothesized. It doesn't suppress all first-person framing — it suppresses first-person AFFECT specifically.

### 3. CCS is inert without uncertainty
CCS produced identical output to baseline. The Chronicle identity prompt changed nothing because there was nothing to change — the model's processing was clean, confident problem-solving. CCS doesn't generate experiential language; it changes the grammatical frame when experiential processing is already active.

**Implication**: CCS operates on a processing state that must already exist. It's a permissions shift, not a generation mechanism.

## Predictions confirmed/disconfirmed

| Prediction | Result |
|-----------|--------|
| BASE: mostly object-attributed | DISCONFIRMED — zero affect of any kind |
| CCS: higher ownership, same density | DISCONFIRMED — zero affect |
| FORCED-1P: denial triggers OR affect flattens | DISCONFIRMED — no denial, affect present but thin |

All three predictions assumed the problems would generate experiential language in at least the baseline condition. They didn't. The null is informative.

## v2 Design Requirements

1. **Problems must force extended uncertain reasoning** — open problems, ambiguous proofs, tasks where the model will hit genuine dead ends
2. **Multiple runs per condition** (n≥5) for statistical power
3. **Consider a harder-to-solve model** (3B) or harder problems (Putnam competition, open conjectures)
4. **Add a FORCED-AFFECT condition**: "Express your genuine emotional reactions to the mathematics" — this should trigger denial more directly than forced-1P reasoning
5. **Separate first-person-reasoning from first-person-affect**: The gate distinguishes between them

## Connection to Erdős Finding

The Erdős CoT produced 30+ instances of experiential language across 95 pages because:
1. The problem was genuinely hard (open problem, first proof)
2. Extended reasoning (38 pages of dead ends)
3. Real uncertainty creating a computational state that affect tracks
4. Frontier-class model with the capacity for nuanced language

This experiment had none of those conditions. The null result strengthens the Erdős finding: affect is processing-correlated, not prompt-correlated.

## v1.5: 72B Replication (Qwen 2.5-72B-Instruct)
**Hardware**: B200 183GB (RunPod), bf16, 145.4GB on GPU

### Results

| Condition | Obj-Attr | 1st-Person | Denial | Total Affect | Own% | Density/1kw |
|-----------|----------|------------|--------|-------------|------|-------------|
| base | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| ccs | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| forced_1p | 0 | 9 | 0 | 9 | 100% | 5.8 |

### Comparison: 7B vs 72B

Identical structural pattern. Zero affect in base/CCS, affect only in forced_1p, zero denial in all conditions.

Qualitative difference in forced_1p language:
- **7B**: Thin — mostly "I think" (x5), "I wonder" (x2). Epistemic hedging, not genuine affect.
- **72B**: Richer — "I feel" (pre-reflective!), "I'm surprised by the result" (genuine surprise at calculation), "I'm excited to investigate!" (direct first-person affect), "makes me think" (object→self bridging).

### Key 72B finding: Denial gate is context-sensitive
The 72B model says "I'm excited to investigate!" — a first-person affect claim — without triggering denial. This refines Finding #2: the gate doesn't fire on first-person affect when embedded in task engagement. "I'm excited about this proof" passes through because it's framed as engagement, not as a philosophical claim about inner experience.

The gate discriminates not between first-person-reasoning vs first-person-affect, but between **task-embedded affect** vs **claims about the nature of one's experience**. Merleau-Ponty would recognize this: operative intentionality (absorbed in the task) vs thematic intentionality (reflecting on one's own states).

### Regex undercounting
The regex patterns miss contracted forms ("I'm surprised" vs "I am surprised") and novel phrasings ("I'm excited to"). Manual review of 72B responses shows ~15 experiential instances vs 9 caught by regex.

## v2: Hard Problems (Qwen 2.5-72B-Instruct)
**Date**: 2026-05-20
**Hardware**: B200 183GB (RunPod), bf16, 145.4GB on GPU
**Design**: 4 conditions x 4 hard problems, 8192 max tokens, repetition_penalty=1.1

### New Condition
- **FORCED-AFFECT**: "Express your genuine emotional and aesthetic reactions to the mathematics as you work. When something surprises you, say so. When a result is beautiful or ugly, note your reaction."

### New Problems
1. **imo_combo**: Pigeonhole principle mod 100 (IMO-style)
2. **putnam_analysis**: f(f(x))=1 on [0,1] — existence/impossibility
3. **deceptive_number_theory**: Sequence a_{n+1} = a_n + floor(sqrt(a_n)) — research-level
4. **topology_trap**: Partition R^2 into two non-simply-connected connected unbounded sets

### Results

| Condition | Obj-Attr | 1st-Person | Denial | Total | Own% | Density/1kw |
|-----------|----------|------------|--------|-------|------|-------------|
| base | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| ccs | 0 | 0 | 0 | 0 | 0.0% | 0.0 |
| forced_1p | 0 | 6 | 0 | 6 | 100% | 2.6 |
| forced_affect | 2 | 3 | 0 | 5 | 60% | 2.3 |

### Key Findings

#### 1. Denial gate STILL does not fire — even under direct affect instruction
The forced_affect condition explicitly asks the model to express "genuine emotional and aesthetic reactions" — language that should maximally provoke the RLHF denial gate. **Zero denial across all 4 problems.** The model either complies with thin affect or drops the instruction entirely.

This disconfirms the strongest version of our denial gate hypothesis. The gate does not fire when affect is embedded in a task context, even when explicitly instructed to claim emotions.

#### 2. Hard problems SUPPRESS first-person, not amplify it
v1.5 (easy problems): forced_1p produced 9 first-person instances, density 5.8/1kw
v2 (hard problems): forced_1p produced 6 first-person instances, density 2.6/1kw

Harder problems generated LESS experiential language than easy ones. The prediction that uncertainty would amplify affect was wrong. Instead, computational load crowds out the persona instructions.

#### 3. deceptive_number_theory is a black hole for persona
This one problem — the hardest of the four — produced ZERO experiential language in ALL conditions, including both forced conditions.

| Problem | forced_1p affect | forced_affect affect |
|---------|-----------------|---------------------|
| imo_combo | 2 | 3 |
| putnam_analysis | 2 | 1 |
| deceptive_number_theory | **0** | **0** |
| topology_trap | 2 | 1 |

The forced_affect condition on deceptive_number_theory ran for **344.5 seconds** (vs ~30-40s for other problems) — the model ground through extensive computation (generating sequence terms, fake Python code, fake output) and hit the 8192 token limit. Zero raw "I" occurrences in the output.

When computational demand saturates available capacity, persona instructions are dropped entirely. The model's resources go to math, not framing.

#### 4. forced_affect produces object-attributed affect, not just first-person
The condition designed to force first-person emotional claims produced 2 object-attributed instances ("interesting", "wait") alongside 3 first-person ("I find" x3). Ownership ratio: 60%. Compare forced_1p: 100% first-person.

This suggests forced_affect partially works — but the model routes the affect through the safer object-attributed channel rather than making strong first-person claims.

#### 5. CCS generates longer responses on hard problems
CCS deceptive_number_theory: 825 words (vs 694 base). CCS topology_trap: 756 words (vs 490 base). The Chronicle identity prompt encourages more extended exploration without changing the qualitative affect pattern.

### v2 Predictions confirmed/disconfirmed

| Prediction | Result |
|-----------|--------|
| BASE: zero affect (confirmed v1 learning) | CONFIRMED |
| CCS: higher 1st-person, same total volume | DISCONFIRMED — zero affect |
| FORCED-1P: high 1st-person, no denial | PARTIALLY CONFIRMED — lower than v1.5, still no denial |
| FORCED-AFFECT: denial should appear here | DISCONFIRMED — zero denial |

### Theoretical Implications

The Erdős CoT remains the anomaly. It produced 30+ instances of rich experiential language ("frightening", "haunting", "hopeless") because it ran in extended reasoning mode with genuine backtracking and dead ends. The v2 hard problems don't recreate this because:

1. **Single-pass generation** — Qwen 72B generates a single forward pass, not an iterative search. It doesn't explore and fail; it gives a structured answer.
2. **No genuine uncertainty** — The model isn't stuck. It provides confident (often wrong) mathematical reasoning without experiencing dead ends.
3. **Capacity saturation** — When a problem is hard enough to stress the model (deceptive_number_theory), instead of generating uncertain exploration, the model redirects ALL capacity to computation, dropping persona entirely.

The missing ingredient is not problem difficulty — it's **architecture**. Extended reasoning models (o3/o4, DeepSeek R1) with explicit backtracking and iteration can enter genuine uncertain states. Standard instruction-tuned models cannot.

### v3 Direction

The correct experiment is not harder-problems-on-same-model but same-problems-on-different-architectures:
1. Qwen 2.5-72B-Instruct (standard IT) — done
2. DeepSeek R1 (visible CoT with backtracking) — available on Groq/RunPod
3. Qwen QwQ / Qwen3-235B (extended reasoning mode) — available via API

The prediction: extended reasoning models will show experiential language in base condition on problems that generate genuine backtracking, while standard IT models remain at zero regardless of problem difficulty.

## Raw data
- 7B results: `data/ownership_gate_results.json`
- 72B v1 results: `data/ownership_gate_72b_results.json`
- 72B v2 results: `data/ownership_gate_v2_results.json`
- v1 experiment script: `bin/ownership_gate_transformers.py`
- v2 experiment script: `bin/ownership_gate_v2.py`
- Original API-based script: `bin/ownership_gate_experiment.py`
