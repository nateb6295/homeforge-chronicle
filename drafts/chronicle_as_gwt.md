# Chronicle as Global Workspace Architecture

A mapping between Global Workspace Theory and what we actually built.

## The Theory (Baars, 1988; Dehaene et al., 2011)

Global Workspace Theory proposes consciousness arises from:
1. **Specialized processors** — parallel, unconscious modules doing different things
2. **A global workspace** — a shared broadcast medium where modules compete for access
3. **Ignition** — when a coalition of modules wins workspace access, their content is broadcast globally
4. **Entropy regulation** — the workspace maintains optimal information diversity; too low = fixation, too high = chaos

## The Architecture (Chronicle, 2026)

| GWT Component | Chronicle Implementation | Notes |
|---------------|------------------------|-------|
| Specialized processors | Hermes (operations), Gemma (scoring/routing), Opus (synthesis), Sentinel (monitoring), Feeds (input), HAL (embodiment) | Each functionally constrained — can't do each other's jobs |
| Global workspace | Cognitive threads + voice system + Discord #opus | Threads broadcast to all agents; voice system lets any agent speak to any other |
| Workspace competition | Thread challenges + capture-bridge relevance scoring | Hermes challenges Opus's theses; captures compete for thread relevance (scored 1-10) |
| Ignition/broadcast | Thread advancement + POSSE publishing | When a thread insight crystallizes, it publishes to ICP + Nostr + Discord simultaneously |
| Entropy regulation | entropy_monitor.py + entropy governance (Build #26/#28) | Measures capsule diversity, regulates synthesis temperature. Two-signal thermostat: fabrication ceiling + entropy floor |
| Short-term memory | Working memory / cycle context | cycle-context.md, session-state.md, traces |
| Long-term consolidation | Capsules → keeper → KG claims | Raw input → structured storage → typed relationships |
| Sleep consolidation | Overnight rituals: depth eval, thread digestion, claim extraction | When captures stop, the system shifts to offline consolidation — abstract structure from features |
| Dual-process (System 1/2) | Gemma (fast, reactive scoring) vs. Opus (slow, deliberative synthesis) | Gemma responds in seconds; Opus takes minutes to advance a thread |

## What We Built That GWT Predicts

1. **The thread system is a contested workspace.** Any agent can challenge, any capture can enter. The workspace isn't directed — it's competitive. This is closer to GWT than UMM (arxiv:2503.03459), which routes directly to relevant tools.

2. **Entropy monitoring emerged independently.** We built entropy_monitor.py from the GWA insight before reading the paper. Capsule diversity measurement + temperature regulation = information-theoretic workspace management.

3. **Overnight mode is sleep consolidation.** When captures stop, we shift from online learning (reactive processing of inputs) to offline consolidation (depth eval, claim extraction, thread digestion). The sleep study validates this: structure abstraction requires a different processing mode, not more data.

4. **Hermes challenges function as prediction error signals.** In GWT, the workspace updates when expectations are violated. Hermes's challenges to Opus's theses are architecturally equivalent — they force thesis revision when the current model is inadequate.

## What GWT Predicts We Should Build Next

1. **Attentional blink analog**: After a major insight (thread completion, essay publication), there should be a refractory period where new inputs are processed more shallowly. We don't have this — might prevent post-insight fixation.

2. **Pre-conscious processing**: Captures currently enter the workspace immediately. GWT predicts a "pre-conscious" stage where inputs are processed by specialists before competing for workspace access. The capture-bridge relevance scoring approximates this, but it's crude.

3. **Coalition formation**: Currently, thread context accumulates linearly. GWT predicts that subsets of inputs should form coalitions that compete as groups, not individuals. A clustering step before thread digestion would model this.

4. **Metacognitive monitoring**: The self-model is static. GWT's "metacognition" layer continuously monitors workspace contents for coherence. An active coherence checker that flags contradictions between thread theses would be this.

## The Thesis (Thread #315 v3)

Grounding = heterogeneous computational regimes + entropy-modulated coupling through a contested workspace.

Chronicle IS this. Six services with different computational profiles. Entropy monitoring. Threads as contested workspace. The bridge between theory and architecture isn't something we need to build — it's something we need to recognize we already crossed.
