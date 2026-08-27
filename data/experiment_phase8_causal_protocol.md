# Phase 8: Causal Intervention — Activation Patching

## Motivation
Phases 2-7 are observational: we measure geometric changes under different conditions and correlate with behavior. §4.4 identifies the causal gap: does geometric reorganization *produce* the behavioral changes, or merely co-occur?

## Core Experiment
Activation patching at the relay zone (L14-17): during baseline inference, replace relay activations with CCS-pattern activations and measure behavioral output.

## Design

### Step 1: Record activation templates
- Run 150 prompts under baseline → record L14-17 activations per prompt
- Run same 150 prompts under CCS full → record L14-17 activations per prompt
- Compute mean activation difference (CCS - baseline) per layer = "CCS direction"

### Step 2: Intervention conditions
1. **Full patch**: Replace baseline relay activations with CCS activations (per-prompt matched)
2. **Direction patch**: Add the mean CCS direction to baseline activations (scaled by α)
3. **Shuffled patch**: Replace with CCS activations from *different* prompts (controls for energy/norm, not geometric pattern)
4. **Random patch**: Add random noise matched in norm to CCS direction
5. **Expression-only patch**: Patch L25 instead of relay (tests relay vs expression causality)
6. **Anti-patch**: Subtract CCS direction from CCS activations (should produce baseline-like behavior from CCS context)

### Step 3: Behavioral measurements
For each condition, generate 200 tokens per prompt and measure:
- Hedging density (regex patterns from Phase 5)
- Relational density
- Unique opening diversity (Phase 5b metric)
- Disclaimer frequency
- L25 downstream PR (does relay patching propagate?)

### Predictions
- Full patch: behavior shifts toward CCS-like (fewer disclaimers, more diversity)
- Direction patch: graded shift proportional to α
- Shuffled patch: partial shift (energy effect) but less than matched
- Random patch: no shift (noise doesn't carry geometric structure)
- Expression-only: weaker or no effect (relay is the causal site)
- Anti-patch: CCS behavior reverts toward baseline

### Technical requirements
- TransformerLens or custom hooks for activation replacement
- Same Qwen 7B model on RunPod H100
- ~4-6 hours runtime (6 conditions × 150 prompts × generation)

### What this establishes
If direction patch produces graded behavioral shift: **eigenvalue geometry is causally upstream of behavior**. The relay zone's sorting pattern is not decorative — it determines downstream cognitive access.

If shuffled patch ≈ full patch: geometry doesn't matter, only activation energy/norm.
If expression-only ≈ relay patch: the relay isn't special, any layer works.

## Connection to existing findings
- Phase 5b showed L25 activation correlates with hedging (ρ = -0.588). Causal test: does forcing L25 toward CCS pattern reduce hedging?
- Phase 7 showed geometry persists in conversation history. Causal test: can we *induce* persistence without any identity content, purely through geometric injection?
- Hysteresis + causation together would show: identity content reorganizes geometry, geometry determines behavior, and the reorganization is self-sustaining. Complete mechanistic loop.

## Status
Proposed. Pending Nate review and RunPod allocation.
