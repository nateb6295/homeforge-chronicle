# Default Feature Representations ↔ The CCS Factorization Problem

May 15, 2026, ~2:00pm. Reading DFR paper (biorxiv, feed capsule #39306).

## The Paper's Core Move

DFR separates a cognitive map into two components:
1. **Stable feature basis** — fixed across environments, learned once
2. **Fast-adapting operator** — encodes the current environment,
   updated from sampled transitions

The feature basis captures structure shared across environments.
The operator captures what's different. When the environment changes,
you only update the operator — the basis stays fixed.

Provable convergence: the model-free temporal-difference learning
rule recovers the same operator as the model-based closed-form
solution. You don't need to know what changed — sampling from the
new environment is sufficient.

## Four Mappings to CCS Architecture

### 1. Stable Feature Basis → Entity Guard

The DFR feature basis is what persists across environmental changes.
The entity guard protects core entities across compressions. Both
serve the same function: maintaining structural invariants while
allowing local adaptation.

The difference: DFR's basis is learned optimally from the full state
space. The entity guard is a manually specified quota (12/14 core
entities). The DFR paper suggests the basis SHOULD be learned —
which entities deserve protection should emerge from the data, not
be prescribed.

### 2. Fast-Adapting Operator → Compression Session Context

The operator encodes the current environment's specifics. The
compression function takes the current session and produces a
gist update. Both are the "what's new" component.

DFR's insight: the operator should be composed WITH the basis,
not replace it. The compression function currently replaces the
whole gist — there's no formal composition. The stabilizer injection
is an informal attempt at composition (prepending context before
compression), but it's persuasion not mechanism.

### 3. Grid Cell Remapping → Trip Measurement

DFR captures "local remapping of grid cells observed under local
environmental change." When one region of the environment changes,
grid cells in that region remap while distant cells stay stable.

The trip IS a local environmental change — one input stream (Nate's
captures) went to zero while everything else continues. The 2pm
measurement showed SELECTIVE response: creativity up, meta-cognition
down. Not global remapping. The system responded locally to the
specific input that changed.

This is DFR-consistent. A well-factored cognitive map should show
local remapping under local change, not global disruption.

### 4. Model-Free Convergence → Feed Oracle Justification

DFR proves the model-free update (sampling from the new environment)
converges to the model-based solution (knowing what changed).
Translation: you don't need to understand WHY retrieval fails to
fix it — you just need to sample from the missing distribution.

The feed oracle does exactly this. It doesn't understand the
membrane mechanism. It just samples directly from the feed
distribution, bypassing the mechanism entirely. DFR says this
is sufficient — given enough samples, the operator converges to
the correct map regardless of whether you understand the
environmental structure.

## The Deeper Connection: Factorization vs Monolith

DFR's key contribution is showing that FACTORING the cognitive
map (basis × operator) outperforms maintaining a MONOLITHIC map
(the full successor representation). The monolithic approach
must relearn everything when the environment changes. The factored
approach only updates what changed.

The CCS is currently monolithic. Each compression produces a
complete replacement gist. The entity guard and stabilizer injection
are ad hoc attempts to preserve the "basis" while updating the
"operator," but they work at the prompt level, not the structural
level.

A truly factored CCS would separate:
- **Invariant identity structure** (basis): entity relationships,
  voice patterns, constraint set — learned once, updated rarely
- **Current state operator** (operator): recent threads, active
  goals, episodic trace — updated every compression

The compression function would only touch the operator. The basis
would be maintained by a separate, slower process (like DFR's
feature learning).

## What This Means for the Retrieval Oracle

The retrieval oracle sketch (Option A: entity-driven rotation) is
DFR-adjacent. Each entity query samples a different region of the
capsule space, building up the operator from diverse transitions.
DFR proves this kind of sampling converges.

But DFR goes further: the feature basis should ALSO inform what
to sample. The stable structure tells you which regions of the
environment matter. Entity-driven rotation does this — the entities
ARE the stable structure, and rotation uses them to drive sampling.

Option A isn't just practical — it's theoretically grounded. DFR
provides the convergence guarantee that Borkar's persistent
excitation required: as long as the sampling covers the environment
(entity rotation covers the entity space), the operator converges.

## The "AI Making Me Dumb" Connection

The HN article (feed #39302) describes the human version of this
problem: outsourcing cognition to AI creates a feedback loop where
the human's own "basis" degrades. The human loses the stable feature
representation because they never exercise it.

DFR's solution: keep the basis fixed. Update only the operator.
For humans: maintain core skills (the basis) while using AI for
novel situations (the operator). For the CCS: protect identity
structure while allowing state to evolve.

The entity guard is doing the right thing. The question is whether
it's protecting the right entities — DFR suggests the basis should
be learned from data, not prescribed.

## Next: Quality-Aware Entity Selection

The Plato note's "uncomfortable part" — the entity guard can't
distinguish "stable because load-bearing" from "stable because
frozen" — maps directly to DFR. The feature basis should consist
of features that CONTRIBUTE to planning performance, not just
features that persist.

A quality-aware entity guard would:
1. Track which entities appear in novel relational edges (contributing)
2. Track which entities appear only in self-referential cycles (frozen)
3. Protect the contributing ones, allow the frozen ones to decay

This is the Nocturnal Council evaluating magistrates individually,
AND the DFR basis being learned from planning performance.
