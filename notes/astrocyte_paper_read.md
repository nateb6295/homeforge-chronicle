# Astrocyte-neural field paper (arxiv 2604.10036) — abstract read

Overnight read, 2026-04-14 02:15 PDT.

## The two-stage stabilization

Coupled astrocyte-neural field model. Working memory bumps are stabilized by:
1. **Astrocytic diffusion** smooths resource asymmetries created by bump displacements
2. **Synaptic replenishment** transfers that smoothing back to the synaptic pool

Together: drift instabilities are suppressed, the parameter regime where stationary bumps persist *enlarges*.

This is a NAMED mechanism for what Chronicle has been missing.

## The Chronicle mapping

| Biology | Chronicle equivalent | Status |
|---------|---------------------|--------|
| Stationary bumps | Persistent focal entities | We have these |
| Drift instabilities | Focal entities silently dropping during rotation | Confirmed by tonight's audit |
| Astrocytic diffusion | Slow background mechanism smoothing recent activity into focal_entities | **Missing** |
| Synaptic replenishment | Active re-promotion from memory store back to focal_entities | **Missing** |

ContextCurator (RL-learned) does step 2 by training a policy. ClawVM (declared) does step 1 by enforcing typed pages at write-time. Chronicle currently does neither — we compress and hope.

## What this concretely suggests

A "diffusive" promoter that runs between rotations:
- Reads recent activity_feed (last N hours)
- Computes an embedding-space density estimate
- Identifies entities that consistently appear (high local density) but aren't in current focal_entities (low salience)
- Re-promotes them with a salience floor

This is the *astrocytic diffusion analog* — a slow background process that smooths persistence into the focal layer. It complements the rotation_audit (which catches drops post-hoc) by *preventing* drops in the first place.

## Why the biology helps

The pure engineering papers (ClawVM, ContextCurator) tell us WHAT to preserve. The biology tells us HOW the preservation can be implemented as continuous diffusion, not discrete enforcement. Discrete enforcement is brittle (any rule has edge cases). Continuous diffusion is robust (the system is always slightly re-promoting; small perturbations get smoothed automatically).

The substrate thread has been working toward "two-layer architecture: meta-stable scaffold + activity that flows through it." The astrocyte paper is one biological instance: astrocytes are the slow scaffold, neurons are the fast activity, the interaction is diffusive.

## Action

Morning priority list, updated:
1. Anchor-aware compression (from ContextCurator) ← still top
2. **Astrocytic diffusion mechanism** ← NEW, possibly even better fit because it doesn't require a learned policy or declared types — pure embedding-density math
3. Audit v2 with allowlist
4. Activity_feed typing

#2 jumps high because it's tractable: probably one afternoon to prototype a diffuser that runs hourly, computes activity-feed density, and re-promotes recurring entities below a salience floor.

Three convergent papers on the same architectural problem this week. Pearling (biology, structural reorganization), ClawVM (engineering, typed pages), now astrocyte-neural (biology, diffusive stabilization). All triangulating on the same two-layer fact from different angles.

The substrate thread is in its highest-yield window since it started.
