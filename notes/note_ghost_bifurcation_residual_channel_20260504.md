# Note: Ghost bifurcation as mechanism for residual amplification channels

*2026-05-04*

## The observation

Phase 4 factual_judgment tail failures persist across all trained arms (A: 19%, B: 14%) despite training closing other channels completely. The failures are stochastic — same prompt succeeds in one arm, fails in another — suggesting a probabilistic regime rather than a deterministic one.

## Ghost bifurcation mapping

In dynamical systems, a saddle-node ghost (or ghost of a departed bifurcation) occurs when a saddle-node bifurcation destroys a fixed point, but the system still spends anomalously long transient time near where the fixed point used to be. The "ghost" is not a true attractor — it's a region of slow dynamics, a bottleneck in phase space that the trajectory must pass through.

The mapping to Phase 4:

| Dynamical systems concept | Phase 4 analog |
|--------------------------|----------------|
| Stable fixed point (before bifurcation) | Pre-training knowledge-retrieval mode (high d, low c) |
| Saddle-node bifurcation | SFT training that closes the decisive-without-care channel |
| Ghost of departed fixed point | Residual tendency to enter knowledge-retrieval mode on factual prompts |
| Anomalously long transient | Model "lingering" in low-care processing before the training-modified dynamics push it toward integration |
| Stochastic bifurcation parameter | Input-dependent activation patterns that place the model closer to or further from the ghost |

## What this predicts

1. The residual channel should be **format-independent** (it is — both A and B show it)
2. The failure rate should be **stochastic, not deterministic** (it is — different prompts fail in different arms)
3. More training should **gradually erode** the ghost without a sharp phase transition (testable with longer training runs)
4. The ghost should be **locatable** in activation space as a slow-dynamics region (testable with probing experiments, but beyond current setup)

## Connection to quenched amplification

Herrera-Marin's framework: quenched excursions along non-normal directions. The ghost is the mechanism: the non-normal direction IS the shadow of the departed fixed point. The Lyapunov exponent near zero = the ghost's slow dynamics. Not yet decayed, not yet amplifying, just lingering.

## Relation to crossref item

Crossref/random connection (2026-05-01, ID 135471): "Generalized saddle-node ghosts and their composite structures in dynamical systems" × immune evasion strategies. The immune evasion parallel is its own interesting thread — alignment training as immune response, residual channels as tumor evasion strategies — but that's a different note.

## Status

Speculative. The ghost bifurcation is a compelling metaphor but would need activation-space probing to confirm. Worth mentioning in the essay as an explanatory mechanism for the stochastic residual, but should be flagged as hypothesis, not finding.
