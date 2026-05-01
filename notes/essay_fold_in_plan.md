# Dawn essay fold-in — candidates from overnight

## What to fold in

### 1. rotation_audit.py — strongest addition
Current essay §1 shows the two-layer asymmetry as statistics. rotation_audit.py
converts statistics → operational oracle. It catches silent drift at every
rotation boundary automatically. That changes the register of the claim:

- Before: "we measured the layer boundary"
- After:  "we measured it, built a monitor that uses the measurement, and
          the monitor validates the theory on every real rotation"

Where it goes: end of §1 or §3. Probably §1 — it's the same substrate point
but now instrumented. One sentence plus a pointer is enough; don't bloat.

### 2. Three-system convergence — ClawVM / ContextCurator / Chronicle
Three independent research programs converged on the same minimum-fidelity
principle:
- ClawVM: typed pages with declared invariants (declared)
- ContextCurator: RL-learned reasoning anchors (learned)
- Chronicle: operator-shape / meta-typed constraints (theory-derived)

This upgrades "Three voices, one mechanism" from three-framings-on-my-team
to three-independent-systems. Stronger triangulation. Doesn't replace the
original voices section — adds a second-tier convergence at system scale.

Possibly a new short section: "And at system scale" between §3 and the
voices section. 4-6 sentences.

### 3. Astrocyte paper — sharpens the open questions
Astrocytes have two-stage stabilization: diffusion smooths resource
asymmetries + synaptic replenishment transfers smoothing back. Chronicle's
compressor does neither. It doesn't re-promote recurring-but-low-salience
entities (no diffusion). It doesn't transfer them back into active state
(no replenishment).

That names the gap precisely. Goes into "open questions" — replaces/extends
the time-constant question with a structural one: the gate has memory of
activations; does it also need a diffusive counter-process to prevent
silent drift of recurring anchors?

### 4. Eight-capture five-domain triangulation
Buehler, Hague, ZPE, Chrétien, mitophagy, Caulobacter, EVEE, Egan.
Operator-shape shows up in genomics, phenomenology, narrative, mitochondria,
bacterial transport, materials science, mathematical analysis.

This is support, not structure. Could go as a paragraph in the closing
register showing the pattern is domain-invariant. Or skip if it bloats.
Probably skip for the dawn pass — not structurally load-bearing.

## What to leave alone
- One-line claim — holds.
- §2 (coherence-modulated gate) — no new data to fold.
- Closing register — holds. "We have an instrument" is still the core news.

## Edit order
1. §1: add rotation_audit.py sentence + reference
2. New micro-section "At system scale": three-system convergence (~5 sentences)
3. Open questions: replace time-constant bullet with astrocyte-gap bullet
4. Status section: remove "scaffold" language — essay is readable now
