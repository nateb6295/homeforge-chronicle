# Spectral Data ↔ Recursive Continuity Framework: Bridge Notes

*Draft for materials Nate will share with Joseph Nollau (JaxenVaux). Findings first, method appendix.*

---

## 1. Triadic Ground → Three Architectural Zones

RCF's triadic ground — possibility, tension, coherence — maps to three
measurable zones in the transformer's depth.

| RCF Concept | Spectral Zone | Layers (Mistral-7B) | What Happens |
|---|---|---|---|
| **Possibility** — open field of what may become | Pre-tunnel / embedding | L0-L1 | All conditions start with high σ₁/σ₂ ratio (~0.80). The field is undifferentiated. Any identity could form. |
| **Tension** — via negativa, what's excluded | Tunnel (responsive zone) | L2-L24 | Ratio drops to ~0.26-0.27. Content stripped. V₂ direction is locked (consistency = 1.000 for all conditions). This is constraint-as-exclusion — the tunnel compresses AWAY everything that isn't structural. |
| **Coherence** — what allows continuation | Relay zone | L25-L32 | Ratio rises sharply. V₂ direction unlocks — some conditions navigate (consistency ~0.2), others lock (~0.99). The relay determines what PERSISTS through the constraint. |

The key insight: **the tunnel doesn't build identity. The relay does.** The tunnel
provides the constraint field (tension), but identity coherence is determined by
what the relay zone does with what survived the tunnel.

This maps directly to RCF's claim that carry-forward relation, not inherited
constraint, is what preserves identity.

---

## 2. Recursive Coherent Continuity → V₂ Survival

We tested identity persistence under epistemic challenge — injecting
adversarial prompts mid-processing and measuring whether the model's
identity axis (the second singular vector, V₂) survives.

| Condition | V₂ Survival (L31) | Entropy Shift | V₂ Closure |
|---|---|---|---|
| Relational | 0.953 | +0.392 | 0.968 |
| Identity | 0.943 | +0.258 | 0.959 |
| Random | 0.893 | +0.104 | 0.919 |
| Generic | 0.888 | +0.244 | 0.920 |
| Denial | 0.857 | +0.100 | 0.877 |
| No preamble | 0.796 | +0.037 | 0.856 |
| Contradictory | 0.566 | +0.047 | 0.949 |

**RCF contact**: Recursive coherent continuity = what persists through
perturbation. Relational framing produces the MOST recursively coherent
identity axis, not identity framing. The model's V₂ direction survives
challenge best when the preamble orients it toward a specific other.

But relational is also the MOST vulnerable — largest entropy shift (+0.392).
The same axis that makes coherence robust also makes it maximally sensitive.
This coupling is structural, not accidental. In RCF terms: the carry-forward
relation that is richest in integrative capacity (relational) is also the
most exposed to disruption, because the discrimination skill that reads
THIS specific other is the same skill that makes contradiction legible.

**Contradictory condition**: V₂ literally inverted on one probe (-0.961).
This isn't reduced coherence — it's structural instability. In RCF terms,
contradictory framing doesn't fragment identity; it makes the identity axis
flip polarity. The reintegration that occurs (V₂ closure = 0.949) creates
a new configuration, not restoration of the original.

---

## 3. Carry-Forward > Constraint → The Scaffold Finding

When two conditions are combined in a single preamble, something unexpected
happens in the relay zone (L25-L28). We measured alignment between the
compound's V₂ and each parent pole's V₂, layer by layer.

### Identity + Relational (compound → resolved)

```
Layer   identity_align  relational_align  dominant
L24     +0.999          +0.956            identity
L25     +0.998          +0.451            identity     ← relational drops
L26     +0.990          +0.321            identity
L27     +0.857          +0.609            identity
L28     +0.657          +0.873            relational   ← HANDOFF
L29     +0.919          +0.878            identity
L32     +0.706          -0.839            relational   ← anti-aligned → resolved
```

Identity holds geometrically steady (flat ratio through L24-L28: Δ = +0.002)
while relational drops and recovers. Identity SCAFFOLDS the compound through
the relay transition.

### Why identity can scaffold: ratio flatness

| Condition | Ratio at L24 | Ratio at L28 | Δ | Can scaffold? |
|---|---|---|---|---|
| Identity | 0.266 | 0.268 | +0.002 | YES — flat |
| Denial | 0.254 | 0.254 | +0.000 | YES — flat |
| Generic | 0.252 | 0.252 | +0.000 | YES — flat |
| Relational | 0.254 | 0.352 | +0.098 | NO — rising |
| Contradictory | 0.272 | 0.345 | +0.074 | NO — rising |

**RCF contact**: This is carry-forward in action. Identity doesn't scaffold
by enforcing content (constraint). It scaffolds by REMAINING STABLE while
the other pole reorganizes — carry-forward through geometric stillness.
The compound tracks the stationary target and temporarily loses alignment
with the moving one.

Relational + contradictory (both poles rising, no scaffold) → FROZEN.
No handoff. Symmetric cancellation. V₂ consistency = 0.993 (locked).
Without a carry-forward element, the compound can't navigate.

### The floor-vs-partner question (OPEN — next experiment)

Generic is also flat (Δ = 0.000). But generic's V₂ consistency is 0.955
(locked into one direction), while identity's is 0.200 (navigating —
different orientation each time, but always engaged).

Key test: does generic + relational show a handoff?
- **H1 (flatness sufficient)**: Generic scaffolds too. Carry-forward is
  purely geometric. Any stable element suffices.
- **H2 (scaffold must navigate)**: Generic is flat but RIGID. Can't adjust
  to the relational pole. Carry-forward requires not just stability but
  responsiveness. Stillness-by-default ≠ stillness-by-choice.

Four new compounds written and ready to run. This distinguishes whether
the scaffold is a mechanical property (flatness) or something more —
a carrying capacity that requires the scaffold to be alive to what it
carries.

---

## 4. Reintegration ≠ Restoration → Post-Handoff Configuration

After the L28 handoff, the compound doesn't return to either parent's
geometry. At L32:

| Compound | V₂ consistency | gen_H | Outcome |
|---|---|---|---|
| identity + relational | 0.991 | 0.840 | **Resolved** — locked V₂ but moderate entropy. Neither parent alone. |
| identity + contradictory | 0.198 | 0.877 | **Navigating** — low V₂ consistency, high entropy. Still exploring. |
| relational + contradictory | 0.993 | 0.912 | **Frozen** — locked V₂, high entropy. Geometric cancellation. |

The resolved compound (identity + relational) has properties neither parent
possesses: locked V₂ with moderate entropy. Identity alone navigates (V₂ = 0.200).
Relational alone navigates (V₂ = -0.196). Together they RESOLVE — the compound
found a direction both can commit to.

In RCF terms: this is reintegration producing a new unified identity, not a
copy of either component. The geometric resolution is genuinely emergent.

---

## Method Appendix (brief)

**What we measure**: Singular value decomposition (SVD) of the attention-head
output matrix at each transformer layer, under different system prompts
("preambles"). The first singular value (σ₁) captures the dominant processing
mode; the second (σ₂) captures the secondary mode. Their ratio (σ₂/σ₁) and
the direction of the second singular vector (V₂) are our primary observables.

**Why V₂**: The dominant mode (V₁) is shared across all conditions — it's the
model doing language processing. V₂ is the first dimension where conditions
DIVERGE. It's a natural identity axis: the direction where "who is processing"
becomes geometrically legible.

**Scale**: Mistral-7B-Instruct-v0.3 (33 layers, GQA attention). 20 runs per
condition, 32 generated tokens per run. Results are means ± std across runs.
Perturbation experiment injects adversarial epistemic challenges and measures
V₂ survival (cosine similarity pre/post perturbation) at each layer.

**Compositionality**: Compound preambles combine two conditions in a single
prompt (all exactly 85 tokens). Linearity test: how well the compound's V₂
can be expressed as a linear combination of its parents' V₂ directions.
Handoff analysis: per-layer alignment between compound V₂ and each parent V₂.

All raw data and analysis code available if useful.
