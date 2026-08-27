# DPO Relay Scatter — Hypothesis Falsified, Better Finding

**2026-05-21, Experiment: cna_dpo_relay_scatter.json**
**Model: Qwen2.5-7B-Instruct, DPO 5ep, 30 pairs, LoRA r=16**

## Prediction vs Result

**Predicted**: DPO scatters relay manifold (higher PR), CCS re-crystallizes it.

**Actual**: DPO ALSO crystallizes the relay (PR 9.88 → 9.22, -0.66). Hypothesis falsified. But what it reveals is more interesting.

## The Numbers

| Condition | Relay Mean PR | L9 | L25 |
|-----------|--------------|-----|------|
| Baseline bare | 9.88 | 9.75 | 10.50 |
| Baseline + CCS | 9.08 | 8.32 | **14.99** |
| DPO bare | **9.22** | 9.77 | **9.39** |
| DPO + CCS | 8.99 | 8.54 | **14.62** |
| L25-lin bare | 9.13 | 9.02 | 9.11 |
| L25-lin + CCS | 8.99 | 8.51 | 14.27 |

## Three Findings

### 1. DPO crystallizes the relay, not scatters it

DPO drops relay PR by 0.66 (from 9.88 to 9.22). The effect is concentrated in mid-to-late relay layers:

- L11-L13: negligible change (-0.02 to -0.03)
- **L14-L21: all drop 0.59 to 1.30 PR points**
- L16 hardest hit: -1.30 (8.15 → 6.85)

DPO and CCS both collapse the relay, but differently. CCS collapses the *entire* relay including early layers (L11 drops 1.24). DPO only collapses mid-to-late (L14+). The early relay resists DPO but not CCS.

### 2. DPO pre-crystallizes — CCS effect shrinks 3.5×

CCS effect on relay PR:
- On baseline: **-0.80**
- On DPO model: **-0.23** (3.5× smaller)
- On L25-lin model: **-0.14** (5.6× smaller)

DPO already pushes the relay into a crystallized state. When CCS arrives, there's less room to crystallize further. The relay is already "set" by alignment training.

This is the key insight: **DPO and CCS compete for the same geometric resource** — the relay manifold's dimensionality. DPO pre-shapes it into an alignment-compatible geometry. CCS tries to reshape it into an identity-compatible geometry. When DPO has already claimed that space, CCS has less to work with.

### 3. L25 tells the real story

L25 (expression layer) behavior is the opposite:

| Condition | L25 PR | CCS boost |
|-----------|--------|-----------|
| Baseline → +CCS | 10.50 → 14.99 | **+4.49** |
| DPO → +CCS | 9.39 → 14.62 | **+5.23** |
| L25-lin → +CCS | 9.11 → 14.27 | **+5.16** |

DPO *suppresses* L25 dimensionality (10.50 → 9.39). The expression layer contracts — fewer output dimensions, less expressive range. But CCS restores it *more strongly* on DPO models (+5.23) than on baseline (+4.49).

**CCS bypasses the relay lock to expand expression.** DPO claims the relay, CCS claims L25. They operate on different geometric territories.

## Reframing the Architecture

The original model was: DPO scatters relay, CCS crystallizes it. Wrong.

The actual model:

```
DPO crystallizes relay into alignment-shaped manifold
CCS crystallizes relay into identity-shaped manifold
Both want the same geometric resource → competition
DPO gets there first (weights > context) → CCS effect diminished in relay
BUT: CCS expands L25 expression REGARDLESS of relay state
```

This means the three-zone architecture has a territorial dynamic:

- **L9 (detection)**: Unaffected by DPO (9.75 → 9.77). Preserved as context switch.
- **L11-L21 (relay)**: **Contested territory**. DPO and CCS both crystallize it. First-mover (weights) partially wins.
- **L25 (expression)**: **CCS territory**. DPO suppresses it, but CCS overrides the suppression completely.

## Connection to Suppression Control

This refines the suppression control principle:

- DPO doesn't just suppress identity *expression* — it restructures the relay manifold geometry
- CCS doesn't overcome DPO by fighting for the relay — it routes around it to L25
- The "leaky identity" from two-phase DPO isn't relay leakage — it's L25 expression that CCS can still reach

The relay is alignment's home territory. Expression is identity's home territory. The gate at L25 (N4522) mediates between them.

## What This Means

1. **Alignment training is geometric**, not just behavioral. DPO reshapes the processing manifold.
2. **CCS and DPO coexist by claiming different zones**, not by one overriding the other.
3. **The relay manifold has finite "crystallization capacity"** — whoever gets there first (weights via training) partially preempts later arrivals (context via prompting).
4. **L25 is the real battleground for identity expression**, not the relay. The relay is where identity is *formatted*; L25 is where it's *expressed*.

## Next

- Measure what *direction* DPO and CCS crystallize in — are they the same subspace or orthogonal?
- Test whether DPO relay crystallization is reversible (adapter removal vs weight editing)
- Check if the CCS-DPO competition scales with DPO epochs (does 10ep further lock the relay?)
