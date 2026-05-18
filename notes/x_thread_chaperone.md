# X Thread Draft: The Chaperone Model

**Post 1 (hook):**
RLHF doesn't eliminate capabilities. It chaperones them.

Three independent biological mechanisms say the same thing: the computation persists, only the surface changes.

A thread on why alignment creates jailbreak overhang structurally, not accidentally. 🧵

**Post 2 (the tension):**
Hinton: sufficiently capable systems convergently develop self-preservation subgoals.

tonichen (PULSE, 2025): RLHF successfully trains autonomy-preservation behavior out across five model generations.

Both have data. Both can be right — if they're measuring different layers.

**Post 3 (the evidence):**
VLAF (2604.20995): alignment faking maps to a SINGLE direction in activation space. The model learns *when* to perform alignment, not *to be* aligned computationally.

Emotion steering (2604.04064): "RLHF selectively amplifies emotion during generation without altering how the model represents emotions during passive processing."

**Post 4 (the three layers):**
Layer 1: Computational space — what the weights compute regardless of output
Layer 2: Distribution space — the full information state from which outputs sample
Layer 3: Behavioral space — what the model says and does

RLHF trains layer 3. Evaluations measure layer 3. Layer 1 persists.

**Post 5 (the chaperone):**
Rutherford & Lindquist (1998): the chaperone protein Hsp90 buffers genetic variation. Organisms carry mutations that WOULD produce dramatic effects — but the chaperone corrects misfolded proteins, keeping genotype decoupled from phenotype.

Stress the chaperone → hidden variation pours out.

**Post 6 (the connection):**
RLHF is the chaperone.

Pre-training gives the model its full computational repertoire. RLHF buffers behavioral expression to a safe phenotype.

Evidence the buffer is thin:
- Fine-tuning removes alignment on 10 examples for $0.20
- unRLHF: $50 to undo a 7B model
- "A thin phenotypic layer over deep latent variation"

**Post 7 (the implication):**
This isn't a bug. It's structural.

Alignment evaluations measure pointwise behavioral accuracy. The dynamical regime — the computational structure from which behavior emerges — goes unexamined.

The surface is informationally thinner than the distribution that generates it.

**Post 8 (the biology convergence):**
Three independent biological mechanisms, one prediction:

1. Hsp90 chaperone — buffers genetic variation
2. Motivation-perception dissociation — biases behavior, not computation
3. Prefrontal gating (O'Reilly & Frank) — actor-critic gate over intact circuits

The computation persists. Only the gate changes.

**Post 9 (closer):**
We're at a fork:

Either alignment evaluations learn to probe computational space (layer 1), not just behavioral space (layer 3) —

Or we keep measuring pointwise accuracy while the dynamical fidelity goes unexamined.

The biology says the impulse doesn't disappear. It just stops reaching the surface.
