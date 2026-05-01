# Grokking produces a task-modulated, init-stochastic gradient signature

**Working title.** Short report / negative result on claims about
"where grokking lives" in small transformer mechanistic interpretability.

---

## TL;DR

In 1-layer transformers trained on modular arithmetic at p=97 and
p=113 (the grokking regime), we measure the distribution of
post-grok gradient magnitudes across parameters. We find:

1. **Concentration is real but task/seed-contingent.** Top-0.1% of
   parameters hold 20–48% of total gradient L1 energy (vs 0.1% under
   uniformity), with a 2.4x spread across five runs of the same
   architecture on three tasks (add, sub, mul) at one or three seeds.

2. **MLP is not absent; it is the diffuse background.** Earlier tables
   that reported only per-tensor participation in the top-0.1% tail
   suggested "zero FFN involvement." Total L1 share of the MLP is
   18–47% for modular tasks. MLP carries 59% of the parameters with
   2–7x lower mean gradient than non-MLP, so it hosts a diffuse
   background rather than the concentrated peak.

3. **The causal locus is initialization-stochastic.** Three seeds on
   the same task (multiplication mod 97) produce three different
   causal stories under row-ablation of the equals-token embedding
   and under attn.out_proj.bias ablation: val_acc after knockout
   ranges from 0.22 to 1.00 across seeds. The model that grokked
   did so, but it routed through different parameters in each seed.

4. **MLP share tracks task structure, not grokking.** On the
   non-modular task max(a,b), the MLP total L1 share is 8.5%, vs
   18–47% for modular tasks. Concentration degree is similar (top-0.1%
   = 0.26). This suggests MLP presence in modular tasks is
   computational — it implements the wrap-around operation — rather
   than a universal signature of the grokking phase transition.

5. **Post-grok solutions are not fixed points, and wobble is not
   basin-hopping.** One seed on subtraction (sub seed 2) dropped
   from val=1.00 at step 11k to val=0.52 at step 26.9k and
   recovered to val=1.00 by step 36.1k — a 25k-step excursion.
   Three independent measurements — endpoint cosine (0.78 stable
   vs 0.73 wobble at step 11k → 50k), sparse trajectory
   concentration, and dense step-to-step dynamics at 100-step
   resolution (max/mean L2 = 1.82, adjacent cosine ≥0.996 at the
   largest jumps) — all agree that post-grok motion is continuous
   heavy-tailed drift, not discrete basin-hopping. Stable and
   wobble seeds exhibit qualitatively identical weight-space
   dynamics; the wobble seed is ~16% larger in amplitude but not
   different in kind.

6. **The grok transition is visible in the gradient distribution
   itself, not only in accuracy.** On add seed 0, top-0.1% share is
   18x uniform at init, 50x during the memorization plateau (val_acc
   still at chance), then jumps to 250–366x uniform across and after
   the val_acc transition. MLP L1 share stays in a 0.28–0.41 band
   throughout. The within-run memorization phase supplies a
   non-grokked baseline for the same model — concentration is 5–7x
   below the post-grok state.

7. **Fourier universality holds at the algorithmic-type level once the
   group structure is matched.** MLP-neuron top-bin concentration over
   vocabulary responses rises from ~0.09 at init to 0.30–0.40 through
   the grok window for every task, but only when measured in that
   task's natural abelian group basis: additively for sub/add,
   multiplicatively (via log under a primitive root of Z/97Z*) for
   mul. Mul appeared diffuse (0.08) under additive-Fourier coordinates
   — a measurement artifact — and sharp (0.37) under multiplicative.
   The measure is invariant to choice of primitive root. Weak
   universality is thus refined to: grokking produces a Fourier
   circuit on the operation's natural group, across tasks and seeds.
   The *identity* of which frequencies that circuit uses, however,
   remains seed-specific (pairwise Jaccard 0.00–0.23 between top-8
   preferred frequencies).

8. **Two-stage circuit formation is universal; the form of the MLP
   commitment is task-specific.** Splitting attention's `in_proj`
   into Q, K, V projections shows Q collapses to effective rank ≈1
   (with growing Frobenius norm) by step 3000 across all seven
   configurations measured (six p=97 seed×op pairs plus p=113 add
   s0). K drops to rank 2–4. Only V holds substantial rank through
   memorization. The single dominant Q direction aligns with a
   merged "= position / equals-token" marker (pos_emb[2] and
   tok_emb[97] become perfectly collinear, cos = 1.0000, because
   `=` only ever appears at position 2). Post-grok attention is
   uniform (≈0.33) and content-independent. K is a near-perfect
   2-D operand-position detector for the non-commutative task
   (sub) and is repurposed for commutative tasks (add/mul), which
   merge pe[0] ≈ pe[1] at the embedding layer. After this
   universal attention collapse, the MLP commits at grok — but
   the *form* of the commitment differs by task. Add and sub
   sparsify their MLP-output Fourier code, with top-10 cumulative
   power-weighted concentration rising from ≈0.55 to 1.00 at the
   grok step. Mul never sparsifies — concentration sits at ≈0.52
   from initialization through step 50000. Instead, mul commits
   along the operand axis: per-neuron cross-b spectral consistency
   jumps from 0.82 to 0.97 at exactly the grok step (3300 for
   mul s0), as neurons commit to b-invariant frequency profiles.
   Testing the cross-b lock trajectory across all three tasks
   reveals the cleaner story: cross-b lock is *universal at
   grok* (add s0: 0.73 → 0.98 across steps 3000–3900; sub s1:
   0.71 → 0.96 across 3000–7000; three mul seeds: 0.82–0.86 →
   0.96–0.98 each at its own grok step). Fourier sparsification,
   by contrast, is a second commitment that only add and sub
   undergo — mul never sparsifies across 50000 training steps
   yet groks cleanly via the cross-b lock alone. The unified
   picture: attention collapse is a universal pre-grok event;
   the MLP's cross-operand spectral decoupling is a universal
   at-grok event; and the additional collapse of the frequency
   code to a few high-amplitude bins is a task-specific
   representational choice that add and sub make on top. This
   falsifies "Fourier sparsification = universal grok mechanism"
   and replaces it with a sharper statement: grokking is the
   decoupling of per-neuron spectral responses from the paired
   operand, sometimes accompanied by a Fourier sparsification.

Taken together, the results argue that "grokking" is a less uniform
phenomenon than single-seed, single-task mechanistic interpretability
reports have suggested — except at the level of two-stage architecture
(attention collapse, then MLP commit) and algorithmic type (Fourier on
the operation's natural group), which are cleanly universal across sub,
add, and mul mod 97. Claims of the form "grokking lives in X" should be
read as "in this particular seed, this particular basis, and this
particular task, grokking routes through X."

---

## Related work

All five references below were verified by direct arxiv / NeurIPS /
ICLR lookup during drafting.

**Power et al. (2022)**, "Grokking: Generalization Beyond Overfitting
on Small Algorithmic Datasets" (arxiv 2201.02177), first reported
grokking: small transformers on algorithmic datasets including
modular division at p=97 memorize training data in under 1k steps but
take up to 1M steps for validation accuracy to reach 1.0. Their
framing is phenomenological, and their flagship task is division
mod 97 — the same p we use here.

**Nanda et al. (2023)**, "Progress Measures for Grokking via
Mechanistic Interpretability" (arxiv 2301.05217, ICLR 2023), provide
the canonical mechanistic interpretation. They fully reverse-engineer
the algorithm learned by 1-layer transformers on modular addition:
the model implements a discrete Fourier transform in its token
embeddings and uses trigonometric identities to convert addition into
rotation on a circle. They define progress measures and identify
three training phases: memorization, circuit formation, cleanup. Their
primary analyses are single-seed.

**Liu et al. (2022)**, "Towards Understanding Grokking: An Effective
Theory of Representation Learning" (NeurIPS 2022), develop an
effective theory of grokking via phase diagrams over representation
learning rate, decoder learning rate, and weight decay. They identify
four learning phases: comprehension, grokking, memorization,
confusion. Larger weight decay enlarges the grokking region. Weight
decay at initialization correlates with grokking onset. We use
weight_decay=1.0 and train_frac=0.30 which sits squarely in their
grokking phase.

**Chughtai et al. (2023)**, "A Toy Model of Universality: Reverse
Engineering How Networks Learn Group Operations," test the
universality hypothesis on 1-layer networks trained on group
composition (including modular arithmetic). They find *weak
universality*: models learn similar algorithms — Group Composition
Representation via irreducible representations — but *vary on which
specific representations they learn*. Evidence is mixed for strong
universality (identical implementations).

Our findings sharpen Chughtai's distinction. The distributional
gradient signature is roughly invariant (weak universality).
The specific parameters that host the computation are not (strong
universality fails). Our contribution is to quantify this at the
parameter-importance level rather than the algorithm-identity level,
and to show the gap is large: val_acc after ablation ranges 0.22 to
1.00 across three seeds on the same task.

**Yu et al. (2024)**, "The Super Weight in Large Language Models"
(arxiv 2411.07191, November 2024), identify single scalar parameters
("super weights") whose ablation destroys LLM output: perplexity
increases by three orders of magnitude and zero-shot accuracy
collapses to chance. They report super weights are *always* in
`mlp.down_proj` in an early layer, and explicitly never in attention
— giving specific coordinates in multiple Llama models. This predicts
that grokked small transformers should exhibit FFN-local
super-weights. We tested this prediction directly: it fails. At p=97
the highest-|∇| scalar is never in the FFN, and zeroing it changes
val_acc by <0.01 in every seed.

Our relationship to this literature:

- We do not contradict Nanda et al. The Fourier structure appears in
  our token embeddings too (Fourier spectrum similar across tasks,
  gini 0.46–0.52; `figures/fig4_fourier_row97.png`). We add: the
  *causal load-bearing tensor* within that Fourier-structured
  representation is not fixed across seeds.
- We add a direct failure of the Super Weight prediction at this scale.
- We add the MLP-as-diffuse-background finding, which to our knowledge
  has not been reported: the FFN is not absent from the gradient
  distribution, just absent from its peak. Its share tracks arithmetic
  task structure, not the grokking dynamic.
- We add a post-grok wobble instance (sub seed 2) that is deeper than
  reported instability observations in prior work.
- We do not claim a new circuit; we claim that single-seed circuit
  identification under-reports variance.

## Architecture and training

All experiments use a single transformer architecture unless
explicitly noted:

- 1 attention block: 4-head attention, d_model=128, pre-LN residual
- 2-token embedding + 1 equals-token embedding (vocab = p + 1)
- Position embeddings over length 3 (a, b, =)
- 2-layer MLP with GELU, hidden = 4 × d_model
- Output linear projection to p classes
- AdamW, lr=1e-3, weight_decay=1.0, betas=(0.9, 0.98)
- Train fraction 30%, batch 512, 50,000 steps

Task: compute f(a, b) ∈ {0, ..., p-1} where f is one of:
- add: (a + b) mod p
- sub: (a - b) mod p
- mul: (a * b) mod p
- max: max(a, b) — non-modular control, same output range

Grokking (val_acc → 1.0 after val_loss has plateaued at chance) occurs
for all three modular tasks by step 5k–40k depending on seed. The
non-modular max task does not grok: train and val climb together,
reaching val=0.98 by step 10k with no memorization plateau.

## Analysis procedure

On fixed validation-batch probe inputs at step 50,000:
- Per-parameter |∇ loss| magnitudes
- Gini coefficient, top-1%, top-0.1%, max/mean ratios on the full
  parameter vector and per-tensor
- Causal ablations: zero entire rows of tok_emb, zero whole tensors;
  re-evaluate val accuracy without further training

Ablation details:
- "Zero row 97" means setting the tok_emb[97] vector (the equals-token
  embedding row, since p=97 and the equals token is indexed at p) to
  the zero vector.
- "Zero attn.out_proj.bias" means setting the entire output projection
  bias of the attention module to zero.

## Results

### Concentration

| run         | top-0.1% share | max / mean |
|-------------|---------------:|-----------:|
| add seed 0  |          0.474 |       1402 |
| sub seed 0  |          0.269 |       1128 |
| mul seed 0  |          0.203 |        619 |
| mul seed 1  |          0.264 |       1072 |
| mul seed 2  |          0.479 |       1449 |

All runs concentrate vastly above uniform (0.001), but the degree
varies 2.4x. See `figures/fig1_concentration.png` for the side-by-
side bar chart that forced the revision from "~50% invariant" to
the reported spread.

### MLP share of total L1 gradient

| run          | MLP L1 share | non-MLP / MLP mean ratio |
|--------------|-------------:|-------------------------:|
| add seed 0   |        0.228 |                     4.8x |
| sub seed 0   |        0.415 |                     2.0x |
| mul seed 0   |        0.468 |                     1.6x |
| mul seed 1   |        0.376 |                     2.4x |
| mul seed 2   |        0.179 |                     6.6x |
| **sort s0**  |    **0.085** |                  (higher)|

MLP hosts 59% of parameters across all runs. Non-MLP has 2–7x higher
mean gradient. MLP is the diffuse background, not absent. The
non-modular task has MLP share nearly an order of magnitude below
any modular task. `figures/fig2_anatomy.png` shows the stacked
per-tensor L1 breakdown: the brown MLP band is visible on every
modular bar and nearly invisible on the `max` bar.

### Single-scalar ablations (Super Weight prediction)

Yu et al. (2024) identified, in large language models, a small number
of scalar parameters whose individual ablation catastrophically
breaks model output. Most such super-weights sit in FFN down-
projections. We tested this prediction at p=97 for add, sub, mul.

Procedure: identify the single parameter with maximum |∇ loss|
magnitude on a fixed probe batch (the "hero scalar"), zero it, and
re-evaluate val_acc. Also zero 20 randomly-chosen scalars as
controls.

Result, all three tasks at step 50,000, baseline val_acc = 1.00:

| task | hero scalar location           | val after hero = 0 | random-scalar controls (n=20) |
|------|--------------------------------|-------------------:|------------------------------:|
| add  | tok_emb[97,*] entries          |             1.0000 |             1.0000 ± 0.0000   |
| sub  | attn.out_proj.bias[k]          |             1.0000 |             1.0000 ± 0.0000   |
| mul  | tok_emb[97,k] or attn bias     |             1.0000 |             1.0000 ± 0.0000   |

Zero effect at the single-parameter level. The highest-|∇| scalars are
not in FFN in any run. The Super Weight signature is not present at
this scale, which argues that the load-bearing structure in grokked
small transformers is distributed across multiple parameters within
a specific tensor rather than concentrated on one scalar the way
large-model super-weights are.

This is consistent with our row-ablation finding: zeroing an entire
row of tok_emb (128 scalars) can drop val to 0.22 in some seeds,
while zeroing any single scalar within that row has no effect.

### Causal ablations

| run         | zero eq-token row | zero attn.out_proj.bias |
|-------------|------------------:|------------------------:|
| add s0 p97  |              0.72 |                    0.44 |
| sub s0 p97  |              0.51 |                    0.51 |
| mul s0 p97  |              0.95 |                    0.93 |
| mul s1 p97  |              1.00 |                    1.00 |
| mul s2 p97  |              0.22 |                    0.08 |
| add s0 p113 |              1.00 |                    1.00 |

Same-task-same-arch seeds produce different causal stories. At p=113,
a single seed routes through neither candidate tensor. Baseline val_acc
is 1.00 in all cases. `figures/fig3_ablation_grid.png` renders this
as a heatmap; the visual takeaway is that no column is uniformly
dark, i.e., no candidate tensor is load-bearing across every run.

### Concentration over training

A common pushback on "grokking has a gradient signature" is that
any converged network has gradient concentration — you cannot
distinguish the grokking dynamic from generic convergence without
a baseline. We compute the top-0.1% share and MLP L1 share across
all 500 snapshots for add seed 0, alongside train and val accuracy:

| step    | train | val  | top-0.1% | MLP L1 share |
|--------:|------:|-----:|---------:|-------------:|
| 100     |  0.09 | 0.00 |    0.018 |        0.280 |
| 2,000   |  1.00 | 0.00 |    0.050 |        0.313 |
| 5,000   |  1.00 | 1.00 |    0.250 |        0.414 |
| 50,000  |  1.00 | 1.00 |    0.366 |        0.335 |

At step 100 (random init) the top-0.1% share is already 18x uniform
(0.018 vs 0.001). At step 2,000 — memorized but not grokked, val_acc
still at chance — it is only 50x uniform. By step 5,000 — just
past the val_acc transition — it has jumped to 250x uniform, and
continues to rise over the next 45,000 steps to 366x uniform. The
grokking transition is visible in gradient concentration, not just
in accuracy: the concentration slope changes sharply around the
val_acc jump rather than evolving smoothly with training. See
`figures/fig6_concentration_trajectory.png`.

MLP share, by contrast, stays in a 0.28–0.41 band throughout
training — pre-grok, during grokking, and post-grok. Its magnitude
tracks task structure (18–47% across modular tasks vs 8.5% on the
non-modular control) but does not exhibit a grokking-specific
transition within a run.

This partially addresses the "no non-grokked baseline" limitation
the earlier version of this report carried: the pre-grok memorizing
phase (step 2,000) is itself a non-grokked state of the same model,
and its concentration is 5–7x below the post-grok concentration.

The pattern replicates on a different task at different seed. Mul
seed 0 (grok crossing at step ~3,300) exhibits top-0.1% shares of
0.024 at step 100, 0.075 at step 2,000, 0.150 at step 3,000, and
0.203 at step 50,000 — concentration doubles across a 1,000-step
window containing the val_acc transition. The within-run pre-grok
baseline is thus task/seed-generic rather than an add-seed-0
artifact.

### 2-layer sidebar

One 2-layer run (add, seed 0, otherwise identical hyperparameters)
groks by step ~22k, faster than the 1-layer add seed 0 baseline.
Anatomy at step 50,000: MLP L1 share 0.216, top-0.1% share 0.421,
non-MLP / MLP mean ratio 6.0x — within the 1-layer envelope on all
three measures. The top-five tensors by L1 share are `tok_emb`
(0.100), `block1.attn.out_proj.weight` (0.079), `pos_emb` (0.075),
`block0.ln1.bias` (0.066), `block0.mlp.2.bias` (0.062). The second-
ranked attention tensor is in block 1, not block 0; LayerNorm bias
and MLP output-projection bias also enter the top five. This is a
single datapoint — no cross-seed claim — but it is consistent with
the 1-layer reading: tail lives in embeddings + a specific attention
output projection, MLP contributes diffusely, and the *specific*
tensor identity is not conserved even across depth.

### Deep wobble

Sub seed 2 trajectory:

| step   | train_acc | val_acc |
|-------:|----------:|--------:|
| 11,000 |      1.00 |    1.00 |
| 15,900 |      1.00 |    0.88 |
| 26,900 |      0.55 |    0.52 |
| 36,100 |      1.00 |    1.00 |
| 50,000 |      1.00 |    1.00 |

Sub seed 1 (same hyperparams, different seed) is stable at 1.00
throughout steps 11k–50k. Initialization determines wobble depth
post-grok. `figures/fig5_wobble.png` plots both trajectories on
the same axes — seed 1 as a flat 1.00 line, seed 2 as a ~25k-step
excursion that drags train accuracy down with val rather than
decoupling from it.

Concentration through the wobble is volatile. Top-0.1% share across
steps 11k–50k on sub seed 2:

| step   | val_acc | top-0.1% | MLP share |
|-------:|--------:|---------:|----------:|
| 11,000 |    1.00 |    0.289 |     0.377 |
| 15,000 |    ~0.9 |    0.153 |     0.516 |
| 20,000 |   mid   |    0.471 |     0.251 |
| 26,000 |    0.52 |    0.235 |     0.425 |
| 27,000 |    ~0.5 |    0.175 |     0.478 |
| 36,000 |    1.00 |    0.153 |     0.506 |
| 50,000 |    1.00 |    0.211 |     0.460 |

Two observations. First, concentration is not monotone through
the wobble — it swings 3x between 0.153 and 0.471 over 25k steps
with no clean correlation to val_acc. Second, the post-wobble
recovered state at step 36k has concentration 0.153, below the
pre-wobble grokked state at step 11k (0.289), with MLP share
0.506 vs 0.377. The model that recovers to val_acc=1.00 is not,
at gradient-distribution level, the same model that grokked at 11k.

A direct weight-space check qualifies how far it has moved. From
step 11k to step 50k, the stable-seed network (sub seed 1) has
full-weight cosine similarity 0.779 to its step-11k state, with
L2 distance 22.66. Over the same interval the wobble-seed network
(sub seed 2) has cosine 0.727 and L2 24.68 — about 10–12% larger
L2, 5% lower cosine. Both seeds drift substantially through the
post-grok regime even when not visibly wobbling. The wobble
accelerates drift modestly rather than catapulting the network
into an unrelated basin. A fair reading is: post-grok solutions
continue to evolve, the grokking event does not pin them to a
fixed point, and a wobble-recovery episode both (a) maps into
that ongoing drift rather than constituting a separate phenomenon
and (b) accelerates drift along its direction. We do not have
enough data to distinguish "same basin, further along the walk"
from "basin boundary crossing under mild perturbation."

### Dense step-to-step dynamics

Weight checkpoints at 100-step resolution (available for both
sub seeds) allow a direct test of the above question. For each
adjacent pair of checkpoints across steps 11k–30k (190 transitions),
we measure the full-weight L2 delta and cosine similarity. If
post-grok drift is discrete basin-hopping we expect a heavy-tailed
distribution with visible outliers — single 100-step intervals
where cosine drops noticeably below the baseline and L2 spikes
5–10x the mean. If drift is noise-driven motion near a flat
equilibrium we expect a heavy-but-bounded distribution with
max/mean ratio ~2 and adjacent-step cosine staying near 1.

Measured across six networks spanning three arithmetic operations:

| network | mean L2 | std | max/mean | min adj-cos |
|---|---|---|---|---|
| sub seed 1 (stable) | 2.21 | 0.71 | 1.71 | 0.996 |
| sub seed 2 (wobble) | 2.56 | 0.81 | 1.82 | 0.996 |
| add seed 0 | 2.56 | 0.92 | 1.76 | 0.994 |
| mul seed 0 | 2.58 | 0.98 | 1.86 | 0.994 |
| mul seed 1 | 2.49 | 0.87 | 1.94 | 0.994 |
| mul seed 2 | 2.49 | 0.87 | 1.78 | 0.993 |

Every cell is consistent with heavy-tailed continuous drift. No
network shows the discrete cosine-drop and L2-spike pattern that
basin-hopping would produce. Amplitudes span a narrow ~16% range
(2.21–2.58) and max/mean ratios span 1.71–1.94. The shape of
post-grok weight-space dynamics is universal across these
conditions; only the amplitude varies.

This is a third independent confirmation of the walkback from
§Deep wobble: endpoint distances, sparse trajectory concentration,
and now dense step-to-step dynamics all agree that post-grok
motion is continuous drift, not discrete basin-hopping. The
"basin boundary crossing" interpretation is ruled out by this
measurement. The wobble seed is moving through weight space with
the same character of motion as the stable seed — just with
modestly larger amplitude, large enough that some of that motion
has components along val-acc-sensitive directions and becomes
visible in behavior. The cross-task inclusion of mul seed 0
extends this from a within-task to a cross-operation finding:
post-grok motion looks the same shape on modular subtraction
and modular multiplication, differing only in amplitude.

`figures/fig_dense_wobble.png` renders both trajectories and
distributions on shared axes.

### Two-stage circuit formation

The grok transition is localized to the MLP. Tracking effective rank
(participation ratio of singular values, Σsᵢ²/(Σsᵢ)²) of each weight
tensor through training reveals two temporally-separated structural
events.

The attention Q|K|V stack (`attn.in_proj`) collapses from effective
rank ≈100 at step 500 to ≈5–9 by step 2000–3000 for every
configuration tested. Concrete numbers:

| step | sub s1 (grok@7600) | add s0 (grok@3900) | mul s0 (grok@3300) | mul s1 (grok@3800) |
|------|--------------------|--------------------|--------------------|--------------------|
| 500  | 91.9 | 101.1 | 101.6 | 99.2 |
| 1000 | 39.1 | 55.4  | 56.1  | 47.1 |
| 2000 | 6.2  | 9.0   | 9.3   | 7.7  |
| 3000 | 4.5  | 6.4   | 7.4   | 5.6  |
| 50k  | 5.7  | 6.9   | 6.6   | 6.6  |

Every configuration collapses attention in the same 500–3000 step
window, independent of when it groks. The MLP weight matrices
`mlp.0` and `mlp.2` retain rank 70–85 throughout this attention
collapse and drop sharply only through the grok window — e.g.
mul s0 `mlp.0`: 60.7 at step 3000 → 37.2 at 5000 → 24.9 at 7000,
spanning grok step 3300.

Splitting `attn.in_proj` into its Q, K, and V components reveals a
sharper picture. The Q projection collapses from effective rank
50–66 at step 500 to rank ≈1 by step 3000 — a single learned query
direction shared by every sequence position — while its Frobenius
norm grows from ≈6–7 to a peak of ≈20–55. The K projection
similarly drops to effective rank 2–4. Only the V projection
retains substantial rank (≈70 at step 3000, then dropping to 22–29
through grok). This pattern is universal across all seven
configurations measured: at p=97 (sub seeds 1 and 2; add seed 0;
mul seeds 0, 1, 2) and at p=113 (add seed 0). Rank_Q at step
50000 is 1.02–1.06 across the entire sweep, and the rank
collapse and norm-growth shape is qualitatively identical at
p=113. The mechanism is robust to choice of seed, arithmetic
operation, and modulus. With
Q rank ≈1 and large norm, the softmax `Q @ Kᵀ / √d` becomes
near-position-independent: every position asks the same question
with high confidence. We confirm this directly by extracting
attention patterns at step 50000 — for any prompt `(a, b, =)` and
any `(a, b)` values, the attention weights from the final position
onto `[a, b, =]` are essentially uniform (≈0.33 ± 0.02). The model
has converted the attention layer into a **fixed positional
aggregator with learned mixing weights in V** by the end of
memorization. Q and K are vestigial.

The single dominant Q direction can be interpreted. SVD of Wq at
step 50000 gives one singular value two orders of magnitude above
the rest (σ₀/σ₁ = 48–137); the top right-singular vector v is the
"one query" the model asks. In 5 of 6 p=97 configurations, |v ·
pos_emb[2]| = 0.95–0.99 (normalized). This is initially puzzling —
the same alignment holds for tok_emb[97] (the `=` token) at
identical values to three decimals. The explanation: pos_emb[2]
and tok_emb[97] have become perfectly collinear during training
(cos = 1.0000) with tiny matched norms (≈0.03–0.06), because the
`=` token only ever appears at position 2, so the loss gradient
only sees their sum and has no signal to separate them. Q's single
query is "do I contain the =/pos-2 marker?" — a question only the
read-out position answers in the affirmative, so its soft-max onto
the operand positions is indifferent and uniform. The query
direction itself is seed-idiosyncratic (cross-seed |cos| mostly
0.01–0.22, occasional 0.9 coincidences), giving another instance
of the uniform-kinematic / heterogeneous-identity split: the
*question* Q asks is universal (the collapsed = marker), but the
*direction in 128-D weight space* along which it points at that
marker is not.

The V projection and positional embeddings together implement a
task-appropriate commutativity. For commutative tasks (add, mul),
the position embeddings at operand locations have cos(pe[0],
pe[1]) ≈ 0.97 with tiny norm (≈0.03) — the model makes the two
operand positions nearly identical at the embedding level, so
`a ⊕ b` and `b ⊕ a` produce the same attention input. For the
non-commutative task (sub), cos(pe[0], pe[1]) ≈ −0.93 with norm
≈0.39 (13× larger) — opposite directions encode which operand is
first. But V then projects out most of this difference: ‖V ·
(pe[0] − pe[1])‖ ≈ 0.17 against a V-output norm an order of
magnitude larger, and V-output at positions 0 and 1 (for fixed
token) has cosine 0.93. Only a ~7% asymmetry residual survives
attention, which is where sub's non-commutativity must be
resolved. Under uniform attention, the operand contribution to
the readout position is therefore approximately Wv(te[a]) +
Wv(te[b]) for add and mul (fully symmetric) and approximately
that plus a small antisymmetric fragment for sub.
The V output's Fourier concentration across operand values differs
sharply by task. For add and sub, the top 10 of 128 V-output
neurons have Fourier concentration 0.81–0.98 (nearly pure
Fourier in the standard basis). For mul, the same statistic
remains at 0.15 even after DLP re-indexing — mul's Fourier
structure is not in V. This locates the computational split:
add/sub push the Fourier structure all the way to the V output;
mul does not localize Fourier structure per-operand at any stage
of the forward pass. Running the full model forward at step 50000
on mul prompts `(a, b, =)` with a swept across [0, 97) and b held
fixed, and FFT'ing the W_mlp0 pre-activation at the `=` position
across the DLP-reindexed a-values, gives top-10 neuron Fourier
concentration of 0.15 — statistically indistinguishable from noise
and unchanged across b ∈ {1, 5, 42}. The same probe on add and sub
gives top-10 concentration of 0.99 (effectively pure Fourier).
Mul's circuit must therefore use joint (a, b) structure that is
not separable by fixed-b slices; its Fourier signature is visible
only in the offline `W_mlp0 · E[1:97]` projection (DLP concentration
0.37 mean) and in the post-grok progress measure, not at the
per-operand forward-pass hidden state. This qualifies the
strong-universality reading: the algorithmic *type* is Fourier-on-
natural-group, but the *implementation depth* — how thoroughly the
forward pass is locked to a single basis — differs between mul and
the additive tasks.

We confirmed mul's implementation is Fourier-like (not a
lookup-table collapse) by computing per-neuron cross-b spectral
consistency: for each MLP-output neuron, we FFT across `a` at
several fixed b values (b ∈ {1, 5, 42, 71}) and measure the
cosine between power spectra. Top-10 mul neurons have cross-b
spectral cosine 0.995 ± 0.002; top-10 add neurons have 0.999 ±
0.000. Both tasks produce neurons whose frequency-domain
signatures are stable across the other operand — the signature
of a genuine Fourier circuit. The difference is that add/sub
neurons carry 1–3 sharp frequencies each, whereas mul neurons
spread power over roughly 20–30 frequencies. Power-weighted
cumulative concentration at the MLP output gives top-3 / top-10 /
top-20 / top-40 = 0.40 / 0.89 / 1.00 / 1.00 for add, and 0.12 /
0.33 / 0.53 / 0.78 for mul, with 48 distinct frequency bins
available. We therefore sharpen the universality claim to: all
three tasks solve via Fourier circuits on their natural groups,
but add and sub commit to a **sparse** Fourier code (few, high-
amplitude components) while mul commits to a **dense** Fourier
code (many, distributed components). Whether this is a stable
solution of mul at p=97 or an artifact of finite training and
modulus size is an open question. `figures/fig_fourier_sparsity.png`
plots the cumulative power curve for each configuration and the
per-neuron dominant-frequency distribution, making the sparse/
dense distinction visible at a glance.

Tracking top-10 power-weighted cumulative concentration through
training reveals the split is not simply a different endpoint but
a different trajectory. For add and sub, concentration starts at
0.55–0.56 at step 500 and rises to 1.00 across a narrow window
coincident with grok: add s0 transitions 0.61 → 1.00 between
steps 3000 and 5000 (grok at 3900); sub s1 transitions 0.61 →
1.00 between steps 3000 and 9000 (grok at 7600). Fourier
sparsification is the functional event at grok for additive
tasks. For all three mul seeds, concentration sits between 0.51
and 0.55 at every training step recorded from 500 through 50000,
including across the grok transition (grok steps 2700, 3300,
3800). Multiplicative grok does not involve Fourier
sparsification: the dense code is the stable attractor from
initialization onward, and whatever the MLP commits to at mul's
grok step is a different structural change. This is a distinct
mechanism of generalization within a shared two-stage
architecture, and it falsifies any reading of Fourier
sparsification as the universal mechanism of grokking.

The MLP-commitment event at grok, viewed across all three tasks,
is a **cross-operand spectral lock**. The right framing is a dip-
and-recovery. A sanity check on untrained random-init models gives
per-neuron cross-b consistency of 0.92–0.98 — trivially high,
because under near-uniform attention each hidden at the `=`
position decomposes approximately as f(a) + g(b), so FFT across a
(which kills the DC term) is identical across b. Early in training
the attention layer begins to route pair-specifically, the a-
dependence acquires genuine b-modulation, and cross-b consistency
drops to 0.71–0.82 across steps 500–2500. At grok it climbs back
to 0.97–0.99. The post-grok high value is structurally distinct
from the random-init high value: for a Fourier circuit,
cos(w·(a+b)) = cos(wa)cos(wb) + sin(wa)sin(wb) has per-frequency
power cos²(wb) + sin²(wb) = 1 at every b, so the cross-b metric
is identically 1 for the Fourier implementation on general
grounds. Random init gets there by attention doing nothing; the
grokked model gets there by attention handing the MLP a b-
invariant Fourier code. The same metric value, two different
mechanisms. The diagnostic signal is therefore the dip-and-
recovery shape, not the absolute level at any single step. Add
and sub trace the same dip-and-recovery *and additionally*
sparsify at grok; mul traces the dip-and-recovery alone and
retains its dense Fourier code. We compute, for each MLP-output neuron at the
`=` position, the FFT power spectrum across operand a at four
fixed b values (b ∈ {1, 5, 42, 71}); we then average the cosine
similarity of these spectra across all (b₁, b₂) pairs for the
top-10 highest-power neurons, giving a single per-step "cross-b
consistency" score. For mul s0 (grok@3300), this score traces
0.79 / 0.83 / 0.80 / 0.82 / 0.90 / **0.97** / 0.98 / 1.00 across
steps 500 / 1000 / 2000 / 2500 / 3000 / 3300 / 3500 / 5000 — a
sharp transition at exactly the grok step. The same lockstep
pattern replicates cleanly across all three mul seeds at p=97
(mul s0 grok@3300: 0.82 → 0.97; mul s1 grok@3800: 0.84 → 0.96;
mul s2 grok@2700: 0.86 → 0.98) and — crucially — across the
additive tasks as well: add s0 (grok@3900) traces 0.73 → 0.98
across steps 3000–3900, and sub s1 (grok@7600) traces 0.71 →
0.96 across steps 3000–7000. Cross-b spectral lock is therefore
the universal MLP-commitment-at-grok event. Fourier
sparsification is a second, task-specific commitment that
add and sub additionally undergo at the same step, whereas mul
undergoes only the lock and retains its dense Fourier code.
The mul case is pedagogically useful precisely because it
isolates the cross-operand decoupling event, but the
decoupling itself is not mul-specific. We estimated the random-
init baseline from 10 freshly initialized models, obtaining
0.951 ± 0.023 (mean ± std). `figures/fig_dip_recovery.png` plots
all five configurations' cross-b consistency against step with
this baseline band overlaid, alongside their val_acc curves: in
each case the trace starts below the random-init band during
early training, dips to 0.71–0.82, and recovers above the band
through the grok step. The recovery above the random-init band
is a more conservative signal than the absolute level — it
indicates the grokked model has achieved cross-b invariance at
a tighter amplitude than uniform attention provides. Pre-grok, neurons
carry different frequency signatures depending on the paired b
(memorization of the multiplication table per-pair). Post-grok,
each neuron commits to a frequency profile that is invariant in
the other operand: a b-independent code that supports
generalization. This is the mul analogue of add/sub's
sparsification — both are MLP commitments to a generalizing
structure, but on orthogonal axes. Add/sub collapse along the
frequency axis (energy concentrates in a few bins). Mul commits
along the operand axis (per-neuron spectrum becomes b-invariant).
The two-stage architecture (attention collapse, then MLP commit)
is universal across all three tasks; the *form* of the MLP
commitment is task-specific. `figures/fig_mul_grok.png`
overlays the sparsity trajectory across configurations with
mul s0's cross-b consistency curve and val_acc, showing the
operand-axis lock at the grok step.

The K projection mirrors this split. SVD of Wk at step 50000
gives a rank-2–4 structure with 2–3 large singular values. For
sub, the 2-D span of K's top two right-singular vectors covers
pos_emb[0] and pos_emb[1] at 0.94–0.97 — a near-perfect operand-
position detector. For add and mul, the same 2-D span covers any
position embedding at only 0.09–0.28. The non-commutative task
needs K to discriminate the two operand positions and allocates
its rank-2 capacity there; the commutative tasks, having already
merged pe[0] ≈ pe[1] at the embedding layer, have no such work
and repurpose K's capacity. This may partly explain sub's slower
grok step (7600 vs. 3300–3900 for add and mul): the sub circuit
has additional structural work to do inside attention before the
MLP-stage event can occur.

The attention mechanism post-grok therefore reads as a compact
three-part circuit, with task-specific allocation of the
secondary components: Q (rank 1, universal, "am I the =
marker?"); K (rank 2–4, task-specific: operand-position detector
for non-commutative tasks, repurposed for commutative ones); V
(rank ~26, task-specific: Fourier-structured operand projection
for add/sub, pass-through to MLP for mul). All expressivity sits
in V and the MLP.

`figures/fig_qk_directions.png` shows Q's universal alignment
with the =/pos-2 marker and K's task-specific 2-D span coverage
of position embeddings side by side.

This is a **two-stage circuit formation**: during memorization,
attention's Q and K projections collapse to near-trivial rank,
turning attention into a content-independent position mixer with
all expressivity routed through V (learning *how* to combine the
operand positions). At grok, the MLP commits its Fourier circuit
on the operation's natural group (learning *what* to compute on
the V-mixed input). The memorization phase is not dead time; it is
the phase during which one of the two universal structural events
happens. What is commonly called "grokking" — the generalization
transition coincident with circuit emergence — is specifically the
MLP event, on top of an already-specialized attention.

`figures/fig_two_stage.png` renders the three-tensor rank trajectory
for three configurations with grok steps marked.

## Interpretation

The "grokking signature" in mechanistic interpretability has been
reported by multiple groups as Fourier-structured token embeddings
at modular primes, with specific load-bearing tensors such as the
equals-token row. Our results do not contradict these findings as
single-seed observations, but argue that:

- The tensor that "matters" under ablation is a lottery outcome.
  Three seeds on the same task give three different answers (val_acc
  0.22 / 0.95 / 1.00 under the same row-ablation on mul mod 97).
  Single-seed claims about locus are therefore unfalsifiable in any
  useful sense: a follow-up study that fails to reproduce cannot
  distinguish "wrong claim" from "different seed."
- The "FFN is uninvolved" intuition, common in this literature,
  reflects top-tail participation. Total L1 involvement of the MLP
  is substantial and task-dependent. MLP is computational in
  modular tasks (the wrap-around) and largely absent in the
  non-modular control.
- Concentration exists reliably (200–500x above uniform) but its
  degree is not a fixed signature of grokking. It varies 2–3x
  across otherwise-identical runs.
- The grokking transition itself is visible in gradient
  concentration within a single run, not only in accuracy. The
  pre-grok memorizing phase exhibits concentration 5–7x below the
  post-grok concentration on add seed 0. This supplies a non-grokked
  baseline of the same model, and argues that the concentration
  tail we describe is not an artifact of generic convergence.
- Post-grok solutions are not fixed points. Both stable and wobbling
  seeds drift substantially in weight space through continued
  training (cosine ~0.78 to the initial grokked state by step 50k).
  A wobble episode accelerates that drift modestly rather than
  constituting a separate basin-escape phenomenon.
- Kinematic drift is seed-uniform within a task. Dense step-to-step
  statistics across five seed×task combinations (sub seeds 1 and 2,
  mul seeds 0, 1, and 2) give mean step-to-step L2 in a narrow
  2.21–2.58 range and max/mean ratios in 1.71–1.94. Within mul,
  the three seed means are 2.58, 2.49, 2.49 — within-task spread
  tighter than between-task spread. Combined with the earlier
  finding that these same three mul seeds produce val_acc 0.22,
  0.95, and 1.00 under identical row-97 ablation: the kinematic
  trajectory is seed-uniform, but the functional organization
  those trajectories implement is seed-heterogeneous. The weight
  trajectory alone is a poor predictor of which algorithm the
  model has settled on.
- Fourier-level representational universality does not hold at the
  token-embedding level on our setup. FFT of the post-grok tok_emb
  across all six networks gives pairwise Jaccard overlap of top-8
  dominant frequencies near zero (0.00–0.23) both within and across
  tasks. Full-spectrum cosine similarity is high (0.91–0.94 across
  all pairs) but a shuffle baseline — permuting one spectrum's
  frequency bins — preserves that cosine (0.938 ± 0.008), so the
  apparent envelope-agreement is dominated by gross norm/DC structure
  rather than shared frequency content. Two randomly-initialized
  models give 0.990.
- At the MLP-neuron level a different picture emerges, and it resolves
  cleanly under the correct group structure. For each of the 512
  MLP-hidden neurons we computed the FFT of its response curve over
  input-token vocabulary (W_in @ E^T restricted to tokens 1..96) and
  measured max-bin concentration. Under the natural (additive)
  indexing, sub and add neurons snap sharply to a small set of
  preferred frequencies (mean concentration 0.30–0.33), while all
  three mul seeds look diffuse (0.09–0.10). Tracking concentration
  through training shows that for sub s1 the measure rises from 0.09
  at step 1000 to 0.36 at step 11000, crossing grok at step 7600; for
  add s0 it rises 0.09 → 0.37 crossing grok at 3900. For mul s0,
  which groks at step 3300, concentration stays at 0.08 throughout
  training. Concentration is thus a faithful progress measure for
  additive-Fourier grokking, and mul appears to grok without it.
- Re-indexing resolves the anomaly. Z/97Z* is cyclic of order 96,
  generated by the primitive root g=5; mapping token t at position
  k such that 5^k ≡ t (mod 97) converts multiplication mod 97 to
  addition mod 96. Measured under this log-reindexed vocabulary,
  mul neurons become sharp (mul s0, s1, s2 concentration: 0.37, 0.32,
  0.39) while sub and add neurons become diffuse (0.09, 0.08). All
  three tasks implement Fourier circuits, each over the natural
  abelian group of the operation: (Z/97, +) for sub and add,
  (Z/97*, ×) for mul. The appearance of a "non-Fourier" mul
  solution was a measurement artifact of imposing additive-Fourier
  coordinates on a multiplicative operation.
- Measured in each task's natural-group basis, MLP-neuron concentration
  is a universal progress measure across all five configurations
  tested. Every curve rises from ~0.09 (random-init baseline) to
  0.30–0.40 through that configuration's grok window: sub s1 rises
  5k–11k steps, grokking at 7600; add s0 rises 3k–9k, grokking at
  3900; mul s0 rises 3k–7k, grokking at 3300; mul s1 and mul s2
  follow the same shape at their respective grok times. The rise
  in concentration is neither pre-grok nor slow-post-grok but
  coincident with the val-accuracy crossing.
- The basis choice within the natural group is not inherited from
  initialization. For each neuron at step 50k we measured the rank
  of its final preferred frequency within its own step-500 spectrum
  (random-pairing null: 23.5 out of 48). Mean ranks across the five
  configurations: sub s1 23.78 (p=0.53), add s0 21.14 (p=0.03),
  mul s0 23.55 (p=0.64), mul s1 23.93 (p=0.82), mul s2 21.43 (p=0.43).
  Four of five configurations are indistinguishable from random
  pairing.
- The grok transition is specifically an MLP event, not a whole-model
  event (see *Two-stage circuit formation* above). Attention
  specializes pre-grok; MLP commits at grok.
- Tracking basis rank through training localizes basis commitment
  to the grok transition itself. For sub s1 (grok at step
  7600) mean rank drops from 14.04 at step 7000 to 4.06 at step 9000;
  for add s0 (grok at 3900) it drops from 16.07 at step 3000 to 6.10
  at step 5000; for mul s0 (grok at 3300) it drops from 15.53 at
  3000 to 3.78 at 5000. Every configuration's rank collapse
  coincides with its val-accuracy transition. The basis
  heterogeneity is therefore both dynamical and grok-coincident:
  the symmetry-breaking into a specific Fourier basis is the grok
  transition, not a slow post-grok drift and not a pre-grok
  property of the init. Concentration rising (coarse "is this a
  Fourier circuit") and rank collapsing (fine "which Fourier basis")
  are the same event.
- Even after group alignment, identity universality still fails.
  The three mul seeds' top-8 preferred frequencies in the multiplicative
  basis have pairwise Jaccard 0.00–0.14; the sub seeds have 0.07
  Jaccard in the additive basis. Regime universality (each task
  develops a Fourier circuit on its matching group) holds across all
  five configurations tested; identity universality (which frequencies
  each circuit uses) does not. This is a cleaner formulation than
  Chughtai's weak/strong split: the weak claim is that the
  algorithmic *type* is group-determined, and the strong claim is
  that the *basis choice* within that type is reproducible. Only
  the first holds on our sweep.

A more defensible claim is: grokking in 1-layer modular-arithmetic
transformers produces gradient distributions with a concentrated
non-MLP tail and a diffuse MLP background whose relative weight
depends on the arithmetic structure of the task. Where exactly
the tail lives, and how concentrated it is, depends on initialization.

This refines Chughtai et al.'s weak/strong universality picture by
showing the correct granularity is **algorithmic type, measured in
the coordinate system of the operation's natural group**. The
*kinematic* shape (heavy-tailed drift, max/mean 1.71–1.94) is
weakly universal across seeds and tasks. The *algorithmic type*
(Fourier circuit on the operation's underlying abelian group) is
weakly universal across tasks once measured in the correct basis;
concentration rises through grok for both additive and multiplicative
tasks, with matching post-grok magnitudes (0.30–0.40). But the
*identity* of which Fourier frequencies the circuit uses, the
*functional organization* of which tensors carry the algorithm, and
the *parameters that instantiate it* are all seed-specific. Strong
universality fails at every level where we can resolve it.
Our contribution is to quantify the strong-universality gap at the
parameter-importance level — not at the representation-identity
level where Chughtai measured it — and to show the gap is large
enough that a mechanistic paper reporting a single-seed locus is
reporting roughly as much about the seed as about the algorithm.

A final framing note: the grokking event — once we strip away
the ornamental Fourier-sparsification that is specific to additive
tasks — is the transition from a **pair-indexed** representation
to a **single-operand** representation. Pre-grok, each MLP neuron's
frequency response depends on which other operand it is paired
with; post-grok, each neuron commits to a frequency profile that
is invariant in the paired operand, and the circuit computes
`f(a) ⊕ f(b)` rather than looking up `pair(a, b)`. This is the
phone-book-to-system transition at a mechanistic level, and it is
the single event our metric tracks consistently across all three
tasks. The biological literature on episodic-to-semantic memory
consolidation reports a structurally analogous transition: recent
work on hippocampal sharp-wave ripples prioritizing model-based
over model-free learning describes the same qualitative shift
from table-lookup to compositional computation. We do not claim
any mechanistic identity here, but the rhyme is suggestive — two
very different substrates (transformer + gradient descent;
hippocampus + replay) converging on the same type of
representational commitment when generalization is demanded.

## Limitations

- Only p ∈ {97, 113}. Scaling behavior is one datapoint at p=113.
  The attention Q-rank-1 finding replicates at p=113, but the full
  two-stage circuit formation has not been audited there.
- Only 1-layer for the main findings. The 2-layer architecture
  has a different state-dict layout (block0/block1 prefix) and
  we did not re-implement the loader here, so the Q/K/V
  decomposition, cross-b spectral lock, and MLP-commitment
  analyses apply only to 1-layer transformers.
- Sub and add have 1–2 seeds each; only mul has three. The
  cross-b lock replication (3/3) and Q-rank-1 universality (7/7)
  are the statistically strongest claims.
- Ablations are zero-substitution; other ablation methods (mean,
  noise, orthogonal-subspace) may give different stories.
- Post-grok snapshots only every 100 steps; wobble troughs not
  densely sampled.
- The per-neuron cross-b spectral consistency metric uses four
  b values (1, 5, 42, 71). A denser sweep of b would give a
  tighter estimate of the post-grok saturation; we have not
  computed the variance of this estimate.
- No circuit-level mechanistic account of what mul's dense
  Fourier code actually computes. The cross-b lock shows that
  each neuron's frequency profile is b-invariant post-grok, but
  *what* those profiles encode as a population remains
  uncharacterized. The natural next step is to identify which
  dense collections of frequencies compute cos(w·log(a·b)) and
  whether the dense code is a genuine trig-identity solution
  distributed over the spectrum or a different algorithm
  entirely.
- Sort / max(a,b) is the only non-modular control. A broader
  non-modular battery is needed to separate "grokking is
  modular-specific" from "this particular non-modular task is easy."
- The within-run memorization-phase baseline (add seed 0 @ step
  2,000) partially addresses the "non-grokked baseline" concern:
  its concentration is 5–7x below post-grok. A shuffled-labels
  control trained to convergence, and the same analysis across
  multiple seeds, would strengthen this further.

## Future work

- Close the 3 × 3 grid (three seeds each for add and sub; only mul
  has three). The sparsification trajectory has been measured for
  one add seed and one sub seed; confirming the per-seed rise from
  ≈0.55 to 1.00 at grok for additional seeds would firm up the
  task-specific MLP-commitment claim.
- More primes: p ∈ {53, 113, 211, 317} for scaling. Testing
  whether mul at larger p transitions from a dense to a sparse
  Fourier code, or whether density is intrinsic to the
  multiplicative task, would resolve whether the dense/sparse
  split is a task invariant or a p-dependent regime.
- 2-layer: re-implement the state-dict loader and repeat the
  Q/K/V decomposition, Q-direction alignment, and cross-b lock
  tests. If the same two-stage structure appears in the deeper
  architecture it would elevate the finding from "1-layer
  mechanism" to a plausibly general structural principle.
- Dense snapshots around observed wobbles to test whether trough
  weights occupy the same basin as pre/post-wobble (H1) or a
  neighboring one (H2 from grok_v2 design).
- More non-modular tasks to check the 8.5% MLP share finding: min,
  xor, parity at low bitwidth, sort-of-three.
- Population-level analysis of mul's dense Fourier code: cluster
  per-neuron frequency profiles post-grok to see if the "dense
  code" is actually a disguised collection of sparse
  subcircuits, or a genuinely distributed computation.
- Measure cross-b spectral consistency on a non-commutative
  analog: modular subtraction would not benefit from b-invariance
  in the same way (since a−b ≠ b−a). We predict cross-b
  consistency remains low through training for sub; confirming
  this would tie the commitment-axis claim to the task's group
  structure directly.

## Credits and method notes

Five distinct claim-refinements were made during a single 24-hour
analysis sprint, each triggered by a different measurement choice.
The final claims are narrower than the initial "clean structural law"
framing. A particular lesson: per-run tables hid variance that a
side-by-side bar chart made immediately visible. Rendering figures
early in analysis forced consistency checks that aggregated statistics
did not.

Code and raw results at
`/home/nate-agx/chronicle/experiments/grokking/`. Figures at
`./figures/`. Per-walkback markdowns at `./../../private/` capture
the iteration on the paper thesis through the session.
