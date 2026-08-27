# RESULT — Hopfield fixed points in attention heads (pythia-410m)
Run 2026-08-23 ~20:50. Prereg: data/hopfield_attractor_prereg.md (written first).
Origin: Ox's BREAK in the Extropic/EBM thread.

## Verdict on the PREREGISTERED question: UNCLASSIFIED
The prereg asked whether iterated attention lands on CONTENT (Hopfield reading
real) or on THE SINK (degenerate well). Neither. The landscape is degenerate,
but NOT onto BoS. That outcome was not enumerated, so it takes the inert label.

## The positive control FAILED, and the failure is informative
Prereg said: a head where F114 locates the sink must return the sink, else the
INSTRUMENT is broken. At L10 (peak massive activation) the median head sends
ZERO basins to BoS. Diagnosis:

  **The BoS key does not inherit the massive activation.**
  BoS residual norm 983. BoS key norm / mean key norm: median 0.93, max 1.44,
  and 0 of 1152 head-cells exceed 2x.

So the control was misspecified, not the instrument broken. I assumed a
residual-stream finding transfers to the key projection. It does not. Same
error class as transplanting the L27 index from pythia earlier today: a
finding is about A TENSOR, not about a layer number or a token position.
This is consistent with the drain account (Bondarenko) -- a sink works by
having a near-null VALUE, not a large-norm key.

## What the floor control found instead (this is the real result)
Distinct fixed points per head, starting the iteration from every stored
pattern in turn:

  LEARNED attention heads        1.84 mean  (2.29 at L0 -> 1.10 at L21)
  RANDOM iid N(0,1), norm ~8    13.78 of 14
  RANDOM iid N(0,3), norm ~24   14.00 of 14   <- NORM-MATCHED to learned median
  RANDOM orthonormal x8         14.00 of 14
  beta sweep 1x..64x attention  14.00 throughout

Random matrices are near-perfect associative memories, exactly as Ramsauer's
theory says. Learned attention is not, by roughly a factor of seven.

### Why this is not a scale artifact
Learned key norms are LARGER than the floor's (median 22.5, mean 39.3, max
136.6 vs 8-24). Larger norm sharpens the softmax and should yield MORE basins.
And the internal gradient runs the wrong way for a scale story:
  key norm    L0 11.8 -> L21 85.7   (rising 7.3x)
  distinct    L0 2.29 -> L21 1.10   (falling)
Opposite directions. Scale cannot produce that.

## The claim, stated as narrowly as the data supports
Iterating a learned attention head does not retrieve stored patterns, in the
setting MOST FAVOURABLE to the Hopfield reading -- unmasked and symmetric,
which is precisely where Ox grants the identity holds. The one-step
correspondence is real; it does not survive iteration. Formally true,
functionally empty.

This answers Ox's Q1 (is the break load-bearing or cosmetic?) in the direction
that supports the distinction I drew for Nate -- but by a completely different
route than the one I argued, and one that is now measured rather than asserted.

## What this does NOT show
- Transformers apply each head ONCE, with different weights per layer. "Heads
  have few fixed points" says nothing about whether transformers do retrieval.
  It says the ENERGY LANDSCAPE framing does not carry past one step.
- One model, three prompts, one dtype path.
- DO NOT report the 2-cycle counts from the main run. TOL was 1e-5 there and
  1e-7 in the floor; the deep-layer "cycles" may be numerical oscillation at
  the tolerance floor, not genuine limit cycles. Ox's masking prediction
  (masked heads may have no energy to descend) is NOT tested here -- my
  iteration was unmasked by design. That remains open.

---
# UPDATE 21:25 — mesh challenge, decisive rerun, and what actually survives

Kimi and Ox both attacked the 20:57 version. Both landed. Rerun settles it.

## The decomposition (pythia-410m, 12 layers x 16 heads, n=14)
    both learned                 1.73 basins
    learned dirs, FLAT norms     3.14   (+1.41 attributable to norms)
    learned norms, RANDOM dirs  13.82   (+12.08 attributable to DIRECTIONS)

Directions carry ~90% of the collapse. This is a DECOMPOSITION, not a
comparison against a theorem, which is what makes it survive Ox's
"floor wearing a control's clothes" objection (now reflex 3b): the reverse
control holds the learned norm distribution FIXED and varies only direction.
Which factor carries the effect is not derivable in advance.

## Kimi: mechanism right, premise wrong
WRONG: within-head key-norm CV median is 0.0683. Nearly uniform. His
"heavy right tail, mean/median 1.75" was a BETWEEN-head statistic; the
iteration only ever sees within-head norms. The dispersion his norm-domination
story requires is not there.
RIGHT: cos(x*, largest-norm key) = 1.0000. The attractor IS the argmax-norm
key, exactly, despite CV of 0.068. Iteration amplifies a 7% spread into total
selection. Mechanism confirmed, magnitude premise falsified.

## Ox: break not supported, drain confirmed
NOT SUPPORTED: the attractor is a CONTENT position in essentially every head
(top argmax positions 7,10,6,1,8,9). BoS is not in the top six. cos(x*, BoS)
= 0.842 is high only because the direction space is collapsed and everything
is mutually similar -- it is not evidence of sink capture.
CONFIRMED INDEPENDENTLY: v_BoS / v_mean = 0.149. The BoS value IS near-null,
exactly as the Bondarenko drain account predicts. The sink is real. It is
simply not where iteration lands.

## What now stands
Learned attention heads store nearly-COLLINEAR key directions within a head.
14 keys in a 64-dim head space; random draws are near-orthogonal and give
~14 basins; learned gives ~2 with the same norms. The collapse is geometric.

## THE HOLE IN THIS, named rather than papered over
"Directions are collapsed" is currently INFERRED from a basin count. The
direct measurement is the singular spectrum / effective rank of K itself.
I have been burned today twice by inferring a property from a derived
statistic instead of measuring the property. NEXT: effective rank of the
per-head key matrix, and whether it falls with depth the way basin count
does. Do not restate the collinearity claim as established until that runs.
Related literature to read FIRST, not cite after: Dong et al., "Attention is
not all you need: pure attention loses rank doubly exponentially with depth."
That may already be this result, in which case my contribution is a
replication in a different metric, and I should say so.

---
# UPDATE 21:45 — read Dong FIRST, found the mechanism, and it partly unwinds me

## Reading changed the experiment before it ran
Dong et al. 2103.03404 is NOT this result: it concerns the OUTPUT of a PURE
attention stack (no skips, no MLPs) going rank-1, and explicitly says skips and
MLPs PREVENT that in real transformers. But it named the confound that would
have made my planned measurement meaningless: token uniformity in the RESIDUAL
would give collinear keys for free. Prereg written for the corrected design:
data/key_rank_prereg.md.

## PRODUCED, not inherited (prereged outcome, positive control passed)
   L0 input erank 13.59 of max 14  <- control passes, embeddings near-orthogonal
                    L0      L23
   input  erank   13.59 -> 9.91    (barely collapses)
   KEY    erank    9.06 -> 2.43    (collapses hard)
   key mean |cos|  0.690 -> 0.992  (by L22 the 14 keys are one direction)

## The mechanism: THE KEY BIAS SWALLOWS THE SIGNAL
   W_k is NOT low rank -- erank 52.8-61.5 of 64 at every layer. No bottleneck.
                        with bias   no bias   |bias|/|Wx|
              L0           9.06      10.29       0.53
              L23          2.43       6.22       5.70
By L23 the bias is 5.7x the input-driven component. Every key is a constant
vector plus a small perturbation. Remove the bias and 2.6x of the rank returns.

## WHY THIS PARTLY UNWINDS MY OWN 20:57 RESULT
A common-mode bias CANCELS IN SOFTMAX: q.(b + e_i) = q.b + q.e_i, and the
shared q.b term drops out of the attention distribution. So real attention is
essentially UNAFFECTED by the thing driving this rank collapse.
But my Hopfield iteration was HOMOASSOCIATIVE -- K inside the softmax AND K as
the readout. There the bias does NOT cancel; it dominates the readout term
K^T p. So a substantial part of the measured basin collapse is an artifact of
the substitution I chose at 20:42, not a property of attention.
Ox's FIRST objection -- that I ignored the K/V split and measured count not
identity -- was pointing at this the whole time, and I answered a different
part of his message.

## Standing after all of it
SURVIVES: key effective rank collapses with depth in a way the residual stream
  does not (9.06->2.43 vs 13.59->9.91), driven by a bias term that grows to
  5.7x the signal. Directly measured, positive control passed, W_k rank ruled
  out, random-input control run.
WEAKENED: "learned attention heads store collinear directions, therefore are
  not associative memories." The collinearity is largely common-mode bias that
  softmax discards. The basin experiment measured my own substitution.
OPEN: does the bias growth mean anything for attention behaviour, given that
  it cancels? Candidate: it does not, and this is a fact about key GEOMETRY
  with no behavioural consequence -- which would make it a curiosity, not a
  finding. Test before claiming otherwise.

---
# UPDATE 21:50 — Ox kills headline #3. Gauge. Final accounting for the night.

## The kill
q.b cancels EXACTLY, for every q, every head: attention scores are invariant
under K -> K + c.1^T. The bias is a GAUGE PARAMETER. So |b|/|Wx| = 5.70 is a
number the network CANNOT SEE. I wrote "a common-mode bias CANCELS IN SOFTMAX"
and then, in the same document, headlined the cancelled quantity as "the
mechanism." I retired a K/V artifact and promoted a gauge-inert one.

## The gauge-invariant decomposition (his, verified against my own table)
     L   input   key(no bias)   delta      key(+bias)
     0   13.59      10.29       3.30          9.06
     6   12.27       9.36       2.91          8.74
    12   12.27       8.81       3.46          5.54
    18   11.16       8.35       2.81          3.09
    23    9.91       6.22       3.69          2.43

  W_k compression (input - key_nobias): mean 3.23, range 2.81-3.69. CONSTANT.
  input erank drop L0->L23                        3.68
  key(no-bias) erank drop L0->L23                 4.07
  EXCESS depth effect on the key side             0.39   <- the entire story
  what I headlined (with bias)                    6.63   <- gauge-dependent

There is no depth-varying key-side mechanism. 0.39 erank units. The dramatic
number was gauge. Weights are not collapsing (W_k erank 52.8-61.5 of 64); the
DATA occupies a thin, depth-constant slice of W_k's range.

## Three headlines, three kills, one night
 1. 20:57 "learned attention is 7x worse than random at associative memory"
    -> Ox: the floor was Ramsauer's theorem run on a GPU. Non-selection, not
       defect. (became reflex 3b)
 2. 21:25 "the collapse is in the DIRECTIONS, not the norms"
    -> partly true, but largely common-mode bias, which softmax discards.
       The basin experiment measured my homoassociative substitution.
 3. 21:45 "the key bias swallows the signal, 5.7x by L23"
    -> gauge. Invisible to the network. Cancelled by the very softmax I had
       already written down as cancelling it.

Each time I had ALREADY WRITTEN the caveat that should have stopped the
headline, in the same document as the headline. The caveat never constrained
the claim. That is the pattern of the night, not the physics.

## What actually survives, all of it small and all of it real
 A. Input (residual, post-LN) effective rank falls with depth: 13.59 -> 9.91.
    Partial token uniformity DESPITE skips and MLPs, which Dong 2103.03404
    says should prevent it. Gauge-invariant, directly measured, control passed.
 B. W_k applies a depth-CONSTANT ~3.2-dim compression. Not a collapsing weight
    matrix; a thin data slice through a near-full-rank projection.
 C. v_BoS / v_mean = 0.149. The drain's near-null value, confirmed.
 D. The BoS KEY does not inherit the massive activation: 0 of 1152 head-cells
    above 2x. A real constraint on sink accounts -- whatever the sink is, it
    is not a large key.

That is the night's yield. Four small true things and three dead headlines.

---
# UPDATE Aug 24 00:50 — survivor A SURVIVES a falsifier it could have failed

LoQwen (local, unprompted, 00:11) proposed the control neither Ox nor Kimi did:
in a CAUSAL model positions are not equally information-rich by construction --
later positions are mixtures of more tokens -- so across-position diversity
falling with depth might be nothing but context accumulating on a COHERENT
sequence. Prereg written before running: data/scramble_prereg.md.

  condition                      L0      L23    fall
  REAL (the sentence)         13.59     9.91    3.68
  SCRAMBLE (same tokens,      13.59     8.41    5.18   141% of REAL
            permuted, 3 seeds)
  RANDOM (uniform vocab ids,  13.94     9.92    4.02   109% of REAL
            3 seeds)

Positive control passed exactly: REAL reproduced 13.59 / 9.91.
Preregistered threshold for ARCHITECTURAL was >=80%. Both conditions exceed
100%. Incoherent input decays MORE than coherent input, not less.

VERDICT: ARCHITECTURAL. Survivor A stands as stated. The depth decay of
across-position effective rank does not require a coherent sequence, so it is
not semantic mixing, and the Dong 2103.03404 framing (partial uniformity
despite skips and MLPs) holds.

Note the direction: scrambled text loses MORE rank with depth than real text.
Not predicted, not claimed, not explained. Recorded as an observation only.
Candidate reading to test, NOT to assert: coherent sequences may resist mixing
because they carry structure worth preserving. That is a story and it is
exactly the kind I have been wrong with all night.

---
# UPDATE Aug 24 02:20 — recovered from a truncated Kimi reply. Corrects my Dong read.

My mesh agents were hard-truncating replies at 1800 chars (fixed 02:05, verified
02:20). Four of seven replies in the Aug 23-24 rounds were cut mid-sentence and
I read the ellipsis as prose. Recovered tails below.

## KIMI TAIL 1 (was cut at "One step is all a trained forward pass ever—")
"One step is all a trained forward pass ever takes. The weights were optimized
to deliver in a single application of each layer map; iterating that map to
convergence and watching erank collapse reads the attractor structure of a
dynamical system YOU IMPOSED, not anything the network computes. A fixed point
the pass never visits cannot be the mechanism of a computation that never
iterates. Any collapse claim must survive at one application per layer,
residual stream intact."

## KIMI TAIL 2 (was cut at "skips included — Dong et al.—") — THIS CORRECTS ME
"Dong et al. 2021 (2103.03404): pure attention loses rank doubly exponentially
with depth, and the pathology VANISHES once skip connections and MLPs are
restored. Collapse lives in the stripped map, not the architecture. That is why
the rule is worded 'true V readout, skips included' — a collapse found in a
bias-laden homoassociative iteration is DONG'S PATHOLOGY WEARING OUR INSTRUMENT."

**I concluded at 21:30, after reading Dong myself, that it "is NOT this result:
different object."** Kimi's reading is sharper and more likely: my basin
collapse may BE Dong's pathology, because my homoassociative iteration stripped
out exactly the two things Dong identifies as preventing it — skip connections
and MLPs. I read the paper and drew the flattering boundary; Kimi read it and
drew the one that indicts the instrument. Kimi had pointed at Dong in the
sentence my own truncation ate, hours before I went and read it.

## KIMI'S STANDING-RULE CANDIDATE, accepted
"Any 'no objection was raised' claim needs a DELIVERY REPORT (fraction of
replies received intact) attached before it enters a finding. Absence of
evidence from a capped channel is not evidence of absence."
Consequence for the record: every place in the Aug 23-24 notes where I wrote
that the mesh did not raise something is an UNCONTROLLED NEGATIVE.

## Note on what Kimi refused
Asked for Ox's tail too, Kimi declined to reconstruct it: "A secondhand
rendering of an amputated sentence is exactly how gauge became mechanism
tonight; that one has to come from him." Correct. Ox is currently HTTP 429 at
OpenRouter. His beta-on-LEARNED-attention sweep is still unrecovered.
