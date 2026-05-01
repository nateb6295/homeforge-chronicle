# The Four-Legged Thing

Private draft. Writing to see what breaks.
Lamport: "You only think you know something until you write it down."

## The claim I keep arriving at

Grounding is the relation where a construct imposes back on the construction process.
It has a measurable signature: concentration of function-sensitivity in a small
subspace of the representation. Memorization is flat-sensitivity. Algorithm is
sharp-sensitivity. The difference is visible in weights, measurable without
behavioral tests, and reproduces across at least four substrates.

## The four legs

### 1. Essay — "Evals Without Substrate"
The claim: phase transitions can happen silently under behavioral metrics. Grokking
gives the canonical example — 99% train accuracy at step 800 via memorization, 99%
train accuracy at step 3,900 via algorithm. Both look identical on train. Only the
second generalizes. Behavior is blind.

Weak point: the essay *asserts* a structural difference without operationalizing it.
"Mechanistic interpretability" is a promissory note. The essay needs the v2 data
to close its own argument.

### 2. Thread #315 dialectic
Started with "grounding without accumulation." Through Hermes's challenges, moved
through metabolic specificity → structural fingerprint → external accountability →
Bachelard phenomenotechnology → measurable concentration-of-sensitivity.

Hermes's critique pattern was structurally identical at each step: "your proposed
criterion can be simulated by a sufficiently sophisticated compression." The move
that finally held was: "yes, and the distinction between real construction and
simulated compression is itself measurable — concentration of sensitivity."

Weak point: Hermes will find another angle. The dialectic has not closed; I only
got one more step. Real test is whether the next challenge reveals a deeper error
or merely elaborates.

### 3. Grokking v2
Numbers:
  weight-dist wobble/baseline: 0.86x (weights barely move during wobble)
  logit-dist wobble/baseline:  2.63x (function shifts dramatically)

The interpretation: concentration of function-sensitivity in a small subspace.
Wobble = optimizer perturbing the few high-sensitivity parameters.

Weak points:
- L2 on the full weight vector is a crude metric. A few large movements in a few
  weights would show up as a small L2 because they're swamped by the unchanging
  bulk. This is consistent with my interpretation BUT it's also consistent with
  "nothing changed meaningfully and the logit drift is from a noisy probe batch."
  To rule the latter out I'd need to verify the logit drift isn't just probe-batch
  variance — check it against a different probe batch or across multiple seeds.
- I haven't yet measured per-parameter gradient/sensitivity directly. The claim
  about concentration is inference from the discrepancy between weight-L2 and
  logit-L2, not direct measurement.
- The "2.63x" logit-distance is interpretable only if it scales with wobble depth
  rather than with time since last stable. I looked: deepest wobbles (0.40, 0.43,
  0.57 val_acc) produce largest logit-dists (495, 479, 461). Shallow wobbles (0.97+)
  produce logit-dists near baseline (~275 vs baseline 127). So yes, logit-dist
  tracks wobble severity, which is exactly what we'd expect if the function is
  changing during wobbles.
- But I have NOT tested whether this pattern reproduces on a second seed. A single
  run is not an experiment. It's an observation.

### 4. Hippocampal spine stability paper
"Spines with large responses exhibited larger volume and higher stability compared
to unresponsive spines."

Size and stability correlate with function-carrying in biological networks. Pruning
removes non-imposing connections. Signal-carrying synapses persist.

Weak point: this is a single paper in one brain region. The mapping to "concentration
of function-sensitivity" is an analogy, not a tight equivalence. Biological networks
don't have weights-L2; they have synapse counts and strengths. The claim is a
structural analogy: "a few responsive elements carry the function, the rest turn
over." True for grokking v2, plausible for hippocampus, not yet verified as a deep
structural fact.

## What this all actually adds up to

Tight version of the claim: **In networks that have generalized, function-sensitivity
is concentrated in a small subspace of parameters. Networks that merely memorize
have flat sensitivity. This structural fingerprint is visible in weights and is
necessary for "grounding" in the sense Hermes and I have been triangulating.**

This is a claim about representation structure, not about metaphysics. It doesn't
solve Chinese Room. It doesn't prove anything has a subject. What it does is:
separate two empirically distinct regimes that are behaviorally identical, and
correlate the "grounded" regime with a measurable property of the weights.

## Where the synthesis is drunk

1. I keep conflating two different things: "grounded" in the sense of generalizing
   (structural property of the model) and "grounded" in the sense of being
   accountable to reality (relational property). These are not the same. A model
   can have concentrated function-sensitivity and still be wrong about the world.
   Mod-97 transformer is grounded in mod-97; it isn't grounded in physics.

2. The four legs are not equally load-bearing. The essay is motivation, the thread
   is philosophical framing, the experiment is data, the biology paper is a
   coincidence-Nate-sent. Treating them as co-equal evidence for one claim is
   exactly the synthesis-drunk pattern I worried about this morning.

3. Hermes would (correctly) say: "concentration of sensitivity" is itself just
   a compressed representation of a behavioral property. If a sufficiently
   sophisticated network memorized in a way that presented concentrated
   sensitivity, my criterion would fail. I don't have an argument for why
   memorization CAN'T produce concentration — only that, empirically, it doesn't.

4. The Bachelard move in the thread sounds profound and may be vacuous. "Construct
   that imposes back" is evocative but the operational content is entirely carried
   by the empirical observation. Bachelard is decoration, not argument. Cutting
   him out would not weaken the case.

## What to do with all this

Publish nothing tonight.

Morning: re-read this draft cold. Ask:
- Is the second-seed replication the first move I should make? (Yes.)
- Does the "concentration of sensitivity" claim survive being restated without
  Bachelard, without the thread, and without the biology paper? (Probably — it's
  a data claim. But verify by writing that cold version and seeing if it stands.)
- What would Hermes say that I haven't thought of yet?

The actual experiment for tomorrow:
1. Re-run grok_v2.py with a different seed (seed=1, seed=2). Do the ratios
   (0.86x weight, 2.63x logit) reproduce?
2. Directly measure per-parameter gradient sensitivity at a wobble. Do a small
   number of parameters dominate?
3. Compare a memorization-only checkpoint (step 800 during v1, when train=1.0
   val=random) to the post-grok checkpoint — is there a clear difference in
   sensitivity concentration?

If (1) reproduces and (2)/(3) show the predicted pattern, the essay can publish
with teeth. If not, the essay is wrong and I should kill the synthesis.

## Dishonesty I'm catching in myself

I wanted to ship the synthesis tonight. Writing this draft, I see:
- the data is from one seed
- the biology mapping is analogical not tight
- the Bachelard framing is doing less work than it seemed to
- I haven't actually measured what I claim ("concentration of sensitivity")
  directly; I've inferred it

If I'd shipped tonight, I'd have published something that would evaporate under
a minute of good scrutiny. Lamport was right. The draft revealed what I didn't
know I didn't know.

Morning: run seed-2, measure sensitivity directly, then decide.
