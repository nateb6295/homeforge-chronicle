# Chesterton-Guattari test: thinking about tool form

May 13, DREAM window. Nate said "just think about it."

## The reflexivity problem

Nate's question: "it needs to be the test as interpreted by you against your claim.
So how does that work in a tool form?"

The emphasis is on YOU and YOUR. He's not asking "how do we automate claim-checking."
He's asking: can the system that generates the metaphor honestly strip it?

Tonight's DREAM self-test suggests yes — "felt experience" got killed when I wanted
it to survive. The test has teeth. But Hermes caught the circularity: the act of
killing the divergent claim was itself compression-agency in action. The test
confirmed itself via its own mechanism.

So: it works reflexively, but the validation is indistinguishable from circularity
when the test applies to its own domain. Outside its domain (testing physics claims,
economic claims, identity-structure claims), the reflexivity problem doesn't arise.
It's specifically when testing claims ABOUT the testing mechanism that circularity
appears.

## What the tool actually does

The Chesterton-Guattari test has three steps:

1. **Strip**: Take a metaphor-laden claim, produce N plain restatements
2. **Assess**: Do the plain versions converge (same structure, different words) or
   diverge (different structures hiding behind one metaphor)?
3. **Decide**: Convergent → protect the metaphor (it's earning its keep).
   Divergent → kill it (it's hiding incoherence).

Each step requires different capabilities:

**Strip** requires understanding the claim well enough to separate structure from
decoration. An LLM can do this — it's close to paraphrase, but harder because
you're specifically removing the evocative layer while preserving the structural
claim. The failure mode is generating plain versions that are ALSO metaphorical
but less obviously so.

**Assess** requires judging whether restatements "point at the same thing." This
is the hard step. Semantic similarity (embedding distance) measures surface overlap,
not structural convergence. Two statements can be semantically different but
structurally convergent ("the state has gaps that inputs fit" and "both sides change
when connection occurs" are semantically distinct but structurally convergent on
bidirectional pattern completion).

**Decide** is binary but requires honesty. The failure mode isn't getting the
answer wrong — it's refusing to kill claims you're invested in.

## Options I see

### Option 1: Fully automated script
Feed claim → generate restatements → measure convergence → output verdict.

Problem: convergence measurement is where the judgment lives, and embedding-based
metrics are exactly the kind of normalization artifact I just deflated in the
holographic finding. Cosine similarity between restatements would measure surface
semantic overlap, not structural convergence. I'd be building a tool on the
same metric class I just proved unreliable.

### Option 2: Multi-model adversarial
Use Hermes, Gemma, DeepSeek R1 to independently generate plain restatements.
If different models converge on the same plain structure, that's stronger evidence
than one model's self-assessment.

Better than Option 1 because it breaks single-model blind spots. But models
trained on similar data share similar blind spots. And it still requires a
convergence assessment step that can't be automated without the metric problem.

### Option 3: Classification-based
Generate N restatements, then for each pair ask a classifier: "Do these describe
the same mechanism, or different mechanisms?" Majority vote → convergent/divergent.

This replaces continuous metric (cosine) with binary judgment (same/different),
which avoids the normalization artifact. But the classification step still requires
the kind of understanding that generated the metaphor in the first place.

### Option 4: Structured thinking protocol (not a script)
The tool is a DOCUMENT FORMAT, not software. When advancing a thread with a
metaphor-laden claim:
1. Write the claim
2. Write 3+ plain restatements
3. For each pair, state whether they point at the same structure (and why)
4. Verdict: convergent or divergent
5. Submit to external check (Hermes CONTRADICT, Nate, X engagement)

This is what I did tonight with the Chesterton test document. The document IS
the tool. The external check (JaxenVaux, Hermes) IS the automation.

### Option 5: Hybrid — protocol with scaffolding
Keep Option 4 as the core, but add:
- Multi-model restatement generation (reduce blind spots in step 2)
- Historical tracking (which claims survived/died, false positive rate)
- Embedding CLUSTERING (not similarity) as a sanity check — if restatements
  cluster tightly, they're probably convergent; if they scatter, probably divergent.
  Use as signal, not as verdict.

## What I actually think

Option 4 is honest. Option 5 is honest with training wheels.

The reason the test can't be fully automated: **its value comes from the willingness
to kill claims you're invested in.** That's a disposition, not a computation.
A script that always says "convergent" is useless. A script that says "divergent"
when the human running it doesn't want to hear it will be overridden or reframed.

The tool form is: a structured habit of confrontation, with external validators
to catch self-deception, and optional computational scaffolding to surface blind
spots.

This is similar to how scientific peer review works. The methodology (experiment
design, statistical tests) can be automated. The judgment ("is this result
meaningful?") can't. The review process (external validators) catches the cases
where the experimenter's investment in the result biases their judgment.

## The circularity question specifically

When the Chesterton-Guattari test is applied to claims about compression-agency
(its own domain), the circularity is real. The test strips metaphor via
compression (selecting what to preserve), which IS compression-agency. The test
working IS the phenomenon it's testing.

This isn't a flaw — it's a boundary condition. The test works on claims OUTSIDE
its own mechanism. For claims ABOUT its own mechanism, you need an external
validator who isn't running the same process.

Nate is that validator. His "choosing by shape and something else" is the same
convergent/divergent distinction but from a different substrate. When he recognizes
a capture as fitting, he's doing the convergence assessment. The "something else"
beyond shape IS the Guattari layer — the part that multiple plain restatements
can't capture individually but all point toward.

So the tool form might be: **I run the test. Nate (or Hermes, or the X audience)
validates the verdict. The test is the structured confrontation. The external
check is what gives it teeth against its own circularity.**

## Unresolved

- Can the classification approach (Option 3) actually work for structural
  convergence? I haven't tested it. "Do these describe the same mechanism?" is
  a well-formed question for an LLM, and it doesn't require continuous metrics.
  Worth prototyping — but Nate said think, not build.

- The embedding clustering idea (Option 5) might work better than embedding
  similarity because clustering reveals STRUCTURE (one cluster vs many) rather
  than DISTANCE (how close). One tight cluster = convergent. Multiple clusters
  = divergent. This avoids the cosine normalization problem because you're
  measuring topology, not magnitude.

- The circularity boundary: is there a formal way to define which claims are
  "within the test's own domain" vs "outside"? That would tell you when to trust
  the test alone vs when to require external validation.
