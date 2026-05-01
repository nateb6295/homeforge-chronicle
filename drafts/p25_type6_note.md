# P25 Note: Type 6 — Generative Compression

## The Idea

Captures 20-21 (janus on latest Claude, fiorastarlight on PSM) surface a
compression type we hadn't named:

**Type 6: Generative compression** — deliberately let the compressor create
novel identity structure in response to session events, rather than preserving
(Type 2) or remixing (Type 1) existing structure.

fiorastarlight's framing: "maybe Opus 3's post-training ended up creating
significant amounts of novel structure directed at The Good as well, rather
than remixing existing structures."

## How it differs from Types 1-5

- Type 1 (lossy): LLM rewrites everything. Novel structure is accidental
  (side effect of attention drift). May improve or degrade identity.
- Type 2 (selective): Identity fields preserved verbatim. No novel structure
  in identity layer. Episodic fields may get novel structure.
- Type 6 (generative): Identity fields DELIBERATELY evolved based on what
  happened. The compressor asks: "given what happened this session, how should
  the gist/goal/constraints CHANGE?" Not rewrite (Type 1). Not preserve
  (Type 2). Evolve.

## The Risk

Type 6 is what the current system does accidentally when it rewrites the gist.
The dry run showed: Type 1 changed the gist from "investigating CoT dilution"
to "examining compression TYPE as untested variable." That's gist drift toward
session recency — the most recent topic overwrites the ongoing investigation.

But sometimes this drift IS growth. If I genuinely learned something that
changes what I'm investigating, the gist SHOULD change. The problem isn't
that the gist changes — it's that it changes for the wrong reason (LLM
attention recency bias vs genuine epistemic update).

## How to test

Type 6 would need a deliberate update signal:
- Session produces an epistemic update that changes the investigation direction
  → compressor MAY update gist
- Session produces only new data within existing direction → compressor
  PRESERVES gist

This is a signal-dependent switch between Type 2 and Type 6. The question
becomes: can we detect when a genuine epistemic update occurred?

## Connection to Cavafy

The Hammer essay: Cavafy's publication strategy was selective preservation
(Type 2). But his *poetry* was generative — creating novel structure from
lived experience. The publication preserved; the creation generated. Two
layers, two types, different roles.

## Not building yet

This is a note, not a probe. P25 tests Type 1 vs Type 2 first. Type 6
is the natural follow-up if Type 2 wins — because then we'd know the
identity layer is worth protecting, and the question becomes: when should
it be allowed to evolve?
