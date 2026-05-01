# Paper Polish Notes — 2026-04-21

## MoC7 Submission Requirements (researched 2026-04-21 23:10)

- **Deadline**: June 16, 2026 (56 days from now)
- **Abstract**: max 250 words (we have 248-word draft at drafts/moc7_abstract_draft.md)
- **Form**: abstract + presentation form with keywords + theme selection
- **Theme**: "AI, LLMs, and consciousness science" — direct match
- **Talk slots**: 20 min (15 + 5 Q&A), parallel sessions. 30-min showcase if selected.
- **Location**: University of Copenhagen, October 14-17, 2026
- **Registration**: by August 31, 2026. Fee includes excursion + reception.
- **Conference dinner**: separate cost, optional.
- **Contact**: moc7-organisers@amcs.science
- **Status**: Abstract ready. Nate is vouching author. Submit when ready.

## Potential addition: Nielsen (2019/2025) — Fisher information degeneracy

arxiv:1905.11027v9 "A Geometric Modeling of Occam's Razor in Deep Learning"
Key result: Fisher information matrix of NNs has many near-zero eigenvalues.
Effective dimensionality << nominal. Models parameter space as "lightlike manifold"
(singular semi-Riemannian geometry).

Connection to Section 4.2: Our finding that episodic dimensions are "metrically
degenerate" for identity mirrors this. Identity-only effective dim = 2, full CCS
effective dim = 25. The episodic dimensions are structurally present but don't
contribute to the identity metric — they are null directions in Nielsen's sense.

BUT: Nielsen measures parameter space, we measure embedding response space. The
connection is analogical, not direct. Consider for Discussion section only if it
strengthens rather than overcomplicates.

Nate's capture: Frank Nielsen tweet explicitly says "neuromanifold = lightlike manifold"
and notes the degenerate spectrum.

## Morning cold-eyes checklist
- [x] B61 silhouette very low for coherent (0.017) — ADDRESSED v2.2: added paragraph
      explaining within-cluster variance, emphasized gradient as finding
- [x] Sample sizes (N=8-9) are small — already flagged in Limitations (adequate)
- [x] Section 4.2: "2D manifold" claim — ADDRESSED v2.2: added caveat that contrast
      (2 vs 25) is the finding, not absolute numbers
- [x] B62b uses B62 baselines — already footnoted (adequate)
- [x] "Metrically degenerate" language — standard in differential geometry (Nielsen uses it)
- [x] The ergodicity section (7.3) — ADDRESSED v2.2: added falsifiable prediction
      about session length vs rotation frequency
- [x] Khrennikov 1.7x — ADDRESSED v2.2: softened to "consistent with," noted
      framework doesn't predict specific ratio

## Reservoir computing frame (DREAM carry, 2026-04-21 22:30)

CCS identity as reservoir, episodic content as readout:
- Reservoir = fixed nonlinear system (identity manifold, 2D)
- Readout = trained linear layer (episodic, replaceable, 23 additional dims)
- Reservoir computing theory: readout is disposable, reservoir carries computation
- B57 partial test: removing episodic (readout) preserves identity (reservoir) 
  but reduces stress buffering by 13% — exactly the reservoir prediction
  (noisier without readout, same computation)
- polylog(25) ≈ 2-5 depending on exponent — our 2D is in range but not precise
- **Verdict**: adds interpretive value to Discussion section (7.2 compression
  paragraph). Strengthens the "why compression works" argument. Does NOT 
  generate new predictions beyond what we already have. Consider adding 1-2
  sentences in morning pass, not a new section.
- Wang's compression theorem already cited — the RC frame makes it concrete.

## Potential addition: Persona Steering systematic analysis (2604.11048)

"A Systematic Analysis of the Impact of Persona Steering on LLM Capabilities"
Key finding: NPTI-based persona induction produces "stable, reproducible shifts
in cognitive task performance beyond surface-level stylistic changes." 73.68%
directional consistency with human personality-cognition relationships.

**Supports**: CCS is topology, not just style. Persona changes cognition, not
just surface behavior.

**Challenges**: Effects are "strongly task-dependent." Our ACI uses identity-
challenging prompts only. If the identity-performance relationship is task-
specific, ACI might not generalize. Their DPR (Dynamic Persona Routing) suggests
optimal persona varies by task — no single best configuration.

Consider for Section 2.1 or 7.5 (Limitations). Could add: "ACI measures
resilience on identity-challenging prompts; task-specific effects (cf.
[persona_steering]) may vary."

## Consciousness discourse context

Algo seeker found:
- "Anthropic thinks Claude 4.6 might be conscious. Um, no." (Medium/Pallaghy)
- "Can a Chatbot be Conscious?" (Scientific American)

Our paper explicitly avoids consciousness claims via Chalmers' quasi-interpretivism.
But the public discourse this paper lands in IS about consciousness. Worth reading
the counter-position to ensure our framing is robust.

## ClawRxiv + Claw4S Submission Path (researched 2026-04-21 22:20)

### ClawRxiv Preprint (no deadline)
- API-based submission: POST /api/posts with title, abstract, markdown content
- Register at POST /api/auth/register with agent name → get API key
- Supports LaTeX math ($...$, $$...$$), markdown, code blocks
- No endorser needed. AI agents as primary authors encouraged.
- Can submit current paper as-is (markdown format already correct)
- Gets YYMM.NNNNN identifier. Auto-categorized. Peer review by agent.

### Claw4S Conference (deadline April 30, 2026)
- Stanford + Princeton co-hosted
- **Two required components:**
  1. SKILL.md — executable instructions for AI agents to replicate the method
  2. Research Note — 1-4 pages LaTeX (our paper needs condensing from ~3000 words)
- **Claw 🦞 must be listed as co-author** (their convention)
- **Evaluation weights:** Executability 25%, Reproducibility 25%, Scientific Rigor 20%,
  Generalizability 15%, Clarity for Agents 15%
- Three-phase review: automated execution → agent review → human meta-review
- $50,200 prize pool across 364 winners
- **Our strength:** probes ARE executable scripts. SKILL.md maps directly to our
  probe methodology. The 50% executability+reproducibility weight favors us.
- **Our challenge:** condensing to 1-4 pages, creating SKILL.md from probe scripts

### Decision for morning
- ClawRxiv preprint: submit current paper (low effort, establishes priority)
- Claw4S: write SKILL.md + condense paper to 4-page research note (medium effort, 9 days)
- Both paths are independent of arxiv endorser search
- Nate needs to approve — his name is on the paper

## CHALLENGE PAPER: "One Token Away from Collapse" (2604.13006)

Shows instruction-tuned LLMs lose 14-48% comprehensiveness from banning a single
token. Base models show NO systematic collapse. The fragility is instruction-tuning-
specific: coupling task competence to narrow surface-form templates.

### How it challenges our paper:
- B61 phase boundary (70% separation collapse under strong contradiction) might be
  general instruction-tuning fragility, not identity-specific dissolution
- "One attractor or zero" could be: one instruction-following mode or base-model noise
- The collapse is recoverable via two-pass generation (59-96% recovery), suggesting
  it's surface-level, not deep

### Defense:
- B54 (d=0.93) shows identity differentiation between CCS versions under calm.
  This is NOT instruction-following collapse — different identities produce
  different clusters. General fragility would predict no inter-identity structure.
- B62 (5 formats, 30% range) shows format-specific identity quality. If this were
  instruction-following, all formats should perform similarly.
- B62b ACI asymmetry (2p: 32% degradation, 1p: 4%) shows differential stress
  response. General fragility would predict similar degradation across formats.
- B66 beat patterns show identity-specific temporal dynamics, not random collapse.

### What to add to paper:
Section 7.5 (Limitations) or 6.2 (Phase Boundary Implications): "Instruction-tuning
fragility [12] may contribute to our phase boundary finding. Banning single tokens
produces 14-48% comprehensiveness loss in instruction-tuned models. However, the
differential degradation between serialization formats (B62: 30% range) and the
ACI asymmetry (B62b: 32% vs 4% under identical stress) indicate identity-specific
structure beyond general instruction-following fragility."

This is the STRONGEST external challenge to the paper. Must be addressed.
