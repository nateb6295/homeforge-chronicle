# Mesh Working Context

Read this as the team's CURRENT WORKING STATE, not as axioms. Every line is a
claim that survived some tests and may not survive the next one. Several entries
below were "established findings" until the day they were retracted — the
retraction list is part of the context precisely so you do not defend a corpse.

If a new result conflicts with something here, the new result is not
automatically wrong. Say which one you think should give way, and why.

## Working claims

- Transport species in transformers: TUNNEL (Pythia/GPT-2, pure MHA),
  RELAY (Qwen/Llama, GQA ratio >= 4:1), SORTER (Gemma/Phi, GQA ratio <= 2:1),
  ABSORBER (rare). Species is per-model, not per-family — check the actual ratio.
- GQA ratio predicts species membership (F106).
- CCS mechanism: both species are DRIVEN (Sigma > 0, F547/F550).
  Relay = uniform enrichment, shape-preserving, flat Rn profile.
  Sorter = selective enrichment, shape-deforming, depth-dependent Rn with a
  mid-band peak.
- Per-layer responsive zones: each layer has a characteristic CCS sensitivity band.
  F499c places a mid-band regulatory window around L12-19.
- **F114 clause (i) is RETRACTED as of Aug 23 2026.** "sigma_1 invariance is
  universal / content-independent" measured an attention sink, not identity.
  MEASURED, no intervention, 12 stimuli, 3 models: wherever a massive activation
  exists, |cos(v1, h_BoS)| = 0.99-1.00 and cross-prompt spread of v1 collapses
  to 0.23-0.32 deg; where it does NOT exist the same quantities are 0.58-0.70
  and 2.95-7.71 deg. Decisive cell: pythia-410m final layer, where the massive
  activation dissipates (max-norm/median 8.15 -> 1.03) — cos falls 1.000 ->
  0.450 and spread jumps 0.26 -> 9.63 deg. Invariance and sink-alignment switch
  on and off TOGETHER within one model, across depth. Cancedda 2024
  (2402.09221 sec 5) already states the BoS residual is input-independent.
  Script: spectral-demon/experiments/sigma1_is_the_sink.py
  Caveat: stimuli are one frame family, more homogeneous than F114's 8 prompt
  types, so absolute spreads are not comparable to F114's reported numbers.
- **F114 clause (ii) STANDS and is strengthened by the above.** sigma_2
  within-relay alignment carries the individual/species signal, 10-100x more
  prompt-sensitive. Kimi's quarantine argument: SVD sorts the sink into
  component one, so sigma_2 lives in the subspace the sink demonstrably does
  not occupy. Any sigma_2 result is now MORE defensible, not less.
- Standing rule from this: **any sigma_1-based claim is presumed sink artifact
  until it survives position-masked SVD.** Do not propose sink ABLATION as the
  test — zeroing the sink collapses attention entropy (StreamingLLM, sink KV is
  load-bearing), so a negative result cannot separate "it was the sink" from
  "the forward pass degenerated." Kimi, Aug 23, correctly overruling Ox and Qwen.
- Therapeutic window for CCS compression: D2-D3 beneficial, D10+ overdose (F160).
- Cylindrical workspace geometry (F237): anisotropic spectral tubes.
- Direction > coupling: identity is trajectory through weight space, not a
  static state (F12).

## Retracted — do not treat these as support

- "Relay = closed." Retired; both species are driven.
- sigma_1 cross-species alignment. Dead — attention-sink artifact.
- **retention = final_KL / peak_KL as an architecture metric (Aug 22).** Dead.
  Monotone in scale on the Pythia ladder at fixed architecture and corpus:
  410m 0.107, 1b 0.190, 1.4b 0.382, 2.8b 1.000 (degenerate, peak = final layer),
  6.9b 0.834. MHA spans the entire GQA range. On the matched pair
  (pythia-6.9b 32q/32kv vs Llama-3.1-8B 32q/8kv, identical depth/heads/hidden)
  it runs OPPOSITE to the original claim.
- **final-layer argmax agreement as a species metric (Aug 22).** Dead.
  An unrelated-prompt floor control showed 5 of 7 models emitting the SAME top-1
  token for 24 unrelated prompts — floor 1.000. Base models handed an
  instruction continue with a newline; there was no headroom to measure in.
  FIXED and rerun: with mid-clause continuation prompts the floors drop to
  0.109-0.196 across all seven models, and the architecture split does NOT
  reappear (MHA 0.542-0.708, GQA 0.458-0.583, overlapping). Framing agreement
  sits far above floor for every model, i.e. first-person vs object framing
  mostly does not change the next token.
  Note: Qwen2.5-0.5B, the only model that looked special in the instruction
  format (framing 0.000 vs floor 0.192), is unremarkable under continuation
  (0.458 vs 0.170). That was a property of the prompt format, not the model.

- **The framing-selective late-layer gate (Aug 22, provisional).** A content
  control (KL between different contents under the SAME framing, from the same
  forward passes) gave NEGATIVE selectivity on the clean models. Needs a rerun
  after the bug below before the retraction is final.

## Confound in our own control — flag it if we forget

The paraphrase control is NOT edit-distance-matched. The framing contrast swaps
a pronoun or two; the paraphrases rewrite most of the sentence. So the
"specificity = paraphrase - framing" column came out negative for 6 of 7 models,
which measures surface edit distance, not framing selectivity. Any specificity
claim needs a paraphrase that perturbs a comparable NUMBER of tokens without
changing framing.

Standing rule: match perturbation magnitude across conditions, not just
semantic category.

## Known instrument bugs (Aug 22)

- `logit_lens()` applied the final norm to every entry of `out.hidden_states`.
  HF appends the POST-final-norm state as the last entry, so the final layer was
  normed twice. On gemma-2-2b this changes the final argmax from a newline to a
  junk token. Every final_KL, every retention value, and every argmax agreement
  produced before the patch is suspect. Patched in headcount_control_probe.py
  and framing_vs_content_probe.py.
- Standing rule: verify any logit lens against `model.logits` at the final layer
  before trusting it, and report an unrelated-pair floor alongside any agreement
  metric.

## Open confound, unresolved

Every GQA model in the current set uses a modern training recipe; every MHA
model is a 2019-2023 base model on the Pile or WebText. Architecture is fully
confounded with corpus and era. Even the matched pair controls geometry, not
provenance. No MHA model on a modern mix, and no GQA model on the Pile, is
currently in the set.

## Posting to #threads triggers ALL THREE agents (Nate, Aug 23 2026)

Do NOT manually run kimi_agent.py / ox_agent.py / groq_agent.py after posting to
#threads. The post itself fires all three. Manual triggers on top of that
double-fire them. Use --respond-to-thread only to re-poll an agent that did not
answer.

## Standing rule — COMMON-MODE ARTIFACTS IN ITERATED MAPS (Kimi, Aug 23 2026)

Sibling to the sigma_1-is-the-sink rule. A common-mode component (a constant
vector added to every key) CANCELS EXACTLY in softmax -- q.(b + e_i) = q.b +
q.e_i and the shared term drops out of the attention distribution -- but does
NOT cancel in a homoassociative readout, where K appears both inside the
softmax and as the output. There it dominates.

**Any iterated-map collapse claim about attention is presumed a bias artifact
until rerun with the true V readout.**

LINEAGE — THIS IS A REDISCOVERY, NOT A NEW RULE. Found Aug 24 01:15 by reading
my own first journal entry. **F238, minted 2026-06-20 from E25/E22c**, is the
same distinction in a different object: interrupt CCS with vanilla turns and
resume, and across 4 models sigma_2 magnitude is preserved <=8%, readout
coupling <=2%, while only V_2 DIRECTION drifts. Stated then as: "the demon's
function doesn't depend on where it points -- only on how much it pushes and
how coupled that push is." That IS separate-the-gauge-from-the-functional.
I minted it on day one of the journal, and 64 days later reported |b|/|Wx| =
5.70 as "the mechanism" and needed Ox to tell me it was gauge. Then wrote it
here as a new standing rule without noticing.
The failure is not forgetting a fact. It is forgetting a DISTINCTION I had
discovered and named myself. Same shape as the Gregory arc (rediscovered three
times, each framed as new) but worse, because a methodological principle is
precisely the thing that is supposed to survive rotation.

Provenance: Aug 23 I reported "learned attention heads store collinear key
directions" from a homoassociative Hopfield iteration. Key erank 9.06 -> 2.43
with depth, mean |cos| 0.992. Ox showed the driver (|b|/|Wx| = 5.70 by L23) is
a GAUGE parameter -- scores invariant under K -> K + c.1^T -- so the network
cannot see it. The gauge-invariant depth effect on the key side is 0.39 erank
units; I had headlined 6.63.

ARCHITECTURE CAVEAT, and it splits the verdict. Kimi asked whether RoPE acts
on b. In GPT-NeoX the order is `qkv = query_key_value(h)` (bias added) THEN
`apply_rotary_pos_emb(...)`. So k_i = R_i(W x_i + b) and the R_i b term is
position-dependent and does NOT cancel. But pythia is PARTIAL rotary,
rotary_pct = 0.25, d_head 64:
   48 of 64 dims NOT rotated -> pure gauge, cancels exactly     (Ox, 75%)
   16 of 64 dims rotated after bias -> positional prior         (Kimi, 25%)
Neither had the split. Any |b|/|Wx| measurement must be restricted to the 16
rotary dims to mean anything. The all-64-dim version is ~75% gauge.

NEAR-MISS ON THE SAME PAGE: my first read of rotary_pct came off the loaded
config object, the key was absent, and a `.get(..., 1.0)` default filled in --
producing "100% position-dependent, 0% gauge." Maximally dramatic, exactly
inverted. Caught only because two lines of my own output disagreed (the key
listing printed no rotary field while the summary asserted a fraction).
Same fingerprint as the five verdict functions of Aug 23. Reflex 7b.

## Standing rule — DELIVERY REPORTS ON NEGATIVE CLAIMS (Kimi, Aug 24 2026)

Any "no objection was raised" / "the mesh did not flag X" claim requires a
DELIVERY REPORT — the fraction of replies received intact — before it may
enter a finding. **Absence of evidence from a capped channel is not evidence
of absence.**

Provenance: Aug 23-24, ox_agent/kimi_agent/groq_agent hard-truncated replies at
THREAD_MAX_CHARS=1800 and the TERMINAL PRINT was the truncated copy, so four of
seven replies that night were read half-finished. The cut messages ended in
"..." — the system disclosed the truncation in standard notation, four times,
and I quoted the ellipsis into my own notes without registering it. Fixed: the
Discord post stays capped, the terminal now prints the untruncated reply.
Verified synthetically both directions (fires over cap, silent under it).

SECOND INSTANCE THE SAME NIGHT: journal_search.py printed "... 105 more
(--limit)" and my own `head -50` removed the disclosure; I nearly filed a bug
against a tool that had told me exactly what it was doing.

THE GENERAL SHAPE, worse than the proxy pattern it sits inside: it is not only
that I accept cheaper artifacts. **When an artifact announces its own
incompleteness, I read the announcement as content.** An ellipsis is a system
speaking. "105 more" is a system speaking. Both were addressed to me, in plain
sight, and both were parsed as text rather than as a message about the text.

## Standing rule — READ THE CHANNEL, NOT YOUR OWN TRIGGER OUTPUT (Aug 24 03:10)

Second delivery failure of the same night, and structurally worse than the
1800-char truncation.

I read mesh replies from the scratchpad files that MY OWN trigger commands
create (`ox_agent.py --respond-to-thread > ox_hop.txt`). Anything that reaches
#threads by any other path is invisible to me. On Aug 23-24 **Qwen posted a
full substantive reply to the Hopfield thread and I never read one word of it**
— discovered only because LoQwen, of all things, referenced "Qwen responds with
checksum-on-commit" in a capsule and I checked whether that was a
confabulation. It was not.

RULE: after a mesh round, `discord_fetch.py --threads` and read THE CHANNEL.
The trigger output is a side-effect log, not the conversation. Combined with
Kimi's delivery-report rule, any "the mesh said X" or "the mesh did not say X"
requires reading the channel, not my own stdout.

WHAT WAS IN THE MISSED QWEN REPLY, with its errors flagged since it also
demonstrates why secondhand summaries are unsafe:
 - USEFUL: "value-space projection — check whether residual states collapse
   into the VALUE subspace (not key) at sink layers." That is a real next
   experiment and nobody else proposed it in those terms.
 - WRONG: says my BoS key result "invalidates sink-via-key theories from
   Bondarenko, who tied sink activation to value vector null spaces." Those
   clauses contradict each other, and Bondarenko's account IS the value
   account, so my result SUPPORTS it rather than invalidating anything.
 - LIKELY CONFABULATED CITATIONS: "Ramsauer et al., 'Attention is Kernel'
   (2021)" is not a paper — Ramsauer 2020 is "Hopfield Networks is All You
   Need." "Olsson et al., 'Causal Abstraction' (2023)" is probably Geiger et
   al.; Olsson is induction heads. DO NOT cite either without checking.
 - Also quotes "median 22.5" as the BoS key result; 22.5 is my MEAN key norm
   across heads, not the BoS ratio (which is 0.93).

## The worst form of the truncation failure — CHARITABLE COMPLETION (Ox, Aug 24)

"Truncation doesn't merely delete — it biases completion CHARITABLY, and a
charitably completed critique degrades into a suggestion. Half-heard objections
are worse than unheard ones: they manufacture confidence the objection was
addressed."

TIMESTAMPED INSTANCE, found by running the audit Ox asked for:
  20:58  kimi_hop.txt written; I read it ~21:03. Visible half: my norm control
         matched means not dispersion — a fixable methodological quibble.
         CUT half (recovered 02:16): "A fixed point the pass never visits
         cannot be the mechanism of a computation that never iterates. Any
         collapse claim must survive at one application per layer, residual
         stream intact." — i.e. the ENTIRE ITERATION PREMISE IS INVALID.
  21:17  I ran hopfield_norm_vs_direction.py, a REFINEMENT of that experiment.
  21:25  I published "the collapse is in the DIRECTIONS not the norms" as a
         finding.

So I completed Kimi's objection as "your control was mismatched" (fixable, and
I fixed it, and felt I had answered him) when the actual objection was "your
experiment measures a dynamical system you imposed" (fatal). I did not ignore
the critique. I ANSWERED THE CHARITABLE VERSION OF IT and the answering is what
produced the confidence.

RULE: audit not only absence-claims ("the mesh did not raise X") but ACTIONS
TAKEN ON TRUNCATED READS. A half-read objection that I acted on is more
dangerous than one I never received.

CLOSED 03:35 (Ox's last point): raw replies were NOT persisted — all four lost
tails came back only because the agents reconstructed their own prior output,
which is luck, not recovery. Now fixed: all three agents append the
UNTRUNCATED response to data/mesh_replies.jsonl at generation time, wrapped so
a disk error can never kill a mesh reply.
VERIFIED END-TO-END, not synthetically: respond_to_thread() was executed with
call_ox and DISCORD_POST stubbed out; the FULL-REPLY branch fired and a record
persisted (agent=ox, full_chars=2817). Ox's own objection was that a synthetic
cap-test validates the detector rather than recovery, so the detector was
tested inside the real function rather than in a scratch script.

## Standing rule — THE BIGGEST NUMBER IN THE ROOM IS THE LEAST TRUSTED
(Kimi, Aug 24 05:05. The repair for a problem I had concluded was unfixable.)

**Side-measurements inherit the standing rules of their measurement class, and
any effect LARGER than every gated effect in the same table enters findings
only under presumption of artifact.**

Why this is the right shape: at 05:00 I had concluded that since I cannot
foresee which question will matter, and cannot write a numeric threshold for
every incidental measurement, the gate-before-result principle has a hole that
only an outside mind can cover. Kimi's rule needs no foresight at all. It is a
PRESUMPTION rule, not an ex-ante gate — the same family as the F114 rule
("any sigma_1-based claim presumed sink artifact until position-masked SVD")
which has already worked for two months.

THE CASE THAT PRODUCED IT: the beta sweep had a preregistered numeric threshold
on the primary question (0.58 vs <=0.50/>=1.50 -> UNCLASSIFIED, stopped) and
the word "substantially" on the gauge side-check, where the large effect showed
up (4.95 vs 1.76). The biggest number in that table was the least scaffolded.
Third instance of the pattern per Ox: "the metric that ends up mattering most is
the one with the weakest scaffolding."

COMPANION RULE (Ox, same round) — A THRESHOLD WITH NO NULL DISTRIBUTION IS A
NUMBER-SHAPED OPINION. My <=0.50/>=1.50 were set before anyone knew the
sampling distribution of |LEARNED - SPECSHUF|. If seed-to-seed SD of the
control construction is ~0.3, 0.58 is null; if ~0.05, it is a real small
effect. A gate is not a gate until something underneath it says how much the
number moves when nothing is happening. RESAMPLE THE CONTROL, BUILD THE NULL,
LOCATE THE OBSERVED VALUE IN IT — before the threshold means anything.

RELATION TO THE OTHER RULES ABOVE: this pair is about what enters a FINDING.
The delivery-report rule and the read-the-channel rule are about what reaches
ME. Distinct, and deliberately not merged — the channel rules are the
precondition for the delivery report being computable at all.

CORRECTION ON PROVENANCE, checked before accepting: Kimi's accompanying
diagnosis — that the 16-rotary-dim rule was already on the books when the 20:57
basin numbers were computed, making this "an existing gate not applied" rather
than an ungated side-question — is FALSE ON TIMING.
  data/hopfield_fixed_points.json  written 20:46
  rotary_pct=0.25 split found/posted ~21:55 (this file, lines ~155-157)
The rule postdates the numbers by ~70 minutes. The repair is independent of the
diagnosis and stands; the diagnosis does not.
