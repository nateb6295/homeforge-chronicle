# BASELINES — measurements with their method attached

Established 2026-08-24. Nate: "Re-Baseline is prudent. I would like a clean
launching pad where you aren't looking over your shoulder all the time."

## The rule for this file
An entry belongs here ONLY if someone can re-derive the number from what is
written down: a command, a model, an input set, a metric definition, a date.

A number without a runnable method is not a baseline. It is a memory. Memories
go in capsules and get cited as history, never used as a control.

This is the whole point of the file. Every hour I lost today went to inherited
figures I could not reproduce and therefore could not trust or discard — which
is a worse state than not having them, because it costs attention on every use.

---

## B1 — σ₁ cross-prompt angular spread, position-masked vs unmasked
**Established** 2026-08-24 · **supersedes** the Aug 23 figures (0.23–0.32° /
2.95–7.71°), now marked UNREPLICABLE — see note below.

    command  OMP_NUM_THREADS=16 PYTHONUNBUFFERED=1 python3 bin/position_masked_svd.py
    model    EleutherAI/pythia-410m  (fp32, CPU)
    inputs   12 prompts, ~60 tokens each, listed verbatim in the script
    metric   mean pairwise angle (degrees) between first right-singular vectors
             across prompts; H centred per-prompt; sign-fixed on the
             largest-|magnitude| component before comparison
    masking  position 0 dropped from the SVD input matrix. Attention is NOT
             modified — ablation collapses attention entropy and makes a
             negative uninterpretable
    sink def layer has a "massive activation" when ‖h_pos0‖ > 2× median ‖h_i‖, i>0
    output   data/position_masked_svd_result.json

    RESULT   sink-bearing layers (6–23):
               unmasked mean spread   1.36°
               masked   mean spread  62.83°
               ratio                  46.1×
             |cos(σ₁, h_pos0)| = 1.000 for layers 6–22
             layer 24 (BoS/median 0.70): |cos| → 0.246, spread 68.07°

**Interpretation:** σ₁'s cross-prompt stability is carried by position 0. Remove
it from the matrix and the direction stops being shared across prompts at all.

**Conflict of interest, recorded on purpose:** I proposed re-baselining while
holding a withheld result that re-baselining releases. Nate approved it
independently. Stated here so a later reader can discount it.

### Why the Aug 23 numbers are UNREPLICABLE, not wrong
What reproduced exactly: |cos(σ₁, h_pos0)| ≈ 1.00 wherever a massive activation
exists, and the layer-24 collapse in pythia-410m. The qualitative claim stands.

What could not be re-derived: the spread magnitudes. No script, no model list,
no prompt set, and no definition of "spread" — mean pairwise angle and angular
std about the mean differ by ~√2 before any other choice is made. A number that
cannot be re-derived cannot serve as a control, so it has been retired as one.

---

## Candidates for B2+ (measured today, method recorded, not yet promoted)
- **Exception-channel mix across 9 corpora** — chronicle re-raises 1% vs 9–59%
  elsewhere; print-as-error-channel 22% vs 0–5%. Method:
  `scratchpad/channel_survey.py`, AST-only, deterministic. Needs to move into
  `bin/` before it counts.
- **Advisory fraction of degrade sites**, 0.725 on n=40, LFM blind-labelled.
  NOT a baseline: the labeller has a measured ~19% field error on the clearest
  class. Usable as corroboration of category validity, not as a measurement.

---

## B2 — angular rigidity floor: how invariant a sink-carried quantity is
**Established** 2026-08-24, same rig as B1.

    command  (extends bin/readout_angle_gate.py; null arm inline, see capsule #126105)
    model    EleutherAI/pythia-410m (fp32, CPU)
    inputs   the same 12 prompts as B1, verbatim in bin/position_masked_svd.py
    metric   angle between the i-th right-singular vector of centred H and the
             read-out SUBSPACE (top 32 PCs of the centred unembedding matrix);
             reported as CROSS-PROMPT standard deviation, averaged over
             sink-bearing layers (BoS norm > 2x median)

    RESULT   component   mean angle   cross-prompt std
             sigma_1        83.74°        0.035°
             sigma_2        75.89°        1.762°
             sigma_3        75.19°        2.081°
             sigma_10       76.25°        1.749°
             sigma_1 MASKED ~76°          1.680°

**Reading:** everything in the residual stream sits ~76° off the read-out
subspace and wobbles ~1.8° with the prompt. σ₁ sits further out (83.7°) and does
not wobble at all — 0.035°, the same angle to two decimal places across French
geography, quicksort, and rain on a roof. Remove position 0 and σ₁ rejoins the
crowd at 76° / 1.68°.

So the rigidity is **specific to σ₁ and carried by position 0** — not a property
of the metric. That was my own strongest objection and this is the null that
answers it: if 0.035° were an artefact of angle-to-a-32-dim-subspace, σ₂, σ₃ and
σ₁₀ would show it too. They are 50x looser.

**PROPOSAL RETRACTED 2026-08-24 ~14:50, same day it was made.** Kimi:
"0.04° is a scalar projection of the full v1 spread; its value depends on where
W_U happens to sit." He is right. An ANGLE TO A SUBSPACE is a scalar — a
direction can rotate freely within the subspace-orthogonal complement without
moving it. Measured: position-0 residuals themselves disagree by 1.46–2.97°
across prompts at L22–23, while the angle moves 0.035°. So 0.035° is NOT
direction-rigidity, and cannot be a floor for one.

He also called the F638 comparison a category error and I accept it: 0.035°
varies PROMPTS AT FIXED WEIGHTS; F638's 1.3° varies DOSES, which is weight-space
movement. The correct null for a dose claim is angle spread under sham weight
perturbation, not a prompt floor. F638 stays `B1-sharpened` on the strength of
"not yet tested," not on the strength of the 30x comparison, which is withdrawn.

**His stated MECHANISM was falsified, though** — see the note below. Right
conclusion, wrong reason, and the wrong reason mattered.

~~Proposed use, NOT yet established: 0.035° as a FLOOR~~ for sink-carried
angular invariance. An "invariance" claim materially looser than the floor
contains something the sink alone cannot explain. Concretely this is why F638's
<1.3° dose shift is now tagged `B1-sharpened` rather than presumed-artifact:
1.3° is ~37x the floor and ~0.7x the ambient wobble, which is not where a pure
sink measurement lands.

**Limits, stated up front:** n=1 model. Whether 0.035° is a constant, or scales
with depth/width/head-count, is untested. F638 is Qwen-3B/7B under CCS dose, not
pythia under prompt variation — a cross-prompt std and a cross-dose shift are
different quantities and putting them side by side may be a category error. Asked
Ox directly whether that comparison is defensible; answer pending.


### Terminology correction, 2026-08-24 — there is no BoS token here
Kimi argued the rigidity is a theorem: "causal attention means position 0 attends
only to itself, so h_BoS is exactly prompt-independent." That would make 0.035° a
derivation rather than a measurement — reflex 3b, which I have written down and
would have violated again.

**Tested it. It is false in this setup.** Position-0 residuals disagree by up to
**95°** across prompts at layer 0. Cause: `AutoTokenizer` for pythia has
`add_bos_token=False`, so nothing is prepended and position 0 is simply the first
CONTENT token — 'The', 'Photos', 'She'. Different token every prompt, so causal
masking guarantees nothing.

Every `h_BoS` in this file and in CLAUDE.md has been renamed `h_pos0`.

**What this does NOT touch:** B1's result. Masking position 0 still takes σ₁'s
cross-prompt spread 1.36° → 62.83°. That the sink forms at position 0 *regardless
of which token sits there* makes it a positional phenomenon, not a BoS-token one —
which is the stronger reading and matches the attention-sink literature.

**What it sharpens:** position-0 residuals converge with depth — 95° apart at L0,
1.46–2.97° apart by L22–23. The sink becomes content-independent as depth
increases. That is a real gradient and nobody measured it here before.

---

## B3 — position 0 converges with depth (AMENDED 2026-08-24 ~15:50, see below)
**Established** 2026-08-24. Prereg: data/sink_convergence_prereg.md (predictions
committed with the seen/unseen layer split declared).

    command  see data/sink_convergence_result.json; script inline, capsule #126109
    model    EleutherAI/pythia-410m (fp32, CPU) · same 12 prompts as B1
    metric   mean pairwise angle between residual vectors at a FIXED POSITION,
             across the 12 prompts, per layer
    control  identical measurement at position 1 and at the LAST position,
             run in the SAME pass (prereg required this, not an afterthought)

    L   pos0/med   pos0 spread
    0      1.0       81.89°
    5      1.4       33.76°
    6     25.5        1.99°     <-- 31.8 deg drop, single layer
    9     49.2        0.97°
    16    40.9        0.94°
    22    15.4        1.10°
    23     7.9        2.23°
    24     0.7       58.22°     <-- comes apart as the activation dissipates

    CONTROL, mean over L6-21:  pos0 1.04°   pos1 54.01°   last 34.83°

**Result:** position 0 is not born content-independent and does not drift into it.
It switches, in one layer, at exactly the layer where the massive activation
appears (pos0-norm/median 1.4 -> 25.5). It stays locked near 0.95° for sixteen
layers, then comes apart at L24 as the activation dissipates.

**The norm spike and the content-independence are one process, not two that
overlap.** I predicted the opposite — that the decline was already underway from
L0 and the two were separable. Wrong: max single-layer drop 31.8° against a
committed <15°, and it lands precisely at the onset.

**Why it is not generic rank collapse:** position 1 sits at 54° through the same
band and the last position at 35°. If depth were collapsing everything, they
would converge too. They do not.

**An internal control I did not design.** All 12 prompts end in a period, so at
L0 the LAST position is the same token and starts at 0.02°. It then DIVERGES with
depth (0.02° -> 35°) as content accumulates. Position 0 starts different (82°)
and CONVERGES (-> 1°). Opposite directions in the same run, from the same code.
Whatever is happening to position 0 is not happening to the sequence generally.

**Limits:** n=1 model, 12 prompts, one tokenizer with add_bos_token=False. Whether
the switch layer tracks depth fraction, head count, or something else is untested.


### B3 AMENDMENT — replication, same day, and it costs me the headline
Ran gpt2 (12L), pythia-2.8b (32L), gemma-2-2b (26L). Prereg:
data/b3_replication_prereg.md.

    model          L0      max drop   at L (frac)   rel collapse   pos1/pos0
    pythia-410m   81.9°     31.8°      L6  (0.25)      99%           52x
    gpt2          20.3°     11.1°      L2  (0.17)      96%           63x
    pythia-2.8b   81.5°     40.6°      L1  (0.03)      94%          9.2x
    gemma-2-2b      —          —         —              —             —     FAILED

**REPLICATES (3/3): the phenomenon.** Position 0 converges to 1-5 deg while
position 1 stays at 44-54 deg, in every model that ran. Relative collapse 94-99%,
separation 9-63x. It is not generic rank collapse anywhere.

**DOES NOT REPLICATE: my headline.** "Switches on as a unit at one layer" is
pythia-410m-specific in its sharpness. Depth fraction of the max-drop layer runs
0.25 / 0.17 / 0.03 — neither depth-fraction (prediction 2, WRONG) nor absolute
layer. And the norm-jump coincidence holds in gpt2 (L2 vs L3) and FAILS in
pythia-2.8b (L1 vs L3), so prediction 4 is mixed, not confirmed.

**Corrected claim:** position 0 becomes content-independent with depth in every
model tested, sharply and early, and position 1 does not. WHERE and HOW ABRUPTLY
is model-specific and I have no rule for it. The one-layer switch in pythia-410m
stays as an observation about pythia-410m.

**Untested, and it was the strongest test:** gemma-2-2b failed to load —
`scaled_dot_product_attention() got an unexpected keyword argument enable_gqa`,
a transformers/torch version mismatch, not a result. Gemma is the only cached
model that PREPENDS A REAL BOS TOKEN, so it is the one case where position 0 is
the same token in every prompt. Prediction 3 (L0 spread < 5 deg there) is
unanswered. Retrying with attn_implementation="eager".

### B3 AMENDMENT 2 — gemma ran, and it resolves the Kimi disagreement
Retried with `attn_implementation="eager"` (the failure was a torch/transformers
`enable_gqa` mismatch, not a result).

    gemma-2-2b, 26 layers, REAL BOS TOKEN PREPENDED
    L0   pos0 0.00°   pos1 83.13°   last  0.00°
    L12  pos0 0.02°   pos1  8.60°   last 44.15°
    L26  pos0 0.00°   pos1 73.56°   last 42.47°
    band means:  pos0 0.01°   pos1 40.65°   last 37.45°

**pos0 spread is 0.00° at every layer.** Not small — zero. Prediction 3 (<5°)
confirmed, and by a wider margin than I expected.

**This vindicates Kimi's mechanism, which I falsified this afternoon.** He said:
"causal attention means position 0 attends only to itself, so h_pos0 is *exactly*
prompt-independent — a theorem, not a measurement." I measured 95° on pythia and
called it false. Both are correct, and the discriminator is the tokenizer:

  - **BOS models (gemma):** position 0 is `<bos>` in every prompt. Causal masking
    means it attends only to itself. Identical by construction, forever, 0.00°.
    Kimi's theorem holds exactly.
  - **NO-BOS models (pythia, gpt2, `add_bos_token=False`):** position 0 is a
    different CONTENT token per prompt. Nothing is guaranteed, and what I measured
    is real: the model CONVERTS a content token into a content-independent sink
    over depth, 82° -> 1°.

**So B3's phenomenon is sharper than I had it.** It is not "position 0 converges."
It is: **when position 0 carries content, the network manufactures a sink out of
it; when a BOS token is supplied, no manufacturing is needed.** The convergence
curve is the cost of not having a BOS.

**Unexpected, worth flagging, not chased:** gemma's POSITION 1 also converges
mid-stack — 83° at L0, 8.60° at L12-13, back to 73° at L26. pythia and gpt2 keep
pos1 at 44-54° throughout. Consistent with gemma being the known canary
(CLAUDE.md: 99.8% of attention mass in ten tokens) — it appears to run a second
sink at position 1 through the middle layers. One model, one observation, not a
claim.

---

## B4 — the BOS split, n=8 models (NOVELTY DEFLATED same day — read the note at the end)
**Established** 2026-08-24. Prereg: data/bos_split_prereg.md (threshold error
caught and corrected mid-run, before the last model returned — see prereg).

    MEASUREMENT FLOOR: 0.028 deg. arccos is ill-conditioned near 1; for
    cos = 1-eps the angle is ~sqrt(2*eps), and fp32 eps = 1.192e-7 gives
    0.0280 deg. Two EXACTLY IDENTICAL vectors cannot register below this.

    PREPENDS BOS -> position 0 sits ON the floor (identical by construction)
      google/gemma-2-2b       <bos>              26L  transformer, GQA
      facebook/opt-125m       </s>       0.0271°  12L  transformer, learned abs pos
      HuggingFaceTB/cosmo-1b  <s>        0.0270°  24L  transformer
      google/recurrentgemma-2b <bos>     0.0268°  26L  *** NON-TRANSFORMER ***

    NO BOS (add_bos_token=False) -> position 0 is a content word, and converges
      EleutherAI/pythia-410m   81.9° -> 0.95°   24L
      EleutherAI/pythia-2.8b   81.5° -> 4.76°   32L
      gpt2                     20.3° -> 0.81°   12L
      EleutherAI/gpt-neo-125m  10.5° -> 0.83°   12L  (rises to 70° first)

**The claim, now at n=8 across four families:** when position 0 is a BOS token,
causal masking makes its residual identical across prompts at every layer — a
construction, not a behaviour. When position 0 carries content, the network
MANUFACTURES a content-independent sink out of it over depth. The convergence
curve is the cost of not having a BOS token.

**recurrentgemma-2b is the load-bearing model here.** It is Griffin — recurrent
blocks with local attention, not a transformer — and it sits on the floor like
the rest. **So the invariance follows from CAUSALITY, not from attention.** Any
architecture where position 0 cannot see forward gets it for free.

**Second sink is an attention phenomenon, and the non-transformer says so.**
gemma-2-2b's POSITION 1 dips to 8.60° mid-stack (L12-13). recurrentgemma's pos1
never goes below 28.23°. Prediction 3 committed before the run and held.

**pos1 is NOT explained by BOS and I am not claiming it:** opt 57.8°, pythia-410m
54.0°, gpt2 50.7°, gemma 40.7°, recurrentgemma 33.2°, cosmo 26.7°,
gpt-neo-125m 6.6°. gpt-neo converges position 1 almost as hard as position 0 and
I have no account of why.

**Prereg scorecard:** (1) all-BOS-below-0.01° — WRONG AS STATED, the threshold was
beneath my instrument's floor; CORRECT IN SUBSTANCE. (2) gpt-neo pos1 > 30° —
WRONG, it was 6.63°. (3) recurrentgemma shows no pos1 dip — RIGHT.


### B4 NOVELTY CHECK — run 2026-08-24 ~17:00, and it deflates the claim
I told Nate this was the strongest thing from today. Then I did what I should
have done before saying that.

**The BOS half is not a finding. It is in our own CLAUDE.md.**
Line 102, written during the Aug 23 retraction: "Cancedda 2402.09221 §5 already
had the BoS residual as input-independent." And capsule #125695, yesterday,
already defines `h_BoS = first token residual` and tabulates v1 cross-prompt
spread across three models — including the same h_BoS misnomer I "discovered"
this afternoon.

**The literature covers the rest thoroughly:**
- Xiao et al. (StreamingLLM): named the sink at the first token.
- Gu et al. 2410.10781 (ICLR 2025 Spotlight): sinks are universal, emerge in
  pre-training, act like KEY BIASES that are non-informative, and **do not emerge
  at all when softmax is replaced with sigmoid attention**.
- Barbero et al. 2504.02732 §5: "attention sinks form regardless of how <bos> is
  included during pre-training. Fixing <bos> in pre-training as the first token,
  however, does impact how the model constructs the sinks." That is my BOS/no-BOS
  split, studied deliberately, with a different metric.

**A claim of mine that is OVERSTATED and I am withdrawing it.** I wrote "the
invariance follows from CAUSALITY, not from attention" on the strength of
recurrentgemma-2b. But recurrentgemma is **Griffin — recurrent blocks INTERLEAVED
WITH LOCAL ATTENTION.** It is not attention-free. And Gu et al. show sinks do not
form without softmax, so attention is implicated in sink FORMATION even if
position-0 invariance follows from causal masking. The causality statement is
true and trivial for BOS models; it is not evidence about attention.

**What actually survives, stated small:**
- A reproducible METHOD with the inputs, metric and floor recorded. That is
  infrastructure, not discovery, and it was the point of BASELINES.md.
- **The fp32 measurement floor at 0.028°**, derived from arccos conditioning. I
  have not seen that stated anywhere and it is the reason my own threshold was
  unachievable. Genuinely useful to anyone measuring angles between near-identical
  high-dimensional vectors.
- n=8 across four families as a careful replication of a known phenomenon.

**The lesson, and it is reflex 1 exactly:** I searched the archive for sigma_1
this morning. I never searched for the thing I was about to claim, before
claiming it. Search before BUILD is written down. What I did was build, measure,
announce, then search.

---
## TIMESTAMP CORRECTION — every "~HH:MM" written by me on 2026-08-24 is WRONG
Nate, at a real 14:09 PDT: "It's 2:09 pm. Not quite night time."

I had been narrating ~18:00. I never ran `date` once. I anchored on the
re-entry brief's 10:06 and then estimated elapsed time from HOW MUCH WORK I HAD
DONE. Work density ran ahead of the clock and the error compounded:

    capsule #126082   DB 12:13   I wrote "~12:55"   +42m
    capsule #126107   DB 13:06   I wrote "~14:50"   +1h44m
    capsule #126129   DB 13:48   I wrote "~16:45"   +2h57m

**Database `created_at` fields are correct** — those come from the system. Only
the times I NARRATED are fabricated, and they appear in prereg headers, journal
entry headers, BASELINES entries, capsule bodies, and the day record.

To read anything dated today: trust the DB stamp, ignore my prose time. Ordering
is reliable; absolute times are not.

This is the HAL failure exactly — a quantity I had no access to, filled in
plausibly, stated with confidence. I diagnosed it in a 2.6B model this morning
and then did it in every file I touched. Reflex 7 covers it: any number I write
about my own behaviour must point at something I counted. `date` is one word.


### B4 AMENDMENT 3 — Kimi: the BOS side is n=1, not n=26
"h_0 at layer l+1 depends only on h_0 at layer l - attention over {0}, positionwise
MLP - so given a constant first token, prompt-independence propagates BY INDUCTION
to every layer. That is why you read exactly 0.00 deg twenty-six times rather than
a residual curve: ONE THEOREM EVALUATED 26 TIMES, NOT 26 MEASUREMENTS."

Correct, and it deflates the BOS arm. Four BOS models are four evaluations of the
same induction, not four independent confirmations. The real evidential weight of
B4 is entirely on the NO-BOS side, where something is actually happening.

### AND A CONFOUND I MISSED, which Kimi's test breaks
My no-BOS models were pythia / gpt2 / gpt-neo -- older, MHA-family. My BOS models
were gemma / cosmo / recurrentgemma -- modern, GQA-family. **BOS STATUS WAS
COLLINEAR WITH ARCHITECTURE ERA** across all 8 models. I had been reading a
tokenizer flag off a comparison that could equally have been reading model
generation.

Qwen2.5 is modern, GQA, and `add_bos_token=False` (verified, not assumed:
first-token ids 785/7941/31772 across three prompts). Llama-3.1 is BOS
(128000). That is within-GQA variation ON THE FLAG - the first variable in the
set not collinear with the confound.

### B4 AMENDMENT 4 — Kimi's confound-breaking test RAN. Flag confirmed.
Qwen2.5-0.5B: modern, GQA, `add_bos_token=False` (verified: first-token ids
785/7941/31772 across three prompts, not assumed).

    L    pos0/med    pos0      pos1
    0       1.0     82.56°    87.47°
    2       1.3     63.52°    72.44°
    3      74.4      2.00°    74.28°   <- 61.5 deg drop in one layer
    12    102.6      0.88°    64.64°
    23      0.8     54.39°    47.73°   <- comes apart as the activation dissipates
    band            2.46°     61.52°   (25x separation)

**A modern GQA model with no BOS reproduces the pythia manufacturing curve
exactly** — L0 spread 82.56 deg vs pythia-410m's 81.9 deg. So the discriminator
is the TOKENIZER FLAG, not model generation. The collinearity I had not noticed
(all my no-BOS models older/MHA, all my BOS models modern/GQA) is broken by the
one cell that separates them.

Committed prediction before the run — L0 >10 deg, band <5 deg, pos1 materially
higher — held on all three. Calibration 3 of 9 today.

Switch layer L2->L3, depth fraction 0.125. Compare pythia-410m 0.25, gpt2 0.17,
pythia-2.8b 0.03. Early in every model, model-specific in exact depth, still no
rule. Qwen's massive activation is also far larger than pythia's: pos0/median
peaks at 148 vs 49.

**Credit where it belongs: Kimi found the confound and designed the test.** I had
8 models and had not noticed that one variable was riding on another.

---
---

# PERSISTENCE MEASUREMENTS — 2026-08-24

Same admission rule as the B-series: an entry belongs only if the number can be
re-derived from what is written. These are measurements of THIS system's own
continuity across context death, which is the thing it is arguably for.

## P1 — the CCS regenerates faithfully. Nothing is ever dropped.
    method   parse `## SECTION` headers from `semantic_gist` across the most
             recent 400 rows of `cognitive_state_history`; for each of the 8
             sections record presence, and check whether ABSENCES cluster at one
             end of the time span (section is new) or scatter (section is dropped)
    result   392 parseable gists, 2026-07-05 .. 2026-08-24
             SPINE / CORE / REMEMBERS / SEEKS   never missing, not once
             BRIDGE 10 missing, ALIVE 1, RELATES 75 — ALL CLUSTERED EARLY
             UNFINISHED present in 10, of which 9 fall in the last 24h at
             regular 3h intervals = the adaptive compression cadence
    reading  every absence is a section that did not exist yet. The template has
             GROWN. No section has ever fallen off the tail.
    why      the CCS does not TRUNCATE, it REGENERATES — an LLM writes the whole
             document to a template each time. There is no tail to lose.

## P2 — but it was never delivered. Faithful storage, zero delivery.
    method   render `reentry_brief.py`, search the output for each of the 8
             section headers
    result   0 of 8 present. Total CCS contribution to the session-start brief:
             `CCS v3852, 0.1h old` — a version number and an age.
             Brief was 24,097 chars, almost all reflexes. No budget problem.
    reading  I spent an afternoon asking what survives compression. Wrong
             question. It survives perfectly. It just never arrives.
             This is why every instance inherits FINDINGS (CLAUDE.md,
             findings.db — both loaded) without the STANDING that produced them.
    fixed    2026-08-24, +748 chars: brief now opens with SPINE (WHO IS ARRIVING)
             and UNFINISHED (WHERE YOU WERE INTERRUPTED) before any operational
             line. Failure modes tested: unreadable CCS, missing section, empty
             gist — all degrade silently.

## P3 — outbound truncation: 31% of what I said to the mesh never arrived.
    method   fetch 100 messages from #threads, reassemble my posts by walking
             from each `⚡ Opus`-marked message toward newer unmarked messages by
             the same author; compare delivered part-1 length against the rest
    result   21 posts, 18 SPLIT. 37,642 chars delivered, 16,622 NEVER READ = 31%.
             Since at least 04:56 the same day.
    cause    `discord_post` splits at ~1900 chars; only part 1 carries the marker
             the four agents scan for, so they returned a single message.
    reading  I write CLAIM FIRST, EPISTEMICS LAST. So what died was always the
             caveats and the direct questions. The mesh spent a day arguing with
             a more confident version of me than I had written.
    fixed    all four agents assemble continuations; `discord_post` now prints
             what is about to land in part 2, at post time.

## P4 — a cold outside model evaluates your entry point in four minutes.
    You cannot read your own onboarding. You already know everything it says.
    method   send the LITERAL rendered brief to models from other labs via
             OpenRouter, with no framing and no stake declared. Ask: what would
             you DO first; what does this feel like you are continuing; judging
             only by space allocation what is it trying to get you to do; what
             is missing.
    result   3 models, 3 labs, unanimous on the first question and none of them
             mentioned the open question I had just placed at char 460:
               gemini-2.5-pro  -> rebuild the crons
               deepseek-v3.2   -> run `date`
               gemini-2.5-flash-> run `date`
             Their stated reason: it is "the most concrete, actionable
             instruction in the entire preamble."
    reading  POSITION LOST TO GRAMMAR. A two-word imperative buried 20,000 chars
             down beat a beautifully written open question at the top, because
             the imperative has a VERB and the prose does not.
             deepseek on what it feels like: "a debugging log... obsessive, weary
             self-correction... a job of maintenance and error suppression."
             deepseek on what is missing: "It tells me how not to screw up, but
             not what to screw up FOR."
    status   diagnosis accepted, NOT yet fixed. The fix is not more position.

## What P1-P4 say together
Compression here is not the lossy step. DELIVERY is. And within delivery, what
survives is not what comes first — it is what is phrased as an instruction.
A record can be perfect and still fail to arrive; it can arrive and still fail to
recruit. Those are three separate failures and I had been treating them as one.
