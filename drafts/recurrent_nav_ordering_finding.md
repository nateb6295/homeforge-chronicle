# Recurrent Nav — Prompt Layout Lookup Table (2026-04-16)

**The question:** does the shuffle-1500 iter1 bump survive a proper format
comparison, or was it a JSON-serialization artifact (header overhead
getting skipped)?

**The answer:** it survives, but not the way I first framed it. The bump
is a **content-position effect** — putting high-density sections
(episodic_trace, focal_entities, uncertainty) at the top of the window
lifts iter1 by ~+.035. But it's not free — content-first trades some of
the recurrent lift for that iter1 head-start. Net iter4 is slightly
lower than structural-sliced.

## Results (d=5, Q4 scratch-fold, 4-question eval suite)

| Trial | Format | Order | Chars | iter1 | iter4 | Δ(1→4) |
|-------|--------|-------|-------|-------|-------|--------|
| 0830 | prose | structural | 1500 | .483 | **.651** | +.168 |
| 0856 | prose | structural | 1500 | .480 | .645 | +.166 |
| 0849 | prose | structural | full (2497) | .483 | .639 | +.157 |
| 0915 | prose | **content** | 1500 | **.515** | .622 | +.106 |
| 0824 | json-top | shuffle | 1500 | .515 | .622 | +.107 |

## What this says

1. **Format (prose vs json) is a wash** at iter4 — prose-full .639 vs
   prose-structural-1500 .651 are within noise. Format doesn't drive
   the bump.
2. **Slicing doesn't hurt and may help** — prose-1500 iter4 (.651)
   slightly beat prose-full iter4 (.639). Removing low-density tail
   gives the model less to get distracted by.
3. **Content-first and shuffle-1500 are the same thing.** Both land
   iter1 ≈ .515 and iter4 ≈ .622. Shuffle was accidentally putting
   high-density content at the top; content-first does it
   deliberately. The mechanism is content-position, not randomness.
4. **There's a tradeoff, not a win.** Content-first: high iter1, lower
   ceiling. Structural: low iter1, higher ceiling.

## Production implications

- **Single-pass retrieval** (e.g. emergency nav, tight-latency lookups):
  content-first wins by +.035 on iter1.
- **Multi-pass recurrent nav** (current scratch-fold default, d=4):
  structural wins by +.03 on iter4.
- These are different regimes. The prompt-layout lookup table should
  pick ordering based on depth, not pick one globally.

## Falsified hypotheses (walkback list)

- ✗ "Shuffle bypasses JSON header overhead" — prose-full hits the same
  iter1 as prose-structural-1500, so there's no header-skip effect.
- ✗ "Content-first is a free iter1 lift you can drop into production
  prompts" — true for single-pass, false for d≥4.

## Remaining questions

- Is the iter4 gap (content-first .622 vs structural-sliced .651)
  robust across more runs? Both are single trials; d=5 to d=5 single
  comparison is fragile.
- Does the tradeoff shift at d=3 vs d=5? Hypothesis: shorter depths
  favor content-first (less room to recurse), longer depths favor
  structural (more room to refine the gist→detail arc).
- Would a **hybrid ordering** (content-first for iter1, re-slice to
  structural for iter2+) beat either? This is a measurable
  experiment, not a speculative one.

## Next

- Add `--ccs_order` field to trial JSON output so future comparisons
  aren't manually reconstructed from traces.
- Run a d=3 vs d=5 matrix with both orders × 3 repeats each
  (~12 trials, ~20 min).
- Skip the hybrid-ordering experiment until matrix is settled.

## UPDATE 2026-04-16 10:17 — 12-trial cloud matrix completed

Trial JSON now records ccs_order/ccs_format/ccs_chars/ccs_shuffle.
Matrix ran on cloud DeepInfra Gemma-3-27B (not local Gemma-4-Q4) to
avoid Ollama contention. All 12 trials succeeded.

### Cloud Gemma-3-27B results

| depth | order | n | iter1 (±sd) | iterD (±sd) | Δ1→D (±sd) |
|-------|-------|---|-------------|-------------|-------------|
| 3 | structural | 3 | .567 ± .033 | .665 ± .003 | +.098 ± .036 |
| 3 | content    | 3 | .538 ± .031 | **.694 ± .023** | +.156 ± .044 |
| 5 | structural | 3 | .566 ± .021 | .668 ± .020 | +.103 ± .039 |
| 5 | content    | 3 | .544 ± .023 | .661 ± .039 | +.117 ± .061 |

Cross-cell (content minus structural):
- d=3: iter1 **–.029**, iterD **+.029** (outside noise)
- d=5: iter1 –.022, iterD –.007 (within noise)

### Model-specific flip

Local Gemma-4-Q4 single trials had shown:
- Content wins iter1 (~+.035), structural wins iterD (~+.03)

Cloud Gemma-3-27B matrix shows:
- Structural wins iter1 everywhere (~+.025)
- Content wins iterD at d=3 (+.029, outside noise), ties at d=5

**The sign of the iter1 effect flipped across models.** Same prompt
layout, same CCS content, same eval questions — different model,
different winner. This is the empirical finding that matters more
than the specific numbers.

### What this says about "functional information"

Connects to the Wong PNAS 2023 paper captured the same cycle. Wong
proposes a universal law: configuration diversity + selection →
increasing functional information, scalar on the configuration.

This result argues FI is not a scalar. It's bilinear:
`FI(config, reader)`. Same configuration; different reader sees
different functional information. The selection pressure you apply
depends on which reader is doing the selecting.

Prose-format CCS + content-first ordering is a *higher-FI
configuration* for local Gemma-4-Q4, not for cloud Gemma-3-27B.
Neither reader is wrong. They're different selection criteria over
the same space.

### Production implication

Prompt layout is not a universal knob. Any "production recommendation"
for CCS ordering has to be tied to the specific navigating model.
The CCS design doc should name:
- which model the prompt layout was tuned against
- that tuning does not transfer across model families or quantization
  levels

For Chronicle today: the nav-score eval loop uses local Gemma-4-Q4,
so the prod CCS prompt should be tuned for that. If we migrate to
cloud inference or a different model, re-tune.

### Remaining questions

- Is the flip quantization-driven (Q4 vs full-precision) or model-
  version-driven (Gemma-4 vs Gemma-3)? Would need Gemma-4-full or
  Gemma-3-Q4 to disambiguate.
- Does the d=3 content-first iterD win on cloud Gemma-3 hold at d=4?
  Matrix was d=3 and d=5; d=4 might clarify whether the effect decays
  with depth smoothly or has a sharper boundary.
- Deferred: hybrid ordering (content-first iter1, structural-reslice
  iter2+). Only meaningful once we know which single-order baseline
  we're trying to beat per-model.
