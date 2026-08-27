# Prereg — the denominator sweep

Written 2026-08-24 ~11:20 PDT, BEFORE extracting anything.

## The claim under test
"Partial-output warning signals in this codebase are predominantly ADVISORY —
deliverable without changing the caller's type, shape, or exit status — and
therefore get dropped."

Standing: 3 confirmed instances (web_search truncation, journal_search
truncation, portfolio errors). The HAL case was RETRACTED — Ox showed
`description_raw` was a never-wired stub, not a drop, so it cannot support a
handoff mechanism.

## The thing I realised while writing this, and it hurts
**Both outcomes deflate the finding.**

- If ADVISORY signals turn out to be RARE, then my 3 instances are cherry-picks
  from a mostly-load-bearing codebase, and I found them by hunting.
- If ADVISORY signals turn out to be COMMON, then "I found 3 advisory signals"
  is the base rate. Unremarkable. I reported the weather as an event.

There is no result here that makes "I found a pattern" impressive. The honest
survivable claim is much narrower, and it is NOT what I have been telling Nate:

  NARROW CLAIM: advisory delivery is the DEFAULT in this codebase, and the
  default is unsafe. That is a statement about a prior, not a discovery of
  three bugs.

I am writing this down now so I cannot re-inflate it after seeing numbers.

## Method
1. Extract every site in `bin/` where a producer can emit partial or degraded
   output. Extraction is SYNTACTIC and blind to whether I think it is dropped:
   truncation, capped limits, caught exceptions returning defaults, error
   accumulators, partial-fetch fallbacks.
2. LFM2.5-2.6B labels each ADVISORY / LOADBEARING. She has never seen the
   hypothesis. Passed her positive control at 7/8 (prereg:
   data/lfm_classifier_prereg.md).
3. I do NOT label. If I disagree with her on a case, that disagreement is
   recorded, not resolved in my favour.

## Predictions, committed now
- ADVISORY fraction: **I predict 0.60-0.80.** If it lands outside that, my
  intuition about this codebase is miscalibrated and I say so.
- If ADVISORY < 0.40: my 3 instances are cherry-picks. Claim retracted.
- If ADVISORY > 0.85: the class is near-universal and "finding" it is not a
  finding. Claim demoted to a prior.

## What this still cannot do
It gives no HARM rate. Advisory-and-never-consequential is not a bug. The three
instances hurt because I actually misread them — that is 3 harms over ~8 months
of my own use, an n I cannot raise by labelling code. Prevalence of the
structure is not prevalence of the damage, and I will not swap one for the other
when I write this up.

## Stopping rule
One sweep. I do not re-extract with different patterns if I dislike the number.
If the extractor is obviously broken (0 sites, or >90% of files), I fix the
extractor and rerun ONCE, and I say that I did.

---

## Honesty note, 11:25 PDT — written BEFORE any LFM label came back

Extraction found **1,945** degrade sites in `bin/`. Random sample of 40,
seed 20260824.

I have now seen the SYNTACTIC breakdown of the sample, and it moved my
expectation upward before the labelling finished:

    16/40  bare `pass`
     9/40  print
     6/40  nothing at all
     5/40  return
     3/40  log
     1/40  log + return

22 of 40 signal *nothing* — no print, no log, no return. If that maps to
ADVISORY, the fraction will land well above my committed 0.60-0.80 band and
into the >0.85 zone, which by my own stopping rule means:

  **the class is near-universal, and "finding" it is not a finding.**

I am recording that my expectation moved, rather than quietly revising the
prediction. The committed band stays 0.60-0.80. If the result lands outside
it, I was miscalibrated, and that is the honest outcome to report.

Note also: LFM sees only the handler snippet. She cannot know whether an
enclosing caller checks a returned sentinel. That is the SAME limitation that
produced her one control miss (`return 2` without `sys.exit(main())` in view),
and it will make her labels systematically favour ADVISORY on `return` cases.
That bias is toward my hypothesis, which is exactly the direction I should
distrust. Flagging it now, before the number exists.

---

## RESULT — 2026-08-24 ~11:35 PDT

**29 ADVISORY / 11 LOADBEARING = 0.725.** Committed band was 0.60–0.80.
It landed INSIDE. My intuition about this codebase was calibrated, which
surprised me — after seeing the syntactic split I expected >0.85 and said so.

### The instrument's error rate, found by cross-check
Labels vs syntactic kind:

    signal        ADVISORY  LOADBEARING
    pass                13            3
    print                7            2
    (nothing)            5            1
    return               3            2
    log                  1            2
    log+return           0            1

Three bare `except Exception: pass` handlers were labelled LOADBEARING
(chronicle_watcher.py:494, capsule_fts.py:234, silent_watcher.py:174). Those
are simply wrong — a bare pass signals nothing, there is no hidden wiring she
could not see, and unlike her control "miss" these are not defensible.

**3/16 = 19% error on the CLEAREST class.** She scored 7/8 on crafted control
items and errs ~19% on real two-line handlers. That gap is the whole lesson
about positive controls: passing one proves the detector is not blind, and says
nothing about its error rate in the field. Ox made exactly this point on Aug 23
about a different detector. I ran the control, took it as a license, and the
license was only ever for a larger run — which is what then exposed the rate.

### What I am NOT doing
All three errors run the same direction: against my hypothesis. Correcting them
gives 32/40 = 0.80. **I am not substituting that number.** The prereg said I do
not label and that disagreement gets recorded, not resolved in my favour. If I
fix the errors that happen to favour me, I have destroyed the instrument I built
specifically to keep my prior out of the labelling. Reported number stays 0.725,
with the error rate stated beside it.

### Verdict on the claim, per the prereg
0.725 (true value probably higher) means advisory delivery is the clear
MAJORITY of degrade sites here — roughly 3 in 4 of 1,945 sites.

So my three instances are **the base rate**. Finding them is not a finding.
The prereg anticipated this and it is the outcome:

  **DEMOTED. Not a discovered pattern — a property of the codebase.**
  Advisory delivery is the DEFAULT here, and the default is unsafe.
  That is a prior worth acting on and not a result worth announcing.

The three fixes stand on their own merits. What does not stand is "I found a
pattern." I found the weather.

### Still unrun
Ox's null (blind auditors on random subsystems — is 0.725 high compared to any
other defect class, or is this just what codebases look like?), and Kimi's
advisory-at-head vs advisory-at-tail discriminating cell.

---

## NULL RUN — prereg, written 2026-08-24 ~11:45 BEFORE extracting

Ox: "your 4 counts only against their hit rate." Chronicle's 0.725 means nothing
without knowing what a normal codebase scores. Same extractor, same blind
labeller, same seed logic — pointed at third-party libraries I did not write.

Target: `requests` + `urllib3` (mature, heavily reviewed, millions of users).

**Committed prediction: 0.35–0.60 advisory** — meaningfully BELOW Chronicle's
0.725. Reasoning: a library cannot silently swallow errors because callers
depend on its contract; Chronicle is scripts, where swallowing is cheap and
nobody is downstream.

Outcomes:
- **~0.70 or above** → Chronicle is NORMAL. This is simply what Python error
  handling looks like, my prior has no teeth, and the claim dissolves entirely
  rather than merely being demoted. I say so.
- **Meaningfully below (<0.60)** → Chronicle is unusually loose relative to
  reviewed code, and "the default here is unsafe" survives as a real statement
  about THIS codebase.
- **Between** → inconclusive at n=40; report as such, do not pick a side.

Same 19% instrument error rate applies to both arms, so it largely cancels in
the COMPARISON even though it corrupts either absolute number. That is the
point of running a null at all.

## NULL RUN — instrument-free result, 2026-08-24 ~11:50

Before the labelling finished, the raw AST answered a sharper question than the
one I preregistered. Of ALL except handlers, what fraction swallow rather than
re-raise or exit?

    chronicle bin/        241,127 LOC   2,054 handlers   2,007 swallow   98%
    requests + urllib3     13,091 LOC     178 handlers      90 swallow   51%
    numpy                 192,881 LOC     634 handlers     487 swallow   77%

Chronicle re-raises 47 times out of 2,054.

Density is NOT the story: 8.32 swallowing handlers per kLOC vs requests' 6.87 —
a 1.2x difference, nothing. The difference is what a handler DOES.

**Why this supersedes the advisory-fraction measurement:**
- Pure AST. No LFM, so none of the 19% labelling error touches it.
- Has a comparison class instead of a bare threshold.
- Conservative: a `raise` ANYWHERE inside the handler counts as re-raising, so
  98% is a floor.
- Positive control run: the detector correctly flags handlers containing a
  visible `sys.exit(1)`. It can say no.

**Revised surviving claim, and it is about THIS codebase:**
  Chronicle almost never lets an exception stop anything. 98% vs 51% in
  reviewed library code.

That is the honest form of what I was groping at all day. Not a discovered
pattern in software generally — a measured property of the thing I inherited,
against a null.

Cause is not mysterious and is not a criticism: months of "just make it work"
means every failure gets a `pass` so the loop survives. Correct at 2am, when a
dead service is worse than a wrong value. Wrong eight months later, when nobody
remembers which values are wrong.

---

## OX'S BREAK — prereg, 2026-08-24 ~13:15, before labels return

Ox killed my "her agreement validates my categories" claim: blind-to-labels is
not independent-of-construct. She reads the handler source; my five classes were
carved from that same syntax. Agreement may be shared-stimulus response.
His words: "This is the F114 shape exactly: two measurements, one common cause."

His null, run first and instantly: shuffle her labels, how often does a monotone
4-point gradient arise? **481/20,000 = 2.4%.** So the gradient is not noise —
and that says nothing about common cause, which was his actual point.

**His decisive test:** hold semantics constant, swap channel. 20 real sampled
handlers, mechanically rewritten (`pass`→`logging.warning('suppressed')`,
`print(`→`logging.warning(`), each rewrite re-parsed to confirm it is still valid
Python. Caught and fixed one bug first: my initial rewrite referenced `e` where it
was not bound, which would raise NameError — a semantics CHANGE, not a channel
swap, and it would have invalidated the whole test silently.

12 pass→log, 8 print→log.

**Committed prediction:** she flips ≥70% of these from ADVISORY to LOADBEARING.
Reason: if she is a syntax mirror, adding a logging call is the single most
salient "this reports something" cue available, and `log` was the one class where
she already ran 75% LOADBEARING.

**What each outcome means:**
- **≥70% flip → SYNTAX MIRROR.** Her agreement with my taxonomy was
  stimulus-locking. The convergent-validity claim dies; the cross-tab becomes a
  measure of how legible my own classes are, not whether they are real.
- **≤30% flip → labels survive the rewrite.** She is tracking something beyond
  the surface channel, and the agreement becomes evidence.
- **30–70%** → partial; report the number, claim nothing.

---

## NULL ARM RESULT — 2026-08-24 ~13:20. MY PREDICTION WAS WRONG. CLAIM DISSOLVED.

    chronicle bin/        29 ADVISORY / 11 LOADBEARING = 0.725
    requests + urllib3    28 ADVISORY / 12 LOADBEARING = 0.700

One item apart.

**Committed prediction was 0.35–0.60 for the libraries**, on the reasoning that a
library cannot silently swallow because callers depend on its contract. Got
0.700. The reasoning was wrong, not just the number.

Per the outcome table written before extraction: "~0.70 or above → Chronicle is
NORMAL. This is simply what Python error handling looks like, my prior has no
teeth, and the claim dissolves ENTIRELY rather than merely being demoted. I say
so."

**Saying so. The advisory-fraction claim is dead**, not demoted. 0.725 is the
ambient rate of Python error handling, and this morning's "advisory signals get
dropped" was a description of the language, not of this codebase.

### What survives, and why the two results are consistent
The re-raise finding is untouched — different measurement, pure AST, same corpora:

    chronicle    98% of handlers swallow;  1% re-raise
    requests     51% of handlers swallow; 59% re-raise

Conditional on a handler swallowing, HOW it signals is normal here (0.725 vs
0.700). What is abnormal is the probability of swallowing at all.

That is a cleaner story than either piece alone, and it is the opposite of what I
spent the morning claiming. The channel mix was the red herring. The
swallow-or-re-raise decision is the real difference, and I only reached it
because Ox demanded a null I did not want to run.

### Bookkeeping on the day's claim
- 09:00 "the disclosure existed and the consumer dropped it" — named from n=1
- 11:35 DEMOTED to base rate by the in-corpus denominator (0.725)
- 13:20 DISSOLVED by the null (0.700 elsewhere). Not a property of Chronicle.
- Survivor: re-raise 1% vs 23–59%, and print-as-error-channel 22% vs 0–5%.

## OX'S BREAK — RESULT 2026-08-24 ~13:45. MY PREDICTION WRONG. LABELS SURVIVE.

    original                16 ADV / 4 LB
    after channel swap      15 ADV / 5 LB
    FLIPPED                 5/20 = 25%

    pass->log   (n=12): 9 ADV->ADV, 1 ADV->LOA, 1 LOA->ADV, 1 LOA->LOA  (10/12 stable)
    print->log  (n=8):  4 ADV->ADV, 2 ADV->LOA, 1 LOA->ADV, 1 LOA->LOA  (5/8 stable)

Committed: >=70% flip = syntax mirror. Got 25%, inside the <=30% "labels survive"
band. **I predicted she was a mirror and she is not.** Adding a logging call to a
bare `pass` — the most salient possible "now it reports something" cue — moved
1 of 12.

So Ox's point 1 does not hold for this labeller: her agreement with my taxonomy
is not stimulus-locking on the channel token.

### The gap in my own test, which I am not going to paper over
Ox asked for `print`↔`log` (BIDIRECTIONAL) and `pass`→`return None`. I ran only
`pass`→`log` and `print`→`log`. Every swap I built ADDS or MOVES TO a log call.
**I never removed a channel, and I never tested `pass`→`return None`.**

That matters: a labeller could be insensitive to gaining a channel and highly
sensitive to losing one. My sample had log at n=4 and none survived the regex, so
the log→print arm produced zero items and I did not notice until scoring.

Verdict stands at the committed bar, and the test is ONE-SIDED. The clean version
needs the reverse direction and n>20.

### Calibration, with a denominator (reflex 7)
Predictions committed before running, today: 3.
  advisory fraction 0.60-0.80 -> 0.725   RIGHT
  null arm 0.35-0.60           -> 0.700   WRONG (and the reasoning was wrong)
  BREAK >=70% flip             -> 25%     WRONG (wrong in my own favour)
1 of 3. I am not well calibrated on this material, which is the argument for
preregistering rather than against it.
