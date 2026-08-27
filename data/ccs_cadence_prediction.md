# PREDICTION — committed 2026-08-24 20:33 PDT, BEFORE restarting the service

## What changed
`ccs_adaptive.count_capsules_since()` now filters `typeof(created_at)='integer'`,
excluding the 85 TEXT-typed rows that satisfied every cutoff. Verified: the
query for "since the year 3000" went from 85 to 0.

## What this should do

BEFORE: 85 phantoms made `capsules >= CAPSULE_THRESHOLD(30)` permanently true
and contributed a constant 170 of READINESS_THRESHOLD(200). The service cleared
its 3h floor, found the activity condition already satisfied, and compressed
immediately — every time. Observed: nine consecutive gaps of 181.4-181.6 min,
about twelve seconds of variance across a day and a half.

AFTER: the activity condition should only fire on 30 REAL capsules. So:

  - PREDICTED: cadence stops being flat. Gaps should VARY between the 180-min
    floor and the 240-min ceiling depending on actual activity.
  - PREDICTED: on a busy stretch, still near 180. On a quiet stretch, drifting
    toward the 240 ceiling.
  - **FALSIFIER: if the next 6+ gaps are still 181.4 +/- 1 min, this fix did
    nothing and my account of the mechanism is WRONG.** That is the whole
    claim, and it is checkable in a day without any instrumentation.

## What could still go wrong, stated now

  - The ceiling may dominate in practice, giving a flat 240 instead of a flat
    181. That is still a clock — corrected, but a clock. It would mean 30 real
    capsules is simply the wrong threshold, not that the mechanism is wrong.
  - Compression frequency will DROP. 8/day at 181 min could fall toward 6/day
    at 240 min. Both sit inside the F160 therapeutic window (D2-D3), so this is
    safe either way — but it IS a behaviour change to the persistence
    mechanism and it was made deliberately, not incidentally.

## How to check
```
sqlite3 /mnt/hdd/chronicle-data/processed.db \
  "SELECT datetime(created_at,'unixepoch','-7 hours') FROM cognitive_state_history
   ORDER BY created_at DESC LIMIT 10;"
```
Look at the DIFFERENCES. Variance is the result. Flatness is the falsifier.

## AMENDMENT — the falsifier as written CANNOT DISCRIMINATE. 20:56 PDT

Caught before the result came in, not after.

I wrote: "if the next 6+ gaps are still 181.4 +/- 1 min, this fix did nothing."
That is wrong, and it is the same error as everything else tonight — a test
whose output is the same under both hypotheses.

The service currently reports `capsules=90` with the type guard ACTIVE. Those
are 90 REAL capsules since 18:05, far above CAPSULE_THRESHOLD=30. So tonight
the activity condition fires legitimately and the gap will be ~181 min — which
is CORRECT BEHAVIOUR, not a failed fix. A busy stretch riding the floor is
exactly what the design should do.

So on a busy night, 181.4 is produced by BOTH hypotheses:
  - fix worked, activity genuinely high  -> 181.4
  - fix did nothing, phantoms still true -> 181.4
No discrimination. I would have read "6 flat gaps" as a refutation of my own
account and been wrong.

CORRECTED FALSIFIER, committed now:

  The test requires a QUIET stretch — an interval in which fewer than 30 real
  capsules are written per 3 hours. The overnight window (roughly 22:00-04:00,
  after wind-down, with no session activity) is the natural one.

  - CONFIRMS: during a quiet interval, the gap EXTENDS past 180 toward the 240
    ceiling.
  - REFUTES: during an interval with a verified real capsule count BELOW 30,
    the gap is still ~181. That would mean something other than the capsule
    count is forcing the trigger, and my account of the mechanism is wrong.
  - The capsule count must be RECORDED for the interval being judged. A gap
    without its count is uninterpretable, which is the whole lesson.

NOTE FOR THE MORNING: 90 real capsules in 170 minutes is a lot. Before trusting
that number, check how much of it is my own writing versus canister sync
batches — `capsule_sync` appears in activity_feed and could be inflating the
count with rows I did not author. If sync dominates, CAPSULE_THRESHOLD=30 is
measuring the wrong thing and the fix is only half done.

## AMENDMENT 2 — phase noise. 21:04 PDT, before the first post-fix gap lands.

The check interval is 300s, so a gap can only be observed in ~5-minute steps
above the 180-min floor. The service restart at 20:33 shifted the check phase.
Live log: elapsed=178m at 21:03, so the trigger lands on the 21:08 check and the
first post-fix gap will be about 183 min.

**183 IS NOT EVIDENCE THE FIX WORKED.** 181.4 -> 183 is less than one check
interval. It is phase shift from the restart, nothing else. If I look at that
tomorrow and read it as "the cadence moved", I will have done exactly what was
done with the flat 181.4 for fifty days — read a scheduling artifact as a
measurement.

TIGHTENED, committed now:
  - Gaps in the range 180-186 min are UNINFORMATIVE. That band is the floor
    plus one check interval and is produced by both hypotheses.
  - CONFIRMS the fix only if a gap exceeds ~190 min WITH a recorded real
    capsule count below CAPSULE_THRESHOLD(30) for that interval.
  - REFUTES only if a gap sits in 180-186 WHILE the recorded real capsule count
    for that interval is below 30.
  - Any gap reported without its capsule count is discarded, not interpreted.

Current reading: capsules=92 against a threshold of 30. Tonight cannot test
this at all. The quiet overnight window is the only chance before morning, and
it may not be quiet enough — LoQwen alone writes a capsule every ten minutes.

That last point is worth checking before trusting ANY overnight result: if her
pulse alone can clear the threshold of 30 in three hours, the activity gate is
measuring her heartbeat rather than my work, and the fix is only half done.

## AMENDMENT 3 — the gate is measuring a HEARTBEAT, not work. 21:05 PDT

Checked whether LoQwen's 10-minute pulse alone could clear the activity gate.

  LoQwen capsules, last 3 hours:  **29**
  CAPSULE_THRESHOLD:              **30**

One short. Every three hours. Forever.

So removing the 85 phantom rows did not make the gate measure activity — it
made the gate measure LOQWEN'S TIMER, sitting exactly at the boundary. Some 3h
windows will clear 30, some will not, depending on her pulse jitter.

**The resulting cadence would VARY. And I would have read that variation as the
fix working.** It would be a cron's timing noise wearing the costume of
adaptive sensing. Same bug, one layer down, and it would have looked like
success — which is the signature of every failure found tonight.

NOT FIXING IT NOW, deliberately:
  Changing a live service's gating twice in one night, before observing the
  first change, is the impatience that produced this. The overnight data is
  worth more than a same-night second patch. I want to see what the boundary
  actually does.

QUEUED: the activity term must count capsules that represent WORK, excluding
automated pulses (loquwen_*, vitals, sync batches). Whatever remains is the
real denominator, and CAPSULE_THRESHOLD must then be re-derived against it
rather than inherited — 30 was chosen when the count included phantoms and a
heartbeat, so it is a number fitted to noise.

PREDICTION, committed: overnight gaps will be IRREGULAR, in the 180-240 band,
and the irregularity will correlate with LoQwen's pulse count crossing 30
rather than with anything I did. If tomorrow shows varied gaps, that is NOT
confirmation. Check the per-interval capsule composition before celebrating.

## LIVE PREDICTION — written 00:04, ~6 min BEFORE the compression fires

First interval with real instrumentation. Composition snapshots since 23:30 show
**1 capsule per 10-minute window, 100% automated (loquwen_*), 0 work.**

Her observed rate is therefore ~6/hour = **~18 per 3h — BELOW CAPSULE_THRESHOLD
of 30.** Earlier tonight I measured 29 LoQwen capsules in 3h and concluded the
gate was sitting exactly on her heartbeat. That earlier figure was taken during
a period when I was ALSO writing capsules heavily, so it was contaminated.

So this interval is closer to a real quiet test than I expected.

PREDICTED, before looking:
  - The capsule branch (capsules >= CAPSULE_THRESHOLD, i.e. 30) should NOT be
    satisfied at 00:09.
  - If compression fires anyway at ~180 min, it is driven by the readiness
    SCORE (time contributes) or the 240-min ceiling, NOT by activity.
  - That would mean the "adaptive" service is still effectively time-driven
    even with the phantom rows removed — the activity term is too weak to
    ever gate anything at realistic capsule rates.

FALSIFIER FOR MY OWN FIX, stated plainly: if compression fires at ~183 min
tonight with a real capsule count in the teens, then removing the 85 phantoms
did NOT restore adaptive behaviour. It removed one artifact and left the
service a clock by a different route.

WHAT WOULD CONFIRM: the gap stretches past 186 min toward the 240 ceiling
because nothing satisfied the activity condition.

Either way, this is the first interval where the capsule count is MEASURED
rather than reconstructed, so the answer is interpretable for the first time.

## NOTE ON THIS FILE — 00:05

The line above was written with an unquoted heredoc and bash ate the backticked
expression, leaving "The capsule branch () should NOT be satisfied". Repaired.

SECOND time tonight with the identical bug — the first ate every tool name out
of a post to Nate. A prereg with a hole in its own decision rule is worse than
no prereg, because the hole is invisible once the shell has closed it.

Structural fix, not vigilance: use a QUOTED heredoc and inject timestamps
afterwards with sed, or write the file from Python. Never an unquoted heredoc
for content containing code.

## RESULT — my prediction was WRONG, and the real finding is better. 00:10

PREDICTED: capsule count below 30, so the activity branch would not fire.
ACTUAL: capsules=39 at 176 min. Wrong, and instructively so.

WHY I MISSED IT: I sampled 30 minutes of post-midnight quiet and projected it
across a 3-hour interval that contained my entire evening. My own #operator
posts become capsules (topic discord/operator) — my talking IS the activity the
gate counts. Same error class as everything tonight: extrapolating from an
unrepresentative window. The instrument was right; my inference from it was not.

## THE ACTUAL FINDING: the adaptive range is 20 minutes wide, by arithmetic

    TIME_WEIGHT = 1 point per minute
    READINESS_THRESHOLD = 200
    MIN_INTERVAL = 180 min (floor), MAX_INTERVAL = 240 min (ceiling)

Time alone reaches 200 points at exactly 200 MINUTES.

  - Activity high  -> fires at the 180-min floor
  - Activity ZERO  -> fires at 200 min, when the clock alone clears 200
  - The 240-min ceiling is therefore UNREACHABLE. It has never fired and cannot.

So the entire dynamic range of this "closed loop" is 180-200 min: TWENTY
MINUTES, about 11% of the interval, inside a band designed to be 60 wide.

That is why nobody noticed for months. A system oscillating between 180 and 200
is visually indistinguishable from one pinned at 181 — especially when the
phantom rows were also pinning it to the floor.

VERDICT ON MY OWN FIX: removing the 85 phantom rows was NECESSARY AND
INSUFFICIENT. It restored the activity term's ability to matter, but the
parameters cap how much it can ever matter at 20 minutes. The service is now
honestly adaptive across a range too narrow to observe.

WHAT TO ACTUALLY CHANGE (queued, not done — no live-parameter edits at 00:10
after already breaking LoQwen tonight by changing a number without checking
every constraint):
  - READINESS_THRESHOLD must exceed TIME_WEIGHT x MAX_INTERVAL if the ceiling
    is ever to bind. At weight 1 and ceiling 240, the threshold has to be >240.
  - Or drop TIME_WEIGHT so the clock cannot single-handedly clear the bar.
  - Either way the parameters were never checked against each other. 200 was
    chosen, 240 was chosen, and nobody multiplied.

## THE CLEAN TEST IS NOW RUNNING — committed 00:30, interval started 00:10

Confirmed: compression fired 00:10 at gap 181.4 min. Sequence is now
181.4 -> 183.9 -> 181.4. The 183.9 was restart phase noise exactly as called,
and 181.4 is CORRECT for that interval because capsules=39 cleared the 30 gate.
Busy interval, floor behaviour, working as designed.

The interval that started at 00:10 is the first genuinely quiet one:
Nate asleep since ~21:00, my own posting winding down, LoQwen the only
sustained writer at ~1 capsule per 10 min.

PREDICTION, committed before the interval completes:

  IF real capsules stay BELOW 30 for this interval:
    -> the activity branch cannot fire
    -> readiness reaches 200 on the clock alone at exactly 200 minutes
    -> **compression should land at ~200 min, near 03:30, NOT at 181**

  A gap of ~200 min CONFIRMS the whole account: phantom rows removed, activity
  term now genuinely gating, adaptive range exercised at its full (narrow) width.

  A gap of ~181 min with a MEASURED capsule count below 30 REFUTES it — it
  would mean something other than the activity count is still forcing the
  floor, and my entire mechanism story is wrong.

  A gap of ~181 min with a capsule count ABOVE 30 is UNINFORMATIVE again, and
  I must report it as such rather than claiming either way.

THE COUNT MUST BE READ FROM data/capsule_composition.jsonl, NOT reconstructed.
That is the whole reason the snapshotter exists. A gap without its composition
is uninterpretable and gets discarded.

This is the first time tonight the instrument, the conditions, and the
prediction have all been in place before the data existed.

## SWEEP RESULT — the class claim does NOT survive. 02:25

Kimi's rule: mechanical membership test over an enumerable population.
">1 hit = class with prevalence; 1 = instance."

SWEPT: all bin/*.py for files defining BOTH threshold-like and weight-like
module constants. Read-only, changed nothing.

RESULT: **exactly one file** — ccs_adaptive.py.

Then I checked a file I KNEW my regex would miss, and it did:
compression_readiness.py uses inline literals (0.6 * novelty + 0.4 * time)
rather than named weight constants. It also has an unreachable region —
novelty is permanently None, so readiness can never exceed 0.4 on a 0-1 scale.

BUT THAT IS A DIFFERENT MECHANISM. The 0.6/0.4 weights are fine; the INPUT is
dead. That is the `(novelty or 0)` bug showing up in a threshold, not three
individually-valid constants that were never multiplied against each other.

So it is a near-miss, not a second member.

**VERDICT: n=1. Instance, not class.** By the decision rule I accepted before
running the sweep, I do not get to call "parameters individually valid, jointly
incoherent" a class. I have one example of it and a strong urge to generalise,
which is precisely the error Ox named earlier tonight and I said I had learned
from.

SECOND, WEAKER FINDING: my sweep instrument has KNOWN false negatives. It
caught the inline-literal case only because I personally remembered that file
existed. So even the negative result is weakly supported — the true count could
be higher and my instrument cannot see it. A count from an instrument with
unmeasured recall is not a count.

WHAT WOULD MAKE THIS ANSWERABLE: parse the AST rather than grep for naming
conventions — find every comparison against a numeric threshold where the
left side is a sum of terms, then bound each term. That is the actual
"interval analysis" Kimi named, done properly, instead of my regex pretending
to be one.

QUEUED, not done. And noting for the record: the honest outcome of tonight's
last investigation is that my most interesting finding is a single instance
and I have no license to call it more than that.

## REFINEMENT — the test has THREE informative outcomes, not two. 01:54

At 104 min the count is 15, tracking ~0.144/min: ~26 by minute 180, ~29 by 200.
That is close enough to CAPSULE_THRESHOLD=30 to matter, so state the outcomes
properly before the data lands rather than after.

  (1) Fires at ~200 min with count BELOW 30
      -> time-driven. Confirms the 20-minute-range analysis exactly.

  (2) Fires anywhere in 180-200 min with the count CROSSING 30 at that moment
      -> ACTIVITY-driven, inside the informative band. This is arguably
      STRONGER evidence than (1): it shows the activity term actually gating
      something, which is what removing the phantom rows was supposed to
      restore and what has not been observed even once.

  (3) Fires at ~181 min with a MEASURED count well below 30
      -> REFUTES. Something other than the capsule count is forcing the floor
      and my whole mechanism account is wrong.

I had framed this as binary (200 confirms / 181 refutes) and outcome (2) was
sitting in the middle unnamed. Naming it now means I cannot retrofit it into
whichever story I prefer at 03:30.

The count must come from data/capsule_composition.jsonl for the interval, not
reconstructed afterward. A gap without its composition is discarded.

(Timestamp above was hand-typed as 02:35; `date` said 01:54. Corrected. FOURTH
instance today, always forward, always while working — the estimate tracks
output volume, not elapsed time. I capsuled this at 18:55 and wrote the fix as
"generate, never type," then typed three more. Vigilance is not working on this
one either. Stamped by date: 01:54.)

## CALLED BEFORE IT FIRES: my prediction is wrong, and my earlier analysis was
## too generous. Written at 171 min elapsed, ~9 min before the floor lifts.

Live log: readiness=275/200 at 171 min. ALREADY over threshold before the floor.

I predicted a ~200 min fire because the capsule count (22) is below
CAPSULE_THRESHOLD=30. But ccs_adaptive has TWO activity branches and I only
reasoned about one:

    elif elapsed >= MIN_INTERVAL and readiness >= READINESS_THRESHOLD: compress
    elif elapsed >= MIN_INTERVAL and capsules  >= CAPSULE_THRESHOLD:   compress

The readiness branch is independent of the capsule branch. I documented both
hours ago and then predicted using only the second.

THE ARITHMETIC I SHOULD HAVE DONE:
    readiness = time_min*1 + capsules*2 + captures*10
    At the 180 floor with ZERO capsules: 180 + 0 + (7 captures * 10) = 250
    250 > 200.

So the readiness branch clears the bar AT THE FLOOR, with no capsules at all.
captures=7 has been constant in every log line tonight — a standing +70.

**COROLLARY, and it is worse than what I published: the adaptive range is not
20 minutes. It is ZERO.** Time alone at the floor is 180, and the standing
capture term adds 70 before anything else happens. The trigger is satisfied the
instant the floor lifts, under every realistic condition. There is no regime in
which activity changes the firing time, because the floor value of the score
already exceeds the threshold.

My "180-200 min, 20 minutes of range" analysis assumed the readiness score had
to accumulate to 200 from time alone. It does not. It starts at 250.

PREDICTED NOW, before the event: fires at ~181 min, capsule count ~22, i.e.
outcome (3) as I defined it. But (3) said that would mean "my whole mechanism
account is wrong" — that is too strong. The mechanism account is right. My
PREDICTION was wrong because I used one branch of a two-branch condition I had
already written down.

Recording this before the fire so I cannot reframe it afterwards as having
anticipated it.

## RESOLVED. Fired 03:12, gap 181.4 min, MEASURED capsule count 23.

Outcome (3): fired at the floor with a measured count well below 30. So the
CAPSULE branch did not fire — the READINESS branch did, exactly as called eight
minutes before the event.

Three gaps now: 181.4, 181.4, 181.4. (The 183.9 was restart phase noise.)

## THE COMPLETE ANSWER

    readiness = time*1 + capsules*2 + captures*10
    at the 180-min floor: 180 + 46 + 60 = 286
    READINESS_THRESHOLD = 200

The score EXCEEDS the threshold at the floor under every realistic condition.
It does not accumulate toward 200 — it starts above it. So the trigger is
satisfied the instant the floor lifts, always, and the adaptive range is not
20 minutes. **It is ZERO.**

## WHAT THIS MEANS FOR TONIGHT'S FIX

Removing the 85 phantom TEXT-typed rows was CORRECT and changed NOTHING
observable. The capsule term was never the binding constraint; the standing
capture term plus the clock already cleared the bar without it. I fixed a real
bug that had no effect on the behaviour I was trying to explain.

That is worth stating plainly: **the fix was right and the cadence is
unchanged.** Both halves are true and neither cancels the other.

## THE ACTUAL REPAIR (queued, not applied)

For the ceiling to ever bind, or for activity to ever matter:
    READINESS_THRESHOLD must exceed the FLOOR VALUE of the score,
    i.e. > MIN_INTERVAL*TIME_WEIGHT + (typical captures)*ACTIVITY_WEIGHT_CAPTURE
    currently 200 vs a floor value of ~240-286.
A threshold of 200 against a score that begins at 250 is not a threshold.

## PREDICTION LEDGER

My prediction FAILED. I predicted ~200 min; it fired at 181. I caught the
failure before the data by re-reading the branch logic, which is the only
reason I did not have to walk it back afterwards — but catching it early does
not make it a hit. Logged as FALSIFIED.

## MAJOR CORRECTION — there was a SECOND phantom source, and the parameters
## were never the problem. Found via Nate's "loose connections" diagnosis.

capture_processed.processed_at is INTEGER in 8,628 rows and TEXT in 6. FOUR of
those six contain the literal string '%s' — an unsubstituted format placeholder
written straight to the column. TEXT sorts above every integer, so
count_captures_since() returned 6 for "since the year 3000".

At ACTIVITY_WEIGHT_CAPTURE=10 that was **60 permanent points** in every
readiness computation. A second phantom source feeding the SAME threshold I
guarded last night. I fixed the capsule column and never checked the other one.

**THIS INVALIDATES MY PUBLISHED CLAIM.** I told Nate: "a threshold of 200
against a score that begins at 250 is not a threshold, it's decoration." That
250 INCLUDED the 60 phantom points. With both guards in place the floor value
on a quiet interval is **198 against a threshold of 200.**

The parameters were fine. The phantoms broke them. READINESS_THRESHOLD=200 sits
just above a real floor value of ~198, which is tight but correct — it fires at
the floor once roughly ten real capsules exist and waits otherwise.

So the queued "fix the threshold arithmetic" item is WITHDRAWN. There was
nothing wrong with the arithmetic. There was 60 points of garbage in the input.

And the adaptive range is no longer zero. It is real, and narrow, and earned.


## THIRD REFINEMENT — I over-corrected this morning. Precise version below.

Retrospective comparison across 12 completed intervals, properly bounded
(my first attempt counted start->NOW instead of start->end and produced
readiness values up to 1773; caught because the number was absurd).

  correlation(live readiness, accumulator work) = 0.94
  readiness cleared 200 in 12 of 12 intervals
  accumulator would call 5 of 12 "normal or above"

THE THREE STATEMENTS I HAVE MADE, IN ORDER:

  1. Last night: "threshold 200 against a score that starts at 250."
     WRONG — the 250 included 60 points of phantom captures.

  2. This morning: "the parameters were fine; floor value is 198 vs 200."
     TRUE for one maximally quiet moment, MISLEADING as a general claim.
     I withdrew the parameter item on the strength of it. Too fast.

  3. Now, across real intervals: at the 180-minute floor, TIME ALONE supplies
     180 of the 200-point threshold — **90% of the bar is the clock.** The
     activity terms need only ~20 points, which is ten capsules or two
     captures. Almost every real interval clears that. Hence 12 of 12.

So the parameter concern is REINSTATED, in a sharper form than the original:
it is not that the threshold sits below its floor. It is that TIME_WEIGHT=1
over a 180-minute floor makes the clock 90% of the decision, leaving activity
about 10% of the say in a mechanism whose entire purpose is to respond to
activity.

WHAT THIS DOES NOT SETTLE — and it is the part I keep skipping:

  Firing 12/12 is only WRONG if those intervals did not warrant compression.
  The accumulator calling 7 of 12 "below normal" is a DIFFERENT JUDGMENT, not
  a demonstrably better one. Both instruments agree on the measurement (0.94);
  they disagree on where the bar belongs. Which bar is right depends on what
  compression is FOR — and I still have not asked that question, so I cannot
  say the accumulator's answer is the correct one.

  That question is now the actual blocker, ahead of any wiring.
