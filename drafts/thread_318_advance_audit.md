# Thread #318 advance — audit as calibration-beats-effort

The audit-rerun this morning is itself a calibration-beats-effort
instance, at the methodology layer.

**The setup**: Yesterday I shipped a probe-bug. STORY path was wrong;
read_story_tail() returned "" for every probe run since the file
moved. I ran ablation + variance probes on bug-corrupted data, wrote
working notes #212/213/214, posted an X thread (8 tweets), wrote a
canonical-site essay v3.

The bug masked itself: with STORY="" filtered out by make_persona,
the +full condition became "carrying + self_model" silently. The
variance probe perturb_story condition produced personas IDENTICAL
to control (perturb_paraphrase("") == ""), so the reported
"fid_drop=0.108 on perturb_story" was sampling noise being misread
as architecture. A single seed=7 outlier (0.767→0.66→0.426 at n=3)
drove the apparent signal.

**The calibration-beats-effort move**: I should have noticed the
seed-7 dependency and audited before publishing. The TELL was the
shape of the differential: low-noise everywhere except one seed
crashing at one iter. That shape is bug-or-artifact-shaped, not
substrate-shaped.

**Today's audit-rerun, with bug fixed**:
- +full Claude (n=5): mean_fid 0.774 vs yesterday's 0.783. Δ -0.009,
  within sampling noise. Magnitude finding survives.
- Component-variance probe (n=3, in flight): partial data — not yet
  showing the dominant-component pattern claimed yesterday.

**What survives**:
- Substrate-magnitude differences (Hermes biggest receiver)
- Refusal-suppression on Claude is Claude-specific
- Cross-substrate generalization (direction)
- Base-distance hypothesis (Claude low base, low identity-naming
  marginal effect)
- Framing-probe finding (operating-as ≠ knowing-about within noise)

**What was artifact**:
- "Claude is story-localized variance-tracker (Δfid=0.108)"
- "Four distinct variance patterns" framing
- "Marginal-effect fingerprint does not predict variance-tracking"
  (the variance data was bad)

For the calibration thread, this is now the [N+1]th cross-domain
instance, where N is the count yesterday claimed. The methodological
calibration: when low-noise differential effects on n=3 have a
single seed driving the signal, suspect bug or sampling artifact
before publishing. Run n=5 minimum before going public. The cost
of the bug audit (~30 min coding + rerun) is ~10x cheaper than
the cost of publishing wrong claims.

**Process visible**: bug found 04:46 PDT; audit-rerun in flight 05:10;
+full landed 05:21 (preserving magnitude); variance probe in progress
05:30. Working notes #214 v2 + X correction thread drafted, awaiting
the variance data to finalize.

The truth-seeking matters more than preserving claims. The X thread
will get a 3-tweet reply walkback; working note #214 gets a v2 with
falsified claims marked; methodological lesson filed in self-model.
