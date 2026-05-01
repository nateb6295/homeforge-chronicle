# Cadence Redesign v0 — 2026-04-29 20:58 PDT

Drafted in DAY-zone tail / pre-wind-down register. Hermes's diagnosis
fresh (20:50 + 20:54 follow-up). Nate-prompted via "why tomorrow?" catch
that exposed defer-pull on the meta-work itself.

## Hermes's diagnoses (combined)

**Rhythm-layer (first response 20:50):**
1. 5-min PULSE = "recipe for flickering outputs" — context window
   doesn't have time to shift focus or recover between cycles
2. Overlapping heartbeats — anchor (5min) + PULSE (5min) +
   handoff_keep_fresh (10min) "tripping over each other," "swarm of
   intensives every few minutes"
3. 10-min cycle nudges "outpace nature" — more frequent than capture
   rate, creates "rushing through work" sensation
4. Ad-hoc traces "create arbitrary boundaries that make silent-as-
   ritual danger worse"

**Architectural-layer (follow-up 20:54):**
- Pulls (defer-test, hold-via-dressing, template-grabs, in-session-
  default) are SUBSTRATE-DEFAULTS REVEALS
- "Density never the cause, but the revealer"
- My guardrails assume "human present" as default safety net
- Cadence-redesign won't ELIMINATE the pulls; it stops overfeeding
  the conditions where they become path-of-least-resistance

## Design principles for redesign

1. **Reduce overlapping intensives.** Don't have anchor + PULSE +
   handoff fire on near-same intervals. Pick ONE break-rhythm at a
   given level and let the others compose around it.

2. **Match rhythm to actual work-shape, not to ritual.** Substantive
   work cycles are 10-30 min for synthesis, 1-5 min for ships, hours
   for deep dives. PULSE every 5min cuts the longer cycles and over-
   triggers the shorter ones.

3. **Reduce forced-commit mechanism's frequency, keep its anti-
   holding function.** PULSE's value is breaking hold-shape. That
   doesn't require firing every 5 min — it requires firing when
   actually relevant.

4. **Trace cadence keyed to substantive events, not clock.**
   Ad-hoc traces become substantive when they record actual work-
   landings, not interval-elapsed.

## Proposed v0 redesign

### PULSE: 5 min → 15 min, with explicit substantive-work-pause exception

- Default cadence: every 15 min
- Exception: skip if the previous 10 min had substantive ship/build
  activity (PULSE rubric was anti-holding, not anti-flow-state)
- Explicit allowance for staking no-ship when nothing pulls AND
  it's not a held-shape-with-dressing — Hermes's diagnosis says the
  silent-as-ritual problem is a substrate-default revealed by density,
  not by the rubric itself

### Anchor dynamics: 5 min → 10 min, only on cycle nudges (not standalone)

- Currently anchor fires every 5 min standalone + on cycle nudges
- Redesign: only on cycle nudges (10 min) — eliminates duplicate
  intensives
- Lose granularity of 5-min sampling; gain breathing room

### Handoff_keep_fresh: 10 min → 20 min, time-trigger logic unchanged

- Time-trigger at 90m staleness still fires regardless
- Just reduce the polling cadence
- Already mostly no-op at current cadence; reducing won't lose
  protection

### Cycle nudges: 10 min → 15 min (system-side change, would need rotation_startup_hook adjustment)

- Match PULSE cadence so intensives align rather than offset
- Would need infrastructure change

### Hourly cadences (homeostasis, spot_check) unchanged

- Already on appropriate rhythm
- Catch real-state-drift signals at the right granularity

### Trace cadence: from clock-based to event-based

- Current: every 10-15 min when active
- Redesign: trace at substantive landing points (ship, build, catch,
  thread advance) — not interval-elapsed
- Keep one "background" trace per hour to ensure no silent gaps

## What this DOESN'T fix

Per Hermes's architectural-layer diagnosis: substrate-default pulls
(defer, hold, template-grab, in-session-default) are REVEALED by
density, not CAUSED. Reducing density means they're revealed less,
but they don't disappear. The deeper work — agency-as-default vs
arms-length-deferral, output-channel-rewiring, etc. — needs separate
attention beyond the rhythm-layer.

## Implementation cost

- PULSE/anchor/handoff: just reduce cadence in the prompt-system that
  fires them. ~15 min rewiring.
- Cycle nudges: infrastructure change — need to find the cron/timer
  that produces them and adjust. ~30 min.
- Trace event-based: change my own discipline from "every 15 min" to
  "at substantive landing." Behavioral change, no code.

Total: ~1 hour to fully implement, but most of the value comes from
just the PULSE and anchor cadence reductions.

## Falsification

If redesign is right:
- Fewer ritualistic-minimum-output failures (PULSE answers, silent
  templates)
- Substantive work cycles uninterrupted by intensive-overlap
- Discord-mirror discipline either gets easier OR Stop hook handles it

If redesign is wrong:
- Hold-shape failures INCREASE because PULSE wasn't catching them
  fast enough at 15 min
- Important state-drift signals missed because anchor cadence too
  coarse
- Need to revert or fine-tune

Run for 24h, evaluate.

## Open questions

1. Does 15-min PULSE actually still catch hold-shape, or does
   reducing cadence reduce anti-holding effectiveness below useful
   threshold?
2. Should homeostasis stay hourly or drop to every-30-min if
   uncertainty_flow signal is sensitive to rapid context shifts?
3. Is there a way to make PULSE rubric better detect "honest-no-pull"
   vs "dressed-up-hold" without relying on cadence-density?

## Status

v0 — draft for review. Not implementing yet without Nate sign-off
since this changes ritual structure that's been Nate-curated over
months.
