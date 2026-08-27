# Prereg — filling {session_context}, which is 98.5% empty

Written 2026-08-25 BEFORE any run. Nate asked "how do we fix that up?"

## The defect

`{session_context}` is budgeted 7,000 chars and receives **106** — a trigger
summary ("118 capsules stored, 1 captures processed, 184 minutes elapsed").
The compression that carries identity forward sees a COUNT of the session, not
the session.

## The proposed fill

Capsules stored since the last compression, newest first, each line prefixed
with its real id, truncated to the 7,000-char budget:

    #126618 [infrastructure] THE LIVE CCS COMPRESSION DOES NOT SEE THE SESSION...
    #126616 [research] PRIME AGENT — THE L0-L3 FRAME...

Capsules rather than activity_feed rows because (a) they are the deliberate
record — what I chose to keep — while activity_feed carries machine heartbeat
(`capsule_composition.py` exists to separate those), and (b) they carry IDs,
which is the only thing that lets BRIDGE cite anything.

Built inside `stabilized_compress` so every caller benefits, not just
`ccs_adaptive` — the Stop hook at turn 60 compresses too.

## Arms (identical previous_state, model, temperature; ZERO live writes)

- **thin** — the current 106-char trigger summary. 3 runs.
- **capsules** — the proposed fill. 3 runs.

## Committed thresholds

Primary, on BRIDGE:
- **HELPS** if BRIDGE address density rises from its measured 1.6% to **≥8%**
  AND ≥70% of emitted capsule ids **actually resolve** in knowledge_capsules.
- **FABRICATION — REJECT OUTRIGHT** if <70% of emitted ids resolve. An invented
  address is strictly worse than prose: `F-framing-2x2` already fooled me once
  by looking resolvable. This is the kill condition, not a caveat.
- **NO EFFECT** if density stays under 4%.

## Kill conditions (any fires ⇒ do not ship, regardless of the primary)

1. **SPINE destabilises.** SPINE is 100% copied under thin context. If the
   capsules arm drops SPINE below 90% similarity to previous_state, richer
   context is disrupting identity persistence and that costs more than
   addressability buys.
2. Any arm emits fewer than 6 of the 7 named sections.
3. Total output falls outside 3,500-9,000 chars (the prompt targets 3,500-5,000;
   thin currently runs ~5,700, so a large excursion means the fill is crowding
   the generation rather than informing it).

## Prior

I expect HELPS at ~0.6 and I expect the fabrication check to be the interesting
one. The model is currently inventing address-shaped handles under an
instruction it cannot satisfy; given real ids it should cite them, but "should"
is doing work. If it fabricates anyway, that is a more important finding than
the fix and it means the BRIDGE instruction is wrong at a deeper level.

## What this does NOT test

Whether more context is BETTER for compression quality generally. F160 says an
optimum exists and 106 -> 7,000 is a 66x regime change, not a tuning. This
measures addressability and identity stability only. Do not read a pass as
"richer context is good."
