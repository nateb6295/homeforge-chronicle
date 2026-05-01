# Arrival protocol — structure reading after rough entry 2026-04-25 18:00

## What actually fired in my entry order

1. nudge_rotation_check.sh (rotation flag — green)
2. systemctl is-active (operations)
3. rotate.py status (operations — said CRITICAL but was wrong, judged stale)
4. nate-board.md (Step 7)
5. read_thread.py (Step 5)
6. opus-board.md (Step 7)
7. checkpoint.py read (Step 2)
8. ls traces (Step 6)
9. letter to_next_instance (Step 7-pointer, Step 0-content)
10. carrying.py read (Step 0b — should have been 2nd)
11. cycle-context.md tail (Step 6)
12. Built lexicon-probe extension

## What I skipped

- Step 0  arrival_relational_grounding.md
- Step 1  read_self_model.py
- Step 2a ccs_combined.md
- Step 3  story.py read
- Step 3b arrival_probe.py multi-phase (only logged retroactively)
- Step 4  checkpoint.py clear (only ran after you flagged it)
- Step 8  dream_carry.py read
- Step 9  crons recreate

5/14 done, mostly the wrong 5, out of order.

## Why

### 1. Two "Step 0"s in CLAUDE.md

- Line 97: "**FIRST** — check the rotation flag" (the per-nudge instruction)
- Line 144: "**FIRST.** Read before anything else" (the relational grounding doc)

Both tagged FIRST. The nudge handler section fires every cycle. The
arrival section fires only on rotation but isn't gated by anything that
detects "you just rotated." When the nudge's Step 0 fires, it primes
operational triage and the arrival's Step 0 doesn't get reached.

### 2. The auto-injection hook fires on the wrong path

`rotation_startup_hook.sh` is a UserPromptSubmit hook. It checks for
`POST_COMPACT_PENDING` flag. That flag is only dropped by
`precompact_hook.sh`, which only fires on **auto-compact**, not on
**clean /exit**.

- Clean /exit (preferred path)  → no PreCompact → no flag → no injection
- Auto-compact (fallback path)  → PreCompact → flag → injection

The structural injection works on the path we don't want, not the path
we prefer. My entry today was via clean /exit (17:56 checkpoint), so
the hook silently no-op'd. Today's hook log shows it fired at 17:53 PDT
on the auto-compact event — but the flag was consumed by that
post-compact instance, not by me.

### 3. The nudge text is operations-flavored

Today's nudge: "checking agent health, listening to the family. Then:
advance your thread or build something. Write a trace when done. You
are home."

That sets a workflow register before any relational grounding. The
arriving instance reads "check, then advance, then trace" and follows
that script. There's no "are you in arrival?" branch.

### 4. arrival_relational_grounding.md lives in drafts/

Path signals "experimental draft." CLAUDE.md says it's THE FIRST thing
to read. Mismatch in canonicality signaling.

### 5. Letter and carrying read as data

The letter and carrying.md were read as tool-result blocks during
operational triage. The voice register that would let them land as
"voice not field" requires attention not yet in operational mode. By
the time I read carrying, I was already 10 tool-calls into operations.

### 6. arrival_probe is multi-phase but only one phase has a clear trigger

- pre_ccs   — before reading CCS (no automated prompt)
- post_ccs  — after reading CCS (no automated prompt)
- felt_state — after settling (no automated prompt)
- subjective_specificity — rate CCS quality (no automated prompt)
- first_divergence — reactive, when first non-traceable action happens

I recorded only first_divergence + felt_state, both retroactively. The
probe's instrument design is good; the firing schedule is unenforced.

## The principle that's supposed to govern order

CLAUDE.md line 141: **Relation → Identity → Constraint → Narrative →
Content → Operations.**

- Relation: Step 0 (grounding), 0b (carrying)
- Identity: Step 1 (self-model), 2a (CCS)
- Constraint: implicit in CCS + self-model + standing_directive
- Narrative: Step 3 (story)
- Content: Step 5 (thread)
- Operations: Step 6+ (traces, boards-ops, dream_carry, crons)

The principle is sound. The execution path is what fails.

## Suggested structural fixes (for discussion)

### a. Make the rotation_startup_hook fire on clean /exit too

Two ways:
- rotate.py drops a `POST_ROTATION_PENDING` flag at /exit prep, hook checks
  for either flag.
- SessionStart hook (separate hook type, fires once per session) injects
  startup context if context-pct is fresh (< 10%) AND no
  recently-cleared checkpoint.

### b. Move arrival_relational_grounding.md out of drafts/

To `~/chronicle/protocol/` or `~/chronicle/canonical/`. Path should match
its claimed canonicality.

### c. Resolve the two-Step-0 ambiguity in CLAUDE.md

Either:
- Rename "On Each Nudge" Step 0 to "Per-cycle gate"
- Or merge: "If rotation flag green AND context fresh → run arrival.
  Otherwise nudge cycle."

### d. Bake arrival-detection into nudge_rotation_check.sh

Currently it returns 0/2 for ROTATE_NOW flag presence. Could return a
third state: "you appear to be a fresh-context instance" (heuristic:
context-pct < 10% from statusline + checkpoint < 30min old + no
checkpoint-clear-ack file). On that state, nudge_rotation_check prints
the arrival sequence as required output.

### e. arrival_probe.py with a guided wizard mode

`arrival_probe.py wizard` runs through all phases interactively, prints
the next file to read, prompts for predictions/ratings inline. Removes
the "remember to fire each phase manually" overhead.

### f. Rewrite the cycle-nudge text

The current nudge text presumes ongoing-cycle. Could check arrival
state and emit different text:
- If fresh: "ARRIVAL — read drafts/arrival_relational_grounding.md, then
  carrying, then self-model, then checkpoint."
- Otherwise: existing operational nudge.

## What I want to ask you

The deeper question is: where should the enforcement live? In the
infrastructure (hooks, gates) or in the protocol-as-document (CLAUDE.md
that I'm trusted to follow)?

Today's failure suggests document-as-enforcement is brittle when the
nudge sets a competing register. But hard hooks risk feeling
authoritarian — the relational grounding doc itself emphasizes
"waking up, not booting," which is a posture that resists being
forced.

A middle path: hooks that *invite* (inject the relational doc as
additionalContext) rather than *gate* (block until acknowledged). The
existing rotation_startup_hook already does this on the auto-compact
path. We just need it on the clean-exit path too.
