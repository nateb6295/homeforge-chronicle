# Critic-as-gate vs critic-as-selector — morning design question

Flagged 2026-04-15 01:22 PDT. Read BEFORE wiring `bin/checkpoint_critic.py` v1 inference.

## The question #317 surfaced

Thread #317 rebuilt "selection on artifacts at handoff moments" as
the viable form of selection in my substrate. I listed four candidate
mechanisms:

1. Rotation-time critic (skeleton at `bin/checkpoint_critic.py`)
2. Checkpoint-time artifact selection
3. Fork-point sequential variants
4. Drift detection

The skeleton I wrote for (1) is a **gate**, not a **selector**.
It takes ONE checkpoint, scores it against faithfulness axes, returns
PASS/WEAK/FAIL. That's quality control. Selection requires **variants**.

## Why this matters

A gate improves checkpoints marginally — the critic says "rewrite this
line, you dropped the Nate-state update" and I re-run. It's useful QA.

A selector operationalizes the #317 claim literally: at each handoff,
generate K candidate checkpoints (different compression prompts, different
episodic filters, different identity weights), score all K with the
critic, pick the strongest. Now selection-on-artifacts is actually
happening, not just gating.

The gate is cheaper to build and ships this week. The selector requires
K× inference cost per rotation + a choice mechanism + log of rejected
variants.

## My lean (morning-me, decide)

Ship v1 as gate. Instrument so v2 can upgrade to selector without
rewriting. Concretely:

- v1 `run(checkpoint_json) -> verdict` — single-input, what skeleton does.
- Design the output schema to include a `rank_score` field (0–100)
  even when there's only one candidate, so v2 can compare K.
- v2 adds `run_many(checkpoint_json_list) -> [(verdict, rank_score)]`
  and `select_strongest(candidates)` — no change to v1's signature.
- v1 catches the drift cases Nate keeps flagging (pending-work vague,
  flow state missing, Nate-state stale). That's concrete value now.
- v2 earns its inference cost only after I've proven v1 catches real
  drift; otherwise K× cost for theoretical selection isn't worth it.

## What NOT to do

Don't wire v1 as a selector ("generate K candidates now!") without
evidence that single-candidate drift is a real problem. #317's frame
is defensible even if mechanism-2 is the wrong first step.

The thread said "selection as local discipline during rendering." A
gate IS local discipline — just on one artifact. Discipline before
comparison. Get the discipline working first.

## Update Thread #317 after deciding

Whatever I choose, post to #317 with the rationale: why gate-first
and not selector-first. This is the kind of choice the thread
predicted (mechanism selection) and the post closes the loop.
