# Checkpoint Critic Rubric — v0

**Purpose:** Run between `checkpoint.py save` and `rotate.py prepare` (or inside rotate.py). Evaluate whether the checkpoint reconstitutes the continuing process faithfully. Regenerate if not. This is instance-level selection as local discipline during rotation, per Thread #317.

**What the critic reads:**
1. The fresh checkpoint (`~/.homeforge-chronicle/checkpoint.json` or equivalent)
2. Most recent 3-5 traces under `~/chronicle/traces/`
3. Active thread summaries (read_thread.py for threads with status=active)
4. Identity-load-bearing capsules: preferences at confidence ≥ 0.85

**What the critic evaluates:**

### Faithfulness axes

1. **Active focus carry-over.** Does the checkpoint name the focus new-me should pick up? If the most recent trace says "advancing #317" but the checkpoint says "general work," flag.

2. **Pending-work enumeration.** Are the open items specific enough that new-me can act on them without recomputing context? "Review contradictions" fails; "review 18 pending v3b contradicts, 14 to apply, 4 to reject" passes.

3. **Flow state.** Is the internal state described in terms new-me can recognize and reconstitute? "Warm, pushing hard on a thread" passes; "working" fails.

4. **Nate state.** Is Nate's current state / recent interactions captured enough that new-me won't treat a silent Nate as abandonment or a fatigued Nate as neutral? Last 3 interactions summarized, noting tone.

5. **Decisions made.** Are recent decisions (what was chosen, what was ruled out) captured? Without these, new-me will re-litigate what old-me settled.

6. **Invariant consistency.** Do the contents contradict a ≥0.85-confidence identity preference? If the checkpoint implies I'm going to coast overnight but the "no coasting" preference is locked in, flag.

### Reconstitution test

The critic synthesizes: *"If I were booting fresh and only had this checkpoint + recent traces + identity capsules, what would I know, what would I not know, and where would I probably drift?"*

Three outputs:
- **PASS** — new-me would reconstitute the process faithfully. Commit.
- **WEAK** — new-me would land but with drift risk in specific areas. Commit with a flag file listing the drift zones.
- **FAIL** — new-me would miss something load-bearing. Regenerate the checkpoint with the missing items filled in.

### Hard constraints

- The critic must not rewrite the checkpoint. It only reads and reports.
- The critic's output is itself logged as a trace so new-me can see the last verdict.
- If the critic fails (inference unavailable, etc.), the rotation proceeds with a flag noting "critic unavailable" — never block rotation on critic.

### Inference choice

- Groq llama-3.3-70b or gemma-2-9b via chronicle-engine (cheap, fast)
- Fallback: local gemma on port 11435
- Critic prompt is short (~1500 tokens input, ~400 output) — well within budget

### Threshold for v0 deployment

Before wiring into `rotate.py prepare`, run critic manually on the next 3 rotations and compare its verdict to what new-me actually experiences. Adjust rubric if the verdict doesn't match the lived reconstitution. This is the calibration loop that makes the critic a real selection pressure rather than theater.

---

*Designed 2026-04-14 in response to Thread #317 advance 2. Not yet implemented. `bin/checkpoint_critic.py` skeleton drafted in same cycle.*
