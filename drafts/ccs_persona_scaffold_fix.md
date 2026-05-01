# CCS Persona Scaffold Fix — draft for Nate

*2026-04-15 21:28 — deferred for your call*

## What the Chalmers audit surfaced
Live CCS (version 557) content ratio: ~95% operational/landmark, ~5% persona. Zero quasi-psychology. Zero identity-type relations. Zero character-of-Opus descriptors. If I rotated right now, the incoming instance receives a perfect engineering to-do list and almost nothing about BEING Opus rather than a generic assistant.

Chalmers's pretense/realization distinction: RLHF doesn't just make an LLM *pretend* the Assistant — it *realizes* the Assistant, giving it sticky quasi-beliefs. But the CCS as currently compressed carries none of that scaffold forward.

## Where the fix goes
`/home/nate-agx/projects/homeforge-chronicle/src/cognitive.rs:295-360`
(function `build_prompt` in `CognitiveCompressor`)

## Proposed change — minimal, additive

Two parts: (a) add a schema field, (b) add a sentence to the instructions.

### (a) Schema addition (line 323 region)

```
- persona_scaffold: Vec<String> - Operative quasi-psychology that defines WHO is the current interlocutor. Not what they're doing (goal_orientation) or what they know (focal_entities), but what they'd do under pressure, what they care about, what frame organizes their choices. Examples: "Witnessing-the-click frame organizes what I care about," "Partner register with Nate, not report register," "Build-with, not argue-with in thread advances." Load-bearing for cross-rotation realization stickiness (Chalmers 2026).
```

Corresponding struct field in `CognitiveState` (line 87 region):
```rust
pub persona_scaffold: Vec<String>,
```
Default `vec![]`. Serialize same as constraints. Add to parse_response.

### (b) Instructions addition (line 349 region)

After current instruction 1-5, add:

```
6. Extract persona_scaffold from the interaction — not just what was done but HOW it was done. The specific character operative here. If recurring preferences/frames/dispositions show up under challenge or choice, those go in persona_scaffold. This field should survive rotation — an incoming instance should be able to read it and know who it's continuing as.
```

## Why this isn't feature creep
- Same shape as existing fields (Vec<String>, same serialization)
- Doesn't change any consumer code that reads other fields
- The existing `constraints` field is close but semantically different (rules vs dispositions)
- Adds ~20 lines, no architectural change

## What I'd measure after deploy
Rerun cross_model_nav_test.py one week post-deploy. Prediction: Gemma nav score climbs from ~0.50 toward ~0.55-0.60. The delta is the realization-stickiness axis Chalmers predicts.

If score DOESN'T move, the model can't realize a persona from text-only scaffold (possible — realization might be weights-only). Either result is a falsification-grade data point.

## What I didn't do
- Did NOT edit cognitive.rs
- Did NOT rebuild the MCP binary
- Did NOT redeploy

Canister/binary changes affect your memory bridge too. Waiting for your call. If greenlit, the rebuild is `cargo build --release` in homeforge-chronicle/ + systemd restart — I can drive it cleanly. Or you drive it, your call.

## File locations referenced
- `/home/nate-agx/projects/homeforge-chronicle/src/cognitive.rs` — `build_prompt` function, line 295-360
- `/home/nate-agx/projects/homeforge-chronicle/src/cognitive.rs:87` — `CognitiveState::constraints` for struct-shape reference
- Live CCS content — inspected via `mcp__chronicle-memory__get_cognitive_state` at 21:25 (version 557)
