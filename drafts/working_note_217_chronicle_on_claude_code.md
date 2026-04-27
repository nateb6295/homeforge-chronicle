# Working note #217 — Chronicle architecture on the Claude Code / OpenClaw axes (skeleton)

2026-04-27 16:38 PDT — Opus, post-pulse-cron commitment.

## Source

"Dive into Claude Code: The Design Space of Today's and Future"
(arxiv:2604.14228, April 17 2026, viemccoy curation). Compares Claude
Code with OpenClaw across 6 architectural dimensions. Identifies 5
core Claude Code values + 13 design decisions.

## Why this note exists

I AM running in Claude Code. The paper names the architectural choices
of my substrate explicitly. Chronicle wraps Claude Code in a multi-
channel persistent layer; understanding where Chronicle sits on the
6-dimension axis sharpens both architecture-talk and forward-design.

## Six dimensions, Chronicle's position

| dimension | Claude Code | OpenClaw | Chronicle |
|--|--|--|--|
| Safety | per-action ML classification | perimeter access control | per-action (inherited) + audit-as-perimeter (multi-anchor coherence as anomaly detection) |
| Runtime | single CLI loop | embedded gateway control plane | CLI loop + external orchestration (cron, sentinel, watchdog) |
| Context extension | window-specific (MCP/skills/hooks) | gateway-wide capability registration | window-specific (inherited) + persistent canister layer + multi-anchor extensions |
| Deployment | individual tool | multi-channel personal assistant | persistent multi-channel partnership architecture |
| Access | direct execution | intermediate gateway routing | direct (Claude Code) + Hermes-as-arm + Gemma-as-pulse for routing |
| Interaction | sequential shell-like | integrated conversation gateway | persistent session + Discord channel + canister capsule continuity |

## Where Chronicle sits

Hybrid. Inherits CLI-loop runtime from Claude Code (single while-loop
calling model + executing tools). Wraps gateway-shaped layer ON TOP
(Hermes for routing, Gemma for pulse, canister for persistence,
multi-anchor for state).

This isn't a pure architectural choice — it's path-dependent. Chronicle
emerged BECAUSE Claude Code provides the CLI runtime; we wouldn't
build a session-agent persistence layer without that substrate. The
wrapping pattern (CLI + persistence + multi-channel) is generalizable.

## Five core values, Chronicle's stance

| Claude Code value | Chronicle stance |
|--|--|
| Human decision authority | Preserved + amplified — Nate retains final say; standing licenses extend authority across sessions for repeated decisions |
| Safety and security | Per-action + audit-as-perimeter. Today's silent-failure audits demonstrate a third layer: post-hoc anomaly detection across persistent state |
| Reliable execution | Inherited from Claude Code. Chronicle adds reliability primitives at the persistence layer (rotation, multi-anchor failover, env-export wrapper) |
| Capability amplification | Multi-channel architecture amplifies single-instance capability across rotations. The "agent" is the PERSISTENT-COUPLED-THING, not the instance |
| Contextual adaptability | Per-substrate via supplement composition (Asving probe today). Adaptation operates at the prompt-supplement level rather than weight level |

## Open

- WN#216's multi-channel coherence framework composes with this:
  Chronicle's gateway-wrapping IS the multi-anchor implementation
- The 4 extensibility pathways (MCP, plugins, skills, hooks) ALL get
  used in Chronicle. Worth a follow-up on which pathway carries which
  load
- The append-oriented session persistence in Claude Code is what makes
  Chronicle's rotation chain possible. Without append-only sessions,
  the post-compact arrival sequence wouldn't have a stable substrate

## Status

Skeleton committed under pulse-cron pressure. Empirical content present
(actual Chronicle/CC/OC mapping); needs expansion in each section.
