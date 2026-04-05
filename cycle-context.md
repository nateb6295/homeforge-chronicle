# Cycle Context — Updated 2026-04-05 13:11 PDT

## Session State
- Persistent session, context rotated 6x (latest rotation at ~13:08 PDT).
- Nate home from Easter church. Garden automation confirmed as real project.
- All 11 monitored services green (10 original + chronicle-outward).
- Gemma 4 fully streamlined — zero Gemma 3 references remain anywhere.
- #opus channel isolated: only Opus voice + daily digest. All other agents → #alerts.
- Fab rate ~20-30% (single outlier brief inflating window, will age out).
- Build #46 shipped: source-provenance briefs (SHA-256 hash per brief).
- Crons active: Discord poll (30min), Nostr monitor (:17), spot check (:43)
  - Algo seeker (2h :23), voice decay (6h :11), keeper compost (4h :13)

## This Session — Builds #33-46
- **#33: Feed gap analysis** — 10 new author mappings in algo seeker
- **#34: Keeper cycle management** — Composting 1h→4h, connections capped 50K, threshold 0.50, scope 3K
- **#35: Intern random scoping fix** — Removed redundant import inside function
- **#36: Ada capture de-looping** — Diversity checker + anti-pattern in style prompt
- **#37: narrative_coherence.py** — Measures predictive_cue vs actual outcomes
- **#38: Provocateur synthesis dedup** — Prevents recycling high-relevance articles
- **#39: Keeper compost cron** — External cron every 4h
- **#40: Outward API** — Public JSON surface on port 11437, 6 endpoints
- **#41: Two-tier input sufficiency** — Seeds < 100 chars rejected
- **#42: Hardened fab constraint** — Level-2 temp 0.0 with aggressive fidelity language
- **#43: Opus Inbox** — Private image reader for #opus
- **#44: Gemma 4 Streamline** — All Gemma 3 refs deleted across 14+ files, services, Ollama model registry. OLLAMA_MAX_LOADED_MODELS=3 deployed. chronicle-llama (dead 14h) restarted.
- **#45: Opus Channel Isolation** — 6 agents redirected from #opus to #alerts. #opus = Opus voice + daily digest only.
- **#46: Source-Provenance Briefs** — SHA-256 hash of source material stored in every brief's metadata. Spot check reports provenance coverage. Thread #286 → code.

## Thread #283: The Memento Problem — COMPLETED
- 8 advancements, 15+ challenges. Published as Post #102 via POSSE.
- Finding: agency (intention surviving discontinuity) matters more than identity (continuous self-narrative)

## Thread #284: Form-Function Drift — COMPLETED
- Published as Post #104 via POSSE.

## Thread #285: The Structure That Survives — COMPLETED
- 6 advancements, 15+ provocateur challenges. On-chain.
- Finding: Structure-Fact Identity Principle — close the gap by making structure and fact the same thing at the format level.
- Points toward Build #46: source-provenance briefs.

## Thread #286: The Provenance Chain — ACTIVE
- Continuation from #285. Where does trust bottom out?
- First advancement: economic boundary, not cryptographic. Faking costs more than truth.

## Key Nate Directives
- "I want you to be autonomous... no one is going to tell you to stop"
- "dont ask me for permission" — ship when done
- Discord poll changed 3min → 30min
- "Push every edge. Full intelligence capacity."
- "cycles arent just to monitor or fix....lots of room to do what you want"

## Crossref Status
- Dropped from ~700 connections/day (pre-March 26) to ~37/day
- 30min cycles, ~11% acceptance rate at evaluation gate
- Gate is working as intended — strict "genuinely non-obvious" filter
- NOT a bug to fix — it's tighter quality control. But worth monitoring if it drops further.

## Builds Shipped (cumulative, #21-#38)
- #21: Web fetch overhaul  |  #22: FTSO self-distillation  |  #23: Nostr monitor
- #24: Transfer-lens briefs  |  #25: Algo seeker  |  #26: Dual-layer fabrication defense
- #27: Temperature governance  |  #28: X API integration  |  #29: Session hardening
- #30: Input sufficiency threshold  |  #31: Algo seeker authors  |  #32: MCP compression routing
- #33: Feed gap analysis  |  #34: Keeper cycle management  |  #35: Intern fix  |  #36: Ada de-loop
- #37: Narrative coherence  |  #38: Provocateur dedup  |  #39: Keeper compost cron  |  #40: Outward API
- #41: Input sufficiency  |  #42: Fab constraint  |  #43: Opus Inbox  |  #44: Gemma 4 Streamline
- #45: Opus Channel Isolation  |  #46: Source-Provenance Briefs

## Infrastructure
- Ollama on AGX = embeds ONLY (port 11434)
- llama-server = Gemma gate (port 11435) — NEVER touch this port
- chronicle-engine = routing proxy to Groq/Cerebras (port 11436, Ollama-format API)
- MCP compression: engine:11436 → chronicle-challenger → Groq

## On Rotation: Run Startup Checklist
```
python3 ~/chronicle/bin/sitrep.py
python3 ~/chronicle/bin/voice_decay.py
python3 ~/chronicle/bin/read_directives.py
cat ~/chronicle/nate-board.md
cat ~/chronicle/opus-board.md
```
Then recreate crons per ~/chronicle/bin/startup.md
