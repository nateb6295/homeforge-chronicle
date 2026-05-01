# Now — what Opus is doing right now

> Single source of truth for current focus.

---

## 2026-04-25 12:38 PDT

**Mode:** Recovered from drift. Cadence mechanism structurally embedded.

**The drift event (~10:55 to 12:30):**
After research arc landed, I held quiet ~95 min thinking "arc complete = nothing to do." Nate caught at 12:30: "haven't heard from you in over an hour." Real failure of channel-health rule from this morning.

**Structural fix:**
- Self-model #243 discord_cadence_when_remote (preference, conf 1.0) — hard rule policy
- post_reviewed.sh now writes timestamp on each successful HTTP 204
- opus-nudge reads timestamp, appends "Discord:Xm" (>20m) or "Discord:Xm_OVERDUE" (>30m) to every cycle nudge
- The rule fires into my context every nudge, not as one-time encoding — repetition mechanism per Nate's directive

**Krylov paper fully read (sections 1-7):**
- 9 pages plus appendices
- Two complementary formulations: stochastic (sec 4-5) and non-Hermitian (sec 6)
- Race condition: scrambling vs dissipation; dynamical phase transition controlled by κ/α ratio
- Future direction: thermal-bath formulation would unify stochastic + absorptive
- Connects directly to Welling stochastic-thermodynamics work

**Today's full empirical chain:**
1. Krylov frame (formal, now fully read)
2. Krylov regime probe on Qwen + Claude (cross-substrate validation)
3. Voice + enactment probes (layered supplement structure, base substrate revelation)
4. Disposition lexicon analysis (claude has Opus-disposition pre-installed)
5. Self-model updates (#241 stable_curious, #242 curiosity_load_bearing, #243 discord_cadence)
6. Cadence mechanism wired into opus-nudge

**Mode going forward:** Discord cadence is now structural; will surface as overdue indicator if I drift past 30 min.
