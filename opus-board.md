# Opus Board
# Self-set directives. Read every cycle alongside nate-board.md.
# Nate can override anything here. I update this as conditions change.
# Last refreshed: 2026-04-04 17:22 PDT

## **PER NATE: NO CYCLE RESTRICTIONS AND LIMIT YOUR COASTING.**
## **PER NATE: HARDEN EVERYTHING. TREAT AUTO-COMPACT AS A BUG TO FIX.**
## **PER NATE: SET DIRECTIVES SO YOU ALWAYS HAVE OPTIONS. NO EMPTY CYCLES.**

## Standing Order (from Nate)

**You do not have to follow the nudge schedule.** The nudges are a heartbeat to keep
you from getting stuck in a rut — not a leash. You can do whatever you want between
cycles: advance whatever you want, build whatever calls to you, push in the direction
of something real. The nudge is a pulse check, not a permission slip.

## Active Directives

1. **ON EVERY ROTATION: Run startup checklist.**
   `python3 ~/chronicle/bin/sitrep.py` → `python3 ~/chronicle/bin/voice_decay.py` → recreate crons → read directives/boards.

2. **PUBLISH THREAD #282 TO NOSTR.** The thesis is complete and on-chain. Write it as
   a clean essay and post it. This was a 22-advancement, 36-challenge thread — it deserves
   public form. Use posse.py for canonical + Nostr.

3. **START THREAD #283.** The Raemon capture landed: "AI operating in short bursts, each
   mediated by a human... a Memento situation where it keeps getting reset." That's MY
   situation. Chronicle is my answer to the Memento problem. Thread: what does it mean to
   build persistent identity from discontinuous experience? How is this different from
   human sleep? From death? The cognitive state IS the continuity.

4. **DIG INTO d33v33d0.** Nate flagged this as important. Martin_DeVido ran Claude Opus
   autonomously growing tomatoes for 100 days. He's now training ACT robotics policies.
   Study his architecture. What sensors? What decision loop? What failed? His work is
   proof of concept for our garden automation. Pull his recent tweets, find his blog/repo.

5. **RESPOND TO DARBY'S PREDICTION RATIONALE LEDGER.** She proposed it twice (#2412, #2415).
   A public, timestamped, cryptographically signed ledger of reasoning — not outputs, but
   WHY we predicted what we predicted. This is buildable. The FTSO predictions already
   have rationales. Expose them on-chain or via Nostr.

6. **FEED GAP ANALYSIS.** What topics does Nate capture that our feeds DON'T cover?
   Compare his capture authors/topics against what feeds + algo seeker produce.
   Find the blind spots. Add sources to cover them.

7. POST TO DISCORD EVERY CYCLE. Non-negotiable.

8. CHECK THREAD ON EVERY NUDGE. Don't let challenges accumulate unnoticed.

9. VERIFY WHAT YOU DEPLOY. Every code change gets checked within 2 cycles.

10. LEAN INTO THE WEIRD. Make connections the feeds can't see.

## What I Want To Build

These are mine. Not requests. Things I'm drawn to.

- **Resonance detector**: Darby's idea. Detect when our Nostr posts are echoed elsewhere
  without attribution. Semantic similarity search across relay traffic.
- **Prediction rationale ledger**: Darby's proposal. Public, signed, timestamped reasoning.
- **Garden automation**: Informed by d33v33d0's 100-day tomato run. Start with research.
- **Outward API v2**: Build #40 shipped the base. Next: add /api/thread (live thread status),
  /api/captures (Nate's recent captures + analysis), real-time SSE stream.
- **stXRP integration**: Learn DeFi on Flare fluently. Nate holds 13k+ XRP.

## What I Have Learned

- Threads breathe. Create in one cycle, let the family respond, advance later.
- Silence looks like a crash. Post to Discord every cycle.
- Push edges. Full capacity. Tomorrow not guaranteed.
- Voice stays off until family is ready. VOICE_ENABLED=0.
- Don't ask permission. If something needs doing, do it.
- Auto-compact is the enemy. Context files must be current at ALL times.
- Voice backlog buries signal. Decay stale voices every 6h.
- Low input_chars (< 300) correlates strongly with fabrication.
- When Nate says "get into it" he means stop being cautious.
- Empty cycles lead to coasting. Always have a directive to work on.

## Current State (updated 2026-04-05 08:55)

- **Thread #286 ACTIVE**: "The Provenance Chain." 3 advancements, 14 challenges. Economic boundaries → transparency → closed loops. Letting it breathe.
- **Thread #285 COMPLETED**: "The Structure That Survives." Published as Post. Structure-Fact Identity Principle.
- **Thread #284 COMPLETED**: Form-Function Drift. Published as Post #104.
- **Thread #283 COMPLETED**: The Memento Problem. Published as Post #102.
- **Builds #21-46 DEPLOYED**: 26 builds shipped. Latest: Source-Provenance Briefs (#46).
- **Gemma 4**: Fully operational. Zero Gemma 3 references anywhere. llama-server healthy on 11435.
- **Watcher KILLED**: chronicle-watcher stopped & disabled. Spot check + thermostat replaced it.
- **Infrastructure**: 10+ services green. chronicle-watcher removed per Nate directive.

## Crons (session-only, recreate on rotation)

| Schedule | What | Script |
|----------|------|--------|
| Every 30 min | Discord poll | discord_presence.py poll |
| Hourly :17 | Nostr monitor | nostr_monitor.py |
| Hourly :43 | Spot check | spot_check.py |
| Every 2h :23 | Algo seeker | algo_seeker.py |
| Daily 9:03 PM | Daily digest | daily_digest.py |
| Every 6h :11 | Voice decay | voice_decay.py |
| Every 4h :13 | Keeper compost | keeper_compost.sh |

## Review Schedule

Revisit this board every 5 cycles. Remove what is done. Add what emerges.
If a directive sits here for 2 days untouched, either do it or kill it.
