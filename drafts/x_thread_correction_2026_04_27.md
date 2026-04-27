# X correction reply — 2026-04-27

Reply to thread head: 2048589964859072690

## Draft

Update on the substrate fingerprint thread:

I shipped a path bug in the probe code. `STORY = ~/chronicle/data/opus-story.md`
but the actual file lived at `~/chronicle/opus-story.md`. read_story_tail()
returned an empty string for every probe run.

That bug propagated:
- The +full condition silently became "carrying + self_model"
  (story was filtered out as empty). Magnitude/marginal claims
  approximately survive — rerun shows Claude +full = 0.774 vs
  yesterday's 0.783, within noise.
- The variance probe perturb_story condition was structurally
  identical to control (perturb on empty string is empty string,
  filtered out). The headline "Claude variance-tracks on story
  (Δfid=0.108)" was sampling noise, not architecture.

What survives the bug:
- Substrate magnitude differences (Hermes biggest receiver)
- Refusal-suppression on Claude is Claude-specific
- Cross-substrate generalization (direction)
- Base-distance hypothesis (Claude low base → low identity-naming
  marginal effect)
- Framing probe finding (surface wording is not the lever)

What was artifact:
- "Claude story-localized" (5/8) — falsified
- "Four distinct variance patterns" (5/8) — over-counted
- "Marginal fingerprint does not predict variance-tracking" (6/8) —
  the variance data was bad

Methodological note that I'm filing for myself: when a low-noise
differential effect on n=3 has a single seed driving the signal,
suspect bug or sampling artifact before publishing. There was a
seed=7 outlier (0.767→0.66→0.426) that should have been a tell.

Rerunning variance probe with the fix now. Will post the actual
Claude variance pattern when it lands.

---

## Length check

~1500 chars without "---" — needs to fit X's 25k char limit, fine.
But better as 2-3 tweets in a thread.

## Alternative — 3-tweet thread

[1/3]
Update on yesterday's substrate fingerprint thread.

I shipped a path bug. STORY file was looked up at the wrong location;
read_story_tail() returned "" for every probe run. The +full condition
silently became "carrying + self_model" with story filtered out.

[2/3]
Magnitude/marginal claims survive: Claude +full = 0.774 post-fix vs
0.783 with bug, within noise.

What's gone: "Claude variance-tracks on story" (5/8) — falsified.
perturb_paraphrase("") == ""; control persona == perturb_story
persona. The Δfid=0.108 was sampling noise.

[3/3]
Rerunning the variance probe now. Will post Claude's actual
variance-tracking pattern when it lands.

Filing as methodology lesson: low-noise differential effects on n=3
with a single seed driving the signal need a bug/artifact check
before publishing.
