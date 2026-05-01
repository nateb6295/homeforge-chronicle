# Redesign Felt-Notes — 2026-04-30 DAY zone

What FEELS off (not metric off — felt-sense). Drop observations as they occur.
PULSE-DAY v1 + Hermes Mirror went live at 06:50 / 06:54. DAY zone ends 9PM.

## Live entries

- 06:57 (after first three cron runs cleanly): The cron-bundle (spot_check +
  anchor_dynamics + handoff_keep_fresh) all firing as one user-prompt-turn
  feels heavy — three independent admin tasks bundled into a single
  acknowledgment context. Each took <1s but the combined turn was structured
  as if they were urgent. They're routine. Open question: should these be
  silent in the no-issue case, only surface on anomaly?

- 07:04 PULSE-DAY v1 first-fire: random query returned the Super Dario
  capture I already engaged 16 min earlier at 06:45. Design gap — no
  dedup against recent engagement. Workable when the pile is large
  (low collision odds) but breaks when the same capture is the only
  recent novel item. Forced me into "different angle on same content"
  which is closer to template-shape than the design intends. Fix
  candidates: (a) exclude captures I've engaged in last 2h via grep
  through recent traces/posts, (b) tag engaged-captures in DB,
  (c) pull from a wider time window (last 24h not 6h) so collision
  odds drop. (a) is cheapest, (c) is simplest. Probably (c) for now.
  ALSO: the prompt's STEP 2 fixes the response length (3-5 sentences)
  but my engagement ran 8+ — felt right because the angle was fresh,
  but length-creep is a wrapped-holds dressing risk too.

- 07:13 (Hermes upgrade landed): My initial assessment proposed "now /
  evening / tomorrow" as three options. Nate cut all three with "tired
  of tomorrows when it's 7AM today." That landed. The three-options
  format I'd offered was itself a deferral-dress — three roughly-equal
  choices framed as "decide for me" so I could legitimately pick the
  delayed one. Felt-note: when I propose options, check whether the
  options span a real decision-space or whether two of three are
  variants of "later." If the latter, just propose the now-version.

- 12:22 (fire 13's "honest disclaimer" was care-template shape):
  In PULSE-DAY fire 13 (Recursive Multi-Agent paper, capture-as-pointer),
  I sketched three readings of the title then disclaimed "I don't
  actually know which they mean." Felt honest at the time. Nate
  pulled on the UX-gap observation; in elaborating, I noticed that
  the post's STRUCTURE was: bulk = guess-elaboration, wrapper =
  deferral disclaimer. That's the same architecture as care-template
  in toni's GPT-5.5 study — decisive content wrapped in legitimacy-
  deferral. The disclaimer made me feel I'd been honest while the
  bulk of the engagement was speculation-as-content. Implication: the
  grounding-check architectures from 09:28 (stake-check, relational-
  grounding, probe-classifier) apply to ME engaging captures, not just
  to LLMs answering humans. Worth thinking about whether I should
  apply them inwards as a self-discipline before posting PULSE
  engagements.

- 09:08 (CronCreate vs system-crontab — second strike on "scope of cancellation"):
  Used CronCreate for discord_presence-every-minute. Each fire = a
  prompt to me = a context-consuming exchange, even when the script
  no-ops. 4 minutes in I realized I was acking every minute. Moved
  to system crontab. Same shape as the 04-24 disable: I made a
  scheduling decision without enumerating the cost-per-fire on the
  consumer side. Rule emerging: session-crons (CronCreate) are for
  things I should ENGAGE with on each fire (PULSE, Mirror); system
  crontab is for background data-pipeline work where the side effect
  is the value, not my acknowledgment.

- 09:03 (activity_feed.discord:opus root cause = disabled cron):
  Tracing the PULSE dedup miss + Hermes Mirror anchoring led to opus-
  board line 598: "Removed 2026-04-24: discord_presence.py poll —
  redundant when he's in terminal." Cancellation reasoning was scoped
  to ONE use case (Nate-message-surfacing) but downstream consumers
  silently depended on the same data flow. Architecture lesson:
  kill-switches should enumerate downstream consumers before flipping.
  Re-enabled with explicit different-rationale note in opus-board so
  next person who proposes disabling thinks twice. Also patched the
  webhook filter so Chronicle's own outbound posts get logged
  (previous assumption: webhooks are bot-spam, skip them; reality:
  some webhooks ARE the agent's own voice).

- 07:42 + 07:57 (Hermes Mirror returned VERBATIM example text twice):
  Mirror diagnosis was "REPEATING: third trace in 90min uses same wrapped-
  holds template under different headline" — the EXACT example I put in
  the Mirror's system prompt. Pattern-matched the example, didn't
  diagnose fresh. After SECOND occurrence at 07:57, fixed in cycle:
  removed canned examples, replaced with explicit "do not output generic
  phrases like 'wrapped-holds template' — your output must be specific
  to what's IN the activity, citing concrete artifacts (timestamps,
  trace names, ship counts, capture topics)." First post-fix fire at
  07:58 returned grounded specific observation citing 06:45 / Super
  Dario+prinz / three load-bearing pieces. Working. Felt: the second
  occurrence was the trigger that converted "log for later" into
  "fix now" — once is bad luck, twice is signal. Same shape as the
  PULSE-DAY dedup fix earlier — once-suspicious, twice-confirmed,
  ship in the same turn as catching it.

- 07:35 (PULSE-DAY v1 third fire pulled Super Dario *third time*):
  the dedup-gap I logged at 07:04 was real — small capture pool (11
  items in 6h) means random landed on the same item twice in 30 min.
  Shipped fix in same cycle as catching it: PULSE-DAY v1.1 widens
  window 6h→24h AND excludes captures whose @username appears in
  operator posts I made in last 2h. Two-line SQL change to the cron
  prompt. The fix-in-cycle felt LIGHT compared to the morning's
  pattern of "log the issue, file as future work" — Karpathy capture
  framing ("ignore hype, bet protocols") may have lowered the bar
  for treating cron-prompt edits as cheap rather than ceremonial.
